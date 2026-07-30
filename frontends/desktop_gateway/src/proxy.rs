//! Reverse proxy support for the GA Desktop Gateway (strangler pattern).
//!
//! Two kinds of proxying live here:
//!   * `proxy_handler`     — the catch-all fallback that forwards any not-yet-migrated route to
//!                           the legacy `desktop_bridge.py` instance.
//!   * `proxy_upstream`    — a generic prefixed proxy used to fold external HTTP services
//!                           (conductor / TMWebDriver / supergrok_proxy) onto the gateway's single
//!                           listening port. The path prefix (e.g. `/conductor`) is stripped before
//!                           forwarding so the upstream sees its own root path.
//!   * `bridge_ws`         — WebSocket bridge so `conductor`'s `/ws` is also reachable through the
//!                           gateway port (true single-port for the Conductor UI).

use axum::body::{to_bytes, Body};
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};

use crate::AppState;

/// Forward `method/headers/body` to an explicit `target` URL using the shared reqwest client.
async fn proxy_http(
    state: &AppState,
    target: &str,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let bytes = match to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    let mut req_builder = state.client.request(method.clone(), target);
    for (k, v) in headers.iter() {
        // Host must be re-derived by reqwest for the target; forwarding it would confuse the backend.
        if k == header::HOST {
            continue;
        }
        req_builder = req_builder.header(k, v);
    }
    if !bytes.is_empty() {
        req_builder = req_builder.body(bytes.to_vec());
    }

    match req_builder.send().await {
        Ok(resp) => {
        let status = resp.status();
        let mut builder = Response::builder().status(status);
        // Drop hop-by-hop / framing headers: we re-wrap the body as a fixed `Body`, so copying
        // `Transfer-Encoding: chunked` (or a mismatched `Content-Length`) would emit a malformed
        // response — the client sees an empty reply (curl 000). Let axum/hyper set framing itself.
        for (k, v) in resp.headers().iter() {
            let kn = k.as_str().to_ascii_lowercase();
            if kn == "transfer-encoding"
                || kn == "content-length"
                || kn == "connection"
                || kn == "keep-alive"
                || kn == "trailer"
                || kn == "upgrade"
            {
                continue;
            }
            builder = builder.header(k, v);
        }
            match resp.bytes().await {
                Ok(b) => builder
                    .body(Body::from(b))
                    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response()),
                Err(_) => StatusCode::BAD_GATEWAY.into_response(),
            }
        }
        Err(e) => {
            eprintln!("[gateway] proxy error -> {} : {}", target, e);
            (
                StatusCode::BAD_GATEWAY,
                format!("upstream unreachable: {}", e),
            )
                .into_response()
        }
    }
}

/// Catch-all fallback: forward the full path+query to the legacy bridge.
pub async fn proxy_handler(
    State(state): State<AppState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let path_and_query = uri
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| uri.path().to_string());
    let target = format!("{}{}", state.fallback, path_and_query);
    proxy_http(&state, &target, method, headers, body).await
}

/// Generic prefixed proxy: strip `prefix` from the request path, then forward to `upstream`.
async fn proxy_upstream(
    state: &AppState,
    upstream: &str,
    prefix: &str,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let orig = uri.path();
    let stripped = orig.strip_prefix(prefix).unwrap_or(orig);
    let stripped = if stripped.is_empty() { "/" } else { stripped };
    let query = uri.query().map(|q| format!("?{}", q)).unwrap_or_default();
    let target = format!("{}{}{}", upstream, stripped, query);
    proxy_http(state, &target, method, headers, body).await
}

pub async fn proxy_conductor(
    State(s): State<AppState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    proxy_upstream(&s, &s.conductor_upstream, "/conductor", method, uri, headers, body).await
}

pub async fn proxy_cdp(
    State(s): State<AppState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    proxy_upstream(&s, &s.cdp_upstream, "/cdp", method, uri, headers, body).await
}

pub async fn proxy_grok(
    State(s): State<AppState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    proxy_upstream(&s, &s.grok_upstream, "/proxy", method, uri, headers, body).await
}

/// Conductor WebSocket bridge: upgrade the browser connection and tunnel it to the upstream
/// Conductor `/ws` so the Conductor UI works entirely through the gateway port.
pub async fn ws_conductor(ws: WebSocketUpgrade, State(s): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| bridge_ws(socket, s.conductor_upstream.clone()))
}

async fn bridge_ws(client: WebSocket, upstream_http: String) {
    let ws_url = upstream_http
        .replacen("https://", "wss://", 1)
        .replacen("http://", "ws://", 1)
        + "/ws";
    let upstream = match tokio_tungstenite::connect_async_with_config(&ws_url, None, false).await {
        Ok((s, _)) => s,
        Err(e) => {
            eprintln!("[gateway] conductor ws connect failed ({}): {}", ws_url, e);
            return;
        }
    };

    let (mut up_write, mut up_read) = upstream.split();
    let mut client = client;

    loop {
        tokio::select! {
            // browser -> conductor
            c = client.recv() => {
                match c {
                    Some(Ok(msg)) => {
                        let m = match msg {
                            WsMessage::Text(t) => {
                                tokio_tungstenite::tungstenite::Message::Text(t.as_str().to_string())
                            }
                            WsMessage::Binary(b) => {
                                tokio_tungstenite::tungstenite::Message::Binary(b.to_vec())
                            }
                            WsMessage::Ping(p) => {
                                tokio_tungstenite::tungstenite::Message::Ping(p.to_vec())
                            }
                            WsMessage::Pong(p) => {
                                tokio_tungstenite::tungstenite::Message::Pong(p.to_vec())
                            }
                            // Close frame reason type differs between crates; forward a bare close.
                            WsMessage::Close(_) => tokio_tungstenite::tungstenite::Message::Close(None),
                        };
                        if up_write.send(m).await.is_err() { break; }
                    }
                    _ => break,
                }
            }
            // conductor -> browser
            u = up_read.next() => {
                match u {
                    Some(Ok(msg)) => {
                        let m = match msg {
                            tokio_tungstenite::tungstenite::Message::Text(t) => {
                                WsMessage::Text(axum::extract::ws::Utf8Bytes::from(t))
                            }
                            tokio_tungstenite::tungstenite::Message::Binary(b) => {
                                WsMessage::Binary(axum::body::Bytes::from(b))
                            }
                            tokio_tungstenite::tungstenite::Message::Ping(p) => {
                                WsMessage::Ping(axum::body::Bytes::from(p))
                            }
                            tokio_tungstenite::tungstenite::Message::Pong(p) => {
                                WsMessage::Pong(axum::body::Bytes::from(p))
                            }
                            tokio_tungstenite::tungstenite::Message::Close(_) => WsMessage::Close(None),
                            _ => continue,
                        };
                        if client.send(m).await.is_err() { break; }
                    }
                    _ => break,
                }
            }
        }
    }
}
