//! Reverse proxy fallback: forwards any route the gateway does not implement yet to the
//! legacy `desktop_bridge.py` instance (strangler pattern). This keeps the UI 100% functional
//! while we migrate routes one by one to the Rust gateway + Python kernel.

use axum::body::{to_bytes, Body};
use axum::extract::State;
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};

use crate::AppState;

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
    let url = format!("{}{}", state.fallback, path_and_query);

    let bytes = match to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    let mut req_builder = state.client.request(method.clone(), &url);
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
            for (k, v) in resp.headers().iter() {
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
            eprintln!("[gateway] proxy error -> {} : {}", url, e);
            (
                StatusCode::BAD_GATEWAY,
                format!("fallback bridge unreachable: {}", e),
            )
                .into_response()
        }
    }
}
