//! GA Desktop Gateway — Rust/axum front layer for the GenericAgent desktop backend.
//!
//! Responsibilities:
//!   * serve the static frontend (index.html / app.js / styles.css / vendor/*)
//!   * implement the core session/config/conv-folder/model-profile routes by talking to the
//!     Python kernel over JSON-RPC (stdio) — see `../kernel_protocol.md`
//!   * forward `session.stream` kernel notifications to the browser over `/ws`
//!   * reverse-proxy every not-yet-migrated route to the legacy bridge (strangler fallback)
//!
//! This crate is both a standalone binary (`ga-desktop-gateway`, for the Phase 1 strangler
//! deployment) and a library so the Tauri shell can embed the axum server in-process
//! (Phase 2: single Rust process + a single Python kernel subprocess).

pub mod bridge;
pub mod kernel;
pub mod proxy;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::ws::{Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;

use kernel::KernelSupervisor;
use bridge::BridgeSupervisor;

#[derive(Clone)]
pub struct AppState {
    pub kernel: Arc<KernelSupervisor>,
    pub bridge: Arc<BridgeSupervisor>,
    pub fallback: String,
    pub static_dir: PathBuf,
    pub bridge_port: u16,
    pub conductor_port: u16,
    pub conductor_upstream: String,
    pub cdp_upstream: String,
    pub grok_upstream: String,
    pub ga_root: String,
    pub client: reqwest::Client,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn content_type(path: &PathBuf) -> &'static str {
    match path.extension().and_then(|e| e.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "application/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json") => "application/json; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("woff2") => "font/woff2",
        Some("woff") => "font/woff",
        Some("ico") => "image/x-icon",
        Some("map") => "application/json",
        _ => "application/octet-stream",
    }
}

// ---------------------------------------------------------------------------
// Static frontend
// ---------------------------------------------------------------------------

async fn index_handler(State(s): State<AppState>) -> Response {
    match tokio::fs::read(s.static_dir.join("index.html")).await {
        Ok(b) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
            .body(Body::from(b))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response()),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn ga_ports_handler(State(s): State<AppState>) -> Response {
    let body = format!(
        "window.GA_PORTS={{bridge:{},conductor:{}}};",
        s.bridge_port, s.conductor_port
    );
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/javascript; charset=utf-8")
        .body(Body::from(body))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

async fn static_file_handler(State(s): State<AppState>, uri: axum::http::Uri) -> Response {
    // only allow single-segment root files (app.js / styles.css); never traverse.
    let p = uri.path().trim_start_matches('/').to_string();
    if p.is_empty() || p.contains('/') || p.contains("..") {
        return StatusCode::NOT_FOUND.into_response();
    }
    let path = s.static_dir.join(&p);
    match tokio::fs::read(&path).await {
        Ok(b) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, content_type(&path))
            .body(Body::from(b))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response()),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

// ---------------------------------------------------------------------------
// Kernel-backed JSON-RPC routes
// ---------------------------------------------------------------------------

/// True when the failure means "there is no kernel to talk to" rather than "the kernel replied
/// with an error". The frontend keys off the resulting 503 to offer a restart instead of
/// rendering a generic failure (or hanging on a spinner forever).
fn is_kernel_down(err: &str) -> bool {
    err.contains("restarting")
        || err.contains("process exited")
        || err.contains("process is gone")
        || err.contains("dropped the response channel")
}

async fn kernel_json(state: &AppState, method: &str, params: Value) -> Response {
    match state.kernel.call(method, params).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => {
            let code = if is_kernel_down(&e) {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            (code, e).into_response()
        }
    }
}

async fn status_handler(State(s): State<AppState>) -> Response {
    kernel_json(&s, "status", json!({})).await
}

async fn kernel_state_handler(State(s): State<AppState>) -> Response {
    Json(json!({ "up": s.kernel.is_up().await, "generation": s.kernel.generation() }))
        .into_response()
}

async fn kernel_restart_handler(State(s): State<AppState>) -> Response {
    match s.kernel.clone().restart().await {
        Ok(()) => Json(json!({ "success": true, "generation": s.kernel.generation() }))
            .into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "success": false, "error": e })),
        )
            .into_response(),
    }
}

async fn bridge_state_handler(State(s): State<AppState>) -> Response {
    Json(json!({ "up": s.bridge.is_up().await, "generation": s.bridge.generation() }))
        .into_response()
}

async fn bridge_restart_handler(State(s): State<AppState>) -> Response {
    match s.bridge.clone().restart().await {
        Ok(()) => Json(json!({ "success": true, "generation": s.bridge.generation() }))
            .into_response(),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "success": false, "error": "bridge restart failed" })),
        )
            .into_response(),
    }
}

async fn config_get(State(s): State<AppState>) -> Response {
    kernel_json(&s, "config.get", json!({})).await
}

async fn config_set(State(s): State<AppState>, Json(body): Json<Value>) -> Response {
    let cfg = body.get("config").cloned().unwrap_or_else(|| body.clone());
    kernel_json(&s, "config.set", json!({ "config": cfg })).await
}

async fn conv_folders_get(State(s): State<AppState>) -> Response {
    kernel_json(&s, "conv_folders.get", json!({})).await
}

async fn conv_folders_put(State(s): State<AppState>, Json(body): Json<Value>) -> Response {
    kernel_json(&s, "conv_folders.set", body).await
}

async fn sessions_list(State(s): State<AppState>) -> Response {
    kernel_json(&s, "session.list", json!({})).await
}

async fn session_new(State(s): State<AppState>, Json(body): Json<Value>) -> Response {
    kernel_json(
        &s,
        "session.create",
        json!({
            "cwd": body.get("cwd").or(body.get("path")).cloned(),
            "project": body.get("project").cloned(),
        }),
    )
    .await
}

async fn session_get(State(s): State<AppState>, Path(sid): Path<String>) -> Response {
    kernel_json(&s, "session.get", json!({ "sid": sid })).await
}

async fn session_delete(State(s): State<AppState>, Path(sid): Path<String>) -> Response {
    kernel_json(&s, "session.delete", json!({ "sid": sid })).await
}

async fn session_patch(
    State(s): State<AppState>,
    Path(sid): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    kernel_json(
        &s,
        "session.patch",
        json!({ "sid": sid, "fields": body }),
    )
    .await
}

async fn session_messages(State(s): State<AppState>, Path(sid): Path<String>) -> Response {
    kernel_json(&s, "session.messages", json!({ "sid": sid })).await
}

async fn session_prompt(
    State(s): State<AppState>,
    Path(sid): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    let prompt = body
        .get("prompt")
        .or(body.get("content"))
        .or(body.get("message"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    kernel_json(
        &s,
        "session.prompt",
        json!({
            "sid": sid,
            "prompt": prompt,
            "images": body.get("images").cloned().unwrap_or(Value::Array(vec![])),
            "llmNo": body.get("llmNo").cloned(),
            "display": body.get("display").cloned(),
            "files": body.get("files").cloned().unwrap_or(Value::Array(vec![])),
            "imageMetas": body.get("imageMetas").cloned().unwrap_or(Value::Array(vec![])),
            "expert": body.get("expert").cloned(),
        }),
    )
    .await
}

async fn session_cancel(State(s): State<AppState>, Path(sid): Path<String>) -> Response {
    kernel_json(&s, "session.cancel", json!({ "sid": sid })).await
}

async fn session_viewed(State(s): State<AppState>, Path(sid): Path<String>) -> Response {
    kernel_json(&s, "session.viewed", json!({ "sid": sid })).await
}

async fn session_restore(State(s): State<AppState>, Path(sid): Path<String>) -> Response {
    kernel_json(&s, "session.restore", json!({ "sid": sid })).await
}

async fn session_suggest(State(s): State<AppState>, Path(sid): Path<String>) -> Response {
    kernel_json(&s, "session.suggest", json!({ "sid": sid })).await
}

async fn session_plan(State(s): State<AppState>, Path(sid): Path<String>) -> Response {
    kernel_json(&s, "session.plan", json!({ "sid": sid })).await
}

async fn model_profiles_list(State(s): State<AppState>) -> Response {
    kernel_json(&s, "model_profiles.list", json!({})).await
}

// ---------------------------------------------------------------------------
// WebSocket: forward kernel `session.stream` notifications as `session-state` frames
// ---------------------------------------------------------------------------

async fn ws_handler(ws: WebSocketUpgrade, State(s): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| handle_ws(socket, s))
}

/// Pull the real services list from the legacy bridge (`GET /services/panel`).
/// Returns `[]` when the fallback bridge is unreachable.
async fn fetch_services_snapshot(state: &AppState) -> Value {
    let url = format!("{}/services/panel", state.fallback);
    match state
        .client
        .get(&url)
        .timeout(std::time::Duration::from_secs(3))
        .send()
        .await
    {
        Ok(resp) => match resp.bytes().await {
            Ok(b) => serde_json::from_slice::<Value>(&b)
                .ok()
                .and_then(|v| v.get("services").cloned())
                .unwrap_or_else(|| json!([])),
            Err(_) => json!([]),
        },
        Err(_) => json!([]),
    }
}

async fn handle_ws(mut socket: WebSocket, state: AppState) {
    let _ = socket
        .send(Message::Text(
            Utf8Bytes::from(
                serde_json::to_string(&json!({
                    "type": "bridge-ready",
                    "gaRoot": state.ga_root,
                    "mykeyPath": format!("{}/mykey.py", state.ga_root),
                    "http": true,
                    "wsEventsOnly": true,
                }))
                .unwrap()
                .as_str(),
            ),
        ))
        .await;
    let snapshot = fetch_services_snapshot(&state).await;
    let _ = socket
        .send(Message::Text(
            Utf8Bytes::from(
                serde_json::to_string(&json!({
                    "type": "services.snapshot",
                    "services": snapshot,
                }))
                .unwrap()
                .as_str(),
            ),
        ))
        .await;

    let mut rx = state.kernel.subscribe();
    let mut svc_tick = tokio::time::interval(std::time::Duration::from_secs(5));
    svc_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    svc_tick.tick().await; // consume the immediate first tick
    loop {
        tokio::select! {
            _ = svc_tick.tick() => {
                let snapshot = fetch_services_snapshot(&state).await;
                let frame = json!({"type": "services.snapshot", "services": snapshot});
                if socket.send(Message::Text(
                    Utf8Bytes::from(serde_json::to_string(&frame).unwrap().as_str()),
                )).await.is_err() { break; }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Text(t))) => {
                        if let Ok(v) = serde_json::from_str::<Value>(t.as_str()) {
                            let kind = v.get("action").or_else(|| v.get("type")).and_then(|x| x.as_str());
                            if kind == Some("ping") {
                                let _ = socket.send(Message::Text(
                                    Utf8Bytes::from(
                                        serde_json::to_string(
                                            &json!({"type":"pong","ts": now_secs()})).unwrap().as_str()),
                                )).await;
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
            notif = rx.recv() => {
                if let Ok(n) = notif {
                    if n.get("method").and_then(|x| x.as_str()) == Some("session.stream") {
                        let p = n.get("params").unwrap_or(&Value::Null);
                        let frame = json!({
                            "type": "session-state",
                            "sessionId": p.get("sid"),
                            "state": p.get("state"),
                            "status": p.get("status"),
                            "seq": p.get("seq"),
                            "updatedAt": p.get("updatedAt"),
                            "title": p.get("title"),
                        });
                        let _ = socket.send(Message::Text(
                            Utf8Bytes::from(serde_json::to_string(&frame).unwrap().as_str()),
                        )).await;
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

fn build_router(state: AppState) -> Router {
    let vendor_dir = state.static_dir.join("vendor");
    Router::new()
        .route("/", get(index_handler))
        .route("/ga-ports.js", get(ga_ports_handler))
        .route("/app.js", get(static_file_handler))
        .route("/styles.css", get(static_file_handler))
        .nest_service("/vendor", ServeDir::new(vendor_dir))
        // core routes -> kernel
        .route("/status", get(status_handler))
        // Kernel liveness + manual respawn. Served by the gateway itself, so they still answer
        // while the kernel (and with it the legacy bridge) is down.
        .route("/kernel/state", get(kernel_state_handler))
        .route("/kernel/restart", post(kernel_restart_handler))
        .route("/bridge/state", get(bridge_state_handler))
        .route("/bridge/restart", post(bridge_restart_handler))
        .route("/config", get(config_get).post(config_set))
        .route("/conv-folders", get(conv_folders_get).put(conv_folders_put))
        .route("/sessions", get(sessions_list))
        .route("/session/new", post(session_new))
        .route(
            "/session/{sid}",
            get(session_get).delete(session_delete).patch(session_patch),
        )
        .route("/session/{sid}/prompt", post(session_prompt))
        .route("/session/{sid}/messages", get(session_messages))
        .route("/session/{sid}/cancel", post(session_cancel))
        .route("/session/{sid}/viewed", post(session_viewed))
        .route("/session/{sid}/restore", post(session_restore))
        .route("/session/{sid}/suggest", post(session_suggest))
        .route("/session/{sid}/plan", get(session_plan))
        .route("/model-profiles", get(model_profiles_list))
        .route("/ws", get(ws_handler))
        // Fold external HTTP services onto this single port (strangler consolidation).
        // Path prefix is stripped before forwarding: /conductor/history -> :8900/history
        // The bare trailing-slash variants (`/conductor/`) are registered explicitly because
        // axum 0.8's `/conductor/{*rest}` catch-all does NOT match `/conductor/`; without them
        // the trailing slash falls through to the legacy-bridge fallback and hangs (curl 000).
        .route("/conductor", any(proxy::proxy_conductor))
        .route("/conductor/", any(proxy::proxy_conductor))
        .route("/conductor/{*rest}", any(proxy::proxy_conductor))
        .route("/conductor/ws", get(proxy::ws_conductor))
        .route("/cdp", any(proxy::proxy_cdp))
        .route("/cdp/", any(proxy::proxy_cdp))
        .route("/cdp/{*rest}", any(proxy::proxy_cdp))
        .route("/proxy", any(proxy::proxy_grok))
        .route("/proxy/", any(proxy::proxy_grok))
        .route("/proxy/{*rest}", any(proxy::proxy_grok))
        // everything else -> try static_dir first, then legacy bridge (strangler fallback)
        .fallback(any(fallback_static_or_proxy))
        .layer(CorsLayer::permissive())
        .with_state(state)
    }

    /// Fallback that serves a file from `static_dir` directly when it exists (so the
    /// frontend never depends on the legacy bridge being booted), and only proxies to
    /// the bridge otherwise. This eliminates the startup race where the WebView
    /// requested static assets before the bridge was ready (HTTP 502 → `<script>`/`<img>`
    /// load failures → invisible icons).
    async fn fallback_static_or_proxy(
        State(s): State<AppState>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Body,
    ) -> Response {
        let path = uri.path().trim_start_matches('/').to_string();
        if !path.is_empty() && !path.contains("..") && s.static_dir.join(&path).is_file() {
            return static_file_handler(State(s), uri).await;
        }
        proxy::proxy_handler(State(s), method, uri, headers, body).await
    }

/// Configuration for running the embedded gateway.
pub struct GatewayConfig {
    /// GA project root (contains `frontends/`). The kernel + legacy fallback are spawned here.
    pub root: PathBuf,
    /// Port the axum server listens on (browser connects here).
    pub port: u16,
    /// Conductor port advertised to the frontend (`window.GA_PORTS.conductor`) and forwarded to
    /// the kernel (which passes it to the legacy fallback bridge).
    pub conductor_port: u16,
    /// TMWebDriver (CDP browser bridge) upstream port, folded onto the gateway as `/cdp`.
    pub cdp_port: u16,
    /// supergrok_proxy upstream port, folded onto the gateway as `/proxy`.
    pub grok_port: u16,
    /// Explicit fallback URL for un-migrated routes. When empty, the gateway proxies to
    /// `http://127.0.0.1:<legacy_port>` (the kernel-spawned legacy bridge).
    pub fallback: String,
    /// Port the gateway spawns the legacy `desktop_bridge.py` on for the strangler fallback.
    /// The bridge is a *sibling* of the kernel (owned by the gateway), so it survives a
    /// kernel crash; it reconnects to the kernel's loopback config server after a respawn.
    pub legacy_port: u16,
    /// Fixed port for the kernel's loopback config store server. The kernel binds it and
    /// the bridge connects to it, so the bridge keeps working across kernel restarts.
    pub config_port: u16,
    /// Python interpreter used to run the kernel.
    pub kernel_python: String,
    /// Isolation dir for the kernel. Empty = kernel runs against the real project root (so it
    /// shares session data with the legacy fallback bridge).
    pub kernel_data_dir: String,
}

/// Run the gateway until the TCP listener closes.
///
/// Spawns the Python kernel (JSON-RPC over stdio) AND the legacy `desktop_bridge.py` as
/// sibling subprocesses, and serves the axum HTTP/WS frontend. Un-migrated routes are
/// reverse-proxied to the bridge. The bridge is a sibling of the kernel (not a child), so a
/// kernel crash no longer takes the bridge — and the sessions it serves — down with it.
pub async fn serve(cfg: GatewayConfig) -> Result<(), String> {
    let root = cfg.root.clone();
    let port = cfg.port;
    let conductor_port = cfg.conductor_port;
    let cdp_port = cfg.cdp_port;
    let grok_port = cfg.grok_port;
    let fallback = if cfg.fallback.is_empty() {
        format!("http://127.0.0.1:{}", cfg.legacy_port)
    } else {
        cfg.fallback.clone()
    };
    let python = cfg.kernel_python.clone();
    let kernel_data_dir = cfg.kernel_data_dir.clone();

    let kernel_script = root.join("frontends").join("kernel_server.py");
    let static_dir = root.join("frontends").join("desktop").join("static");

    let mut extra_env: HashMap<String, String> = HashMap::new();
    // The kernel publishes its DuckDB store on this fixed loopback port so the bridge can
    // reconnect to it after a kernel restart. The gateway owns the bridge lifecycle now, so
    // we no longer tell the kernel to spawn it (GA_LEGACY_PORT is ignored by the kernel).
    extra_env.insert("GA_CONFIG_PORT".to_string(), cfg.config_port.to_string());
    extra_env.insert("GA_CONDUCTOR_PORT".to_string(), conductor_port.to_string());
    if let Ok(v) = std::env::var("GA_NO_IM_AUTOSTART") {
        extra_env.insert("GA_NO_IM_AUTOSTART".to_string(), v);
    }

    let data_label = if kernel_data_dir.is_empty() {
        "<real root>".to_string()
    } else {
        kernel_data_dir.clone()
    };
    println!(
        "[gateway] spawning kernel: {} {} (data_dir={})",
        python,
        kernel_script.display(),
        data_label
    );
    let kernel =
        KernelSupervisor::start(&python, &kernel_script, &root, &kernel_data_dir, extra_env)
            .await
            .map_err(|e| format!("failed to spawn kernel subprocess: {}", e))?;

    // Block until the kernel answers: this also guarantees its config server is published
    // on the fixed port, so the bridge spawned next never starts against a dead address
    // (which would make its first reads degrade to the stale legacy json).
    let default_root = root.to_string_lossy().into_owned();
    let ga_root = match kernel.call("status", json!({})).await {
        Ok(v) => v
            .get("gaRoot")
            .and_then(|x| x.as_str())
            .unwrap_or(&default_root)
            .to_string(),
        Err(e) => {
            eprintln!("[gateway] kernel status failed: {} (continuing)", e);
            default_root
        }
    };

    // Spawn the legacy bridge as a sibling of the kernel (owned by the gateway). It is a
    // replica of the store, reading/writing config through the kernel's loopback config
    // server on config_port; it survives a kernel crash and reconnects after a respawn.
    let bridge_script = root.join("frontends").join("desktop_bridge.py");
    let bridge_log = root.join("temp").join("legacy_bridge.log");
    let mut bridge_env: HashMap<String, String> = HashMap::new();
    bridge_env.insert("BRIDGE_PORT".to_string(), cfg.legacy_port.to_string());
    bridge_env.insert("CONDUCTOR_PORT".to_string(), conductor_port.to_string());
    bridge_env.insert("GA_STORAGE_SECONDARY".to_string(), "1".to_string());
    bridge_env.insert("GA_CONFIG_PORT".to_string(), cfg.config_port.to_string());
    bridge_env.insert("GA_NO_IM_AUTOSTART".to_string(), "1".to_string());
    bridge_env.insert("GA_ROOT".to_string(), root.to_string_lossy().into_owned());
    eprintln!(
        "[gateway] spawning bridge: {} {} (bridge_port={}, config_port={})",
        python,
        bridge_script.display(),
        cfg.legacy_port,
        cfg.config_port
    );
    let bridge =
        BridgeSupervisor::start(&python, &bridge_script, &root, &bridge_log, bridge_env)
            .await
            .map_err(|e| format!("failed to spawn bridge subprocess: {}", e))?;

    let client = reqwest::Client::builder()
        // Bound upstream latency so a hung/missing upstream returns 502 instead of hanging the
        // gateway connection (which would surface to the caller as an empty response / curl 000).
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| e.to_string())?;
    let state = AppState {
        kernel,
        bridge,
        fallback: fallback.clone(),
        static_dir,
        bridge_port: port,
        conductor_port,
        conductor_upstream: format!("http://127.0.0.1:{}", conductor_port),
        cdp_upstream: format!("http://127.0.0.1:{}", cdp_port),
        grok_upstream: format!("http://127.0.0.1:{}", grok_port),
        ga_root: ga_root.clone(),
        client,
    };

    let app = build_router(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    println!(
        "[gateway] listening on http://{}:{}  (ga_root={}, fallback={})",
        addr.ip(),
        port,
        ga_root,
        fallback
    );
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| format!("bind {}: {}", port, e))?;
    axum::serve(listener, app)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}
