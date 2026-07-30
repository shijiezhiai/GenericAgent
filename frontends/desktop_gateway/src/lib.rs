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
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;

use kernel::KernelClient;

#[derive(Clone)]
pub struct AppState {
    pub kernel: Arc<KernelClient>,
    pub fallback: String,
    pub static_dir: PathBuf,
    pub bridge_port: u16,
    pub conductor_port: u16,
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

async fn kernel_json(state: &AppState, method: &str, params: Value) -> Response {
    match state.kernel.call(method, params).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

async fn status_handler(State(s): State<AppState>) -> Response {
    kernel_json(&s, "status", json!({})).await
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
        // everything else -> legacy bridge (strangler fallback)
        .fallback(any(proxy::proxy_handler))
        .layer(CorsLayer::permissive())
        .with_state(state)
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
    /// Explicit fallback URL for un-migrated routes. When empty, the gateway proxies to
    /// `http://127.0.0.1:<legacy_port>` (the kernel-spawned legacy bridge).
    pub fallback: String,
    /// Port the kernel spawns the legacy `desktop_bridge.py` on for the strangler fallback.
    pub legacy_port: u16,
    /// Python interpreter used to run the kernel.
    pub kernel_python: String,
    /// Isolation dir for the kernel. Empty = kernel runs against the real project root (so it
    /// shares session data with the legacy fallback bridge).
    pub kernel_data_dir: String,
}

/// Run the gateway until the TCP listener closes.
///
/// Spawns the Python kernel (JSON-RPC over stdio) and serves the axum HTTP/WS frontend.
/// Un-migrated routes are reverse-proxied to the legacy bridge, which the kernel spawns
/// on `legacy_port` — so from the caller's perspective the kernel is the only Python subprocess.
pub async fn serve(cfg: GatewayConfig) -> Result<(), String> {
    let root = cfg.root.clone();
    let port = cfg.port;
    let conductor_port = cfg.conductor_port;
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
    extra_env.insert("GA_LEGACY_PORT".to_string(), cfg.legacy_port.to_string());
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
    let kernel = KernelClient::spawn(&python, &kernel_script, &root, &kernel_data_dir, extra_env)
        .await
        .map_err(|e| format!("failed to spawn kernel subprocess: {}", e))?;

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

    let client = reqwest::Client::builder()
        .build()
        .map_err(|e| e.to_string())?;
    let state = AppState {
        kernel,
        fallback: fallback.clone(),
        static_dir,
        bridge_port: port,
        conductor_port,
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
