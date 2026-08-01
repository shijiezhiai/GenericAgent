//! Standalone binary for the GA Desktop Gateway (Phase 1 strangler deployment).
//!
//! In Phase 2 the same gateway is embedded in-process inside the Tauri shell via the
//! `ga_desktop_gateway` library; this binary is the standalone equivalent for running the
//! gateway as its own process (e.g. behind a reverse proxy or for local testing).

use ga_desktop_gateway::{serve, GatewayConfig};
use std::path::PathBuf;

fn parse_port(key: &str, default: u16) -> u16 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() {
    let root = PathBuf::from(
        std::env::var("GA_GATEWAY_ROOT").unwrap_or_else(|_| ".".to_string()),
    );
    let cfg = GatewayConfig {
        root: root.clone(),
        port: parse_port("GA_GATEWAY_PORT", 34168),
        conductor_port: parse_port("GA_CONDUCTOR_PORT", 8900),
        cdp_port: parse_port("GA_CDP_PORT", 18766),
        grok_port: parse_port("GA_GROK_PORT", 15433),
        fallback: std::env::var("GA_FALLBACK_URL").unwrap_or_default(),
        legacy_port: parse_port("GA_LEGACY_PORT", 14169),
        config_port: parse_port("GA_CONFIG_PORT", 14170),
        kernel_python: std::env::var("GA_KERNEL_PYTHON").unwrap_or_else(|_| "python3".to_string()),
        kernel_data_dir: std::env::var("GA_KERNEL_ROOT").unwrap_or_default(),
    };
    if let Err(e) = serve(cfg).await {
        eprintln!("[gateway] fatal: {}", e);
        std::process::exit(1);
    }
}
