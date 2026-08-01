//! Supervised sibling of the GA Python kernel: the legacy `desktop_bridge.py`.
//!
//! Unlike the kernel (JSON-RPC over stdio), the bridge is a plain HTTP server that the
//! gateway reverse-proxies un-migrated routes (`/api/*`, `/services/panel`, …) to. It is
//! spawned as a *sibling* of the kernel — not a child of it — so a kernel crash no longer
//! takes the bridge (and the sessions/conversations it serves) down with it. The bridge
//! simply reconnects to the kernel's loopback config server on its fixed port after the
//! kernel respawns, which is why the gateway — not the kernel — owns the bridge's lifecycle.

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::process::{Child, Command};
use tokio::sync::{broadcast, Mutex, RwLock};

/// Maximum supervised respawn attempts before giving up (operator must intervene).
const MAX_ATTEMPTS: u32 = 8;

/// Kill a process left listening on `port` by a previous incarnation of us.
///
/// Mirrors the Python side's `_reap_port`: only ever kills a process whose command line
/// contains `marker` (our own script), so a foreign listener on the same port is left alone.
fn reap_port(port: u16, marker: &str) {
    if port == 0 {
        return;
    }
    let cmd = format!(
        "for p in $(lsof -ti tcp:{} -sTCP:LISTEN 2>/dev/null); do c=$(ps -p $p -o command= 2>/dev/null); case \"$c\" in *\"{}\"*) kill -9 $p 2>/dev/null;; esac; done",
        port, marker
    );
    let _ = std::process::Command::new("sh").arg("-c").arg(cmd).status();
}

/// A live bridge subprocess. Cheap to clone (Arc); the supervisor holds one in `current`.
pub struct BridgeChild {
    pid: u32,
    exit: broadcast::Sender<()>,
    /// Owned by the watcher task; `kill_on_drop` fires when that task ends. We never take
    /// it out of the Mutex here, so `shutdown` kills by pid instead (avoids a lock/wait
    /// deadlock where the watcher holds the child awaiting exit while shutdown waits for
    /// the same lock).
    _child: Mutex<Option<Child>>,
}

impl BridgeChild {
    /// Kill the child by pid (the watcher still owns the `Child` handle). On unix `kill` is
    /// always available; elsewhere we fall back to `start_kill` on the stored handle.
    pub async fn shutdown(&self) {
        #[cfg(unix)]
        {
            let _ = std::process::Command::new("kill")
                .arg("-9")
                .arg(self.pid.to_string())
                .status();
        }
        #[cfg(not(unix))]
        {
            let mut g = self._child.lock().await;
            if let Some(c) = g.as_mut() {
                let _ = c.start_kill();
            }
        }
    }

    /// Fires once when the child process exits.
    pub fn exited(&self) -> broadcast::Receiver<()> {
        self.exit.subscribe()
    }
}

pub struct BridgeSupervisor {
    spec: BridgeSpec,
    current: RwLock<Option<Arc<BridgeChild>>>,
    generation: AtomicU64,
    fails: AtomicU32,
    /// Serialises respawns so a manual restart and the watchdog cannot race.
    restart_lock: Mutex<()>,
}

#[derive(Clone)]
struct BridgeSpec {
    python: String,
    script: PathBuf,
    cwd: PathBuf,
    log_path: PathBuf,
    env: HashMap<String, String>,
}

impl BridgeSupervisor {
    pub async fn start(
        python: &str,
        script: &Path,
        cwd: &Path,
        log_path: &Path,
        env: HashMap<String, String>,
    ) -> std::io::Result<Arc<Self>> {
        let sup = Arc::new(BridgeSupervisor {
            spec: BridgeSpec {
                python: python.to_string(),
                script: script.to_path_buf(),
                cwd: cwd.to_path_buf(),
                log_path: log_path.to_path_buf(),
                env,
            },
            current: RwLock::new(None),
            generation: AtomicU64::new(0),
            fails: AtomicU32::new(0),
            restart_lock: Mutex::new(()),
        });
        sup.clone().spawn_generation().await?;
        Ok(sup)
    }

    /// Boxed rather than a plain `async fn` because spawning and supervising are mutually
    /// recursive (`spawn_generation` -> watchdog -> `on_exit` -> `spawn_generation`), and the
    /// compiler cannot infer `Send` for a self-referential opaque future.
    fn spawn_generation(
        self: Arc<Self>,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>> {
        Box::pin(async move {
            let s = &self.spec;
            // Reap any stale bridge still holding our port (e.g. a previous gateway's
            // bridge that outlived it) before binding.
            let port: u16 = s
                .env
                .get("BRIDGE_PORT")
                .and_then(|p| p.parse().ok())
                .unwrap_or(0);
            reap_port(port, "desktop_bridge.py");

            // Bridge logs go to a file so they don't corrupt the gateway's stdio.
            let logf = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&s.log_path)
                .or_else(|_| {
                    if let Some(parent) = s.log_path.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&s.log_path)
                });
            let (stdout, stderr) = match logf {
                Ok(f) => {
                    let f2 = f.try_clone().ok();
                    (
                        Stdio::from(f),
                        match f2 {
                            Some(f2) => Stdio::from(f2),
                            None => Stdio::null(),
                        },
                    )
                }
                _ => (Stdio::null(), Stdio::null()),
            };

            let mut child = Command::new(&s.python);
            child
                .arg(&s.script)
                .current_dir(&s.cwd)
                .stdin(Stdio::null())
                .stdout(stdout)
                .stderr(stderr);
            for (k, v) in &s.env {
                child.env(k, v);
            }
            let child = child.kill_on_drop(true).spawn()?;
            let pid = child.id().expect("bridge child pid");
            let (exit_tx, _) = broadcast::channel::<()>(4);

            let client = Arc::new(BridgeChild {
                pid,
                exit: exit_tx,
                _child: Mutex::new(Some(child)),
            });

            let gen = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
            *self.current.write().await = Some(client.clone());

            // Watcher: take the Child out of the Mutex and await its exit. Dropping the
            // Child at the end triggers kill_on_drop (a no-op once it has already exited).
            let mut slot = client._child.lock().await;
            let owned = slot.take().expect("spawned bridge child");
            drop(slot);
            let sup = self.clone();
            let bc = client.clone();
            tokio::spawn(async move {
                let mut c = owned;
                let _ = c.wait().await;
                let _ = bc.exit.send(());
                sup.on_exit(gen).await;
            });

            eprintln!(
                "[bridge] spawned on :{} (pid={}, generation {})",
                port, pid, gen
            );
            Ok(())
        })
    }

    async fn on_exit(self: Arc<Self>, gen: u64) {
        // A newer generation is already live (manual restart won the race): nothing to do.
        if self.generation.load(Ordering::SeqCst) != gen {
            return;
        }
        let _guard = self.restart_lock.lock().await;
        if self.generation.load(Ordering::SeqCst) != gen {
            return;
        }
        // Drop the write lock before awaiting the reap: `is_up` takes a read lock, and
        // holding the writer across the await would make callers queue behind the corpse.
        let old = self.current.write().await.take();
        if let Some(c) = old {
            c.shutdown().await;
        }
        eprintln!("[bridge] generation {} exited; supervising restart", gen);
        for _ in 0..MAX_ATTEMPTS {
            let n = self.fails.fetch_add(1, Ordering::SeqCst);
            let delay = Duration::from_secs(1u64 << n.min(5));
            tokio::time::sleep(delay).await;
            match self.clone().spawn_generation().await {
                Ok(()) => {
                    eprintln!(
                        "[bridge] respawned as generation {}",
                        self.generation.load(Ordering::SeqCst)
                    );
                    return;
                }
                _ => eprintln!("[bridge] respawn attempt {} failed", n + 1),
            }
        }
        eprintln!(
            "[bridge] gave up after {} attempts; POST /bridge/restart to retry",
            MAX_ATTEMPTS
        );
    }

    /// Force a fresh bridge now, resetting the backoff. Used by `POST /bridge/restart`.
    pub async fn restart(self: Arc<Self>) -> Result<(), String> {
        let _guard = self.restart_lock.lock().await;
        self.fails.store(0, Ordering::SeqCst);
        let old = self.current.write().await.take();
        if let Some(c) = old {
            c.shutdown().await;
        }
        self.clone()
            .spawn_generation()
            .await
            .map_err(|e| format!("respawn failed: {}", e))
    }

    pub async fn is_up(&self) -> bool {
        self.current.read().await.is_some()
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }
}
