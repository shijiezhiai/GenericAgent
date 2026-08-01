//! JSON-RPC 2.0 client over stdio for the GA Python kernel (`frontends/kernel_server.py`).
//!
//! The gateway spawns the kernel as a child process. Communication is newline-delimited
//! JSON on stdout/stdin:
//!   - request  (gateway -> kernel): {"jsonrpc":"2.0","id":N,"method":M,"params":P}
//!   - response (kernel  -> gateway): {"jsonrpc":"2.0","id":N,"result":R} | {"...,"error":{code,message}}
//!   - notify  (kernel  -> gateway): {"jsonrpc":"2.0","method":M,"params":P}  (no id; used for streaming)

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex, RwLock};

pub struct RpcError {
    pub code: i64,
    pub message: String,
}

type PendingMap = Mutex<HashMap<u64, oneshot::Sender<Result<Value, RpcError>>>>;

pub struct KernelClient {
    writer_tx: mpsc::UnboundedSender<String>,
    next_id: Mutex<u64>,
    pending: PendingMap,
    notify: broadcast::Sender<Value>,
    exit: broadcast::Sender<()>,
    // dropping the client kills the child (kill_on_drop); `shutdown` reaps it explicitly.
    child: Mutex<Child>,
}

impl KernelClient {
    pub async fn spawn(
        python: &str,
        script: &Path,
        cwd: &Path,
        kernel_data_dir: &str,
        extra_env: HashMap<String, String>,
    ) -> std::io::Result<Arc<Self>> {
        let mut cmd = Command::new(python);
        cmd.arg(script).current_dir(cwd);
        if !kernel_data_dir.is_empty() {
            cmd.env("GA_KERNEL_ROOT", kernel_data_dir);
        }
        for (k, v) in &extra_env {
            cmd.env(k, v);
        }
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;

        let mut stdin = child.stdin.take().expect("child stdin");
        let stdout = child.stdout.take().expect("child stdout");

        let (writer_tx, mut writer_rx) = mpsc::unbounded_channel::<String>();
        tokio::spawn(async move {
            while let Some(line) = writer_rx.recv().await {
                if stdin.write_all(line.as_bytes()).await.is_err() {
                    break;
                }
                if stdin.write_all(b"\n").await.is_err() {
                    break;
                }
                let _ = stdin.flush().await;
            }
        });

        let (notify_tx, _) = broadcast::channel::<Value>(512);
        let (exit_tx, _) = broadcast::channel::<()>(4);
        let pending: PendingMap = Mutex::new(HashMap::new());
        let client = Arc::new(KernelClient {
            writer_tx,
            next_id: Mutex::new(1),
            pending,
            notify: notify_tx,
            exit: exit_tx,
            child: Mutex::new(child),
        });

        let client2 = client.clone();
        let mut reader = BufReader::new(stdout).lines();
        tokio::spawn(async move {
            while let Ok(Some(line)) = reader.next_line().await {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let v: Value = match serde_json::from_str(line) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("[kernel] bad json ({}): {}", e, &line[..line.len().min(140)]);
                        continue;
                    }
                };
                if v.get("id").is_some() {
                    // response
                    let id = v["id"].as_u64().unwrap_or(0);
                    let mut map = client2.pending.lock().await;
                    if let Some(tx) = map.remove(&id) {
                        drop(map);
                        let result = if let Some(err) = v.get("error") {
                            let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(-32000);
                            let msg = err
                                .get("message")
                                .and_then(|m| m.as_str())
                                .unwrap_or("kernel error")
                                .to_string();
                            Err(RpcError { code, message: msg })
                        } else {
                            Ok(v.get("result").cloned().unwrap_or(Value::Null))
                        };
                        let _ = tx.send(result);
                    }
                } else if v.get("method").is_some() {
                    // notification (streaming)
                    let _ = client2.notify.send(v);
                }
            }
            // The kernel is gone. Fail every in-flight call now: otherwise each caller blocks
            // until its oneshot sender happens to be dropped, which only occurs once the last
            // Arc<KernelClient> goes away.
            let mut map = client2.pending.lock().await;
            for (_, tx) in map.drain() {
                let _ = tx.send(Err(RpcError {
                    code: -32001,
                    message: "kernel process exited".to_string(),
                }));
            }
            drop(map);
            let _ = client2.exit.send(());
            eprintln!("[kernel] stdout closed; kernel process exited");
        });

        Ok(client)
    }

    /// Fires once when the kernel's stdout closes (i.e. the process is gone).
    pub fn exited(&self) -> broadcast::Receiver<()> {
        self.exit.subscribe()
    }

    /// Kill the child and reap it. Required before a respawn: the successor cannot take the
    /// DuckDB lock or bind the legacy-bridge port while the corpse is still around.
    pub async fn shutdown(&self) {
        let mut child = self.child.lock().await;
        let _ = child.start_kill();
        let _ = child.wait().await;
    }

    pub async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let id = {
            let mut n = self.next_id.lock().await;
            *n += 1;
            *n
        };
        let (tx, rx) = oneshot::channel();
        {
            let mut map = self.pending.lock().await;
            map.insert(id, tx);
        }
        let req = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        let line = serde_json::to_string(&req).map_err(|e| e.to_string())?;
        if self.writer_tx.send(line).is_err() {
            return Err("kernel process is gone".to_string());
        }
        match rx.await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(format!("kernel [{}]: {}", e.code, e.message)),
            Err(_) => Err("kernel dropped the response channel".to_string()),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.notify.subscribe()
    }
}

// ---------------------------------------------------------------------------
// Supervisor
// ---------------------------------------------------------------------------

/// A kernel that survives its own death was previously impossible: the gateway spawned the
/// child exactly once and merely logged the exit. Because the legacy bridge is a *child of the
/// kernel* and is killed with it, one kernel exit took down every `/api/*` route permanently
/// (endless 502s, UI stuck on "loading") until the user restarted the whole app.
///
/// `KernelSupervisor` owns the current generation, respawns it with backoff, and re-publishes
/// every generation's notifications onto one stable broadcast channel — so live WebSocket
/// clients keep working across a swap instead of silently attaching to a dead sender.
pub struct KernelSupervisor {
    spec: SpawnSpec,
    current: RwLock<Option<Arc<KernelClient>>>,
    notify: broadcast::Sender<Value>,
    generation: AtomicU64,
    fails: AtomicU32,
    /// Serialises respawns so a manual restart and the watchdog cannot race.
    restart_lock: Mutex<()>,
}

#[derive(Clone)]
struct SpawnSpec {
    python: String,
    script: PathBuf,
    cwd: PathBuf,
    data_dir: String,
    env: HashMap<String, String>,
}

/// A kernel that stayed up this long counts as healthy: its next exit restarts the backoff
/// from zero instead of inheriting the previous crash streak.
const HEALTHY_UPTIME: Duration = Duration::from_secs(60);
const MAX_ATTEMPTS: u32 = 8;

impl KernelSupervisor {
    pub async fn start(
        python: &str,
        script: &Path,
        cwd: &Path,
        data_dir: &str,
        env: HashMap<String, String>,
    ) -> std::io::Result<Arc<Self>> {
        let (notify, _) = broadcast::channel::<Value>(512);
        let sup = Arc::new(KernelSupervisor {
            spec: SpawnSpec {
                python: python.to_string(),
                script: script.to_path_buf(),
                cwd: cwd.to_path_buf(),
                data_dir: data_dir.to_string(),
                env,
            },
            current: RwLock::new(None),
            notify,
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
            let client =
                KernelClient::spawn(&s.python, &s.script, &s.cwd, &s.data_dir, s.env.clone())
                    .await?;
            let gen = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
            *self.current.write().await = Some(client.clone());

            // Forward this generation's notifications onto the supervisor's stable channel; the
            // task ends by itself when the client's sender is dropped.
            let out = self.notify.clone();
            let mut rx = client.subscribe();
            tokio::spawn(async move {
                while let Ok(v) = rx.recv().await {
                    let _ = out.send(v);
                }
            });

            let sup = self.clone();
            let mut exited = client.exited();
            tokio::spawn(async move {
                let started = Instant::now();
                let _ = exited.recv().await;
                sup.on_exit(gen, started.elapsed()).await;
            });
            Ok(())
        })
    }

    async fn on_exit(self: Arc<Self>, gen: u64, uptime: Duration) {
        // A newer generation is already live (manual restart won the race): nothing to do.
        if self.generation.load(Ordering::SeqCst) != gen {
            return;
        }
        let _guard = self.restart_lock.lock().await;
        if self.generation.load(Ordering::SeqCst) != gen {
            return;
        }
        // Drop the write lock before awaiting the reap: `call` takes a read lock, and holding
        // the writer across the await would make callers queue behind the corpse instead of
        // failing fast with "kernel is restarting".
        let old = self.current.write().await.take();
        if let Some(c) = old {
            c.shutdown().await;
        }
        if uptime >= HEALTHY_UPTIME {
            self.fails.store(0, Ordering::SeqCst);
        }
        eprintln!(
            "[kernel] generation {} exited after {:?}; supervising restart",
            gen, uptime
        );
        for _ in 0..MAX_ATTEMPTS {
            let n = self.fails.fetch_add(1, Ordering::SeqCst);
            let delay = Duration::from_secs(1u64 << n.min(5));
            tokio::time::sleep(delay).await;
            match self.clone().spawn_generation().await {
                Ok(()) => {
                    eprintln!(
                        "[kernel] respawned as generation {}",
                        self.generation.load(Ordering::SeqCst)
                    );
                    return;
                }
                Err(e) => eprintln!("[kernel] respawn attempt {} failed: {}", n + 1, e),
            }
        }
        eprintln!(
            "[kernel] gave up after {} attempts; POST /kernel/restart to retry",
            MAX_ATTEMPTS
        );
    }

    /// Force a fresh kernel now, resetting the backoff. Used by `POST /kernel/restart`.
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

    pub async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let client = self.current.read().await.clone();
        match client {
            Some(c) => c.call(method, params).await,
            None => Err("kernel is restarting".to_string()),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.notify.subscribe()
    }

    pub async fn is_up(&self) -> bool {
        self.current.read().await.is_some()
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }
}
