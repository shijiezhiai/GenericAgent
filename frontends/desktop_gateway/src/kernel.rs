//! JSON-RPC 2.0 client over stdio for the GA Python kernel (`frontends/kernel_server.py`).
//!
//! The gateway spawns the kernel as a child process. Communication is newline-delimited
//! JSON on stdout/stdin:
//!   - request  (gateway -> kernel): {"jsonrpc":"2.0","id":N,"method":M,"params":P}
//!   - response (kernel  -> gateway): {"jsonrpc":"2.0","id":N,"result":R} | {"...,"error":{code,message}}
//!   - notify  (kernel  -> gateway): {"jsonrpc":"2.0","method":M,"params":P}  (no id; used for streaming)

use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};

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
    // kept only so the child is killed when the client (and thus this field) is dropped
    _child: Mutex<Child>,
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
        let pending: PendingMap = Mutex::new(HashMap::new());
        let client = Arc::new(KernelClient {
            writer_tx,
            next_id: Mutex::new(1),
            pending,
            notify: notify_tx,
            _child: Mutex::new(child),
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
            eprintln!("[kernel] stdout closed; kernel process exited");
        });

        Ok(client)
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
