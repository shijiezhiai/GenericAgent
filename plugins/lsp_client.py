"""LSP Client — Language Server Protocol JSON-RPC client（零依赖）。

实现 plan 4.3：为 GA 提供 LSP 集成能力。

Transport: stdio（Content-Length 帧协议，LSP 标准）
功能:
  - initialize / initialized 握手
  - didOpen / didChange / didSave / didClose 文档同步
  - publishDiagnostics 异步收集（后台 reader 线程）
  - 自动重启（maxRestarts）
  - 诊断格式化注入 agent context

生命周期:
  c = LspClient(name, config)
  c.start()          # 启动进程 + initialize 握手
  c.did_open(path, text)
  c.did_change(path, text)
  c.get_diagnostics(path)  # -> [diag_dict, ...]
  c.format_diagnostics()   # -> str (注入 context)
  c.stop()

配置 (.lsp.json):
  {
    "command": "pylsp",
    "args": [],
    "env": {},
    "extensionToLanguage": {".py": "python"},
    "workspaceFolder": "/path/to/workspace",
    "initializationOptions": {},
    "settings": {},
    "startupTimeout": 30,
    "maxRestarts": 3,
    "diagnostics": true
  }
"""
import os
import json
import time
import threading
import subprocess
import sys

LSP_DEFAULT_TIMEOUT = 30
LSP_DEFAULT_STARTUP_TIMEOUT = 30
LSP_DEFAULT_MAX_RESTARTS = 3


class LspError(Exception):
    """LSP server 返回的 JSON-RPC error。"""

    def __init__(self, err):
        self.code = err.get("code")
        self.message = err.get("message", "")
        self.data = err.get("data")
        super().__init__("LSP error %s: %s" % (self.code, self.message))


class LspClient:
    """Language Server Protocol client over stdio (Content-Length framing)."""

    def __init__(self, name, config):
        self.name = name
        self.config = config or {}
        self._id = 0
        self._lock = threading.Lock()
        self._closed = False
        self._proc = None
        self._reader_thread = None
        self._resp_q = None
        self._notifications = []  # 收集 server→client 通知
        self._diag_lock = threading.Lock()
        self._diagnostics = {}  # {uri: [diag, ...]}
        self._open_docs = {}  # {uri: version}
        self._restarts = 0
        self._max_restarts = self.config.get("maxRestarts", LSP_DEFAULT_MAX_RESTARTS)
        self._startup_timeout = self.config.get("startupTimeout", LSP_DEFAULT_STARTUP_TIMEOUT)
        self._diag_enabled = self.config.get("diagnostics", True)
        self.server_info = {}
        self._workspace = self.config.get("workspaceFolder") or os.getcwd()

    def _next_id(self):
        self._id += 1
        return self._id

    # ---------- 生命周期 ----------
    def start(self):
        """启动 LSP server 进程并完成 initialize 握手。"""
        self._start_process()
        result = self._initialize()
        if result:
            self.server_info = result.get("serverInfo", {})
        self._notify("initialized", {})
        return True

    def _start_process(self):
        cmd = [self.config["command"]] + list(self.config.get("args", []))
        env = dict(os.environ)
        env.update(self.config.get("env", {}) or {})
        cwd = self._workspace
        import queue
        self._resp_q = queue.Queue()
        self._proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=env, cwd=cwd, bufsize=0)
        self._reader_thread = threading.Thread(target=self._read_loop, daemon=True)
        self._reader_thread.start()

    def _initialize(self):
        """发送 initialize 请求。"""
        params = {
            "processId": os.getpid(),
            "capabilities": {
                "textDocument": {
                    "synchronization": {"didSave": True},
                    "publishDiagnostics": {"relatedInformation": True},
                },
                "workspace": {"workspaceFolders": True},
            },
            "rootUri": self._path_to_uri(self._workspace),
            "workspaceFolders": [{"uri": self._path_to_uri(self._workspace), "name": os.path.basename(self._workspace)}],
        }
        init_opts = self.config.get("initializationOptions")
        if init_opts:
            params["initializationOptions"] = init_opts
        return self._request("initialize", params, timeout=self._startup_timeout)

    def stop(self):
        """关闭 LSP server（先发 shutdown 再 exit）。"""
        self._closed = True
        try:
            if self._proc and self._proc.poll() is None:
                try:
                    self._request("shutdown", None, timeout=5)
                except Exception:
                    pass
                try:
                    self._notify("exit", None)
                except Exception:
                    pass
                try:
                    self._proc.wait(timeout=3)
                except Exception:
                    try:
                        self._proc.terminate()
                        self._proc.wait(timeout=2)
                    except Exception:
                        try:
                            self._proc.kill()
                        except Exception:
                            pass
        except Exception:
            pass

    def _try_restart(self):
        """尝试重启 LSP server（maxRestarts 限制）。"""
        if self._closed or self._restarts >= self._max_restarts:
            return False
        self._restarts += 1
        print(f"[LSP:{self.name}] restarting ({self._restarts}/{self._max_restarts})", file=sys.stderr)
        try:
            self._proc.kill()
        except Exception:
            pass
        try:
            self._start_process()
            self._initialize()
            self._notify("initialized", {})
            # 重新打开之前打开的文档
            for uri, ver in list(self._open_docs.items()):
                path = self._uri_to_path(uri)
                try:
                    with open(path, "r", encoding="utf-8", errors="replace") as f:
                        text = f.read()
                    self._notify("textDocument/didOpen", {
                        "textDocument": {"uri": uri, "languageId": self._lang_id(path), "version": ver, "text": text}
                    })
                except Exception:
                    pass
            return True
        except Exception as e:
            print(f"[LSP:{self.name}] restart failed: {e}", file=sys.stderr)
            return False

    # ---------- 文档同步 ----------
    def did_open(self, path, text=None):
        """通知 LSP 打开文档。"""
        uri = self._path_to_uri(path)
        if text is None:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                text = f.read()
        self._open_docs[uri] = 1
        self._notify("textDocument/didOpen", {
            "textDocument": {"uri": uri, "languageId": self._lang_id(path), "version": 1, "text": text}
        })

    def did_change(self, path, text=None):
        """通知 LSP 文档内容变更（全量同步）。"""
        uri = self._path_to_uri(path)
        if text is None:
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as f:
                    text = f.read()
            except FileNotFoundError:
                return
        ver = self._open_docs.get(uri, 0) + 1
        self._open_docs[uri] = ver
        if uri not in self._open_docs or self._open_docs.get(uri, 0) <= 1:
            # 未打开过，先 didOpen
            self.did_open(path, text)
            return
        self._notify("textDocument/didChange", {
            "textDocument": {"uri": uri, "version": ver},
            "contentChanges": [{"text": text}]
        })

    def did_save(self, path, text=None):
        """通知 LSP 文档已保存。"""
        uri = self._path_to_uri(path)
        params = {"textDocument": {"uri": uri}}
        if text is not None:
            params["text"] = text
        self._notify("textDocument/didSave", params)

    def did_close(self, path):
        """通知 LSP 关闭文档。"""
        uri = self._path_to_uri(path)
        self._open_docs.pop(uri, None)
        self._notify("textDocument/didClose", {"textDocument": {"uri": uri}})

    def notify_file_changed(self, path):
        """外部编辑事件入口：自动判断 didOpen/didChange。"""
        if not self._diag_enabled:
            return
        uri = self._path_to_uri(path)
        if uri in self._open_docs:
            self.did_change(path)
        else:
            self.did_open(path)

    # ---------- 诊断 ----------
    def get_diagnostics(self, path=None):
        """获取指定文件或全部诊断。"""
        with self._diag_lock:
            if path:
                uri = self._path_to_uri(path)
                return list(self._diagnostics.get(uri, []))
            return dict(self._diagnostics)

    def format_diagnostics(self, max_items=20):
        """格式化诊断为可注入 context 的文本。无诊断返回空串。"""
        with self._diag_lock:
            all_diags = {k: v for k, v in self._diagnostics.items() if v}
        if not all_diags:
            return ""
        lines = ["[LSP Diagnostics]"]
        count = 0
        for uri, diags in all_diags.items():
            path = self._uri_to_path(uri)
            rel = os.path.relpath(path, self._workspace) if path.startswith(self._workspace) else path
            for d in diags:
                if count >= max_items:
                    lines.append(f"  ... and more ({sum(len(v) for v in all_diags.values()) - max_items} remaining)")
                    return "\n".join(lines)
                rng = d.get("range", {})
                start = rng.get("start", {})
                ln = start.get("line", 0) + 1
                sev = {1: "ERROR", 2: "WARN", 3: "INFO", 4: "HINT"}.get(d.get("severity", 1), "?")
                msg = d.get("message", "")
                src = d.get("source", "")
                prefix = f"{src}: " if src else ""
                lines.append(f"  {rel}:{ln} [{sev}] {prefix}{msg}")
                count += 1
        return "\n".join(lines)

    def clear_diagnostics(self, path=None):
        """清除诊断缓存。"""
        with self._diag_lock:
            if path:
                self._diagnostics.pop(self._path_to_uri(path), None)
            else:
                self._diagnostics.clear()

    # ---------- JSON-RPC 通信（Content-Length 帧） ----------
    def _read_loop(self):
        """后台读取 LSP server stdout（Content-Length 帧协议）。"""
        import queue
        try:
            while not self._closed:
                # 读 headers
                headers = {}
                while True:
                    line = self._proc.stdout.readline()
                    if not line:
                        # EOF - server 退出
                        if not self._closed:
                            self._try_restart()
                        return
                    line = line.decode("utf-8", errors="replace").rstrip("\r\n")
                    if line == "":
                        break
                    if ":" in line:
                        k, v = line.split(":", 1)
                        headers[k.strip().lower()] = v.strip()
                content_length = int(headers.get("content-length", 0))
                if content_length <= 0:
                    continue
                body = self._proc.stdout.read(content_length)
                if not body:
                    break
                try:
                    msg = json.loads(body.decode("utf-8"))
                except json.JSONDecodeError:
                    continue
                self._handle_message(msg)
        except Exception:
            if not self._closed:
                self._try_restart()

    def _handle_message(self, msg):
        """分发 server 消息：response → resp_q，notification → 处理。"""
        if "id" in msg and ("result" in msg or "error" in msg):
            # response
            self._resp_q.put(msg)
        elif "method" in msg:
            # notification / server request
            method = msg["method"]
            params = msg.get("params", {})
            if method == "textDocument/publishDiagnostics":
                self._on_publish_diagnostics(params)
            elif method == "window/logMessage":
                pass  # 忽略 server 日志
            # 其他通知存入队列供调试
            self._notifications.append(msg)
            # 限制队列大小
            if len(self._notifications) > 200:
                self._notifications = self._notifications[-100:]

    def _on_publish_diagnostics(self, params):
        uri = params.get("uri", "")
        diags = params.get("diagnostics", [])
        with self._diag_lock:
            if diags:
                self._diagnostics[uri] = diags
            else:
                self._diagnostics.pop(uri, None)

    def _send(self, msg):
        """发送 JSON-RPC 消息（Content-Length 帧）。"""
        body = json.dumps(msg).encode("utf-8")
        header = f"Content-Length: {len(body)}\r\n\r\n".encode("utf-8")
        with self._lock:
            if self._proc and self._proc.stdin:
                self._proc.stdin.write(header + body)
                self._proc.stdin.flush()

    def _request(self, method, params, timeout=LSP_DEFAULT_TIMEOUT):
        """发送请求并等待响应。"""
        if self._closed or not self._proc or self._proc.poll() is not None:
            raise LspError({"code": -1, "message": "LSP client not running"})
        rid = self._next_id()
        msg = {"jsonrpc": "2.0", "id": rid, "method": method}
        if params is not None:
            msg["params"] = params
        self._send(msg)
        return self._wait_response(rid, timeout)

    def _notify(self, method, params=None):
        """发送通知（无需响应）。"""
        if self._closed or not self._proc or self._proc.poll() is not None:
            return
        msg = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        try:
            self._send(msg)
        except Exception:
            pass

    def _wait_response(self, rid, timeout):
        import queue as _q
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                msg = self._resp_q.get(timeout=0.5)
            except _q.Empty:
                if self._closed:
                    raise LspError({"code": -1, "message": "client closed"})
                continue
            if msg.get("id") == rid:
                if "error" in msg:
                    raise LspError(msg["error"])
                return msg.get("result")
        raise TimeoutError("LSP %s request id=%s 超时 %ss" % (self.name, rid, timeout))

    # ---------- 工具方法 ----------
    @staticmethod
    def _path_to_uri(path):
        path = os.path.abspath(path)
        return "file://" + path.replace("\\", "/")

    @staticmethod
    def _uri_to_path(uri):
        if uri.startswith("file://"):
            return uri[7:].replace("%20", " ")
        return uri

    def _lang_id(self, path):
        """根据扩展名映射 languageId。"""
        ext_map = self.config.get("extensionToLanguage", {})
        ext = os.path.splitext(path)[1].lower()
        if ext in ext_map:
            return ext_map[ext]
        # 默认映射
        default_map = {
            ".py": "python", ".js": "javascript", ".ts": "typescript",
            ".jsx": "javascriptreact", ".tsx": "typescriptreact",
            ".java": "java", ".go": "go", ".rs": "rust",
            ".c": "c", ".cpp": "cpp", ".h": "c", ".hpp": "cpp",
            ".rb": "ruby", ".php": "php", ".swift": "swift",
            ".kt": "kotlin", ".scala": "scala", ".sh": "shellscript",
            ".json": "json", ".yaml": "yaml", ".yml": "yaml",
            ".md": "markdown", ".html": "html", ".css": "css",
        }
        return default_map.get(ext, "plaintext")

    def handles_extension(self, path):
        """判断此 LSP server 是否处理该文件类型。"""
        ext_map = self.config.get("extensionToLanguage", {})
        ext = os.path.splitext(path)[1].lower()
        if ext_map:
            return ext in ext_map
        # 无显式配置时，用默认映射判断
        return ext in {".py", ".js", ".ts", ".jsx", ".tsx", ".java", ".go", ".rs",
                       ".c", ".cpp", ".rb", ".php", ".swift", ".kt", ".scala"}
