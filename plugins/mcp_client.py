"""MCP Client — 自实现 Model Context Protocol JSON-RPC client（零依赖）。

支持 transport:
  - stdio: 本地子进程，stdin/stdout newline-delimited JSON（最常见）
  - http : Streamable HTTP（POST JSON-RPC，响应 JSON 或 SSE stream）
  - sse  : 旧版 SSE transport（GET /sse 建流 + POST message）

契合 GA agent_loop 同步调用模式：所有方法阻塞返回，对外 fail-open。

生命周期:
  c = McpClient(name, config)   # config 来自 _discover_plugin_mcp
  c.start()                      # 启动并 initialize handshake
  tools = c.list_tools()         # -> [{name, description, inputSchema}]
  res = c.call_tool(name, args) # -> {content:[{type,text,...}], isError}
  c.stop()

所有异常 McpError/TimeoutError 由调用方（生命周期层）捕获后 fail-open。
"""
import os
import json
import time
import shutil
import threading
import queue
import subprocess
import urllib.request
import urllib.error
import urllib.parse

# 打包 app 由 launchd 拉起时 PATH 极简，MCP server 多是 npx/uvx 脚本，找不到解释器会
# 起得来却不握手（表现为硬等超时）。这里统一补齐，与内核 _ensure_exec_path 同源。
_EXTRA_BINS = ("/opt/homebrew/bin", "/opt/homebrew/sbin", "/usr/local/bin",
               "/usr/bin", "/bin", "/usr/sbin", "/sbin")


def _exec_path(base):
    parts = [p for p in base.split(os.pathsep) if p]
    return os.pathsep.join(parts + [p for p in _EXTRA_BINS
                                    if os.path.isdir(p) and p not in parts])


# MCP 协议版本（2025-03-26: 新增 OAuth/Streamable HTTP/Batching/Tool Annotations/Audio）
PROTOCOL_VERSION = "2025-03-26"

CLIENT_INFO = {"name": "GenericAgent", "version": "1.0"}


class McpError(Exception):
    """MCP server 返回的 JSON-RPC error。"""

    def __init__(self, err):
        self.code = err.get("code")
        self.message = err.get("message", "")
        self.data = err.get("data")
        super().__init__("MCP error %s: %s" % (self.code, self.message))


class _SseStream:
    """旧版 SSE transport：后台 GET 长连接，分发 endpoint 与 response。"""

    def __init__(self, url):
        self.url = url
        parts = urllib.parse.urlsplit(url)
        self._origin = parts.scheme + "://" + parts.netloc
        self.endpoint = None
        self._q = queue.Queue()
        self._resp = None
        self._buf = ""
        self._closed = False
        self._thread = threading.Thread(target=self._read, daemon=True)

    def start(self):
        self._resp = urllib.request.urlopen(self.url, timeout=30)
        self._thread.start()
        deadline = time.time() + 10
        while time.time() < deadline and self.endpoint is None:
            time.sleep(0.05)
        if self.endpoint is None:
            raise McpError({"code": -1, "message": "SSE endpoint 未收到"})

    def _read(self):
        try:
            while not self._closed:
                raw = self._resp.readline()
                if not raw:
                    break
                line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                if line == "":
                    if self._buf:
                        self._dispatch(self._buf)
                        self._buf = ""
                else:
                    self._buf += line + "\n"
        except Exception:
            pass

    def _dispatch(self, block):
        event = ""
        data = []
        for line in block.split("\n"):
            if line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].lstrip())
        if not data:
            return
        payload = "\n".join(data)
        if event == "endpoint":
            ep = payload
            if ep.startswith("http://") or ep.startswith("https://"):
                self.endpoint = ep
            else:
                self.endpoint = self._origin + "/" + ep.lstrip("/")
            return
        try:
            msg = json.loads(payload)
        except json.JSONDecodeError:
            return
        self._q.put(msg)

    def get_response(self, rid, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                msg = self._q.get(timeout=0.5)
            except queue.Empty:
                if self._closed:
                    raise McpError({"code": -1, "message": "sse closed"})
                continue
            if msg.get("id") == rid:
                if "error" in msg:
                    raise McpError(msg["error"])
                return msg.get("result")
        raise TimeoutError("MCP sse request id=%s 超时 %ss" % (rid, timeout))

    def close(self):
        self._closed = True
        try:
            if self._resp:
                self._resp.close()
        except Exception:
            pass


class McpClient:
    def __init__(self, name, config):
        self.name = name
        self.config = config or {}
        self.transport = self.config.get("transport", "stdio")
        self.plugin_dir = self.config.get("plugin_dir")
        # 工具调用可能很慢，保留 30s；握手/列举只是本地进程应答，配错的 server 不该拖满 30s
        self.timeout = float(self.config.get("timeout") or 30)
        self.handshake_timeout = float(self.config.get("handshake_timeout") or 8)
        self._id = 0
        self._lock = threading.Lock()        # 保护 stdin 写
        self._req_lock = threading.RLock()   # 保证同一 client 同时只有一个在飞请求
        self._closed = False
        self.server_info = {}
        # stdio
        self._proc = None
        self._reader = None
        self._resp_q = None
        # http
        self._base_url = self.config.get("url")
        self._session_id = None
        # sse
        self._sse = None

    def _next_id(self):
        self._id += 1
        return self._id

    # ---------- 通用生命周期 ----------
    def start(self):
        if self.transport == "stdio":
            self._start_stdio()
        elif self.transport == "http":
            pass  # Streamable HTTP 无需预连接，首次请求建 session
        elif self.transport == "sse":
            self._sse = _SseStream(self._base_url)
            self._sse.start()
        else:
            raise ValueError("未知 transport: %s" % self.transport)
        result = self._request("initialize", {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": CLIENT_INFO,
        }, timeout=self.handshake_timeout)
        if result:
            self.server_info = result.get("serverInfo", {})
        self._notify("notifications/initialized")
        return True

    def list_tools(self):
        result = self._request("tools/list", {}, timeout=self.handshake_timeout)
        return (result or {}).get("tools", [])

    def call_tool(self, name, arguments=None):
        return self._request("tools/call", {"name": name, "arguments": arguments or {}})

    def alive(self):
        if self._closed:
            return False
        return self._proc is None or self._proc.poll() is None   # 非 stdio 无进程可探

    def _stderr_tail(self, limit=300):
        """进程已退出时读残留 stderr，给出可读的失败原因（不阻塞：管道已 EOF）。"""
        try:
            return (self._proc.stderr.read() or b"").decode("utf-8", "replace").strip()[-limit:]
        except Exception:
            return ""

    def stop(self):
        self._closed = True
        try:
            if self.transport == "stdio" and self._proc:
                try:
                    self._proc.stdin.close()
                except Exception:
                    pass
                try:
                    self._proc.terminate()
                except Exception:
                    pass
                try:
                    self._proc.wait(timeout=3)
                except Exception:
                    try:
                        self._proc.kill()
                    except Exception:
                        pass
            elif self.transport == "sse" and self._sse:
                self._sse.close()
        except Exception:
            pass

    def _request(self, method, params, timeout=None):
        timeout = self.timeout if timeout is None else timeout
        with self._req_lock:   # client 跨会话共用，单飞请求保证 id 与响应一一对应
            if self.transport == "stdio":
                return self._stdio_request(method, params, timeout)
            if self.transport == "http":
                return self._http_request(method, params, timeout)
            if self.transport == "sse":
                return self._sse_request(method, params, timeout)
        raise ValueError("未知 transport: %s" % self.transport)

    def _notify(self, method, params=None):
        if self.transport == "stdio":
            self._stdio_notify(method, params)
        elif self.transport == "http":
            self._http_notify(method, params)
        elif self.transport == "sse":
            self._sse_notify(method, params)

    # ---------- stdio ----------
    def _start_stdio(self):
        env = dict(os.environ)
        env["PATH"] = _exec_path(env.get("PATH", ""))
        env.update(self.config.get("env", {}) or {})
        exe = shutil.which(self.config["command"], path=env["PATH"])
        if not exe:
            raise McpError({"code": -1, "message": "命令未找到: %s (PATH=%s)" % (
                self.config["command"], env["PATH"])})
        cmd = [exe] + list(self.config.get("args", []))
        cwd = self.config.get("cwd") or self.plugin_dir
        self._proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=env, cwd=cwd, bufsize=0)
        self._resp_q = queue.Queue()
        self._reader = threading.Thread(target=self._stdio_read_loop, daemon=True)
        self._reader.start()

    def _stdio_read_loop(self):
        try:
            while not self._closed:
                line = self._proc.stdout.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue  # server 日志/非 JSON 行
                self._resp_q.put(msg)
        except Exception:
            pass

    def _stdio_send(self, msg):
        data = (json.dumps(msg) + "\n").encode()
        with self._lock:
            self._proc.stdin.write(data)
            self._proc.stdin.flush()

    def _stdio_request(self, method, params, timeout=30):
        rid = self._next_id()
        req = {"jsonrpc": "2.0", "id": rid, "method": method, "params": params}
        self._stdio_send(req)
        return self._wait_response(rid, timeout)

    def _wait_response(self, rid, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                msg = self._resp_q.get(timeout=0.5)
            except queue.Empty:
                if self._closed:
                    raise McpError({"code": -1, "message": "client closed"})
                if self._proc is not None and self._proc.poll() is not None:
                    raise McpError({"code": -1, "message": "server 退出(code=%s): %s" % (
                        self._proc.returncode, self._stderr_tail())})
                continue
            if msg.get("id") == rid:
                if "error" in msg:
                    raise McpError(msg["error"])
                return msg.get("result")
            # 其他 id 响应或通知：MCP stdio 多为单请求单响应，此处忽略不匹配
        raise TimeoutError("MCP %s request id=%s 超时 %ss" % (self.name, rid, timeout))

    def _stdio_notify(self, method, params=None):
        req = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            req["params"] = params
        self._stdio_send(req)

    # ---------- http (Streamable HTTP) ----------
    def _http_request(self, method, params, timeout=30):
        rid = self._next_id()
        req = {"jsonrpc": "2.0", "id": rid, "method": method, "params": params}
        body = json.dumps(req).encode()
        r = urllib.request.Request(self._base_url, data=body, method="POST")
        r.add_header("Content-Type", "application/json")
        r.add_header("Accept", "application/json, text/event-stream")
        if self._session_id:
            r.add_header("Mcp-Session-Id", self._session_id)
        try:
            resp = urllib.request.urlopen(r, timeout=timeout)
        except urllib.error.HTTPError as e:
            raise McpError({"code": -1, "message": "HTTP %s: %s" % (e.code, e.reason)})
        sid = resp.headers.get("Mcp-Session-Id")
        if sid:
            self._session_id = sid
        ctype = (resp.headers.get("Content-Type") or "").lower()
        raw = resp.read()
        if "text/event-stream" in ctype:
            return self._parse_sse_response(raw, rid)
        msg = json.loads(raw)
        if "error" in msg:
            raise McpError(msg["error"])
        return msg.get("result")

    def _parse_sse_response(self, raw, rid):
        text = raw.decode("utf-8", errors="replace")
        for block in text.split("\n\n"):
            data_lines = []
            for line in block.split("\n"):
                if line.startswith("data:"):
                    data_lines.append(line[5:].lstrip())
            if data_lines:
                try:
                    msg = json.loads("\n".join(data_lines))
                except json.JSONDecodeError:
                    continue
                if msg.get("id") == rid:
                    if "error" in msg:
                        raise McpError(msg["error"])
                    return msg.get("result")
        raise McpError({"code": -1, "message": "SSE stream 无匹配响应 id=%s" % rid})

    def _http_notify(self, method, params=None):
        req = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            req["params"] = params
        body = json.dumps(req).encode()
        r = urllib.request.Request(self._base_url, data=body, method="POST")
        r.add_header("Content-Type", "application/json")
        r.add_header("Accept", "application/json, text/event-stream")
        if self._session_id:
            r.add_header("Mcp-Session-Id", self._session_id)
        try:
            urllib.request.urlopen(r, timeout=10)
        except Exception:
            pass  # notify fail-open

    # ---------- sse (旧版) ----------
    def _sse_request(self, method, params, timeout=30):
        rid = self._next_id()
        req = {"jsonrpc": "2.0", "id": rid, "method": method, "params": params}
        body = json.dumps(req).encode()
        r = urllib.request.Request(self._sse.endpoint, data=body, method="POST")
        r.add_header("Content-Type", "application/json")
        urllib.request.urlopen(r, timeout=timeout)
        return self._sse.get_response(rid, timeout)

    def _sse_notify(self, method, params=None):
        req = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            req["params"] = params
        body = json.dumps(req).encode()
        r = urllib.request.Request(self._sse.endpoint, data=body, method="POST")
        r.add_header("Content-Type", "application/json")
        try:
            urllib.request.urlopen(r, timeout=10)
        except Exception:
            pass
