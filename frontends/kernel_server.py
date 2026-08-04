#!/usr/bin/env python3
"""GA desktop kernel — JSON-RPC 2.0 over stdio.

Phase 0 of the Rust-gateway plan: extract the "business logic" of desktop_bridge.py
into a standalone, protocol-addressable kernel WITHOUT touching desktop_bridge.py.

Design:
- Import `desktop_bridge` and reuse its `AgentManager` (`manager`) unchanged.
- Redirect `desktop_bridge.hub.emit` (used by `emit_session_state`) to stdio JSON-RPC
  `session.stream` notifications, so the streaming path survives the transport swap.
- Serve a JSON-RPC 2.0 request/response loop over stdin/stdout (newline-delimited).

This makes the gateway<->kernel boundary explicit and lets a future Rust/axum gateway
(or the Tauri shell) drive the kernel over stdio, while the legacy HTTP `desktop_bridge.py`
keeps working byte-for-byte.
"""
import atexit
import faulthandler
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

# 诊断用：`kill -USR1 <kernel pid>` 把所有线程栈 dump 到 /tmp/ga_kernel_fault.log（排查卡死）。
# 每次重开文件，避免 fd 被内核 stdout(JSON-RPC 管道) 重定向干扰。
_FAULT_PATH = "/tmp/ga_kernel_fault.log"


def _dump_threads(signum, frame):
    with open(_FAULT_PATH, "a") as f:
        f.write("\n===== SIGUSR1 dump @ %s pid=%s =====\n" % (time.strftime("%H:%M:%S"), os.getpid()))
        faulthandler.dump_traceback(file=f, all_threads=True)


faulthandler.enable()
signal.signal(signal.SIGUSR1, _dump_threads)

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))


def _ensure_exec_path():
    """打包 app 由 launchd 拉起，PATH 极简（无 /opt/homebrew/bin）。
    子进程（rg / npx / git / MCP server）据此解析可执行文件，缺了会 FileNotFoundError
    或起得来却依赖断裂而卡死握手。启动最早期补齐，所有 subprocess 自动继承。"""
    cur = os.environ.get("PATH", "").split(os.pathsep)
    extra = [p for p in ("/opt/homebrew/bin", "/opt/homebrew/sbin", "/usr/local/bin",
                         "/usr/bin", "/bin", "/usr/sbin", "/sbin",
                         os.path.expanduser("~/.local/bin"))
             if os.path.isdir(p) and p not in cur]
    if extra:
        os.environ["PATH"] = os.pathsep.join(extra + [p for p in cur if p])


_ensure_exec_path()

# The GA core modules (agentmain, llmcore, ga, ...) live at the repo root. In the real
# deployment ga_root IS the repo root (so ensure_ga_import_path() puts it on sys.path),
# but when the kernel is rooted elsewhere (e.g. GA_KERNEL_ROOT isolation), we still need
# the core importable. Unconditionally put the repo root on the path.
REPO_ROOT = HERE.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import desktop_bridge as bridge  # noqa: E402

manager = bridge.manager


class RpcError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


# --------------------------------------------------------------------------
# stdout writer (single lock so notifications & responses never interleave)
# --------------------------------------------------------------------------
_write_lock = threading.Lock()


def _write(obj):
    with _write_lock:
        sys.stdout.write(json.dumps(obj, ensure_ascii=False, default=str) + "\n")
        sys.stdout.flush()


def _notify(method, params):
    _write({"jsonrpc": "2.0", "method": method, "params": params})


# Redirect the bridge's WS emit hook to a stdio notification. `emit_session_state`
# pushes {"type","sessionId","state","status","seq","updatedAt","title"}; we mirror
# it as a `session.stream` notification and attach the live `partial` preview so a
# gateway can render incremental tokens without polling.
def _emit_hook(payload: dict):
    sid = payload.get("sessionId")
    sess = manager.sessions.get(sid) if sid else None
    partial = None
    if sess is not None and getattr(sess, "partial", None) is not None:
        partial = sess.partial
    _notify("session.stream", {
        "sid": sid,
        "state": payload.get("state"),
        "status": payload.get("status"),
        "seq": payload.get("seq"),
        "updatedAt": payload.get("updatedAt"),
        "title": payload.get("title"),
        "partial": partial,
    })


bridge.hub.emit = _emit_hook


# --------------------------------------------------------------------------
# Legacy bridge fallback (strangler)
# --------------------------------------------------------------------------
# Phase 2: the gateway embeds the kernel and, when GA_LEGACY_PORT is set, the
# kernel spawns the original desktop_bridge.py on that port so un-migrated
# routes still work. This makes the kernel the only Python subprocess Tauri
# manages directly (the legacy bridge is a kernel-managed grandchild). Once
# every route is migrated (plan 2.1) this fallback is removed entirely.
legacy_child = None


def _maybe_spawn_legacy_bridge():
    """Deprecated: the legacy bridge is now spawned by the gateway (sibling of the
    kernel) so it survives a kernel crash. This is a no-op; the gateway owns its
    lifecycle. Kept only so an older gateway that still passes GA_LEGACY_PORT does
    not spawn a stray grandchild."""
    port = int(os.environ.get("GA_LEGACY_PORT", "0") or "0")
    if port:
        print(f"[kernel] GA_LEGACY_PORT={port} ignored: bridge is owned by the gateway now",
              file=sys.stderr)
    return None


def _cleanup_legacy():
    """No-op: the gateway owns the bridge lifecycle now, the kernel does not."""
    return None


def _reap_port(port):
    """Kill a process holding `port` that is not us (e.g. a stale config server
    left by a kernel that was SIGKILLed before its atexit ran). Only ever kills a
    process whose command line references our own repo, so we never touch strangers."""
    if os.name == "nt":
        return
    try:
        out = subprocess.run(["lsof", "-ti", f"tcp:{port}", "-sTCP:LISTEN"],
                             capture_output=True, text=True, timeout=5).stdout
    except Exception:
        return
    for pid in [p for p in out.split() if p.isdigit()]:
        if int(pid) == os.getpid():
            continue
        try:
            cmd = subprocess.run(["ps", "-p", pid, "-o", "command="],
                                 capture_output=True, text=True, timeout=5).stdout
        except Exception:
            continue
        if "frontends" not in cmd and "GenericAnt" not in cmd:
            print(f"[kernel] :{port} held by pid {pid} ({cmd.strip()[:80]}); not ours, leaving it",
                  file=sys.stderr)
            continue
        try:
            os.kill(int(pid), signal.SIGKILL)
            print(f"[kernel] reaped stale pid {pid} on :{port}", file=sys.stderr)
        except Exception as e:
            print(f"[kernel] failed to reap pid {pid}: {e}", file=sys.stderr)
    time.sleep(0.3)


def _start_bridge_server():
    """Host the (former legacy) bridge HTTP API + merged conductor inside this process.

    G4 retired the standalone desktop_bridge.py subprocess: the gateway no longer spawns
    it, so the kernel serves the fallback HTTP API (:BRIDGE_PORT, default 14169) and the
    conductor API (:CONDUCTOR_PORT, default 8900) itself via desktop_bridge._serve_dual.
    Handlers call the in-process manager/DBStore directly -- no replica, no gateway
    round-trip. Runs in a daemon thread with a restart loop; a crashed site is rebound.
    """
    bridge_port = int(os.environ.get("BRIDGE_PORT", "14169"))
    # the gateway passes CONDUCTOR_PORT (legacy env name GA_CONDUCTOR_PORT also honored)
    conductor_port = int(os.environ.get("CONDUCTOR_PORT",
                                        os.environ.get("GA_CONDUCTOR_PORT", "8900")))
    for port in (bridge_port, conductor_port):
        _reap_port(port)

    def run():
        import asyncio
        import desktop_bridge
        import conductor_core
        # hand the DB-backed kv to conductor history (in-process owner)
        try:
            conductor_core.attach_store(getattr(manager, "store", None))
        except Exception as e:  # noqa: BLE001
            print(f"[kernel] conductor store attach skipped: {e}", file=sys.stderr)
        while True:
            try:
                asyncio.run(desktop_bridge._serve_dual("127.0.0.1", bridge_port, conductor_port))
            except asyncio.CancelledError:
                return
            except Exception as e:  # noqa: BLE001 - keep the bridge alive
                print(f"[kernel] bridge server died: {e}; rebinding in 2s", file=sys.stderr)
            time.sleep(2)

    threading.Thread(target=run, name="kernel-bridge", daemon=True).start()
    print(f"[kernel] bridge API on :{bridge_port}, conductor on :{conductor_port} (in-process)",
          file=sys.stderr)


def _start_config_server():
    """Publish the skill/plugin/mcp store to processes that cannot open the DB.

    DuckDB takes an exclusive file lock and this process holds it. The gateway now
    spawns the legacy bridge as a *sibling* process (not our grandchild); that bridge
    is a non-owning replica that must still reach the store. We attach our live store
    and serve it over a FIXED loopback port (GA_CONFIG_PORT) so the bridge — which the
    gateway launches independently and may outlive us — always has a stable address to
    reconnect to after a kernel crash/restart. Returns the bound port, 0 when there is
    nothing to share."""
    store = getattr(manager, "store", None)
    if store is None:
        return 0
    port = int(os.environ.get("GA_CONFIG_PORT", "0") or "0")
    if port:
        _reap_port(port)
    try:
        import ga_config
        ga_config.attach(store)
        bound = ga_config.serve(port=port)
        atexit.register(ga_config.stop_serving)
        print(f"[kernel] config store server on :{bound}", file=sys.stderr)
        return bound
    except Exception as e:
        print(f"[kernel] config store server unavailable: {e}", file=sys.stderr)
        return 0


# Optional root isolation (smoketest / multi-instance). Overrides ga_root and clears
# in-memory sessions so we never touch the production temp/desktop_sessions.json.
def _apply_root(root: str):
    root = os.path.abspath(root)
    manager.ga_root = root
    manager._sessions_file = Path(root) / "temp" / "desktop_sessions.json"
    manager._conv_folders_file = Path(root) / "temp" / "conv_folders.json"
    manager.sessions = {}
    manager.active_session_id = None
    try:
        manager._load_sessions()
    except Exception:
        pass


# --------------------------------------------------------------------------
# RPC method implementations (thin adapters over manager; mirrors the aiohttp handlers)
# --------------------------------------------------------------------------
def rpc_status(params):
    return {
        "ok": True, "running": True, "ready": True,
        "gaRoot": manager.ga_root, "mykeyPath": manager.mykey_path,
        "sessionCount": len(manager.sessions),
        "activeSessionId": manager.active_session_id,
        "transport": {"http": False, "wsEventsOnly": False, "stdioJsonRpc": True},
    }


def rpc_config_get(params):
    profiles = manager.list_model_profiles()
    active = next((p["id"] for p in profiles if p.get("active")), manager.config.get("llmNo", 0))
    cfg = dict(manager.config)
    if "llmNo" not in cfg:
        cfg["llmNo"] = active
    cfg.update(bridge._desktop_ui())
    return {"gaRoot": manager.ga_root, "mykeyPath": manager.mykey_path, "config": cfg}


def rpc_config_set(params):
    cfg = params.get("config", params)
    if isinstance(cfg, dict):
        patch = {k: cfg[k] for k in bridge._UI_KEYS if k in cfg}
        if patch:
            try:
                doc = json.loads(bridge._SETTINGS.read_text(encoding="utf-8")) if bridge._SETTINGS.is_file() else {}
                if not isinstance(doc, dict):
                    doc = {}
                ui = doc["ui"] if isinstance(doc.get("ui"), dict) else {}
                ui.update(patch)
                doc["ui"] = ui
                bridge._SETTINGS.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception as e:
                print(f"[kernel] save ui prefs failed: {e}", file=sys.stderr)
        manager.config.update(cfg)
    return {"ok": True, "gaRoot": manager.ga_root, "mykeyPath": manager.mykey_path, "config": manager.config}


def rpc_session_list(params):
    with manager.lock:
        sessions = [manager.snapshot(s, include_messages=False) for s in manager.sessions.values()]
    return {"sessions": sessions, "activeSessionId": manager.active_session_id}


def rpc_session_create(params):
    sess = manager.create_session(cwd=params.get("cwd") or params.get("path"), project=params.get("project"))
    return {"ok": True, "sessionId": sess.id, "session": manager.snapshot(sess)}


def rpc_session_get(params):
    sess = manager.get_session(params["sid"])
    return {"sessionId": params["sid"], "session": manager.snapshot(sess),
            "messages": list(sess.messages), "partial": sess.partial}


def rpc_session_delete(params):
    return manager.delete_session(params["sid"])


def rpc_session_patch(params):
    sess = manager.get_session(params["sid"])
    data = params.get("fields", params)
    if "title" in data:
        sess.title = data["title"]
        sess.untitled = False
    if "pinned" in data:
        sess.pinned = bool(data["pinned"])
    if "untitled" in data:
        sess.untitled = bool(data["untitled"])
    if "plan_scan_baseline" in data:
        sess.plan_scan_baseline = int(data["plan_scan_baseline"])
    if "folder_id" in data:
        fid = str(data["folder_id"] or "")
        sess.folder_id = fid[:64]
    sess.updated_at = time.time()
    manager._persist()
    return {"ok": True, "session": manager.snapshot(sess, include_messages=False)}


def rpc_session_messages(params):
    return manager.messages(params["sid"], after=int(params.get("after", 0)), limit=int(params.get("limit", 200)))


def rpc_session_plan(params):
    return manager.plan_snapshot(params["sid"])


def rpc_session_prompt(params):
    sid = params["sid"]
    prompt = params.get("prompt") or params.get("content") or params.get("message") or ""
    images = params.get("images") or []
    llm_no = params.get("llmNo")
    if llm_no is not None:
        llm_no = int(llm_no)
    return manager.submit_prompt(
        sid, prompt, images=images, llm_no=llm_no,
        display=params.get("display"),
        files_meta=params.get("files") or [],
        image_metas=params.get("imageMetas") or [],
        expert=params.get("expert"),
    )


def rpc_session_cancel(params):
    return manager.cancel(params["sid"])


def rpc_session_viewed(params):
    return manager.mark_viewed(params["sid"])


def rpc_session_restore(params):
    return manager.restore_context(params["sid"])


def rpc_session_suggest(params):
    return manager.suggest(params["sid"])


def rpc_conv_folders_get(params):
    with manager.lock:
        return {"folders": manager._load_conv_folders(), "assignments": manager._conv_folder_assignments()}


def rpc_conv_folders_set(params):
    # Faithful re-implementation of conv_folders_put_handler normalization,
    # so the canonical folder/assignment rules stay identical to the HTTP bridge.
    raw = params.get("folders")
    if not isinstance(raw, list):
        raise RpcError(-32602, "folders 字段必须是数组")
    SYS = {"default", "archived"}
    folders = []
    seen = set()
    for it in raw:
        if not it or not it.get("id"):
            continue
        fid = str(it["id"])
        name = str(it.get("name") or "").strip()
        if not name:
            continue
        locked = bool(it.get("locked")) or fid in SYS
        if fid in SYS:
            name = "default" if fid == "default" else "archived"
        if fid in seen:
            continue
        seen.add(fid)
        folders.append({"id": fid, "name": name, "locked": locked,
                        "sort_order": int(it.get("sort_order", 0) or 0)})
    manager._save_conv_folders(folders)
    assignments = params.get("assignments")
    changed = False
    if isinstance(assignments, dict):
        valid = {f["id"] for f in folders} | SYS
        with manager.lock:
            for sid, fid in assignments.items():
                sess = manager.sessions.get(sid)
                if not sess:
                    continue
                fid = str(fid or "")
                if fid and fid not in valid:
                    fid = ""
                if sess.folder_id != fid:
                    sess.folder_id = fid
                    changed = True
        if changed:
            manager._persist()
    return {"ok": True, "folders": folders, "assignments": manager._conv_folder_assignments()}


def rpc_model_profiles_list(params):
    return {"profiles": manager.list_model_profiles()}


def rpc_token_history_get(params):
    """Serve /token-history from the DuckDB store (the kernel owns the file lock)."""
    store = getattr(manager, "store", None)
    if store is None:
        return dict(bridge._TOKEN_EMPTY if hasattr(bridge, "_TOKEN_EMPTY") else
                    {"history": [], "snap": {}, "conductorHist": [], "conductorLast": None})
    return store.load_token_history()


def rpc_token_history_set(params):
    store = getattr(manager, "store", None)
    if store is None:
        return {"ok": False, "skipped": "no_db"}
    data = params.get("data", params)
    if not isinstance(data, dict):
        data = {}
    return store.save_token_history(data)


def rpc_model_profiles_add(params):
    return {"ok": True, **manager.add_model_profile(params.get("data", params))}


def rpc_model_profiles_get(params):
    return {"profile": manager.get_model_profile(int(params["id"]))}


def rpc_model_profiles_update(params):
    return {"ok": True, **manager.update_model_profile(int(params["id"]), params.get("data", params))}


def rpc_model_profiles_delete(params):
    return {"ok": True, **manager.delete_model_profile(int(params["id"]))}


DISPATCH = {
    "status": rpc_status,
    "config.get": rpc_config_get,
    "config.set": rpc_config_set,
    "session.list": rpc_session_list,
    "session.create": rpc_session_create,
    "session.get": rpc_session_get,
    "session.delete": rpc_session_delete,
    "session.patch": rpc_session_patch,
    "session.messages": rpc_session_messages,
    "session.plan": rpc_session_plan,
    "session.prompt": rpc_session_prompt,
    "session.cancel": rpc_session_cancel,
    "session.viewed": rpc_session_viewed,
    "session.restore": rpc_session_restore,
    "session.suggest": rpc_session_suggest,
    "conv_folders.get": rpc_conv_folders_get,
    "conv_folders.set": rpc_conv_folders_set,
    "model_profiles.list": rpc_model_profiles_list,
    "model_profiles.add": rpc_model_profiles_add,
    "model_profiles.get": rpc_model_profiles_get,
    "model_profiles.update": rpc_model_profiles_update,
    "model_profiles.delete": rpc_model_profiles_delete,
    "token_history.get": rpc_token_history_get,
    "token_history.set": rpc_token_history_set,
}


def _extract_error(e: Exception) -> str:
    text = getattr(e, "text", None)
    if isinstance(text, str) and text.strip():
        try:
            j = json.loads(text)
            if isinstance(j, dict) and j.get("error"):
                return str(j["error"])
        except Exception:
            return text
    return str(e)


def main():
    root = os.environ.get("GA_KERNEL_ROOT")
    if root:
        _apply_root(root)

    # The gateway spawns and owns the legacy bridge as a sibling process, so the
    # kernel no longer manages its lifecycle. We only publish the store over a fixed
    # loopback port so that bridge (a non-owning replica) can reach it across our
    # own crashes/restarts.
    config_port = _start_config_server()
    if config_port:
        print(f"[kernel] ready; config store published on :{config_port} for the bridge",
              file=sys.stderr)
    _start_bridge_server()

    # One-time import of the legacy desktop_token_history.json into the DB (idempotent).
    try:
        store = getattr(manager, "store", None)
        if store is not None:
            imported = store.import_token_history_file()
            if imported:
                print(f"[kernel] token_history: {imported}", file=sys.stderr)
    except Exception as e:
        print(f"[kernel] token_history import skipped: {e}", file=sys.stderr)

    # handshake so a gateway knows the kernel is ready on stdio
    _notify("kernel.ready", {"gaRoot": manager.ga_root})

    for raw in sys.stdin:
            line = raw.strip()
            if not line:
                continue
            try:
                req = json.loads(line)
            except Exception:
                _write({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}})
                continue
            if not isinstance(req, dict):
                _write({"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid Request"}})
                continue

            method = req.get("method")
            params = req.get("params") or {}
            rid = req.get("id")

            if method == "kernel.shutdown":
                if rid is not None:
                    _write({"jsonrpc": "2.0", "id": rid, "result": {"ok": True}})
                break

            fn = DISPATCH.get(method)
            if fn is None:
                if rid is not None:
                    _write({"jsonrpc": "2.0", "id": rid,
                            "error": {"code": -32601, "message": f"Method not found: {method}"}})
                continue

            try:
                result = fn(params)
                if rid is not None:
                    _write({"jsonrpc": "2.0", "id": rid, "result": result})
            except RpcError as e:
                if rid is not None:
                    _write({"jsonrpc": "2.0", "id": rid, "error": {"code": e.code, "message": e.message}})
            except Exception as e:
                if rid is not None:
                    _write({"jsonrpc": "2.0", "id": rid, "error": {"code": -32000, "message": _extract_error(e)}})
                print(f"[kernel] unhandled: {e}", file=sys.stderr)

    sys.exit(0)


if __name__ == "__main__":
    main()
