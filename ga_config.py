"""Skill hub configuration store — skills / plugins / mcp, backed by DuckDB.

Replaces the former json triple (skills_config.json, plugin_configs.json,
mcp_servers.json) as the source of truth. Those files are now only read once, on
first import into the db, and then left alone.

Why an indirection layer at all: DuckDB takes an exclusive file lock, so exactly one
process may hold the store, while skill/plugin/mcp config is read by many processes
(the kernel, the kernel-spawned legacy bridge, plain `ga` CLI runs). Access therefore
resolves in three tiers, in order:

  attached   the caller registered a live DBStore (kernel: attach() from AgentManager)
  proxy      the lock is held elsewhere; talk to the owner's loopback store server
             (address from GA_CONFIG_PORT, else temp/kernel_store.port)
  transient  nobody is serving, so open a connection and close it right away —
             a `ga` CLI run must not squat on the lock and block a later kernel

GA_STORAGE=json disables all of the above and keeps reading the legacy files, so a
broken migration can be rolled back with one env var.
"""
from __future__ import annotations

import json
import os
import sys
import threading

_HERE = os.path.dirname(os.path.abspath(__file__))
GA_ROOT = os.environ.get("GA_ROOT") or _HERE

_lock = threading.RLock()
_attached = None      # DBStore registered by the owning process
_own_failed = False   # transient open failed (no duckdb / locked) -> stop retrying
_PORT_FILE = os.path.join(GA_ROOT, "temp", "kernel_store.port")

SKILLS = "skill_meta"
PLUGINS = "plugin_meta"
_DEFAULT = {"skills_roots": [], "plugin_dirs": [], "enabled": True, "disabled_skills": []}


def _json_mode():
    return (os.environ.get("GA_STORAGE") or "").lower() == "json"


# ----------------------------------------------------------------------
# store resolution
# ----------------------------------------------------------------------
def attach(store):
    """Register the live DBStore of the process that owns the lock.

    The legacy-json import runs here, not lazily on first read. Deferring it is
    unsafe: a replica can write through serve() long before the owner does its
    first local read, and a bootstrap running at that point would import the stale
    json over the replica's writes."""
    global _attached
    with _lock:
        _attached = store
    _bootstrap(store)


def _open_transient():
    """Open our own connection, for callers that don't own the store. The caller
    must close() it: holding the lock for the life of a `ga` CLI run would stop the
    desktop kernel from ever starting."""
    global _own_failed
    if _own_failed:
        return None
    try:
        fe = os.path.join(_HERE, "frontends")   # _HERE, not GA_ROOT: the code lives
        if fe not in sys.path:                  # next to us even if GA_ROOT is moved
            sys.path.insert(0, fe)
        from db_store import DBStore
        st = DBStore(GA_ROOT)
    except Exception:  # noqa: BLE001 - lock held elsewhere or duckdb missing
        _own_failed = True
        return None
    _bootstrap(st)          # before any read/write: see the note in attach()
    return st


def _store():
    """The store only if this process owns it — never opens a new connection."""
    with _lock:
        return _attached


def _proxy_addr():
    port = os.environ.get("GA_CONFIG_PORT")
    if not port and os.path.isfile(_PORT_FILE):
        try:
            with open(_PORT_FILE, encoding="utf-8") as f:
                port = f.read().strip()
        except Exception:  # noqa: BLE001
            port = None
    try:
        return int(port) if port else 0
    except ValueError:
        return 0


def _rpc(op, **kw):
    """Call the owner's store server. Uses http.client so an HTTP_PROXY in the
    environment cannot hijack a loopback call."""
    port = _proxy_addr()
    if not port:
        return None
    import http.client
    body = json.dumps({"op": op, **kw}, ensure_ascii=False, default=str)
    try:
        con = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        con.request("POST", "/store", body=body.encode(), headers={"Content-Type": "application/json"})
        resp = con.getresponse()
        data = json.loads(resp.read().decode() or "{}")
        con.close()
    except Exception as e:  # noqa: BLE001
        print(f"[ga_config] store proxy {op} failed: {e}", file=sys.stderr)
        return None
    if isinstance(data, dict) and data.get("error"):
        print(f"[ga_config] store proxy {op}: {data['error']}", file=sys.stderr)
        return None
    return data.get("result") if isinstance(data, dict) else None


def _call(op, fallback, **kw):
    """Resolve in three tiers: the store we own, else the owner's proxy, else a
    transitive connection of our own (closed immediately, so we never squat on the
    lock). All three unavailable -> `fallback()`, the legacy-json degraded path."""
    st = _store()
    if st is not None:
        return OPS[op](st, **kw)
    if _proxy_addr():
        out = _rpc(op, **kw)
        if out is not None:
            return out
        # A proxy address is configured (this is a replica/secondary bridging to the
        # owner's loopback server). When it is unreachable -- e.g. the owner kernel is
        # restarting -- degrade to the legacy json instead of opening the DB ourselves:
        # taking the exclusive DuckDB lock would block the owner from ever coming back
        # up, and a failed open would stick `_own_failed` on and silently break every
        # later read. The owner republishes the store on its fixed port, so the next
        # call reconnects automatically.
        _warn_degraded()
        return fallback() if callable(fallback) else fallback
    st = _open_transient()
    if st is None:
        _warn_degraded()
        return fallback() if callable(fallback) else fallback
    try:
        return OPS[op](st, **kw)
    finally:
        st.close()

_warned = False


def _warn_degraded():
    """The store is unreachable (duckdb missing, or the owner died holding the
    lock). Say so once — silently serving an empty config would look to the user
    like every skill, plugin and mcp server had vanished."""
    global _warned
    if not _warned:
        _warned = True
        print("[ga_config] duckdb store unreachable; falling back to legacy json "
              "(set GA_STORAGE=json to silence)", file=sys.stderr)


# ----------------------------------------------------------------------
# operations (same signatures locally and over the wire)
# ----------------------------------------------------------------------
OPS = {
    "skills_config": lambda st: st.load_skills_config(),
    "save_skills_config": lambda st, cfg=None: (st.save_skills_config(cfg or {}), True)[1],
    "plugin_configs": lambda st: st.load_plugin_configs(),
    "save_plugin_configs": lambda st, configs=None: (st.save_plugin_configs(configs or {}), True)[1],
    "mcp_servers": lambda st: st.load_mcp_servers(),
    "save_mcp_servers": lambda st, servers=None: (st.save_mcp_servers(servers or {}), True)[1],
    "config_rev": lambda st: st.skills_config_rev(),
    "meta_get": lambda st, kind=SKILLS, wanted=None: {
        d: m for d, mt in (wanted or {}).items()
        for m in [st.get_meta(kind, d, mt)] if m is not None
    },
    "meta_put": lambda st, kind=SKILLS, rows=None: (_meta_put(st, kind, rows or []), True)[1],
    "meta_prune": lambda st, kind=SKILLS, dirs=None: (st.prune_meta(kind, dirs or []), True)[1],
}


def _meta_put(st, kind, rows):
    for r in rows:
        if kind == SKILLS:
            st.put_skill_meta(r["dir"], r.get("root", ""), r.get("name", ""), r.get("meta") or {}, r["mtime"])
        else:
            st.put_plugin_meta(r["dir"], r.get("name", ""), r.get("meta") or {}, r["mtime"])


# ----------------------------------------------------------------------
# public API
# ----------------------------------------------------------------------
def skills_config():
    """{skills_roots, plugin_dirs, enabled, disabled_skills} — raw, unresolved paths."""
    if _json_mode():
        return _legacy_read_skills() or dict(_DEFAULT)
    cfg = _call("skills_config", lambda: _legacy_read_skills() or dict(_DEFAULT))
    return cfg if isinstance(cfg, dict) else dict(_DEFAULT)


def save_skills_config(cfg):
    if _json_mode():
        return _legacy_write_skills(cfg)
    return _call("save_skills_config", lambda: _degraded_write(_legacy_write_skills, cfg), cfg=cfg)


def plugin_configs():
    """{plugin_id: {KEY: value}}"""
    if _json_mode():
        return _legacy_read_plugin_configs()
    out = _call("plugin_configs", _legacy_read_plugin_configs)
    return out if isinstance(out, dict) else {}


def save_plugin_configs(configs):
    if _json_mode():
        return _legacy_write_plugin_configs(configs)
    return _call("save_plugin_configs",
                 lambda: _degraded_write(_legacy_write_plugin_configs, configs), configs=configs)


def mcp_servers():
    """{name: {command, args, env, url, ...}}"""
    if _json_mode():
        return _legacy_read_mcp()
    out = _call("mcp_servers", _legacy_read_mcp)
    return out if isinstance(out, dict) else {}


def save_mcp_servers(servers):
    if _json_mode():
        return _legacy_write_mcp(servers)
    return _call("save_mcp_servers", lambda: _degraded_write(_legacy_write_mcp, servers), servers=servers)


def config_rev():
    """Monotonic revision stamp; replaces the config file mtime as a cache key."""
    if _json_mode():
        p = _legacy_skills_path()
        return os.path.getmtime(p) if os.path.isfile(p) else 0.0
    return _call("config_rev", 0.0) or 0.0


def meta_get(kind, wanted):
    """wanted={dir: mtime} -> {dir: meta} for entries still fresh."""
    if _json_mode() or not wanted:
        return {}
    out = _call("meta_get", {}, kind=kind, wanted=wanted)
    return out if isinstance(out, dict) else {}


def meta_put(kind, rows):
    if _json_mode() or not rows:
        return False
    return _call("meta_put", False, kind=kind, rows=rows)


def meta_prune(kind, dirs):
    if _json_mode():
        return False
    return _call("meta_prune", False, kind=kind, dirs=dirs)


# ----------------------------------------------------------------------
# legacy json (bootstrap source + GA_STORAGE=json fallback)
# ----------------------------------------------------------------------
def _is_bundle():
    if os.environ.get("GA_BUILD_ID"):
        return True
    return "Application Support" in GA_ROOT or ".app/Contents/Resources/runtime" in GA_ROOT


def _support_root():
    return os.path.dirname(GA_ROOT) if _is_bundle() else GA_ROOT


def _legacy_skills_path():
    env = os.environ.get("GA_SKILLS_CONFIG")
    if env:
        return env
    p = os.path.join(_support_root(), "skills_config.json") if _is_bundle() \
        else os.path.join(GA_ROOT, "plugins", "skills_config.json")
    if os.path.isfile(p):
        return p
    seed = os.path.join(GA_ROOT, "plugins", "skills_config.json")
    return seed if os.path.isfile(seed) else p


def _legacy_plugin_configs_path():
    return os.path.join(os.path.dirname(_legacy_skills_path()), "plugin_configs.json")


def _legacy_mcp_paths():
    return [os.path.join(GA_ROOT, "mcp_servers.json"),
            os.path.join(os.path.expanduser("~"), ".config", "ga", "mcp_servers.json")]


def _read_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception:  # noqa: BLE001
        return None


def _write_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.utime(path, None)
    return True


def _legacy_read_skills():
    return _read_json(_legacy_skills_path())


def _legacy_write_skills(cfg):
    return _write_json(_legacy_skills_path(), cfg)


def _legacy_read_plugin_configs():
    data = _read_json(_legacy_plugin_configs_path()) or {}
    cfgs = data.get("pluginConfigs") if isinstance(data.get("pluginConfigs"), dict) else {}
    return {pid: v["options"] for pid, v in cfgs.items()
            if isinstance(v, dict) and isinstance(v.get("options"), dict)}


def _legacy_write_plugin_configs(configs):
    payload = {"pluginConfigs": {pid: {"options": opts} for pid, opts in (configs or {}).items()}}
    return _write_json(_legacy_plugin_configs_path(), payload)


def _legacy_read_mcp():
    for p in _legacy_mcp_paths():
        data = _read_json(p)
        if data is None:
            continue
        servers = data.get("mcpServers") if isinstance(data.get("mcpServers"), dict) else data
        return {k: v for k, v in servers.items() if isinstance(v, dict)}
    return {}


def _legacy_write_mcp(servers):
    return _write_json(_legacy_mcp_paths()[0], {"mcpServers": servers or {}})


_DIRTY_FLAG = os.path.join(GA_ROOT, "temp", ".skillhub_json_wins")


def _degraded_write(fn, payload):
    """Write while the store is unreachable: land it in the legacy json and drop a
    flag, so the next process that does reach the db re-imports instead of serving
    a stale copy. Without the flag the edit would silently disappear."""
    ok = fn(payload)
    try:
        os.makedirs(os.path.dirname(_DIRTY_FLAG), exist_ok=True)
        open(_DIRTY_FLAG, "w").close()
    except Exception:  # noqa: BLE001
        pass
    return ok


_bootstrapped = False


def _bootstrap(st, force=False):
    """One-shot import of the three legacy json files into `st`. Idempotent twice
    over: a process-local flag skips the round trip, and a kv stamp in the db means
    a later hand-edit of the json files is NOT re-imported (the db wins once
    stamped) — unless _degraded_write left the dirty flag, which means the json
    genuinely is newer. Runs under whichever connection reached the store first."""
    global _bootstrapped
    if _bootstrapped and not force:
        return False
    _bootstrapped = True
    dirty = os.path.isfile(_DIRTY_FLAG)
    try:
        if st.kv_get("skillhub_imported") and not (force or dirty):
            return False
        legacy = _legacy_read_skills()
        if legacy:
            cfg = dict(_DEFAULT)
            cfg.update({k: v for k, v in legacy.items() if v is not None})
            st.save_skills_config(cfg)
        pc = _legacy_read_plugin_configs()
        if pc:
            st.save_plugin_configs(pc)
        mcp = _legacy_read_mcp()
        if mcp:
            st.save_mcp_servers(mcp)
        st.kv_set("skillhub_imported", {"at": __import__("time").time(),
                                        "skills": bool(legacy), "plugin_configs": len(pc), "mcp": len(mcp)})
        if dirty:
            os.remove(_DIRTY_FLAG)
        print(f"[ga_config] imported legacy config into duckdb "
              f"(roots={len((legacy or {}).get('skills_roots', []))}, "
              f"plugin_dirs={len((legacy or {}).get('plugin_dirs', []))}, "
              f"plugin_configs={len(pc)}, mcp={len(mcp)})", file=sys.stderr)
        return True
    except Exception as e:  # noqa: BLE001
        print(f"[ga_config] legacy import failed: {e}", file=sys.stderr)
        return False


# ----------------------------------------------------------------------
# store server (run by the lock owner so replicas can read/write)
# ----------------------------------------------------------------------
def serve(port=0):
    """Start the loopback store server in a daemon thread; returns the bound port.
    Publishes it to temp/kernel_store.port for processes started later."""
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_POST(self):
            n = int(self.headers.get("Content-Length") or 0)
            try:
                req = json.loads(self.rfile.read(n).decode() or "{}")
                op = req.pop("op", "")
                fn = OPS.get(op)
                st = _store()
                if fn is None or st is None:
                    raise RuntimeError(f"bad op {op!r}" if fn is None else "store unavailable")
                payload = {"result": fn(st, **req)}
            except Exception as e:  # noqa: BLE001
                payload = {"error": str(e)}
            body = json.dumps(payload, ensure_ascii=False, default=str).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    bound = srv.server_address[1]
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        os.makedirs(os.path.dirname(_PORT_FILE), exist_ok=True)
        with open(_PORT_FILE, "w", encoding="utf-8") as f:
            f.write(str(bound))
    except Exception:  # noqa: BLE001
        pass
    return bound


def stop_serving():
    try:
        os.remove(_PORT_FILE)
    except Exception:  # noqa: BLE001
        pass
