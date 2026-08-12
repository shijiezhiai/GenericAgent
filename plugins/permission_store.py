"""GA permission layer — storage & decision helpers (P0+).

Lives in its own DuckDB file (temp/ga_permission.duckdb) on purpose, so it does NOT
touch the fragile db_store sync machinery (which must be mirrored across 4 deploy
dirs per the deployment iron law). Audit is always recorded; if duckdb is
unavailable it degrades gracefully to a JSONL file.

Storage decision (2026-08-11): rules + audit both live in DuckDB.
Default mode: workspace-write.
"""
from __future__ import annotations
import os, time, threading, json

try:
    import duckdb
    _HAVE_DUCKDB = True
except Exception:  # pragma: no cover - duckdb optional in some envs
    _HAVE_DUCKDB = False

_ROOT = os.environ.get("GA_ROOT") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DB = os.path.join(_ROOT, "temp", "ga_permission.duckdb")
_JSONL = os.path.join(_ROOT, "temp", "ga_permission_audit.jsonl")
_lock = threading.Lock()

DEFAULT_MODE = "workspace-write"
_VALID_MODES = ("full-access", "workspace-write", "read-only", "plan")

_session = {"id": "unknown"}


def set_session_id(sid):
    _session["id"] = sid or "unknown"


def _session_id():
    return _session.get("id", "unknown")


def _conn():
    if not _HAVE_DUCKDB:
        return None
    os.makedirs(os.path.dirname(_DB), exist_ok=True)
    c = duckdb.connect(_DB)
    c.execute(
        "CREATE TABLE IF NOT EXISTS permission_audit ("
        " id BIGINT AUTO_INCREMENT PRIMARY KEY,"
        " ts DOUBLE, session_id VARCHAR, tool VARCHAR, target VARCHAR,"
        " action VARCHAR, decision VARCHAR, mode VARCHAR, frontend VARCHAR, detail VARCHAR)"
    )
    c.execute(
        "CREATE TABLE IF NOT EXISTS permission_rules ("
        " id BIGINT AUTO_INCREMENT PRIMARY KEY,"
        " mode VARCHAR, tool VARCHAR, target_glob VARCHAR, decision VARCHAR,"
        " created_at DOUBLE, note VARCHAR)"
    )
    c.execute(
        "CREATE TABLE IF NOT EXISTS permission_config ("
        " key VARCHAR PRIMARY KEY, value VARCHAR)"
    )
    # seed the persisted default mode once
    c.execute(
        "INSERT OR IGNORE INTO permission_config(key, value) VALUES('default_mode', ?)",
        [DEFAULT_MODE])
    return c


def record_audit(tool, target="", action="call", decision="allow",
                 mode=DEFAULT_MODE, frontend="", detail=""):
    """Append one audit row. Never raises — observability must not break the agent."""
    ts = time.time()
    sid = _session_id()
    with _lock:
        try:
            c = _conn()
            if c is not None:
                c.execute(
                    "INSERT INTO permission_audit"
                    "(ts,session_id,tool,target,action,decision,mode,frontend,detail)"
                    " VALUES (?,?,?,?,?,?,?,?,?)",
                    [ts, sid, str(tool), str(target)[:1024], str(action),
                     str(decision), str(mode), str(frontend), str(detail)[:2048]])
                c.close()
                return
        except Exception:
            pass
        # fallback: JSONL so audit survives even without duckdb
        try:
            with open(_JSONL, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "ts": ts, "session_id": sid, "tool": tool, "target": target,
                    "action": action, "decision": decision, "mode": mode,
                    "frontend": frontend, "detail": detail}, ensure_ascii=False) + "\n")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Rule table + config (P1). The deny>ask>allow decision itself lives in
# plugins/permissions.PermissionBroker; this module only stores/serves rules.
# Primary store is DuckDB; if duckdb is unavailable it degrades to a JSON file
# (same graceful-degradation philosophy as the audit JSONL fallback above).
# ---------------------------------------------------------------------------
_RULES_JSON = os.path.join(_ROOT, "temp", "ga_permission_rules.json")
_CONFIG_JSON = os.path.join(_ROOT, "temp", "ga_permission_config.json")
_rules_cache = None
_rules_cache_ts = 0.0
_RULE_TTL = 2.0  # seconds; rules change rarely, so a short TTL avoids per-call DB reads


def invalidate_rules_cache():
    global _rules_cache
    _rules_cache = None


def _json_load(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _json_save(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _json_rules():
    return _json_load(_RULES_JSON, [])


def _json_config():
    return _json_load(_CONFIG_JSON, {})


def load_rules():
    """Return list of {id,mode,tool,target_glob,decision,note}. Cached with TTL."""
    global _rules_cache, _rules_cache_ts
    now = time.time()
    if _rules_cache is not None and (now - _rules_cache_ts) < _RULE_TTL:
        return _rules_cache
    out = []
    c = _conn()
    if c is not None:
        try:
            for row in c.execute(
                "SELECT id,mode,tool,target_glob,decision,note "
                "FROM permission_rules ORDER BY id").fetchall():
                out.append({
                    "id": row[0], "mode": row[1], "tool": row[2],
                    "target_glob": row[3] or "", "decision": row[4], "note": row[5] or "",
                })
            c.close()
        except Exception:
            c.close() if c else None
            out = _json_rules()
    else:
        out = _json_rules()
    _rules_cache, _rules_cache_ts = out, now
    return out


def add_rule(mode, tool, decision, target_glob="", note=""):
    """Insert a rule; returns the new row id or None on failure."""
    c = _conn()
    if c is not None:
        try:
            c.execute(
                "INSERT INTO permission_rules(mode,tool,target_glob,decision,created_at,note) "
                "VALUES (?,?,?,?,?,?)",
                [str(mode), str(tool), str(target_glob), str(decision), time.time(), str(note)])
            rid = c.execute("SELECT max(id) FROM permission_rules").fetchone()[0]
            c.close()
            invalidate_rules_cache()
            return rid
        except Exception:
            try: c.close()
            except Exception: pass
    # JSON fallback
    rules = _json_rules()
    rid = (max([r["id"] for r in rules], default=0) + 1)
    rules.append({"id": rid, "mode": str(mode), "tool": str(tool),
                  "target_glob": str(target_glob), "decision": str(decision), "note": str(note)})
    _json_save(_RULES_JSON, rules)
    invalidate_rules_cache()
    return rid


def remove_rule(rule_id):
    """Delete a rule by id; returns True if a row was removed."""
    c = _conn()
    if c is not None:
        try:
            before = c.execute("SELECT count(*) FROM permission_rules WHERE id=?", [rule_id]).fetchone()[0]
            c.execute("DELETE FROM permission_rules WHERE id=?", [rule_id])
            after = c.execute("SELECT count(*) FROM permission_rules WHERE id=?", [rule_id]).fetchone()[0]
            c.close()
            invalidate_rules_cache()
            return before > 0 and after == 0
        except Exception:
            try: c.close()
            except Exception: pass
    # JSON fallback
    rules = _json_rules()
    before = len(rules)
    rules = [r for r in rules if r["id"] != rule_id]
    if len(rules) != before:
        _json_save(_RULES_JSON, rules)
        invalidate_rules_cache()
        return True
    return False


def get_config(key, default=None):
    c = _conn()
    if c is not None:
        try:
            r = c.execute("SELECT value FROM permission_config WHERE key=?", [key]).fetchone()
            return r[0] if r else default
        except Exception:
            return default
        finally:
            c.close()
    cfg = _json_config()
    return cfg.get(key, default)


def set_config(key, value):
    c = _conn()
    if c is not None:
        try:
            c.execute("INSERT OR REPLACE INTO permission_config(key,value) VALUES(?,?)",
                      [str(key), str(value)])
            return
        except Exception:
            pass
        finally:
            c.close()
    cfg = _json_config()
    cfg[str(key)] = str(value)
    _json_save(_CONFIG_JSON, cfg)
