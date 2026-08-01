#!/usr/bin/env python3
"""DuckDB storage backend for GenericAgent desktop sessions/folders/token-history.

This module replaces the previous "rewrite the whole desktop_sessions.json on every
message" file backend with a relational store. The win: appending a message becomes a
single INSERT instead of a full-file rewrite (O(1) instead of O(N)).

Design notes
------------
* DuckDB is imported lazily inside DBStore so that importing this module (and the
  modules that import it, e.g. desktop_bridge) never fails when `duckdb` is not
  installed. If duckdb mode is requested but the package is missing, the caller is
  expected to fall back to the json backend.
* All access is serialized through a single RLock. DuckDB connections are not safe to
  share across threads without serialization, and our writer (the kernel process) is
  single-threaded for writes anyway. Reads are cheap and infrequent.
* SINGLE OWNER: DuckDB takes an exclusive lock on the database file, so exactly one
  process may open it. The kernel is that owner; the legacy-bridge grandchild runs in
  "replica" mode (no store, no persistence). Anything that must be served by the legacy
  bridge (currently /token-history) therefore stays file-backed.
* Writes still go through a short lock-retry loop to ride out transient conflicts
  between the writer thread and a concurrent reader in the same process.
* The session/message rows are shaped to be reconstructable into the exact dict the
  old JSON backend produced, so AgentManager's Session reconstruction logic is shared.
"""
from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

_JSON_PLACEHOLDER = "{}"

# Bump when the physical schema changes; add a branch in _migrate_schema for the step.
SCHEMA_VERSION = 3

# Runtime maintenance. DuckDB's CHECKPOINT only folds the WAL into the main file; it
# never hands free blocks back to the OS (a 268MB production store measured 514/1041
# blocks free). The only effective reclaim is a full rewrite via COPY FROM DATABASE,
# which needs exclusive access -- so it runs at open time, the one guaranteed
# single-writer moment, and only past a threshold so normal startups stay instant.
COMPACT_MIN_BYTES = 64 * 1024 * 1024
COMPACT_FREE_RATIO = 0.35
CHECKPOINT_INTERVAL = 600
BACKUP_RETAIN_DAYS = 7


class DBStore:
    def __init__(self, ga_root: str, mode: str = "duckdb", db_filename: str = "ga_store.duckdb",
                 maintain: bool = False):
        self.ga_root = str(ga_root)
        self.mode = mode
        self.db_path = Path(self.ga_root) / "temp" / db_filename
        self._lock = threading.RLock()
        self.con = None
        self._maint_stop = None
        self._maint_thread = None
        if self.mode == "duckdb":
            import duckdb  # lazy: only required when actually using duckdb
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._recover_swap()
            self.con = duckdb.connect(str(self.db_path))
            self.ensure_schema()
            if maintain:  # owner only; transient readers must not rewrite the file
                self.compact()
                self._cleanup_backups()
                self.start_maintenance()

    def close(self):
        """Release the file lock. Callers that only borrow the store (transient
        readers outside the owning process) must call this."""
        self.stop_maintenance()
        with self._lock:
            if self.con is not None:
                self.con.close()
                self.con = None

    # ------------------------------------------------------------------
    # runtime maintenance (WAL fold + free-block reclaim)
    # ------------------------------------------------------------------
    @property
    def _swap_path(self) -> Path:
        return self.db_path.with_name(self.db_path.name + ".pre-compact")

    def _recover_swap(self):
        """A crash mid-swap leaves the live file missing and the original parked at
        .pre-compact. Put it back before anyone opens the store."""
        swap = self._swap_path
        if swap.exists() and not self.db_path.exists():
            swap.replace(self.db_path)
            print(f"[db_store] recovered interrupted compaction: {swap.name}", file=sys.stderr)

    def storage_stats(self) -> Dict[str, Any]:
        if self.con is None:
            return {}
        with self._lock:
            cur = self.con.execute("PRAGMA database_size")
            row, cols = cur.fetchone(), [d[0] for d in cur.description]
        st = dict(zip(cols, row)) if row else {}
        block, total, free = st.get("block_size") or 0, st.get("total_blocks") or 0, st.get("free_blocks") or 0
        st["bytes_total"] = block * total
        st["free_ratio"] = free / total if total else 0.0
        return st

    def checkpoint(self):
        """Fold the WAL into the main file. Milliseconds; safe while serving."""
        if self.con is None:
            return
        try:
            self._w("CHECKPOINT")
        except Exception as e:  # noqa: BLE001 - a busy checkpoint just retries next tick
            print(f"[db_store] checkpoint skipped: {e}", file=sys.stderr)

    def _table_counts(self) -> Dict[str, int]:
        rows = self.con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()
        return {t: self.con.execute(f'SELECT count(*) FROM "{t}"').fetchone()[0] for (t,) in rows}

    def compact(self, force: bool = False) -> Optional[Dict[str, Any]]:
        """Rewrite the store to reclaim free blocks; returns a report or None when
        skipped. Owner only -- it briefly closes and reopens the connection."""
        if self.con is None:
            return None
        import duckdb
        self.checkpoint()
        before = self.storage_stats().get("bytes_total", 0)
        if not force and (before < COMPACT_MIN_BYTES
                          or self.storage_stats().get("free_ratio", 0) < COMPACT_FREE_RATIO):
            return None
        tmp, swap = self.db_path.with_name(self.db_path.name + ".compact"), self._swap_path
        for p in (tmp, tmp.with_name(tmp.name + ".wal")):
            p.unlink(missing_ok=True)
        with self._lock:
            counts = self._table_counts()
            try:
                name = self.con.execute("SELECT current_database()").fetchone()[0]
                self.con.execute(f"ATTACH '{tmp}' AS _compact")
                self.con.execute(f'COPY FROM DATABASE "{name}" TO _compact')
                self.con.execute("DETACH _compact")
            except Exception as e:  # noqa: BLE001 - keep serving from the original file
                print(f"[db_store] compaction aborted: {e}", file=sys.stderr)
                tmp.unlink(missing_ok=True)
                return None
            self.con.close()
            self.con = None
            try:
                probe = duckdb.connect(str(tmp), read_only=True)
                fresh = {t: probe.execute(f'SELECT count(*) FROM "{t}"').fetchone()[0] for t in counts}
                probe.close()
                if fresh != counts:
                    raise RuntimeError(f"row count mismatch {fresh} != {counts}")
                self.db_path.replace(swap)   # park the original
                tmp.replace(self.db_path)    # promote the rewrite
                self.db_path.with_name(self.db_path.name + ".wal").unlink(missing_ok=True)
                self.con = duckdb.connect(str(self.db_path))
                swap.unlink(missing_ok=True)  # committed
            except Exception as e:  # noqa: BLE001 - roll back to the parked original
                print(f"[db_store] compaction rolled back: {e}", file=sys.stderr)
                tmp.unlink(missing_ok=True)
                if swap.exists() and not self.db_path.exists():
                    swap.replace(self.db_path)
                self.con = duckdb.connect(str(self.db_path))
                return None
        after = self.storage_stats().get("bytes_total", 0)
        print(f"[db_store] compacted {before / 1048576:.0f}MB -> {after / 1048576:.0f}MB", file=sys.stderr)
        return {"before": before, "after": after, "freed": before - after}

    def start_maintenance(self, interval: int = CHECKPOINT_INTERVAL):
        """Periodic WAL fold so a long-lived store never carries an unbounded WAL."""
        if self._maint_thread is not None or self.con is None:
            return
        self._maint_stop = threading.Event()
        stop = self._maint_stop
        ticks = 0

        def loop():
            nonlocal ticks
            while not stop.wait(interval):
                self.checkpoint()
                ticks += 1
                if ticks % 5 == 0:  # ~50 min: log growth metrics
                    try:
                        st = self.storage_stats()
                        self.kv_set("db_metrics", {
                            "bytes": st.get("bytes_total", 0),
                            "free_ratio": st.get("free_ratio", 0),
                            "wal": st.get("wal_size", 0),
                            "at": time.time(),
                        })
                    except Exception as e:  # noqa: BLE001
                        print(f"[db_store] metrics skipped: {e}", file=sys.stderr)

        self._maint_thread = threading.Thread(target=loop, name="db-maint", daemon=True)
        self._maint_thread.start()

    def _cleanup_backups(self, retain_days: int = BACKUP_RETAIN_DAYS) -> int:
        """Rotate stale runtime backups out of temp/.

        Matches ONLY backup-named files (*.bak*, *.pre-compact, *.imported-*); the
        live store is never matched. The .pre-compact swap is only a recovery source
        while the live file is missing -- _recover_swap already ran, so a leftover
        alongside a healthy live file is safe to drop."""
        temp = self.db_path.parent
        if not temp.is_dir():
            return 0
        now = time.time()
        removed = 0
        for pat in ("*.bak*", "*.pre-compact", "*.imported-*"):
            for p in temp.glob(pat):
                if not p.is_file():
                    continue
                try:
                    if now - p.stat().st_mtime > retain_days * 86400:
                        p.unlink()
                        removed += 1
                        print(f"[db_store] rotated stale backup: {p.name}", file=sys.stderr)
                except OSError as e:
                    print(f"[db_store] cleanup skip {p.name}: {e}", file=sys.stderr)
        return removed

    def stop_maintenance(self):
        if self._maint_stop is not None:
            self._maint_stop.set()
        self._maint_thread = None

    # ------------------------------------------------------------------
    # low-level helpers
    # ------------------------------------------------------------------
    def _w(self, sql: str, params: Optional[List[Any]] = None):
        """Write with lock-retry (handles cross-process DuckDB write locks)."""
        last = None
        for attempt in range(12):
            try:
                with self._lock:
                    self.con.execute(sql, params or [])
                return
            except Exception as e:  # noqa: BLE001
                last = e
                msg = str(e).lower()
                if "lock" in msg or "conflict" in msg or "busy" in msg or "transaction" in msg:
                    time.sleep(0.1 * (attempt + 1))
                    continue
                raise
        print(f"[db_store] write failed after retries: {last}", file=sys.stderr)
        raise last

    def _q(self, sql: str, params: Optional[List[Any]] = None) -> List[tuple]:
        with self._lock:
            return self.con.execute(sql, params or []).fetchall()

    def _q1(self, sql: str, params: Optional[List[Any]] = None):
        with self._lock:
            row = self.con.execute(sql, params or []).fetchone()
        return row

    def ensure_schema(self):
        with self._lock:
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    id VARCHAR PRIMARY KEY,
                    title VARCHAR,
                    cwd VARCHAR,
                    folder_id VARCHAR,
                    pinned BOOLEAN,
                    untitled BOOLEAN,
                    plan_scan_baseline BIGINT,
                    plan_path VARCHAR,
                    workspace VARCHAR,
                    project VARCHAR,
                    created_at DOUBLE,
                    updated_at DOUBLE,
                    llm_history VARCHAR,
                    msg_seq BIGINT
                )
            """)
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    session_id VARCHAR,
                    seq BIGINT,
                    role VARCHAR,
                    content VARCHAR,
                    content_lc VARCHAR,
                    created_at DOUBLE,
                    extra VARCHAR,
                    PRIMARY KEY (session_id, seq)
                )
            """)
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS conv_folders (
                    id VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    locked BOOLEAN,
                    sort_order INTEGER
                )
            """)
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS projects (
                    name VARCHAR PRIMARY KEY,
                    instruction VARCHAR,
                    workspace VARCHAR,
                    created_at DOUBLE,
                    meta VARCHAR
                )
            """)
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS raw_logs (
                    logid VARCHAR,
                    session_id VARCHAR,
                    line_no BIGINT,
                    content VARCHAR,
                    ts DOUBLE
                )
            """)
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS log_session_map (
                    logid VARCHAR PRIMARY KEY,
                    session_id VARCHAR,
                    created_at DOUBLE
                )
            """)
            # --- skill hub: skills / plugins / mcp -------------------------------
            # Config tables are authoritative (v2 replaced skills_config.json,
            # plugin_configs.json and mcp_servers.json). *_meta are scan caches keyed
            # by directory, invalidated on mtime.
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS config_kv (
                    key VARCHAR PRIMARY KEY,
                    value VARCHAR,
                    updated_at DOUBLE
                )
            """)
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS skill_roots (
                    path VARCHAR PRIMARY KEY,
                    sort_order INTEGER,
                    updated_at DOUBLE
                )
            """)
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS plugin_dirs (
                    path VARCHAR PRIMARY KEY,
                    sort_order INTEGER,
                    updated_at DOUBLE
                )
            """)
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS skill_flags (
                    name VARCHAR PRIMARY KEY,
                    disabled BOOLEAN,
                    updated_at DOUBLE
                )
            """)
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS plugin_configs (
                    plugin_id VARCHAR PRIMARY KEY,
                    options VARCHAR,
                    updated_at DOUBLE
                )
            """)
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS mcp_servers (
                    name VARCHAR PRIMARY KEY,
                    spec VARCHAR,
                    sort_order INTEGER,
                    updated_at DOUBLE
                )
            """)
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS skill_meta (
                    dir VARCHAR PRIMARY KEY,
                    root VARCHAR,
                    name VARCHAR,
                    meta VARCHAR,
                    mtime DOUBLE,
                    scanned_at DOUBLE
                )
            """)
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS plugin_meta (
                    dir VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    meta VARCHAR,
                    mtime DOUBLE,
                    scanned_at DOUBLE
                )
            """)
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_skill_meta_root ON skill_meta(root)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_messages_sid_seq ON messages(session_id, seq)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_sessions_updated ON sessions(updated_at)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_sessions_folder ON sessions(folder_id)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_rawlogs_logid ON raw_logs(logid)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_rawlogs_sid ON raw_logs(session_id)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_logmap_sid ON log_session_map(session_id)")
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS token_history (
                    id INTEGER PRIMARY KEY CHECK (id = 1),  -- single-document row
                    doc VARCHAR NOT NULL
                )
            """)
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY,
                    applied_at DOUBLE
                )
            """)
        self._migrate_schema()

    def _current_schema_version(self) -> int:
        row = self._q1("SELECT max(version) FROM schema_version")
        return int(row[0]) if row and row[0] is not None else 0

    def _migrate_schema(self):
        """Bring an existing DB up to SCHEMA_VERSION.

        Version 0 means "created before schema_version existed" — the physical layout is
        already identical to v1 (ensure_schema is idempotent), so it is stamped, not rebuilt.
        v2 only adds tables (skill hub config + scan caches), which ensure_schema already
        created, so it is likewise a pure stamp; the json->db data import is done once by
        ga_config.bootstrap_from_legacy_json(). Future steps add `if cur < N: <ALTER ...>`
        branches before the stamp."""
        cur = self._current_schema_version()
        if cur >= SCHEMA_VERSION:
            return
        self._w("INSERT INTO schema_version (version, applied_at) VALUES (?,?)",
                [SCHEMA_VERSION, time.time()])

    # ------------------------------------------------------------------
    # sessions
    # ------------------------------------------------------------------
    def _session_row(self, s) -> tuple:
        llm_hist = None
        if s.agent and hasattr(s.agent, "llmclient"):
            try:
                llm_hist = s.agent.llmclient.backend.history
            except Exception:  # noqa: BLE001
                pass
        if llm_hist is None:
            llm_hist = s.llm_history
        return (
            s.id, s.title, s.cwd, s.folder_id or "",
            bool(s.pinned), bool(s.untitled),
            int(s.plan_scan_baseline or 0), s.plan_path or "",
            s.workspace or "", s.project or "",
            float(s.created_at), float(s.updated_at),
            json.dumps(llm_hist, ensure_ascii=False, default=str) if llm_hist is not None else None,
            int(s.msg_seq or 0),
        )

    def _upsert_session_row(self, s):
        self._w(
            """INSERT INTO sessions
               (id,title,cwd,folder_id,pinned,untitled,plan_scan_baseline,plan_path,
                workspace,project,created_at,updated_at,llm_history,msg_seq)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(id) DO UPDATE SET
                 title=excluded.title, cwd=excluded.cwd, folder_id=excluded.folder_id,
                 pinned=excluded.pinned, untitled=excluded.untitled,
                 plan_scan_baseline=excluded.plan_scan_baseline, plan_path=excluded.plan_path,
                 workspace=excluded.workspace, project=excluded.project,
                 created_at=excluded.created_at, updated_at=excluded.updated_at,
                 llm_history=excluded.llm_history, msg_seq=excluded.msg_seq""",
            list(self._session_row(s)),
        )

    def _insert_message(self, sid: str, m: dict):
        seq = int(m.get("id", 0))
        content = m.get("content", "")
        created_at = float(m.get("ts", time.time()))
        extra = {k: v for k, v in m.items() if k not in ("id", "role", "content", "ts")}
        self._w(
            """INSERT INTO messages (session_id, seq, role, content, content_lc, created_at, extra)
               VALUES (?,?,?,?,?,?,?)""",
            [sid, seq, m.get("role", ""), content, (content or "").lower(), created_at,
             json.dumps(extra, ensure_ascii=False, default=str)],
        )

    def _replace_messages(self, s):
        self._w("DELETE FROM messages WHERE session_id=?", [s.id])
        for m in s.messages:
            self._insert_message(s.id, m)

    def upsert_all(self, sessions: List):
        """Low-frequency full sync (create/delete/mark_viewed/patch/plan-preset)."""
        if not sessions:
            # refuse to wipe an existing store (mirrors the json "refuse empty overwrite" guard)
            row = self._q1("SELECT count(*) FROM sessions")
            if row and row[0] > 0:
                print("[db_store] refuse empty upsert_all (kept existing sessions)", file=__import__("sys").stderr)
            return
        for s in sessions:
            self._upsert_session_row(s)
            # A lazily-loaded session whose messages were never materialized cannot have
            # diverged from the DB, so skip the (expensive) delete+reinsert rewrite.
            if getattr(s, "messages_loaded", True):
                self._replace_messages(s)

    def upsert_session_meta(self, s):
        """Insert/update a session row (no message rewrite). Used on every new message."""
        self._upsert_session_row(s)

    def import_session(self, item: dict):
        """Bulk-import one session dict (shape produced by the old _persist) into the DB."""
        sid = item["id"]
        llm_hist = item.get("llm_history")
        self._w(
            """INSERT INTO sessions
               (id,title,cwd,folder_id,pinned,untitled,plan_scan_baseline,plan_path,
                workspace,project,created_at,updated_at,llm_history,msg_seq)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(id) DO UPDATE SET
                 title=excluded.title, cwd=excluded.cwd, folder_id=excluded.folder_id,
                 pinned=excluded.pinned, untitled=excluded.untitled,
                 plan_scan_baseline=excluded.plan_scan_baseline, plan_path=excluded.plan_path,
                 workspace=excluded.workspace, project=excluded.project,
                 created_at=excluded.created_at, updated_at=excluded.updated_at,
                 llm_history=excluded.llm_history, msg_seq=excluded.msg_seq""",
            [sid, item.get("title", "New chat"), item.get("cwd", ""), item.get("folder_id", "") or "",
             bool(item.get("pinned")), bool(item.get("untitled")),
             int(item.get("plan_scan_baseline") or 0), item.get("plan_path", "") or "",
             item.get("workspace", "") or "", item.get("project", "") or "",
             float(item.get("created_at") or time.time()), float(item.get("updated_at") or time.time()),
             json.dumps(llm_hist, ensure_ascii=False, default=str) if llm_hist is not None else None,
             int(item.get("msg_seq", 0))],
        )
        self._w("DELETE FROM messages WHERE session_id=?", [sid])
        for m in item.get("messages", []):
            self._insert_message(sid, m)

    def delete_session(self, sid: str):
        self._w("DELETE FROM messages WHERE session_id=?", [sid])
        self._w("DELETE FROM sessions WHERE id=?", [sid])
        self._w("DELETE FROM raw_logs WHERE session_id=?", [sid])

    def append_message(self, sid: str, msg: dict):
        self._insert_message(sid, msg)

    def _get_messages(self, sid: str, after: int = 0, limit: int = 0) -> List[dict]:
        sql = "SELECT seq, role, content, created_at, extra FROM messages WHERE session_id=? AND seq>?"
        params: List[Any] = [sid, after]
        if limit and limit > 0:
            sql += " ORDER BY seq LIMIT ?"
            params.append(limit)
        else:
            sql += " ORDER BY seq"
        rows = self._q(sql, params)
        out = []
        for r in rows:
            extra = json.loads(r[4]) if r[4] else {}
            out.append({"id": r[0], "role": r[1], "content": r[2], "ts": r[3], **extra})
        return out

    def get_messages(self, sid: str, after: int = 0, limit: int = 0) -> List[dict]:
        """Public accessor used by the lazy per-session loader in AgentManager."""
        return self._get_messages(sid, after, limit)

    def count_messages(self) -> Dict[str, int]:
        return {r[0]: int(r[1]) for r in
                self._q("SELECT session_id, count(*) FROM messages GROUP BY session_id")}

    def load_message_digests(self, sid: Optional[str] = None) -> Dict[str, List[dict]]:
        """Slim message rows used to derive sidebar rounds without loading payloads.

        Only user messages carry label text (the sidebar renders nothing else), so a
        session with megabytes of assistant output still yields a tiny result."""
        sql = ("SELECT session_id, seq, role, created_at, "
               "CASE WHEN role='user' THEN substr(content,1,200) ELSE '' END, "
               "CASE WHEN role='user' THEN extra ELSE NULL END "
               "FROM messages")
        params: List[Any] = []
        if sid is not None:
            sql += " WHERE session_id=?"
            params.append(sid)
        sql += " ORDER BY session_id, seq"
        out: Dict[str, List[dict]] = {}
        for r in self._q(sql, params):
            m = {"id": r[1], "role": r[2], "ts": r[3], "content": r[4]}
            if r[5]:
                try:
                    disp = json.loads(r[5]).get("display")
                    if isinstance(disp, str):
                        m["display"] = disp[:200]
                except Exception:  # noqa: BLE001
                    pass
            out.setdefault(r[0], []).append(m)
        return out

    def load_all_sessions(self, with_messages: bool = True) -> List[dict]:
        rows = self._q(
            "SELECT id,title,cwd,folder_id,pinned,untitled,plan_scan_baseline,plan_path,"
            "workspace,project,created_at,updated_at,llm_history,msg_seq FROM sessions"
        )
        counts = {} if with_messages else self.count_messages()
        out = []
        for r in rows:
            llm_history = json.loads(r[12]) if r[12] else None
            item = {
                "id": r[0], "title": r[1], "cwd": r[2], "folder_id": r[3],
                "pinned": r[4], "untitled": r[5], "plan_scan_baseline": r[6],
                "plan_path": r[7], "workspace": r[8], "project": r[9],
                "created_at": r[10], "updated_at": r[11], "llm_history": llm_history,
                "msg_seq": r[13],
            }
            if with_messages:
                item["messages"] = self._get_messages(r[0], 0, 0)
            else:
                item["msg_count"] = counts.get(r[0], 0)
            out.append(item)
        return out

    # ------------------------------------------------------------------
    # conv_folders
    # ------------------------------------------------------------------
    def load_conv_folders(self) -> List[dict]:
        rows = self._q("SELECT id, name, locked, sort_order FROM conv_folders ORDER BY sort_order")
        return [{"id": r[0], "name": r[1], "locked": bool(r[2]), "sort_order": r[3]} for r in rows]

    def save_conv_folders(self, folders: List[dict]):
        if not folders and self._q1("SELECT count(*) FROM conv_folders") and \
                self._q1("SELECT count(*) FROM conv_folders")[0] > 0:
            print("[db_store] refuse empty conv_folders overwrite (kept existing)", file=__import__("sys").stderr)
            return
        self._w("DELETE FROM conv_folders")
        for f in folders:
            self._w(
                "INSERT INTO conv_folders (id, name, locked, sort_order) VALUES (?,?,?,?)",
                [str(f.get("id")), str(f.get("name", "")), bool(f.get("locked")), int(f.get("sort_order", 0) or 0)],
            )

    # ------------------------------------------------------------------
    # projects (registration info only)
    # ------------------------------------------------------------------
    def upsert_project(self, name: str, instruction: str = "", workspace: str = "", meta: Optional[dict] = None):
        self._w(
            "INSERT INTO projects (name, instruction, workspace, created_at, meta) VALUES (?,?,?,?,?) "
            "ON CONFLICT(name) DO UPDATE SET instruction=excluded.instruction, "
            "workspace=excluded.workspace, meta=excluded.meta",
            [name, instruction or "", workspace or "", time.time(),
             json.dumps(meta or {}, ensure_ascii=False, default=str) if meta is not None else None],
        )

    def load_projects(self) -> List[dict]:
        rows = self._q("SELECT name, instruction, workspace, created_at, meta FROM projects")
        out = []
        for r in rows:
            meta = json.loads(r[4]) if r[4] else {}
            out.append({"name": r[0], "instruction": r[1], "workspace": r[2],
                        "created_at": r[3], "meta": meta})
        return out

    # ------------------------------------------------------------------
    # raw model_responses logs + logid<->session mapping
    # ------------------------------------------------------------------
    def map_log_session(self, logid: str, sid: str):
        self._w(
            "INSERT INTO log_session_map (logid, session_id, created_at) VALUES (?,?,?) "
            "ON CONFLICT(logid) DO UPDATE SET session_id=excluded.session_id, created_at=excluded.created_at",
            [logid, sid, time.time()],
        )

    def replace_raw_log(self, logid: str, lines: List[str], sid: Optional[str] = None):
        """Mirror one model_responses log. sid is NULL for historically imported logs
        (the logid<->session mapping was never persisted before this migration)."""
        ts = time.time()
        self._w("DELETE FROM raw_logs WHERE logid=?", [logid])
        with self._lock:
            self.con.executemany(
                "INSERT INTO raw_logs (logid, session_id, line_no, content, ts) VALUES (?,?,?,?,?)",
                [[logid, sid, i, line, ts] for i, line in enumerate(lines)],
            )

    def get_logid_for_session(self, sid: str) -> Optional[str]:
        row = self._q1("SELECT logid FROM log_session_map WHERE session_id=?", [sid])
        return row[0] if row else None

    # ------------------------------------------------------------------
    # skill hub: config_kv
    # ------------------------------------------------------------------
    def kv_get(self, key: str, default=None):
        row = self._q1("SELECT value FROM config_kv WHERE key=?", [key])
        if not row or row[0] is None:
            return default
        try:
            return json.loads(row[0])
        except Exception:  # noqa: BLE001
            return default

    def kv_set(self, key: str, value):
        self._w("INSERT INTO config_kv (key, value, updated_at) VALUES (?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                [key, json.dumps(value, ensure_ascii=False, default=str), time.time()])

    # ------------------------------------------------------------------
    # token history (single-document row; same whole-doc semantics as the old JSON file)
    # ------------------------------------------------------------------
    _EMPTY_TOKEN_HISTORY = {"history": [], "snap": {}, "conductorHist": [], "conductorLast": None}

    def load_token_history(self) -> dict:
        row = self._q1("SELECT doc FROM token_history WHERE id = 1")
        if not row or not row[0]:
            return dict(self._EMPTY_TOKEN_HISTORY)
        try:
            data = json.loads(row[0])
            return data if isinstance(data, dict) else dict(self._EMPTY_TOKEN_HISTORY)
        except Exception:
            return dict(self._EMPTY_TOKEN_HISTORY)

    def save_token_history(self, data: dict) -> dict:
        """Whole-document overwrite. Refuses to clobber a non-empty history with an
        empty one (frontend has been known to write back an empty array before the old
        data is fetched -- that used to wipe all usage records)."""
        new_hist = (data or {}).get("history") or []
        old = self.load_token_history()
        if not new_hist and (old or {}).get("history"):
            return {"ok": False, "skipped": "refuse_empty_overwrite",
                    "kept": len(old.get("history") or [])}
        self._w("INSERT INTO token_history (id, doc) VALUES (1, ?) "
                "ON CONFLICT(id) DO UPDATE SET doc = excluded.doc",
                [json.dumps(data or {}, ensure_ascii=False)])
        return {"ok": True}

    def import_token_history_file(self) -> Optional[dict]:
        """One-time import of the legacy desktop_token_history.json into the DB. Skips
        when the DB already holds data; renames the file (kept for rollback) on success.
        Idempotent, safe to call on every kernel start."""
        f = Path(self.ga_root) / "temp" / "desktop_token_history.json"
        if not f.is_file():
            return None
        if (self.load_token_history() or {}).get("history"):
            return {"skipped": "db_already_has_data"}
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[db_store] token_history import: unreadable file, skipped: {e}", file=sys.stderr)
            return None
        self._w("INSERT INTO token_history (id, doc) VALUES (1, ?) "
                "ON CONFLICT(id) DO UPDATE SET doc = excluded.doc",
                [json.dumps(data, ensure_ascii=False)])
        stamp = time.strftime("%Y%m%d-%H%M%S")
        f.replace(f.with_name(f"desktop_token_history.json.imported-{stamp}"))
        return {"imported": len((data or {}).get("history") or [])}

    def _replace_ordered(self, table: str, paths: List[str]):
        ts = time.time()
        self._w(f"DELETE FROM {table}")
        for i, p in enumerate(paths):
            if not p:
                continue
            self._w(f"INSERT INTO {table} (path, sort_order, updated_at) VALUES (?,?,?) "
                    f"ON CONFLICT(path) DO UPDATE SET sort_order=excluded.sort_order", [str(p), i, ts])

    # ------------------------------------------------------------------
    # skill hub: skills config (roots / plugin dirs / enabled / disabled skills)
    # ------------------------------------------------------------------
    def load_skills_config(self) -> Dict[str, Any]:
        roots = [r[0] for r in self._q("SELECT path FROM skill_roots ORDER BY sort_order")]
        dirs = [r[0] for r in self._q("SELECT path FROM plugin_dirs ORDER BY sort_order")]
        disabled = [r[0] for r in self._q("SELECT name FROM skill_flags WHERE disabled ORDER BY name")]
        cfg = {"skills_roots": roots, "plugin_dirs": dirs,
               "enabled": bool(self.kv_get("skills_enabled", True)), "disabled_skills": disabled}
        extra = self.kv_get("skills_config_extra")
        if isinstance(extra, dict):
            for k, v in extra.items():
                cfg.setdefault(k, v)
        return cfg

    def save_skills_config(self, cfg: Dict[str, Any]):
        known = ("skills_roots", "plugin_dirs", "enabled", "disabled_skills")
        self._replace_ordered("skill_roots", list(cfg.get("skills_roots") or []))
        self._replace_ordered("plugin_dirs", list(cfg.get("plugin_dirs") or []))
        ts = time.time()
        self._w("DELETE FROM skill_flags")
        for n in cfg.get("disabled_skills") or []:
            if n:
                self._w("INSERT INTO skill_flags (name, disabled, updated_at) VALUES (?,?,?) "
                        "ON CONFLICT(name) DO UPDATE SET disabled=excluded.disabled", [str(n), True, ts])
        self.kv_set("skills_enabled", bool(cfg.get("enabled", True)))
        extra = {k: v for k, v in cfg.items() if k not in known}
        self.kv_set("skills_config_extra", extra)
        self.kv_set("skills_config_rev", ts)

    def skills_config_rev(self) -> float:
        """Monotonic stamp for cache invalidation (replaces the old file mtime key)."""
        return float(self.kv_get("skills_config_rev", 0) or 0)

    # ------------------------------------------------------------------
    # skill hub: plugin user configs
    # ------------------------------------------------------------------
    def load_plugin_configs(self) -> Dict[str, dict]:
        out = {}
        for pid, opts in self._q("SELECT plugin_id, options FROM plugin_configs"):
            try:
                v = json.loads(opts) if opts else {}
            except Exception:  # noqa: BLE001
                v = {}
            if isinstance(v, dict):
                out[pid] = v
        return out

    def save_plugin_configs(self, configs: Dict[str, dict]):
        ts = time.time()
        self._w("DELETE FROM plugin_configs")
        for pid, opts in (configs or {}).items():
            self._w("INSERT INTO plugin_configs (plugin_id, options, updated_at) VALUES (?,?,?) "
                    "ON CONFLICT(plugin_id) DO UPDATE SET options=excluded.options",
                    [str(pid), json.dumps(opts or {}, ensure_ascii=False, default=str), ts])
        self.kv_set("skills_config_rev", ts)

    # ------------------------------------------------------------------
    # skill hub: mcp servers
    # ------------------------------------------------------------------
    def load_mcp_servers(self) -> Dict[str, dict]:
        out = {}
        for name, spec in self._q("SELECT name, spec FROM mcp_servers ORDER BY sort_order"):
            try:
                v = json.loads(spec) if spec else {}
            except Exception:  # noqa: BLE001
                v = {}
            out[name] = v if isinstance(v, dict) else {}
        return out

    def save_mcp_servers(self, servers: Dict[str, dict]):
        ts = time.time()
        self._w("DELETE FROM mcp_servers")
        for i, (name, spec) in enumerate((servers or {}).items()):
            self._w("INSERT INTO mcp_servers (name, spec, sort_order, updated_at) VALUES (?,?,?,?) "
                    "ON CONFLICT(name) DO UPDATE SET spec=excluded.spec, sort_order=excluded.sort_order",
                    [str(name), json.dumps(spec or {}, ensure_ascii=False, default=str), i, ts])
        self.kv_set("skills_config_rev", ts)

    # ------------------------------------------------------------------
    # skill hub: scan caches (SKILL.md frontmatter / plugin manifest)
    # ------------------------------------------------------------------
    def get_meta(self, table: str, dir_path: str, mtime: float) -> Optional[dict]:
        row = self._q1(f"SELECT meta, mtime FROM {table} WHERE dir=?", [dir_path])
        if not row or row[1] is None or abs(float(row[1]) - float(mtime)) > 1e-6:
            return None
        try:
            return json.loads(row[0]) if row[0] else None
        except Exception:  # noqa: BLE001
            return None

    def put_skill_meta(self, dir_path: str, root: str, name: str, meta: dict, mtime: float):
        self._w("INSERT INTO skill_meta (dir, root, name, meta, mtime, scanned_at) VALUES (?,?,?,?,?,?) "
                "ON CONFLICT(dir) DO UPDATE SET root=excluded.root, name=excluded.name, "
                "meta=excluded.meta, mtime=excluded.mtime, scanned_at=excluded.scanned_at",
                [dir_path, root or "", name or "", json.dumps(meta, ensure_ascii=False, default=str),
                 float(mtime), time.time()])

    def put_plugin_meta(self, dir_path: str, name: str, meta: dict, mtime: float):
        self._w("INSERT INTO plugin_meta (dir, name, meta, mtime, scanned_at) VALUES (?,?,?,?,?) "
                "ON CONFLICT(dir) DO UPDATE SET name=excluded.name, meta=excluded.meta, "
                "mtime=excluded.mtime, scanned_at=excluded.scanned_at",
                [dir_path, name or "", json.dumps(meta, ensure_ascii=False, default=str),
                 float(mtime), time.time()])

    def prune_meta(self, table: str, keep_dirs: List[str]):
        rows = self._q(f"SELECT dir FROM {table}")
        keep = set(keep_dirs or [])
        for (d,) in rows:
            if d not in keep:
                self._w(f"DELETE FROM {table} WHERE dir=?", [d])
