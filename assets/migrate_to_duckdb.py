#!/usr/bin/env python3
"""One-time migration: JSON file storage -> DuckDB.

Reads the legacy JSON sources (desktop_sessions.json, conv_folders.json), the
projects registry, and the raw model_responses logs, inserts them into a DuckDB
file (temp/ga_store.duckdb), verifies row counts, then renames the originals to
.bak so the app switches to the DB.

desktop_token_history.json is deliberately left alone: /token-history is served by
the legacy bridge, which runs as a non-owning "replica" (DuckDB allows a single
process to hold the file lock), so it stays file-backed.

Raw model_responses .txt files are NOT deleted (they remain the source of truth
for /continue and the L4 history archive); they are mirrored into raw_logs with a
NULL session_id (the logid<->session mapping was never persisted historically).

Usage:
    python assets/migrate_to_duckdb.py --ga-root /path/to/ga_root [--force]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
FRONTENDS = HERE.parent / "frontends"
if str(FRONTENDS) not in sys.path:
    sys.path.insert(0, str(FRONTENDS))

from db_store import DBStore  # noqa: E402


def _count(con, sql):
    try:
        row = con.execute(sql).fetchone()
        return row[0] if row else 0
    except Exception:
        return -1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ga-root", default=str(HERE.parent), help="GA root (default: repo root)")
    ap.add_argument("--force", action="store_true", help="rebuild even if the DB already has data")
    args = ap.parse_args()

    ga_root = Path(args.ga_root).resolve()
    db_path = ga_root / "temp" / "ga_store.duckdb"

    temp = ga_root / "temp"
    sessions_file = temp / "desktop_sessions.json"
    folders_file = temp / "conv_folders.json"
    model_responses_dir = temp / "model_responses"
    projects_dir = temp / "projects"

    if args.force and db_path.exists():
        print(f"[migrate] --force: dropping existing {db_path}")
        db_path.unlink()

    store = DBStore(str(ga_root), mode="duckdb")
    con = store.con

    existing = _count(con, "SELECT count(*) FROM sessions")
    if existing > 0:
        print(f"[migrate] ABORT: {db_path} already has {existing} sessions. "
              f"Use --force to rebuild (drops existing data first).", file=sys.stderr)
        return 1

    # ---- sessions + messages ----
    src_sessions = 0
    src_messages = 0
    if sessions_file.is_file():
        arr = json.loads(sessions_file.read_text(encoding="utf-8"))
        for item in arr:
            store.import_session(item)
            src_sessions += 1
            src_messages += len(item.get("messages", []))
        print(f"[migrate] sessions: {src_sessions}, messages: {src_messages}")

    # ---- conv_folders ----
    src_folders = 0
    if folders_file.is_file():
        folders = json.loads(folders_file.read_text(encoding="utf-8"))
        if isinstance(folders, list):
            store.save_conv_folders(folders)
            src_folders = len(folders)
        print(f"[migrate] conv_folders: {src_folders}")

    # ---- projects (registration info only) ----
    src_projects = 0
    if projects_dir.is_dir():
        for pdir in sorted(projects_dir.iterdir()):
            if not pdir.is_dir():
                continue
            name = pdir.name
            workspace = ""
            ws_file = pdir / ".workspace.json"
            if ws_file.is_file():
                try:
                    workspace = (json.loads(ws_file.read_text(encoding="utf-8")).get("workspace") or "")
                except Exception:
                    pass
            instruction = ""
            meta_file = pdir / "project_memory.md"
            if meta_file.is_file():
                try:
                    instruction = meta_file.read_text(encoding="utf-8", errors="replace")[:500]
                except Exception:
                    pass
            store.upsert_project(name, instruction=instruction, workspace=workspace)
            src_projects += 1
        print(f"[migrate] projects: {src_projects}")

    # ---- raw model_responses logs (session_id = NULL) ----
    src_raw_lines = 0
    if model_responses_dir.is_dir():
        for txt in sorted(model_responses_dir.glob("model_responses_*.txt")):
            logid = txt.stem[len("model_responses_"):]
            lines = txt.read_text(encoding="utf-8", errors="replace").splitlines()
            store.replace_raw_log(logid, lines)
            src_raw_lines += len(lines)
        print(f"[migrate] raw_logs lines: {src_raw_lines}")

    # ---- verification ----
    print("\n[migrate] verification:")
    db_sessions = _count(con, "SELECT count(*) FROM sessions")
    db_messages = _count(con, "SELECT count(*) FROM messages")
    db_folders = _count(con, "SELECT count(*) FROM conv_folders")
    db_projects = _count(con, "SELECT count(*) FROM projects")
    db_raw = _count(con, "SELECT count(*) FROM raw_logs")

    def ok(a, b, label):
        flag = "OK " if a == b else "!! "
        print(f"  [{flag}] {label}: source={a} db={b}")

    ok(src_sessions, db_sessions, "sessions")
    ok(src_messages, db_messages, "messages")
    ok(src_folders, db_folders, "conv_folders")
    ok(src_projects, db_projects, "projects")
    ok(src_raw_lines, db_raw, "raw_logs")

    # ---- compact: row-by-row inserts leave ~2x block fragmentation; a fresh copy
    # halves the file size (measured 272MB -> 136MB on an 86MB JSON source) ----
    try:
        compact = db_path.with_name(db_path.stem + "_compact" + db_path.suffix)
        compact.unlink(missing_ok=True)
        con.execute(f"ATTACH '{compact.as_posix()}' AS compact")
        con.execute(f"COPY FROM DATABASE {db_path.stem} TO compact")
        con.execute("DETACH compact")
        store.con.close()
        before, after = db_path.stat().st_size, compact.stat().st_size
        compact.replace(db_path)
        print(f"[migrate] compacted DB: {before/1e6:.0f} MB -> {after/1e6:.0f} MB")
    except Exception as e:  # noqa: BLE001
        print(f"[migrate] compact skipped ({e}) — DB is valid, just larger", file=sys.stderr)

    # ---- rename originals to .bak (keep as rollback) ----
    for f in (sessions_file, folders_file):
        if f.is_file():
            bak = f.with_suffix(f.suffix + ".bak")
            if not bak.exists():
                f.rename(bak)
                print(f"[migrate] renamed {f.name} -> {bak.name}")
            else:
                print(f"[migrate] kept {f.name} (backup {bak.name} already exists)")
    print(f"\n[migrate] done. DB at {db_path}")
    print("[migrate] To run the app on DuckDB: set env GA_STORAGE=duckdb")
    print("[migrate] To roll back: unset GA_STORAGE (defaults to json) and restore the .bak files.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
