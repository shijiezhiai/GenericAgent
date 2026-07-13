#!/usr/bin/env python3
"""
GenericAgent Web2 Bridge.

Clear split:
1) AgentManager: owns GenericAgent instances, sessions and histories.
2) Transport: HTTP is the command/data channel; WebSocket only pushes small
   session-state notifications.

HTTP API:
  GET    /status
  GET    /config
  POST   /config
  GET    /model-profiles  (+ POST / PUT / DELETE by id)
  GET    /sessions
  POST   /session/new
  GET    /session/{sid}
  DELETE /session/{sid}
  POST   /session/{sid}/prompt
  GET    /session/{sid}/messages?after=0&limit=200
  POST   /session/{sid}/cancel
  POST   /services/start        body: {"id":"frontends/qqapp.py"}
  POST   /services/stop         body: {"id":"frontends/qqapp.py"}
  GET    /services/logs?id=frontends/qqapp.py&tail=200
  GET    /services/panel
  GET    /services/mykey
  POST   /services/mykey       body: {"content":"..."}
  POST   /services/stop-extras   stop conductor + scheduler (127.0.0.1 only)
  POST   /services/start-extras  start conductor + scheduler (127.0.0.1 only)
  POST   /services/bridge/exit    stop managed services, then exit bridge (127.0.0.1 only)

WS API (state sync):
  GET /ws -> on connect sends services.snapshot; service.changed on updates
  {"type":"services.snapshot","services":[...]}
  {"type":"service.changed","service":{...}}
"""
from __future__ import annotations

import asyncio, atexit, contextlib, importlib, json, os, re, subprocess, sys
from datetime import datetime
from collections import Counter, deque
import threading, time, traceback, uuid, hmac
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from aiohttp import web, WSMsgType
import workspace_cmd

APP_DIR = Path(__file__).resolve().parent


def find_default_ga_root() -> Path:
    candidates = [
        APP_DIR / "..",
        APP_DIR / ".." / "..",
        APP_DIR / ".." / "GenericAgent",
        APP_DIR / ".." / ".." / "GenericAgent",
    ]
    for p in candidates:
        root = p.resolve()
        if (root / "agentmain.py").exists():
            return root
    return APP_DIR.parent.parent.resolve()


DEFAULT_GA_ROOT = find_default_ga_root()

_FINAL_INFO_RE = re.compile(r'\n*`{5}\n*\[Info\] Final response to user\.\n*`{5}\s*$')


def strip_final_info_marker(text: Any) -> str:
    return _FINAL_INFO_RE.sub('', str(text or ''))


for _s in (sys.stdout, sys.stderr):
    with contextlib.suppress(Exception):
        _s.reconfigure(encoding="utf-8", errors="replace")


# ---------------------------------------------------------------------------
# Agent management layer
# ---------------------------------------------------------------------------

@dataclass
class Session:
    id: str
    title: str = "New chat"
    cwd: str = ""
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    messages: List[dict] = field(default_factory=list)
    msg_seq: int = 0
    partial: Optional[dict] = None
    status: str = "idle"  # idle|running|error|cancelled
    agent: Any = None
    thread: Optional[threading.Thread] = None
    cancel_requested: bool = False
    last_error: str = ""
    pinned: bool = False
    untitled: bool = True
    plan_scan_baseline: int = 0
    plan_path: str = ""
    workspace: str = ""
    project: str = ""
    llm_history: Optional[List[dict]] = None


def _load_plan_baseline(item: dict, msgs: list) -> int:
    """Persisted per-session baseline (tuiapp_v2: set on /continue, not on preset text)."""
    base = int(item.get("plan_scan_baseline", 0) or 0)
    if base >= len(msgs):
        return 0
    return max(0, base)


def _sanitize_desktop_plan_path(session_id: str, plan_path: str) -> str:
    """Desktop: drop shared plan_demo paths so sessions do not read the same file."""
    import plan_state
    p = (plan_path or "").strip()
    if not p:
        return ""
    if plan_state.is_session_scoped_plan_path(p, session_id):
        return p
    return plan_state.default_session_plan_path(session_id)


class AgentManager:
    def __init__(self):
        self.lock = threading.RLock()
        self.ga_root = str(DEFAULT_GA_ROOT)
        self.config: Dict[str, Any] = {}
        self.sessions: Dict[str, Session] = {}
        self.active_session_id: Optional[str] = None
        self._sessions_file = Path(self.ga_root) / "temp" / "desktop_sessions.json"
        self._load_sessions()

    @property
    def mykey_path(self) -> str:
        return str(Path(self.ga_root) / "mykey.py")

    def _persist(self):
        try:
            self._sessions_file.parent.mkdir(parents=True, exist_ok=True)
            arr = []
            with self.lock:
                for s in self.sessions.values():
                    llm_hist = None
                    if s.agent and hasattr(s.agent, 'llmclient'):
                        try: llm_hist = s.agent.llmclient.backend.history
                        except Exception: pass
                    if llm_hist is None:
                        llm_hist = s.llm_history
                    arr.append({"id": s.id, "title": s.title, "cwd": s.cwd,
                                "created_at": s.created_at, "updated_at": s.updated_at,
                                "messages": s.messages, "msg_seq": s.msg_seq,
                                "pinned": s.pinned, "untitled": s.untitled,
                                "plan_scan_baseline": s.plan_scan_baseline,
                                "plan_path": s.plan_path or "",
                                "workspace": s.workspace or "",
                                "project": s.project or "",
                                "llm_history": llm_hist})
            self._sessions_file.write_text(json.dumps(arr, ensure_ascii=False, default=str), encoding="utf-8")
        except Exception as e:
            print(f"[bridge] persist sessions failed: {e}", file=sys.stderr)

    def _load_sessions(self):
        try:
            if not self._sessions_file.exists():
                return
            arr = json.loads(self._sessions_file.read_text(encoding="utf-8"))
            for item in arr:
                msgs = item.get("messages", [])
                sess = Session(id=item["id"], title=item.get("title", "New chat"),
                               cwd=item.get("cwd", self.ga_root),
                               created_at=item.get("created_at", time.time()),
                               updated_at=item.get("updated_at", time.time()),
                               messages=msgs,
                               msg_seq=item.get("msg_seq", 0),
                               pinned=item.get("pinned", False),
                               untitled=item.get("untitled", True),
                               plan_scan_baseline=_load_plan_baseline(item, msgs),
                               plan_path=_sanitize_desktop_plan_path(
                                   item["id"], item.get("plan_path") or ""),
                               workspace=item.get("workspace", ""),
                               project=item.get("project", ""),
                               status="idle", agent=None,
                               llm_history=item.get("llm_history"))
                self.sessions[sess.id] = sess
            if self.sessions:
                self.active_session_id = max(self.sessions.values(), key=lambda s: s.updated_at).id
        except Exception as e:
            print(f"[bridge] load sessions failed: {e}", file=sys.stderr)

    def _datasources_file(self):
        return Path(self.ga_root) / "temp" / "desktop_datasources.json"

    def _project_dir(self, project_name: str) -> Path:
        return Path(self.ga_root) / "temp" / "projects" / str(project_name or "")

    def _project_datasources_file(self, project_name: str) -> Path:
        return self._project_dir(project_name) / ".datasources.json"

    def _project_events_file(self, project_name: str) -> Path:
        return self._project_dir(project_name) / ".events.jsonl"

    def _load_datasources(self) -> Dict[str, Any]:
        try:
            f = self._datasources_file()
            if not f.exists():
                return {}
            return json.loads(f.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[bridge] load datasources failed: {e}", file=sys.stderr)
            return {}

    def _save_datasources(self, d: Dict[str, Any]):
        try:
            f = self._datasources_file()
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_text(json.dumps(d, ensure_ascii=False, default=str), encoding="utf-8")
        except Exception as e:
            print(f"[bridge] save datasources failed: {e}", file=sys.stderr)

    def _load_project_datasource_ids(self, project_name: str) -> list:
        try:
            f = self._project_datasources_file(project_name)
            if not f.exists():
                return []
            data = json.loads(f.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return [str(x).strip() for x in data if str(x).strip()]
            return []
        except Exception:
            return []

    def _save_project_datasource_ids(self, project_name: str, ids: list) -> None:
        f = self._project_datasources_file(project_name)
        f.parent.mkdir(parents=True, exist_ok=True)
        clean, seen = [], set()
        for x in ids or []:
            s = str(x).strip()
            if s and s not in seen:
                clean.append(s)
                seen.add(s)
        f.write_text(json.dumps(clean, ensure_ascii=False), encoding="utf-8")

    # ── 项目待办 (todos) 持久化 ──────────────────────────────
    def _project_todos_file(self, project_name: str) -> Path:
        return self._project_dir(project_name) / ".todos.json"

    def _load_todos(self, project_name: str) -> list:
        try:
            f = self._project_todos_file(project_name)
            if not f.exists():
                return []
            data = json.loads(f.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
        except Exception as e:
            print(f"[bridge] load todos failed: {e}", file=sys.stderr)
            return []

    def _save_todos(self, project_name: str, items: list) -> None:
        f = self._project_todos_file(project_name)
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(json.dumps(items, ensure_ascii=False, default=str), encoding="utf-8")

    def list_todos(self, project_name: str) -> list:
        return self._load_todos(project_name)

    def create_todo(self, project_name: str, data: dict) -> dict:
        import time, secrets
        items = self._load_todos(project_name)
        now = int(time.time())
        status = str(data.get("status") or "todo")
        if status not in ("todo", "doing", "pause", "done"):
            status = "todo"
        item = {
            "id": "td_" + str(now) + "_" + secrets.token_hex(3),
            "title": str(data.get("title", "")).strip()[:500],
            "desc": str(data.get("desc", "")).strip()[:8000],
            "status": status,
            "assignee": str(data.get("assignee", "")).strip()[:100],
            "due": str(data.get("due", "")).strip()[:20],
            "createdAt": now,
            "updatedAt": now,
        }
        items.append(item)
        self._save_todos(project_name, items)
        return item

    def update_todo(self, project_name: str, tid: str, data: dict) -> dict:
        import time
        items = self._load_todos(project_name)
        for it in items:
            if it.get("id") == tid:
                if "title" in data:
                    it["title"] = str(data["title"]).strip()[:500]
                if "desc" in data:
                    it["desc"] = str(data["desc"]).strip()[:8000]
                if "status" in data:
                    s = str(data["status"])
                    if s in ("todo", "doing", "pause", "done"):
                        it["status"] = s
                if "assignee" in data:
                    it["assignee"] = str(data["assignee"]).strip()[:100]
                if "due" in data:
                    it["due"] = str(data["due"]).strip()[:20]
                it["updatedAt"] = int(time.time())
                self._save_todos(project_name, items)
                return it
        return {}

    def delete_todo(self, project_name: str, tid: str) -> bool:
        items = self._load_todos(project_name)
        before = len(items)
        items = [it for it in items if it.get("id") != tid]
        if len(items) < before:
            self._save_todos(project_name, items)
            return True
        return False

    def bind_project_datasource(self, project_name: str, dsid: str) -> list:
        ids = self._load_project_datasource_ids(project_name)
        if dsid not in ids:
            ids.append(dsid)
            self._save_project_datasource_ids(project_name, ids)
        return ids

    def unbind_datasource_from_projects(self, dsid: str) -> None:
        base = Path(self.ga_root) / "temp" / "projects"
        if not base.exists():
            return
        for pdir in base.iterdir():
            if not pdir.is_dir():
                continue
            f = pdir / ".datasources.json"
            if not f.exists():
                continue
            try:
                ids = json.loads(f.read_text(encoding="utf-8"))
                if not isinstance(ids, list) or dsid not in ids:
                    continue
                ids = [str(x).strip() for x in ids if str(x).strip() and str(x).strip() != dsid]
                if ids:
                    f.write_text(json.dumps(ids, ensure_ascii=False), encoding="utf-8")
                else:
                    f.unlink()
            except Exception:
                continue

    def list_project_datasources(self, project_name: str) -> list:
        ids = self._load_project_datasource_ids(project_name)
        all_ds = self._load_datasources()
        return [all_ds[dsid] for dsid in ids if dsid in all_ds]

    def append_project_event(self, project_name: str, event: dict) -> Optional[dict]:
        pdir = self._project_dir(project_name)
        if not pdir.is_dir():
            return None
        out = dict(event)
        out["project_name"] = project_name
        f = self._project_events_file(project_name)
        f.parent.mkdir(parents=True, exist_ok=True)
        with f.open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(out, ensure_ascii=False) + "\n")
        return out

    def create_datasource(self, data: dict) -> dict:
        dsid = "ds_" + uuid.uuid4().hex[:12]
        project = str(data.get("project") or "").strip()
        dstype = str(data.get("type") or "gitlab").strip() or "gitlab"
        default_names = {
            "gitlab": "GitLab",
            "github": "GitHub",
            "cnb": "CNB",
            "tapd": "TAPD",
        }
        default_events = {
            "gitlab": ["issue_open", "issue_update", "merge_request_open", "merge_request_update"],
            "github": ["issues_opened", "issues_edited", "pull_request_opened", "pull_request_edited"],
            "cnb": [],
            "tapd": [],
        }
        ds = {
            "id": dsid,
            "type": dstype,
            "name": data.get("name", default_names.get(dstype, "Datasource")),
            "events": data.get("events") or default_events.get(dstype, []),
            "secret": (data.get("secret") or "").strip() or uuid.uuid4().hex,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "event_log": [],
            "project": project,
        }
        with self.lock:
            d = self._load_datasources()
            d[dsid] = ds
            self._save_datasources(d)
            if project:
                self.bind_project_datasource(project, dsid)
        return ds

    def list_datasources(self) -> list:
        with self.lock:
            return list(self._load_datasources().values())

    def get_datasource(self, dsid: str) -> Optional[dict]:
        with self.lock:
            return self._load_datasources().get(dsid)

    def delete_datasource(self, dsid: str) -> bool:
        with self.lock:
            d = self._load_datasources()
            if dsid not in d:
                return False
            del d[dsid]
            self._save_datasources(d)
            self.unbind_datasource_from_projects(dsid)
            return True

    def append_datasource_event(self, dsid: str, event: dict) -> Optional[dict]:
        with self.lock:
            d = self._load_datasources()
            ds = d.get(dsid)
            if ds is None:
                return None
            ds.setdefault("event_log", []).append(event)
            if len(ds["event_log"]) > 200:  # ponytail: keep last 200, split store if volume matters
                ds["event_log"] = ds["event_log"][-200:]
            self._save_datasources(d)
            project = str(ds.get("project") or "").strip()
            if project:
                self.append_project_event(project, {**event, "datasource_id": dsid, "datasource_name": ds.get("name", "")})
            return ds

    def _mykey_file(self) -> Path:
        p = Path(self.ga_root) / "mykey.py"
        if not p.exists():
            tpl = Path(self.ga_root) / "mykey_template.py"
            p.write_text(tpl.read_text(encoding="utf-8") if tpl.exists() else "", encoding="utf-8")
        return p

    @staticmethod
    def _next_native_var(text: str, protocol: str) -> str:
        # 协议必选(由前端下拉强制),不再用 apibase 兜底瞎猜
        proto = str(protocol or "").strip().lower()
        if proto == "claude":
            prefix = "native_claude_config"
        elif proto in ("oai", "openai"):
            prefix = "native_oai_config"
        else:
            raise ValueError("protocol is required: choose 'oai' or 'claude'")
        nums = [0]
        if re.search(rf"^{prefix}\s*=", text, re.M):
            nums.append(0)
        nums.extend(int(m.group(1)) for m in re.finditer(rf"^{prefix}(\d+)\s*=", text, re.M))
        n = max(nums) + 1
        return prefix if n == 1 and not re.search(rf"^{prefix}\s*=", text, re.M) else f"{prefix}{n}"

    @staticmethod
    def _format_py_dict(d: dict) -> str:
        lines = [f"    '{k}': {json.dumps(v, ensure_ascii=False)}," if isinstance(v, str) else f"    '{k}': {v}," for k, v in d.items()]
        return "{\n" + "\n".join(lines) + "\n}"

    def _invalidate_mykey_cache(self) -> None:
        self.ensure_ga_import_path()
        sys.modules.pop("mykey", None)
        with contextlib.suppress(Exception):
            import llmcore
            llmcore._mykey_mtime = None

    def _profile_keys(self) -> List[str]:
        self.ensure_ga_import_path()
        from llmcore import reload_mykeys
        return [k for k in reload_mykeys()[0] if any(x in k for x in ("api", "config", "cookie"))]

    def _profile_at(self, profile_id: int) -> tuple[str, dict]:
        keys = self._profile_keys()
        if profile_id < 0 or profile_id >= len(keys):
            raise ValueError("profile not found")
        var = keys[profile_id]
        if "mixin" in var:
            raise ValueError("mixin profiles not supported here")
        from llmcore import reload_mykeys
        cfg = reload_mykeys()[0].get(var)
        if not isinstance(cfg, dict):
            raise ValueError("profile not editable")
        return var, dict(cfg)

    @staticmethod
    def _find_var_block_span(text: str, var_name: str) -> Optional[tuple[int, int]]:
        m = re.search(rf"^{re.escape(var_name)}\s*=\s*\{{", text, re.M)
        if not m:
            return None
        start, i, depth = m.start(), m.end() - 1, 0
        while i < len(text):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    while end < len(text) and text[end] in "\r\n":
                        end += 1
                    return start, end
            i += 1
        return None

    def _patch_var_block(self, text: str, var: str, cfg: Optional[dict] = None) -> str:
        if not (span := self._find_var_block_span(text, var)):
            raise ValueError(f"config block not found: {var}")
        s, e = span
        if cfg is None:
            return text[:s].rstrip() + "\n" + text[e:].lstrip("\n")
        return text[:s] + f"{var} = {self._format_py_dict(cfg)}\n" + text[e:]

    def _build_cfg(self, data: dict, existing: Optional[dict] = None, *, require_key: bool = True) -> dict:
        apibase, model = str(data.get("apibase") or "").strip(), str(data.get("model") or "").strip()
        if not apibase or not model:
            raise ValueError("apibase and model are required")
        apikey = str(data.get("apikey") or "").strip() or str((existing or {}).get("apikey") or "").strip()
        if require_key and not apikey:
            raise ValueError("apikey is required")
        # 从 existing 起步：保留表单未覆盖的高级字段（proxy / temperature / api_mode /
        # reasoning_effort / fake_cc_system_prompt / thinking_type …），避免 GUI 编辑时丢失
        cfg: Dict[str, Any] = dict(existing or {})
        cfg.update({"apikey": apikey, "apibase": apibase, "model": model})
        if "name" in data:
            name = str(data.get("name") or "").strip()
            if name:
                cfg["name"] = name
            else:
                cfg.pop("name", None)
        for k in ("max_retries", "connect_timeout", "read_timeout"):
            if data.get(k) is not None and str(data.get(k)).strip() != "":
                cfg[k] = int(data[k])
        # 流式开关：默认 True 不写（保持 mykey 干净），仅显式非流式才落 'stream': False
        if "stream" in data:
            s = data["stream"]
            stream = s if isinstance(s, bool) else str(s).strip().lower() not in ("false", "0", "no", "off")
            if stream:
                cfg.pop("stream", None)
            else:
                cfg["stream"] = False
        return cfg

    def _save_mykey_text(self, text: str) -> list:
        self._mykey_file().write_text(text, encoding="utf-8")
        self._invalidate_mykey_cache()
        self._reload_live_agents()
        return self.list_model_profiles()

    def _reload_live_agents(self) -> None:
        """mykey.py 改动后，强制所有活着的会话 agent 重建 LLM session，让新 key/模型
        立即生效（无需重启）。重建保留对话 history（agentmain 内部用 oldhistory 接回）。

        纯 bridge 侧实现，不改 agentmain：每次调 agent.load_llm_sessions() 前，把
        llmcore 的全局 mtime 标志清空（与 _invalidate_mykey_cache 同一手法），使其内部
        reload_mykeys() 报告 changed=True、从而真正重建——否则刷新模型列表等路径会先
        消费掉变更标志，常驻 agent 的 load_llm_sessions 会因 changed=False 跳过重建。"""
        self.ensure_ga_import_path()
        try:
            import llmcore
        except Exception:
            return
        with self.lock:
            agents = [s.agent for s in self.sessions.values() if getattr(s, "agent", None) is not None]
        for agent in agents:
            fn = getattr(agent, "load_llm_sessions", None)
            if not callable(fn):
                continue
            try:
                llmcore._mykey_mtime = None   # 让本次 reload_mykeys() 视为“已变更”，触发真正重建
                fn()
            except Exception as e:
                print(f"[bridge] reload live agent failed: {e}", file=sys.stderr)

    def add_model_profile(self, data: dict) -> dict:
        cfg = self._build_cfg(data)
        text = self._mykey_file().read_text(encoding="utf-8")
        var = self._next_native_var(text, data.get("protocol", ""))
        profiles = self._save_mykey_text(text.rstrip() + f"\n{var} = {self._format_py_dict(cfg)}\n")
        return {"varName": var, "profileId": profiles[-1]["id"] if profiles else 0, "profiles": profiles}

    def get_model_profile(self, profile_id: int) -> dict:
        var, cfg = self._profile_at(profile_id)
        ks = ("model", "apibase", "apikey", "name", "max_retries", "connect_timeout", "read_timeout")
        out = {"id": profile_id, "varName": var, **{k: cfg.get(k, d) for k, d in zip(ks, ("", "", "", "", 5, 15, 300))}}
        out["stream"] = cfg.get("stream", True)
        return out

    def update_model_profile(self, profile_id: int, data: dict) -> dict:
        var, existing = self._profile_at(profile_id)
        text = self._mykey_file().read_text(encoding="utf-8")
        profiles = self._save_mykey_text(self._patch_var_block(text, var, self._build_cfg(data, existing, require_key=False)))
        return {"varName": var, "profileId": profile_id, "profiles": profiles}

    def delete_model_profile(self, profile_id: int) -> dict:
        if len(self._profile_keys()) <= 1:
            raise ValueError("cannot delete the last profile")
        var, cfg = self._profile_at(profile_id)
        text = self._patch_var_block(self._mykey_file().read_text(encoding="utf-8"), var).rstrip() + "\n"
        # 顺手把它从聚合渠道里摘掉，避免 llm_nos 残留指向已删除的模型（会让 Mixin 构建失败）
        name = str(cfg.get("name") or cfg.get("model") or "").strip()
        keys, mk = self._mykey_vars()
        mvar, mcfg = self._mixin_entry(keys, mk)
        if mcfg and mvar is not None and name in [str(m) for m in (mcfg.get("llm_nos") or [])]:
            mcfg = {**mcfg, "llm_nos": [str(m) for m in (mcfg.get("llm_nos") or []) if str(m) != name]}
            if self._find_var_block_span(text, mvar):
                text = self._patch_var_block(text, mvar, mcfg)
        profiles = self._save_mykey_text(text)
        return {"profileId": profile_id, "profiles": profiles}

    def ensure_ga_import_path(self) -> Path:
        root = Path(self.ga_root).resolve()
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        return root

    def make_agent(self, sess: Session):
        root = self.ensure_ga_import_path()
        try: import cost_tracker; cost_tracker.install()
        except Exception: pass
        old_cwd = os.getcwd()
        try:
            os.chdir(sess.cwd or str(root))
            agentmain = importlib.import_module("agentmain")
            GA = getattr(agentmain, "GenericAgent")
            agent = GA()
            agent.inc_out = True
            agent.verbose = True
            # 桌面端 workspace 绑定在 sess.workspace(名字)，需把真实 path 同步到 agent，
            # 供 agentmain 每轮创建 handler 时设 handler.cwd（code_run/bash 执行目录）
            _ws_name = getattr(sess, 'workspace', '') or ''
            if _ws_name:
                try:
                    _ws_ent = workspace_cmd.registry_load().get(_ws_name) or {}
                    _ws_path = _ws_ent.get('path', '')
                    if _ws_path and os.path.isdir(_ws_path):
                        agent._ga_project_mode_workspace_path = _ws_path
                except Exception:
                    pass
            if sess.project:
                agent._ga_project_mode_name = sess.project
            threading.Thread(target=agent.run, daemon=True, name=f"GA-{sess.id}").start()
            return agent
        finally:
            with contextlib.suppress(Exception):
                os.chdir(old_cwd)

    @staticmethod
    def _base_display_name(var: str, cfg: Optional[dict]) -> str:
        c = cfg or {}
        return str(c.get("name") or c.get("model") or var)

    def _mykey_vars(self):
        """(keys, mk)：mykey 里的模型变量名（按定义顺序，与 agentmain.llmclients 索引
        一一对齐）和原始 dict。过滤规则与 _profile_keys / load_llm_sessions 完全一致，
        因此 id == llmclients 下标，前端选中 llmNo 能正确激活对应 client。"""
        self._mykey_file()   # 确保 mykey.py 存在（首次从模板生成空配置），否则全新安装时
                             # reload_mykeys 找不到 mykey 会返回空，空聚合渠道就不显示了
        self.ensure_ga_import_path()
        from llmcore import reload_mykeys
        mk = reload_mykeys()[0]
        keys = [k for k in mk if any(x in k for x in ("api", "config", "cookie"))]
        return keys, mk

    def _mixin_entry(self, keys, mk):
        """返回 (mixin_var, mixin_cfg_dict) 或 (None, None)。单一主聚合渠道，只取第一个。"""
        for k in keys:
            if "mixin" in k and isinstance(mk.get(k), dict):
                return k, dict(mk[k])
        return None, None

    def list_model_profiles(self):
        """直接读 mykey.py 结构（不依赖能否成功构建出 client），这样空聚合渠道、
        未填 key 的模型也能如实展示。聚合渠道(kind=mixin)带 members；基本模型
        (kind=native)带 inMixin/group。"""
        try:
            keys, mk = self._mykey_vars()
        except Exception as e:
            print(f"get model profiles failed: {e}", file=sys.stderr)
            return []
        active = self.config.get("llmNo", 0)
        # collect all mixin members for inMixin check
        all_mixin_members: set = set()
        for k in keys:
            if "mixin" in k:
                c = mk.get(k) if isinstance(mk.get(k), dict) else {}
                all_mixin_members.update(str(m) for m in (c.get("llm_nos") or []))
        out = []
        for i, k in enumerate(keys):
            cfg = mk.get(k) if isinstance(mk.get(k), dict) else {}
            if "mixin" in k:
                mems = [str(m) for m in (cfg.get("llm_nos") or [])]
                out.append({"id": i, "varName": k, "kind": "mixin", "name": "",
                            "members": mems, "active": i == active})
            else:
                name = self._base_display_name(k, cfg)
                out.append({"id": i, "varName": k, "kind": "native", "name": name,
                            "model": cfg.get("model", ""),
                            "group": "native" if "native" in k else "std",
                            "inMixin": name in all_mixin_members, "active": i == active})
        return out

    def add_to_mixin(self, profile_id: int) -> dict:
        """把一个基本模型加入主聚合渠道：把它的 name 追加进 mixin_config['llm_nos']。
        坑1：校验 Native 一致性（聚合内必须全 Native 或全非 Native）。
        坑2：加入前若该模型没有显式 name，先把 name 写进它的配置块（保证引用稳定）。"""
        var, cfg = self._profile_at(profile_id)   # 对 mixin 会抛错（只接受 native）
        name = str(cfg.get("name") or cfg.get("model") or "").strip()
        if not name:
            raise ValueError("this model needs a name or model before joining the channel")
        keys, mk = self._mykey_vars()
        mvar, mcfg = self._mixin_entry(keys, mk)
        new_is_native = "native" in var
        name2var = {self._base_display_name(k, mk.get(k) if isinstance(mk.get(k), dict) else {}): k
                    for k in keys if "mixin" not in k}
        existing = [str(m) for m in (mcfg.get("llm_nos") or [])] if mcfg else []
        for m in existing:
            mv = name2var.get(m)
            if mv is not None and ("native" in mv) != new_is_native:
                raise ValueError("aggregation channel requires all-Native or all-non-Native models")
        text = self._mykey_file().read_text(encoding="utf-8")
        if not cfg.get("name"):
            text = self._patch_var_block(text, var, {**cfg, "name": name})
        if mcfg is None:
            mcfg, mvar, existing = {"llm_nos": [], "max_retries": 10, "base_delay": 0.5}, "mixin_config", []
        if name not in existing:
            existing.append(name)
        mcfg = {**mcfg, "llm_nos": existing}
        if self._find_var_block_span(text, mvar):
            text = self._patch_var_block(text, mvar, mcfg)
        else:
            text = text.rstrip() + f"\n{mvar} = {self._format_py_dict(mcfg)}\n"
        return {"profiles": self._save_mykey_text(text)}

    def remove_from_mixin(self, profile_id: int) -> dict:
        """把一个基本模型移出主聚合渠道。"""
        var, cfg = self._profile_at(profile_id)
        name = str(cfg.get("name") or cfg.get("model") or "").strip()
        keys, mk = self._mykey_vars()
        mvar, mcfg = self._mixin_entry(keys, mk)
        if not mcfg or mvar is None:
            return {"profiles": self.list_model_profiles()}
        members = [str(m) for m in (mcfg.get("llm_nos") or []) if str(m) != name]
        mcfg = {**mcfg, "llm_nos": members}
        text = self._patch_var_block(self._mykey_file().read_text(encoding="utf-8"), mvar, mcfg)
        return {"profiles": self._save_mykey_text(text)}

    def reorder_mixin(self, members: list) -> dict:
        """按前端拖拽后的顺序重写主渠道组 llm_nos。只接受当前成员的重排，不增删。"""
        keys, mk = self._mykey_vars()
        mvar, mcfg = self._mixin_entry(keys, mk)
        if not mcfg or mvar is None:
            raise ValueError("mixin channel not found")
        old = [str(m) for m in (mcfg.get("llm_nos") or [])]
        new = [str(m) for m in (members or [])]
        if len(new) != len(old) or Counter(new) != Counter(old):
            raise ValueError("reorder must contain the same channel members")
        if new == old:
            return {"profiles": self.list_model_profiles()}
        mcfg = {**mcfg, "llm_nos": new}
        text = self._patch_var_block(self._mykey_file().read_text(encoding="utf-8"), mvar, mcfg)
        return {"profiles": self._save_mykey_text(text)}

    @staticmethod
    def _live_model(sess: Session) -> Optional[dict]:
        """该会话 agent 当前真正在用的模型（渠道组会随故障转移变化）。
        agent 还没建（没跑过 turn）时返回 None，前端回退到静态显示。"""
        ag = getattr(sess, "agent", None)
        if ag is None:
            return None
        try:
            back = ag.llmclient.backend
            if "Mixin" in type(back).__name__:
                return {"current": back.current_name, "isMixin": True}
            return {"current": back.name, "isMixin": False}
        except Exception:
            return None

    def _workspace_branch(self, ws_name: str) -> str:
        """Current git branch of a workspace (by registry name), or '' if not a git repo."""
        try:
            if not ws_name:
                return ""
            entry = workspace_cmd.registry_load().get(ws_name)
            path = entry.get("path") if entry else None
            if not path:
                return ""
            r = subprocess.run(
                ["git", "-C", path, "symbolic-ref", "--short", "HEAD"],
                capture_output=True, text=True, timeout=2)
            if r.returncode == 0:
                return r.stdout.strip()
            r2 = subprocess.run(
                ["git", "-C", path, "rev-parse", "--abbrev-ref", "HEAD"],
                capture_output=True, text=True, timeout=2)
            return r2.stdout.strip() if r2.returncode == 0 else ""
        except Exception:
            return ""

    def snapshot(self, sess: Session, include_messages: bool = True) -> dict:
        out = {
            "sessionId": sess.id,
            "id": sess.id,
            "title": sess.title,
            "cwd": sess.cwd,
            "status": sess.status,
            "createdAt": sess.created_at,
            "updatedAt": sess.updated_at,
            "lastError": sess.last_error,
            "msgSeq": sess.msg_seq,
            "pinned": sess.pinned,
            "untitled": sess.untitled,
            "model": self._live_model(sess),
            "workspace": sess.workspace or "",
            "project": sess.project or "",
            "branch": self._workspace_branch(sess.workspace) if sess.workspace else "",
        }
        if include_messages:
            out["messages"] = list(sess.messages)
            out["partial"] = dict(sess.partial) if sess.partial else None
        return out

    def add_message(self, sess: Session, role: str, content: str, **extra) -> dict:
        sess.msg_seq += 1
        msg = {"id": sess.msg_seq, "role": role, "content": content, "ts": time.time()}
        msg.update(extra)
        sess.messages.append(msg)
        sess.updated_at = time.time()
        if role == "user" and content.strip() and sess.title == "New chat":
            sess.title = content.strip().replace("\n", " ")[:40]
        self._persist()
        return msg

    def create_session(self, cwd: Optional[str] = None, project: Optional[str] = None) -> Session:
        sid = "sess-" + uuid.uuid4().hex[:12]
        if not cwd:
            cwd = str(resolve_chat_files_dir(self.ga_root))
        sess = Session(id=sid, cwd=str(cwd or self.ga_root), project=project or "")
        with self.lock:
            self.sessions[sid] = sess
            self.active_session_id = sid
        emit_session_state(sess, "created")
        self._persist()
        return sess

    def get_session(self, sid: str) -> Session:
        with self.lock:
            sess = self.sessions.get(sid)
            if not sess:
                raise web.HTTPNotFound(text=json.dumps({"error": f"session not found: {sid}"}, ensure_ascii=False), content_type="application/json")
            return sess

    def delete_session(self, sid: str) -> dict:
        with self.lock:
            sess = self.sessions.pop(sid, None)
            if not sess:
                raise web.HTTPNotFound(text=json.dumps({"error": f"session not found: {sid}"}, ensure_ascii=False), content_type="application/json")
            if self.active_session_id == sid:
                self.active_session_id = next(iter(self.sessions), None)
            if sess.agent and hasattr(sess.agent, "abort"):
                with contextlib.suppress(Exception):
                    sess.agent.abort()
        emit_session_state(sess, "closed")
        self._persist()
        _purge_session_uploads(sid)
        return {"ok": True, "sessionId": sid}

    def submit_prompt(self, sid: str, prompt: Any, images: Optional[list] = None, llm_no: Optional[int] = None, display: Optional[str] = None, files_meta: Optional[list] = None, image_metas: Optional[list] = None) -> dict:
        prompt, image_ids = normalize_prompt(prompt, images)
        if llm_no is not None:
            self.config["llmNo"] = int(llm_no)
        with self.lock:
            sess = self.sessions.get(sid)
            if not sess:
                raise web.HTTPNotFound(text=json.dumps({"error": f"session not found: {sid}"}, ensure_ascii=False), content_type="application/json")
            if sess.status == "running":
                raise web.HTTPConflict(text=json.dumps({"error": "session is already running"}, ensure_ascii=False), content_type="application/json")
            extra = {}
            if image_ids:
                extra["image_ids"] = image_ids
            if isinstance(display, str) and display.strip() and display != prompt:
                extra["display"] = display
            if files_meta:
                extra["files"] = files_meta
            if image_metas:
                extra["images"] = image_metas
            user_msg = self.add_message(sess, "user", prompt, **extra)
            import plan_state
            if plan_state.is_plan_preset_prompt(prompt):
                plan_state.bind_plan_session(sess, prompt)
                self._persist()
            sess.status = "running"
            sess.cancel_requested = False
            sess.last_error = ""
            sess.partial = {"id": sess.msg_seq + 1, "role": "assistant", "content": "", "ts": time.time(), "partial": True,
                            "curr_turn": 0, "turn_segs": []}  # turn_segs[i]=第i轮全文(权威结构化,前端按轮渲染);content保留双轨兜底
            t = threading.Thread(target=self.run_agent_turn, args=(sess, prompt, None, llm_no), daemon=True, name=f"Turn-{sid}")
            sess.thread = t
            t.start()
            seq = sess.msg_seq
        emit_session_state(sess, "running")
        return {"ok": True, "sessionId": sid, "accepted": True, "userMessageId": user_msg["id"], "seq": seq}

    def run_agent_turn(self, sess: Session, prompt: str, images: Optional[list] = None, llm_no: Optional[int] = None):
        try:
            if sess.agent is None:
                sess.agent = self.make_agent(sess)
            agent = sess.agent
            no = self.config.get("llmNo") if llm_no is None else llm_no
            if no is not None and hasattr(agent, "next_llm"):
                with contextlib.suppress(Exception):
                    agent.next_llm(int(no))
            full = ""
            done_outputs = None  # done时agent给的全量轮文本(turn_resps.copy())
            if hasattr(agent, "put_task"):
                display_q = agent.put_task(prompt, images=images or [])
                pieces = []
                import queue as _queue
                while True:
                    if sess.cancel_requested:
                        break
                    try:
                        item = display_q.get(timeout=1.0)
                    except _queue.Empty:
                        # heartbeat: update ts so frontend knows agent is still alive
                        with self.lock:
                            if sess.partial is not None:
                                sess.partial["ts"] = time.time()
                                sess.updated_at = time.time()
                        continue
                    if isinstance(item, dict):
                        if item.get("next"):
                            text = str(item["next"])
                            pieces.append(text)
                            with self.lock:
                                if sess.partial is not None:
                                    sess.partial["content"] = "".join(pieces) if getattr(agent, "inc_out", False) else text
                                    sess.partial["ts"] = time.time()
                                    sess.updated_at = time.time()
                                    # 轨道2: bridge 归一化为前端直接可渲染的 0 基 turn_segs；outputs=turn_resps[-2:]
                                    _t = int(item.get("turn", 0) or 0)
                                    _outs = item.get("outputs") or []
                                    _idx = max(0, _t - 1)
                                    sess.partial["curr_turn"] = _idx
                                    _segs = sess.partial["turn_segs"]
                                    while len(_segs) <= _idx:
                                        _segs.append("")
                                    if _outs:
                                        _segs[_idx] = str(_outs[-1])
                                        if len(_outs) >= 2 and _idx >= 1:
                                            _segs[_idx - 1] = str(_outs[-2])
                        if "done" in item:
                            full = strip_final_info_marker(item.get("done") or "")
                            done_outputs = item.get("outputs")  # done时=turn_resps.copy()全量轮
                            if done_outputs:
                                done_outputs = [strip_final_info_marker(s) for s in done_outputs]
                                with self.lock:
                                    if sess.partial is not None:
                                        sess.partial["content"] = full
                                        sess.partial["ts"] = time.time()
                                        sess.partial["updatedAt"] = sess.partial["ts"] if "updatedAt" in sess.partial else sess.partial.get("updatedAt")
                                        sess.partial["curr_turn"] = max(0, len(done_outputs) - 1)
                                        sess.partial["turn_segs"] = list(done_outputs)
                                        sess.updated_at = time.time()
                            break
                    else:
                        pieces.append(str(item))
                if not full and pieces:
                    full = pieces[-1] if not getattr(agent, "inc_out", False) else "".join(pieces)
            else:
                full = "GenericAgent object has no put_task method"
            if not full:
                full = "(completed)"
            if sess.cancel_requested:
                with self.lock:
                    sess.partial = None
                    # Ensure status stays cancelled (don't overwrite)
                    if sess.status != "cancelled":
                        sess.status = "cancelled"
                    sess.updated_at = time.time()
                emit_session_state(sess, "cancelled")
                return
            with self.lock:
                sess.partial = None
                full = strip_final_info_marker(full)
                if done_outputs:
                    done_outputs = [strip_final_info_marker(s) for s in done_outputs]
                import plan_state
                plan_state.sync_plan_path_from_text(sess, full, sess.cwd or self.ga_root)
                # 轨道2: 落库时带结构化全量轮(权威turn_segs),前端按轮渲染;content保留兜底
                _final_segs = [str(s) for s in done_outputs] if done_outputs else None
                if _final_segs:
                    self.add_message(sess, "assistant", full, turn_segs=_final_segs)
                else:
                    self.add_message(sess, "assistant", full)
                try: sess.llm_history = json.loads(json.dumps(agent.llmclient.backend.history, ensure_ascii=False, default=str))
                except Exception: pass
                sess.status = "idle"
                sess.last_error = ""
            emit_session_state(sess, "idle")
        except Exception as e:
            tb = traceback.format_exc()
            with self.lock:
                sess.partial = None
                sess.status = "error"
                sess.last_error = str(e)
                self.add_message(sess, "error", str(e))
            print(tb, file=sys.stderr)
            emit_session_state(sess, "error")

    def messages(self, sid: str, after: int = 0, limit: int = 200) -> dict:
        with self.lock:
            sess = self.sessions.get(sid)
            if not sess:
                raise web.HTTPNotFound(text=json.dumps({"error": f"session not found: {sid}"}, ensure_ascii=False), content_type="application/json")
            msgs = [m for m in sess.messages if int(m.get("id", 0)) > after]
            if limit > 0:
                msgs = msgs[-limit:]
            import plan_state
            return {
                "sessionId": sid,
                "status": sess.status,
                "messages": msgs,
                "partial": dict(sess.partial) if sess.partial else None,
                "plan": plan_state.desktop_plan_payload_from_session(sess, self.ga_root),
                "msgSeq": sess.msg_seq,
                "updatedAt": sess.updated_at,
                "lastError": sess.last_error,
                "model": self._live_model(sess),
            }

    def plan_snapshot(self, sid: str) -> dict:
        with self.lock:
            sess = self.sessions.get(sid)
            if not sess:
                raise web.HTTPNotFound(text=json.dumps({"error": f"session not found: {sid}"}, ensure_ascii=False), content_type="application/json")
            import plan_state
            return {
                "sessionId": sid,
                "plan": plan_state.desktop_plan_payload_from_session(sess, self.ga_root),
            }

    def suggest(self, sid: str) -> dict:
        """根据最近对话生成 1-3 条下一步建议。在锁内取 agent 引用，锁外执行 LLM 调用。"""
        with self.lock:
            sess = self.sessions.get(sid)
            if not sess:
                raise web.HTTPNotFound(text=json.dumps({"error": f"session not found: {sid}"}, ensure_ascii=False), content_type="application/json")
            agent = getattr(sess, "agent", None)
            recent = [m for m in sess.messages[-12:] if m.get("role") in ("user", "assistant")]
        # 会话结束后 agent 可能已被释放（从磁盘加载的会话 agent=None）。
        # 临时创建一个 agent 用于生成建议，复用 restore_context 的 history 注入逻辑。
        if not agent or not getattr(agent, "llmclient", None):
            try:
                agent = self.make_agent(sess)
                if sess.llm_history:
                    try: agent.llmclient.backend.history = sess.llm_history
                    except Exception: pass
                else:
                    history = []
                    for m in sess.messages:
                        role = m.get("role"); content = m.get("content", "")
                        if role == "user":
                            history.append({"role": "user", "content": [{"type": "text", "text": content}]})
                        elif role == "assistant":
                            history.append({"role": "assistant", "content": [{"type": "text", "text": content}]})
                    if history:
                        try: agent.llmclient.backend.history = history
                        except Exception: pass
                sess.agent = agent
            except Exception as e:
                return {"suggestions": [], "error": f"failed to create agent: {e}"}
        if len(recent) < 2:
            return {"suggestions": []}
        # 构建最近对话摘要
        conv_snippets = []
        for m in recent:
            role = "用户" if m["role"] == "user" else "助手"
            text = ""
            content = m.get("content", "")
            if isinstance(content, str):
                text = content
            elif isinstance(content, list):
                text = " ".join(b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text")
            text = text.strip()[:600]
            if text:
                conv_snippets.append(f"{role}: {text}")
        conv = "\n".join(conv_snippets[-8:])
        if not conv.strip():
            return {"suggestions": []}
        sys_prompt = (
            "你是对话助手。根据以下最近对话，推测用户接下来可能想问的 1-3 个问题或指令。"
            "要求：简短自然（中文，每条不超过 20 字）、与当前对话相关、是用户可能直接想做的事。"
            "只输出建议，每行一条，不要编号，不要多余解释。\n\n"
            f"最近对话:\n{conv}\n\n建议:"
        )
        try:
            client = agent.llmclient
            backend = client.backend
            saved_history = list(getattr(backend, "history", []))
            gen = client.chat([{"role": "user", "content": sys_prompt}])
            resp = None
            try:
                while True:
                    next(gen)
            except StopIteration as e:
                resp = e.value
            result_text = (getattr(resp, "content", "") or "").strip()
        except Exception as e:
            import traceback as _tb
            try: open("/tmp/suggest_debug.log","a").write("[ask] "+_tb.format_exc()+"\n\n")
            except Exception: pass
            return {"suggestions": [], "error": str(e)}
        finally:
            try: backend.history = saved_history
            except Exception: pass
        suggestions = [s.strip().lstrip("0123456789.-、） )").strip() for s in result_text.splitlines()]
        suggestions = [s for s in suggestions if s and len(s) <= 60][:3]
        return {"suggestions": suggestions}

    def cancel(self, sid: str) -> dict:
        with self.lock:
            sess = self.sessions.get(sid)
            if not sess:
                raise web.HTTPNotFound(text=json.dumps({"error": f"session not found: {sid}"}, ensure_ascii=False), content_type="application/json")
            sess.cancel_requested = True
            if sess.agent and hasattr(sess.agent, "abort"):
                with contextlib.suppress(Exception):
                    sess.agent.abort()
            partial_text = ""
            if sess.partial:
                partial_text = (sess.partial.get("content") or "").strip()
            if partial_text:
                self.add_message(sess, "assistant", partial_text, stopped=True)
            sess.status = "cancelled"
            sess.partial = None
            sess.updated_at = time.time()
        emit_session_state(sess, "cancelled")
        return {"ok": True, "sessionId": sid}

    def restore_context(self, sid: str) -> dict:
        with self.lock:
            sess = self.sessions.get(sid)
            if not sess:
                raise web.HTTPNotFound(text=json.dumps({"error": f"session not found: {sid}"}, ensure_ascii=False), content_type="application/json")
            if sess.agent is not None:
                return {"ok": True, "sessionId": sid, "restored": False, "reason": "agent already alive"}
        agent = self.make_agent(sess)
        if sess.llm_history:
            try:
                agent.llmclient.backend.history = sess.llm_history
            except Exception as e:
                print(f"[bridge] restore llm_history failed: {e}", file=sys.stderr)
        else:
            history = []
            for m in sess.messages:
                role = m.get("role")
                content = m.get("content", "")
                if role == "user":
                    history.append({"role": "user", "content": [{"type": "text", "text": content}]})
                elif role == "assistant":
                    history.append({"role": "assistant", "content": [{"type": "text", "text": content}]})
            if history:
                try:
                    agent.llmclient.backend.history = history
                except Exception as e:
                    print(f"[bridge] inject history failed: {e}", file=sys.stderr)
        with self.lock:
            sess.agent = agent
            sess.status = "idle"
        return {"ok": True, "sessionId": sid, "restored": True, "messageCount": len(sess.llm_history or sess.messages)}


import base64


def normalize_prompt(prompt: Any, images: Optional[list] = None):
    """Flatten a prompt (str or content-part list) to plain text.

    Image/file attachments are handled by the frontend, which inlines the
    uploaded file path into the prompt text (see expandFilePlaceholders) and
    sends path-only metadata via files/imageMetas — so no per-prompt image
    persistence happens here. The `images` arg is accepted for backward compat
    and ignored; the returned image-id list is always empty.
    """
    if isinstance(prompt, list):
        text_parts = []
        for part in prompt:
            if isinstance(part, str):
                text_parts.append(part)
            elif isinstance(part, dict) and part.get("type") in ("text", "input_text"):
                text_parts.append(str(part.get("text") or part.get("content") or ""))
        prompt = "\n".join([p for p in text_parts if p])

    return str(prompt or ""), []


manager = AgentManager()


# ---------------------------------------------------------------------------
# Transport layer: WS state push
# ---------------------------------------------------------------------------

class WsHub:
    def __init__(self):
        self.websockets: Set[web.WebSocketResponse] = set()
        self.loop: Optional[asyncio.AbstractEventLoop] = None

    def emit(self, obj: dict):
        if self.loop and self.loop.is_running():
            asyncio.run_coroutine_threadsafe(self._broadcast(obj), self.loop)

    async def _broadcast(self, obj: dict):
        data = json.dumps(obj, ensure_ascii=False, default=str)
        dead = set()
        for ws in list(self.websockets):
            try:
                await ws.send_str(data)
            except Exception:
                dead.add(ws)
        self.websockets.difference_update(dead)


hub = WsHub()


# ---------------------------------------------------------------------------
# Service management (hub.pyw core + WS notify)
# ---------------------------------------------------------------------------

_SKIP = frozenset({"goal_mode.py", "chatapp_common.py", "tuiapp.py", "qtapp.py"})
BRIDGE_ID = "__bridge__"

_SERVICE_KEYS: Dict[str, tuple] = {
    "frontends/qqapp.py": ("qq_app_id", "qq_app_secret"),
    "frontends/dcapp.py": ("discord_bot_token",),
    "frontends/dingtalkapp.py": ("dingtalk_client_id", "dingtalk_client_secret"),
    "frontends/fsapp.py": ("fs_app_id", "fs_app_secret"),
    "frontends/tgapp.py": ("tg_bot_token",),
    "frontends/wecomapp.py": ("wecom_bot_id", "wecom_secret"),
}

# 服务 -> 单例/监听端口映射。bridge 重启后, 上次启动的子进程会变成孤儿
# (PPID=1) 仍占着 bind 端口, 导致新实例 bind 失败 (Errno 48 / 单例锁冲突),
# watchdog 重试 5 次耗尽后放弃。start_service 前用此映射清理孤儿。
_SERVICE_PORTS: Dict[str, int] = {
    "frontends/wechatapp.py": 19531,    # socket 单例锁
    "frontends/wecomapp.py": 19531,     # socket 单例锁 (与微信共用)
    "frontends/conductor.py": 8900,     # uvicorn 监听
    "reflect/scheduler.py": 45762,      # socket 单例锁 (agentmain --reflect)
}


def _load_mykeys(ga_root: Path) -> dict:
    if not (ga_root / "mykey.py").exists():
        return {}
    root = str(ga_root.resolve())
    if root not in sys.path:
        sys.path.insert(0, root)
    import mykey as mk
    importlib.reload(mk)
    return {k: v for k, v in vars(mk).items() if not k.startswith("_")}


def discover_im_services(ga_root: Path) -> List[dict]:
    out: List[dict] = []
    d = ga_root / "frontends"
    if not d.is_dir():
        return out
    for f in sorted(os.listdir(d)):
        if "app" not in f or not f.endswith(".py") or f in _SKIP or "stapp" in f or "tuiapp" in f:
            continue
        rel = f"frontends/{f}"
        out.append({"id": rel, "cmd": [sys.executable, str(d / f)]})
    return out


def discover_extra_services(ga_root: Path) -> List[dict]:
    out: List[dict] = []
    sched = ga_root / "reflect" / "scheduler.py"
    if sched.is_file():
        out.append({
            "id": "reflect/scheduler.py",
            "cmd": [sys.executable, "agentmain.py", "--reflect", "reflect/scheduler.py"],
        })
    # conductor 跟 scheduler 一样,bridge 启动时自动拉起。--no-browser 是关键:
    # conductor.py 默认会用 webbrowser.open 在用户浏览器弹一个 8900 端口 UI,
    # 桌面版自启时不需要这个独立 UI(用户从「指挥家」页直接访问)。
    conductor = ga_root / "frontends" / "conductor.py"
    if conductor.is_file():
        out.append({
            "id": "frontends/conductor.py",
            "cmd": [sys.executable, "frontends/conductor.py", "--no-browser"],
        })
    return out


def _mem_mb(pid: Optional[int]) -> Optional[int]:
    if not pid:
        return None
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes
        class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t), ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t),
            ]
        counters = PROCESS_MEMORY_COUNTERS()
        counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
        h = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
        if not h:
            return None
        ok = ctypes.windll.psapi.GetProcessMemoryInfo(h, ctypes.byref(counters), counters.cb)
        ctypes.windll.kernel32.CloseHandle(h)
        return round(counters.WorkingSetSize / 1024 / 1024) if ok else None
    status = Path(f"/proc/{pid}/status")
    if status.is_file():
        for line in status.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("VmRSS:"):
                return round(int(line.split()[1]) / 1024)
    return None


def _cpu_pct(pid: Optional[int]) -> Optional[float]:
    if not pid:
        return None
    try:
        import psutil
        return round(psutil.Process(pid).cpu_percent(0) or 0, 1)
    except Exception:
        return None


class ServiceManager:
    """hub.pyw ServiceManager + HTTP/WS glue."""

    def __init__(self, ga_root: str, emit_fn):
        self.ga_root = Path(ga_root)
        self.procs: Dict[str, subprocess.Popen] = {}
        self.buffers: Dict[str, deque] = {}
        self._emit = emit_fn
        im = discover_im_services(self.ga_root)
        extra = discover_extra_services(self.ga_root)
        self._im_catalog = {s["id"]: s for s in im}
        self._catalog = {**self._im_catalog, **{s["id"]: s for s in extra}}
        self._stopping: Set[str] = set()
        # watchdog: 进程异常退出后自动重启
        self._restart_counts: Dict[str, int] = {}
        self._watchdog_max: int = 5          # 最多重试次数
        self._watchdog_base: float = 5.0     # 初始退避秒数
        self._watchdog_cap: float = 60.0     # 最大退避秒数
        # 持久化用户启用的IM通道, 重启后自动恢复
        self._autostart_path: Path = Path.home() / ".ga_services_autostart.json"
        self._autostart_set: Set[str] = self._load_autostart()

    def _load_autostart(self) -> Set[str]:
        try:
            import json as _json
            data = _json.loads(self._autostart_path.read_text("utf-8"))
            return {s for s in data.get("im_services", []) if s in self._im_catalog}
        except Exception:
            return set()

    def _save_autostart(self) -> None:
        try:
            import json as _json
            self._autostart_path.write_text(
                _json.dumps({"im_services": sorted(self._autostart_set)}, ensure_ascii=False, indent=2),
                "utf-8")
        except Exception:
            pass

    def _is_configured(self, sid: str) -> bool:
        keys = _SERVICE_KEYS.get(sid)
        if not keys:
            return True
        mykeys = _load_mykeys(self.ga_root)
        return all(str(mykeys.get(k) or "").strip() for k in keys)

    def _log_tail(self, sid: str, n: int = 3) -> str:
        buf = self.buffers.get(sid)
        if not buf:
            return ""
        lines = [ln.strip() for ln in list(buf)[-n:] if ln.strip()]
        return lines[-1][:300] if lines else ""

    def _state(self, sid: str, *, err: str = "") -> dict:
        proc = self.procs.get(sid)
        running = proc is not None and proc.poll() is None
        status = "running" if running else "offline"
        last_error = err
        if proc is not None and proc.poll() is not None:
            if sid in self._stopping:
                status, last_error = "offline", ""
            else:
                status = "error"
                last_error = err or self._log_tail(sid) or f"exit code {proc.returncode}"
        elif err:
            status, running = "error", False
        return {
            "id": sid,
            "status": status,
            "running": running,
            "pid": proc.pid if running else None,
            "lastError": last_error,
        }

    def list_state(self) -> List[dict]:
        return [self._state(sid) for sid in sorted(self._im_catalog)]

    def _bridge_state(self) -> dict:
        pid = os.getpid()
        port = int(os.environ.get("BRIDGE_PORT", "14168"))
        return {
            "id": BRIDGE_ID,
            "name": f"bridge (:{port})",
            "status": "running",
            "running": True,
            "pid": pid,
            "memMb": _mem_mb(pid),
            "cpuPct": _cpu_pct(pid),
            "managed": False,
            "lastError": "",
        }

    def list_panel_state(self) -> List[dict]:
        out = [self._bridge_state()]
        for sid in sorted(self._catalog, key=lambda s: (s in self._im_catalog, s)):
            item = self._state(sid)
            item["name"] = sid
            item["memMb"] = _mem_mb(item.get("pid"))
            item["cpuPct"] = _cpu_pct(item.get("pid"))
            item["managed"] = True
            out.append(item)
        return out

    def _notify(self, sid: str, *, err: str = "") -> None:
        self._emit({"type": "service.changed", "service": self._state(sid, err=err)})

    def _wait_started(self, proc: subprocess.Popen, timeout: float = 2.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if proc.poll() is not None:
                return
            time.sleep(0.1)

    def _reader(self, sid: str, proc: subprocess.Popen) -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            buf = self.buffers.get(sid)
            if buf is not None:
                buf.append(line)
        # 进程已退出: 主动stop不重启, 异常退出则触发watchdog
        if sid in self._stopping:
            self._notify(sid)
        elif proc.returncode == 0:
            self._notify(sid)           # 正常退出, 不重启
        else:
            self._notify(sid)
            threading.Thread(target=self._watchdog, args=(sid,), daemon=True).start()

    def _watchdog(self, sid: str) -> None:
        """进程异常退出后自动重启, 指数退避, 最多 _watchdog_max 次。"""
        count = self._restart_counts.get(sid, 0)
        if count >= self._watchdog_max:
            err = f"watchdog: 已达最大重试次数({self._watchdog_max}), 放弃自动重启"
            buf = self.buffers.get(sid)
            if buf is not None:
                buf.append(err + "\n")
            self._notify(sid, err=err)
            return
        delay = min(self._watchdog_base * (2 ** count), self._watchdog_cap)
        count += 1
        self._restart_counts[sid] = count
        msg = f"[watchdog] {sid} 异常退出(returncode见上), {delay:.0f}s 后第 {count}/{self._watchdog_max} 次自动重启..."
        buf = self.buffers.get(sid)
        if buf is not None:
            buf.append(msg + "\n")
        self._notify(sid)
        time.sleep(delay)
        # 再次检查: 用户可能在退避期间手动stop了
        if sid in self._stopping:
            return
        # 清理旧进程引用, 再启动 (reset_count=False: watchdog重启保留计数)
        self.procs.pop(sid, None)
        try:
            res = self.start_service(sid, reset_count=False)
            if res.get("ok"):
                # 启动成功, 但不立即重置计数 — 若稳定运行足够久由外部重置
                ok_msg = f"[watchdog] {sid} 第 {count} 次重启成功 (pid={res.get('service', {}).get('pid')})"
                buf = self.buffers.get(sid)
                if buf is not None:
                    buf.append(ok_msg + "\n")
            else:
                # 启动失败, 递归继续watchdog (count已+1)
                threading.Thread(target=self._watchdog, args=(sid,), daemon=True).start()
        except Exception as e:
            err = f"[watchdog] {sid} 重启异常: {e}"
            buf = self.buffers.get(sid)
            if buf is not None:
                buf.append(err + "\n")
            self._notify(sid, err=err)

    def _reap_orphan(self, sid: str) -> None:
        """启动新实例前, 清理占用该服务端口但不属于本 bridge 追踪的孤儿进程。

        场景: bridge 重启后, 上次启动的子进程变成孤儿 (PPID=1) 仍占着 bind 端口,
        导致新实例 bind 失败。lsof 查端口占用者, 若 PID 不在本 bridge 追踪的
        self.procs 存活进程里 (即孤儿), 则 kill 掉。无端口映射的服务直接跳过。
        """
        port = _SERVICE_PORTS.get(sid)
        if not port:
            return
        try:
            out = subprocess.run(
                ["lsof", "-ti", f":{port}"],
                capture_output=True, text=True, timeout=3,
            )
        except Exception:
            return  # lsof 不可用则跳过, 不阻塞启动
        # 本 bridge 当前追踪的存活 PID 集合 (新实例尚未启动, 通常为空或已退出的旧引用)
        tracked = {p.pid for p in self.procs.values() if p.poll() is None}
        own_pid = os.getpid()
        for line in out.stdout.split():
            line = line.strip()
            if not line:
                continue
            try:
                pid = int(line)
            except ValueError:
                continue
            if pid == own_pid or pid in tracked:
                continue  # 本 bridge 自己 / 本 bridge 追踪的进程, 不动
            # 孤儿进程, kill 掉
            try:
                os.kill(pid, 9)
                buf = self.buffers.get(sid)
                if buf is not None:
                    buf.append(f"[bridge] 清理占用端口 {port} 的孤儿进程 pid={pid}\n")
            except (ProcessLookupError, PermissionError):
                pass
        # 给内核一点时间回收端口
        time.sleep(0.3)

    def start_service(self, sid: str, reset_count: bool = True) -> dict:
        svc = self._catalog.get(sid)
        if not svc:
            raise KeyError(sid)
        proc = self.procs.get(sid)
        if proc is not None and proc.poll() is None:
            return {"ok": True, "service": self._state(sid)}
        if not self._is_configured(sid):
            keys = ", ".join(_SERVICE_KEYS.get(sid, ()))
            err = f"not configured in mykey.py ({keys})"
            self._notify(sid, err=err)
            return {"ok": False, "error": "not_configured", "service": self._state(sid, err=err)}
        # 手动启动时重置watchdog计数; watchdog内部调用(reset_count=False)保留计数
        if reset_count:
            self._restart_counts[sid] = 0
        self.buffers[sid] = deque(maxlen=500)
        env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}
        kw: Dict[str, Any] = dict(
            cwd=str(self.ga_root), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace", bufsize=1, env=env,
        )
        if sys.platform == "win32":
            kw["creationflags"] = subprocess.CREATE_NO_WINDOW
        # 启动新实例前清理占用端口的孤儿进程 (bridge 重启后遗留的子进程)
        self._reap_orphan(sid)
        proc = subprocess.Popen(svc["cmd"], **kw)
        self.procs[sid] = proc
        threading.Thread(target=self._reader, args=(sid, proc), daemon=True).start()
        self._wait_started(proc)
        item = self._state(sid)
        self._notify(sid)
        if item["status"] == "error":
            return {"ok": False, "error": item["lastError"] or "start_failed", "service": item}
        # 持久化: 用户手动启用的IM通道记录下来, 重启后自动恢复
        if sid in self._im_catalog and sid not in self._autostart_set:
            self._autostart_set.add(sid)
            self._save_autostart()
        return {"ok": True, "service": item}

    def autostart_extras(self) -> None:
        """Auto-start non-IM services on bridge boot. Currently:
          - reflect/scheduler.py (drives L4 archive cron every 12h).
        IM services stay manual (need explicit mykey.py config + user opt-in),
        but user-enabled IM services are persisted and auto-restored here."""
        for sid in sorted(set(self._catalog) - set(self._im_catalog)):
            try:
                res = self.start_service(sid)
                tag = "ok" if res.get("ok") else f"fail: {res.get('error')}"
            except Exception as e:
                tag = f"exception {type(e).__name__}: {e}"
            print(f"[autostart] {sid}: {tag}", file=sys.stderr)
        # 恢复用户重启前启用的IM通道
        for sid in sorted(self._autostart_set):
            try:
                res = self.start_service(sid)
                tag = "ok" if res.get("ok") else f"fail: {res.get('error')}"
            except Exception as e:
                tag = f"exception {type(e).__name__}: {e}"
            print(f"[autostart-im] {sid}: {tag}", file=sys.stderr)

    def stop_all_extras(self) -> None:
        for sid in sorted(set(self._catalog) - set(self._im_catalog)):
            with contextlib.suppress(Exception):
                self.stop_service(sid)

    def stop_service(self, sid: str) -> dict:
        if sid not in self._catalog:
            raise KeyError(sid)
        self._stopping.add(sid)
        proc = self.procs.get(sid)
        if proc and proc.poll() is None:
            proc.terminate()
            proc.wait()
        self.procs.pop(sid, None)
        self._stopping.discard(sid)
        self._restart_counts.pop(sid, None)  # 主动stop清零watchdog计数
        # 用户主动停止 → 从持久化中移除, 重启后不再自动启动
        if sid in self._autostart_set:
            self._autostart_set.discard(sid)
            self._save_autostart()
        item = self._state(sid)
        self._notify(sid)
        return {"ok": True, "service": item}

    def read_logs(self, sid: str, tail: int = 200) -> dict:
        if sid == BRIDGE_ID:
            return {"ok": True, "lines": [f"GenericAgent bridge pid={os.getpid()}"]}
        if sid not in self._catalog:
            raise KeyError(sid)
        tail = max(1, min(int(tail or 200), 2000))
        buf = self.buffers.get(sid)
        lines = [ln.rstrip("\n") for ln in list(buf or [])[-tail:]]
        return {"ok": True, "lines": lines}


services = ServiceManager(str(DEFAULT_GA_ROOT), hub.emit)


def _bridge_shutdown_services() -> None:
    with contextlib.suppress(Exception):
        services.stop_all_extras()


atexit.register(_bridge_shutdown_services)


def emit_session_state(sess: Session, state_name: str):
    hub.emit({
        "type": "session-state",
        "sessionId": sess.id,
        "state": state_name,
        "status": sess.status,
        "seq": sess.msg_seq,
        "updatedAt": sess.updated_at,
        "title": sess.title,
    })


async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    hub.websockets.add(ws)
    await ws.send_str(json.dumps({
        "type": "bridge-ready",
        "gaRoot": manager.ga_root,
        "mykeyPath": manager.mykey_path,
        "http": True,
        "wsEventsOnly": True,
    }, ensure_ascii=False))
    await ws.send_str(json.dumps({
        "type": "services.snapshot",
        "services": services.list_state(),
    }, ensure_ascii=False, default=str))
    async for msg in ws:
        if msg.type == WSMsgType.TEXT:
            # WS is intentionally not a data/command channel anymore.
            with contextlib.suppress(Exception):
                data = json.loads(msg.data)
                if data.get("action") == "ping":
                    await ws.send_str(json.dumps({"type": "pong", "ts": time.time()}, ensure_ascii=False))
    hub.websockets.discard(ws)
    return ws


# ---------------------------------------------------------------------------
# Transport layer: HTTP command/data API
# ---------------------------------------------------------------------------

def cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    }


@web.middleware
async def cors_middleware(request, handler):
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=cors_headers())
    resp = await handler(request)
    for k, v in cors_headers().items():
        resp.headers[k] = v
    return resp


def json_ok(data: dict, status: int = 200):
    return web.json_response(data, status=status, headers=cors_headers(), dumps=lambda x: json.dumps(x, ensure_ascii=False, default=str))


async def read_json(request) -> dict:
    if request.can_read_body:
        try:
            data = await request.json()
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    return {}


def webhook_base(request) -> str:
    """webhook URL 的 host 部分。优先用环境变量 WEBHOOK_BASE_URL（对外可达地址，
    如 http://10.23.230.9:14168），否则回退 request.host（本机访问时是 localhost）。
    对外暴露(BRIDGE_HOST=0.0.0.0)时必须设 WEBHOOK_BASE_URL，否则界面显示 localhost 无法填入 GitLab。"""
    base = os.environ.get("WEBHOOK_BASE_URL", "").strip().rstrip("/")
    return base if base else request.host


# ---- Data sources (webhook-based integrations, e.g. GitLab) ----

async def datasources_handler(request):
    if request.method == "GET":
        project = (request.query.get("project") or "").strip()
        items = manager.list_project_datasources(project) if project else manager.list_datasources()
        for it in items:
            it["webhook_url"] = f"http://{webhook_base(request)}/datasources/{it['id']}/webhook"
        return json_ok({"datasources": items})
    data = await read_json(request)
    dstype = (data.get("type") or "").strip()
    if dstype not in ("gitlab", "github", "cnb", "tapd"):
        return web.json_response({"error": "invalid type"}, status=400, headers=cors_headers())
    project = str(data.get("project") or "").strip()
    if project and (not manager._project_dir(project).is_dir()):
        return web.json_response({"error": "project not found", "project": project}, status=404, headers=cors_headers())
    ds = manager.create_datasource(data)
    ds["webhook_url"] = f"http://{webhook_base(request)}/datasources/{ds['id']}/webhook"
    return json_ok({"datasource": ds}, status=201)


async def datasource_detail_handler(request):
    dsid = request.match_info.get("dsid", "")
    if request.method == "DELETE":
        return json_ok({"deleted": manager.delete_datasource(dsid)})
    ds = manager.get_datasource(dsid)
    if ds is None:
        return web.json_response({"error": "not found"}, status=404, headers=cors_headers())
    ds["webhook_url"] = f"http://{webhook_base(request)}/datasources/{ds['id']}/webhook"
    return json_ok({"datasource": ds})


async def datasource_webhook_handler(request):
    dsid = request.match_info.get("dsid", "")
    ds = manager.get_datasource(dsid)
    if ds is None:
        return web.json_response({"error": "not found"}, status=404, headers=cors_headers())
    raw_body = await request.read()
    secret = ds.get("secret", "")
    dstype = str(ds.get("type") or "gitlab").strip() or "gitlab"
    if secret:
        if dstype == "github":
            sig = request.headers.get("X-Hub-Signature-256", "")
            expected = "sha256=" + hmac.new(secret.encode("utf-8"), raw_body, "sha256").hexdigest()
            if not sig or not hmac.compare_digest(sig, expected):  # ponytail: constant-time compare at trust boundary
                return web.json_response({"error": "invalid signature"}, status=403, headers=cors_headers())
        else:
            token = request.headers.get("X-Gitlab-Token", "")
            if not hmac.compare_digest(token, secret):  # ponytail: constant-time compare at trust boundary
                return web.json_response({"error": "invalid token"}, status=403, headers=cors_headers())
    try:
        payload = json.loads(raw_body.decode("utf-8")) if raw_body else {}
    except Exception:
        payload = {}
    monitored = set(ds.get("events", []))
    kind = ""
    action = ""
    event = None
    if dstype == "github":
        kind = str(request.headers.get("X-GitHub-Event", "") or payload.get("hook", {}).get("type", "") or "")
        action = str(payload.get("action", "") or "")
        evt_key = f"{kind}_{action}" if kind and action else kind
        repo = payload.get("repository") or {}
        sender = payload.get("sender") or {}
        item = payload.get("issue") or payload.get("pull_request") or {}
        if evt_key and evt_key in monitored:
            event = {
                "ts": datetime.now().isoformat(timespec="seconds"),
                "kind": kind,
                "action": action,
                "iid": item.get("number") or payload.get("number"),
                "title": item.get("title", ""),
                "url": item.get("html_url") or repo.get("html_url", ""),
                "author": sender.get("login", ""),
                "project": repo.get("html_url", ""),
            }
    else:
        kind = str(payload.get("object_kind", "") or "")
        obj = payload.get("object_attributes", {}) or {}
        action = str(obj.get("action", "") or "")
        evt_key = f"{kind}_{action}" if kind and action else ""
        if evt_key and evt_key in monitored:
            event = {
                "ts": datetime.now().isoformat(timespec="seconds"),
                "kind": kind,
                "action": action,
                "iid": obj.get("iid"),
                "title": obj.get("title", ""),
                "url": obj.get("url", ""),
                "author": (payload.get("user") or {}).get("name", ""),
                "project": (payload.get("project") or {}).get("web_url", ""),
            }
    if event:
        manager.append_datasource_event(dsid, event)
    return json_ok({"received": True, "kind": kind, "action": action})


async def status_handler(request):
    return json_ok({
        "ok": True,
        "running": True,
        "ready": True,
        "gaRoot": manager.ga_root,
        "mykeyPath": manager.mykey_path,
        "sessionCount": len(manager.sessions),
        "activeSessionId": manager.active_session_id,
        "ws": "/ws",
        "transport": {"http": True, "wsEventsOnly": True},
    })


_SETTINGS = Path.home() / ".ga_desktop_settings.json"
_UI_KEYS = ("lang", "theme", "appearance", "plain", "llmNo", "fontSize", "chatFilesDir")


def _desktop_ui() -> dict:
    try:
        ui = json.loads(_SETTINGS.read_text(encoding="utf-8")).get("ui")
        ui = dict(ui) if isinstance(ui, dict) else {}
    except Exception:
        ui = {}
    if "chatFilesDir" not in ui:
        ui["chatFilesDir"] = "temp"
    return ui


def resolve_chat_files_dir(ga_root) -> Path:
    """对话生成文件存放目录(配置项 chatFilesDir, 默认 temp); 相对 ga_root 解析为绝对路径."""
    rel = _desktop_ui().get("chatFilesDir") or "temp"
    p = Path(rel)
    if not p.is_absolute():
        p = Path(ga_root) / rel
    return p


async def get_config_handler(request):
    profiles = manager.list_model_profiles()
    active = next((p["id"] for p in profiles if p.get("active")), manager.config.get("llmNo", 0))
    cfg = dict(manager.config)
    if "llmNo" not in cfg:
        cfg["llmNo"] = active
    cfg.update(_desktop_ui())
    return json_ok({"gaRoot": manager.ga_root, "mykeyPath": manager.mykey_path, "config": cfg})


async def save_config_handler(request):
    data = await read_json(request)
    cfg = data.get("config", data)
    if isinstance(cfg, dict):
        patch = {k: cfg[k] for k in _UI_KEYS if k in cfg}
        if patch:
            try:
                doc = json.loads(_SETTINGS.read_text(encoding="utf-8")) if _SETTINGS.is_file() else {}
                if not isinstance(doc, dict):
                    doc = {}
                ui = doc["ui"] if isinstance(doc.get("ui"), dict) else {}
                ui.update(patch)
                doc["ui"] = ui
                _SETTINGS.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception as e:
                print(f"[bridge] save ui prefs failed: {e}", file=sys.stderr)
        manager.config.update(cfg)
    return json_ok({"ok": True, "gaRoot": manager.ga_root, "mykeyPath": manager.mykey_path, "config": manager.config})


async def model_profiles_handler(request):
    try:
        pid = request.match_info.get("id")
        if pid is not None:
            profile_id = int(pid)
            if request.method == "GET":
                return json_ok({"profile": manager.get_model_profile(profile_id)})
            if request.method == "PUT":
                return json_ok({"ok": True, **manager.update_model_profile(profile_id, await read_json(request))})
            if request.method == "DELETE":
                return json_ok({"ok": True, **manager.delete_model_profile(profile_id)})
            return json_ok({"ok": False, "error": "method not allowed"}, status=405)
        if request.method == "POST":
            return json_ok({"ok": True, **manager.add_model_profile(await read_json(request))})
        return json_ok({"profiles": manager.list_model_profiles()})
    except ValueError as e:
        return json_ok({"ok": False, "error": str(e)}, status=400)
    except Exception as e:
        return json_ok({"ok": False, "error": str(e)}, status=500)


async def mixin_handler(request):
    """聚合渠道成员管理：POST 加入 / DELETE 移出 主聚合渠道。"""
    try:
        profile_id = int(request.match_info.get("id"))
        if request.method == "POST":
            return json_ok({"ok": True, **manager.add_to_mixin(profile_id)})
        if request.method == "DELETE":
            return json_ok({"ok": True, **manager.remove_from_mixin(profile_id)})
        return json_ok({"ok": False, "error": "method not allowed"}, status=405)
    except ValueError as e:
        return json_ok({"ok": False, "error": str(e)}, status=400)
    except Exception as e:
        return json_ok({"ok": False, "error": str(e)}, status=500)


async def mixin_order_handler(request):
    """渠道组成员拖拽排序：PUT {members:[name,...]}。"""
    try:
        data = await read_json(request)
        return json_ok({"ok": True, **manager.reorder_mixin(data.get("members") or [])})
    except ValueError as e:
        return json_ok({"ok": False, "error": str(e)}, status=400)
    except Exception as e:
        return json_ok({"ok": False, "error": str(e)}, status=500)


async def list_sessions_handler(request):
    with manager.lock:
        sessions = [manager.snapshot(s, include_messages=False) for s in manager.sessions.values()]
    return json_ok({"sessions": sessions, "activeSessionId": manager.active_session_id})


async def new_session_handler(request):
    data = await read_json(request)
    sess = manager.create_session(cwd=data.get("cwd") or data.get("path"), project=data.get("project"))
    return json_ok({"ok": True, "sessionId": sess.id, "session": manager.snapshot(sess)}, status=201)


async def get_session_handler(request):
    sid = request.match_info["sid"]
    sess = manager.get_session(sid)
    return json_ok({"sessionId": sid, "session": manager.snapshot(sess), "messages": list(sess.messages), "partial": sess.partial})


async def delete_session_handler(request):
    sid = request.match_info["sid"]
    return json_ok(manager.delete_session(sid))


async def patch_session_handler(request):
    sid = request.match_info["sid"]
    sess = manager.get_session(sid)
    data = await read_json(request)
    if "title" in data:
        sess.title = data["title"]
        sess.untitled = False
    if "pinned" in data:
        sess.pinned = bool(data["pinned"])
    if "untitled" in data:
        sess.untitled = bool(data["untitled"])
    if "plan_scan_baseline" in data:
        sess.plan_scan_baseline = int(data["plan_scan_baseline"])
    sess.updated_at = time.time()
    manager._persist()
    return json_ok({"ok": True, "session": manager.snapshot(sess, include_messages=False)})


async def prompt_handler(request):
    sid = request.match_info["sid"]
    data = await read_json(request)
    prompt = data.get("prompt", data.get("content", data.get("message", "")))
    images = data.get("images") or []
    display = data.get("display")
    files_meta = data.get("files") or []        # 非图片附件 [{name, path}]
    image_metas = data.get("imageMetas") or []   # 图片附件 [{name, path}]（不含 dataUrl）
    llm_no = data.get("llmNo")
    if llm_no is not None:
        llm_no = int(llm_no)
    return json_ok(manager.submit_prompt(sid, prompt, images, llm_no=llm_no, display=display,
                                          files_meta=files_meta, image_metas=image_metas))


async def messages_handler(request):
    sid = request.match_info["sid"]
    after = int(request.query.get("after") or request.query.get("afterId") or 0)
    limit = int(request.query.get("limit") or 200)
    return json_ok(manager.messages(sid, after=after, limit=limit))


async def cancel_handler(request):
    sid = request.match_info["sid"]
    return json_ok(manager.cancel(sid))


async def restore_handler(request):
    sid = request.match_info["sid"]
    return json_ok(manager.restore_context(sid))


def _apply_project_workspace(pdir, ws_name, ws_path):
    """为项目绑定 workspace。
    - ws_name 非空：使用已有 workspace（校验存在于 registry）
    - ws_path 非空：新建 workspace（调用 workspace_cmd.prepare）
    - 两者皆空：清除绑定（删除 .workspace.json）
    返回 {"workspace": name} 或 {"error": msg}
    """
    ws_file = os.path.join(pdir, '.workspace.json')
    # 新建 workspace
    if ws_path:
        try:
            r = workspace_cmd.prepare(ws_path)
        except Exception as e:
            return {"error": f"workspace prepare 失败: {e}"}
        if not r.get("ok"):
            return {"error": r.get("error", "workspace prepare 失败")}
        ws_name = r.get("name", "")
    if ws_name:
        try:
            with open(ws_file, 'w', encoding='utf-8') as f:
                json.dump({"workspace": ws_name}, f, ensure_ascii=False)
        except OSError:
            pass
        return {"workspace": ws_name}
    # 清除绑定
    try:
        if os.path.isfile(ws_file):
            os.remove(ws_file)
    except OSError:
        pass
    return {"workspace": ""}


async def projects_list_handler(request):
    """列出 temp/projects/ 下所有项目目录（路径与 project_mode._project_dir 一致）。"""
    base = os.path.join(manager.ga_root, 'temp', 'projects')
    items = []
    if os.path.isdir(base):
        for name in sorted(os.listdir(base)):
            pdir = os.path.join(base, name)
            if not os.path.isdir(pdir) or name.startswith('.'):
                continue
            mem = os.path.join(pdir, 'project_memory.md')
            has_mem = os.path.isfile(mem)
            mem_lines = 0
            if has_mem:
                try:
                    with open(mem, encoding='utf-8') as f:
                        mem_lines = len(f.read().splitlines())
                except OSError:
                    pass
            try:
                mtime = os.path.getmtime(pdir)
            except OSError:
                mtime = 0
            # 读取项目绑定的 workspace 名称
            ws_name = ""
            ws_file = os.path.join(pdir, '.workspace.json')
            if os.path.isfile(ws_file):
                try:
                    with open(ws_file, encoding='utf-8') as f:
                        ws_data = json.load(f)
                    if isinstance(ws_data, dict):
                        ws_name = (ws_data.get("workspace") or "").strip()
                except (OSError, json.JSONDecodeError):
                    pass
            items.append({"name": name, "hasMemory": has_mem, "memLines": mem_lines, "mtime": mtime, "workspace": ws_name})
    return json_ok({"projects": items})


async def project_create_handler(request):
    """创建项目：mkdir temp/projects/<name>/ + 初始化 project_memory.md。"""
    data = await read_json(request)
    name = (data.get("name") or "").strip()
    if not name or '/' in name or '\\' in name or '..' in name or name.startswith('.'):
        return web.json_response({"error": "invalid project name"}, status=400, headers=cors_headers())
    base = os.path.join(manager.ga_root, 'temp', 'projects')
    pdir = os.path.join(base, name)
    if os.path.exists(pdir):
        return web.json_response({"error": "project already exists", "name": name}, status=409, headers=cors_headers())
    os.makedirs(pdir, exist_ok=True)
    mem = os.path.join(pdir, 'project_memory.md')
    if not os.path.exists(mem):
        with open(mem, 'w', encoding='utf-8') as f:
            f.write(f"# {name} 项目记忆\n\n本文件由 GA 项目模式自动创建。每轮对话后，agent 会在此沉淀本项目值得长期复用的关键信息（决策、约束、踩坑、进度）。\n")
    instruction = (data.get("instruction") or "").strip()
    if instruction:
        claude_md = os.path.join(pdir, 'CLAUDE.md')
        try:
            with open(claude_md, 'w', encoding='utf-8') as f:
                f.write(instruction.rstrip() + "\n")
        except OSError:
            pass
    # 项目级 skills 绑定：写入 .skills.json（选中 skill 的 name 列表）
    skills = data.get("skills")
    if isinstance(skills, list) and skills:
        clean = [s for s in (str(s).strip() for s in skills) if s]
        if clean:
            try:
                with open(os.path.join(pdir, '.skills.json'), 'w', encoding='utf-8') as f:
                    json.dump(clean, f, ensure_ascii=False)
            except OSError:
                pass
    # 项目级 workspace 绑定：可选已有 workspace name 或新建（需 path）
    ws_name = (data.get("workspace") or "").strip()
    ws_path = (data.get("workspacePath") or "").strip()
    _ws_result = _apply_project_workspace(pdir, ws_name, ws_path)
    return json_ok({"ok": True, "name": name, "path": pdir, "workspace": _ws_result.get("workspace", "")}, status=201)


async def project_skills_get_handler(request):
    """读取已建项目绑定的 skills 列表（GET /projects/{name}/skills）。"""
    name = request.match_info.get("name", "")
    if not name or '/' in name or '\\' in name or '..' in name or name.startswith('.'):
        return web.json_response({"error": "invalid project name"}, status=400, headers=cors_headers())
    pdir = os.path.join(manager.ga_root, 'temp', 'projects', name)
    if not os.path.isdir(pdir):
        return web.json_response({"error": "project not found", "name": name}, status=404, headers=cors_headers())
    skills_file = os.path.join(pdir, '.skills.json')
    skills = []
    if os.path.isfile(skills_file):
        try:
            with open(skills_file, encoding='utf-8') as f:
                loaded = json.load(f)
            if isinstance(loaded, list):
                skills = [str(s) for s in loaded]
        except (OSError, json.JSONDecodeError):
            pass
    return json_ok({"name": name, "skills": skills})


async def project_skills_update_handler(request):
    """更新已建项目绑定的 skills 列表（PUT /projects/{name}/skills）。
    body: {"skills": ["ponytail", ...]}  空列表=删除绑定（恢复全局注入）。
    """
    name = request.match_info.get("name", "")
    if not name or '/' in name or '\\' in name or '..' in name or name.startswith('.'):
        return web.json_response({"error": "invalid project name"}, status=400, headers=cors_headers())
    pdir = os.path.join(manager.ga_root, 'temp', 'projects', name)
    if not os.path.isdir(pdir):
        return web.json_response({"error": "project not found", "name": name}, status=404, headers=cors_headers())
    data = await read_json(request)
    skills = data.get("skills")
    if not isinstance(skills, list):
        return web.json_response({"error": "skills must be a list"}, status=400, headers=cors_headers())
    clean = [s for s in (str(s).strip() for s in skills) if s]
    skills_file = os.path.join(pdir, '.skills.json')
    try:
        if clean:
            with open(skills_file, 'w', encoding='utf-8') as f:
                json.dump(clean, f, ensure_ascii=False)
        else:
            # 空列表=删除绑定，恢复全局注入
            if os.path.isfile(skills_file):
                os.remove(skills_file)
    except OSError as e:
        return web.json_response({"error": f"write failed: {e}"}, status=500, headers=cors_headers())
    return json_ok({"ok": True, "name": name, "skills": clean})


async def project_workspace_update_handler(request):
    """更新项目绑定的 workspace（PUT /projects/{name}/workspace）。
    body: {"workspace": "已有name"} 或 {"workspacePath": "/abs/path"} 或 {}(清除)。
    """
    name = request.match_info.get("name", "")
    if not name or '/' in name or '\\' in name or '..' in name or name.startswith('.'):
        return web.json_response({"error": "invalid project name"}, status=400, headers=cors_headers())
    pdir = os.path.join(manager.ga_root, 'temp', 'projects', name)
    if not os.path.isdir(pdir):
        return web.json_response({"error": "project not found", "name": name}, status=404, headers=cors_headers())
    data = await read_json(request)
    ws_name = (data.get("workspace") or "").strip()
    ws_path = (data.get("workspacePath") or "").strip()
    result = _apply_project_workspace(pdir, ws_name, ws_path)
    if "error" in result:
        return web.json_response(result, status=400, headers=cors_headers())
    return json_ok({"ok": True, "name": name, "workspace": result.get("workspace", "")})


async def project_datasources_handler(request):
    name = request.match_info.get("name", "")
    if not name or '/' in name or '\\' in name or '..' in name or name.startswith('.'):
        return web.json_response({"error": "invalid project name"}, status=400, headers=cors_headers())
    pdir = os.path.join(manager.ga_root, 'temp', 'projects', name)
    if not os.path.isdir(pdir):
        return web.json_response({"error": "project not found", "name": name}, status=404, headers=cors_headers())
    items = manager.list_project_datasources(name)
    for it in items:
        it["webhook_url"] = f"http://{webhook_base(request)}/datasources/{it['id']}/webhook"
    return json_ok({"name": name, "datasources": items})


async def project_todos_handler(request):
    """项目待办列表 / 新建待办（GET|POST /projects/{name}/todos）。
    GET 返回 {name, todos}；POST body {title, desc?, status?, assignee?, due?} 返回 {todo}。"""
    name = request.match_info.get("name", "")
    if not name or '/' in name or '\\' in name or '..' in name or name.startswith('.'):
        return web.json_response({"error": "invalid project name"}, status=400, headers=cors_headers())
    pdir = os.path.join(manager.ga_root, 'temp', 'projects', name)
    if not os.path.isdir(pdir):
        return web.json_response({"error": "project not found", "name": name}, status=404, headers=cors_headers())
    if request.method == 'GET':
        return json_ok({"name": name, "todos": manager.list_todos(name)})
    data = await read_json(request)
    if not str(data.get("title", "")).strip():
        return web.json_response({"error": "title is required"}, status=400, headers=cors_headers())
    todo = manager.create_todo(name, data)
    return json_ok({"todo": todo}, status=201)


async def project_todo_detail_handler(request):
    """项目待办详情 / 更新 / 删除（GET|PATCH|DELETE /projects/{name}/todos/{tid}）。"""
    name = request.match_info.get("name", "")
    tid = request.match_info.get("tid", "")
    if not name or '/' in name or '\\' in name or '..' in name or name.startswith('.'):
        return web.json_response({"error": "invalid project name"}, status=400, headers=cors_headers())
    pdir = os.path.join(manager.ga_root, 'temp', 'projects', name)
    if not os.path.isdir(pdir):
        return web.json_response({"error": "project not found", "name": name}, status=404, headers=cors_headers())
    if request.method == 'GET':
        for it in manager.list_todos(name):
            if it.get("id") == tid:
                return json_ok({"todo": it})
        return web.json_response({"error": "todo not found"}, status=404, headers=cors_headers())
    if request.method == 'DELETE':
        ok = manager.delete_todo(name, tid)
        if not ok:
            return web.json_response({"error": "todo not found"}, status=404, headers=cors_headers())
        return json_ok({"deleted": True, "id": tid})
    data = await read_json(request)
    it = manager.update_todo(name, tid, data)
    if not it.get("id"):
        return web.json_response({"error": "todo not found"}, status=404, headers=cors_headers())
    return json_ok({"todo": it})


async def project_rename_handler(request):
    """重命名项目（PUT /projects/{name}/rename）。
    body: {"newName": "..."}。支持目录和符号链接；符号链接只改链接名不改源目录。
    """
    old_name = request.match_info.get("name", "")
    data = await read_json(request)
    new_name = (data.get("newName") or "").strip()
    invalid = lambda n: (not n or '/' in n or '\\' in n or '..' in n or n.startswith('.'))
    if invalid(old_name) or invalid(new_name) or old_name == new_name:
        return web.json_response({"error": "invalid project name"}, status=400, headers=cors_headers())
    base = os.path.join(manager.ga_root, 'temp', 'projects')
    old_dir = os.path.join(base, old_name)
    new_dir = os.path.join(base, new_name)
    if not os.path.exists(old_dir):
        return web.json_response({"error": "project not found", "name": old_name}, status=404, headers=cors_headers())
    if os.path.exists(new_dir):
        return web.json_response({"error": "project already exists", "name": new_name}, status=409, headers=cors_headers())
    try:
        os.rename(old_dir, new_dir)
    except OSError as e:
        return web.json_response({"error": f"rename failed: {e}"}, status=500, headers=cors_headers())
    # 同步更新 project_memory.md 标题（仅普通文件，符号链接跳过）
    if not os.path.islink(new_dir):
        mem = os.path.join(new_dir, 'project_memory.md')
        if os.path.isfile(mem):
            try:
                with open(mem, 'r', encoding='utf-8') as f:
                    content = f.read()
                content = content.replace(f"# {old_name} 项目记忆", f"# {new_name} 项目记忆")
                with open(mem, 'w', encoding='utf-8') as f:
                    f.write(content)
            except OSError:
                pass
    return json_ok({"ok": True, "oldName": old_name, "newName": new_name})


async def project_delete_handler(request):
    """删除项目（DELETE /projects/{name}）。
    符号链接只删链接不删源目录；普通目录递归删除。
    """
    name = request.match_info.get("name", "")
    if not name or '/' in name or '\\' in name or '..' in name or name.startswith('.'):
        return web.json_response({"error": "invalid project name"}, status=400, headers=cors_headers())
    pdir = os.path.join(manager.ga_root, 'temp', 'projects', name)
    if not os.path.exists(pdir) and not os.path.islink(pdir):
        return web.json_response({"error": "project not found", "name": name}, status=404, headers=cors_headers())
    try:
        if os.path.islink(pdir):
            os.remove(pdir)  # 只删符号链接
        elif os.path.isdir(pdir):
            import shutil
            shutil.rmtree(pdir)
        else:
            os.remove(pdir)
    except OSError as e:
        return web.json_response({"error": f"delete failed: {e}"}, status=500, headers=cors_headers())
    return json_ok({"ok": True, "name": name})


async def skills_list_handler(request):
    """列出可用 skills（供前端弹窗渲染多选）。复用 skills_loader 的发现逻辑。"""
    try:
        import sys as _sys
        _ga_root = manager.ga_root
        if _ga_root not in _sys.path:
            _sys.path.insert(0, _ga_root)
        from plugins import skills_loader as _sl
        roots = _sl._load_config()
        if not roots:
            return json_ok({"skills": []})
        skills = _sl._discover_skills(roots)
        items = []
        for sk in skills:
            items.append({
                "name": sk.get("name", ""),
                "description": sk.get("description", ""),
                "has_scripts": bool(sk.get("has_scripts", False)),
                "path": sk.get("path", ""),
            })
        return json_ok({"skills": items})
    except Exception as e:
        return json_ok({"skills": [], "error": str(e)})


async def plan_handler(request):
    sid = request.match_info["sid"]
    return json_ok(manager.plan_snapshot(sid))


async def suggest_handler(request):
    """对话结束后自动推荐下一步动作。用当前会话的 llmclient 生成 1-3 条简短建议。
    临时备份/恢复 backend.history，不污染主会话上下文。"""
    sid = request.match_info["sid"]
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(None, manager.suggest, sid)
        return json_ok(result)
    except Exception as e:
        import traceback as _tb
        try: open("/tmp/suggest_debug.log","a").write("[handler] "+_tb.format_exc()+"\n\n")
        except Exception: pass
        return json_ok({"suggestions": [], "error": str(e)})


async def path_open_handler(request):
    data = await read_json(request)
    kind = data.get("kind", "")
    mode = data.get("mode", "open")
    if kind == "mykey":
        target = Path(manager.ga_root) / "mykey.py"
        if not target.exists():
            template = Path(manager.ga_root) / "mykey_template.py"
            target = template if template.exists() else target
    elif kind == "mykeyTemplate":
        target = Path(manager.ga_root) / "mykey_template.py"
    elif kind == "upload":
        raw = Path(data.get("path") or "")
        try:
            resolved = raw.resolve()
            upload_root = _WEB_UPLOAD_DIR.resolve()
            resolved.relative_to(upload_root)
        except (ValueError, OSError):
            return json_ok({"ok": False, "error": "path not in upload dir"}, status=403)
        target = resolved
    else:
        target = Path(data.get("path") or data.get("target") or manager.ga_root)
    target = target.resolve()
    if not target.exists():
        return json_ok({"ok": False, "error": f"File not found: {target}"}, status=404)
    try:
        if mode == "reveal":
            _reveal_path_in_file_manager(target)
        elif kind in ("mykey", "mykeyTemplate"):
            _open_path_in_editor(target)  # mykey 配置文件仍用编辑器(edit 动词)
        else:
            _open_path_default(target)  # upload/task/config 等普通文件用系统默认程序(open 动词)
    except OSError as e:
        return json_ok({"ok": False, "error": str(e), "path": str(target)}, status=500)
    return json_ok({"ok": True, "path": str(target)})


# ── @ file mention: list files ────────────────────────────────────────

_IGNORED_DIRS = {'.git', 'node_modules', '__pycache__', 'venv', '.venv',
                 '.DS_Store', '.idea', '.vscode', 'dist', 'build', '.next',
                 '.nuxt', 'target', 'egg-info', '.eggs', 'temp'}


async def files_list_handler(request):
    """GET /api/files/list?path={dir}&filter={optional}
    
    Recursively searches the directory tree, matching filter against
    the path relative to dir (e.g. "src/util" matches "src/utils.py").
    """
    dir_path = request.query.get("path", "")
    filter_str = (request.query.get("filter") or "").lower()
    if not dir_path:
        return json_ok({"error": "path is required"}, status=400)
    root = Path(dir_path)
    if not root.is_dir():
        return json_ok({"error": f"not a directory: {dir_path}"}, status=400)
    
    MAX_RESULTS = 100
    entries: list[dict] = []
    filter_parts = filter_str.split('/') if filter_str else []
    
    try:
        stack: list[tuple[Path, str]] = [(root, "")]
        while stack and len(entries) < MAX_RESULTS:
            cur_dir, rel_prefix = stack.pop()
            try:
                children = sorted(cur_dir.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower()))
            except (PermissionError, OSError):
                continue
            for child in children:
                if len(entries) >= MAX_RESULTS:
                    break
                name = child.name
                if name.startswith('.') and name not in ('.env', '.env.local', '.gitignore'):
                    continue
                if child.is_dir() and name in _IGNORED_DIRS:
                    continue
                rel = f"{rel_prefix}/{name}" if rel_prefix else name
                # Always push dirs to stack so children can be searched
                if child.is_dir() and name not in _IGNORED_DIRS:
                    stack.append((child, rel))
                # Apply filter: only add matching items to results
                if filter_parts:
                    rel_lower = rel.lower()
                    if not all(part in rel_lower for part in filter_parts):
                        continue
                entries.append({
                    "name": name,
                    "type": "dir" if child.is_dir() else "file",
                    "path": str(child),
                    "rel": rel,
                })
    except Exception as e:
        return json_ok({"error": str(e)}, status=500)
    
    entries.sort(key=lambda x: (0 if x["type"] == "dir" else 1, x.get("rel", x["name"]).lower()))
    return json_ok({"path": dir_path, "entries": entries})


async def files_browse_handler(request):
    """GET /api/files/browse - aggregate files from chat uploads, task reports, and config."""
    ga_root = Path(DEFAULT_GA_ROOT)
    sche_tasks_dir = ga_root / "sche_tasks"
    uploads_dir = ga_root / "temp" / "desktop_uploads"

    files: list[dict] = []

    # a. Chat uploads: temp/desktop_uploads/sess-{id}/
    if uploads_dir.exists():
        for sess_dir in sorted(uploads_dir.iterdir()):
            if not sess_dir.is_dir() or not sess_dir.name.startswith("sess-"):
                continue
            sid = sess_dir.name
            for f in sess_dir.iterdir():
                if not f.is_file():
                    continue
                try:
                    stat = f.stat()
                except OSError:
                    continue
                files.append({
                    "name": f.name,
                    "path": str(f.relative_to(ga_root)),
                    "size": stat.st_size,
                    "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    "created": datetime.fromtimestamp(getattr(stat, "st_birthtime", stat.st_ctime)).isoformat(),
                    "source": "chat",
                    "type": f.suffix.lower().lstrip(".") or "file",
                    "session": sid,
                    "referencedBy": {"type": "session", "id": sid, "alive": sess_dir.exists()},
                })

    # b. Task reports: sche_tasks/done/*.md  (format: {ts}_{tid}.md)
    done_dir = sche_tasks_dir / "done"
    if done_dir.exists():
        # Build a set of known task ids from config .json files
        known_tids: set[str] = set()
        if sche_tasks_dir.exists():
            for fj in sche_tasks_dir.iterdir():
                if fj.suffix == ".json":
                    known_tids.add(fj.stem)

        for f in sorted(done_dir.iterdir()):
            if not f.is_file() or f.suffix != ".md":
                continue
            try:
                stat = f.stat()
            except OSError:
                continue
            stem = f.stem
            parts = stem.split("_")
            # Try to match a known task id suffix
            tid = None
            for k in range(1, min(len(parts), 5)):
                candidate = "_".join(parts[k:])
                if candidate in known_tids:
                    tid = candidate
                    break
            if tid is None and len(parts) >= 2:
                tid = "_".join(parts[1:])

            alive = False
            if tid:
                task_json = sche_tasks_dir / f"{tid}.json"
                if task_json.exists():
                    try:
                        task_data = json.loads(task_json.read_text(encoding="utf-8"))
                        alive = task_data.get("enabled", False)
                    except Exception:
                        pass

            files.append({
                "name": f.name,
                "path": str(f.relative_to(ga_root)),
                "size": stat.st_size,
                "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "created": datetime.fromtimestamp(getattr(stat, "st_birthtime", stat.st_ctime)).isoformat(),
                "source": "task",
                "type": "md",
                "taskId": tid,
                "referencedBy": {"type": "task", "id": tid, "alive": alive} if tid else None,
            })

    # c. Config/scripts: sche_tasks/*.json, *.py
    if sche_tasks_dir.exists():
        for f in sorted(sche_tasks_dir.iterdir()):
            if not f.is_file() or f.suffix not in (".json", ".py"):
                continue
            try:
                stat = f.stat()
            except OSError:
                continue
            files.append({
                "name": f.name,
                "path": str(f.relative_to(ga_root)),
                "size": stat.st_size,
                "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "created": datetime.fromtimestamp(getattr(stat, "st_birthtime", stat.st_ctime)).isoformat(),
                "source": "config",
                "type": f.suffix.lower().lstrip(".") or "file",
                "taskId": f.stem,
                "referencedBy": {"type": "task", "id": f.stem, "alive": True},
            })

    # d. Generated files: 对话生成文件存放目录(配置项 chatFilesDir, 默认 temp/)
    gen_dir = resolve_chat_files_dir(ga_root)
    if gen_dir.exists():
        for f in sorted(gen_dir.iterdir()):
            if not f.is_file():
                continue
            try:
                stat = f.stat()
            except OSError:
                continue
            try:
                rel_path = str(f.relative_to(ga_root))
            except ValueError:
                rel_path = str(f)
            files.append({
                "name": f.name,
                "path": rel_path,
                "size": stat.st_size,
                "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "created": datetime.fromtimestamp(getattr(stat, "st_birthtime", stat.st_ctime)).isoformat(),
                "source": "generated",
                "type": f.suffix.lower().lstrip(".") or "file",
                "referencedBy": None,
            })

    counts = {
        "total": len(files),
        "chat": sum(1 for x in files if x["source"] == "chat"),
        "task": sum(1 for x in files if x["source"] == "task"),
        "config": sum(1 for x in files if x["source"] == "config"),
        "generated": sum(1 for x in files if x["source"] == "generated"),
        "session": len({x.get("session") for x in files if x.get("session")}),
    }
    return json_ok({"ok": True, "files": files, "counts": counts})


async def files_delete_handler(request):
    """DELETE /api/files/delete - delete a file within allowed directories."""
    data = await read_json(request)
    raw = data.get("path") or ""
    try:
        ga_root = Path(DEFAULT_GA_ROOT)
        p = Path(raw)
        if not p.is_absolute():
            p = ga_root / p
        target = p.resolve()
        upload_root = (ga_root / "temp" / "desktop_uploads").resolve()
        sche_root = (ga_root / "sche_tasks").resolve()

        in_upload = upload_root in target.parents or target == upload_root
        in_sche = sche_root in target.parents or target == sche_root
        if not (in_upload or in_sche):
            return json_ok({"ok": False, "error": "path outside allowed directories"}, status=403)

        if not target.exists():
            return json_ok({"ok": False, "error": "file not found"}, status=404)
        if not target.is_file():
            return json_ok({"ok": False, "error": "not a file"}, status=400)
        target.unlink()
        return json_ok({"ok": True, "deleted": str(target)})
    except Exception as e:
        return json_ok({"ok": False, "error": str(e)})


async def files_copy_handler(request):
    """POST /api/files/copy - duplicate a file within allowed directories."""
    data = await read_json(request)
    raw = data.get("path") or ""
    try:
        ga_root = Path(DEFAULT_GA_ROOT)
        p = Path(raw)
        if not p.is_absolute():
            p = ga_root / p
        target = p.resolve()
        upload_root = (ga_root / "temp" / "desktop_uploads").resolve()
        sche_root = (ga_root / "sche_tasks").resolve()
        in_upload = upload_root in target.parents or target == upload_root
        in_sche = sche_root in target.parents or target == sche_root
        if not (in_upload or in_sche):
            return json_ok({"ok": False, "error": "path outside allowed directories"}, status=403)
        if not target.exists() or not target.is_file():
            return json_ok({"ok": False, "error": "file not found"}, status=404)
        import shutil
        stem = target.stem
        suffix = target.suffix
        parent = target.parent
        new_path = parent / f"{stem} copy{suffix}"
        i = 1
        while new_path.exists():
            new_path = parent / f"{stem} copy{i}{suffix}"
            i += 1
        shutil.copy2(target, new_path)
        return json_ok({"ok": True, "copied": str(new_path), "name": new_path.name})
    except Exception as e:
        return json_ok({"ok": False, "error": str(e)})


async def files_read_handler(request):
    """GET /api/files/read?path=... - read a text file for preview within allowed directories."""
    from urllib.parse import unquote
    raw = unquote(request.query.get("path") or "")
    try:
        ga_root = Path(DEFAULT_GA_ROOT)
        p = Path(raw)
        # 相对路径基于 ga_root 解析（browse 返回相对 GA 根的路径）
        if not p.is_absolute():
            p = ga_root / p
        target = p.resolve()
        upload_root = (ga_root / "temp" / "desktop_uploads").resolve()
        sche_root = (ga_root / "sche_tasks").resolve()
        in_upload = upload_root in target.parents or target == upload_root
        in_sche = sche_root in target.parents or target == sche_root
        if not (in_upload or in_sche):
            return json_ok({"ok": False, "error": "path outside allowed directories"}, status=403)
        if not target.exists() or not target.is_file():
            return json_ok({"ok": False, "error": "file not found"}, status=404)
        size = target.stat().st_size
        if size > 2 * 1024 * 1024:
            return json_ok({"ok": False, "error": "file too large to preview"})
        content = target.read_text(encoding="utf-8", errors="replace")
        return json_ok({"ok": True, "content": content, "name": target.name, "size": size})
    except Exception as e:
        return json_ok({"ok": False, "error": str(e)})


async def commands_list_handler(request):
    """GET /api/commands → list available slash commands."""
    try:
        import slash_cmds
        cmds = slash_cmds.list_all_commands(str(manager.ga_root))
    except Exception as e:
        return json_ok({"error": str(e)}, status=500)
    return json_ok({"commands": cmds})


async def slash_handler(request):
    """POST /session/{sid}/slash → execute a skill command via prompt injection."""
    sid = request.match_info["sid"]
    data = await read_json(request)
    cmd = (data or {}).get("cmd", "").strip()
    args = (data or {}).get("args", "")
    if not cmd:
        return json_ok({"error": "cmd is required"}, status=400)
    # --- /scheduler: special-cased (not prompt injection; launches services) ---
    if cmd == "/scheduler":
        try:
            import slash_cmds
            # If args contains "start a,b,c", apply the diff (stop removed / start added).
            parts = args.split()
            if parts and parts[0] == "start":
                names_raw = " ".join(parts[1:])
                selected = [n.strip() for n in names_raw.split(",") if n.strip()]
                wanted = set(selected)
                try:
                    running = slash_cmds.running_services(use_cache=False)
                except Exception:
                    running = {}
                results = []
                # stop first (services running but not in wanted set)
                for nm in list(running.keys()):
                    if nm not in wanted:
                        ok, msg = slash_cmds.stop_service(nm)
                        results.append({"name": nm, "action": "stop", "ok": ok, "msg": msg})
                # then start (wanted but not running)
                for nm in selected:
                    if nm not in running:
                        ok, msg = slash_cmds.start_service(nm)
                        results.append({"name": nm, "action": "start", "ok": ok, "msg": msg})
                return json_ok({"schedulerResults": results})
            # No "start" args → return service list + running state for the picker UI
            services = slash_cmds.list_launchable_services()
            running = slash_cmds.running_services(use_cache=False)
            return json_ok({"schedulerPicker": True, "services": services, "running": running})
        except Exception as e:
            return json_ok({"error": str(e)}, status=500)
    try:
        import slash_cmds
        injected = slash_cmds.prompt_for(cmd, args)
    except Exception as e:
        return json_ok({"error": str(e)}, status=500)
    if injected is None:
        return json_ok({"error": f"unknown command: {cmd}"}, status=400)
    display = cmd + (" " + args if args else "")
    files_meta = (data or {}).get("files") or []
    image_metas = (data or {}).get("imageMetas") or []
    llm_no = (data or {}).get("llmNo")
    if llm_no is not None:
        llm_no = int(llm_no)
    return json_ok(manager.submit_prompt(sid, injected, [], llm_no=llm_no,
                                          display=display, files_meta=files_meta,
                                          image_metas=image_metas))


async def exec_handler(request):
    """POST /session/{sid}/exec → execute a shell command and return output."""
    sid = request.match_info["sid"]
    data = await read_json(request)
    cmd = (data or {}).get("cmd", "").strip()
    if not cmd:
        return json_ok({"error": "cmd is required"}, status=400)
    try:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
        stdout = stdout.decode("utf-8", errors="replace").rstrip("\n")
        stderr = stderr.decode("utf-8", errors="replace").rstrip("\n")
        return json_ok({
            "ok": proc.returncode == 0,
            "code": proc.returncode,
            "stdout": stdout,
            "stderr": stderr,
        })
    except asyncio.TimeoutError:
        return json_ok({"error": "command timed out (30s)"}, status=504)
    except Exception as e:
        return json_ok({"error": str(e)}, status=500)


# File attachments live under GA's own temp dir (gitignored), NOT the OS temp
# dir, so they survive bridge restarts. Instead of wiping everything on startup,
# we keep files for UPLOAD_RETENTION_DAYS and only sweep stale ones.
_WEB_UPLOAD_DIR = Path(DEFAULT_GA_ROOT) / "temp" / "desktop_uploads"
_WEB_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

UPLOAD_RETENTION_DAYS = 30


def _safe_session_dir(sid: str) -> str:
    """Sanitize a session id into a safe single-level folder name."""
    s = re.sub(r"[^A-Za-z0-9_-]", "", str(sid or ""))
    return s or "_misc"


def _session_upload_dir(sid: str) -> Path:
    """Per-session upload subdir under desktop_uploads/, created on demand."""
    d = _WEB_UPLOAD_DIR / _safe_session_dir(sid)
    d.mkdir(parents=True, exist_ok=True)
    return d


def _purge_session_uploads(sid: str) -> None:
    """Best-effort: drop a session's whole upload subdir when the session is deleted."""
    import shutil
    with contextlib.suppress(Exception):
        shutil.rmtree(_WEB_UPLOAD_DIR / _safe_session_dir(sid), ignore_errors=True)


def _sweep_stale_uploads(retention_days: int = UPLOAD_RETENTION_DAYS) -> None:
    """Best-effort: delete uploaded files older than retention_days (by mtime),
    then drop empty session subdirs. Replaces the old wholesale rmtree-on-startup
    so attachments persist across restarts while temp storage can't grow forever."""
    cutoff = time.time() - retention_days * 86400
    try:
        for f in _WEB_UPLOAD_DIR.rglob("*"):
            try:
                if f.is_file() and f.stat().st_mtime < cutoff:
                    f.unlink()
            except OSError:
                pass
        for d in _WEB_UPLOAD_DIR.iterdir():
            try:
                if d.is_dir() and not any(d.iterdir()):
                    d.rmdir()
            except OSError:
                pass
    except OSError:
        pass


_sweep_stale_uploads()


async def upload_handler(request):
    """Save a file uploaded by the web client and return its absolute path.
    Body: {name: "<original filename>", dataUrl: "data:<mime>;base64,<...>", sid: "<session id>"}
    Files are grouped per session under desktop_uploads/<sid>/ so deleting a
    session can purge its attachments. Missing sid falls back to a _misc bucket.
    Returns: {ok: true, path: "<abs path>"}
    """
    try:
        data = await request.json()
        if not isinstance(data, dict):
            data = {}
    except web.HTTPRequestEntityTooLarge:
        return json_ok({"ok": False, "error": "file too large for bridge body limit"})
    except Exception as e:
        return json_ok({"ok": False, "error": f"invalid request: {e}"})
    name = (data.get("name") or "file").strip().replace("/", "_").replace("\\", "_")
    data_url = data.get("dataUrl") or ""
    if "," in data_url:
        b64 = data_url.split(",", 1)[1]
    else:
        b64 = data_url
    try:
        blob = base64.b64decode(b64)
    except Exception as e:
        return json_ok({"ok": False, "error": f"decode failed: {e}"})
    if not blob:
        return json_ok({"ok": False, "error": "empty file"})
    safe_name = name or "file"
    fpath = _session_upload_dir(data.get("sid") or "") / f"{uuid.uuid4().hex[:12]}__{safe_name}"
    fpath.write_bytes(blob)
    return json_ok({"ok": True, "path": str(fpath)})


async def upload_delete_handler(request):
    """Delete a previously-uploaded file. Path must live under _WEB_UPLOAD_DIR."""
    data = await read_json(request)
    raw = data.get("path") or ""
    try:
        target = Path(raw).resolve()
        upload_root = _WEB_UPLOAD_DIR.resolve()
        if upload_root not in target.parents:
            return json_ok({"ok": False, "error": "path outside upload dir"})
        if target.exists():
            target.unlink()
        return json_ok({"ok": True})
    except Exception as e:
        return json_ok({"ok": False, "error": str(e)})


async def upload_raw_handler(request):
    """Stream an uploaded file. inline by default (browser preview / <img>),
    ?download=1 forces a download. Path must live under _WEB_UPLOAD_DIR
    (whitelist — prevents path traversal). Works for remote browsers too,
    so it covers both 'preview after refresh' and 'download from remote'."""
    import mimetypes
    from urllib.parse import quote
    raw = request.query.get("path", "")
    try:
        target = Path(raw).resolve()
        target.relative_to(_WEB_UPLOAD_DIR.resolve())
    except (ValueError, OSError):
        return web.Response(status=403, text="path not in upload dir")
    if not target.is_file():
        return web.Response(status=404, text="file not found")
    ctype = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
    disp = "attachment" if request.query.get("download") in ("1", "true") else "inline"
    orig_name = target.name.split("__", 1)[-1]  # 去掉 <uuid>__ 前缀，还原原始文件名
    return web.Response(
        body=target.read_bytes(),
        content_type=ctype,
        headers={
            "Content-Disposition": f"{disp}; filename*=UTF-8''{quote(orig_name)}",
            "Cache-Control": "no-cache",
        },
    )


def _open_path_in_editor(target: Path) -> None:
    """Open a file in the user's editor; Windows .py often has no default association."""
    import platform
    path = str(target.resolve())
    if platform.system() == "Windows":
        try:
            os.startfile(path, "edit")
            return
        except OSError:
            pass
        for cmd in (["notepad.exe", path], ["cursor.cmd", path], ["code.cmd", path], ["cursor", path], ["code", path]):
            try:
                subprocess.Popen(cmd, close_fds=True)
                return
            except (FileNotFoundError, OSError):
                continue
        raise OSError(f"No editor available to open: {path}")
    if platform.system() == "Darwin":
        subprocess.Popen(["open", path])
        return
    subprocess.Popen(["xdg-open", path])


def _reveal_path_in_file_manager(target: Path) -> None:
    """Open the system file manager and select/highlight the target file."""
    import platform
    path = str(target.resolve())
    if platform.system() == "Windows":
        subprocess.Popen(["explorer", "/select,", path])
        return
    if platform.system() == "Darwin":
        subprocess.Popen(["open", "-R", path])
        return
    # Linux: no universal "select file" command; fall back to opening parent dir
    subprocess.Popen(["xdg-open", str(target.parent)])


def _open_path_default(target: Path) -> None:
    """Open a file with the OS default app (default 'open' verb).

    For user uploads. Unlike _open_path_in_editor (which uses Windows' 'edit'
    verb and falls back to Notepad), this respects each file type's registered
    default app — PDF viewer, Word, archive tool, etc. — so binaries like pdf
    or docx no longer land in Notepad as garbage."""
    import platform
    path = str(target.resolve())
    if platform.system() == "Windows":
        os.startfile(path)  # default "open" verb = double-click behavior
        return
    if platform.system() == "Darwin":
        subprocess.Popen(["open", path])
        return
    subprocess.Popen(["xdg-open", path])


def _mykey_file() -> Path:
    root = Path(manager.ga_root)
    target = root / "mykey.py"
    if not target.is_file():
        template = root / "mykey_template.py"
        if template.is_file():
            target.write_text(template.read_text(encoding="utf-8"), encoding="utf-8")
    return target


async def mykey_get_handler(request):
    target = _mykey_file()
    content = target.read_text(encoding="utf-8") if target.is_file() else ""
    return json_ok({"content": content, "path": str(target)})


async def mykey_save_handler(request):
    data = await read_json(request)
    content = data.get("content")
    if content is None:
        return json_ok({"ok": False, "error": "missing_content"}, status=400)
    try:
        profiles = manager._save_mykey_text(str(content))
    except Exception as e:
        return json_ok({"ok": False, "error": str(e)}, status=400)
    return json_ok({"ok": True, "path": str(manager._mykey_file()), "profiles": profiles})


async def service_start_handler(request):
    body = await read_json(request)
    sid = body.get("id") or request.query.get("id")
    if not sid:
        return json_ok({"ok": False, "error": "missing_id"}, status=400)
    result = services.start_service(sid)
    if not result.get("ok"):
        return json_ok(result, status=400)
    return json_ok(result)


async def service_stop_handler(request):
    body = await read_json(request)
    sid = body.get("id") or request.query.get("id")
    if not sid:
        return json_ok({"ok": False, "error": "missing_id"}, status=400)
    return json_ok(services.stop_service(sid))


async def service_logs_handler(request):
    sid = request.query.get("id")
    if not sid:
        return json_ok({"ok": False, "error": "missing_id"}, status=400)
    tail = int(request.query.get("tail") or 200)
    return json_ok(services.read_logs(sid, tail=tail))


async def service_panel_handler(request):
    return json_ok({"services": services.list_panel_state()})


def _is_local_peer(peer: str) -> bool:
    p = (peer or "").strip()
    return p in ("127.0.0.1", "::1") or p.startswith("::ffff:127.0.0.1")


@web.middleware
async def local_only_guard(request, handler):
    # ponytail: 监听 0.0.0.0 时, webhook 单路由对内网开放(已有 X-Gitlab-Token 验签);
    # 其余路由(exec/files/read/delete/mykey/bridge-exit 等高危无认证)仅本机 127.0.0.1.
    path = request.path
    if path.startswith("/datasources/") and path.endswith("/webhook"):
        return await handler(request)
    if not _is_local_peer(request.remote or ""):
        return web.json_response(
            {"error": "forbidden: local only"}, status=403, headers=cors_headers()
        )
    return await handler(request)


async def stop_extras_handler(request):
    if not _is_local_peer(request.remote or ""):
        return json_ok({"ok": False, "error": "forbidden"}, status=403)
    services.stop_all_extras()
    return json_ok({"ok": True})


async def start_extras_handler(request):
    if not _is_local_peer(request.remote or ""):
        return json_ok({"ok": False, "error": "forbidden"}, status=403)
    services.autostart_extras()
    return json_ok({"ok": True})


async def identity_handler(request):
    return json_ok({"ga_root": str(DEFAULT_GA_ROOT), "app_dir": str(APP_DIR), "pid": os.getpid(),
                    "build_id": os.environ.get("GA_BUILD_ID", "")})


def _exit_bridge() -> None:
    with contextlib.suppress(Exception):
        services.stop_all_extras()
    threading.Timer(0.4, lambda: os._exit(0)).start()


async def bridge_exit_handler(request):
    if not _is_local_peer(request.remote or ""):
        return json_ok({"ok": False, "error": "forbidden"}, status=403)
    _exit_bridge()
    return json_ok({"ok": True})


async def token_stats_handler(request):
    try:
        sys.path.insert(0, str(APP_DIR)) if str(APP_DIR) not in sys.path else None
        import cost_tracker
        trackers = cost_tracker.all_trackers()
        records = []
        for k, v in trackers.items():
            model = ''
            sid = k.replace('GA-', '')
            with manager.lock:
                sess = manager.sessions.get(sid)
            if sess and sess.agent:
                try: model = sess.agent.get_llm_name(model=True) or ''
                except Exception: pass
            records.append({"thread": k, "input": v.input, "output": v.output,
                            "cacheCreate": v.cache_create, "cacheRead": v.cache_read, "model": model})
    except Exception:
        records = []
    return json_ok({"records": records})


_TOKEN_HISTORY_FILE = None

def _tok_file() -> Path:
    global _TOKEN_HISTORY_FILE
    if _TOKEN_HISTORY_FILE is None:
        _TOKEN_HISTORY_FILE = Path(manager.ga_root) / "temp" / "desktop_token_history.json"
    return _TOKEN_HISTORY_FILE

async def get_token_history_handler(request):
    f = _tok_file()
    if f.is_file():
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            return json_ok(data)
        except Exception:
            pass
    return json_ok({"history": [], "snap": {}})

async def post_token_history_handler(request):
    data = await read_json(request)
    f = _tok_file()
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return json_ok({"ok": True})


# ---------------------------------------------------------------------------
# Workspace handlers
# ---------------------------------------------------------------------------
async def list_workspaces_handler(request):
    """GET /workspaces → list of registered workspaces."""
    try:
        items = workspace_cmd.registry_list()
    except Exception as e:
        return json_ok({"error": str(e)}, status=500)
    return json_ok({"workspaces": items})


async def prepare_workspace_handler(request):
    """POST /workspace/prepare → add/activate a workspace. Body: {path}."""
    data = await read_json(request)
    path = (data or {}).get("path", "")
    if not path:
        return json_ok({"error": "path is required"}, status=400)
    try:
        r = workspace_cmd.prepare(path)
    except Exception as e:
        return json_ok({"error": str(e)}, status=500)
    return json_ok(r)


async def remove_workspace_handler(request):
    """DELETE /workspace/{name} → remove a workspace registration."""
    name = request.match_info["name"]
    if not name:
        return json_ok({"error": "name is required"}, status=400)
    try:
        workspace_cmd.remove(name)
    except Exception as e:
        return json_ok({"error": str(e)}, status=500)
    return json_ok({"ok": True})


async def session_workspace_get_handler(request):
    """GET /session/{sid}/workspace → get workspace bound to session."""
    sid = request.match_info["sid"]
    sess = manager.get_session(sid)
    ws_name = sess.workspace
    if not ws_name:
        return json_ok({"workspace": None})
    ent = workspace_cmd.registry_load().get(ws_name) or {}
    return json_ok({"workspace": {"name": ws_name, "path": ent.get("path", "")}})


async def session_workspace_set_handler(request):
    """POST /session/{sid}/workspace → bind workspace to session. Body: {name}."""
    sid = request.match_info["sid"]
    sess = manager.get_session(sid)
    data = await read_json(request)
    name = (data or {}).get("name", "")
    if not name:
        return json_ok({"error": "name is required"}, status=400)
    ent = workspace_cmd.registry_load().get(name)
    if not ent:
        return json_ok({"error": f"workspace not found: {name}"}, status=404)
    path = ent.get("path", "")
    try:
        r = workspace_cmd.prepare(path)
    except Exception as e:
        return json_ok({"error": str(e)}, status=500)
    sess.workspace = name
    sess.updated_at = time.time()
    workspace_cmd.registry_upsert(name, path)
    manager._persist()
    return json_ok({"ok": True, "workspace": {"name": name, "path": path}})


async def session_workspace_off_handler(request):
    """POST /session/{sid}/workspace/off → unbind workspace from session."""
    sid = request.match_info["sid"]
    sess = manager.get_session(sid)
    sess.workspace = ""
    sess.updated_at = time.time()
    manager._persist()
    return json_ok({"ok": True})


def create_app():
    app = web.Application(middlewares=[cors_middleware, local_only_guard], client_max_size=500 * 1024 * 1024)
    app.router.add_get("/ws", ws_handler)
    app.router.add_get("/status", status_handler)
    app.router.add_get("/config", get_config_handler)
    app.router.add_post("/config", save_config_handler)
    app.router.add_get("/model-profiles", model_profiles_handler)
    app.router.add_post("/model-profiles", model_profiles_handler)
    app.router.add_put("/model-profiles/mixin/order", mixin_order_handler)
    app.router.add_post("/model-profiles/{id}/mixin", mixin_handler)
    app.router.add_delete("/model-profiles/{id}/mixin", mixin_handler)
    app.router.add_get("/model-profiles/{id}", model_profiles_handler)
    app.router.add_put("/model-profiles/{id}", model_profiles_handler)
    app.router.add_delete("/model-profiles/{id}", model_profiles_handler)
    app.router.add_get("/sessions", list_sessions_handler)
    app.router.add_post("/session/new", new_session_handler)
    app.router.add_get("/session/{sid}", get_session_handler)
    app.router.add_delete("/session/{sid}", delete_session_handler)
    app.router.add_patch("/session/{sid}", patch_session_handler)
    app.router.add_post("/session/{sid}/prompt", prompt_handler)
    app.router.add_get("/session/{sid}/messages", messages_handler)
    app.router.add_get("/session/{sid}/plan", plan_handler)
    app.router.add_post("/session/{sid}/cancel", cancel_handler)
    app.router.add_post("/session/{sid}/restore", restore_handler)
    app.router.add_post("/session/{sid}/suggest", suggest_handler)
    app.router.add_get("/projects", projects_list_handler)
    app.router.add_post("/projects", project_create_handler)
    app.router.add_get("/api/skills", skills_list_handler)
    app.router.add_get("/projects/{name}/skills", project_skills_get_handler)
    app.router.add_put("/projects/{name}/skills", project_skills_update_handler)
    app.router.add_put("/projects/{name}/workspace", project_workspace_update_handler)
    app.router.add_get("/projects/{name}/datasources", project_datasources_handler)
    # 项目待办 (todos) CRUD —— /projects/{name}/todos 与 /projects/{name}/todos/{tid}
    app.router.add_get("/projects/{name}/todos", project_todos_handler)
    app.router.add_post("/projects/{name}/todos", project_todos_handler)
    app.router.add_get("/projects/{name}/todos/{tid}", project_todo_detail_handler)
    app.router.add_patch("/projects/{name}/todos/{tid}", project_todo_detail_handler)
    app.router.add_delete("/projects/{name}/todos/{tid}", project_todo_detail_handler)
    app.router.add_put("/projects/{name}/rename", project_rename_handler)
    app.router.add_delete("/projects/{name}", project_delete_handler)
    # Data sources (webhook-based, e.g. GitLab)
    app.router.add_get("/datasources", datasources_handler)
    app.router.add_post("/datasources", datasources_handler)
    app.router.add_get("/datasources/{dsid}", datasource_detail_handler)
    app.router.add_delete("/datasources/{dsid}", datasource_detail_handler)
    app.router.add_post("/datasources/{dsid}/webhook", datasource_webhook_handler)
    app.router.add_post("/path/open", path_open_handler)
    app.router.add_post("/upload", upload_handler)
    app.router.add_delete("/upload", upload_delete_handler)

    # @ mention & slash command APIs
    app.router.add_get("/api/files/list", files_list_handler)
    app.router.add_get("/api/files/browse", files_browse_handler)
    app.router.add_delete("/api/files/delete", files_delete_handler)
    app.router.add_post("/api/files/copy", files_copy_handler)
    app.router.add_get("/api/files/read", files_read_handler)
    app.router.add_get("/api/commands", commands_list_handler)
    app.router.add_post("/session/{sid}/slash", slash_handler)
    app.router.add_post("/session/{sid}/exec", exec_handler)

    # Workspace routes
    app.router.add_get("/workspaces", list_workspaces_handler)
    app.router.add_post("/workspace/prepare", prepare_workspace_handler)
    app.router.add_delete("/workspace/{name}", remove_workspace_handler)
    app.router.add_get("/session/{sid}/workspace", session_workspace_get_handler)
    app.router.add_post("/session/{sid}/workspace", session_workspace_set_handler)
    app.router.add_post("/session/{sid}/workspace/off", session_workspace_off_handler)

    app.router.add_get("/upload/raw", upload_raw_handler)
    app.router.add_get("/token-stats", token_stats_handler)
    app.router.add_get("/token-history", get_token_history_handler)
    app.router.add_post("/token-history", post_token_history_handler)
    app.router.add_post("/services/start", service_start_handler)
    app.router.add_post("/services/stop", service_stop_handler)
    app.router.add_get("/services/logs", service_logs_handler)
    app.router.add_get("/services/panel", service_panel_handler)
    app.router.add_get("/services/mykey", mykey_get_handler)
    app.router.add_post("/services/mykey", mykey_save_handler)
    app.router.add_post("/services/stop-extras", stop_extras_handler)
    app.router.add_post("/services/start-extras", start_extras_handler)
    app.router.add_get("/services/identity", identity_handler)
    app.router.add_post("/services/bridge/exit", bridge_exit_handler)

    async def tasks_list_handler(request):
        tasks_dir = APP_DIR.parent / "sche_tasks"
        tasks = []
        if tasks_dir.exists():
            for f in sorted(tasks_dir.iterdir()):
                if f.suffix == '.json':
                    try:
                        with open(f, 'r', encoding='utf-8') as fp:
                            task = json.load(fp)
                        tasks.append({
                            'id': f.stem,
                            'name': task.get('name', f.stem),
                            'schedule': task.get('schedule', ''),
                            'repeat': task.get('repeat', ''),
                            'enabled': task.get('enabled', False),
                            'model': task.get('model', ''),
                            'status': 'healthy'
                        })
                    except Exception:
                        tasks.append({
                            'id': f.stem,
                            'name': f.stem,
                            'schedule': '',
                            'repeat': '',
                            'enabled': False,
                            'status': 'error'
                        })
        return web.json_response({'tasks': tasks})

    async def tasks_history_handler(request):
        done_dir = APP_DIR.parent / "sche_tasks" / "done"
        history = []
        if done_dir.exists():
            for f in sorted(done_dir.iterdir(), reverse=True):
                if f.suffix == '.md':
                    try:
                        parts = f.stem.split('_')
                        if len(parts) >= 2:
                            time_str = parts[0]
                            task_name = '_'.join(parts[1:])
                            history.append({'name': task_name, 'time': time_str})
                    except Exception:
                        pass
        return web.json_response({'history': history})

    async def tasks_toggle_handler(request):
        tid = request.match_info.get('tid')
        tasks_dir = APP_DIR.parent / "sche_tasks"
        task_file = tasks_dir / f"{tid}.json"
        if not task_file.exists():
            return web.json_response({'error': 'task not found'}, status=404)
        try:
            with open(task_file, 'r', encoding='utf-8') as fp:
                task = json.load(fp)
            task['enabled'] = not task.get('enabled', False)
            with open(task_file, 'w', encoding='utf-8') as fp:
                json.dump(task, fp, ensure_ascii=False, indent=2)
            return web.json_response({'ok': True})
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)

    async def tasks_get_handler(request):
        tid = request.match_info.get('tid')
        tasks_dir = APP_DIR.parent / "sche_tasks"
        task_file = tasks_dir / f"{tid}.json"
        if not task_file.exists():
            return web.json_response({'error': 'task not found'}, status=404)
        try:
            with open(task_file, 'r', encoding='utf-8') as fp:
                task = json.load(fp)
            return web.json_response({'task': {**task, 'id': tid}})
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)

    async def tasks_update_handler(request):
        tid = request.match_info.get('tid')
        tasks_dir = APP_DIR.parent / "sche_tasks"
        task_file = tasks_dir / f"{tid}.json"
        if not task_file.exists():
            return web.json_response({'error': 'task not found'}, status=404)
        try:
            body = await request.json()
            with open(task_file, 'r', encoding='utf-8') as fp:
                task = json.load(fp)
            for k in ('name', 'schedule', 'repeat', 'enabled', 'model', 'prompt', 'max_delay_hours'):
                if k in body:
                    task[k] = body[k]
            with open(task_file, 'w', encoding='utf-8') as fp:
                json.dump(task, fp, ensure_ascii=False, indent=2)
            return web.json_response({'ok': True})
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)

    async def tasks_delete_handler(request):
        tid = request.match_info.get('tid')
        tasks_dir = APP_DIR.parent / "sche_tasks"
        task_file = tasks_dir / f"{tid}.json"
        if not task_file.exists():
            return web.json_response({'error': 'task not found'}, status=404)
        try:
            task_file.unlink()
            return web.json_response({'ok': True})
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)

    async def tasks_create_handler(request):
        data = await request.json()
        name = (data.get('name') or '').strip()
        prompt = (data.get('prompt') or '').strip()
        if not prompt:
            return web.json_response({'error': 'prompt is required'}, status=400)
        if not name:
            name = prompt[:20]
        schedule = (data.get('schedule') or '08:00').strip()
        repeat = (data.get('repeat') or 'daily').strip()
        # repeat Whitelist
        valid_repeats = {'once', 'daily', 'weekday', 'weekly', 'monthly'}
        if repeat not in valid_repeats and not repeat.startswith('every_'):
            repeat = 'daily'
        enabled = bool(data.get('enabled', True))
        max_delay_hours = float(data.get('max_delay_hours', 6))
        model = (data.get('model') or '').strip()
        # generate tid: sanitize name + short random suffix
        import re, uuid
        safe = re.sub(r'[^A-Za-z0-9\u4e00-\u9fff_]', '_', name)[:30] or 'task'
        tid = f"{safe}_{uuid.uuid4().hex[:6]}"
        tasks_dir = APP_DIR.parent / "sche_tasks"
        tasks_dir.mkdir(exist_ok=True)
        task_file = tasks_dir / f"{tid}.json"
        while task_file.exists():
            tid = f"{safe}_{uuid.uuid4().hex[:6]}"
            task_file = tasks_dir / f"{tid}.json"
        task = {
            'name': name,
            'schedule': schedule,
            'repeat': repeat,
            'enabled': enabled,
            'prompt': prompt,
            'max_delay_hours': max_delay_hours,
            'model': model
        }
        try:
            with open(task_file, 'w', encoding='utf-8') as fp:
                json.dump(task, fp, ensure_ascii=False, indent=2)
            return web.json_response({'ok': True, 'id': tid})
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)

    app.router.add_get("/services/tasks/list", tasks_list_handler)
    app.router.add_get("/services/tasks/history", tasks_history_handler)
    app.router.add_post("/services/tasks/create", tasks_create_handler)
    app.router.add_get("/services/tasks/get/{tid}", tasks_get_handler)
    app.router.add_post("/services/tasks/update/{tid}", tasks_update_handler)
    app.router.add_post("/services/tasks/toggle/{tid}", tasks_toggle_handler)
    app.router.add_delete("/services/tasks/delete/{tid}", tasks_delete_handler)

    # Serve static frontend (desktop/static/)
    static_dir = APP_DIR / "desktop" / "static"

    async def index_handler(request):
        return web.FileResponse(
            static_dir / "index.html",
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )

    app.router.add_get("/", index_handler)
    app.router.add_static("/", static_dir, show_index=False)

    async def on_startup(app):
        hub.loop = asyncio.get_running_loop()
        services.autostart_extras()

    async def on_shutdown(app):
        services.stop_all_extras()

    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    return app


if __name__ == "__main__":
    host = os.environ.get("BRIDGE_HOST", "127.0.0.1")
    port = int(os.environ.get("BRIDGE_PORT", "14168"))
    print(f"GenericAgent Web2 bridge: http://{host}:{port}  ws://{host}:{port}/ws", file=sys.stderr)
    web.run_app(create_app(), host=host, port=port, print=None)
