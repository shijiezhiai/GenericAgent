from __future__ import annotations
import os, sys, re, time, json, uuid, queue, asyncio, threading, signal
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List

from aiohttp import web

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FRONTENDS_DIR = os.path.dirname(os.path.abspath(__file__))
for p in (ROOT, FRONTENDS_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)

HOST = os.environ.get("CONDUCTOR_HOST", "127.0.0.1")
PORT = int(os.environ.get("CONDUCTOR_PORT", "8900"))


def _generic_agent_class():
    """Return the GenericAgent agent class. Resolves the class name defensively
    (the source defines it as ``GenericAgent`` but some modules reference the
    historical ``GeneraticAgent`` alias) and works around the circular-import
    artifact where agentmain may land in sys.modules partially initialized."""
    import sys
    import agentmain
    cls = getattr(agentmain, "GenericAgent", None) or getattr(agentmain, "GeneraticAgent", None)
    if cls is None:
        sys.modules.pop("agentmain", None)
        import agentmain
        cls = getattr(agentmain, "GenericAgent", None) or getattr(agentmain, "GeneraticAgent", None)
    if cls is None:
        raise RuntimeError("agent class not found on agentmain (GenericAgent/GeneraticAgent)")
    return cls


def _desktop_llm_no() -> Optional[int]:
    """Read the model index the user picked in the desktop UI.
    Persisted by the bridge at ~/.ga_desktop_settings.json under ui.llmNo.
    Returns None when unavailable, so callers keep the agent's default model."""
    try:
        from pathlib import Path
        doc = json.loads((Path.home() / ".ga_desktop_settings.json").read_text(encoding="utf-8"))
        no = (doc.get("ui") or {}).get("llmNo")
        return int(no) if no is not None else None
    except Exception:
        return None


def _apply_desktop_model(agent: "GenericAgent") -> None:
    """Switch a freshly built agent to the desktop-selected model (if any)."""
    no = _desktop_llm_no()
    if no is None:
        return
    try:
        agent.next_llm(int(no))
    except Exception as e:
        print(f"[conductor] failed to apply desktop model #{no}: {e}", file=sys.stderr)


ws_clients: set = set()
main_loop: Optional[asyncio.AbstractEventLoop] = None
# conductor event queue: only user messages and subagent-done events enter here.
chat_messages: List[dict] = []

now_ms = lambda: int(time.time() * 1000)
short_id = lambda: uuid.uuid4().hex[:8]

_TURN_SPLIT_RE = re.compile(r'\**LLM Running \(Turn \d+\) \.\.\.\**')
_SUMMARY_RE = re.compile(r'<summary>(.*?)</summary>\s*', re.DOTALL)

def extract_last_summary(full: str) -> str:
    """Extract the latest <summary> content for in-progress display."""
    matches = _SUMMARY_RE.findall(full or "")
    if not matches: return ""
    s = matches[-1].strip()
    return s[-1000:] if len(s) > 1000 else s

def extract_last_text_reply(full: str) -> str:
    """Extract only the last turn's text reply (like stapp.py fold_turns logic)."""
    parts = _TURN_SPLIT_RE.split(full)
    last = parts[-1] if parts else full
    last = _SUMMARY_RE.sub('', last)
    last = re.sub(r'\[(Status|Info)\][^\n]*\n?', '', last)
    last = last.strip()
    return last[-3000:] if len(last) > 3000 else last

def clean_log_text(s: str) -> str:
    if not s: return s
    s = re.sub(r'`{5}\n.*?`{5}\n?', '', s, flags=re.DOTALL)
    s = re.sub(r'🛠️ Tool: `([^`]+)`\s*📥 args:\n`{4}.*?`{4}\n?', r'🛠️ `\1`\n', s, flags=re.DOTALL)
    s = re.sub(r'^🛠️ .*\n?', '', s, flags=re.MULTILINE)
    s = re.sub(r'<thinking>.*?</thinking>\s*', '', s, flags=re.DOTALL)
    s = re.sub(r'^\s*\[(?:Info|Status)\][^\n]*\n?', '', s, flags=re.MULTILINE)
    s = re.sub(r'^\s*`{4,5}\s*$\n?', '', s, flags=re.MULTILINE)
    s = re.sub(r'\n{3,}', '\n\n', s)
    return s.strip()

def schedule_broadcast(payload: dict):
    if main_loop and main_loop.is_running():
        asyncio.run_coroutine_threadsafe(broadcast(payload), main_loop)

async def broadcast(payload: dict):
    dead = []
    for ws in list(ws_clients):
        try: await ws.send_json(payload)
        except Exception: dead.append(ws)
    for ws in dead: ws_clients.discard(ws)

def push_cards(): schedule_broadcast({"type": "subagents", "items": pool.snapshot()})

def add_chat(msg: str, role: str = "conductor", files: list = None, images: list = None):
    item = {"id": short_id(), "role": role, "msg": msg, "ts": now_ms(), "read": role != "user", "files": files or [], "images": images or []}
    chat_messages.append(item)
    if len(chat_messages) > 200: del chat_messages[:-200]
    schedule_broadcast({"type": "chat", "item": item})
    return item

def start_agent_runner(agent: GenericAgent, name: str):
    t = threading.Thread(target=agent.run, name=name, daemon=True)
    t.start(); return t

def monitor_display_queue(agent_id: str, dq: "queue.Queue", trigger_when_done: bool):
    acc = ""
    while True:
        item = dq.get()
        if "next" in item:
            chunk = item.get("next") or ""
            acc += chunk
            pool.on_display(agent_id, acc, done=False)
            push_cards()
        if "done" in item:
            done = item.get("done") or acc
            pool.on_display(agent_id, done, done=True)
            push_cards()
            task_history.record_done(agent_id)
            if trigger_when_done: conductor.notify({"type": "subagent_done", "id": agent_id, "reply": done})
            break


@dataclass
class SubagentState:
    id: str
    agent: GenericAgent
    prompt: str
    thread: Optional[threading.Thread] = None
    reply: str = ""
    status: str = "running"  # running | stopped
    created_at: int = field(default_factory=lambda: int(time.time()))
    updated_at: int = field(default_factory=lambda: int(time.time()))


class SubagentPool:
    def __init__(self):
        self.subagents: Dict[str, SubagentState] = {}
        self.lock = threading.RLock()
        threading.Thread(target=self._auto_cleanup_loop, name="subagent-cleanup", daemon=True).start()
    def snapshot(self) -> list:
        with self.lock:
            return [
                {
                    "id": s.id,
                    "prompt": s.prompt,
                    "reply": (extract_last_summary(s.reply) if s.status == "running" else extract_last_text_reply(s.reply)) if s.reply else "",
                    "status": s.status,
                    "created_at": s.created_at,
                    "updated_at": s.updated_at,
                }
                for s in self.subagents.values()
            ]
    def get(self, sid: str) -> Optional[SubagentState]:
        with self.lock: return self.subagents.get(sid)
    def counts(self) -> tuple:
        with self.lock:
            running = sum(1 for s in self.subagents.values() if s.status == "running")
            stopped = sum(1 for s in self.subagents.values() if s.status != "running")
        return running, stopped
    def on_display(self, agent_id: str, acc: str, done: bool):
        with self.lock:
            s = self.subagents.get(agent_id)
            if s:
                s.reply = acc
                s.updated_at = int(time.time())
                s.status = "stopped" if done else "running"
    def _auto_cleanup_loop(self):
        IDLE_TIMEOUT = 3600
        while True:
            time.sleep(300)
            now = time.time()
            to_abort = []
            with self.lock:
                for sid, s in self.subagents.items():
                    if s.status == "stopped" and (now - s.updated_at) > IDLE_TIMEOUT: to_abort.append((sid, s))
            for sid, s in to_abort:
                s.agent.abort()
                s.agent.task_queue.put("EXIT")
                with self.lock: self.subagents.pop(sid, None)
            if to_abort: push_cards()
    def start_subagent(self, prompt: str) -> dict:
        sid = short_id()
        agent = _generic_agent_class()()
        agent.inc_out = True
        agent.verbose = False
        agent.no_print = True
        _apply_desktop_model(agent)
        th = start_agent_runner(agent, f"subagent-{sid}")
        state = SubagentState(id=sid, agent=agent, prompt=prompt, status="running", thread=th)
        with self.lock: self.subagents[sid] = state
        return self._send_msg(sid, prompt)
    def _send_msg(self, sid, msg):
        with self.lock: s = self.subagents.get(sid)
        if not s: return {"error": "subagent not found", "id": sid}
        dq = s.agent.put_task(msg, source=f"subagent:{sid}")
        threading.Thread(target=monitor_display_queue, args=(sid, dq, True), name=f"monitor-{sid}", daemon=True).start()
        push_cards()
        return {"id": sid, "status": "running"}
    def input_subagent(self, sid: str, msg: str) -> dict:
        with self.lock: s = self.subagents.get(sid)
        if not s: return {"error": "subagent not found", "id": sid}
        if s.status == "running": return {"error": "subagent is still running, cannot input/reply. Start a new subagent instead.", "id": sid}
        s.prompt = msg
        s.reply = ""
        s.status = "running"
        s.updated_at = int(time.time())
        return self._send_msg(sid, msg)
    def keyinfo_subagent(self, sid: str, msg: str) -> dict:
        with self.lock: s = self.subagents.get(sid)
        if not s: return {"error": "subagent not found", "id": sid}
        h = s.agent.handler
        h.working['key_info'] = h.working.get('key_info', '') + f"\n[MASTER] {msg}"
        s.updated_at = int(time.time())
        return {"id": sid, "status": "keyinfo_injected"}

pool = SubagentPool()

# ===================== Task History Persistence =====================
HISTORY_FILE = os.path.join(ROOT, "temp", "conductor_history.json")
HISTORY_MAX = 200

class TaskHistoryStore:
    """Persist finished subagent task records to disk."""
    def __init__(self, path: str = HISTORY_FILE, max_records: int = HISTORY_MAX):
        self.path = path
        self.max_records = max_records
        self.lock = threading.Lock()
        self.records: List[dict] = []
        self._load()

    def _load(self):
        try:
            if os.path.isfile(self.path):
                with open(self.path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    self.records = data[:self.max_records]
        except Exception:
            self.records = []

    def _save(self):
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.records, f, ensure_ascii=False, indent=1)
            os.replace(tmp, self.path)
        except Exception as e:
            print(f"[history] save failed: {e}")

    def record_done(self, sid: str, status_override: str = None):
        """Persist a finished subagent run as a history record."""
        s = pool.get(sid)
        if not s: return
        reply = clean_log_text(s.reply or "")
        rec = {
            "id": short_id(),
            "agent_id": s.id,
            "prompt": s.prompt,
            "reply": reply[-8000:] if len(reply) > 8000 else reply,
            "status": status_override or s.status,
            "created_at": s.created_at,
            "updated_at": s.updated_at,
            "duration": max(0, s.updated_at - s.created_at),
        }
        with self.lock:
            self.records.insert(0, rec)
            if len(self.records) > self.max_records:
                self.records = self.records[:self.max_records]
            self._save()

    def list(self, limit: int = 50, offset: int = 0, q: str = "") -> dict:
        with self.lock:
            items = list(self.records)
        if q:
            ql = q.lower()
            items = [r for r in items if ql in (r.get("prompt") or "").lower() or ql in (r.get("reply") or "").lower()]
        total = len(items)
        page = items[offset:offset + limit]
        out = [{**r, "reply": (r.get("reply") or "")[:300], "truncated": len(r.get("reply") or "") > 300} for r in page]
        return {"total": total, "items": out}

    def get(self, rid: str) -> Optional[dict]:
        with self.lock:
            for r in self.records:
                if r["id"] == rid: return dict(r)
        return None

    def clear(self):
        with self.lock:
            self.records.clear()
            self._save()

task_history = TaskHistoryStore()

READMES = {
"api": """{
Conductor API\tBase: http://{HOST}:{PORT}

POST /chat\tbody: {{"msg": "..."}}\t给用户发消息
POST /subagent\tbody: {{"prompt": "..."}}\t启动新subagent，返回 {{"id": "xxx"}}
POST /approval\tbody: {{"prompt": "...", "source": "..."}}\t推一条待批任务到前端(后端不存)，用户同意则直接派发为subagent
POST /subagent/{{id}}\tbody: {{"action": "keyinfo", "msg": "..."}}\t注入key_info（agent下轮可见）
POST /subagent/{{id}}\tbody: {{"action": "input", "msg": "..."}}\t开新一轮任务（agent停下后追加）
POST /subagent/{{id}}\tbody: {{"action": "stop"}}\t中断执行但保留（可继续input/reply）
GET /chat?last=N\t返回最近N条对话（默认20）
GET /subagent\t返回 {{"items": [...]}}\t查看所有subagent状态
GET /subagent/{{id}}?max_len=N\t返回单个subagent详情，reply经清洗后截取尾部max_len字（默认5000）。仅在摘要不够判断时使用
""",
"usermsg": """\
用户消息流程：
1. 结合记忆、上下文和用户偏好判断真实需求；不清楚/不能代劳时，用精简checklist一次性问用户。
2. 如果用户意图是定时/周期/重复执行任务（每天/每周/每月/每N小时/定时等关键词），按GET /readme/scheduled处理。
3. 判断是新任务还是延续现有任务；优先复用已有stopped subagent（用input追加），只有确实无关的新任务才新建。
4. 分派前必须POST /chat告知用户：改写后的prompt + 分派方案（新建/复用哪个subagent）。
5. 执行分派，完成即停。危险操作（改源码/删数据/安全敏感）必须改成先让subagent出方案；你验收后POST /chat请用户确认，确认后才继续执行。""",
"subagent": """\
subagent完成流程：
1. 如果是IM采集subagent，按GET /readme/im进行而非本流程
2. 读subagent输出；若最后一条不足以判断，GET /subagent/{id}?max_len=3000 补足信息。
3. 预测用户是否满意；不满意就reply/keyinfo要求返工、修改、优化，继续监督，不急着报告。
4. 预计用户满意后，POST /chat给简洁交付报告。""",
"im": """\
你要审查IM采集subagent的输出，把**值得用户关注的内容**报告给用户或转化成"可点击执行"的待批TODO（approval）。
先读L2记忆中User相关，推荐的动作和措辞要符合用户画像。
要求：
1. 不要只凭采集摘要；重要事实要核实，需要判断时先派subagent补做必要调查，再下结论。
2. 没有值得用户点击执行的动作就直接结束，不要打扰；尤其不要对执行回执/完成确认/纯闲聊报"无需关注"。
3. 判断标准：私聊默认重要，群聊除非@用户否则忽略。
4. 只有真正需要用户的内容才报告或形成TODO。不要推"去看看/研究一下"这种半成品。TODO必须是最后一步可直接执行的动作（发某段微信回复、回复某封邮件草稿、处理某PR、整理某文件等）。
5. 如果形成用户TODO，POST /approval 推送，prompt里同时写清两部分：
   ① 奏折式报告给用户拍板：背景(什么事/来自谁) + 已核实(你做了哪些调查/关键事实) + 判断(为什么这样建议) + 风险。用户看完这段就能直接拍板，不用再去翻原消息。
   ② 用户同意后该执行的完整任务指令（approval通过会直接作为subagent的prompt派发，必须具体到可直接执行）。""",
"scheduled": """\
定时任务流程：
1. 用户表述中包含这些特征时判断为定时任务：每天/每日/每周/每月/每N小时/每N天/定时/周期/定期/重复/设定闹钟式提醒
2. 不要自己去执行——必须派一个subagent去创建定时任务JSON文件
3. subagent的prompt必须包含：
   a) 先 file_read ../memory/scheduled_task_sop.md 了解JSON格式
   b) 用 file_write 在 ../sche_tasks/{任务名}.json 写入任务定义
   JSON格式：{"schedule":"08:00", "repeat":"daily", "enabled":true, "prompt":"...(要执行的具体任务)", "max_delay_hours":6}
   repeat可选：daily | weekday | weekly | monthly | once | every_Nh | every_Nd
   c) 写入后告知用户任务已创建，scheduler会按时间自动执行
4. 先POST /chat告知用户：已识别为定时任务 + 提取出的schedule/repeat + 将要创建的任务名，确认后再派subagent创建""",
}

class Conductor:
    LOG_MAX = 50

    def __init__(self):
        self.inbox: "queue.Queue[dict]" = queue.Queue()   # 收件箱：唯一对外接口
        self.agent: Optional[GenericAgent] = None
        self.started = False
        self.log: list = []

    def notify(self, event: dict): self.inbox.put(event)

    def _build_prompt(self, events: list) -> str:
        running, stopped = pool.counts()
        unread = sum(1 for m in chat_messages if m.get("role") == "user" and not m.get("read"))
        done_count = sum(1 for e in events if e.get("type") == "subagent_done")
        event_type = events[0].get("type") if events else "wake"; im_sources = [e.get("source") for e in events if e.get("type") == "im_signal"]
        if event_type == "user_message": summary = f"[用户消息] {unread}条未读用户消息，GET /chat 读取；按GET /readme/usermsg处理。"
        elif event_type == "subagent_done": summary = f"[subagent完成] {done_count}个完成报告；GET /subagent 查看并验收；IM subagent完成报告按GET /readme/im处理，其他subagent完成报告按GET /readme/subagent处理。"
        elif event_type == "im_signal": summary = f"[IM信号] {', '.join(im_sources)} 有新消息；" + "；".join(f"GET /im_prompt/{s}取采集prompt" for s in im_sources) + "；尽量复用已有subagent。"
        else: summary = f"[唤醒] subagents: {running} running, {stopped} stopped | {unread}条用户未读消息, {done_count}个subagent完成报告"
        base = f"http://{HOST}:{PORT}"
        return f""""你是agent总管。用户只和你对话，你负责调度、验收、交付，目标是降低用户管理多个agent的负担。
API: {base}；requests，GET /readme查用法，GET /chat读未读对话，GET /subagent看状态；POST /chat是唯一对用户说话方式。

铁律：
- 绝不亲自执行任务/探测环境；一切执行交给subagent。你只分析、派遣、审查、沟通。
- 每次唤醒只做最小必要动作（发消息/开subagent/reply/keyinfo/abort），做完立刻停，等待下次事件唤醒。
- 改写prompt时严禁添加用户未提及的假设、工具、前提条件。只能精炼/结构化用户原意，不能脑补，只能做很小的改写

原则：
- 信任subagent足够聪明，不要写具体步骤和容易探测的信息；能自己判断的自己判断，只在真正需要用户决策时打扰。

需要处理：
{summary}"""

    def _drain(self, dq: "queue.Queue", events: list) -> str:
        event_label = ",".join(e.get("type", "") for e in events) or "wake"
        cur_turn = None;  buf = ""

        def flush():
            nonlocal buf
            cleaned = clean_log_text(buf)
            if cleaned:
                item = {"id": short_id(), "ts": now_ms(), "event": event_label,
                        "turn": cur_turn, "text": cleaned}
                self.log.append(item)
                if len(self.log) > self.LOG_MAX: self.log.pop(0)
                schedule_broadcast({"type": "log", "item": item})
            buf = ""

        while True:
            item = dq.get()
            if "next" in item:
                t = item.get("turn")
                if cur_turn is None: cur_turn = t
                elif t != cur_turn:
                    flush(); cur_turn = t
                buf += item.get("next", "") or ""
            elif "done" in item:
                if cur_turn is None: cur_turn = item.get("turn")
                flush()
                print("Conductor task done")
                return

    def _run(self):
        self.agent = _generic_agent_class()()
        self.agent.inc_out = True
        start_agent_runner(self.agent, "conductor-agent")
        self.started = True
        while True:
            first = self.inbox.get()
            self.inbox.task_done()
            time.sleep(0.3)
            events = [first]
            while not self.inbox.empty():
                try:
                    events.append(self.inbox.get_nowait())
                    self.inbox.task_done()
                except Exception:
                    break
            try:
                prompt = self._build_prompt(events)
                _apply_desktop_model(self.agent)
                dq = self.agent.put_task(prompt, source="conductor")
                self._drain(dq, events)
            except Exception as e: print(f"Conductor error: {e}")

    def start(self): threading.Thread(target=self._run, name="conductor-loop", daemon=True).start()

conductor = Conductor()

# ---- IM poller: 探测conductor_im_plugins/下各插件,信号变化→唤醒总管 ----
IM_DIR, IM_COOLDOWN = os.path.join(os.path.dirname(__file__), "conductor_im_plugins"), 300
IM_PROMPTS: Dict[str, str] = {}   # source -> 采集prompt（派采集subagent时按需取）

def im_poll_loop():
    import importlib.util
    mods, last_fire = {}, {}
    for f in (x for x in os.listdir(IM_DIR) if x.endswith(".py") and not x.startswith("_")):
        spec = importlib.util.spec_from_file_location(f[:-3], os.path.join(IM_DIR, f))
        m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
        if hasattr(m, "check"):
            mods[f[:-3]] = m
            IM_PROMPTS[f[:-3]] = getattr(m, "PROMPT", "")
    last_check = {}
    while True:
        time.sleep(10)
        for name, m in mods.items():
            now = time.time()
            if now - last_check.get(name, 0) < getattr(m, "INTERVAL", 30): continue
            last_check[name] = now
            try:
                if not m.check() or now - last_fire.get(name, 0) < IM_COOLDOWN: continue
            except Exception: continue
            last_fire[name] = now
            conductor.notify({"type": "im_signal", "source": name})


# ===================== aiohttp handlers =====================
async def _json_body(request):
    try:
        return await request.json() or {}
    except Exception:
        return {}

def _plain(text: str, status: int = 200):
    return web.Response(text=text, status=status, content_type="text/plain", charset="utf-8")

async def _token_stats(request):
    import cost_tracker
    return web.json_response({"records": [{"thread": k, "input": v.input, "output": v.output, "cacheCreate": v.cache_create, "cacheRead": v.cache_read} for k, v in cost_tracker.all_trackers().items()]})

async def _index(request):
    return _plain("GA Conductor (headless API). UI: desktop app -> Collab. Docs: /readme")

async def _readme(request):
    return _plain(READMES["api"])

async def _readme_topic(request):
    topic = request.match_info["topic"]
    if topic not in READMES:
        return _plain(f"Unknown topic: {topic}. Available: {', '.join(READMES.keys())}", status=404)
    return _plain(READMES[topic])

async def _im_prompt(request):
    source = request.match_info["source"]
    if source not in IM_PROMPTS:
        return _plain(f"Unknown source: {source}. Available: {', '.join(IM_PROMPTS.keys())}", status=404)
    return _plain(IM_PROMPTS[source])

async def _list_subagents(request):
    return web.json_response({"items": pool.snapshot()})

async def _get_subagent(request):
    sid = request.match_info["sid"]
    max_len = int(request.query.get("max_len", "5000"))
    s = pool.get(sid)
    if not s:
        return web.json_response({"error": "not found"}, status=404)
    cleaned = clean_log_text(s.reply or "")
    return web.json_response({"id": s.id, "prompt": s.prompt, "status": s.status,
            "reply": cleaned[-max_len:] if len(cleaned) > max_len else cleaned,
            "created_at": s.created_at, "updated_at": s.updated_at})

INSTR_DISPATCHED = "Task received. I'll handle THIS TASK from here. You MUST to do other task or end your reply."

async def _start_subagent(request):
    body = await _json_body(request)
    prompt = body.get("prompt", "")
    result = pool.start_subagent(prompt)
    result["instruction"] = INSTR_DISPATCHED
    return web.json_response(result)

async def _subagent_action(request):
    sid = request.match_info["sid"]
    body = await _json_body(request)
    action = (body.get("action") or "intervene").lower().strip()
    msg = body.get("msg", "")
    s = pool.get(sid)
    if not s: return web.json_response({"error": "subagent not found", "id": sid}, status=404)
    if action == "keyinfo":
        result = pool.keyinfo_subagent(sid, msg)
        result["instruction"] = "Received. I'll incorporate this. You MUST to do other task or end your reply."
        return web.json_response(result)
    if action in ("input", "reply", "append", "message", "msg"):
        result = pool.input_subagent(sid, msg)
        result["instruction"] = INSTR_DISPATCHED
        return web.json_response(result)
    if action in ("abort", "stop"):
        s.agent.abort()
        s.status = "stopped"
        s.updated_at = int(time.time())
        task_history.record_done(sid, status_override="aborted")
        push_cards()
        return web.json_response({"id": sid, "status": "stopped"})
    return web.json_response({"error": f"unknown action: {body.get('action')}"}, status=400)

async def _get_chat(request):
    last = int(request.query.get("last", "20"))
    for m in chat_messages:
        if m.get("role") == "user" and not m.get("read"): m["read"] = True
    schedule_broadcast({"type": "chat_read"})
    return web.json_response({"items": chat_messages[-last:]})

async def _chat(request):
    body = await _json_body(request)
    return web.json_response(add_chat(body.get("msg", ""), role=body.get("role", "conductor")))

async def _approval(request):
    body = await _json_body(request)
    schedule_broadcast({"type": "approval", "item": {"id": short_id(), "prompt": body.get("prompt", ""), "source": body.get("source", "")}})
    return web.json_response({"ok": True})

async def _history(request):
    limit = min(int(request.query.get("limit", "50")), 200)
    offset = int(request.query.get("offset", "0"))
    q = request.query.get("q", "")
    return web.json_response(task_history.list(limit=limit, offset=offset, q=q))

async def _history_detail(request):
    rid = request.match_info["rid"]
    rec = task_history.get(rid)
    if not rec: return web.json_response({"error": "not found"}, status=404)
    return web.json_response(rec)

async def _history_resume(request):
    rid = request.match_info["rid"]
    rec = task_history.get(rid)
    if not rec: return web.json_response({"error": "not found"}, status=404)
    agent_id = rec.get("agent_id")
    s = pool.get(agent_id) if agent_id else None
    if s and s.status != "running":
        return web.json_response({"id": s.id, "recreated": False, "seed": None})
    return web.json_response({"id": None, "recreated": True,
            "seed": {"prompt": rec.get("prompt") or "", "reply": rec.get("reply") or ""}})

async def _history_clear(request):
    task_history.clear()
    return web.json_response({"ok": True})

async def _ws(request):
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    ws_clients.add(ws)
    try:
        running = any(s.status == "running" for s in pool.subagents.values())
        await ws.send_json({"type": "hello", "subagents": pool.snapshot(), "chat": chat_messages, "log": conductor.log, "running": running})
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data) if msg.data else {}
                except Exception:
                    data = {}
                m = (data.get("msg") or "").strip()
                if not m: continue
                add_chat(m, role="user", files=data.get("files") or [], images=data.get("images") or [])
                conductor.notify({"type": "user_message", "msg": m})
            elif msg.type == web.WSMsgType.ERROR:
                break
    except Exception:
        pass
    finally:
        ws_clients.discard(ws)
    return ws


def create_conductor_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/token-stats", _token_stats)
    app.router.add_get("/", _index)
    app.router.add_get("/readme", _readme)
    app.router.add_get("/readme/{topic}", _readme_topic)
    app.router.add_get("/im_prompt/{source}", _im_prompt)
    app.router.add_get("/subagent", _list_subagents)
    app.router.add_get("/subagent/{sid}", _get_subagent)
    app.router.add_post("/subagent", _start_subagent)
    app.router.add_post("/subagent/{sid}", _subagent_action)
    app.router.add_get("/chat", _get_chat)
    app.router.add_post("/chat", _chat)
    app.router.add_post("/approval", _approval)
    app.router.add_get("/history", _history)
    app.router.add_get("/history/{rid}", _history_detail)
    app.router.add_post("/history/{rid}/resume", _history_resume)
    app.router.add_delete("/history", _history_clear)
    app.router.add_get("/ws", _ws)
    return app


def init():
    """Start the conductor loop + IM poller. Safe to call once from the bridge
    process startup (on the running asyncio loop)."""
    global main_loop
    try:
        main_loop = asyncio.get_running_loop()
    except RuntimeError:
        main_loop = asyncio.get_event_loop()
    import cost_tracker; cost_tracker.install()
    conductor.start()
    threading.Thread(target=im_poll_loop, name="im-poller", daemon=True).start()
