import os, sys, subprocess
from urllib.request import urlopen
from urllib.parse import quote
if sys.stdout is None: sys.stdout = open(os.devnull, "w")
if sys.stderr is None: sys.stderr = open(os.devnull, "w")
try: sys.stdout.reconfigure(errors='replace')
except: pass
try: sys.stderr.reconfigure(errors='replace')
except: pass
script_dir = os.path.dirname(__file__)
sys.path.append(os.path.abspath(os.path.join(script_dir, '..')))
sys.path.append(os.path.abspath(script_dir))

import streamlit as st
import time, json, re, threading, queue
from datetime import timedelta
from agentmain import GeneraticAgent
import chatapp_common  # activate /continue command (monkey patches GeneraticAgent)
from continue_cmd import handle_frontend_command, reset_conversation, list_sessions, extract_ui_messages
from btw_cmd import handle_frontend_command as btw_handle_frontend
from export_cmd import last_assistant_text, export_to_temp, wrap_for_clipboard
from remote_channels import CHANNEL_REGISTRY, ChannelManager, get_channel

st.set_page_config(page_title="Cowork", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
[data-testid="stBottom"]{position:fixed!important;bottom:0!important;left:0!important;right:0!important;width:100vw!important;z-index:999;background:var(--background-color,#fff)}
@media (min-width:768px){[data-testid="stSidebar"][aria-expanded="true"]~div [data-testid="stBottom"]{left:300px!important;width:calc(100vw - 300px)!important}}
.stMainBlockContainer{padding-bottom:10rem!important}
.ga-channel-btn{position:fixed!important;top:100px!important;right:20px!important;z-index:9999!important;width:48px!important;height:48px!important;border-radius:50%!important;border:none!important;background:linear-gradient(135deg,#667eea 0%,#764ba2 100%)!important;color:white!important;font-size:20px!important;cursor:pointer!important;box-shadow:0 4px 15px rgba(102,126,234,0.4)!important;transition:transform 0.2s,box-shadow 0.2s!important;display:flex!important;align-items:center!important;justify-content:center!important}
.ga-channel-btn:hover{transform:scale(1.1)!important;box-shadow:0 6px 20px rgba(102,126,234,0.6)!important}
.ga-channel-btn:active{transform:scale(0.95)!important}
</style>
""", unsafe_allow_html=True)

# 注入 JS：在 Streamlit 三点菜单中添加"远控通道"菜单项
# st.html 直接渲染在页面 DOM 中（无 iframe），可以直接操作父页面
st.html("""
<div style="display:none" id="ga-js-anchor"></div>
<script>
(function(){
    const LABEL = '🔗 远控通道';

    function addFloatingButton() {
        if (document.querySelector('.ga-channel-btn')) return;
        const btn = document.createElement('button');
        btn.className = 'ga-channel-btn';
        btn.innerHTML = '🔗';
        btn.title = '远控通道';
        btn.addEventListener('click', function(e){
            e.preventDefault();
            e.stopPropagation();
            const loc = window.location;
            window.location.href = loc.origin + loc.pathname + '?channels=open';
        });
        document.body.appendChild(btn);
    }

    function isSettingsMenu(pop) {
        const hasSettings = pop.querySelector('[aria-label*="Settings"], [data-testid*="settings"], button[aria-label*="Settings"], button[data-testid*="settings"]');
        const hasMenuItems = pop.querySelector('ul');
        return hasSettings || hasMenuItems;
    }

    function inject() {
        const popovers = document.querySelectorAll('[data-baseweb="popover"], [role="menu"], [aria-haspopup="menu"]');
        for (const pop of popovers) {
            if (pop.querySelector('.ga-ch-item')) continue;
            if (!isSettingsMenu(pop)) continue;

            let ul = pop.querySelector('ul');
            if (!ul) {
                ul = pop.querySelector('ol');
            }
            if (!ul) {
                const list = pop.querySelector('[role="list"]');
                if (list) ul = list;
            }
            if (!ul) continue;

            const lis = ul.querySelectorAll(':scope > li, :scope > [role="menuitem"], :scope > [role="listitem"]');
            if (lis.length === 0) continue;

            const tpl = lis[lis.length - 1];

            const sep = document.createElement('li');
            sep.style.cssText = 'list-style:none;margin:0;padding:0;border-top:1px solid rgba(49,51,63,0.15);height:8px;';
            sep.setAttribute('role','separator');
            ul.appendChild(sep);

            const li = tpl.cloneNode(true);
            li.classList.add('ga-ch-item');

            li.querySelectorAll('a').forEach(a => {
                a.removeAttribute('href');
                a.style.cursor = 'pointer';
            });
            li.querySelectorAll('button').forEach(btn => {
                btn.removeAttribute('onclick');
                btn.removeAttribute('onmouseup');
            });

            const textEls = li.querySelectorAll('span, p, a, button');
            let done = false;
            textEls.forEach(el => {
                if (!done && el.childNodes.length <= 3 && el.textContent.trim()) {
                    el.textContent = LABEL;
                    done = true;
                }
            });
            if (!done) li.textContent = LABEL;

            li.style.cursor = 'pointer';
            li.addEventListener('click', function(e){
                e.preventDefault();
                e.stopPropagation();
                const loc = window.location;
                window.location.href = loc.origin + loc.pathname + '?channels=open';
            }, true);
            ul.appendChild(li);
        }
    }

    function scheduleInject() {
        inject();
        addFloatingButton();
        setTimeout(scheduleInject, 1000);
    }

    const obs = new MutationObserver(() => {
        requestAnimationFrame(inject);
        addFloatingButton();
    });

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => {
            addFloatingButton();
            scheduleInject();
            obs.observe(document.body, {childList: true, subtree: true});
        });
    } else {
        addFloatingButton();
        scheduleInject();
        obs.observe(document.body, {childList: true, subtree: true});
    }
})();
</script>
""", unsafe_allow_javascript=True)

LANG = os.environ.get('GA_LANG', 'zh')
if LANG not in ('zh', 'en'): LANG = 'zh'
I18N = {
    'zh': {
        'force_stop': '强行停止任务',
        'desktop_pet': '🐱 桌面宠物',
        'suggest_btn': '🎯 给我找点事做',
        'suggest_prompt': '按照自主行动的规划部分，充分分析我的情况，给我生成一批TODO，务必让我感兴趣',
        'auto_start': '开始空闲自主行动',
        'auto_pause': '⏸️ 禁止自主行动',
        'auto_enable': '▶️ 允许自主行动',
        'auto_on_cap': '🟢 自主行动运行中，会在你离开它30分钟后自动进行',
        'auto_off_cap': '🔴 自主行动已停止',
        'auto_prompt': '[AUTO]🤖 用户已经离开超过30分钟，作为自主智能体，请阅读自动化sop，执行自动任务。',
    },
    'en': {
        'force_stop': 'Force Stop',
        'desktop_pet': '🐱 Desktop Pet',
        'suggest_btn': '🎯 Suggest tasks',
        'suggest_prompt': 'Following the planning section of autonomous sop, analyze my situation thoroughly and generate a batch of TODOs that will interest me.',
        'auto_start': 'Start idle auto-action',
        'auto_pause': '⏸️ Pause auto-action',
        'auto_enable': '▶️ Enable auto-action',
        'auto_on_cap': '🟢 Auto-action enabled, triggers after 30min idle',
        'auto_off_cap': '🔴 Auto-action disabled',
        'auto_prompt': '[AUTO]🤖 User has been idle for over 30 minutes. As an autonomous agent, read the automation SOP and execute automatic tasks.',
    },
}
def T(key): return I18N.get(LANG, I18N['zh']).get(key, key)

@st.cache_resource
def init():
    agent = GeneraticAgent()
    if agent.llmclient is None:
        st.error("⚠️ Please set mykey.py!")
        st.stop()
    else: threading.Thread(target=agent.run, daemon=True).start()
    return agent

agent = init()

def build_prompt(objective):
    return f"""读取 {agent.log_path} 尾部，获取 agent 的最新输出。
用户的 loop 诉求：<objective>{objective}</objective>
判断该 agent 是否偷懒、是否真正完成诉求，用 <next_prompt></next_prompt> 输出要追加给它的指令：
一般复述 objective，或不超过 10 字的**督促**，如：别停，继续 / 这就叫最优？你优化到位了吗 / 看我要求，你达成了吗 / 你好好看清楚 / 你能不能看看记忆 / 把关键发现和阶段性成果落盘，然后继续
不允许促进 agent 停止或代替宣告任务完成，只允许催促不要对原任务进行评价，特别**禁止**“任务已完成，结束”这种让agent结束的指令，你的任务是让agent继续loop而非停止。
只输出 <next_prompt>…</next_prompt>，若需要停止则不要输出此tag。
"""

@st.cache_resource
def get_controller():
    b = {'ev': threading.Event(), 'obj': '', 'out': None, 'ready': False}
    def loop():
        ag = GeneraticAgent(); ag.verbose = False; ag.log_path = False
        threading.Thread(target=ag.run, daemon=True).start()
        while True:
            b['ev'].wait(); b['ev'].clear()
            dq = ag.put_task(build_prompt(b['obj']), source="controller")
            while 'done' not in (it := dq.get()): pass
            ms = re.findall(r'<next_prompt>(.*?)</next_prompt>', it['done'], re.S)
            b['out'] = ms[-1].strip() if ms else None; b['ready'] = True
    threading.Thread(target=loop, daemon=True).start(); return b

@st.cache_resource
def _get_channel_mgr():
    return ChannelManager()


def _get_wechat_qr_url():
    """调用微信接口获取二维码图片 URL，不阻塞主线程。"""
    import requests as _req
    _API = 'https://ilinkai.weixin.qq.com'
    for attempt in range(3):
        try:
            r = _req.get(f'{_API}/ilink/bot/get_bot_qrcode', params={'bot_type': 3},
                         headers={'User-Agent': 'openclaw-weixin/2.1.10'}, timeout=10)
            r.raise_for_status()
            d = r.json()
            if d.get('qrcode') and d.get('qrcode_img_content'):
                return d['qrcode_img_content'], d['qrcode']
        except Exception:
            pass
        time.sleep(2 ** attempt)
    return None, None


@st.dialog("🔗 远控通道", width="large")
def _show_channels_dialog():
    """以 dialog 形式展示远控通道管理界面。"""
    mgr = _get_channel_mgr()
    st.caption("选择并配置远控通道，通过社交软件与 Agent 交互")

    for row_start in range(0, len(CHANNEL_REGISTRY), 2):
        cols = st.columns(2)
        for col_idx, ch in enumerate(CHANNEL_REGISTRY[row_start:row_start + 2]):
            with cols[col_idx]:
                status = mgr.get_status(ch.key)
                badge_map = {"running": "🟢 运行中", "stopped": "🔴 已停止", "unconfigured": "暂未配置 Agent"}
                status_text = badge_map.get(status, "")
                with st.container(border=True):
                    hdr_cols = st.columns([3, 1])
                    with hdr_cols[0]:
                        rec_tag = " `推荐`" if ch.recommended else ""
                        st.markdown(f"**{ch.label}**{rec_tag}")
                    with hdr_cols[1]:
                        if status == "running":
                            if st.button("管理", key=f"dlg_manage_{ch.key}", use_container_width=True):
                                st.session_state[f'_ch_action_{ch.key}'] = 'manage'
                                st.rerun()
                        elif status == "stopped":
                            if st.button("启动", key=f"dlg_start_{ch.key}", use_container_width=True, type="primary"):
                                ok, msg = mgr.start(ch.key)
                                st.toast(f"{'✅' if ok else '❌'} {msg}"); st.rerun()
                        else:
                            if st.button("添加", key=f"dlg_add_{ch.key}", use_container_width=True):
                                st.session_state['_ch_adding'] = ch.key
                                st.rerun()
                    st.caption(ch.description)
                    st.markdown(f"<small style='color:gray'>{status_text}</small>", unsafe_allow_html=True)

    for ch in CHANNEL_REGISTRY:
        if st.session_state.get(f'_ch_action_{ch.key}') == 'manage':
            st.divider()
            st.markdown(f"**管理 {ch.label}**")
            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button(f"停止 {ch.label}", key=f"dlg_stop_{ch.key}"):
                    mgr.stop(ch.key); del st.session_state[f'_ch_action_{ch.key}']
                    st.toast(f"✅ {ch.label} 已停止"); st.rerun()
            with c2:
                if st.button(f"移除 {ch.label}", key=f"dlg_rm_{ch.key}"):
                    mgr.remove(ch.key); del st.session_state[f'_ch_action_{ch.key}']
                    st.toast(f"🗑️ 已移除"); st.rerun()
            with c3:
                if st.button("取消", key=f"dlg_cancel_{ch.key}"):
                    del st.session_state[f'_ch_action_{ch.key}']; st.rerun()

    adding_key = st.session_state.get('_ch_adding')
    if adding_key:
        ch = get_channel(adding_key)
        if ch:
            st.divider()
            st.markdown(f"**配置 {ch.label}**")
            if not ch.config_keys:
                st.info("请使用微信扫描下方二维码完成登录")
                with st.spinner("正在获取二维码..."):
                    qr_url, qr_id = _get_wechat_qr_url()
                if qr_url:
                    import qrcode as _qr, io as _io
                    qr_img = _qr.make(qr_url)
                    buf = _io.BytesIO()
                    qr_img.save(buf, format='PNG')
                    st.image(buf.getvalue(), caption="微信扫码登录", width=260)
                    st.caption("扫码后等待确认...")
                    if st.button("已扫码，启动通道", key="dlg_wx_confirm"):
                        mgr.configure(adding_key, {"qr_id": qr_id})
                        ok, msg = mgr.start(adding_key)
                        st.session_state.pop('_ch_adding', None)
                        st.toast(f"{'✅' if ok else '❌'} {msg}"); st.rerun()
                else:
                    st.error("获取二维码失败，请稍后重试")
                if st.button("取消", key="dlg_wx_cancel"):
                    st.session_state.pop('_ch_adding', None); st.rerun()
            else:
                vals = {}
                for cfg_key in ch.config_keys:
                    vals[cfg_key] = st.text_input(cfg_key, key=f"dlg_cfg_{ch.key}_{cfg_key}")
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("确认添加", key=f"dlg_cfm_{ch.key}", type="primary"):
                        if all(v.strip() for v in vals.values()):
                            config = {k: v.strip() for k, v in vals.items()}
                            mgr.configure(adding_key, config)
                            try:
                                from llmcore import mykeys
                                for k, v in config.items():
                                    mykeys[k] = v
                            except Exception:
                                pass
                            ok, msg = mgr.start(adding_key)
                            st.session_state.pop('_ch_adding', None)
                            st.toast(f"{'✅' if ok else '❌'} {msg}"); st.rerun()
                        else:
                            st.warning("请填写所有配置项")
                with c2:
                    if st.button("取消", key=f"dlg_cfg_cancel_{ch.key}"):
                        st.session_state.pop('_ch_adding', None); st.rerun()


# 远控通道入口：通过三点菜单注入的 JS 项 → 导航到 /?channels=open → 触发 dialog
_qp = st.query_params.to_dict()
if _qp.get("channels") == "open":
    st.query_params.clear()
    _show_channels_dialog()

st.title("🖥️ Cowork")

st.session_state.setdefault('autonomous_enabled', False)

@st.fragment
def render_sidebar():
    st.session_state.setdefault('autonomous_enabled', False)
    llm_options = agent.list_llms()
    current_idx = agent.llm_no
    llm_labels = {idx: f"{idx}: {(name or '').strip()}" for idx, name, _ in llm_options}
    st.caption(f"LLM Core: {llm_labels.get(current_idx, str(current_idx))}")
    selected_idx = st.selectbox("LLM", [idx for idx, _, _ in llm_options], index=next((i for i, (idx, _, _) in enumerate(llm_options) if idx == current_idx), 0), format_func=llm_labels.get, label_visibility="collapsed", key="sidebar_llm_select")
    if selected_idx != current_idx:
        agent.next_llm(selected_idx); st.rerun(scope="fragment")
    if st.button(T('force_stop')):
        agent.abort(); st.toast("Stop signal sended"); st.rerun()
    if st.button(T('desktop_pet')):
        kwargs = {'creationflags': 0x08} if sys.platform == 'win32' else {}
        pet_script = os.path.join(script_dir, 'desktop_pet_v2.pyw')
        if not os.path.exists(pet_script):
            st.error("desktop_pet_v2.pyw not found")
            return
        subprocess.Popen([sys.executable, pet_script], **kwargs)
        def _pet_req(q):
            def _do():
                try: urlopen(f'http://127.0.0.1:41983/?{q}', timeout=2)
                except Exception: pass
            threading.Thread(target=_do, daemon=True).start()
        agent._pet_req = _pet_req
        if not hasattr(agent, '_turn_end_hooks'): agent._turn_end_hooks = {}
        def _pet_hook(ctx):
            parts = [f"Turn {ctx.get('turn','?')}"]
            if ctx.get('summary'): parts.append(ctx['summary'])
            if ctx.get('exit_reason'): parts.append('DONE')
            _pet_req(f'msg={quote(chr(10).join(parts))}')
            if ctx.get('exit_reason'): _pet_req('state=idle')
        agent._turn_end_hooks['pet'] = _pet_hook
        st.toast("Desktop pet started")
    
    if st.button(T('suggest_btn')):
        st.session_state['_inject_prompt'] = T('suggest_prompt')
        st.rerun(scope="app")
    st.divider()
    st.markdown("""<style>
    [data-testid="stSidebar"] .stTextArea textarea {
        field-sizing: content; min-height: 1.6em !important; height: auto !important;
    }
    </style>""", unsafe_allow_html=True)
    st.text_area("Loop prompt", value=st.session_state.get('loop_prompt_input', "继续" if LANG=='zh' else 'next'), key="loop_prompt_input", height=1)
    if st.session_state.get('loop_enabled'):
        if st.button("⏹️ Stop Loop"):
            st.session_state.loop_enabled = False
            st.toast("⏹️ Loop stopped"); st.rerun(scope="app")
        st.caption("🔁 Looping")
    else:
        if st.button("🔁 Loop!"):
            st.session_state.loop_enabled = True
            get_controller()
            st.session_state['_inject_prompt'] = st.session_state.get('loop_prompt_input', '')
            st.toast("🔁 Looping"); st.rerun(scope="app")
    st.divider()
    if st.button(T('auto_start')):
        st.session_state.last_reply_time = int(time.time()) - 1800
        st.session_state.autonomous_enabled = True
        st.rerun(scope="app")
    if st.session_state.autonomous_enabled:
        if st.button(T('auto_pause')):
            st.session_state.autonomous_enabled = False
            st.toast(T('auto_pause')); st.rerun(scope="app")
        st.caption(T('auto_on_cap'))
    else:
        if st.button(T('auto_enable'), type="primary"):
            st.session_state.autonomous_enabled = True
            st.toast("✅"); st.rerun(scope="app")
        st.caption(T('auto_off_cap'))

with st.sidebar: render_sidebar()

def fold_turns(text):
    """Return list of segments: [{'type':'text','content':...}, {'type':'fold','title':...,'content':...}]"""
    # 先把4+反引号块替换为占位符，避免误切子agent嵌套的 LLM Running
    _ph = []
    safe = re.sub(r'`{4,}.*?`{4,}', lambda m: (_ph.append(m.group(0)), f'\x00PH{len(_ph)-1}\x00')[1], text, flags=re.DOTALL)
    # 流式中间态：末尾可能有未闭合的4+反引号块，也需保护
    safe = re.sub(r'`{4,}[^`].*$', lambda m: (_ph.append(m.group(0)), f'\x00PH{len(_ph)-1}\x00')[1], safe, flags=re.DOTALL)
    parts = re.split(r'(\**LLM Running \(Turn \d+\) \.\.\.\*\**)', safe)
    parts = [re.sub(r'\x00PH(\d+)\x00', lambda m: _ph[int(m.group(1))], p) for p in parts]
    if len(parts) < 4: return [{'type': 'text', 'content': text}]
    segments = []
    if parts[0].strip(): segments.append({'type': 'text', 'content': parts[0]})
    turns = []
    for i in range(1, len(parts), 2):
        marker = parts[i]
        content = parts[i+1] if i+1 < len(parts) else ''
        turns.append((marker, content))
    for idx, (marker, content) in enumerate(turns):
        if idx < len(turns) - 1:
            _c = re.sub(r'`{3,}.*?`{3,}|<thinking>.*?</thinking>', '', content, flags=re.DOTALL)
            matches = re.findall(r'<summary>\s*((?:(?!<summary>).)*?)\s*</summary>', _c, re.DOTALL)
            if matches:
                title = matches[0].strip()
                title = title.split('\n')[0]
                if len(title) > 50: title = title[:50] + '...'
            else:
                _plain = _c.strip().split('\n', 1)[0]
                title = (_plain[:50] + '...') if len(_plain) > 50 else (_plain or marker.strip('*'))
            segments.append({'type': 'fold', 'title': title, 'content': content})
        else: segments.append({'type': 'text', 'content': marker + content})
    return segments
_SUMMARY_TAG_RE = re.compile(r'<summary>.*?</summary>\s*', re.DOTALL)

def render_segments(segments, suffix=''):
    # 整块重画：调用方用 slot.container() 包裹，保证 DOM 路径稳定、跨 rerun 对齐（消除"灰色重影"）。
    # heartbeat 空转时 segments 不变 → Streamlit 后端 diff 无变化 → 前端零闪烁；
    # 但 container/markdown 本身是 API 调用，StopException 仍会被抛出（abort 照常起作用）。
    for seg in segments:
        if seg['type'] == 'fold':
            with st.expander(seg['title'], expanded=False): st.markdown(seg['content'])
        else:
            st.markdown(seg['content'] + suffix)

def agent_backend_stream(prompt=None):
    """Drain main task display_queue.
    - prompt given:  start a fresh task; new dq is kept in session_state.
    - prompt is None: resume a dq left in session_state by a prior run (e.g. after /btw).
    Per-chunk progress is mirrored to session_state.partial_response so the rendered
    bubble survives reruns. No implicit agent.abort() — explicit stop is on the Stop button."""
    if prompt is not None:
        st.session_state.display_queue = agent.put_task(prompt, source="user")
        st.session_state.partial_response = ''
    dq = st.session_state.get('display_queue')
    if dq is None: return
    # Drop a dangling 'LLM Running (Turn N) ...' marker if the captured partial
    # ended right at a turn boundary with no content yet — otherwise the resume
    # bubble flashes as a marker-only gray line. The marker reappears with
    # content on the next chunk (raw_resp is cumulative).
    response = re.sub(r'\**LLM Running \(Turn \d+\) \.\.\.\**\s*$',
                      '', st.session_state.get('partial_response', '')).rstrip()
    try:
        while True:
            try: item = dq.get(timeout=1)
            except queue.Empty:
                yield response   # heartbeat: let outer st.markdown() run → Streamlit checks StopException
                continue
            if 'next' in item:
                response = item['next']
                st.session_state.partial_response = response
                yield response
            if 'done' in item:
                st.session_state.display_queue = None
                st.session_state.partial_response = ''
                yield item['done']; break
    finally:
        agent.abort()
        try:
            st.session_state.display_queue = None
            st.session_state.partial_response = ''
        except BaseException:
            pass


def render_main_stream(prompt=None):
    """Render the assistant bubble for the main task (new or resumed). Saves final to messages."""
    with st.chat_message("assistant"):
        frozen = 0; live = st.empty(); response = ''
        CURSOR = ' ▌'
        for response in agent_backend_stream(prompt):
            segs = fold_turns(response)
            n_done = max(0, len(segs) - 1)
            while frozen < n_done:
                with live.container(): render_segments([segs[frozen]])
                live = st.empty(); frozen += 1
            with live.container(): render_segments([segs[-1]], suffix=CURSOR)   # live 区域
        segs = fold_turns(response)
        for i in range(frozen, len(segs)):
            with live.container(): render_segments([segs[i]])
            if i < len(segs) - 1: live = st.empty()
    if response:
        st.session_state.messages.append({"role": "assistant", "content": response})
        st.session_state.last_reply_time = int(time.time())
        # ── 循环回调：回答完成戳醒 controller 决策(去程,现取最新objective) ──
        if st.session_state.get('loop_enabled'):
            b = get_controller()
            b['obj'] = st.session_state.get('loop_prompt_input', ''); b['ready'] = False; b['ev'].set()

if "messages" not in st.session_state: st.session_state.messages = []
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        # 用 slot=st.empty() + with slot.container(): ... 的外壳，DOM 路径和流式渲染完全一致，跨 rerun 对齐
        slot = st.empty()
        with slot.container():
            if msg["role"] == "assistant": render_segments(fold_turns(msg["content"]))
            else: st.markdown(msg["content"])

# Scroll-height ghost fix: during streaming, expander open/close mid-animation can leave
# phantom height → scrollbar long but can't scroll to bottom. Periodically detect & reflow.
try:
    from streamlit import iframe as _st_iframe  # 1.56+
    _embed_html = lambda html, **kw: _st_iframe(html, **{k: max(v, 1) if isinstance(v, int) else v for k, v in kw.items()})
except (ImportError, AttributeError):
    from streamlit.components.v1 import html as _embed_html  # ≤1.55
# IME composition fix (macOS only) - prevents Enter from submitting during CJK input
_js_ime_fix = ("" if os.name == 'nt' else
    "!function(){if(window.parent.__imeFix)return;window.parent.__imeFix=1;"
    "var d=window.parent.document,c=0;"
    "d.addEventListener('compositionstart',()=>c=1,!0);"
    "d.addEventListener('compositionend',()=>c=0,!0);"
    "function f(){d.querySelectorAll('textarea[data-testid=stChatInputTextArea]')"
    ".forEach(t=>{t.__imeFix||(t.__imeFix=1,t.addEventListener('keydown',e=>{"
    "e.key==='Enter'&&!e.shiftKey&&(e.isComposing||c||e.keyCode===229)&&"
    "(e.stopImmediatePropagation(),e.preventDefault())},!0))})}"
    "f();new MutationObserver(f).observe(d.body,{childList:1,subtree:1})}()")
_embed_html(f'<script>{_js_ime_fix}</script>', height=0)

_injected = st.session_state.pop('_inject_prompt', None)
prompt = st.chat_input("any task?") or _injected
if prompt:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    cmd = (prompt or "").strip()
    def _reset_and_rerun():
        st.session_state.streaming = False
        st.session_state.stopping = False
        st.session_state.display_queue = None
        st.session_state.partial_response = ""
        st.session_state.reply_ts = ""
        st.session_state.current_prompt = ""
        st.session_state.last_reply_time = int(time.time())
        st.rerun()
    if cmd == "/new":
        st.session_state.messages = [{"role": "assistant", "content": reset_conversation(agent), "time": ts}]
        _reset_and_rerun()
    if cmd.startswith("/continue"):
        m = re.match(r'/continue\s+(\d+)\s*$', cmd.strip())
        sessions = list_sessions(exclude_pid=os.getpid()) if m else []
        idx = int(m.group(1)) - 1 if m else -1
        # Resolve target path BEFORE handle (which snapshots current log, shifting indices).
        target = sessions[idx][0] if 0 <= idx < len(sessions) else None
        result = handle_frontend_command(agent, cmd)
        history = extract_ui_messages(target) if target and result.startswith('✅') else None
        tail = [{"role": "assistant", "content": result, "time": ts}]
        if history: st.session_state.messages = history + tail
        else: st.session_state.messages = list(st.session_state.messages)+[{"role": "user", "content": cmd, "time": ts}]+tail
        _reset_and_rerun()
    if cmd.startswith("/btw"):
        answer = btw_handle_frontend(agent, cmd)  # sync; bypasses put_task → main agent.run() untouched
        st.session_state.messages = list(st.session_state.messages) + [
            {"role": "user", "content": prompt, "time": ts},
            {"role": "assistant", "content": answer, "time": ts},
        ]
        st.rerun()  # preserve display_queue/partial_response so resume path drains the running main task
    if cmd.startswith("/export"):
        parts = cmd.split(maxsplit=1)
        sub = parts[1].strip() if len(parts) > 1 else ""
        sub_lower = sub.lower()
        if not sub:
            result = (
                "**选择导出方式：**\n\n"
                "- `/export clip` — 整理到代码块中\n"
                "- `/export <文件名>` — 导出到 `temp/<文件名>`（默认 .md 后缀）\n"
                "- `/export all` — 显示完整对话日志路径"
            )
        elif sub_lower == "all":
            log = agent.log_path
            result = (f"📂 完整对话日志:\n\n`{log}`" if os.path.isfile(log)
                      else f"❌ 当前会话尚无日志文件")
        else:
            text = last_assistant_text(agent)
            if not text:
                result = "❌ 还没有模型回复可导出"
            elif sub_lower in ("clip", "copy"):
                result = f"📋 最后一轮回复（点代码块右上角 📋 复制）:\n\n{wrap_for_clipboard(text)}"
            else:
                try:
                    path = export_to_temp(text, sub)
                    result = f"✅ 已导出:\n\n`{path}`"
                except Exception as e:
                    result = f"❌ 导出失败: {e}"
        st.session_state.messages = list(st.session_state.messages) + [
            {"role": "user", "content": cmd, "time": ts},
            {"role": "assistant", "content": result, "time": ts},
        ]
        _reset_and_rerun()
    # Regular prompt: any in-flight task will be aborted by the finally block in
    # agent_backend_stream when StopException interrupts the prior generator.
    st.session_state.messages.append({"role": "user", "content": prompt})
    if hasattr(agent, '_pet_req') and not prompt.startswith('/'): agent._pet_req('state=walk')
    with st.chat_message("user"): st.markdown(prompt)
    render_main_stream(prompt)
elif st.session_state.get('display_queue') is not None:
    # No new prompt but a task is mid-flight (typically a /btw rerun) — resume drain.
    render_main_stream()

# ── 空闲自主行动：fragment 定时检测，替代 launch.pyw 的 idle_monitor ──
@st.fragment(run_every=timedelta(minutes=1))
def _idle_checker():
    if st.session_state.get('loop_enabled'):
        b = get_controller()
        if b['ready']:
            b['ready'] = False
            if b['out'] and '停止循环' not in b['out']: st.session_state['_inject_prompt'] = b['out']
            else: st.session_state.loop_enabled = False
            st.rerun(scope="app")
        return
    if not st.session_state.get('autonomous_enabled'): return
    if st.session_state.get('display_queue') is not None: return   # 正在运行中
    last = st.session_state.get('last_reply_time', int(time.time()))
    if time.time() - last > 1800:
        st.session_state['_inject_prompt'] = T('auto_prompt')
        st.session_state['last_reply_time'] = int(time.time())     # 防重入
        st.rerun(scope="app")
_idle_checker()
