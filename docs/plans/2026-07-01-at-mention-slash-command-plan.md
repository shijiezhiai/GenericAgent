# @ 文件选择 & / 命令面板 实现计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 为 GA 桌面端仿照 Claude Code 增加 @ 文件选择和 / 命令面板能力

**Architecture:** 在 `app.js` 中新增 @panel 和 /panel UI 逻辑，在 `desktop_bridge.py` 中新增 3 个 API 端点，在 `slash_cmds.py` 中新增 `list_all_commands()` 函数。面板为绝对定位浮层，附着在 composer-inset 内部。

**Tech Stack:** 原生 HTML/CSS/JS (无框架), Python aiohttp (bridge), Python pathlib (文件操作)

**Design doc:** `docs/plans/2026-07-01-at-mention-slash-command-design.md`

---

### Task 1: `slash_cmds.py` — 新增 `list_all_commands()` 函数

**Files:**
- Modify: `frontends/slash_cmds.py` (追加到 PALETTE_ENTRIES 之后，prompt_for 之前)

**Step 1: 添加 `list_all_commands()` 函数**

在 `PALETTE_ENTRIES` 定义之后（约 L594 后）、`prompt_for()` 之前，添加：

```python
import os
from pathlib import Path

# ---- skill / command discovery for desktop slash panel ----

# Additional SOP files not covered by PALETTE_ENTRIES but are valid skills
_SOP_SKILL_MAP: dict[str, tuple[str, str]] = {
    "review_sop.md": ("/review", "深度审查 — 按 code_review_principles.md 审查代码"),
    "plan_sop.md":   ("/plan",   "进入 Plan 模式：探索→规划→执行→验证"),
}

def list_all_commands(ga_root: Optional[str] = None) -> list[dict]:
    """Return all slash-commands available for the desktop / palette.

    Returns a list of dicts: {name, label, desc, group}
    - Skills: from PALETTE_ENTRIES + memory/*_sop.md scan
    - Actions: hardcoded UI-level commands
    """
    commands: list[dict] = []

    # --- skills from PALETTE_ENTRIES ---
    seen_names: set[str] = set()
    for cmd, hint, desc in PALETTE_ENTRIES:
        label = cmd.lstrip("/")
        if hint:
            label = f"{label} {hint}"
        commands.append({"name": cmd, "label": label, "desc": desc, "group": "skills"})
        seen_names.add(cmd)

    # --- skills from memory/*_sop.md scan ---
    if ga_root:
        mem_dir = Path(ga_root) / "memory"
        if mem_dir.is_dir():
            for sop_path in sorted(mem_dir.glob("*_sop.md")):
                fname = sop_path.name
                if fname in _SOP_SKILL_MAP:
                    cmd, desc = _SOP_SKILL_MAP[fname]
                    if cmd not in seen_names:
                        commands.append({"name": cmd, "label": cmd, "desc": desc, "group": "skills"})
                        seen_names.add(cmd)
                else:
                    # auto-derive from first heading
                    try:
                        first_line = sop_path.read_text(encoding="utf-8").split("\n", 1)[0].strip()
                    except Exception:
                        first_line = ""
                    skill_name = sop_path.stem.replace("_sop", "")
                    label = first_line.lstrip("# ").strip() or skill_name
                    cmd = f"/{skill_name}"
                    if cmd not in seen_names:
                        commands.append({"name": cmd, "label": label, "desc": label, "group": "skills"})
                        seen_names.add(cmd)

    # --- UI action commands ---
    actions = [
        ("/new",      "新建会话", "创建新的对话会话"),
        ("/clear",    "清空消息", "清空当前会话消息列表"),
        ("/stop",     "停止运行", "停止当前正在运行的 agent"),
        ("/settings", "设置",     "打开设置面板"),
        ("/help",     "帮助",     "显示可用命令列表"),
    ]
    for name, label, desc in actions:
        commands.append({"name": name, "label": label, "desc": desc, "group": "actions"})

    return commands
```

**Step 2: 验证导入**

```bash
cd /Users/mayangyang/code/ai/GenericAgent && python3 -c "from frontends.slash_cmds import list_all_commands; cmds=list_all_commands('.'); print(len(cmds), 'commands'); [print(c['name'], c['group']) for c in cmds]"
```
Expected: 输出 12+ 条 commands，包含 skills 和 actions 分组。

---

### Task 2: `desktop_bridge.py` — 新增 `/api/files/list` 端点

**Files:**
- Modify: `frontends/desktop_bridge.py`

**Step 1: 添加 handler 函数**

在 `desktop_bridge.py` 的 handler 区域（`path_open_handler` 之后约 L1429），添加：

```python
# ── @ file mention: list files ────────────────────────────────────────

_IGNORED_DIRS = {'.git', 'node_modules', '__pycache__', 'venv', '.venv',
                 '.DS_Store', '.idea', '.vscode', 'dist', 'build', '.next',
                 '.nuxt', 'target', 'egg-info', '.eggs'}

async def files_list_handler(request):
    """GET /api/files/list?path={dir}&filter={optional}"""
    dir_path = request.query.get("path", "")
    filter_str = (request.query.get("filter") or "").lower()
    if not dir_path:
        return json_err("path is required", status=400)
    p = Path(dir_path)
    if not p.is_dir():
        return json_err(f"not a directory: {dir_path}", status=400)
    entries: list[dict] = []
    try:
        for child in sorted(p.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
            name = child.name
            # skip hidden/dot-files
            if name.startswith('.') and name not in ('.env', '.env.local', '.gitignore'):
                continue
            if child.is_dir() and name in _IGNORED_DIRS:
                continue
            if filter_str and filter_str not in name.lower():
                continue
            entries.append({
                "name": name,
                "type": "dir" if child.is_dir() else "file",
                "path": str(child),
            })
            if len(entries) >= 50:
                break
    except PermissionError:
        return json_err("permission denied", status=403)
    except Exception as e:
        return json_err(str(e), status=500)
    return json_ok({"path": dir_path, "entries": entries})
```

**Step 2: 注册路由**

在 `create_app()` 函数的 `app.router.add_*` 区域（workspace routes 之后约 L2016），添加：

```python
    # @ mention & slash command APIs
    app.router.add_get("/api/files/list", files_list_handler)
```

**Step 3: 验证**

```bash
cd /Users/mayangyang/code/ai/GenericAgent && python3 -c "
from frontends.desktop_bridge import files_list_handler
print('OK: files_list_handler defined')
"
```
Expected: `OK: files_list_handler defined`

---

### Task 3: `desktop_bridge.py` — 新增 `/api/commands` 端点

**Files:**
- Modify: `frontends/desktop_bridge.py`

**Step 1: 添加 handler 函数**

在 `files_list_handler` 之后添加：

```python
async def commands_list_handler(request):
    """GET /api/commands → list available slash commands."""
    try:
        from frontends import slash_cmds
        cmds = slash_cmds.list_all_commands(str(manager.ga_root))
    except Exception as e:
        return json_err(str(e), status=500)
    return json_ok({"commands": cmds})
```

**Step 2: 注册路由**

```python
    app.router.add_get("/api/commands", commands_list_handler)
```

**Step 3: 验证**

```bash
cd /Users/mayangyang/code/ai/GenericAgent && python3 -c "
from frontends.desktop_bridge import commands_list_handler
print('OK')
"
```

---

### Task 4: `desktop_bridge.py` — 新增 `POST /session/{sid}/slash` 端点

**Files:**
- Modify: `frontends/desktop_bridge.py`

**Step 1: 添加 handler 函数**

在 `commands_list_handler` 之后添加：

```python
async def slash_handler(request):
    """POST /session/{sid}/slash → execute a skill command via prompt injection."""
    sid = request.match_info["sid"]
    data = await read_json(request)
    cmd = (data or {}).get("cmd", "").strip()
    args = (data or {}).get("args", "")
    if not cmd:
        return json_err("cmd is required", status=400)
    try:
        from frontends import slash_cmds
        injected = slash_cmds.prompt_for(cmd, args)
    except Exception as e:
        return json_err(str(e), status=500)
    if injected is None:
        return json_err(f"unknown command: {cmd}", status=400)
    # Send injected prompt to agent (same flow as /prompt)
    display = cmd + (" " + args if args else "")
    files_meta = (data or {}).get("files") or []
    image_metas = (data or {}).get("imageMetas") or []
    llm_no = (data or {}).get("llmNo")
    if llm_no is not None:
        llm_no = int(llm_no)
    return json_ok(manager.submit_prompt(sid, injected, [], llm_no=llm_no,
                                          display=display, files_meta=files_meta,
                                          image_metas=image_metas))
```

**Step 2: 注册路由**

```python
    app.router.add_post("/session/{sid}/slash", slash_handler)
```

**Step 3: 验证**

```bash
cd /Users/mayangyang/code/ai/GenericAgent && python3 -c "
from frontends.desktop_bridge import slash_handler
print('OK')
"
```

---

### Task 5: `index.html` — 添加两个面板的 DOM 结构

**Files:**
- Modify: `frontends/desktop/static/index.html`

**Step 1: 添加 @ 文件面板**

在 `#chat-composer` 的 composer-slot 内部、composer-inset 之前（约 L163），添加：

```html
<!-- @ file mention panel -->
<div class="at-panel" id="at-panel" hidden>
  <div class="at-panel-inner">
    <div class="at-search"><input type="text" id="at-search-input" placeholder="Search files..." autocomplete="off" /></div>
    <div class="at-list" id="at-list"></div>
    <div class="at-footer"><button type="button" class="at-browse-btn" id="at-browse-btn">Browse files...</button></div>
  </div>
</div>
```

**Step 2: 添加 / 命令面板**

在 @ 面板之后添加：

```html
<!-- / slash command panel -->
<div class="slash-panel" id="slash-panel" hidden>
  <div class="slash-panel-inner">
    <div class="slash-search"><input type="text" id="slash-search-input" placeholder="Search commands..." autocomplete="off" /></div>
    <div class="slash-groups" id="slash-groups"></div>
  </div>
</div>
```

---

### Task 6: `styles.css` — 面板样式

**Files:**
- Modify: `frontends/desktop/static/styles.css`

**Step 1: 在文件末尾追加样式**

```css
/* ── @ file mention panel ─────────────────────────────────────────── */
.at-panel{
  position:absolute; bottom:100%; left:0; right:0;
  margin-bottom:6px; z-index:100;
  background:var(--card); border:1px solid var(--line); border-radius:var(--radius);
  box-shadow:0 8px 24px rgba(0,0,0,.18); max-height:320px; display:flex; flex-direction:column;
}
.at-panel-inner{ display:flex; flex-direction:column; overflow:hidden; border-radius:var(--radius); }
.at-search{ padding:6px 8px; border-bottom:1px solid var(--line); }
.at-search input{
  width:100%; border:none; background:var(--bg); color:var(--txt);
  padding:6px 8px; font-size:13px; border-radius:4px; outline:none; font-family:inherit;
}
.at-search input::placeholder{ color:var(--dim); }
.at-list{ flex:1; overflow-y:auto; padding:4px 0; max-height:220px; }
.at-item{
  padding:5px 12px; font-size:13px; cursor:pointer; display:flex; align-items:center; gap:6px;
  white-space:nowrap; overflow:hidden; text-overflow:ellipsis; color:var(--txt);
}
.at-item:hover, .at-item.active{ background:var(--hover); }
.at-item .at-icon{ font-size:14px; flex-shrink:0; }
.at-item .at-path{ font-size:11px; color:var(--dim); margin-left:4px; overflow:hidden; text-overflow:ellipsis; }
.at-footer{ padding:4px 8px; border-top:1px solid var(--line); }
.at-browse-btn{
  width:100%; border:none; background:transparent; color:var(--accent); cursor:pointer;
  padding:6px 8px; font-size:12px; text-align:center; border-radius:4px; font-family:inherit;
}
.at-browse-btn:hover{ background:var(--hover); }
.at-empty{ padding:20px 12px; text-align:center; color:var(--dim); font-size:12px; }

/* ── / slash command panel ────────────────────────────────────────── */
.slash-panel{
  position:absolute; bottom:100%; left:0; right:0;
  margin-bottom:6px; z-index:100;
  background:var(--card); border:1px solid var(--line); border-radius:var(--radius);
  box-shadow:0 8px 24px rgba(0,0,0,.18); max-height:360px; display:flex; flex-direction:column;
}
.slash-panel-inner{ display:flex; flex-direction:column; overflow:hidden; border-radius:var(--radius); }
.slash-search{ padding:6px 8px; border-bottom:1px solid var(--line); }
.slash-search input{
  width:100%; border:none; background:var(--bg); color:var(--txt);
  padding:6px 8px; font-size:13px; border-radius:4px; outline:none; font-family:inherit;
}
.slash-search input::placeholder{ color:var(--dim); }
.slash-groups{ flex:1; overflow-y:auto; padding:4px 0; max-height:280px; }
.slash-group-label{
  padding:6px 12px 2px; font-size:11px; font-weight:600; color:var(--dim);
  text-transform:uppercase; letter-spacing:.5px;
}
.slash-item{
  padding:6px 12px; font-size:13px; cursor:pointer; display:flex; align-items:center; gap:8px;
  color:var(--txt);
}
.slash-item:hover, .slash-item.active{ background:var(--hover); }
.slash-item .slash-cmd{ font-weight:600; color:var(--accent); min-width:60px; }
.slash-item .slash-desc{ color:var(--dim); font-size:12px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.slash-empty{ padding:20px 12px; text-align:center; color:var(--dim); font-size:12px; }
```

---

### Task 7: `app.js` — @ 文件选择面板核心逻辑

**Files:**
- Modify: `frontends/desktop/static/app.js`

**Step 1: 在 `handleSlash` 之后（约 L3493），添加常量定义**

```js
/* ═══════════════ @ 文件选择 & / 命令面板 ═══════════════ */

const AT_PANEL = document.getElementById('at-panel');
const AT_LIST = document.getElementById('at-list');
const AT_SEARCH = document.getElementById('at-search-input');
const AT_BROWSE = document.getElementById('at-browse-btn');
const SLASH_PANEL = document.getElementById('slash-panel');
const SLASH_GROUPS = document.getElementById('slash-groups');
const SLASH_SEARCH = document.getElementById('slash-search-input');

let _atFiles = [];
let _atIdx = -1;
let _atActive = false;
let _slashCmds = [];
let _slashIdx = -1;
let _slashActive = false;
```

**Step 2: 添加面板定位辅助函数**

```js
function panelParentEl(ctx = 'chat') {
  const root = composerRootEl(ctx);
  return root?.querySelector('.composer-slot') || root;
}

function setPanelPosition(panel) {
  if (!panel || panel.hidden) return;
  // panel is absolutely positioned inside composer-slot, bottom:100% puts it above inset
}
```

**Step 3: 添加 @ 面板核心函数**

```js
// ── @ panel ─────────────────────────────────────────────────────────

function getWorkspacePath() {
  const sess = activeSess();
  if (sess && sess._workspacePath) return sess._workspacePath;
  return state.lastWorkspacePath || '';
}

async function fetchFiles(dirPath, filter) {
  let url = `${BRIDGE_ORIGIN}/api/files/list?path=${encodeURIComponent(dirPath)}`;
  if (filter) url += `&filter=${encodeURIComponent(filter)}`;
  try {
    const res = await fetch(url);
    const j = await res.json();
    return j.ok ? j : { ok: false, entries: [] };
  } catch (e) { return { ok: false, entries: [] }; }
}

function renderAtList(files) {
  if (!files.length) {
    AT_LIST.innerHTML = '<div class="at-empty">No files found</div>';
    _atIdx = -1;
    return;
  }
  _atIdx = Math.min(_atIdx, files.length - 1);
  AT_LIST.innerHTML = files.map((f, i) => {
    const icon = f.type === 'dir' ? '📁' : '📄';
    const cls = 'at-item' + (i === _atIdx ? ' active' : '');
    return `<div class="${cls}" data-idx="${i}"><span class="at-icon">${icon}</span>${f.name}<span class="at-path">${f.path}</span></div>`;
  }).join('');
}

async function showAtPanel() {
  hideSlashPanel();
  const dir = getWorkspacePath() || state.homeDir || '/';
  const res = await fetchFiles(dir, '');
  _atFiles = res.entries || [];
  _atIdx = _atFiles.length > 0 ? 0 : -1;
  renderAtList(_atFiles);
  if (AT_SEARCH) AT_SEARCH.value = '';
  AT_PANEL.hidden = false;
  _atActive = true;
}

function hideAtPanel() {
  AT_PANEL.hidden = true;
  _atActive = false;
  _atFiles = [];
  _atIdx = -1;
}

function selectAtItem() {
  if (_atIdx < 0 || _atIdx >= _atFiles.length) return;
  const f = _atFiles[_atIdx];
  insertAtChip(f);
  hideAtPanel();
}

function insertAtChip(f) {
  const input = composerCfg('chat').input;
  if (!input) return;
  const seq = ++state.fileSeq;
  const isDir = f.type === 'dir';
  const kind = isDir ? '@folder' : '@file';
  state.pendingFiles.push({
    sid: seq, name: f.name, path: f.path, isImage: false,
    isAtMention: true, atKind: kind, ctx: 'chat',
  });
  const chip = document.createElement('span');
  chip.className = 'ph-chip';
  chip.contentEditable = 'false';
  chip.dataset.sid = seq;
  chip.dataset.kind = kind;
  chip.innerHTML = isDir ? `[Folder #${seq}]` : `[@${f.name}]`;
  // Insert at cursor or append
  insertAtCursorOrEnd(input, chip);
  // add trailing space
  const space = document.createTextNode(' ');
  chip.after(space);
  input.dispatchEvent(new Event('input', { bubbles: true }));
}

function insertAtCursorOrEnd(input, node) {
  const sel = window.getSelection();
  if (sel && sel.rangeCount && input.contains(sel.anchorNode)) {
    const range = sel.getRangeAt(0);
    range.deleteContents();
    range.insertNode(node);
    range.setStartAfter(node);
    sel.removeAllRanges();
    sel.addRange(range);
  } else {
    input.appendChild(node);
  }
}

AT_SEARCH?.addEventListener('input', async () => {
  const q = AT_SEARCH.value.trim();
  const dir = getWorkspacePath() || state.homeDir || '/';
  const res = await fetchFiles(dir, q);
  _atFiles = res.entries || [];
  _atIdx = _atFiles.length > 0 ? 0 : -1;
  renderAtList(_atFiles);
});

AT_LIST?.addEventListener('click', (e) => {
  const item = e.target.closest('.at-item');
  if (!item) return;
  _atIdx = Number(item.dataset.idx);
  selectAtItem();
});

AT_BROWSE?.addEventListener('click', async () => {
  hideAtPanel();
  // Use Tauri pick_folder if available, otherwise fallback to input
  try {
    const picked = await window.__TAURI__?.core?.invoke?.('pick_folder');
    if (picked) {
      // Treat picked dir as a folder chip
      const name = picked.split('/').pop() || picked;
      insertAtChip({ name, path: picked, type: 'dir' });
      return;
    }
  } catch (_) {}
  // Fallback: open system file dialog via hidden input
  const inp = document.createElement('input');
  inp.type = 'file';
  inp.webkitdirectory = true;
  inp.onchange = () => {
    const files = inp.files;
    if (files && files.length > 0) {
      const p = files[0].webkitRelativePath || files[0].name;
      insertAtChip({ name: p.split('/')[0], path: (files[0].path || p), type: 'dir' });
    }
  };
  inp.click();
});
```

**Step 4: 添加键盘事件处理（在 composer keydown 中集成）**

在现有的 `inputEl.addEventListener('keydown', ...)` 中（L3460），扩展为：

```js
inputEl.addEventListener('keydown', (e) => {
  // Panel navigation
  if (_atActive) {
    if (e.key === 'ArrowDown') { e.preventDefault(); _atIdx = Math.min(_atIdx + 1, _atFiles.length - 1); renderAtList(_atFiles); return; }
    if (e.key === 'ArrowUp') { e.preventDefault(); _atIdx = Math.max(_atIdx - 1, 0); renderAtList(_atFiles); return; }
    if (e.key === 'Enter') { e.preventDefault(); selectAtItem(); return; }
    if (e.key === 'Escape') { e.preventDefault(); hideAtPanel(); return; }
  }
  if (_slashActive) {
    if (e.key === 'ArrowDown') { e.preventDefault(); _slashIdx = Math.min(_slashIdx + 1, _slashCmds.length - 1); renderSlashList(_slashCmds); return; }
    if (e.key === 'ArrowUp') { e.preventDefault(); _slashIdx = Math.max(_slashIdx - 1, 0); renderSlashList(_slashCmds); return; }
    if (e.key === 'Enter') { e.preventDefault(); selectSlashItem(); return; }
    if (e.key === 'Escape') { e.preventDefault(); hideSlashPanel(); return; }
  }
  // @ trigger detection
  if (e.key === '@' && !_atActive && !_slashActive) {
    // will be handled in input event
  }
  if (e.key === 'Backspace' && _atActive) {
    const text = composerText('chat');
    if (!text.includes('@')) { hideAtPanel(); }
  }
  if (e.key === 'Enter' && !e.shiftKey && !e.isComposing && e.keyCode !== 229) {
    if (_atActive || _slashActive) return; // handled above
    e.preventDefault(); submitInput();
  }
});
```

---

### Task 8: `app.js` — / 命令面板核心逻辑

**Files:**
- Modify: `frontends/desktop/static/app.js`

**Step 1: 添加 / 面板核心函数（在 @ 面板函数之后）**

```js
// ── / command panel ─────────────────────────────────────────────────

async function fetchCommands() {
  try {
    const res = await fetch(`${BRIDGE_ORIGIN}/api/commands`);
    const j = await res.json();
    return j.ok ? (j.commands || []) : [];
  } catch (e) { return []; }
}

function getCustomPresetCommands() {
  try {
    const raw = localStorage.getItem('ga_custom_presets');
    if (!raw) return [];
    return JSON.parse(raw).map(p => ({
      name: '/preset:' + p.id, label: p.title, desc: p.title, group: 'custom',
      _presetPrompt: p.prompt,
    }));
  } catch (_) { return []; }
}

function renderSlashList(cmds) {
  if (!cmds.length) {
    SLASH_GROUPS.innerHTML = '<div class="slash-empty">No commands found</div>';
    _slashIdx = -1;
    return;
  }
  _slashIdx = Math.min(_slashIdx, cmds.length - 1);
  // group by group key
  const groups = {};
  for (const c of cmds) {
    (groups[c.group] = groups[c.group] || []).push(c);
  }
  const groupLabels = { skills: 'Skills', actions: 'Actions', custom: 'Custom' };
  const order = ['skills', 'actions', 'custom'];
  let html = '';
  for (const g of order) {
    const items = groups[g];
    if (!items || !items.length) continue;
    html += `<div class="slash-group-label">${groupLabels[g] || g}</div>`;
    for (let i = 0; i < items.length; i++) {
      const c = items[i];
      const globalIdx = cmds.indexOf(c);
      const cls = 'slash-item' + (globalIdx === _slashIdx ? ' active' : '');
      html += `<div class="${cls}" data-idx="${globalIdx}"><span class="slash-cmd">${c.name}</span><span class="slash-desc">${c.desc}</span></div>`;
    }
  }
  SLASH_GROUPS.innerHTML = html;
}

async function showSlashPanel() {
  hideAtPanel();
  if (!_slashCmds.length) {
    const bridgeCmds = await fetchCommands();
    const customCmds = getCustomPresetCommands();
    _slashCmds = [...bridgeCmds, ...customCmds];
  }
  _slashIdx = _slashCmds.length > 0 ? 0 : -1;
  renderSlashList(_slashCmds);
  if (SLASH_SEARCH) SLASH_SEARCH.value = '';
  SLASH_PANEL.hidden = false;
  _slashActive = true;
}

function hideSlashPanel() {
  SLASH_PANEL.hidden = true;
  _slashActive = false;
  _slashIdx = -1;
}

function selectSlashItem() {
  if (_slashIdx < 0 || _slashIdx >= _slashCmds.length) return;
  const cmd = _slashCmds[_slashIdx];
  hideSlashPanel();
  if (cmd.group === 'actions') {
    // directly execute UI command
    inputEl.innerHTML = '';
    handleSlash(cmd.name);
  } else if (cmd._presetPrompt) {
    // custom preset → fill input with prompt text
    inputEl.textContent = cmd._presetPrompt;
    inputEl.focus();
  } else {
    // skill command → insert /cmd into input for user to add args
    inputEl.textContent = cmd.name + ' ';
    inputEl.focus();
    // place cursor at end
    const range = document.createRange();
    range.selectNodeContents(inputEl);
    range.collapse(false);
    const sel = window.getSelection();
    sel.removeAllRanges();
    sel.addRange(range);
  }
}

SLASH_SEARCH?.addEventListener('input', () => {
  const q = SLASH_SEARCH.value.trim().toLowerCase();
  let filtered = _slashCmds;
  if (q) {
    filtered = _slashCmds.filter(c =>
      c.name.toLowerCase().includes(q) || c.desc.toLowerCase().includes(q) || c.label.toLowerCase().includes(q)
    );
  }
  _slashIdx = filtered.length > 0 ? 0 : -1;
  renderSlashList(filtered);
});

SLASH_GROUPS?.addEventListener('click', (e) => {
  const item = e.target.closest('.slash-item');
  if (!item) return;
  _slashIdx = Number(item.dataset.idx);
  selectSlashItem();
});
```

**Step 2: 改造 `submitInput()` 和 `sendPrompt()` 支持 skill 命令**

修改 `submitInput()` (L3428) 中的 `/` 检测逻辑：

```js
async function submitInput() {
  if (_submitInFlight) return;
  let text = composerText('chat');
  if (!text.trim()) return;
  if (text.trim().startsWith('/')) {
    const cmdName = text.trim().split(/\s+/)[0];
    // UI action commands → handle directly
    if (['/help','/new','/clear','/stop','/settings'].includes(cmdName)) {
      inputEl.innerHTML = '';
      handleSlash(text.trim());
      return;
    }
    // Skill commands → send via slash endpoint
    inputEl.innerHTML = '';
    await sendSlashCommand(text.trim());
    return;
  }
  // ... rest unchanged
}
```

新增 `sendSlashCommand()`:

```js
async function sendSlashCommand(rawText) {
  const parts = rawText.split(/\s+/);
  const cmd = parts[0];
  const args = parts.slice(1).join(' ');
  if (!state.bridgeReady) { showError(t('err.bridge')); return; }
  if (!state.activeId) { await newSession(); }
  const sess = activeSess();
  const r = rt(sess);
  if (r.busy) {
    const interrupted = await interruptBeforeSend(sess);
    if (!interrupted) return;
  }
  try {
    const sid = await ensureBridgeSession(sess);
    const text = expandFilePlaceholders(rawText).trim();
    const usedFiles = collectUsedFiles(text);
    const previewFiles = usedFiles.filter(f => !f.isImage).map(f => ({ id: 'f-' + f.sid, name: f.name, path: f.path }));
    const previewImgs = usedFiles.filter(f => f.isImage).map(f => ({ id: 'f-' + f.sid, name: f.name, path: f.path, dataUrl: f.dataUrl || '' }));

    const userMsg = { role: 'user', content: rawText, ts: Date.now() / 1000 };
    sess.messages.push(userMsg); appendMessage(sess, userMsg);
    setBusy(sess, true);

    const res = await bridgeFetch(`/session/${encodeURIComponent(sid)}/slash`, {
      method: 'POST',
      body: { cmd, args, files: previewFiles, imageMetas: previewImgs.map(im => ({ name: im.name, path: im.path })), llmNo: state.llmNo },
    });
    if (res?.error) throw new Error(res.error.message || res.error);
    removeUsedPendingFiles(usedFiles);
    pollSession(sess);
  } catch (e) {
    const em = { role: 'error', content: e.message || String(e) };
    sess.messages.push(em); appendMessage(sess, em);
    setBusy(sess, false);
  }
}
```

**Step 3: 修改 `expandFilePlaceholders()` 支持新的 chip 类型**

修改 `expandFilePlaceholders()` (L4184):

```js
function expandFilePlaceholders(text) {
  return text.replace(/\[(Image|File|@file|@folder) #(\d+)\]/g, (m, kind, n) => {
    const f = state.pendingFiles.find(x => x.sid === Number(n));
    if (!f) return '';
    if (f.path) return f.path;
    return '';
  });
}
```

**Step 4: 修改 `readComposerTextFrom()` 支持新 chip**

修改 `readComposerTextFrom()` (L4153) 中的 chip 序列化逻辑：

```js
if (node.classList && node.classList.contains('ph-chip')) {
  const kind = node.dataset.kind || 'file';
  if (kind === 'image') return `[Image #${node.dataset.sid}]`;
  if (kind === '@file') return `[@file #${node.dataset.sid}]`;
  if (kind === '@folder') return `[@folder #${node.dataset.sid}]`;
  return `[File #${node.dataset.sid}]`;
}
```

**Step 5: 添加 `input` 事件监听 — 检测 @ 和 / 触发面板**

```js
inputEl.addEventListener('input', () => {
  const text = composerText('chat');
  // Detect @ trigger — show file panel when @ is typed
  if (!_atActive && !_slashActive) {
    // Check if last character typed was @ by looking at DOM
    const sel = window.getSelection();
    if (sel && sel.rangeCount) {
      const node = sel.anchorNode;
      const offset = sel.anchorOffset;
      if (node && node.nodeType === 3 && offset > 0 && node.nodeValue[offset - 1] === '@') {
        showAtPanel();
        return;
      }
    }
  }
  // Detect / trigger at line start
  if (!_atActive && !_slashActive && text.trimStart().startsWith('/') && text.trimStart().length === 1) {
    showSlashPanel();
    return;
  }
  // Update @ filter if panel open
  if (_atActive) {
    const afterAt = text.slice(text.lastIndexOf('@') + 1);
    if (afterAt !== undefined) {
      AT_SEARCH.value = afterAt;
      AT_SEARCH.dispatchEvent(new Event('input'));
    }
  }
});
```

**Step 6: 全局点击关闭面板**

```js
document.addEventListener('click', (e) => {
  if (_atActive && !AT_PANEL.contains(e.target) && e.target !== inputEl) {
    hideAtPanel();
  }
  if (_slashActive && !SLASH_PANEL.contains(e.target) && e.target !== inputEl) {
    hideSlashPanel();
  }
});
```

---

### Task 9: 保存 workspace path 到 session state

**Files:**
- Modify: `frontends/desktop/static/app.js`

在 workspace 相关函数中，设置 session 时同步保存 `_workspacePath`：

找到 session workspace 设置相关的代码（在 `session_workspace_set_handler` 调用的前端代码附近），确保在 workspace 绑定成功后保存路径：

```js
// 在 workspace set 成功后添加：
sess._workspacePath = data.workspace.path;
state.lastWorkspacePath = data.workspace.path;
```

---

### Task 10: 端到端测试

**Step 1: 启动 bridge**

```bash
cd /Users/mayangyang/code/ai/GenericAgent && python3 frontends/desktop_bridge.py &
sleep 2
```

**Step 2: 测试 API**

```bash
# Test commands API
curl -s http://127.0.0.1:14168/api/commands | python3 -m json.tool | head -30

# Test files API
curl -s "http://127.0.0.1:14168/api/files/list?path=/" | python3 -m json.tool | head -30
```

Expected: commands 返回 JSON 包含 skills 和 actions，files 返回根目录的文件列表。

**Step 3: 清理**

```bash
kill %1 2>/dev/null
```

---

### 实现顺序建议

1. Task 1 (slash_cmds.py) → Task 2 (files/list) → Task 3 (api/commands) → Task 4 (slash handler)
2. Task 5 (HTML DOM) → Task 6 (CSS)
3. Task 7 (@ panel JS) → Task 8 (/ panel JS) → Task 9 (workspace path)
4. Task 10 (E2E test)

每个 Task 完成后验证一次再继续。
