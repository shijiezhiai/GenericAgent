# @ 文件选择 & / 命令面板 设计文档

日期: 2026-07-01

## 概述

仿照 Claude Code，为 GA 桌面端增加两个交互能力：
- **@ 文件/文件夹选择** — 在 composer 中输入 `@` 触发，可通过模糊搜索或原生文件选择器将文件/文件夹添加到上下文
- **/ 命令面板** — 在 composer 中输入 `/` 触发，展示可用 skill、操作命令、自定义 preset，支持搜索和选择

## 架构

采用方案 A（纯前端 + 少量 Bridge API），所有修改集中在以下文件：

| 文件 | 改动类型 | 说明 |
|------|----------|------|
| `frontends/desktop_bridge.py` | 修改 | 新增 3 个 API handler |
| `frontends/slash_cmds.py` | 修改 | 新增 `list_all_commands()` |
| `frontends/desktop/static/app.js` | 修改 | 新增 @panel + /panel 逻辑 |
| `frontends/desktop/static/index.html` | 修改 | 新增面板 DOM |
| `frontends/desktop/static/styles.css` | 修改 | 新增面板样式 |

---

## 一、@ 文件选择面板

### 交互行为

1. 用户在 composer 中输入 `@` → 弹出悬浮面板（定位在光标下方，`.composer-inset` 内部）
2. 面板包含：搜索框 + 文件列表 + "浏览文件..." 按钮
3. 输入字符实时模糊过滤文件名/路径
4. 选中文件/文件夹后，以 **chip** 形式插入 composer
5. 点击"浏览文件..."调用 Tauri 原生文件选择器

### Chip 类型扩展

新增两种 kind：
- `kind: "@file"` — 引用的本地文件，存储绝对路径
- `kind: "@folder"` — 引用的本地文件夹，存储绝对路径

与现有 `[Image #N]` / `[File #N]` 共存的 placeholder 格式：
- `[@file #N]` — 引用文件
- `[@folder #N]` — 引用文件夹

发送时 `expandFilePlaceholders()` 将 `[@file #N]` / `[@folder #N]` 替换为绝对路径。

### 数据来源

- 优先使用当前 session 的 workspace 路径（`sess.workspace` → `workspace_cmd.registry_load()` → `path`）
- 无 workspace 时由前端提供默认路径
- 文件列表通过 `GET /api/files/list` 获取

### API: GET /api/files/list

参数:
- `path` (必填) — 目录绝对路径
- `filter` (可选) — 模糊过滤文件名

返回:
```json
{
  "ok": true,
  "path": "/Users/xxx/project",
  "entries": [
    { "name": "src", "type": "dir", "path": "/Users/xxx/project/src" },
    { "name": "app.js", "type": "file", "path": "/Users/xxx/project/app.js" }
  ]
}
```

最多 50 条，目录在前，按名称排序。忽略 `.git`、`node_modules`、`__pycache__`、`venv`、`.DS_Store`。

### 面板 UI 结构

```html
<div class="at-panel" id="at-panel" hidden>
  <div class="at-search"><input type="text" placeholder="Search files..." /></div>
  <div class="at-list">
    <div class="at-item at-dir">📁 src/</div>
    <div class="at-item at-file">📄 app.js</div>
  </div>
  <div class="at-footer"><button class="at-browse-btn">Browse files...</button></div>
</div>
```

### 键盘操作

| 按键 | 行为 |
|------|------|
| ↑/↓ | 上下导航 |
| Enter | 确认选择 |
| Esc | 关闭面板 |
| Backspace (光标在 @ 后且无后续字符) | 关闭面板 |

---

## 二、/ 命令面板

### 交互行为

1. 用户在 composer 行首（或前面是空白）输入 `/` → 弹出命令面板
2. 面板按分组展示命令：Skills / 操作 / 自定义
3. 输入字符实时过滤
4. 选中命令后，替换输入框内容，聚焦等待用户输入参数

### 命令分组

| 分组 | 内容 | 来源 |
|------|------|------|
| Skills | review/plan/goal/autonomous/morphling/hive/conductor 等 | `slash_cmds.py` PALETTE_ENTRIES + `memory/*_sop.md` 扫描 |
| 操作 | /new /clear /stop /settings /help | `app.js` 现有 UI 命令 |
| 自定义 | 用户 custom presets | `localStorage` ga_custom_presets |

### Skill 自动发现

在 `slash_cmds.py` 中新增 `list_all_commands()`：
1. 从 `PALETTE_ENTRIES` 读取已注册的 TUI 命令 → group `"skills"`
2. 扫描 `memory/*_sop.md` 文件，提取 `# 标题` 作为 label → 补充到 skills 组
3. 硬编码 UI 操作命令 → group `"actions"`

### API: GET /api/commands

返回:
```json
{
  "ok": true,
  "commands": [
    { "name": "/goal", "label": "Goal Mode", "desc": "Enter Goal mode...", "group": "skills" },
    { "name": "/plan", "label": "Plan Mode", "desc": "Enter Plan mode...", "group": "skills" },
    { "name": "/new", "label": "New Session", "desc": "Create new session", "group": "actions" }
  ]
}
```

### API: POST /session/{sid}/slash

Body: `{ "cmd": "/goal", "args": "build a todo app" }`

内部调用 `slash_cmds.prompt_for(cmd, args)` 生成注入 prompt，然后走和 `/prompt` 相同的 agent 调用流程。

### 面板 UI 结构

```html
<div class="slash-panel" id="slash-panel" hidden>
  <div class="slash-search"><input type="text" placeholder="Search commands..." /></div>
  <div class="slash-groups">
    <div class="slash-group">
      <div class="slash-group-label">Skills</div>
      <div class="slash-item">🔍 /review Deep Review</div>
    </div>
    <div class="slash-group">
      <div class="slash-group-label">Actions</div>
      <div class="slash-item">📝 /new New Session</div>
    </div>
    <div class="slash-group">
      <div class="slash-group-label">Custom</div>
      ...
    </div>
  </div>
</div>
```

### 选中命令后行为

- **Skills**: 输入框替换为命令文本（如 `/goal`），用户输入参数，按 Enter → `POST /session/{sid}/slash`
- **操作**: 直接执行（/new → newSession()），不发送给 agent
- **自定义**: 替换为 preset 对应的 prompt 文本

---

## 三、触发规则总结

| 按键 | 条件 | 行为 |
|------|------|------|
| `@` | composer 中任意位置输入 `@` | 弹出文件选择面板 |
| `/` | composer 行首或前为空白的 `/` | 弹出命令面板 |
| Esc | 面板打开时 | 关闭面板 |
| ↑/↓ | 面板打开时 | 导航 |
| Enter | 面板中有选中项 | 确认选择 |
| Backspace | `@` 后无字符 | 关闭面板 |

@panel 和 /panel 互斥（同时只有一个打开）。

---

## 四、发送流程变更

```
submitInput()
  ├─ 文本以 / 开头
  │   ├─ UI 操作命令 (/new /clear /stop /settings /help) → 直接执行
  │   └─ Skill 命令 (/goal /plan /review ...) → POST /session/{sid}/slash
  └─ 普通文本 → sendPrompt()
       └─ expandFilePlaceholders() 展开 [@file #N] → 绝对路径
```
