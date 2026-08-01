# AGENTS.md

> 本文件供 AI 编程 Agent（Claude Code / Cursor / Copilot / GA 自身等）在新会话中快速建立项目认知，免去反复扫描全仓。最后更新：2026-07-02。

## 项目定位

**GenericAgent** — 极简自进化自主 Agent 框架。核心约 3K 行种子代码，仅靠 **9 个原子工具 + ~100 行 Agent Loop** 赋予任意 LLM 对本机的系统级控制（浏览器、终端、文件、键鼠、屏幕视觉、ADB）。设计哲学：**不预载技能，运行中演化技能**。

- 仓库：`git@github.com:lsdefine/GenericAgent.git`，分支 `main`
- 语言：Python ≥3.10, <3.14
- 包名：`genericagent`（pyproject.toml），安装入口 `ga` 命令
- 许可：MIT

## 环境与依赖

- 包管理用 `uv`（`uv.lock` 在根）。也可 `pip install -e .`
- **不要一次装全部依赖**。按需选 extras：
  - 核心：`requests beautifulsoup4 bottle simple-websocket-server aiohttp`（已含）
  - GUI：`pip install -e ".[ui]"`（streamlit / pywebview / prompt_toolkit / rich / pillow）
  - IM 机器人：`pip install -e ".[all-frontends]"`（按需）
- 虚拟环境：`.venv/`（本地存在，勿提交）
- macOS 系统解释器路径示例：`/opt/homebrew/bin/python3`

## 凭证配置（关键，勿读勿改 mykey.py）

- `mykey.py` 含 API Key，**被 .gitignore 排除，禁止读取/移动/提交**
- 模板：`mykey_template.py`（中文）/ `mykey_template_en.py`（英文），复制为 `mykey.py` 后填 key
- 配置工具：`python assets/configure_mykey.py` 或 `ga configure`
- 命名规则（变量名关键字决定 Session 类型，见模板注释）：
  - `native_claude_*` → `NativeClaudeSession`（API 原生 tool 字段，推荐）
  - `native_oai_*` → `NativeOAISession`（API 原生 tool 字段，推荐）
  - `mixin_*` → `MixinSession`（多 session 故障转移）
  - 旧文本协议 `claude_*` / `oai_*` 已 deprecated
- 加载逻辑在 `llmcore.py`（`reload_mykeys()`，热重载，按 mtime 判断变更）

## 核心架构与入口

```
用户输入 → agentmain.GenericAgent.run() → agent_loop.agent_runner_loop()
    → LLM(llmcore) → 工具调用 → ga.GenericAgentHandler(BaseHandler) 执行 9 工具
    → 结果回灌 LLM → 循环直至完成
```

| 文件 | 角色 | 说明 |
|---|---|---|
| `agent_loop.py` | Agent Loop 内核 | `BaseHandler`、`StepOutcome`、`agent_runner_loop()`、`json_default`。~100 行循环，协议无关 |
| `ga.py` | 工具实现层 | `GenericAgentHandler(BaseHandler)` 实现 12 个原子工具的物理执行。最大文件，含 `code_run` / `git_checkpoint` 等 |
| `llmcore.py` | LLM 适配层 | `ToolClient`/`NativeClaudeSession`/`NativeOAISession`/`MixinSession`/`resolve_client`，mykey 热重载，历史压缩 |
| `agentmain.py` | 主编排器 | `GenericAgent` 类（SDK 入口）、系统提示拼装、工具 schema 加载、slash 命令分发、多 LLM 轮转 |
| `simphtml.py` | HTML 简化器 | web_scan 底层，提取页面结构化纯文本 |
| `TMWebDriver.py` | 浏览器自动化 | CDP 桥接，进阶 web 操作（见 memory SOP） |

### 9 个原子工具（assets/tools_schema.json）

| 工具 | 用途 |
|---|---|
| `code_run` | 执行 Python / bash。优先 python，支持 Multi-call 并行 |
| `file_read` | 读文件，支持分页/关键字搜索，改前必先读 |
| `file_patch` | 精细局部替换，old_content 须唯一且精确匹配 |
| `file_write` | 新建/覆盖/追加，仅大段写入用 |
| `web_scan` | 获取页面简化 HTML + 标签页列表 |
| `web_execute_js` | 浏览器注入 JS，支持多 tab 并行 |
| `update_working_checkpoint` | 短期工作记忆便签，每轮注入上下文 |
| `ask_user` | 需用户决策时中断提问 |
| `start_long_term_update` | 触发长期记忆提炼 |
| `code_search` | ripgrep 搜索内容（file:line:content） |
| `file_find` | ripgrep 按文件名模式找文件 |
| `repo_index` | 预加载仓库结构索引（build/rebuild/stats/find/symbols），文件清单+符号映射+语言统计，缓存 `.ga_search/repo_index.json`。大仓库开局先 build |
| `semantic_search` | 语义/自然语言搜代码。mykey 配了 embedding 后端（`embedding_apikey` 等）走向量检索（分块+sqlite+余弦）；未配自动降级为关键词+同义词扩展 ripgrep |
| `git_checkpoint` | 结构化 Git 工作流（status/diff/commit/checkpoint/list/restore/branch/log/pr），替代裸 `code_run` 调 git。checkpoint 用 git tag `ga-ckpt-<ts>-<shortsha>` 标记，`restore` 一键回退（需 `no_confirm=true`）。`pr` 优先 `gh pr create`，回退 GitHub REST API（需 `mykey.github_token`）|

> Windows 下 `code_run` 用 powershell；非 Windows 用 bash。schema 在 `agentmain.load_tool_schema()` 中按 OS 替换。

## 目录速览

```
GenericAgent/
├── agent_loop.py        # Agent Loop 内核（最小协议层）
├── ga.py                # 工具实现（GenericAgentHandler + 9 工具）
├── llmcore.py           # LLM 适配 + mykey 热重载 + 历史压缩
├── agentmain.py         # 主编排器（GenericAgent SDK 类）
├── simphtml.py          # HTML→结构化文本（web_scan 底层）
├── TMWebDriver.py       # CDP 浏览器自动化
├── ga_cli/              # CLI 命令分发（`ga <cmd>`）
│   └── cli.py           # COMMANDS 字典：gui/configure/hub/tui/tui2/cli/launch/status/update/list
├── frontends/           # 所有前端/UI/IM 接入
│   ├── qtapp.py         # PyQt 桌面 GUI（主 GUI）
│   ├── stapp.py         # Streamlit Web UI（launch.pyw 启动）
│   ├── tuiapp.py / tuiapp_v2.py / tui_v3.py  # 终端 TUI（三代）
│   ├── desktop_bridge.py + desktop/ (Tauri)  # Tauri 桌面应用
│   ├── conductor.py     # 总管 agent + subagent 池 + IM 信号（headless API，UI 在桌面 App「协作」页）
│   ├── tgapp / qqapp / fsapp / dcapp / wechatapp / wecomapp / dingtalkapp.py  # 各 IM bot
│   ├── slash_cmds.py    # /update /autorun /morphling /goal /hive /scheduler 命令构建
│   └── chatapp_common.py / cost_tracker.py / plan_state.py / remote_channels.py
├── reflect/             # 后台自治模式（被 .gitignore 白名单管理）
│   ├── autonomous.py    # 自主运行入口
│   ├── scheduler.py     # 定时任务守护
│   ├── goal_mode.py     # 目标模式
│   ├── checklist_master.py
│   └── agent_team_worker.py
├── plugins/             # 插件系统
│   ├── hooks.py         # discover_and_load() 启动时自动发现加载
│   ├── project_mode.py  # 项目模式
│   └── langfuse_tracing.py
├── memory/              # 记忆体系（核心！见下节）
├── assets/              # 系统提示、工具 schema、安装脚本、CDP 桥、模板
│   ├── sys_prompt.txt / sys_prompt_en.txt      # 系统提示（中/英）
│   ├── tools_schema.json / tools_schema_cn.json
│   ├── code_run_header.py                      # code_run 注入头
│   ├── configure_mykey.py / ga_install.sh/.ps1
│   └── tmwd_cdp_bridge/                        # CDP 桥配置（config.js 自动生成，不入库）
├── docs/                # 安装与上手文档
├── sche_tasks/          # 定时任务 JSON（运行时生成，不入库）
├── temp/                # 运行时临时区（会话/响应/上传，不入库）
├── launch.pyw           # pywebview + Streamlit 一键启动
├── hub.pyw              # tkinter 零依赖服务启动器（单例锁端口 19735）
├── build_app.sh / setup_app.py / GenericAgent.spec / make_icns.sh  # 打包
├── mykey_template*.py   # 凭证模板（mykey.py 不入库）
├── pyproject.toml / uv.lock
├── README.md / CONTRIBUTING.md / LICENSE
└── project_memory.md    # 空，预留
```

## 记忆体系（memory/，重要）

GenericAgent 的自进化核心。分层结构，新会话默认注入 L0+L1 到系统提示。

| 层级 | 文件 | 作用 |
|---|---|---|
| **L0 META-SOP** | `memory_management_sop.md` | 记忆管理元规则，写任何记忆前必先读 |
| **L1 Insight** | `global_mem_insight.txt` | 极简索引，L2/L3 变更时同步 |
| **L2 全局事实** | `global_mem.txt` | 持久化环境事实/用户偏好 |
| **L3 SOP/工具** | `memory/*.md` `memory/*.py` | 可复用技能（SOP 文档 + Python 工具脚本） |
| **L4 原始会话** | `L4_raw_sessions/` | 历史会话归档 |

### 常用 SOP/工具（L3）

- 浏览器进阶：`tmwebdriver_sop.md`（文件上传/图搜/PDF/Cookie/CDP/跨 tab）
- 键鼠控制：`ljqCtrl_sop.md` + `ljqCtrl.py` / `macljqCtrl.py`（禁 pyautogui，先 activate）
- UI/截图/视觉：`computer_use.md` / `vision_sop.md` / `ui_detect.py` / `ocr_utils.py`
- 手机：`adb_ui.py`
- 定时任务：`scheduled_task_sop.md`
- 自主运行：`autonomous_operation_sop.md`
- 规划模式：`plan_sop.md` / `ultraplan_sop.md`
- 子代理：`subagent.md`
- 代码审查：`code_review_principles.md` / `review_sop.md`
- 记忆整理：`memory_cleanup_sop.md`

> **memory/ 被 .gitignore 大量排除**，仅白名单 SOP/工具脚本入库。修改 memory 下文件只能用 `file_patch`（除非新建）。

## 启动方式

```bash
# CLI 交互（最直接）
python agentmain.py            # 或 ga cli

# 桌面 GUI
python frontends/qtapp.py      # 或 ga gui

# Web UI（pywebview + Streamlit）
python launch.pyw              # 或 ga launch

# 服务启动器（tkinter，管理多个后台服务）
python hub.pyw                 # 或 ga hub

# 终端 TUI
python frontends/tuiapp.py     # ga tui  /  ga tui2 (v2)  /  tui_v3.py

# 配置凭证
python assets/configure_mykey.py   # 或 ga configure

# 作为 SDK 嵌入
# agent = GenericAgent(); threading.Thread(target=agent.run, daemon=True).start()
# q = agent.put_task("你的指令")
```

## 代码规范（CONTRIBUTING.md 精要）

仓库每个文件都会被 AI 反复读取，**冗余文字 = 浪费 token + 挤占上下文 + 增加幻觉**。

- **自文档化代码，最少注释**。需大段解释就重写
- **紧凑、视觉统一**。行数少、行宽一致、无废话
- **小变更半径**。改 A 不应波及 B/C/D
- **功能更多 → 代码更少**。好抽象让仓库缩小而非膨胀
- **按失败半径放行**。关键错误大声崩溃，琐碎错误静默通过。禁止 blanket try-catch
- PR 经严格自动代码审查，多数 AI 直出代码不通过，先读 `code_review_principles.md`

## 开发注意事项

1. **改前必读目标文件**，用 `file_read` 获取最新行号上下文
2. **精细修改用 `file_patch`**（old_content 须唯一精确匹配），全量覆盖才用 `file_write`
3. **搜代码内容用 `code_search`、按文件名找文件用 `file_find`，大仓库开局 `repo_index(build)` 预索引、意图式查询用 `semantic_search`**（均基于 ripgrep/可选 embedding，工具已注册）；替代 `code_run` 跑裸 `grep`/`find` 更省 token。搜索网络用 Google，禁递归遍历/猜路径
4. **不可逆操作先问用户**；3 次失败请求干预
5. **进程操作精确 PID**，禁无条件杀 python（会杀自己）
6. **打包**：`build_app.sh`（macOS py2app）/ PyInstaller（spec 在 `GenericAgent.spec`），macOS 需 `xattr -cr` + `codesign`
7. **temp/ 和 sche_tasks/ 是运行时区**，勿提交、勿在其中放源码
8. `.claude/` 目录为 Claude Code 本地配置，不入库
9. **git_checkpoint 操作禁忌**：commit/checkpoint 不能在 main/master 上做（工具会硬拒），先 `action=branch` 切工作分支；`restore` 是 `reset --hard`，**必须**传 `no_confirm=true`，且操作前应有上一个 checkpoint 可回退。`.workbuddy/git_restore.log` 记录所有 restore，可审计。

## 常见任务速查

| 任务 | 起点文件 |
|---|---|
| 加/改一个工具 | `ga.py`（实现）+ `assets/tools_schema.json`（声明）|
| 改系统提示 | `assets/sys_prompt.txt`（中）/ `sys_prompt_en.txt`（英）|
| 加 CLI 命令 | `ga_cli/cli.py` 的 `COMMANDS` 字典 |
| 加前端/IM 接入 | `frontends/` 新建 `xxxapp.py`，参考 `chatapp_common.py` |
| 加 slash 命令 | `frontends/slash_cmds.py` 的 `build_*_prompt()` |
| 加后台模式 | `reflect/` 新建 `.py`（注意 .gitignore 白名单）|
| 加插件 | `plugins/`，实现 hooks 接口，`hooks.py` 会自动发现 |
| 加 SOP/记忆 | 先读 `memory/memory_management_sop.md`，再 patch memory 文件 |
| 改 LLM 适配 | `llmcore.py`（Session 类 + resolve_client）|
