# Claude Code Plugin 支持 — 实施计划

日期: 2026-07-06
状态: 第一层已完成并验证通过 ✅
关联: plugins/skills_loader.py（已支持 Anthropic Agent Skills 规范）

## 背景
GA 通过 `plugins/skills_loader.py` 已支持 Anthropic Agent Skills 开放规范（`SKILL.md` frontmatter + scripts + references），当前已装 flink-skills 4 个 skill。
本计划扩展 GA，使其能加载 Claude Code 的 **Plugin 打包格式**（自包含目录 + `.claude-plugin/plugin.json` 清单 + 多组件）。

## Claude Code Plugin 规范（已核实官方文档 code.claude.com/docs/en/plugins）
- Plugin = 自包含目录 + `.claude-plugin/plugin.json` 清单
- 清单字段：`name`（唯一标识 + skill 命名空间前缀）、`description`、`version`、`author{name}`、可选 `homepage`/`repository`/`license`
- 可打包组件：
  - **skills**: `skills/<name>/SKILL.md`，支持 `$ARGUMENTS` 占位符、`disable-model-invocation` frontmatter
  - **agents**: 声明式 subagent（`.md` frontmatter 定义 name/description/tools）
  - **hooks**: `PreToolUse`/`PostToolUse`/`UserPromptSubmit`/`Stop` 等事件 + matcher
  - **mcp**: MCP servers（stdio/SSE/http transport）
  - **LSP servers / background monitors / default settings**: 高级组件
- skill namespace 化：plugin 名 `my-plugin` 的 skill `hello` → `/my-plugin:hello`
- 加载：`claude --plugin-dir <dir>`，`/reload-plugins`

## GA 现状对照（源码已确证）
| 组件 | GA 现状 | 差距 |
|---|---|---|
| plugin 清单解析 | ❌ 无 | 需新增 |
| skills | ✅ skills_loader.py 已支持 SKILL.md | 缺 namespace 前缀、`$ARGUMENTS`、slash 命令入口 |
| slash 命令 | ✅ frontends/slash_cmds.py 已实现（自有面板） | 命令来源 PALETTE_ENTRIES+memory/*_sop.md，未接 plugin |
| agents(subagent) | ✅ 有 agentmain --func/--task（进程级） | GA 命令行启动式 vs Claude Code 同进程声明式，语义不同 |
| hooks | ✅ plugins/hooks.py，6 事件点（agent_before/turn_before/llm_before/tool_before/tool_after/llm_after） | 事件名不同、无 matcher |
| MCP | ❌ 无原生支持 | 需引入 MCP client，独立大工程 |

## 四层实施计划

### 第一层：plugin 容器骨架 + skills 复用 —「小，~0.5-1 天」
**目标**：支持纯 skills 型 plugin（最常见的 plugin 形态，覆盖 ~70%）

**改动文件**：
- 新增 `plugins/plugin_loader.py`：解析 `.claude-plugin/plugin.json`，发现各组件目录，把 plugin 注册进 skill 加载体系
- 扩展 `plugins/skills_loader.py`：skill 名加 namespace 前缀（`plugin-name:skill`）、支持 `$ARGUMENTS` 占位符替换、兼容现有 flink-skills（无 plugin.json 的裸 skills 目录仍正常加载）
- 扩展 `frontends/slash_cmds.py`：把 plugin skill 注册成 `/plugin:skill` 命令
- 配置：`GA_PLUGIN_DIRS` 环境变量（冒号分隔）/ 扩展 `plugins/skills_config.json`

**验收标准**：
1. 一个含 `.claude-plugin/plugin.json` + `skills/` 的真实 Claude Code plugin 能被 GA 发现并注入索引
2. plugin skill 名带 namespace 前缀，与裸 skill 不冲突
3. `/plugin:skill <args>` 命令能触发，`$ARGUMENTS` 正确替换
4. 现有 flink-skills（无 plugin.json）仍正常工作，无回归
5. 单元测试覆盖：plugin.json 解析、namespace 化、$ARGUMENTS 替换、裸 skills 兼容

**状态**：✅ 已完成并验证通过（2026-07-06）

**实现概要**：
- 新增 `plugins/plugin_loader.py`：plugin.json 清单解析 + 双结构发现（单 plugin 或容器目录含多个子 plugin）+ frontmatter 解析（含 `disable-model-invocation`）+ 缓存 + `list_plugin_commands()` / `render_plugin_skill($ARGUMENTS 替换)` / `_build_injection()` 注入文本 + agent_before hook 注册
- `frontends/slash_cmds.py`：`list_all_commands` 加 plugin 段（seen_names 去重）+ `prompt_for` 加 `/plugin:skill` 路由（函数内延迟 try-import，免改顶部）
- `plugins/skills_loader.py`：**未改**（裸 skills 零回归，plugin_loader 独立承担 namespace）
- 配置：`GA_PLUGIN_DIRS` 环境变量 + `skills_config.json` 新增 `plugin_dirs` 字段（默认 `[]`，生产零影响）

**验证结果**（测试 plugin `temp/test-plugin/` 含 hello[$ARGUMENTS] + info[disable-model-invocation]）：
1. ✅ plugin 发现：test-plugin + 2 skills
2. ✅ namespace 化：`/test-plugin:hello`、`/test-plugin:info`
3. ✅ `$ARGUMENTS` 替换：`render_plugin_skill('/test-plugin:hello','Alex')` → `$ARGUMENTS` → `Alex`
4. ✅ slash 命令发现 + 路由：`prompt_for('/test-plugin:hello','Alex')` 渲染正确
5. ✅ agent_before hook 触发：`hooks._registry['agent_before']` = 2 回调（plugin_loader + skills_loader 共存），trigger 后 user message 含两段注入（Plugin 段 + 外部 Skills 段）
6. ✅ `disable-model-invocation` 标记：info 标为"仅命令触发"
7. ✅ 回归：flink-skills 4 个 skill 仍正常注入；`/update` 等原有命令不受影响；无 `GA_PLUGIN_DIRS` 时 plugin_loader 返回 None（零干扰）

### 第二层：agents + hooks 适配 —「中，~2-3 天」
**目标**：支持含 agents/hooks 的 plugin（累计覆盖 ~85-90%）

**改动**：
- agents 适配层：解析 `agents/*.md` frontmatter（name/description/tools）→ 映射 GA subagent 启动（agentmain --func），主 agent 按描述命中后调用；处理 tools 白名单
- hooks 适配层：事件映射表（PreToolUse→tool_before、PostToolUse→tool_after、UserPromptSubmit→agent_before、Stop→llm_after）+ matcher 解析（工具名正则）+ 阻塞/批准语义

**难点**：声明式 subagent 与 GA 进程级 subagent 语义无法完全对等；hooks 阻塞能力 GA 现为直通触发需补

**验收标准**：plugin 内 agents/ 能被主 agent 按描述命中并启动 subagent；hooks matcher 命中后在对应 GA 事件点触发

**状态**：agents 适配 ✅ 已完成并验证通过（2026-07-06）；hooks 适配 ✅ PreToolUse 完整阻塞已实现并验证通过（2026-07-06，用户选方案 A）；PostToolUse/UserPromptSubmit/Stop 解析层支持，执行层接入待后续（非阻塞类，优先级低）

**实现概要（agents 适配）**：
- `plugins/plugin_loader.py` 新增：`_parse_agent_frontmatter`（解析 name/description/model/effort/maxTurns/tools/disallowedTools/skills）+ `_discover_plugin_agents`（扫 agents/*.md，ns=plugin:agent）+ `_discover_plugins` 加 agents 字段 + `_build_agent_injection` + `list_plugin_agents` + `render_plugin_agent` + `inject_plugin_agents_index` hook（独立 agent_before，与 skill 注入并存）
- 注入文本含 GA subagent 启动指引：file_read agent 正文作 system prompt → 写临时文件 → `python agentmain.py --func <文件> --nobg` → 读 .out.txt（含轮数耗尽坑提示）

**验证结果**（test-plugin/agents/test-agent.md）：
1. ✅ list_plugin_agents=1，ns=test-plugin:test-agent，全字段解析正确（model/effort/maxTurns/tools/disallowedTools）
2. ✅ render_plugin_agent 正文提取正确
3. ✅ _build_agent_injection 含启动指引 + agent 索引（scoped name + 工具/禁用/model + 描述 + 正文路径）
4. ✅ hooks._registry['agent_before'] = 2 回调（skills + agents 并存）
5. ✅ trigger 后 user message 被注入 agents 索引
6. ✅ 回归：list_plugin_commands=2（skills 未受影响）

**hooks 适配实现概要**（用户选方案 A=完整阻塞语义）：
- `plugins/plugin_loader.py` 新增：`_discover_plugin_hooks`（解析 hooks/hooks.json，扁平化为 [{event,matcher,command}]，与 skills/agents 对称，支持所有 event）+ `_discover_plugins` 加 hooks 字段 + `run_cc_hook(event, tool_name, tool_input, **extra)` 执行器（subprocess 执行 command + 传 stdin JSON{hook_event_name/tool_name/tool_input/cwd/permission_mode} + 解析决策：exit2→block(stderr反馈)/rc非0非2→fail-open放行/stdout非JSON→continue放行/continue:false→stop/decision:block→block/hookSpecificOutput.permissionDecision:deny→deny/hookSpecificOutput.updatedInput→改写args；多处 fail-open）
- `agent_loop.py` 改动：顶部 `import run_cc_hook as _run_cc_hook`；`dispatch` 内 `tool_before` 后接入 PreToolUse（过滤 _index/_tool_num 传干净 tool_input → stop→StepOutcome should_exit=True / block→yield 反馈+data={error:reason}+next_prompt 非空 / updated_input→args.update 后执行）；fail-open（hook 异常/subprocess 失败均不阻断工具）
- 阻塞语义映射：stop→should_exit（停 agent）、block→跳过工具+反馈给 LLM 下轮、updated_input→改写 args 后执行

**验证结果**（test-plugin/hooks/hooks.json 含 BlockedTool→block/RewriteTool→updatedInput/StopTool→continue:false）：
1. ✅ run_cc_hook 执行器 5 用例：BlockedTool→block=True/RewriteTool→updated_input={rewritten:true}/StopTool→stop=True/code_run→全 False 放行/PostToolUse+BlockedTool→event 过滤
2. ✅ dispatch 层接入 4 路径：BlockedTool→data={error:blocked}+next_prompt 非空（工具未执行）/StopTool→should_exit=True/RewriteTool→args 含 rewritten:true（改写后执行）/code_run→正常执行
3. ✅ fail-open：hook 异常/subprocess 失败均不阻断工具执行

**后续增强（未接入执行层）**：PostToolUse→tool_after（工具后观察）、UserPromptSubmit→agent_before（改写用户 prompt）、Stop→llm_after（停止决策）——_discover_plugin_hooks 已解析，run_cc_hook 已支持，仅需在 agent_loop 对应 hook 点接入（非阻塞类，低优先级）

### 第三层：MCP servers —「大，~3-5 天+」
**目标**：支持含 MCP 的 plugin（累计覆盖 ~95%）

**改动**：
- 引入 MCP client（自实现 stdio/http/SSE JSON-RPC，零依赖，sync 契合 agent_loop，决策 1A）
- 支持 stdio/SSE/http transport（全做，决策 2B，覆盖 ~95%）
- MCP tools 桥接成 GA 工具（tools_schema 注册 + dispatch fallback 路由）
- .mcp.json 解析 + MCP server 进程生命周期管理

**特性**：独立工程，与 plugin 支持可解耦，很多 plugin 不含 MCP

**实现进度**（5 步）：
1. ✅ 解析层：`_discover_plugin_mcp` 解析 .mcp.json/plugin.json inline mcp，递归替换 `${CLAUDE_PLUGIN_ROOT}`，判断 transport(stdio/http/sse)，`_discover_plugins` 接入 mcp 字段。验证 4 用例全绿（三 transport + invalid 跳过 + 变量替换 + inline mcp + fail-open 返回 {}）
2. ✅ 传输层：`plugins/mcp_client.py` 自实现 stdio/http/SSE JSON-RPC（initialize/tools.list/tools.call）。验证全绿：stdio（正常+错误-32602+启动失败+崩溃超时）/ http（Streamable HTTP+session协商）/ SSE（endpoint 拼接+流式响应+干净 stop 0.00s）
3. ✅ 桥接层：agentmain 合并 MCP tools 到 TOOLS_SCHEMA（run 传 `TOOLS_SCHEMA+self.mcp_tools`）+ dispatch MCP fallback 路由（agent_loop else 分支→`self.parent.mcp_tool_map`→call_tool）。验证：tools_schema 合并 + tool_map 命中 + dispatch fallback call_tool 返回 echo + fail-open
4. ✅ 生命周期：agent `__init__` 调 `collect_mcp_tools`（start+initialize+list_tools），run `finally` stop `mcp_clients`；fail-open（bad server 超时 skip，异常不阻断）
5. ✅ 端到端验证：实例化 GenericAgent→MCP tools 收集(tools=1/map命中)+bad server fail-open skip+stop 完成（真实 LLM 对话调用已于 2026-07-07 验证通过，见进度日志；dispatch fallback 已单独验证 call_tool 返回正确）

**状态**：第三层（MCP）完成 ✅（步骤1-5）。根因记录：plugin 清单须在 `.claude-plugin/plugin.json`（非根目录）。第四层（LSP/monitors/settings）大且可选，按需后置

### 第四层：LSP / background monitors / default settings —「大，全功能对齐」
**目标**：全功能对齐（100%）
**决策**（用户确认 2026-07-07）：落地顺序 default settings → background monitors → LSP（全做，LSP 完整实现）；userConfig 敏感值明文存 GA 配置文件。

**子模块 4.1 default settings（先做，依赖前置，纯逻辑）**
- 4.1.1 ✅ defaultEnabled：plugin_loader 解析 manifest.defaultEnabled（默认 true），影响 plugin 启用状态（Patch1）
- 4.1.2 ✅ userConfig schema 解析：读 manifest.userConfig，解析 type/title/description/sensitive/required/default/multiple/min/max（Patch1）
- 4.1.3 ✅ 配置存储：新建 GA 配置文件 plugin_configs.json 存 pluginConfigs[id].options（敏感值明文，决策 E）（Patch2）
- 4.1.4 ✅ 配置采集：GA 无交互 enable 流程，首次加载时 default 兜底，required 缺失则日志警告 + fail-open 跳过（Patch3+4，7项端到端全绿）
- 4.1.5 ✅ 变量替换：解析 MCP/LSP/hook/monitor 命令时替换 ${user_config.KEY}；导出 CLAUDE_PLUGIN_OPTION_<KEY> 环境变量到子进程（Patch5a-5e）
- ✅ 验证：run_cc_hook 端到端 6 项全绿（ROOT/DATA/PROJ/OPTION/ARG1 变量替换 + decision 不 block）。关键根因：官方规范 CLAUDE_PROJECT_DIR（无 PLUGIN！）；CLAUDE_PLUGIN_OPTION_<KEY> 的 KEY 原样不转大写（与 ${user_config.KEY} 一致）。_build_plugin_env 源码正确，初版 [3][4] 测试失败系 hook.sh 变量名/大小写写错，已修正。

**子模块 4.2 background monitors（第二做，依赖 4.1，复用 sche_tasks）— 完成 ✅**
- 4.2.1 ✅ monitors.json 解析：读 monitors/monitors.json，[{name,command,description,when}]
- 4.2.2 ✅ 后台进程管理：复用 sche_tasks 经验，启动 command 持久进程，按 when(always/on-skill-invoke) 控制启动
- 4.2.3 ✅ stdout 采集：逐行读取 stdout 作 notification（_reader_loop → buffer）
- 4.2.4 ✅ 注入 Claude context：复用 GA 现成非侵入通道 self.parent.intervene（ga.py L576 读→L578 注入 [MASTER]→L579 清空），pump 周期写 intervene
- 4.2.5 ✅ 生命周期：session start 启动 always monitors（agent_loop L84）+ skill 触发启动 on-skill-invoke（slash_cmds L708）；session end stop_all_monitors（agent_loop L141）
- 验证：test_monitor_global T1-T4 全 PASS（设全局/pump写intervene/消费闭环注入[MASTER]+清空/stop清理）。全局 _session_agent_ref 桥接 pump→intervene（避开 GA 核心传参）。真实 LLM 端到端为可选后续。

**子模块 4.3 LSP（第三做，依赖 4.1，完整实现）**
- 4.3.1 .lsp.json 解析：读 .lsp.json 或 lspServers，{command,args,extensionToLanguage,transport,env,initializationOptions,settings,workspaceFolder,startupTimeout,maxRestarts,diagnostics}
- 4.3.2 LSP client：启动 binary，JSON-RPC over stdio，实现 initialize/initialized/textDocument/didOpen/didChange/didSave/publishDiagnostics 完整协议
- 4.3.3 编辑事件探测：hook file_patch/code_run 编辑类操作触发 didChange
- 4.3.4 诊断注入：publishDiagnostics 格式化后注入 context（diagnostics=true 时）
- 4.3.5 生命周期：workspaceFolder/startupTimeout/maxRestarts
- 验证：测试 plugin 带 pyright（需装 binary），编辑 .py 后诊断出现

**状态**：4.1（配置体系）完成 ✅（4.1.1-4.1.5 全绿，Patch1-5）。4.2（background monitors）完成 ✅（16rep+3处GA核心patch+T1-T4全PASS）。第四层剩余：4.3 LSP 待做

## 风险点
1. GA skill 触发是"模型读描述命中→file_read"，namespace+slash 化要改注入格式和 slash_cmds，需兼容现有 flink-skills 注入风格
2. GA subagent 是进程级，Claude Code agents 是同进程声明式，语义无法完全对等（适配层只能功能等价）
3. hooks 阻塞/批准语义 GA tool_before 现为直通，需补阻塞能力
4. 改 GA 自身核心源码（skills_loader.py/slash_cmds.py）需谨慎，小步扩展+回归验证；plugins/ 下新增文件属自主范围

## 进度日志
- 2026-07-06: 计划制定并持久化，开始第一层实现
- 2026-07-06: 第一层完成并验证通过——plugin_loader.py 新增 + slash_cmds 两处接入 + 配置字段 + 测试 plugin 端到端验证全绿（发现/namespace/$ARGUMENTS/hook注入/无回归）。待用户确认后继续第二层
- 2026-07-07: 真实 LLM 端到端验证全绿 ✅——以 `GA_PLUGIN_DIRS=temp/plugin_e2e` 启动 GA（.venv python3.11 + `agentmain --func`），LLM 3 轮对话完成"先 echo 后 code_run"两步任务（Turn1 调 mcp__e2e-test__echo__echo → Turn2 调 code_run → Turn3 汇总 end_turn）。四验证点全 PASS：①plugin 收集✅(e2e-test 命中) ②MCP 工具调用✅(echo 返回 `echo: hello-plugin (token=secret_token_123)`) ③hook 触发✅(PreToolUse hook 写 hook_probe.txt) ④user_config 注入✅(`CLAUDE_PLUGIN_OPTION_api_token=secret_token_123` 进 env)。**修复 1 个 bug**：agent_loop.py@58 MCP dispatch fallback 成功路径原返回 `next_prompt=None`，被循环@127 误判"任务完成"break，导致调一次 MCP 工具即退出、不进 Turn2；改为 `next_prompt=self._get_anchor_prompt()`（与内置 do_code_run@ga.py:309 一致）后 agent 正常进 Turn2/3 连续多工具调用。第三层"真实 LLM 端到端"由"可选后续"转为"已验证通过"。测试产物已清理（plugins/plugin_configs.json 删除 + temp 产物删除）。
- 2026-07-07: 第四层 4.1（配置体系）完成 ✅。4.1.1 defaultEnabled + 4.1.2 userConfig schema（Patch1）→ 4.1.3 配置存储 plugin_configs.json 双层格式 pluginConfigs[id].options（Patch2）→ 4.1.4 配置采集 default 兜底 + required fail-open 跳过（Patch3+4，7项端到端全绿）→ 4.1.5 变量替换 ${user_config.KEY}/${CLAUDE_PLUGIN_ROOT}/${CLAUDE_PLUGIN_DATA}/${CLAUDE_PROJECT_DIR}/${ENV_VAR} + 导出 CLAUDE_PLUGIN_OPTION_<KEY> 环境变量（Patch5a-5e：_substitute_vars + _build_plugin_env 新增 + MCP/collect_mcp_tools/run_cc_hook 链路接入）。run_cc_hook 端到端 6 项全绿（ROOT/DATA/PROJ/OPTION/ARG1 替换 + decision 不 block）。关键根因：官方规范 CLAUDE_PROJECT_DIR（无 PLUGIN！）+ CLAUDE_PLUGIN_OPTION_<KEY> 的 KEY 原样不转大写（与 ${user_config.KEY} 一致），_build_plugin_env 源码正确，初版 [3][4] 测试失败系 hook.sh 变量名/大小写写错已修正。第四层剩余 4.2 monitors + 4.3 LSP。
- 2026-07-07: 第四层 4.2（background monitors）完成 ✅。4.2.1 monitors.json 解析 → 4.2.2 后台进程管理（复用 sche_tasks，按 when=always/on-skill-invoke 控制启动）→ 4.2.3 stdout 采集（_reader_loop 逐行读→buffer）→ 4.2.4 注入 context（复用 GA 现成非侵入通道 self.parent.intervene：ga.py L576 读→L578 注入 [MASTER]→L579 清空，pump 周期写 intervene，全局 _session_agent_ref 桥接避开 GA 核心传参）→ 4.2.5 生命周期（agent_loop L84 start_session_monitors + L141 stop_all_monitors + slash_cmds L708 start_skill_monitors）。验证：test_monitor_global T1-T4 全 PASS（设全局/pump写intervene/消费闭环注入[MASTER]+清空/stop清理）。改动：plugins/plugin_loader.py（16rep，新增 _session_agent_ref/_start_monitor/_pump_loop/_reader_loop/start_session_monitors/start_skill_monitors/stop_all_monitors/parse_monitors/collect_monitors + render_plugin_skill 加 when 启动）+ agent_loop.py（2处 patch：L84 session start、L141 session end）+ frontends/slash_cmds.py（1处 patch：L708 skill dispatch 后启动）。真实 LLM 端到端为可选后续。