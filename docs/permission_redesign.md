# GA 权限管理重设计 — 分析与改进方向

> 状态：**P0 + P1 + P2 + P2.5 + P3 + P4 已实施**（2026-08-12）；IM 交互确认、子 agent 权限衰减、IM 发起人鉴权均已落地，待集成冒烟。所有改动**均未 commit**。
> 结论先行：GA 当前**没有权限层**，只有两处硬编码门禁 + 若干条提示词软约束。可插桩点已存在（`BaseHandler.dispatch`），但只对外部 CC plugin 开放且 fail-open。
> 五条关键决策（2026-08-11 已拍板）：① 默认 `workspace-write`；② 所有前端补齐 ask 态；③ 做 OS 级沙箱（P3）；④ 直接删 `inline_eval`；⑤ 规则与审计存 **DuckDB**。

---

## 一、现状盘点（代码事实）

### 1.1 工具执行链路与唯一插桩点

```
llmcore.chat → agent_loop.py:114-116 解析 tool_calls
             → agent_loop.py:126 handler.dispatch(...)
             → BaseHandler.dispatch (agent_loop.py:20-39) 反射 do_<tool>
             → ga.GenericAgentHandler.do_xxx 直接执行物理操作
```

`dispatch` 内已有 PreToolUse 语义骨架（`agent_loop.py:26-36`）：支持 `stop` / `block` / `updated_input`，实现在 `plugins/plugin_loader.py:1046-1137`。但有三个致命限制：

| 限制 | 位置 | 后果 |
|---|---|---|
| 只服务外部 CC plugin（shell 子进程） | `plugin_loader.py:1096` | 内置策略无法复用，每次调用 fork 一个进程，30s timeout |
| **fail-open**：异常/超时/非零退出/非 JSON 一律放行 | `plugin_loader.py:1102-1118` | 门禁不可靠，安全语义下应 fail-closed |
| `permission_mode` 硬编码 `"default"` | `plugin_loader.py:1091` | 无模式概念，hook 无从判断当前授权级别 |

Python 侧 `plugins/hooks.py` 的 `trigger()` **本身支持回写 ctx**（`hooks.py:20-22`），但 `agent_loop.py:24` 调用时**丢弃返回值** —— 能力已具备，只差接线。

### 1.2 现存门禁：只有两处

| 门禁 | 位置 | 说明 |
|---|---|---|
| `git_checkpoint restore` 需 `no_confirm=true` | `ga.py:385-386` | 二次确认由 LLM 自己传参完成，本质是"自我授权" |
| main/master 禁 commit | `ga.py:330-335` | 唯一的硬拒绝 |

**不存在**：命令黑白名单、路径限制、`rm -rf` 拦截、网络出站控制、凭证访问控制。

其余全是提示词软约束：`assets/sys_prompt.txt:5`「不可逆操作先询问用户」、`ga.py:1231/1240` 高轮次注入 `[DANGER] Must call ask_user`、`memory/autonomous_operation_sop.md:39-42`「绝对禁止读取密钥」。**模型不遵守时没有任何兜底**。

### 1.3 四个具体漏洞

**① 路径逃逸（一行代码级）**

```python
# ga.py:495-497
def _get_abs_path(self, path):
    return os.path.abspath(os.path.join(self.cwd, path))
```

`os.path.join` 遇绝对路径直接丢弃前缀。`file_write(path="/Users/x/.ssh/authorized_keys")` 畅通无阻。`file_read` / `file_patch` / `expand_file_refs`（`ga.py:190-202`）同理。`code_run` 的 cwd（`ga.py:513-514`）同样逃逸。

**② `inline_eval` 是完全的逃逸口**

```python
# ga.py:517-518
if code_type == 'python' and args.get("inline_eval"):
    ns = {'handler':self, 'parent':self.parent, 'history':...}
```

在 GA **自己的进程内** `eval/exec`，命名空间直接给出 `handler` / `parent`。任何进程外沙箱对它都无效 —— 模型可以改自己的权限对象、读 `mykey` 内存、篡改 history。

**③ 凭证随 env 泄漏给子进程**

`code_run` 的 `subprocess.Popen`（`ga.py:69-74`）继承父进程完整环境。GA 进程里有全部 LLM API key。任意 `code_run` 都能 `os.environ` 全量外传。

**④ IM 前端无鉴权**

`wechatapp` / `wecomapp` / `qqapp` / `dingtalkapp` 收到的远程消息直接进同一个 handler，可触发任意 `code_run`（含 `inline_eval`）。且这几个前端**完全没有 INTERRUPT 处理**（`desktop_bridge.py` 同样零命中），`ask_user` 在这些前端上表现为「agent 沉默退出」。

### 1.4 架构级错配：`ask_user` 不是审批原语

```python
# ga.py:531-536
def do_ask_user(self, args, response):
    result = ask_user(question, candidates)
    return StepOutcome(result, next_prompt="", should_exit=True)   # ← 终止整个 run
```

`ask_user` 语义是**中断并退出 agent loop**，不是**挂起等待后恢复**。恢复要靠前端捕获 INTERRUPT 再发起新 run（只有 `tgapp.py:252-290`、`tuiapp_v2.py:4284-4306`、`tui_v3.py:1348` 实现了）。

这决定了「询问」成本极高，也解释了为什么 GA 只能靠提示词劝模型少问。**任何 `ask` 态权限设计都以修好这一点为前置条件** —— 否则三态只能退化成 allow/deny 二态。

### 1.5 场景权限一律拉平

- `--no-user-tools` 把 `ask_user` 从 schema 直接删掉（`agentmain.py:27,32`），不是"自动批准"而是"无法提问"，agent 只能自行决断。
- 子 agent（`agentmain.py --func/--task` 独立进程）权限与主 agent **完全相同**，无衰减。
- 定时任务（`reflect/scheduler.py`）、自主模式（`reflect/autonomous.py`）只是产生 prompt 唤醒 agent，不改权限。
- `agent_loop.py:190-198` 每轮自动跑 lint/test（`subprocess.run`），无审批。

### 1.6 可复用的地基

`ga_config.py` 已是成熟配置层：DuckDB 为真源、三级访问（attached / loopback proxy / transient）、`GA_STORAGE=json` 回退。现有表 `skill_meta` / `plugin_meta`。**权限规则和审计日志可直接挂上去，不需要造新存储。**

---

## 二、主流 Agent 的权限模型

### 2.1 Claude Code：模式 × 规则三态

- **Permission Mode**：`default` / `acceptEdits` / `plan` / `dontAsk` / `bypassPermissions` / `delegate`
- **规则三态**：`deny > ask > allow`，第一条命中即生效，deny 恒优先
- **规则语法**：`Tool` 或 `Tool(specifier)`，如 `Bash(npm run test *)` / `Read(./.env)` / `WebFetch(domain:example.com)` / `mcp__github__*`
- **四级配置来源**：managed（组织）> `.claude/settings.local.json` > `.claude/settings.json`（入库共享）> `~/.claude/settings.json`
- **内容级规则优先于工具级规则**：可以整体放行 `Read`，再单独 deny `Read(~/secrets.json)`
- 官方明确承认：**Bash 参数约束是脆弱的**，推荐改用 deny 网络命令 + WebFetch 域名白名单，或用 PreToolUse hook 做真校验

### 2.2 Codex CLI：沙箱 × 审批策略（两个正交维度）

| `sandbox_mode` | 文件系统 | 网络 |
|---|---|---|
| `read-only` | 不可写 | 禁 |
| `workspace-write` | 仅 CWD + 配置的 writable_roots + /tmp | 默认禁 |
| `danger-full-access` | 无限制 | 无限制 |

| `approval_policy` | 行为 |
|---|---|
| `untrusted` | 仅内置可信只读命令自动跑，其余全问 |
| `on-request` | 沙箱内自由跑，越界才问 |
| `on-failure` | 命令失败才问 |
| `never` | 不问（配严格沙箱用于 CI） |

三个关键设计值得抄：

1. **保护路径**：即使在可写根内，`.git` / `.codex` / `.agents` 递归只读 —— 防止 agent 破坏仓库历史和自己的配置。
2. **升级流（escalation）**：沙箱内执行失败 → 请求审批 → 批准后**放宽策略重试**。把「沙箱太紧」从阻塞变成一次交互。
3. **auto_review**（2026 新增）：审批请求先过一个 guardian 子 agent，按数据外泄 / 凭证探测 / 持久性削弱 / 破坏性操作四类风险打分，critical 直接拒、high 需授权。prompt 构建失败 / 解析失败 **fail-closed**。

### 2.3 关键教训：文本过滤挡不住 shell（GuardFall）

2026 年的 GuardFall 研究对 11 个开源 agent 做 shell 注入测试，**10 个被绕过**：OpenHands、Aider、Open Interpreter、Cline、Goose、SWE-agent、Roo-Code、Plandex、opencode、Hermes。唯一幸存的 Continue 是因为做了 bash 感知的 tokenize，而非字符串匹配。

原因是组合爆炸：`$IFS` 展开、命令替换、花括号展开、base64 解码可任意组合，同一命令的等价表示无上界。要正确 canonicalize 等于重写一遍 bash 解析器。

> **对 GA 的直接含义：不要把「正则黑名单拦 `rm -rf`」当作安全边界。** 它可以作为 UX 提示（提醒用户这条命令危险），但真正的边界必须下沉到内核（seatbelt / landlock / bwrap），或者退一步——只在**路径**这种可归一化的维度上做强校验。

---

## 三、GA 的改进方向

### 3.1 目标模型：三层 + 两维度

```
┌─ L2 策略层  mode × rules  ─────────────────────────┐
│   sandbox_mode:  readonly | workspace | full        │
│   approval:      untrusted | on-request | never     │
│   rules:         deny > ask > allow, Tool(spec)     │
├─ L1 决策点  PermissionBroker @ dispatch ────────────┤
│   统一入口：内置工具 + MCP 工具 + CC plugin hook     │
│   输出 allow / ask / deny + 审计事件                 │
├─ L0 执行层  真实边界 ───────────────────────────────┤
│   路径归一化校验（realpath，防 symlink/../）          │
│   OS 沙箱：sandbox-exec(mac) / bwrap+landlock(linux) │
│   env 白名单剥离凭证                                 │
└─────────────────────────────────────────────────────┘
```

分层的意义：**L2 可以被绕过（模型骗过规则），L0 不能**。安全承诺只由 L0 提供，L2 负责减少打扰。

### 3.2 落点：单点插桩，不散弹

改造集中在三处，符合仓库「小变更半径」规范：

| 文件 | 改动 |
|---|---|
| `plugins/permission_store.py`（新增） | 独立 DuckDB（`temp/ga_permission.duckdb`）存规则表 + 配置表 + 审计表；无 duckdb 时降级 JSON 文件（与审计 JSONL 同哲学）。刻意绕开 `frontends/db_store.py` 的"四处同步"雷区 |
| `plugins/permissions.py`（新增，~230 行） | `PermissionBroker`（fail-closed，deny>ask>allow，按 mode×tool×target_glob 匹配）+ `/permissions` CLI |
| `agent_loop.py` `dispatch` | 审计之后、工具执行之前插入 Broker：deny→阻断；ask→非交互拒绝/交互挂起；allow→放行。统一覆盖内置 + MCP + CC hook |
| `ga.py`（P0） | `_safe_write_path` 路径边界；删 `inline_eval`；`_sanitize_child_env` 剥离凭证；非交互下 ask_user 自动拒绝 |
| `agentmain.py`（P1） | `GenericAgent.__init__` 接 `permission_mode`/`frontend`/`non_interactive`；`_handle_slash_cmd` 加 `/permissions` |

> 决策修正（vs 原案 3.2）：规则与审计**未**挂 `ga_config.py`，改用独立 `permission_store`，原因见上文"刻意绕开 db_store 同步雷区"。存储仍统一为 DuckDB（决策#5）。

### 3.3 六个关键设计决策

**① 规则维度选型：按可归一化程度分级**

| 工具 | 规则维度 | 强度 |
|---|---|---|
| `file_read/write/patch`、`file_find`、`code_search` | **路径**（realpath 后 glob） | 强，可作安全边界 |
| `web_scan`、`web_execute_js`、MCP 工具 | 域名 / server名 / tool名 | 中 |
| `code_run` | 仅粗分类 + 沙箱 | **弱，不作安全边界** |
| `git_checkpoint` | action 枚举 | 强 |

`code_run` 的命令字符串匹配只用于「要不要弹审批」，不用于「能不能跑」。

**② 默认拒绝清单（不可被 allow 覆盖，除非显式 bypass）**

- 读：`mykey.py`、`.env*`、`~/.ssh/**`、`~/.aws/**`、`~/.config/gh/**`、浏览器 Cookie 库
- 写：`.git/**`（借鉴 Codex）、GA 自身源码（自主模式下）、`~/Library/**`、`/etc/**`
- `inline_eval`：默认关闭，仅 `full` 模式 + 显式开关

**③ 场景化默认 profile**

| 场景 | sandbox | approval | 备注 |
|---|---|---|---|
| 交互 CLI/TUI/桌面 | workspace | on-request | 日常默认 |
| IM 远程（wechat/qq/…） | readonly | untrusted | **且必须先做发起人鉴权** |
| 定时任务 / 自主模式 | workspace | never | 靠沙箱兜底，禁读凭证、禁改 GA 源码 |
| 子 agent | **⊆ 父 agent** | 继承 | 权限单调衰减，不可提权 |
| CI / headless | readonly | never | — |

**④ `ask` 态需要「挂起-恢复」，这是最大的一块**

现状 `ask_user` = 终止 run。要支持 `ask` 规则，需要：
- 新增 pending-approval 状态（不复用 `ask_user`，避免语义混淆）
- 统一前端协议：`desktop_bridge.py` 补齐（当前零处理），IM 前端要么补齐要么**降级为 deny**（更安全，推荐）
- 会话可恢复 —— 可考虑复用现有 `partial_state` 机制

**如果这块暂不做，权限模型先退化为 allow/deny 二态也完全可用**，不阻塞 P0/P1。

**⑤ fail-closed vs fail-open**

现有 CC hook 是 fail-open（`plugin_loader.py:1102-1118`）。内置 Broker 应 **fail-closed**：策略加载失败 / 规则解析异常 → 拒绝并报错，不静默放行。CC hook 的 fail-open 保留（兼容性），但审计要记。

**⑥ 迁移不能破坏现有行为**

GA 现在是事实上的 full-access。硬切默认会让所有存量自主任务和 IM bot 集体失效。建议：
- 首版默认 = `full` + `never`（行为不变）+ **审计日志全开**
- 跑一段时间后看审计数据，再决定收紧到 `workspace` + `on-request`
- 新装用户默认 `workspace`
- 提供 `--yolo` / `GA_PERM=off` 逃生舱

### 3.4 分阶段路线

| 阶段 | 内容 | 风险 | 收益 | 状态 |
|---|---|---|---|---|
| **P0** | 路径逃逸修复；敏感文件默认 deny；`code_run` env 白名单；`tool_before` 接线可 block；审计日志 | 低 | 高 —— 堵住最现实的四个漏洞 | ✅ 已完成 |
| **P1** | `PermissionBroker` + mode/rules + `/permissions` 斜杠命令 | 中 | 高 —— 真正的策略层 | ✅ 已完成 |
| **P2** | ask 态挂起-恢复 + 前端协议统一（session 记忆根治循环；CLI 同步 / TUI 双版本 INTERRUPT；IM 交互确认留 P2.5） | 中高 | 中 —— 让 ask 可用 | ✅ 已完成（核心+CLI+TUI） |
| **P3** | OS 沙箱（mac `sandbox-exec` / linux `bwrap`）封装为 L0 真边界 | 高 | 高 —— 唯一真实边界 | ✅ 已做 |
| **P4** | 子 agent 权限衰减；IM 发起人鉴权；auto-review guardian（guardian 留待后续） | 中 | 中 | ✅ 已完成（衰减 + 发起人鉴权） |

P0 可独立交付且**不引入任何新概念**，建议先做。

---

## 四、待拍板的问题

1. **默认激进度**：首版保持 full-access + 审计（无感），还是直接默认 workspace-write（会打断存量自主任务）？
2. **ask 态做到什么程度**：全前端补齐，还是只做桌面 + TUI、IM 一律 deny？
3. **P3 沙箱做不做**：工作量最大且 mac/linux 差异明显。不做的话安全承诺止步于「路径级强校验 + 审计」，`code_run` 仍是敞开的。
4. **`inline_eval` 去留**：它让任何沙箱失效。保留（仅 full 模式）还是直接删掉？
5. **规则存储**：DuckDB（`ga_config` 已有）还是 json 文件（可入库共享、便于 diff review）？倾向 json 为主 + DuckDB 存审计。

---

## 五、实施进度（2026-08-11 ~ 08-12）

### P0（2026-08-11 完成）

- 新增 `plugins/permission_store.py`：独立 `temp/ga_permission.duckdb`，表 `permission_audit` / `permission_rules` / `permission_config`；无 duckdb 时审计降级 JSONL、规则/配置降级 JSON。
- `ga.py`：
  - `_get_abs_path` 改 realpath 归一化；新增 `_safe_write_path`，拒绝写入 `~/.ssh` / `/etc` / `/System` / Keychains 及工作区外（修复路径逃逸漏洞①）。
  - 删除 `inline_eval` 分支（漏洞②）。
  - `_sanitize_child_env()` 剥离凭证类 env 后传给 `code_run` 子进程（漏洞③）；正则用"字母数字边界"避免误杀 `PATH`。
  - `do_ask_user` 在非交互模式（env `GA_NON_INTERACTIVE` 或 `parent.non_interactive`）自动拒绝并继续（漏洞④ 的兜底）。
- `agent_loop.py` `dispatch` 中央接入 `record_audit`，覆盖所有前端。
- `assets/tools_schema*.json` 移除 `inline_eval` 参数。

### P1（2026-08-12 完成）

- 新增 `plugins/permissions.py`：
  - `PermissionBroker.fail_closed`：评估 `deny > ask > allow`，按 `(mode, tool, target_glob)` 匹配，特异性 tie-break（精确 tool > 通配 tool > `*`，显式 target > 无）。**任何异常 → DENY**。
  - 模式默认策略：`read-only`/`plan` 仅只读工具放行；`workspace-write` 工作区内读写 + `code_run` 放行（沙箱留 P3）；`full-access` 全放行。
  - `permissions_cli()`：`/permissions [list | mode=<m> | add <tool> <dec|ask|allow> [glob] | rm <id>]`。
- `agent_loop.py` `dispatch`：Broker 决策在审计之后、工具执行之前；
  - `deny` → 返回 error（`should_exit=False`，不杀 run，让模型换方案）+ 审计 `deny`；
  - `ask` → 见 P2（session 记忆 + 三路分发：CLI 同步 input / TUI INTERRUPT / 无能力前端安全降级 deny）；
  - `allow` → 放行（仍过 CC hook，fail-open 兼容）。
- `agentmain.py`：`GenericAgent.__init__` 接 `permission_mode`（env/持久化默认 > env 覆盖 > `workspace-write`）、`frontend`、`non_interactive`；`_handle_slash_cmd` 加 `/permissions` 分发。
- 冒烟测试 23 项全过（模式策略、规则特异性、fail-closed、`/permissions` CLI、dispatch 阻断、ask+headless 拒绝），覆盖 DuckDB 缺失时的 JSON 降级路径。

### P2（2026-08-12 完成 — 核心 + CLI + 双 TUI）

**核心机制**
- `plugins/permissions.py`：
  - `record_session(parent, tool, decision, mode)` + `PermissionBroker.evaluate` 开头查 `parent._perm_allow/_perm_deny`（`mode|tool` 键）。**首次 ask 用户决定后写入 session 记忆，后续同工具调用直接短路**——这是根治"模型重发同一调用反复弹窗"循环的关键，等价于 Claude Code 的 "Allow once for session"。
  - 新增 `PERMISSION_INTENT = "PERMISSION_REQUEST"` 常量，与 `ask_user` 的 `HUMAN_INTERVENTION` 区分。
- `agentmain.py` `GenericAgent.__init__`：加 `self._perm_allow/_perm_deny = set()` + `self.ask_capable = False`（默认 False；无此能力的前端走安全降级 deny，不再静默退出）。
- `agent_loop.py` `dispatch` 的 `ask` 分支改为三路分发（根治循环 + 前端协议统一）：
  - **session 命中** → 直接 allow/deny 短路，不再询问；
  - **非交互（headless/调度/自主）** → 记 session deny + 返回 error（`should_exit=False`，让模型换方案，不杀 run）；
  - **CLI（`frontend=='cli'` 且 stdin 是 tty）** → 同步 `input()` 授权，结果写入 session 记忆；
  - **`ask_capable` 前端（TUI/桌面）** → 产生 `PERMISSION_REQUEST` INTERRUPT（`should_exit=True`），复用 `ask_user` 既有的 `turn_end_hook → 重新 put_task` 跨 run 恢复通道；用户在前端卡片允许/拒绝，前端把决定写入 session 记忆后重新触发 run，模型重发工具 → broker 短路放行/拒绝；
  - **其他交互前端（IM/未实现）** → 安全降级 deny + 明确提示（不再静默退出）。
- 前端补齐：
  - `tuiapp_v2.py`：`default_agent_factory` 设 `frontend='tui'` + `ask_capable=True`；`AgentSession` 加 `permission_events` 队列；`_install_ask_user_hook` 识别 `PERMISSION_REQUEST` 并 push；新增 `_drain_permission_events`（done 时渲染 允许/拒绝 卡片）+ `_answer_permission`（写 session 记忆 + 触发重发）。
  - `tui_v3.py`：`AgentBridge` 设 `frontend='tui'` + `ask_capable=True`；加 `_perm_pending`；`_on_turn_end` 捕获 `PERMISSION_REQUEST` 并复用 `ask_user_queue` 渲染 允许/拒绝 卡片；`_apply_perm_answer` 把回答写入 session 记忆。
- 冒烟测试 10 项全过：session 短路覆盖规则、CLI 同/异步 allow/deny 记忆、headless 安全 deny、INTERRUPT 携带正确 intent、无能力前端安全降级。

### 下一阶段待办（未做 / 待验证）

- **auto-review guardian**：P4 未含 Codex 式「审批前过 guardian 子 agent 打分」；如需 critical 自动拒，后续补 `plugins/permission_guardian.py` + `PermissionBroker` 接入点。
- **集成冒烟**：tgapp / qtapp / conductor 权限 ask 流程未做端到端冒烟；P4 衰减建议验证「subagent 模式应低于父」。
- **`frontend` 标识细化**：IM 各 bot 已显式设 `ask_capable=True` + `frontend=<source>`，但 `frontend` 取值目前等于 source 名（telegram/wechat/...），与 TUI 的 `'tui'` 不一，属良性差异。
- **提交**：本阶段 P0–P4 全部改动**仍未 commit**，待走 `git_checkpoint` 评审后入库。

### P2.5 实施记录（2026-08-12）— IM / 桌面交互确认

镜像 `ask_user` 已验证的 INTERRUPT→`turn_end_hook`→重新 `put_task` 跨 run 续跑机制，复用 `agent._perm_allow/_perm_deny` 会话记忆避免循环。

- `plugins/permissions.py`：
  - 新增 `extract_permission_event(ctx)`：从 turn_end hook ctx 抽取 `PERMISSION_REQUEST` INTERRUPT，返回 `{"tool","mode","target","question"}` 或 `None`。
  - `record_session(..., initiator=...)`：末尾 `if initiator: parent.initiator = initiator`（写入任务发起人，供 P4 鉴权）。
  - `answer_permission(agent, tool, decision, mode, resume_prompt, initiator)`：记录 session 后 `agent.put_task(resume_prompt, source=frontend)` 跨 run 续跑。
  - 新增 `permission_decision_allowed(agent, requester)`：无 initiator 或无 requester → 放行（向后兼容）；否则比对 `requester == agent.initiator`。
- `frontends/tgapp.py`：模块级 `agent.ask_capable=True; agent.frontend="telegram"`；新增 inline 键盘（`perm:` 前缀 callback）Allow/Deny + `/perm allow|deny <tool>` 文本兜底；`_register_permission_hook` 写 `_perm_events` 队列；`handle_msg` 设 `agent.initiator=f"telegram:{uid}"`；`answer_permission(..., initiator=...)`；`handle_permission_callback` 与 `cmd_perm` 均加 `permission_decision_allowed` 守卫（非发起人拒绝）。
- `frontends/qtapp.py`（PySide6 桌面 GUI）：`ChatPanel` 加 `permission_requested` Signal；`_show_permission_dialog` 模态 Allow/Deny；`_resume_after_permission` 经 display_queue 续跑；`main()` 设 `ask_capable=True/frontend="qt"` 并注册 `_qt_permission_hook`（emit→阻塞等决策→`answer_permission`→QueuedConnection 续跑）。
- `frontends/chatapp_common.py`（混入层，覆盖 qq/dingtalk/wecom/fsapp）：`AgentChatMixin.__init__` 设 `ask_capable=True/frontend=self.source` + 注册 `_perm_exit_hook`（写 `agent._last_exit_reason`）；新增 `_permission_prompt` / `_maybe_send_permission_prompt`；`handle_command` 加 `/perm`（含 `permission_decision_allowed` 守卫 + `initiator=f"{source}:{chat_id}"`）；`run_agent` 完成时优先提示权限而非 send_done。
- `frontends/fsapp.py` / `wecomapp.py`：各自重写 `run_agent`，在 `done` 分支接 `extract_permission_event` → 发 `_permission_prompt`。
- `frontends/dcapp.py`（per-chat 独立 agent）：`_get_agent` 内每个新建 agent 设 `ask_capable` + 注册 `_perm_exit_hook`；`handle_command` 加 `/perm`（守卫 + initiator）。
- `frontends/wechatapp.py`（单共享 agent）：模块级设 `ask_capable=True/frontend="wechat"` + `_perm_exit_hook`；`_handle` 重构为模块级 `_run_agent_task(prompt, uid, ctx)`（done 分支加权限检测）；`on_message` 加 `/perm`。
- 8 个前端文件经 `py_compile` 全过；`permissions.py` 的 `extract_permission_event/answer_permission/record_session` 经 FakeAgent 单测全断言通过（`ALL_OK`）。

### P4 实施记录（2026-08-12）— 子 agent 衰减 + IM 发起人鉴权

- **子 agent 权限衰减（conductor_core.py）**：
  - 新增 `_PERM_PRIV_ORDER = {"plan":0,"read-only":1,"workspace-write":2,"full-access":3}` 与 `_decay_permission_mode(parent_mode=None)`：child = max(0, 父序−1)，父为 `plan` 时保持 `plan`（无法再降）。
  - `SubagentPool` 加 `parent_permission_mode`；`Conductor._run` 创建 master 后写入 `pool.parent_permission_mode = self.agent.permission_mode`（master 即父）。
  - `start_subagent`：`agent.permission_mode = _decay_permission_mode(pool.parent_permission_mode)`（真正「父 − 1」，非固定配置默认）；`agent.initiator = f"subagent:{sid}"`。
  - `Conductor._run` 自身的 master agent 为父级，**不衰减**（符合「子 ≤ 父」）。
- **IM 发起人鉴权（贯穿各前端）**：
  - `agent.initiator` 在消息入口处设为人标识：`tgapp`→`telegram:{uid}`、`chatapp_common`→`{source}:{chat_id}`、`dcapp`→per-chat、`wechatapp`→`wechat:{uid}`。
  - 权限决策点（tgapp `handle_permission_callback`/`cmd_perm`、chatapp_common `/perm`、dcapp `/perm`、wechatapp `/perm`）均先 `permission_decision_allowed(agent, <当前人>)` 校验，非发起人拒绝并记录提示；`answer_permission(..., initiator=<当前人>)` 回写发起人。
  - `permission_decision_allowed` 无 initiator / 无 requester 时向后兼容放行（老会话无 initiator 仍可决策）。
- 漏点修复：`tgapp.handle_permission_callback` 原守卫在 `edit_message_text` 之后（非发起人仍能写决策），已提前到状态变更之前。

### P3 实施记录（2026-08-12）

- 新增 `plugins/sandbox.py`：`wrap_code_run(cmd, cwd, extra_roots, deny_network)`。
  - **macOS**：`sandbox-exec` seatbelt profile —— `(allow default)` + `(deny file-write*)` + 仅允许 `cwd` / `tmp_dir` / `~/.cache` / `~/Library/Caches` / `/tmp`(`/private/tmp`) / `/private/var/folders` / `/dev` 子路径写；`(deny file-read*)` 经典密钥库（`.ssh/` `.aws/` `.gnupg/` `.config/gcloud/` `.config/gh/` `Keychains/` `mykey.py` `id_rsa` `id_ed25519` `.netrc` `credentials` `/etc/shadow` `/etc/sudoers`）。网络默认放行，可选 `GA_SANDBOX_DENY_NETWORK=1` 收紧。
  - **Linux**：检测到 `bwrap` 时等价封装；否则降级无沙箱。
  - **fail-closed**：darwin 且存在 `sandbox-exec` 但 profile 构建失败时**抛异常** → `code_run` 返回 error，绝不裸跑。
  - 逃生舱：`GA_DISABLE_SANDBOX=1` 完全跳过沙箱（调试用）。
- `ga.py` `code_run` 在 `Popen` 前调用 `wrap_code_run`；`finally` 中清理临时 profile 文件；沙箱不可用（darwin 无 sandbox-exec）时记一条 `unavailable` 审计事件。与 P0-T3 的 `_sanitize_child_env` 凭证剥离协同。
- **踩坑（已修）**：① seatbelt 在本版 macOS 不支持 `#"..."#` 读取器，改用 `(regex "...")` 并在字符串内转义 `\`；② `/tmp` 是 `/private/tmp` 的符号链接，seatbelt 按真实路径匹配，必须同时允许两者；③ 初版 `*.pem$` 规则误杀了系统 CA 包 `/etc/ssl/cert.pem`，导致沙箱内全部 TLS 失败（curl exit 77）——已移除该宽规则，仅保留高置信密钥目录。
- 冒烟测试 12 项全过：wrap 生效、`/dev/null` 写放行、工作区内写成功、越界写拒、密钥读拒、普通文件读放行、默认网络放行、env 收紧网络拒、`code_run` 集成越界写阻/区内写成。

#### 沙箱环境变量
```
GA_DISABLE_SANDBOX=1           # 完全关闭沙箱（调试）
GA_SANDBOX_DENY_NETWORK=1      # 在沙箱内额外禁止网络出口（高安全场景）
```

### 用法速查

```
/permissions                          # 查看当前模式 + 规则
/permissions mode=read-only           # 切换并持久化
/permissions add code_run ask         # 当前模式给 code_run 加"需确认"
/permissions add file_write deny "*.secret"   # 带 glob 的 deny
/permissions rm <id>                  # 删规则
# 环境变量：GA_PERMISSION_MODE / GA_FRONTEND / GA_NON_INTERACTIVE
```

