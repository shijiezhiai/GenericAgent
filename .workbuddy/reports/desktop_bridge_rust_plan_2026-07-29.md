# 引入 Rust 的落地方案：desktop_bridge 网关层 + JSON-RPC 内核边界

> 依据对 `frontends/desktop_bridge.py`(6431 行 / 126 路由)、`agentmain.GenericAgent` 接口、`frontends/desktop/src-tauri/src/lib.rs` 的实地调研撰写。本文是**规划**，不含代码改动。

## 0. 目标与边界

**目标**：把"HTTP/WebSocket 网关"与"GenericAgent 内核"解耦，网关改用 Rust/axum 实现，并借机还清 bridge 直接戳 `agent._ga_*` / `agent.llmclient.backend.history` / `agent.abort()` 等活对象耦合的债；最终把 axum 网关内嵌进 Tauri 二进制，形成「单 Rust 进程 + 单 Python 内核子进程」。

**明确不做**（保留 Python）：
- `agent_loop.py` / `ga.py` / `llmcore.py` 内核 3K 种子代码 —— 其卖点是动态演化，不 Rust 化。
- `code_run` 进程内 `eval/exec` 注入 `handler` 的能力 —— 仍在 Python 内核里。
- 7 个 IM 机器人 / memory/*.py 自进化技能 / mykey·plugins 热重载 —— 仍在 Python 侧。
- 前端 `desktop/static/{app.js,index.html,styles.css}` —— 由网关静态托管，不改写。

## 1. 当前架构 vs 目标架构

```
现在                                         目标
┌─ Tauri(Rust) ─────────────┐               ┌─ Tauri(Rust, 内嵌 axum) ──────────┐
│  spawn_bridge_process     │               │  spawn kernel(Python, stdio)      │
│    └─ bridge(Python) ─────┼─:14168 HTTP   │  ├─ axum 网关(HTTP+WS+static) ─┐  │
│         ├ HTTP 路由       │   ↑WebView     │  │   ↕ JSON-RPC over stdio     │  │
│         ├ WS 流式         │               │  └─ kernel(Python) ────────────┘  │
│         ├ AgentManager    │               │       ├ 会话/持久化                │
│         │  (持有 agent    │               │       ├ run_agent_turn             │
│         │   活对象)       │               │       └ spawn conductor(:8900)     │
│         └─ spawn ─────────┼─:8900        │                                  │
│              conductor    │               └──────────────────────────────────┘
└──────────────────────────┘
```

关键变化：内核不再是"住在 bridge 进程里的活对象集合"，而是**通过 JSON-RPC 协议对话的独立子进程**；网关（Rust）只做传输、静态托管、进程监管。

## 2. 网关↔内核协议草案（JSON-RPC 2.0 over stdio）

传输：网关 spawn `python -m frontends.kernel_server`，用 **stdin/stdout  newline-delimited JSON** 通信（无额外 TCP 端口，最契合"单进程合并"）。异步流式靠 **server→client 通知**实现。

### 2.1 请求（gateway→kernel）
| method | params | 说明 |
|---|---|---|
| `session.list` | — | 返回 sessions[] |
| `session.create` | `{title?, folder_id?, cwd?}` | 返回 `{sid}` |
| `session.get` / `session.delete` / `session.patch` | `{sid, fields?}` | 元数据 CRUD（含 `folder_id` 赋值） |
| `session.messages` | `{sid, offset?, limit?}` | 历史消息 |
| `session.prompt` | `{sid, text, images?}` | 返回 `{task_id}`；随后流式通知 |
| `session.cancel` / `session.viewed` / `session.restore` | `{sid}` | 取消/已读/恢复上下文 |
| `session.suggest` / `session.slash` / `session.exec` | `{sid, ...}` | 变体 |
| `conv_folders.get` / `conv_folders.set` | `{assignments?}` | 读写 `temp/conv_folders.json` |
| `config.get` / `config.set` | — | 全局配置 |
| `model_profiles.list/get/set` | — | 模型档案 |
| `status` | — | 运行态摘要 |
| `projects.*` | `{name, ...}` | 项目模式（instruction/skills/experts/workspace…） |
| `datasources.*` | `{dsid?, ...}` | 数据源 |
| `files.list/browse/read/raw/diff/favorite` | `{path, ...}` | 工作区文件操作（相对 project_dir） |
| `files.upload` | `{path, dest}` | 网关先收 multipart 落盘，传路径给内核 |
| `token_stats` / `token_history` | — | token 统计 |

### 2.2 通知（kernel→gateway，server→client）
| method | params | 映射自 |
|---|---|---|
| `session.stream` | `{sid, event}` event∈`{token,segment,done,cancelled,error}` | `display_q` 的 `next`/`done` + `emit_session_state` |
| `session.state` | `{sid, status, partial, ts}` | `sess.partial` 实时预览态 |

> `display_q` 实测推送 `{"next":text,"turn":N,"outputs":[...]}` 与 `{"done":full,"outputs":[...]}`，bridge 据此维护 `sess.partial` 并落库 `llm_history` —— 这些语义由内核保留，网关只转发/持久化结果。

## 3. 分阶段实施

### Phase 0 — 协议契约 + Python 内核侧抽取（纯 Python 重构，完全可逆）⭐ 最高性价比
- **0.1** 写 `frontends/kernel_protocol.md` 锁定方法集/参数/通知/错误码。
- **0.2** 新建 `frontends/kernel_server.py`：把 `AgentManager`/`Session`/`make_agent`/`run_agent_turn`/持久化/`WsHub` 搬成 `AgentKernel`，对外只暴露 `handle_rpc(method, params)`，传输层从 aiohttp 换成 stdin/stdout JSON-RPC。
- **0.3** `desktop_bridge.py` 保持行为不变；新增"以 `python kernel_server.py` 独立运行"的能力并自测。
- **0.4** 写契约测试 `assets/kernel_protocol_smoketest.py`：subprocess 拉起内核 → 发请求/收通知 → 与现 bridge 行为断言一致。
- **退出标准**：旧 bridge 行为 100% 不变；内核可独立被 smoketest 驱动。**此步即还清耦合债**，且零 Rust 风险。

### Phase 1 — Rust/axum 网关 MVP（绞杀者模式，proxy 兜底）
- **1.1** axum 骨架：静态托管 `desktop/static/` + `/ga-ports.js`（端口参数化 `BRIDGE_PORT`）。
- **1.2** 实现 stdio JSON-RPC 客户端：`RpcClient`（request id 匹配 + 通知回调），持有 kernel 子进程 stdin/stdout。
- **1.3** `/ws`：浏览器 WS ↔ 内核 `session.stream`/`session.state` 通知转发。
- **1.4** 迁移无状态路由组（会话 CRUD、conv-folders、config/status、model_profiles、projects 只读）→ 每个 handler 构造 JSON-RPC 请求→等响应→转 HTTP。
- **1.5** `/services/*` 进程监管（spawn/kill conductor、读日志）移到 Rust —— 进程管理本就是 Rust 强项，且为 Phase 2 并入 Tauri 铺垫。
- **1.6** 未迁移路由（files 写 / datasources / upload / projects 写 / skill-expert-plugin-mcp 等）由 axum **反向代理到仍运行的旧 Python bridge（内部端口 14169）**，保证功能零回退。
- **1.7** 启动切换：Tauri `spawn_bridge_process` 改为 spawn kernel(stdio) + 启动 axum。
- **退出标准**：浏览器端全部现功能可用；Rust 接管 ≥60% 路由；smoketest 覆盖已迁移路由。

### Phase 2 — 全量迁移 + 单进程合并
- **2.1** 迁移剩余路由组，逐条删 proxy 规则，旧 bridge 退场（保留 `--legacy` 回滚模式）。
- **2.2** 把 axum 作为 Tauri `async_runtime` 内一个 task 启动（共享进程），不再单独 spawn bridge；kernel 作为 Tauri 的 stdio 子进程 spawn。
- **2.3** 端口收敛：浏览器只连 Tauri 的 14168（axum）；内核走 stdio（无 TCP）；conductor 仍 8900（Tauri 监管）。
- **2.4** 改写 `lib.rs` 现占大篇幅的 port-takeover / stale-bridge 逻辑 —— 改为对 kernel 子进程做健康检查与重拉，消除 14168 抢端口逻辑。
- **2.5** 删除 `desktop_bridge.py`（或留 `--legacy`）。
- **退出标准**：单 Rust 进程 + 单 Python 内核子进程；端到端行为与旧版一致；冷启动无抢端口问题。

### Phase 3（可选）— 内核侧热点 Rust 化
仅当协议稳定且确有性能/分发收益时：把最热的纯逻辑（会话持久化、conv-folder 索引）用 Rust 重写替换内核里的 Python 实现。因内核已是协议边界，替换对网关透明。

## 4. 代价重估（针对本切口）

| 阶段 | 周期 | 人数 | 风险 | 可独立交付价值 |
|---|---|---|---|---|
| Phase 0 | 2–3 周 | 1 | 低 | 还清耦合债，行为不变 |
| Phase 1 | 3–5 周 | 1 | 中 | Rust 网关上线，proxy 兜底 |
| Phase 2 | 4–8 周 | 1 | 中 | 单进程合并，启动收敛 |
| **合计** | **~10–16 周 ≈ 2.5–4 人月** | | | 远小于全量重写的 6–12 人月 |

> Rust 新手熟悉 axum + Tauri 集成会占 Phase 1 部分时间；macOS 上 Tauri 构建需 `SDKROOT`/`CC` 指向 CommandLineTools 的 clang（见项目 MEMORY），Phase 2 触碰 Tauri crate 时必踩。

## 5. 关键风险与应对
1. **流式保真**：`token/segment/done/cancel` 时序与 `sess.partial` 实时预览 —— Phase 0 必须写契约测试锁住，否则前端会"假死/乱跳"。
2. **二进制上传**：multipart 走 stdio JSON-RPC 不便 → 网关侧落盘、传路径给内核（已在 2.1 体现）。
3. **内核崩溃恢复**：内核抽成子进程后，网关要能重拉 kernel 并可靠 `restore` 会话 —— 这反而比"bridge 崩=全崩"更稳。
4. **多前端共用内核**：浏览器直连 / Streamlit / Tauri 现共用同一 bridge；抽成独立 kernel 后其他前端也能连（利好），但需约定单内核实例与认证（local-only 守卫已在 `local_only_guard`）。
5. **Tauri 启动逻辑耦合**：`lib.rs` 大量针对 :14168 的 takeover 逻辑，Phase 2 才动，Phase 1 保留旧 bridge 端口共存。

## 6. 需你确认的三个决策点
- **D1 传输方式**：stdio（推荐，零端口、最契合单进程）vs Unix socket vs localhost TCP。
- **D2 是否先做 Phase 0**：强烈建议先做（纯 Python、可逆、直接还清耦合债），再碰 Rust。
- **D3 网关形态**：Phase 2 内嵌 Tauri（推荐，单进程）vs 长期保留独立 Rust 网关进程（部署更简单、但多一个进程）。

## 7. 建议落地顺序
先执行 **Phase 0（D2=是）**，交付一份 `kernel_protocol.md` + 可独立运行的 `kernel_server.py` + 契约测试；此步无论后续是否引入 Rust 都值得做（解耦债本身就有价值）。确认 D1/D3 后再启动 Phase 1。
