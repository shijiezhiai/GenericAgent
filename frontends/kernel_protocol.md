# GA Desktop Kernel — JSON-RPC 2.0 协议（stdio 传输）

> 本文是「desktop_bridge 网关层 Rust 化」Phase 0 的契约基线。内核 = `frontends/kernel_server.py`，
> 业务逻辑 100% 复用 `desktop_bridge.py` 的 `AgentManager`（未改动），仅把传输层从 aiohttp HTTP/WS
> 换成 stdio 上的 JSON-RPC 2.0。网关（未来 Rust/axum）通过此协议与内核对话。

## 1. 传输

- 内核作为子进程由网关 spawn：`python frontends/kernel_server.py`（cwd = 仓库根）。
- **stdio，newline-delimited JSON**：每行一条完整的 JSON 消息。
- 请求（gateway→kernel）：`{"jsonrpc":"2.0","id":<int>,"method":<str>,"params":<obj>}`，`id` 必填。
- 响应（kernel→gateway）：`{"jsonrpc":"2.0","id":<int>,"result":<obj>}` 或
  `{"jsonrpc":"2.0","id":<int>,"error":{"code":<int>,"message":<str>}}`（`code` 见 §4）。
- 通知（kernel→gateway，无 `id`）：`{"jsonrpc":"2.0","method":<str>,"params":<obj>}`——用于流式。
- 关闭：gateway 发 `{"method":"kernel.shutdown"}` 或关闭 stdin，内核退出。
- 所有 stdout 写入均经同一把锁，保证一行一条消息；stderr 仅供日志，不参与协议。

## 2. 请求方法（gateway→kernel）

方法名统一 `domain.action`。`sid` 为会话 id 字符串。

| method | params | result |
|---|---|---|
| `status` | — | `{ok,running,ready,gaRoot,mykeyPath,sessionCount,activeSessionId,transport}` |
| `config.get` | — | `{gaRoot,mykeyPath,config}` |
| `config.set` | `{config}` | `{ok,gaRoot,mykeyPath,config}` |
| `session.list` | — | `{sessions:[snapshot],activeSessionId}` |
| `session.create` | `{cwd?,project?}` | `{ok,sessionId,session}` |
| `session.get` | `{sid}` | `{sessionId,session, messages, partial}` |
| `session.delete` | `{sid}` | `{ok,...}` |
| `session.patch` | `{sid, fields:{title?,pinned?,untitled?,plan_scan_baseline?,folder_id?}}` | `{ok,session}` |
| `session.messages` | `{sid, after?, limit?}` | `{messages, ...}` |
| `session.plan` | `{sid}` | plan snapshot |
| `session.prompt` | `{sid, prompt, images?, llmNo?, display?, files?, imageMetas?, expert?}` | `{ok,sessionId,accepted,userMessageId,seq}`（异步，随后发 `session.stream` 通知）|
| `session.cancel` | `{sid}` | `{ok,...}` |
| `session.viewed` | `{sid}` | `{...}` |
| `session.restore` | `{sid}` | `{ok,restored,...}` |
| `session.suggest` | `{sid}` | `{...}` |
| `conv_folders.get` | — | `{folders, assignments}` |
| `conv_folders.set` | `{folders:[{id,name,locked?,sort_order?}], assignments?:{sid:fid}}` | `{ok,folders,assignments}` |
| `model_profiles.list` | — | `{profiles}` |
| `model_profiles.add` | `{data}` | `{ok,...}` |
| `model_profiles.get` | `{id}` | `{profile}` |
| `model_profiles.update` | `{id,data}` | `{ok,...}` |
| `model_profiles.delete` | `{id}` | `{ok,...}` |

`session.snapshot` 结构（来自 `manager.snapshot`）：含 `id,title,status,created_at,updated_at,
folder_id,partial?,...`，其中 `folder_id` 为蛇形（服务端真相源），与前端 camelCase `folderId` 区分。

## 3. 通知（kernel→gateway）

| method | params | 触发 |
|---|---|---|
| `session.stream` | `{sid, state, status, seq, updatedAt, title, partial?}` | 内核调用 `emit_session_state`（running/done/cancelled 等）；`partial` 为当前实时预览（token 文本在 `partial.content`）|

> 说明：原 bridge 的 WS 只推送 `session-state` 状态帧，token 文本经前端轮询 `session.get` 的 `partial` 获取。
> 内核把 `WsHub.emit` 重定向为此通知，并附带 `partial`，使网关无需额外轮询即可拿到增量文本（Phase 1 直接用）。

## 4. 错误码

| code | 含义 |
|---|---|
| -32700 | Parse error |
| -32600 | Invalid Request |
| -32601 | Method not found |
| -32602 | Invalid params |
| -32000 | 内核业务异常（如 session not found / already running），`message` 取自 aiohttp.HTTPException 的 text 或 str(e) |
