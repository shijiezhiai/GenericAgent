# GA 首响应延迟排查报告

> 现象：桌面 App 输入问题点发送后，"停止"按钮空转数十秒才出现第一个响应。
> 排查日期：2026-08-03。所有数字均为本机实测，非估算。

## 一、结论速览

延迟的 **91%** 花在一件与本次提问毫无关系的事上：**串行冷启动 8 个 MCP server**。
其中 3 个因缺少环境变量而永远启动不了，却各自硬等满 30 秒超时。

| 项 | 实测 | 说明 |
|---|---|---|
| 首条消息总阻塞 | **99.4s**（热缓存）/ 151.7s（冷缓存首跑） | 每个会话的第一条消息都要付 |
| └ 3 个坏 server 纯超时 | **90.6s** | 产出 0 个工具，纯浪费 |
| └ 5 个正常 server | 8.8s | 实际有用的部分 |
| `_snapshot_cwd` × 2 | 144ms × 2 = 288ms | **每轮**都付（cwd = 仓库根时） |
| 轮询粒度 | ≤500ms | 每轮 |
| 前端 setBusy → 按钮点亮 | 0ms | 无延迟，非问题 |

**关键误区澄清**：前端轮询没有初始延迟、没有退避问题。首轮 poll 是立即发起的，稳态间隔 500ms，退避只在连续 fetch 失败时触发。按钮一亮就开始的等待，全部是真实后端阻塞。

## 二、阻塞链路

```
app.js sendPrompt()
  ├─ setBusy(true)                                    0ms   ← 按钮立刻亮
  ├─ await ensureBridgeSession()                      ~ms
  ├─ await POST /session/{id}/restore                 ← 阻塞点，每次发送都调
  │    └─ restore_handler (async def, 但直接调同步函数)
  │         └─ restore_context() → make_agent()
  │              └─ GenericAgent.__init__()
  │                   └─ collect_mcp_tools()   ★ 串行 for 循环 8 个 server = 99.4s
  ├─ await rpc('session/prompt')                      ~ms（内部起线程，快速返回）
  └─ pollSession()                                    500ms 粒度
```

### 逐个 MCP server 实测（热缓存，第二次跑）

```
  2.25s  tools=24  dev-enegine__playwright
  1.30s  tools= 1  content-create__rss-reader-server
 30.19s  tools= 0  content-create__minimax-mcp                 TimeoutError 超时 30s
 30.17s  tools= 0  content-create__wenyan-mcp                  TimeoutError 超时 30s
  1.76s  tools=24  content-create__playwright
 30.19s  tools= 0  content-create__redbook-search-comment-mcp  TimeoutError 超时 30s
  2.45s  tools=19  content-create__lark-mcp
  1.04s  tools=24  long-running-agent__playwright
────────────────────────────────────────────────
串行总计: 99.36s
```

## 三、根因（4 条，按影响排序）

### R1 — 3 个 MCP server 缺环境变量，启动即挂死 30s（90.6s）

`skills_external/claude-plugins/content-create/.mcp.json` 里：

- `minimax-mcp` 需要 `MINIMAX_API_KEY` / `MINIMAX_API_HOST`
- `wenyan-mcp` 需要 `WECHAT_APP_ID` / `WECHAT_APP_SECRET`
- `redbook-search-comment-mcp` 需登录态

这些占位符 `${...}` 未被赋值，进程起来后不响应 `initialize`，`mcp_client.py:208` 的
`timeout=30` 硬编码只能死等满 30 秒。**三个加起来 90.6 秒，产出 0 个工具。**

### R2 — MCP 启动是串行的，且 playwright 被重复启动 3 次

`plugins/plugin_loader.py:761-782` 是朴素 for 循环，逐个 `start()` + `list_tools()`。

三个插件（dev-enegine / content-create / long-running-agent）各自配了**完全相同**的
`npx @playwright/mcp@latest`，被当作三个独立 server 各起一个进程，返回的 24 个工具一模一样。
浪费 ~5s + 两个多余的浏览器驱动进程。

另外 `@latest` 意味着每次启动都要访问 npm registry 解析版本。

### R3 — `restore_handler` 在 async 里跑同步代码，卡死整个事件循环

```python
# frontends/desktop_bridge.py:3111
async def restore_handler(request):
    sid = request.match_info["sid"]
    return json_ok(manager.restore_context(sid))   # 同步阻塞 99s
```

后果不只是这一个请求慢——aiohttp 单线程事件循环被独占，这 99 秒内 **所有** 其他请求
（poll、session 状态、心跳）全部排队。所以界面是"整体僵住"，而不只是消息不出来。

对照 `submit_prompt`（`desktop_bridge.py:1854`）用 `threading.Thread().start()` 立即返回，
是正确写法。

### R4 — 前端每次发送都串行 await `/restore`，且无超时

```javascript
// app.js:8802
await bridgeFetch(`/session/${encodeURIComponent(sid)}/restore`, { method: 'POST', body: {} });
```

`bridgeFetch`（app.js:964）没有超时控制。agent 已存活时该请求会走
`return {..., "reason": "agent already alive"}` 快速返回，但仍是一次多余的串行 RTT；
agent 不存在时就是完整的 99 秒干等，且前端**没有任何进度提示**。

### R5（附带 bug）— MCP client 每轮被关闭，却永不重启

```python
# agentmain.py:155-222
def run(self):
    while True:
        task = self.task_queue.get()
        try:  ...
        finally:
            self.task_queue.task_done()
            for _c in list(getattr(self, 'mcp_clients', {}).values()):
                try: _c.stop()          # ← 每轮结束都关掉全部 MCP
```

而 `collect_mcp_tools()` 只在 `__init__`（agentmain.py:69-72）调用一次。
`make_agent` 又只在 `sess.agent is None` 时触发。

结论：**花 99 秒启动的 MCP，只有第一轮能用**。第二轮起进程已死，但工具 schema 仍挂在
LLM 的工具列表里，模型若调用会直接失败。这让 R1/R2 的代价更加不划算。

## 四、优化方案

### P0-a｜删掉/修复 3 个坏 server（改配置，0 代码，省 90.6s）

编辑 `skills_external/claude-plugins/content-create/.mcp.json`，移除
`minimax-mcp`、`wenyan-mcp`、`redbook-search-comment-mcp` 三项；
或补齐对应环境变量（真要用的话）。

> 单这一步就能把 99.4s 压到 **8.8s**，收益占全部优化的 91%，且零风险。

### P0-b｜MCP 并行启动 + 缩短超时 + 去重（省 ~6.3s）

`plugins/plugin_loader.py` 的 `collect_mcp_tools()`：

```python
from concurrent.futures import ThreadPoolExecutor

def _boot_one(key, cfg):
    c = McpClient(key, cfg)
    c.start()
    return key, c, (c.list_tools() or [])

# 1) 按 (command, args) 去重，多插件共用同一 server 只起一次
seen = {}
jobs = []
for _p in plugins:
    for _sn, _cfg in (_p.get("mcp") or {}).items():
        sig = (_cfg.get("command"), tuple(_cfg.get("args") or []))
        if sig in seen:
            continue          # 复用已有 client，仅追加 tool_map 别名
        seen[sig] = f"{_p['name']}__{_sn}"
        jobs.append((f"{_p['name']}__{_sn}", _cfg))

# 2) 并行启动，整体墙钟 = 最慢的那个
with ThreadPoolExecutor(max_workers=8) as ex:
    for fut in [ex.submit(_boot_one, k, c) for k, c in jobs]:
        try:
            key, client, tl = fut.result()
            ...
        except Exception as e:
            print(f"[MCP] {e}", file=sys.stderr)   # fail-open 不变
```

同时把 `plugins/mcp_client.py:208` 的握手超时从 30s 降到 **8s**
（`list_tools` 可保持较长）。健康的 server 实测都在 2.5s 内完成。

组合效果：8.8s → **~2.5s**（取最慢的 lark-mcp）。即便将来又混进坏 server，
最坏也只是 8s 而非 30s×N。

### P1-a｜MCP 改为懒加载（推荐的根本解）

最优解是**不在 `GenericAgent.__init__` 里启动任何 MCP 进程**：

- 启动时只读 `.mcp.json` 拿到 server 清单，工具 schema 从磁盘缓存加载
  （首次探测后把 `list_tools` 结果按 `command+args+mtime` 缓存到本地 json）；
- 真正 dispatch 到 `mcp__<plugin>__<server>__<tool>` 时才 `start()` 对应的那一个 server。

绝大多数会话根本不会用到 playwright / lark，没有理由为它们付启动成本。
这条落地后，首响应里的 MCP 开销 → **0**。

### P1-b｜`restore_handler` 移出事件循环

```python
async def restore_handler(request):
    sid = request.match_info["sid"]
    return json_ok(await asyncio.to_thread(manager.restore_context, sid))
```

一行改动，消除"整个界面僵住"。即使 agent 构造仍需时间，poll 和其他请求也能正常响应。

### P1-c｜前端去掉冗余的串行 `/restore`

`restore` 的语义是"agent 不在就重建"，这件事 `run_agent_turn`（desktop_bridge.py:1863）
已经做了（`if sess.agent is None: sess.agent = self.make_agent(sess)`）。
前端 `app.js:8802` 的这次 await 属于重复劳动，建议：

- 直接删除，让 `session/prompt` 自己负责；或
- 改为不 await 的预热（`bridgeFetch(...).catch(()=>{})`），并给 `bridgeFetch` 加 10s 超时。

顺带在 setBusy 后显示"正在准备运行环境…"之类的阶段提示，避免纯空转。

### P2-a｜修复 MCP 生命周期 bug（R5）

`agentmain.py:219-222` 的每轮 `_c.stop()` 与"agent 复用"矛盾。二选一：

- **推荐**：删掉 finally 里的 MCP stop，改到 agent 真正销毁时（会话关闭）再清理；
- 或配合 P1-a 懒加载，让 client 具备"用时自愈重启"能力。

### P2-b｜`_snapshot_cwd` 降本（每轮省 ~250ms）

`desktop_bridge.py:88-127` 目前对每个 ≤256KB 的文件 `read_bytes()` 读全文，
仓库根实测 772 文件 / 5.46MB / 144ms，一轮跑两次（前后快照）。

改法：第一遍快照只记 `(mtime_ns, size)`，执行后 diff 出**变化的文件**再回读文本内容。
产出文件通常只有几个，能省掉 99% 的读盘。

## 五、预期收益

| 阶段 | 首条消息阻塞 | 措施 |
|---|---|---|
| 现状 | **99.4s** | — |
| P0-a | 8.8s | 删 3 个坏 server（改配置） |
| P0-a + P0-b | **~2.5s** | 并行 + 去重 + 超时 8s |
| 再加 P1-a | **~0s** | MCP 懒加载 |

界面观感上，P1-b/P1-c 还会消除"整个 App 僵住、无任何反馈"的体验问题。

## 六、验证方法

```bash
# 1. 逐个测 MCP server 冷启动耗时（复现本报告数据）
#    对每个 .mcp.json 条目做 McpClient(key,cfg).start() + list_tools() 计时

# 2. 改配置后端到端验证：重启内核，新建会话发一条消息，掐表
curl --noproxy '*' -X POST http://127.0.0.1:14168/kernel/restart

# 3. 快速判断是不是 MCP 的锅：临时把 skills_config.json 的
#    plugin_dirs 置为 []，对比首响应耗时
```

> 注意：本机 shell 有 `HTTP_PROXY`，测本地端口必须加 `--noproxy '*'`，
> 否则端口未监听时代理会返回 502，误判为"服务在但报错"。
