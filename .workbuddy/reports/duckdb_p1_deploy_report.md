# DuckDB 会话存储优化 — P0a/P0b/P1 部署报告

> 状态：**已上线验证通过**（2026-07-31）
> 仓库：`/Users/mayangyang/code/ai/Generic Agent`（注意：目录名无空格）

## 本次完成的三块改造

### P0a — duckdb 纳入自包含 bundle（修 G5）
- `bundle_build.py` 支持 `--only bake`；`wheels/` 现含 `duckdb-1.5.5-cp310-macosx_arm64.whl`（共 39 个）。
- bundle python 已 `pip install --no-index` 预装并写 `.prepared`，未来重打 bundle 不再丢失 duckdb。

### P0b — schema 版本化
- `db_store.py` 新增 `SCHEMA_VERSION=1` + `schema_version` 表 + 幂等 `_migrate_schema()`（未来 ALTER 分支入口）。

### P1 — 会话消息惰性加载 + 服务端 rounds（核心收益）
- `Session.messages` 改为 property + `_LAZY_MESSAGES` 哨兵：首次访问才从 DB 拉，列表阶段完全不物化。
- `db_store.load_all_sessions(with_messages=False)` 返回 `msg_count`；新增 `get_messages/count_messages/load_message_digests/rounds_map`。
- `kernel_server.rpc_session_list` 改用 `rounds_map()`，列表只带 `rounds` 不带 `messages`。
- `app.js`：未水合会话回退服务端 `rounds`；点击 round 先水合再渲染。

## 部署

| 项 | 值 |
|---|---|
| 部署文件 | db_store.py / desktop_bridge.py / kernel_server.py / desktop/static/app.js |
| 运行态 | `~/Library/Application Support/Generic Agent/app` |
| 克隆源 | `/Applications/Generic Agent.app/Contents/Resources/runtime/app` |
| md5 校验 | 仓库 / 运行态 / 克隆源 **三方一致** |
| 重启 | `pkill ga-desktop` → 补杀 kernel/bridge → `open .app` |
| 新内核 PID | 16617（持有 duckdb 独占锁） |
| 回滚备份(pre-P1) | `~/opt/ga_p1_backup2_20260731-103449` |

> 注：上一会话的 `ga_p1_backup_20260731-102232` 因 cp 循环引号错误把文件名当目录写入，内容损坏，已删除。

## 上线验证结果

| 检查 | 结果 |
|---|---|
| `GET /sessions` 会话数 | 65 |
| `GET /sessions` 体积 | **108 KB**（旧 109.6 MB，≈719× 缩减） |
| 列表含 `messages` | 否（惰性加载生效） |
| 列表含服务端 `rounds` | 65/65（侧栏展开箭头可用） |
| `GET /session/{sid}/messages` | 200，单会话 20 条正常返回（点击 round 水合可用） |
| 存储模式 | duckdb（DB 自检测） |
| 消息丢失 | 无（`_persist` 对惰性会话跳过 `_replace_messages`） |

## 后续阶段（未启动）
P2–P6：G3 运行时 VACUUM、G4 退役 legacy bridge、G1 token_history 入 DB、G6 统一其余前端、G8 备份轮转、P6 清理。
