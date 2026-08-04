# DuckDB 会话存储：当前实现 vs 计划完全体 — Gap 与分阶段实现规划

> 依据：`frontends/db_store.py`、`frontends/desktop_bridge.py`、`assets/migrate_to_duckdb.py`、`.workbuddy/memory/2026-07-31.md`（生产切换日志）。
> 日期：2026-07-31

## 一、计划"完全体"的定义
把桌面端文件级会话存储（O(N) 全量重写 `desktop_sessions.json`）彻底替换为 DuckDB 单写者存储，并超越原能力：核心会话数据全在 DB + token_history 也入库 + 启动不读全量 + 运行期自维护（压实/备份）+ 退役 legacy 桥（消除 replica workaround）+ 所有前端统一到同一存储。

## 二、已经做完（不是 gap，已代码验证）
- 6 张表 + 索引：`sessions/messages/conv_folders/projects/raw_logs/log_session_map`；含 `idx_messages_sid_seq`、`idx_sessions_updated`、`idx_sessions_folder`、`idx_rawlogs_logid/sid`、`idx_logmap_sid`（db_store.py:145-150，已验证有索引）。
- 三存储模式 `json/duckdb/replica` + 自动探测（Finder 启动无 `GA_STORAGE` → 探测 DB 文件存在即 duckdb）。
- replica 模式解决 DuckDB 独占锁：kernel spawn legacy 桥时注入 `GA_STORAGE_SECONDARY=1`，该进程不连 DB、不落盘。
- `raw_logs` 实时镜像：`add_message` 路径调用 `_mirror_raw_log`（desktop_bridge.py:1764）。
- 一次性迁移 + 行数校验 + VACUUM 压实(255→135MB) + 幂等守卫 + 回滚预案。
- replica 下 workspace 路由经 `_gateway_call` 回网关（修复 404 回归）。

## 三、Gap 清单（证据驱动）
| # | Gap | 证据 | 完全体目标 |
|---|---|---|---|
| G1 | token_history 仍在 JSON | `db_store.py` 无 token_history；`/token-history` 走 legacy 桥 fallback | token_history 入库，用量页脱离 legacy 桥 |
| G2 | `_load_sessions` 全量读进内存 | 日志明确"启动成本未改善" | 懒加载：列表轻量、消息按需 |
| G3 | 运行期无 VACUUM/压实 | VACUUM 仅 migrate 时一次 | 定期/在线压实，抑制 DB 膨胀 |
| G4 | legacy 桥未退役 | `/token-history` 非网关显式路由；`GA_LEGACY_PORT=14169` | 全部路由迁网关/内核，删 replica 模式 |
| G5 | duckdb wheel 随包分发未验证 | 日志"打包链路尚未验证" | bundle_build+tauri build 真把 duckdb 烤进包、首启免 pip |
| G6 | 其它前端未统一 | qtapp/stapp/tuiapp 各有 `class AgentSession`，未引用 AgentManager/DBStore | 所有前端经网关单写者或统一 DBStore |
| G7 | 无 schema 版本管理 | 无 schema_version 表 | 表结构演进有迁移路径 |
| G8 | 备份/维护靠人工 | `.bak` 保留一周靠手动；无自动 VACUUM/旋转 | 自动压实 + 备份旋转 |

## 四、分阶段实现规划（依赖/风险排序，每阶段含验证与回滚）

### P0 地基（降低后续风险，零/低危）
- **P0a (G5) 验证打包分发**：在干净 clone 跑 `bundle_build.py` + `tauri build`，确认 duckdb wheel 进 `runtime/python` 且首启无需 `pip install`；否则把 duckdb 写入 wheels 清单并在 bake 期预装。验证：新包首启 `import duckdb` 成功、无 `ModuleNotFoundError`。回滚：旧包回退。
- **P0b (G7) schema 版本**：加 `schema_version` 表 + `ensure_schema` 版本分支（未来 ALTER 走迁移函数）。零风险。

### P1 内部性能（安全，纯 AgentManager 改造）
- **G2 懒加载**：`_load_sessions` 只载会话元数据（标题/updated_at/folder_id/workspace），消息表按需（按 sid 分页）加载；网关/bridge 的 `get_messages` 走 `DBStore._get_messages`。验证：65 会话启动耗时与内存下降；消息翻页正确。回滚：`GA_STORAGE=json` 或还原 `desktop_bridge.py`。

### P2 运行期自维护（运维）
- **G3+G8 定期压实 + 备份旋转**：`DBStore.compact()`（在线：ATTACH 新库 `COPY FROM DATABASE` 替换，或 CHECKPOINT+重建）；由 kernel 定时（每天/空闲）或退出时触发；备份目录按日保留 N 份。验证：运行数日 DB 体积稳定；备份目录符合预期。回滚：无（只增不改写路径）。

### P3 token_history 入库（G1）
- 先给网关/内核加显式 `/token-history`（GET/PUT）路由，由 kernel 直接读/写 DB（新增 `token_history` 表）；用量页改走该路由 → 脱离 legacy 桥。再做一次性回填（读 `desktop_token_history.json` → 写 DB，保留 `.bak` + 防呆）。最后写入也走 DB。验证：用量页 200 且数据一致；空覆盖防呆仍在。回滚：路由开关切回 legacy 桥。

### P4 退役 legacy 桥（G4，最高风险）
- 逐个 grep handler，把其余 fallback 反代路由迁为网关/内核显式路由；验证 `/session/{sid}/workspace` 等已走显式路由后，关闭 `GA_LEGACY_PORT`（默认 14169），删除 replica 模式与 `_gateway_call` 回环。验证：全功能冒烟（会话/消息/文件夹/用量/workspace）；`legacy_bridge.log` 不再有 `session not found`。回滚：恢复 `GA_LEGACY_PORT` + replica 代码（保留分支）。

### P5 统一其它前端（G6）
- 推荐：qtapp/stapp/tuiapp/CLI/SDK 改为经网关（HTTP/JSON-RPC）访问会话，由 kernel 单写者落 DB（避免多进程抢 DuckDB 独占锁）。若必须直连 DB，则把 `DBStore` 抽为公共库，且每前端独立 DB 文件或统一经 gateway。验证：各前端会话读写一致；无 DB 锁冲突。回滚：各前端保留原 JSON 存储分支。

### P6 收尾
- 删除 `.bak`（保留期满）、写运维文档（压实/备份/回滚）、加 DB 增长监控。

## 五、建议起点
- 低风险高价值先做 **P0a（打包验证，防重建丢包）+ P1（懒加载，直接改善启动）**。
- **P4 风险最高**、收益是消除架构债，建议放在 P3 之后、P5 之前。

## 六、假设
- "完全体"以桌面端 duckdb 迁移计划为基准；G6 是延伸（原计划 root cause 仅在 `desktop_bridge`）。若目标是"全前端统一"，P5 权重上升。
