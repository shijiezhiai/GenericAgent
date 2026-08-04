# 项目长期记忆 (MEMORY.md)

## Git 协作
- `origin`=lsdefine/GenericAgent（上游只读）；`myfork`=shijiezhiai/GenericAgent（推送 `git push -u myfork <branch>`）。本地 main 与 origin/main 偏离，开 PR 前 rebase。易冲突：agentmain.py、conductor.py、desktop 相关、desktop_bridge.py、stapp.py。
- 勿提交 `.workbuddy/`、`.file_favorites.json`、`mcp_servers.json`。git 操作走 `git_checkpoint`（main/master 禁 commit；restore 需 `no_confirm=true`）。

## 运行架构（当前形态）
- Tauri `.app`（bundle id `com.genericagent.desktop`）→ Rust 网关 `frontends/desktop_gateway`（axum，:14168）→ **并列 spawn 两个兄弟进程**：JSON-RPC 内核 `kernel_server.py`（stdio，持 DuckDB 独占锁）+ legacy bridge `desktop_bridge.py`（:14169，replica 模式）。协议见 `frontends/kernel_protocol.md`。
- **端口约定**：14168 网关 / 14169 bridge / **14170 内核 config server（固定）** / 8900 conductor。dev 实例对应 25168/25169/25170/29900。
- **bridge 生命周期解耦（C，2026-08-01 落地）**：bridge 不再是内核子进程（`_maybe_spawn_legacy_bridge` 已退化为 no-op shim）。内核通过 `_start_config_server()` 把 store 发布在**固定端口 `GA_CONFIG_PORT`**，bridge 以 `GA_STORAGE_SECONDARY=1` 经该端口读写配置。**内核崩溃重启时 bridge 不重启**（generation 不变），新内核 `_reap_port()` 清掉旧持有者后重新绑同一端口，bridge 下次调用自动重连。网关侧 `BridgeSupervisor`（bridge.rs）独立监管 bridge，端点 `GET /bridge/state`、`POST /bridge/restart`。
- **启动时序（勿改）**：网关必须先 `kernel.call("status")` 阻塞等内核 ready（= config server 已发布），**再** spawn bridge。顺序反了 bridge 首批读会降级到过期 legacy json。
- **实测拓扑修正（2026-08-03）**：当前运行构建里 **没有独立 bridge 进程**——`kernel_server.py` 同时持有 14169/14170 并 import `desktop_bridge` 提供 HTTP 路由（`lsof` 实测 14169/14170 均属 kernel_server.py 的 PID，且 `pgrep -fl desktop_bridge.py` 为空）。因此改 `desktop_bridge.py` 的路由后必须 `POST /kernel/restart`（网关 14168）重载内核；只 `POST /bridge/restart` 不会重载 HTTP 路由（仍 405）。新增 `/open-url` 等路由同理。前端静态文件改动只需浏览器重载。
- **内核重启窗口的降级是设计而非 bug**：那 1~2s 内 bridge 报 `Connection refused` 并降级 json。三个高危配置写（skills_config/plugin_configs/mcp_servers）走 `_degraded_write()` 落 json + 写脏标记 `temp/.skillhub_json_wins`，下个连上 DB 的进程 `_bootstrap()` force 重导入，数据不丢；`meta_put/prune` 是扫描缓存丢了会重建。`ga_config._call` 每次都重试 `_rpc`，降级自愈。**`_call` 里配了 proxy 时绝不能 `_open_transient()` 自己开库**——会抢独占锁把内核永久挡在门外。
- **内核 stdio 协议红线**：`kernel_server.py` 的 `sys.stdout` = JSON-RPC 管道，任何裸 `print()` 都会污染→会话中断。修复（已落地）：`import desktop_bridge` 前捕获 `_PROTOCOL_STDOUT=sys.stdout`，把 `sys.stdout` 换成 `_StderrProxy`（裸 print 只写 `temp/kernel_stderr.log`），`_write()` 用 `_PROTOCOL_STDOUT` 写协议。裸 print 永不到 fd1。
- **打包 app 的 PATH 陷阱（关键，2026-07-31）**：Rust 网关拉起内核未注入 PATH，`.app` 内内核继承 launchd 极简 PATH（无 `/opt/homebrew/bin`），`shutil.which('rg')`=None。`ga._run_rg` 已兜底探测绝对路径（含 `~/.cargo/bin/rg`、`/opt/homebrew/opt/ripgrep/bin/rg`）。
- **code_search/file_find 静默退出的真正根因（已修，2026-08-03）**：根因**不是** rg 找不到，而是 `do_code_search`/`do_file_find` 的 3 类错误返回（pattern 缺失、`_run_rg` 返回 None、rg `rc>1`=LLM 给了非法正则/glob）都写 `return StepOutcome("Error: ...")` **漏了 `next_prompt`** → 默认 None → 命中 `agent_loop.py:143` `if not outcome.next_prompt: break` → 会话静默 CURRENT_TASK_DONE。**即使 rg 找到了，bad regex 仍会让 `rc>1` 路径无 next_prompt**（这是"还是退出"的真因）。修复：6 处错误返回统一加 `next_prompt="\n"`；`_run_rg` 兜底候选扩到 cargo/opt 路径。已同步三处 + `POST /kernel/restart`（generation 1→2 验证）。排查此类"工具导致会话默默中断"优先查 `agent_loop.py` 的 `not outcome.next_prompt` 分支与工具的 error-return 是否带 `next_prompt`。
- **同步三处（铁律）**：任何内核/工具改动须同步 源码仓 + 运行态 `~/Library/Application Support/GenericAgent/app/` + 克隆源 `/Applications/GenericAgent.app/Contents/Resources/runtime/app/`，并清 `__pycache__`。改内核须 `pkill` 真重启（Tauri 单实例，`open` 只激活旧窗口）。
- **部署路径打错会静默成功（2026-08-01 踩过，白查两小时）**：`mkdir -p` + `cp` 组合下，目标路径拼错（如 `GenericAnt` 少了 "Age"）不会报错，只会凭空造出一个目录，运行态仍跑旧代码。**部署后必须 md5 三处校验**，并用 `pgrep -fl kernel_server.py` 看进程命令行里的**实际绝对路径**，而不是相信自己写的路径。
- bridge 由网关 spawn 时 stdout 重定向 `temp/legacy_bridge.log`；所有 subprocess 须 `stdin=subprocess.DEVNULL`。
- **内核 watchdog（2026-08-01 落地）**：网关侧 `KernelSupervisor`（kernel.rs）监控内核退出并带退避自动 respawn（`HEALTHY_UPTIME=60s` 重置计数，MAX 8 次），内核类错误返回 **503** 而非 500；新端点 `GET /kernel/state`、`POST /kernel/restart`。内核侧 `_reap_stale_bridge()` 在 spawn 前回收占住 :14169 的孤儿 bridge（内核被 SIGKILL 时 atexit 不跑）。前端 `shApi/shRenderError` 把 502/503/超时渲染成「内核已断开 + 重启内核」卡片，不再无限「加载中」。
- **Rust 编译环境（必用）**：`/opt/homebrew/bin/cc` 被 npm 包 `claude-code-switcher` 劫持，且 `SDKROOT` 指向不存在的 Xcode.app。编译一律加前缀：`env PATH="/usr/bin:$PATH" SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk LIBRARY_PATH=/usr/lib cargo build --release --offline`。**若把 PATH 收窄成 `/usr/bin:/bin` 则 cargo 自己也丢了**（它在 `~/.cargo/bin`），完整版：`PATH="/Library/Developer/CommandLineTools/usr/bin:/usr/bin:/bin:/usr/sbin:/sbin:$HOME/.cargo/bin"`。换 `.app` 二进制后要 `xattr -cr` + `codesign --force --deep --sign -`，**备份文件不能留在 Contents/MacOS/ 内**（破坏签名）。
- **gateway 是嵌入主二进制的库（重要）**：`src-tauri/Cargo.toml` 以 `ga-desktop-gateway = { path = "../../desktop_gateway" }` path 依赖引入；`spawn_gateway` 用 `tauri::async_runtime::spawn(ga_desktop_gateway::serve(cfg))` 在主进程内跑。所以**改 `desktop_gateway` 的 Rust 代码只需重编 `ga-desktop`**（cd src-tauri && cargo build --release），没有独立 gateway 二进制要替换。运行态唯一要换的主二进制：`/Applications/GenericAgent.app/Contents/MacOS/ga-desktop`。
- **替换主二进制踩坑（2026-08-02 实证）**：Contents/MacOS 内若残留旧 `ga-desktop.bak-*`（历史替换遗留），`codesign --force --deep --sign -` 仍会报 `valid on disk`（脏签名，把无关文件签进 bundle）。替换前务必先 `ls Contents/MacOS | grep bak` 清空，再 xattr -cr + codesign。
- **调试忌讳**：别手动 exec `Contents/MacOS/ga-desktop` 做第二实例——`spawn_gateway` 在 `tauri::Builder` 之前跑，会先起一个内核再被 single-instance 判退，内核报 `BrokenPipeError` 是假故障。清场需 `pkill` + `rm -f /tmp/com_genericagent_desktop_si.sock`，且杀与启写在同一条命令里。

## 自包含 bundle
- `bundle_build.py` 产 `src-tauri/runtime/{python, app, wheels, install_macos.sh}`；构建期装 wheels 并写 `.prepared`，首启只克隆不跑 pip。运行时 lib.rs 把 `runtime/{app,python}` 克隆到 app-support。分发方首启 `xattr -dr com.apple.quarantine`。
- 多副本陷阱：多个 `.app` 共用 bundle id → 单实例只显示最先启动的；排查 `find` 全机副本 + `lsof -i :14168`。

## 用户数据保护
- `USER_DATA_ENTRIES`=temp/sche_tasks/memory/mykey.py/.file_favorites.json，升级克隆走合并模式（dst 已有用户数据保留）。勿用 remove_dir_all+stash 旧方案（曾丢会话）。
- 空覆盖事故：前端无条件 POST 回写服务端文件会覆盖非空数据，已在 `post_token_history_handler` 加防呆（空不覆盖+写前 `.bak`）。

## 开发/发布流程
- 只在源码仓改；热改需同步三处+清 pyc。`open /path/GenericAgent.app` 启动（沙箱 exec 二进制无 WindowServer 会退出）。**测本地端口必须 `curl --noproxy '*'`**（shell 有 HTTP_PROXY→502 假象）。
- **同步三处真实路径（2026-08-02 校正）**：静态文件运行态是 `…/app/frontends/desktop/static/`，**不是** `…/app/static/`。三处：①`~/Library/Application Support/GenericAgent/app/frontends/desktop/static`（运行实例读取处）②`/Applications/GenericAgent.app/Contents/Resources/runtime/app/frontends/desktop/static`（分发包源）③`/Users/mayangyang/code/ai/GenericAgent/frontends/desktop/src-tauri/runtime/app/frontends/desktop/static`（dev 构建源）。⚠️ 路径打错会静默成功（cp 造出空目录），务必 md5 三处校验 + `pgrep -fl kernel_server.py` 看进程实际路径。
- **App 重启会触发克隆覆盖 app-support（2026-08-03 实测）**：`ga-desktop` 启动 `ensure_writable_runtime()` 仅在 `GA_BUILD_ID` 变化/缺失时把 bundle `runtime/app` 克隆到 `~/Library/Application Support/GenericAgent/app`，gateway 的 `static_dir` 指向 **app-support 副本**（非 bundle、非源码仓）。若只同步 app-support 而 bundle 是旧版，用户重启 App 即被还原成旧版。→ **静态资源改动必须同时同步 bundle 源**（三处含 `/Applications/.../runtime/app/...`）；gateway 每次实时读文件系统，改完用 `curl --noproxy '*' http://127.0.0.1:14168/` 验证是否生效，勿只信文件 md5。
- 桌面 App 用 `frontends/desktop/static/{index.html,app.js,styles.css}`（非 conductor.html）；图标 `gaHydrateIcons()` 尺寸 CSS 写元素自身 class。会话文件夹 `temp/conv_folders.json`（API camelCase `folderId`，磁盘 snake `folder_id`）。

## 会话存储 DuckDB（2026-07-30）
- 会话/消息/文件夹/项目存 `temp/ga_store.duckdb`（仅 kernel 可开，独占锁）；`token_history` 仍走 JSON。env `GA_STORAGE=json` 回滚；`GA_STORAGE_SECONDARY=1`→replica（无状态）。`app-support/python` 与 bundle `runtime/python` 都需装 duckdb。

## 目录名 / 外部资源
- 目录名纯 ASCII `GenericAgent`（非 GenericAI/Generic Agent）。仓库 `/Users/mayangyang/code/ai/GenericAgent`；运行态 `~/Library/Application Support/GenericAgent/app`；克隆源 `/Applications/GenericAgent.app/Contents/Resources/runtime/app`。
- 用户 plugin/skill 放 `~/Library/Application Support/GenericAgent/ext_plugins/`（与 `app/` 同级，持久不被升级覆盖）；配置 `app-support/skills_config.json`。bundle 靠 env `GA_BUILD_ID` 判定。

## 杂项
- 历史 trace 找回：`grep -rl "关键词" ~/.workbuddy/traces/`。kill bridge 不杀孤儿 worker（wechatapp/fsapp），须 lsof 补杀；venv 重定向后清 .pyc；macOS 无 setsid，常驻用 run_in_background。

## 会话中断 salvag（2026-08-04）
- DB 表 `partial_state` 存流式 partial 节流快照（2s）；内核启动 `_salvage_interrupted_turns()`（须在 `_load_sessions` 前）把残留 partial 转 `stopped+interrupted` assistant 消息并 bump msg_seq；done/cancel/error/delete 清快照。前端对 `interrupted` 显示中断提示。改 db_store/desktop_bridge 后须同步三处+清 pyc+`POST /kernel/restart`。
