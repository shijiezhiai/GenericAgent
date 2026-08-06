# 项目长期记忆 (MEMORY.md)

## Git 协作
- `origin`=lsdefine/GenericAgent（只读）；`myfork`=shijiezhiai/GenericAgent（`git push -u myfork <branch>`）。本地 main 偏离 origin/main，开 PR 前 rebase。勿提交 `.workbuddy/`、`.file_favorites.json`、`mcp_servers.json`。git 走 `git_checkpoint`（main/master 禁 commit；restore 需 `no_confirm=true`）。

## 运行架构
- Tauri `.app` → Rust 网关 `frontends/desktop_gateway`（axum，:14168，**以 path 依赖嵌入主二进制 ga-desktop**，改网关只需重编 ga-desktop）→ 内核 `kernel_server.py`（stdio JSON-RPC，持 DuckDB 独占锁，同时 import desktop_bridge 持有 14169/14170 提供 HTTP 路由，**无独立 bridge 进程**）。协议见 `frontends/kernel_protocol.md`。
- **端口**：14168 网关 / 14169 bridge HTTP / 14170 内核 config server（固定）/ 8900 conductor。dev：25168/25169/25170/29900。
- 改 `desktop_bridge.py` 路由须 `POST /kernel/restart`（14168）重载；前端静态改动只需浏览器重载。网关端点：`/kernel/state`、`/kernel/restart`（内核 down 时 503，前端渲染重启卡片）。
- **桌面端 macOS 用 Overlay 无边框标题栏**（`tauri.conf.json` 窗口 `titleBarStyle:"Overlay"`、`trafficLightPosition` 对齐 28px 顶栏中心），原生标题栏透明、交通灯浮内容上方，应用名已在系统菜单栏。
  - `.tauri-desktop` 类由 `index.html` 按 **`location.hostname`（127.0.0.1/localhost，即网桥/桌面端）** 注入，**不依赖 Tauri JS API**。
  - 交通灯避让：`styles.css` `.tauri-desktop .brand { padding-left:92px; }` 并隐藏 `.brand-name`/`.brand-sub`/`.ga-wordmark`，只留 GA logo。
  - ⚠️ **铁律（2026-08-04 踩坑 4 轮才定位）**：主窗经网桥 `http://127.0.0.1:14168/` 加载，**Tauri 不向这种 HTTP 源 webview 注入 JS API**，即 `window.__TAURI__`/`window.__TAURI_INTERNALS__` 在本 webview 内**根本不存在**。因此**前端绝不可用 `invoke('...')` 控制窗口**（拖拽/最大化/最小化/关闭），`pick_folder`/`notify` 这类 `window.__TAURI__.core.invoke` 也大概率失效（GA 主功能走 HTTP 到 gateway 故不明显）。
  - **双击最大化正确做法**：不用 `-webkit-app-region:drag`（它吞 JS 双击事件且 WKWebView 下不保证触发最大化），而是前端 JS 捕获 `dblclick`/`mousedown` 后 `fetch('/__win/toggle_maximize')` → gateway crate 的 `WIN_CTRL` 全局闭包 → Tauri Rust `.setup()` 里注册的闭包调用 `WebviewWindow::maximize/unmaximize`。拖拽由 macOS 原生 Overlay 标题栏处理。改 conf/加 gateway 路由属二进制级，需重编 ga-desktop + 重部署。
- **内核 stdio 红线**：kernel_server.py 的 stdout=JSON-RPC 管道，裸 print 会断会话；已用 `_StderrProxy` 兜底，勿退化。
- **DuckDB 独占锁**：仅内核可开库；配了 proxy 的 `ga_config._call` 绝不可 `_open_transient()` 自开库。内核崩溃重启窗口 bridge 降级 json 自愈（高危写走 `_degraded_write`+脏标记）。
- 会话/消息/文件夹/项目存 `temp/ga_store.duckdb`；`GA_STORAGE=json` 回滚；`GA_STORAGE_SECONDARY=1`=replica。`partial_state` 表 + `_salvage_interrupted_turns()`（须在 `_load_sessions` 前）防流式中断丢消息。

## 部署（铁律）
- **同步三处+清 pyc**：①源码仓 ②运行态 `~/Library/Application Support/GenericAgent/app/`（gateway static_dir 指向此处）③bundle 源 `/Applications/GenericAgent.app/Contents/Resources/runtime/app/`（App 重启会按 GA_BUILD_ID 克隆 bundle 覆盖 app-support，不同步 bundle 会被还原）。静态路径是 `…/app/frontends/desktop/static/`。
- **路径打错静默成功**（cp+mkdir -p 造空目录）：部署后必须 md5 三处校验 + `pgrep -fl kernel_server.py` 看进程实际路径 + `curl --noproxy '*' http://127.0.0.1:14168/…` 实测生效（勿只信 md5）。
- **Rust 编译环境（必用）**：cc 被 claude-code-switcher 劫持 + SDKROOT 指错。前缀 `env PATH="/Library/Developer/CommandLineTools/usr/bin:/usr/bin:/bin:/usr/sbin:/sbin:$HOME/.cargo/bin" SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk LIBRARY_PATH=/usr/lib cargo build --release --offline`（在 src-tauri 下）。
- **换主二进制**：备份放 /tmp（勿留 Contents/MacOS 内，破坏签名）→ cp 新二进制 → `xattr -cr` → `codesign --force --deep --sign -` → `codesign -v` 验证 → pkill+`rm -f /tmp/com_genericagent_desktop_si.sock`+open 同一命令重启。
- 打包 app 内核无 /opt/homebrew PATH，`ga._run_rg` 已兜底绝对路径。
- **axum 路由陷阱（2026-08-04 实证）**：显式 `.route()` 命中路径但方法未注册 → 直接 405，不走 fallback 代理。迁移路由须把旧 handler 全部 method 迁齐；排查 405 先看响应 `allow` 头来源。

## 用户数据保护
- `USER_DATA_ENTRIES`=temp/sche_tasks/memory/mykey.py/.file_favorites.json，升级走合并模式（勿 remove_dir_all+stash，曾丢会话）。前端 POST 回写防呆：空不覆盖+写前 .bak。

## 其他
- **设置弹窗=5 tab**（2026-08-05 起）：通用/模型/快捷键/定时任务/后台服务（原独立"服务"导航块已移除，定时任务→`settings-panel-tasks`，后台服务→`settings-panel-services`）。settings-nav 按钮**无 id、用 `data-settings-tab` 属性**区分（panel id 才是 `settings-panel-<tab>`）。外部入口 `window.openTasksSettings()` / `window.openServicesSettings('channels'|'status')`；tasks 面板含 list/history 子 tab，services 面板含 `.svc-tab`（消息通道/状态面板）+ `.svc-panel` 子区块。services 迁移 WIP 由并行会话 llmNo 写码（v343），本会话接管验证/部署/提交（a70210d）。
- 目录名纯 ASCII `GenericAgent`。用户 plugin/skill 在 `app-support/ext_plugins/`（持久）。
- 多 `.app` 共用 bundle id → 单实例只显最先启动的；`lsof -i :14168` 排查。
- 调试忌讳：勿手动 exec 二进制做第二实例（BrokenPipeError 假故障）；测本地端口必须 `curl --noproxy '*'`（shell 有 HTTP_PROXY）。
- 会话中断排查：`/kernel/state` generation + `/session/{sid}/messages` 看落库。kill bridge 不杀孤儿 worker，lsof 补杀。
