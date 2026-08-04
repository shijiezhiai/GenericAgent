# 把 Tauri App 做成「完全无外部依赖」的方案

> 调研结论：2026-07-29。基于 `frontends/desktop/src-tauri/src/lib.rs` 现有实现 + 当前 `.app` 实际运行方式盘点。

## 一、现状：当前 `.app` 到底依赖了什么外部东西

真机运行的 `.app`（`~/opt/GenericAgent/GenericAgent.app`）拆开看，外部依赖有四类：

1. **Python 解释器**：`find_python()` 在 bundle 内找不到 `runtime/python` 时，fallback 到系统 `python3`，再被 `resolve_python()` 指向部署目录的 `.venv/bin/python3`。机器上必须有 Python。
2. **项目源码目录**：`find_project_dir()` 向上 8 层找含 `agentmain.py` 的目录（当前命中的是 `~/opt/GenericAgent/`）。`.app` 自己不带源码。
3. **第三方包**：`aiohttp` / `bottle` / `requests` / `beautifulsoup4` / `simple-websocket-server` / `fastapi` / `uvicorn` / `websockets` / `psutil` / `pillow` ……（核心 `pyproject.toml` + `desktop_bridge.py` 实际 import 的并集）。装在 `.venv` 里。
4. **运行时写目录**：`temp/` `sche_tasks/` 会话 json / `conv_folders.json` / logs 都写在 GA_ROOT（= 项目目录）下。

### 关键好消息

`lib.rs` **已经实现了一整套自包含 bundle 机制**，只是构建侧没接上：

- `bundle_python()` 优先找 `Contents/Resources/runtime/python/bin/python3`
- `find_project_dir()` 优先找 `runtime/app/agentmain.py`
- `needs_first_run_prepare()` + `run_offline_prepare()` + `install_macos.sh`：首次运行把 wheels 装进嵌入式 python，写 `.prepared` marker
- `sanitize_bundle_env()`：清掉宿主注入的 `PYTHONHOME/PYTHONPATH/LD_LIBRARY_PATH`，避免污染自带解释器
- `running_inside_app_bundle()`：App Translocation 下拒绝信任旧 `~/.ga_desktop_settings.json`

**所以"无外部依赖"不是从零设计，而是：把缺失的 `runtime/` 资源填进构建管道 + 修一处运行时重定向。**

## 二、什么叫「完全无外部依赖」（目标定义）

用户拿到 `.app`，**双击即跑**，机器上：
- 不需要预装 Python
- 不需要源码目录
- 不需要 `.venv`
- 不写回 `.app` 包内部（包是只读的）

**唯一合理例外**：API Key（`mykey.py`）因安全不能打进包，必须首次运行引导用户配置。这是必要的、可接受的设计选择，不是"依赖"。

## 三、要做的 4 大构建块（管道缺失部分）

### 1. 嵌入式 Python — `runtime/python`
- 用 **`python-build-standalone`**（relocatable，静态链接 OpenSSL/libffi/sqlite 等，不依赖系统 dylib）下载 macOS arm64 版本。
- 解压到 `frontends/desktop/runtime/python`（构建时拷进 `.app/Contents/Resources/runtime/python`）。
- 用 `--no-venv` 把依赖直装进它（见块 3），保证 `.app` 可整体移动（绝对路径问题归零）。
- `lib.rs` 的 `bundle_python()` 已经按 `runtime/python/bin/python3` 找，无需改 Rust。

### 2. 嵌入式源码 — `runtime/app` = GA_ROOT
- 把 GA 仓库根复制到 `runtime/app/`，保持**完整导入结构**：
  - 根层：`agentmain.py` `ga.py` `llmcore.py` `agent_loop.py` `simphtml.py` `TMWebDriver.py` `mykey_template*.py`
  - `frontends/`（含 `desktop_bridge.py` `kernel_server.py` `conductor.py` 等）
  - `plugins/` `assets/` `memory/`（白名单）
- 排除：`.git` `.venv` `mykey.py`（密钥！）`node_modules` `target` `temp/` `sche_tasks/` `ARCHIVED/` `*.pyc` `migrated_from_src`
- 为什么必须整结构带：`desktop_bridge.py` 里 `from frontends import workspace_cmd`、`from plugins import ...`、`import plan_state`、`import desktop_bridge` 都靠 GA_ROOT 的包布局解析。只拷单个文件必崩。
- `lib.rs` 的 `find_project_dir()` 找 `runtime/app/agentmain.py` 即命中 → 设为 GA_ROOT。

### 3. 依赖 wheels — `runtime/wheels` + `install_macos.sh`
- 在干净 venv 里 `pip download` 所有**实际 import** 的包（注意不只 `pyproject.toml`，`desktop_bridge.py` 还用 `fastapi/uvicorn/websockets/psutil`，`ui` extra 含 `pillow` 等）：
  `aiohttp bottle requests beautifulsoup4 simple-websocket-server fastapi uvicorn websockets psutil pillow aiofiles python-dotenv` ……（建议写脚本：扫描全部 `.py` 的 `import` 与 `pyproject` 取并集，避免漏）
- 补全 `install_macos.sh`（lib.rs 已引用它）：首次运行 `python -m pip install --no-venv runtime/wheels/*.whl`，写 `runtime/.prepared`。
- `run_offline_prepare()` 已经会调它并转发 `GAPROGRESS` 进度到 loading 窗口——Rust 侧不用动。

### 4. Tauri 构建配置 — 把 `runtime/` 打进 `.app`
- `tauri.conf.json` 的 `bundle` 节（当前第 38 行，空）加 `resources`：把 `runtime` 拷进 `Contents/Resources/runtime/`。或在 `build.rs`/prebuild 脚本里 cp。
- 确认 `frontendDist: ../static` 已把前端打进包（WebView 加载）。gateway 的 `/` `/app.js` `/styles.css` `/vendor` 路由从 `GatewayConfig.root`（= runtime/app）下 `frontends/desktop/static` 读，结构在即 OK。

## 四、必须改的运行时代码（最易漏的两处）

### 5. 运行时态重定向到用户可写目录（**最关键**）
- 当前 `temp/` `sche_tasks/` 会话 json / `conv_folders.json` / logs 都写在 GA_ROOT 下 = **bundle 内 runtime/app = 只读**。启动后任何写都会失败/崩溃。
- 改：所有「可变数据」根目录从 GA_ROOT 改为 `~/Library/Application Support/GenericAgent/`：
  - 涉及 `desktop_bridge.py` / `kernel_server.py` / `agentmain.py` 里所有 `os.path.join(root,"temp",...)` 或基于 `os.getcwd()` 派生的路径。
  - 「读默认配置/模板」仍从 bundle 读；「写」走 app support。

### 6. 凭证 `mykey.py` 首次配置引导
- `mykey.py` **不打包**。首次启动若 GA_ROOT 与 app support 都没有 `mykey.py`，前端弹配置引导（已有 `export_mykey`/`pick_folder` 命令可复用），把用户填的 key 写到 **app support 目录**（不要写回只读 bundle）。
- 开发态仍从项目目录读（兼容）。

## 五、签名 / 公证 / Sandbox

- 自带 python + 一堆 `.so` 必须 `codesign --deep --force --sign` 整个 `.app`；分发需 notarize。dev 用 `xattr -dr com.apple.quarantine` 绕过。
- **App Sandbox 建议关闭**：GA 需要 spawn 子进程（内核/legacy）+ 监听 localhost（14168/14169/8900）+ CDP 连 localhost Chrome，sandbox 会全挡。独立分发（非 Mac App Store 上架）关掉即可；要上架 MAS 再谈精细 entitlements。

## 六、不推荐的「彻底去 Python」路线

- **纯 Rust 重写内核**：`ga.py`/`desktop_bridge.py` 数万行 + `aiohttp`/`fastapi` 生态，代价不可接受。
- **PyInstaller/Nuitka 冻结单二进制**：比 `python-build-standalone` 复杂，且 `plugins` 动态发现 / skills 热加载 / `importlib` 动态 import 要写大量 hook；standalone 路线是「真实解释器跑源码」，动态 import 天然 OK。**优先 standalone。**

> 注：浏览器自动化（TMWebDriver）走 CDP 连用户已装的 Chrome，**不需要打包 Chromium**。

## 七、工作量与落地顺序

- **工作量估计**：构建脚本 + 运行时重定向是中大改造，约 **2–4 天**（含签名与真机验证）。
- **推荐顺序**：
  1. 写 `bundle_build.py`：下 standalone python → `pip download` wheels → 复制 `runtime/app`（排除清单）→ 生成 `install_macos.sh`。
  2. `tauri.conf.json` 加 `resources` 拷贝 `runtime/`。
  3. 改运行时态重定向（最大风险点，先改先验证）。
  4. 本地 `tauri build` → 得到自带 runtime 的 `.app` → 在**无 Python 的干净环境**双击验证。
  5. `codesign` +（可选）notarize。
- **真机验证要点**：在一台完全没装 Python 的机器/容器跑，确认：不依赖任何外部解释器、不写 bundle 内部、会话数据落在 `~/Library/Application Support/GenericAgent/`。

## 八、风险清单

- 动态 import 漏打包 → 运行时 `ImportError`：用「import 扫描 ∪ pyproject」缓解，但仍需真机跑全功能验证。
- 体积：`.app` 从 17MB → 约 **100–200MB**（python + wheels），可接受。
- App Translocation：dmg 首次打开挂 `/Volumes` 只读副本，`lib.rs` 已有 `running_inside_app_bundle()` 防护，runtime 路径靠 `current_exe` 向上找 `.app/Contents/Resources/runtime`，不受影响。
- `psutil`/`pillow` 等需预编译 wheel（arm64），在同架构机器 `pip download` 最省事。

---

## 二、实施状态（2026-07-29 已完成 ✅）

原方案预测「必改 2 处代码」偏保守，实际落地多了几处隐蔽坑，均已在 `lib.rs` / `bundle_build.py` 修复并真机验收通过。

### 实际改动清单
1. **`bundle_build.py`**（新建）：下载 python-build-standalone 3.10.20 → `runtime/python`；复制 GA 源码（排除 mykey/.venv/temp/sche_tasks/.pyc/skills_external）→ `runtime/app`；`pip download` 38 个核心 wheel → `runtime/wheels`；生成 `install_macos.sh`。`runtime` 生成在 `src-tauri/runtime`（非 `frontends/desktop/runtime`，否则 Tauri `resources:["../runtime"]` 会落成 `Resources/_up_/runtime`）。
2. **`lib.rs` 运行时改造（Python 端零改动）**：
   - `ensure_writable_runtime()`：bundle 模式把只读 `runtime/{app,python}` 克隆到 `~/Library/Application Support/GenericAgent/{app,python}`（可写）。内核 `ga_root`=app-support/app、python=app-support/python。
   - `find_python` 优先 app-support/python（装了 wheels 的可写副本）；包内 `runtime/python` 仅作克隆源。
   - `prepared_marker` 改到 app-support/.prepared（包内只读 → 否则每次重装 wheels）。
   - `install_macos.sh` 去掉 `touch runtime/.prepared`（包内只读会让 prepare 非零退出），改由 lib.rs 写 app-support/.prepared。
   - `bundle_anchor_dir` 加 App Translocation 识别（卷根无 `*.app` 目录，原 `ext=="app"` 判断失效）。
   - `clone_dir` 先 `create_dir_all(dst.parent())`（否则父目录不存在 cp 失败 → 整条 bundle 链路退化 dev 回退，最隐蔽）。
3. **`tauri.conf.json`**：`bundle.resources:["runtime"]`。

### 真机验收结果（open 启动新 .app）
- `/status` `gaRoot=/Users/.../Application Support/GenericAgent/app`（可写副本，非包内只读、非 ~/opt）。
- 内核进程打开的全是 `app-support/python/lib/python3.10/site-packages/*` → **完全不依赖外部 Python/部署目录/系统解释器**。
- `POST /session/new` 写入 `app-support/app/temp/desktop_sessions.json`（可写验证通过）。
- 反代 `/api/skills /projects /services/panel /model-profiles /conv-folders` 全 200；`.prepared` 存在（wheels 已装）。
- `mykey.py` 落在 app-support（用户数据目录，不进包，符合密钥不打包；新机器首次需引导配置但不阻断启动）。

### 与原方案差异
- 原方案假设「GA_ROOT 只读 → 重定向 temp 到 app-support」，实际改为「整体克隆 runtime/app 到可写副本」，Python 端 50+ 处 `ga_root/temp` 路径逻辑**一行未改**即天然成立（更低风险、不破坏线上部署）。
- 新增「克隆 python + prepare 装进 app-support/python」这一步（原方案只说装 wheels 进包内 python，但包内只读装不了——这是原方案的盲点，已修）。
