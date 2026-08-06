# GenericAgent-9441e32e 项目记忆

## 2026-08-04 thinking_display 会话级精简（桌面端补缺）
- thinking_display 会话级选项已由 d9c303c 实现（llmcore.py，full/brief/off+chars，/session.thinking_display=brief 可调），实测通过。
- **桌面端 /session 命令被 bridge 拦截的坑**：desktop_bridge.py slash_handler 对 prompt_for=None 的未知命令原返回 400 → 已改为回退 submit_prompt 原文（与 TUI 一致），使 /session.xxx 能到达 agentmain._handle_slash_cmd。该补丁在 repo working tree（未单独提交，等并行会话 llmNo 完成后一起提交）且已同步部署副本。
- 部署三处：repo ↔ ~/Library/Application Support/GenericAgent/app ↔ /Applications/GenericAgent.app/Contents/Resources/runtime/app；llmcore.py 已三处 md5 一致(0837f9bc24)，pyc 已清。重启桌面应用后生效。
- 并行会话活跃开发 desktop_bridge.py（llmNo 会话级模型选择）→ 改该文件前先 git diff 确认无他人 in-flight 改动。
- 2026-08-04 workspace chip "未选择"bug 已根治: v339(commit 782bc66,统一_wsSid=bridgeSessionId优先+gaSetCurrentWorkspace权威写), 已与并行会话 llmNo WIP 合并部署(df0c326a,三处md5一致), index 已 bump v=340, 浏览器回归3步全过。改app.js部署后必须bump index v号(并行会话曾漏bump→缓存不刷新), 详见 ga_desktop_debug_sop。
- 2026-08-04 真实App物理验证通过: App于16:36重启已加载v340, macljqCtrl.GrabWindow('GenericAgent')截图+swift Vision OCR(脚本/tmp/ga_ocr.swift)确认窗口chip显示"GenericAgent"(非未选择), 会话列表workspace标签全部正常。本机rapidocr/Vision-pyobjc均未装, 窗口OCR用 swift CLI + Vision 框架(需ScreenRecording权限, 已有); 辅助功能(AX键鼠)权限=无, 无法在真实App内模拟点击。并行会话llmNo WIP(8be86c43)未提交未部署, 其对话中用户已选"等提交+手动重启"。
- 2026-08-04 收尾核查：desktop_bridge.py repo=deployed 同md5(93b66ea4)=slash回退补丁+并行llmNo已全同步；agentmain.py 另有未提交 thinking_display 健壮性增强(值取首token+非法值校验)——in-flight 未同步部署，部署旧逻辑核心功能仍可用。
- 2026-08-04 brief模式thinking平铺bug已修: llmcore.py SSE解析对thinking/reasoning delta包`<thinking>`标签(td_open状态机,Claude+OAI两路径,front-end app.js零改动,前端L1994折叠+fold-thinking CSS自动生效), 单元测试14/14(Claude8+OAI6), commit 0c792f6, 三处md5=5d1a5049已同步, kernel 17:24重启已加载生效。**教训: 部署三处含/Applications/GenericAgent.app/Contents/Resources/runtime/app(第三处易漏,本次漏过被project_memory提醒补上)**。

## 2026-08-04 全局默认配置 GLOBAL_DEFAULT 控制默认 thinking 输出模式
- mykey.py 顶层新增 `GLOBAL_DEFAULT={'thinking_display':'brief','thinking_display_chars':400}`，作用于所有 backend session；优先级: backend 自身字段 > GLOBAL_DEFAULT > 内置默认 full；/session.xxx 会话级 setattr 最高。
- 实现: llmcore.py resolve_session 构造 session 时 merge GLOBAL_DEFAULT 的 thinking 字段（commit d3bc71c，仅改 llmcore.py+模板 zh/en，all:false 避开并行 WIP）。mykey.py 不入 git（本地配置）。
- ⚠️ GLOBAL_DEFAULT 变量名不能含 api/config/cookie，否则被 load_llm_sessions 误当 backend；改默认输出模式只需改 mykey.py 里 GLOBAL_DEFAULT['thinking_display']，重启生效。

## 2026-08-05 定时任务并入设置弹窗第4个 tab（tasks→settings）
- index.html：settings-nav 加"定时任务"按钮；删独立"服务"导航块；tasks 页（list/history 子 tab）迁入 `settings-panel-tasks`。app.js：`set.tabTasks` i18n、`window.openTasksSettings()` 全局入口、switchSettingsTab 支持 tasks、services-btn 绑定移除；`app.js?v=342`。commit/checkpoint `4114e25`（分支 feat/memory-project-parity，all:false 仅 app.js+index.html）。
- **坑**：settings-nav 按钮**无 id，用 `data-settings-tab` 属性**（aria-labelledby 引用的 settings-tab-* id 悬空但无害）；查 DOM 时别用 getElementById('settings-tab-tasks')。
- 验证链：node --check → 三处部署 md5 一致 → curl served v342 → chrome 实开 14168 运行时验证（4 按钮/panel 切换/历史 84 条/子 tab/openTasksSettings 直达）全绿。
- index.html 混入并行会话 llmNo 两行（brand-name + styles v262），已随部署；styles.css 改动属 llmNo 未纳入本提交。工作笔记：temp/tasks_tab_work_notes.md。
