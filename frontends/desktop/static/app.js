// GenericAgent 桌面版 —— bridge 适配 + 业务 UI（HTTP 命令 / WS 状态 / i18n）。
// 文案全部走 i18n：静态用 data-i18n / data-i18n-ph / data-i18n-title，
// 动态用 t(key)。dev 标注层与发给 agent 的预设 prompt 不进 UI 字典。
'use strict';

/* ═══════════════ 端口/URL 常量 ═══════════════
   bridge / conductor 的端口和 origin 都集中在这里。要换端口、要切同源、
   要让 bridge 代理 conductor —— 改这一块即可,下面所有 URL 引用全都跟着走。
   *_ORIGIN 不带尾巴 path,调用方自己拼 "/sessions" "/ws" 等。 */
const BRIDGE_PORT = 14168;
const CONDUCTOR_PORT = 8900;
const BRIDGE_ORIGIN = `${location.protocol}//${location.hostname}:${BRIDGE_PORT}`;
const BRIDGE_WS_ORIGIN = `${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.hostname}:${BRIDGE_PORT}`;
const CONDUCTOR_ORIGIN = `${location.protocol}//${location.hostname}:${CONDUCTOR_PORT}`;
const CONDUCTOR_WS_ORIGIN = `${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.hostname}:${CONDUCTOR_PORT}`;

/* ═══════════════ 进程状态 store ═══════════════ */
const _serviceById = {};
const _serviceListeners = new Set();

function _serviceList() {
  return Object.values(_serviceById).sort((a, b) => String(a.id).localeCompare(String(b.id)));
}

function _serviceNotify() {
  const items = _serviceList();
  for (const cb of _serviceListeners) {
    try { cb(items, _serviceById); } catch (e) { console.error('[service-store]', e); }
  }
}

const gaServiceStore = {
  applySnapshot(services) {
    for (const k of Object.keys(_serviceById)) delete _serviceById[k];
    for (const s of services || []) {
      if (s && s.id) _serviceById[s.id] = s;
    }
    _serviceNotify();
  },
  applyChanged(service) {
    if (service && service.id) _serviceById[service.id] = service;
    _serviceNotify();
  },
  onServices(cb) {
    _serviceListeners.add(cb);
    cb(_serviceList(), _serviceById);
    return () => _serviceListeners.delete(cb);
  },
  list: _serviceList,
  get: (id) => _serviceById[id],
};

let bridgeUiOffline = false;

/* ═══════════════ Bridge 适配（HTTP 命令 + WS 状态） ═══════════════ */
(function initGaBridge() {
  const listeners = new Map();
  let ws = null;
  let cachedBridgeReady = null;
  let wsRetries = 0;
  let wsRetryTimer = null;
  const bridgeBase = BRIDGE_ORIGIN;
  const wsUrl = `${BRIDGE_WS_ORIGIN}/ws`;

  function on(channel, cb) {
    if (typeof cb !== 'function') return () => {};
    if (!listeners.has(channel)) listeners.set(channel, new Set());
    listeners.get(channel).add(cb);
    if (channel === 'bridge-ready' && cachedBridgeReady) {
      try { cb(cachedBridgeReady); } catch (err) { console.error('[ga bridge] replay bridge-ready', err); }
    }
    return () => listeners.get(channel)?.delete(cb);
  }

  function emit(channel, payload) {
    if (channel === 'bridge-ready') cachedBridgeReady = payload;
    const set = listeners.get(channel);
    if (!set) return;
    for (const cb of Array.from(set)) {
      try { cb(payload); } catch (err) { console.error('[ga bridge]', channel, err); }
    }
  }

  function handleServiceWs(msg) {
    if (msg.type === 'services.snapshot') gaServiceStore.applySnapshot(msg.services);
    else if (msg.type === 'service.changed') gaServiceStore.applyChanged(msg.service);
    emit('service-state', msg);
  }

  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
  const tauriInvoke = (name, args = {}) => {
    const invoke = window.__TAURI__?.core?.invoke;
    if (!invoke) throw new Error('Tauri IPC is not available');
    return invoke(name, args);
  };

  async function http(path, options = {}) {
    const headers = Object.assign({}, options.headers || {});
    const init = Object.assign({}, options, { headers });
    if (init.body && typeof init.body !== 'string') {
      headers['Content-Type'] = headers['Content-Type'] || 'application/json';
      init.body = JSON.stringify(init.body);
    }
    const res = await fetch(`${bridgeBase}${path}`, init);
    const text = await res.text();
    let data = null;
    try { data = text ? JSON.parse(text) : {}; } catch (_) { data = { raw: text }; }
    if (!res.ok) {
      const err = new Error((data && (data.error || data.message)) || `${res.status} ${res.statusText}`);
      err.status = res.status;
      err.data = data;
      throw err;
    }
    return data;
  }

  async function waitBridgeStatus(timeoutMs = 20000) {
    const deadline = Date.now() + timeoutMs;
    let lastErr = null;
    while (Date.now() < deadline) {
      try {
        const status = await http('/status');
        wsRetries = 0;
        connectWs();
        return status;
      } catch (err) {
        lastErr = err;
        await sleep(350);
      }
    }
    throw lastErr || new Error('Bridge did not become ready');
  }

  function connectWs() {
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;
    if (wsRetryTimer) { clearTimeout(wsRetryTimer); wsRetryTimer = null; }
    try {
      ws = new WebSocket(wsUrl);
      ws.addEventListener('open', () => { wsRetries = 0; emit('bridge-log', 'WS connected'); });
      ws.addEventListener('message', (ev) => {
        let msg;
        try { msg = JSON.parse(ev.data); } catch (_) { return; }
        if (msg.type === 'bridge-ready') emit('bridge-ready', msg);
        else if (msg.type === 'services.snapshot' || msg.type === 'service.changed') handleServiceWs(msg);
        else if (msg.type === 'session-state') emit('bridge-notification', msg);
        else if (msg.type === 'bridge-log') emit('bridge-log', msg.payload || msg);
        else if (msg.type === 'bridge-error') emit('bridge-error', msg.payload || msg);
      });
      ws.addEventListener('close', () => { emit('bridge-closed', { reason: 'ws-closed' }); scheduleWsReconnect(); });
      ws.addEventListener('error', () => emit('bridge-error', { type: 'ws-error', message: 'WebSocket error' }));
    } catch (err) {
      emit('bridge-error', { type: 'ws-error', message: err.message || String(err) });
      scheduleWsReconnect();
    }
  }

  /* WS 自动重连(指数退避,封顶 30s)。手机浏览器后台会被 OS 掐 WS,
     不重连的话回到前台还是死连接。`visibilitychange` 那一段是回前台立刻重连。 */
  function scheduleWsReconnect() {
    if (wsRetryTimer) clearTimeout(wsRetryTimer);
    if (typeof document !== 'undefined' && document.hidden) return; // 后台等回前台再连
    const delay = Math.min(30000, 1000 * Math.pow(2, wsRetries));
    wsRetries++;
    wsRetryTimer = setTimeout(() => { wsRetryTimer = null; connectWs(); }, delay);
  }
  if (typeof document !== 'undefined') {
    document.addEventListener('visibilitychange', () => {
      if (!document.hidden && (!ws || ws.readyState >= WebSocket.CLOSING)) {
        wsRetries = 0; connectWs();
      }
    });
  }

  async function rpc(method, params = {}) {
    switch (method) {
      case 'app/status': return http('/status');
      case 'app/config/get': return http('/config');
      case 'app/config/save': return http('/config', { method: 'POST', body: params || {} });
      case 'get/model-profiles': return http('/model-profiles');
      case 'session/new': return http('/session/new', { method: 'POST', body: params || {} });
      case 'projects/list': return http('/projects');
      case 'projects/create': {
        if (!params || !params.name) throw new Error('projects/create missing name');
        return http('/projects', { method: 'POST', body: params });
      }
      case 'projects/skills_get': {
        if (!params || !params.name) throw new Error('projects/skills_get missing name');
        return http(`/projects/${encodeURIComponent(params.name)}/skills`);
      }
      case 'projects/skills_update': {
        if (!params || !params.name) throw new Error('projects/skills_update missing name');
        return http(`/projects/${encodeURIComponent(params.name)}/skills`, { method: 'PUT', body: params });
      }
      case 'projects/workspace_update': {
        if (!params || !params.name) throw new Error('projects/workspace_update missing name');
        return http(`/projects/${encodeURIComponent(params.name)}/workspace`, { method: 'PUT', body: params });
      }
      case 'projects/rename': {
        if (!params || !params.name) throw new Error('projects/rename missing name');
        if (!params.newName) throw new Error('projects/rename missing newName');
        return http(`/projects/${encodeURIComponent(params.name)}/rename`, { method: 'PUT', body: { newName: params.newName } });
      }
      case 'projects/delete': {
        if (!params || !params.name) throw new Error('projects/delete missing name');
        return http(`/projects/${encodeURIComponent(params.name)}`, { method: 'DELETE', body: {} });
      }
      case 'session/prompt': {
        const sid = params.sessionId || params.id || params.bridgeSessionId;
        if (!sid) throw new Error('session/prompt missing sessionId');
        return http(`/session/${encodeURIComponent(sid)}/prompt`, { method: 'POST', body: params || {} });
      }
      case 'session/poll': {
        const sid = params.sessionId || params.id || params.bridgeSessionId;
        if (!sid) throw new Error('session/poll missing sessionId');
        const after = params.afterId ?? params.after ?? 0;
        const limit = params.limit ?? 200;
        return http(`/session/${encodeURIComponent(sid)}/messages?after=${encodeURIComponent(after)}&limit=${encodeURIComponent(limit)}`);
      }
      case 'session/cancel': {
        const sid = params.sessionId || params.id || params.bridgeSessionId;
        if (!sid) throw new Error('session/cancel missing sessionId');
        return http(`/session/${encodeURIComponent(sid)}/cancel`, { method: 'POST', body: params || {} });
      }
      case 'app/path/open': return http('/path/open', { method: 'POST', body: params || {} });
      case 'services/start': {
        const id = params.id;
        if (!id) throw new Error('services/start missing id');
        return http('/services/start', { method: 'POST', body: { id } });
      }
      case 'services/stop': {
        const id = params.id;
        if (!id) throw new Error('services/stop missing id');
        return http('/services/stop', { method: 'POST', body: { id } });
      }
      case 'services/logs': {
        const id = params.id;
        if (!id) throw new Error('services/logs missing id');
        const tail = params.tail ?? 200;
        return http(`/services/logs?id=${encodeURIComponent(id)}&tail=${encodeURIComponent(tail)}`);
      }
      case 'services/panel': return http('/services/panel');
      case 'services/bridge/exit': return http('/services/bridge/exit', { method: 'POST' });
      case 'services/mykey/get': return http('/services/mykey');
      case 'services/mykey/save': return http('/services/mykey', { method: 'POST', body: params || {} });
      case 'app/path/selectGaRoot': return http('/config');
      case 'list_continuable_sessions': return { sessions: [] };
      case 'restore_session': throw new Error('restore_session is not implemented in web2 bridge');
      case 'workspace/list': return http('/workspaces');
      case 'workspace/prepare': return http('/workspace/prepare', { method: 'POST', body: params || {} });
      case 'workspace/remove': {
        const wname = params.name;
        if (!wname) throw new Error('workspace/remove missing name');
        return http(`/workspace/${encodeURIComponent(wname)}`, { method: 'DELETE' });
      }
      case 'session/workspace/get': {
        const wsid = params.sessionId;
        if (!wsid) throw new Error('session/workspace/get missing sessionId');
        return http(`/session/${encodeURIComponent(wsid)}/workspace`);
      }
      case 'session/workspace/set': {
        const wsid2 = params.sessionId;
        if (!wsid2) throw new Error('session/workspace/set missing sessionId');
        return http(`/session/${encodeURIComponent(wsid2)}/workspace`, { method: 'POST', body: params });
      }
      case 'session/workspace/off': {
        const wsid3 = params.sessionId;
        if (!wsid3) throw new Error('session/workspace/off missing sessionId');
        return http(`/session/${encodeURIComponent(wsid3)}/workspace/off`, { method: 'POST' });
      }
      default: throw new Error(`Unknown RPC method: ${method}`);
    }
  }

  async function startService(id) {
    try {
      const res = await rpc('services/start', { id });
      if (res.service) gaServiceStore.applyChanged(res.service);
      return res;
    } catch (e) {
      if (e.data && e.data.service) gaServiceStore.applyChanged(e.data.service);
      throw e;
    }
  }

  async function stopService(id) {
    const res = await rpc('services/stop', { id });
    if (res.service) gaServiceStore.applyChanged(res.service);
    return res;
  }

  async function spawnBridge() {
    connectWs();
    try {
      const status = await http('/status');
      bridgeUiOffline = false;
      return status;
    } catch (_) {
      await tauriInvoke('start_bridge');
      const status = await waitBridgeStatus();
      bridgeUiOffline = false;
      return status;
    }
  }

  async function exitBridge() {
    const res = await rpc('services/bridge/exit');
    cachedBridgeReady = null;
    if (ws) {
      try { ws.close(); } catch (_) {}
    }
    return res;
  }

  window.ga = {
    platform: navigator.platform.toLowerCase().includes('mac') ? 'darwin' : 'win32',
    startBridge: async () => { connectWs(); return http('/status'); },
    spawnBridge,
    stopBridge: async () => ({ ok: true }),
    exitBridge,
    checkStatus: () => rpc('app/status', {}),
    getConfig: () => rpc('app/config/get', {}),
    saveConfig: (cfg) => rpc('app/config/save', cfg || {}),
    getModelProfiles: () => rpc('get/model-profiles', {}),
    selectGaRoot: () => rpc('app/path/selectGaRoot', {}),
    openMykeyTemplate: () => rpc('app/path/open', { kind: 'mykeyTemplate' }),
    openMykey: () => rpc('app/path/open', { kind: 'mykey' }),
    startService,
    stopService,
    getServiceLogs: (id, tail = 200) => rpc('services/logs', { id, tail }),
    getServicePanel: () => rpc('services/panel', {}),
    getMykeyContent: () => rpc('services/mykey/get', {}),
    saveMykeyContent: (content) => rpc('services/mykey/save', { content }),
    tauriInvoke,
    setBridgeUiOffline: (offline) => { bridgeUiOffline = !!offline; },
    pollSession: (sessionId, afterId = 0) => rpc('session/poll', { sessionId, afterId }),
    rpc,
    onBridgeMessage: (cb) => on('bridge-message', cb),
    onBridgeNotification: (cb) => on('bridge-notification', cb),
    onBridgeError: (cb) => on('bridge-error', cb),
    onBridgeClosed: (cb) => on('bridge-closed', cb),
    onBridgeReady: (cb) => on('bridge-ready', cb),
    onBridgeLog: (cb) => on('bridge-log', cb),
    onServiceState: (cb) => on('service-state', cb),
    onOpenSearch: (cb) => on('open-search', cb),
    // Workspace helpers
    listWorkspaces: () => rpc('workspace/list'),
    prepareWorkspace: (path) => rpc('workspace/prepare', { path }),
    removeWorkspace: (name) => rpc('workspace/remove', { name }),
    getSessionWorkspace: (sessionId) => rpc('session/workspace/get', { sessionId }),
    setSessionWorkspace: (sessionId, name) => rpc('session/workspace/set', { sessionId, name }),
    offSessionWorkspace: (sessionId) => rpc('session/workspace/off', { sessionId }),
  };

  connectWs();
  http('/status').then(status => emit('bridge-ready', status))
    .catch(err => emit('bridge-error', { type: 'http-error', message: err.message || String(err) }));
})();

/* ═══════════════ i18n ═══════════════ */
const I18N = {
  zh: {
    'app.title': 'GenericAgent 桌面版',
    'brand.sub': '桌面终端',
    'nav.chat': '聊天', 'nav.services': '后台服务', 'nav.channels': '消息通道', 'nav.status': '状态面板',
    'nav.collab': '指挥家', 'nav.token': '用量', 'nav.tasks': '定时任务',
    'foot.settings': '配置', 'foot.ver': 'GenericAgent · 桌面版',
    'chat.startTitle': '开始对话', 'chat.startSub': '直接输入，或点预设功能一键启动',
    'nav.project': '项目', 'project.startTitle': '项目空间', 'project.startSub': '选择或创建项目，开启专注会话', 'project.placeholder': '在项目中输入…',
    'project.empty': '还没有项目', 'project.hasMemory': '记忆', 'project.noMemory': '无记忆', 'project.loadErr': '加载项目失败', 'project.enterErr': '进入项目失败', 'project.createErr': '创建项目失败', 'project.enterBtn': '进入', 'project.memLines': '行',
    'project.title': '项目', 'project.subtitle': '多人协同打造超级团队', 'project.newBtn': '新建项目', 'project.myProjects': '我的项目', 'project.searchPh': '搜索项目', 'project.fromTemplate': '从模版创建', 'project.menuTitle': '更多操作', 'project.promptName': '请输入项目名称', 'project.addedAgo': '添加于 {0} 前', 'project.justNow': '刚刚', 'project.minAgo': '{0} 分钟前', 'project.hourAgo': '{0} 小时前', 'project.dayAgo': '{0} 天前', 'project.monAgo': '{0} 个月前', 'project.yearAgo': '{0} 年前',
    'project.create': '新建项目', 'project.name': '项目名称', 'project.template': '选择模板', 'project.tplBlank': '不使用模板', 'project.instruction': '指令', 'project.instructionPh': '输入项目背景、规范或系统提示词…', 'project.instructionHint': '可选。作为项目指令写入 CLAUDE.md，进入项目后自动生效。', 'project.createBtn': '创建', 'project.nameRequired': '请输入项目名称', 'project.tplOverwrite': '切换模板将覆盖当前指令内容，是否继续？', 'project.skills': '技能', 'project.skillsHint': '可选。选择该项目启用的 Skills，未勾选的不会被注入。留空则启用全部。', 'project.skillsLoading': '加载中…', 'project.skillsNone': '未发现可用技能', 'project.editSkills': '编辑技能', 'project.skillsSaved': '技能已保存，新会话生效', 'project.rename': '重命名', 'project.renamePrompt': '输入新的项目名称', 'project.renameErr': '重命名失败', 'project.delete': '删除项目', 'project.deleteConfirm': '确定要删除项目「{0}」吗？此操作不可恢复。', 'project.deleteErr': '删除失败', 'project.nameInvalid': '名称不能包含 / \\ 或以 . 开头', 'project.workspace': '工作区', 'project.wsNone': '无绑定', 'project.wsExisting': '使用已有工作区', 'project.wsNew': '新建工作区', 'project.wsPathPh': '输入新工作区的完整路径…', 'project.wsHint': '新建工作区时会自动注册到工作区列表。', 'project.wsSelectErr': '请选择一个已有工作区', 'project.wsPathErr': '请输入新工作区路径', 'project.wsSaved': '工作区已更新', 'project.wsCurrent': '当前工作区',
    'project.tpl.req.t': '产品需求全流程', 'project.tpl.req.d': '从需求采集到评审的完整流程', 'project.tpl.research.t': '市场调研与竞品分析', 'project.tpl.research.d': '行业趋势、用户洞察与竞品对比', 'project.tpl.kb.t': '团队知识库', 'project.tpl.kb.d': '沉淀团队经验与协作规范', 'project.tpl.delivery.t': '项目交付', 'project.tpl.delivery.d': '里程碑、交付物与验收管理', 'project.tpl.bug.t': 'Bug 跟踪测试验收', 'project.tpl.bug.d': '缺陷记录、复现与回归验证',
    'ph.invite': '邀请', 'ph.tab.feed': '动态', 'ph.tab.plan': '计划', 'ph.tab.task': '任务', 'ph.tab.asset': '资产', 'ph.filter.mine': '与我相关', 'ph.filter.member': '成员动态', 'ph.empty.feed': '暂无与我有关的动态', 'ph.empty.plan': '暂无计划', 'ph.empty.task': '暂无任务', 'ph.empty.asset': '暂无资产', 'ph.composer.ph': '输入消息…', 'ph.tool.craft': 'Craft', 'ph.tool.auto': '自动', 'ph.tool.skill': '技能', 'ph.tool.connector': '连接器', 'ph.config.title': '项目配置', 'ph.cfg.instruction': '指令', 'ph.cfg.instruction.d': '设定项目背景与规范', 'ph.cfg.connector': '连接器', 'ph.cfg.connector.d': '连接外部服务', 'ph.cfg.expert': '专家', 'ph.cfg.expert.d': '为项目配置专家角色', 'ph.cfg.skill': '技能', 'ph.cfg.skill.d': '配置项目技能', 'ph.cfg.automation': '自动化', 'ph.cfg.automation.d': '让 AI 按计划自动执行', 'ph.plan.newTodo': '新建待办', 'ph.plan.addSource': '添加数据源', 'ph.plan.filter.ownership': '全部归属', 'ph.plan.filter.source': '全部来源', 'ph.plan.batch': '批量操作', 'ph.plan.searchPh': '搜索计划', 'ph.plan.col.todo': '待开始', 'ph.plan.col.doing': '进行中', 'ph.plan.col.pause': '暂停', 'ph.plan.col.done': '完成', 'ph.plan.empty': '暂无事项', 'ph.plan.empty.todo': '暂无事项，可从这里开始新建。',
    'preset.butler.t': '指挥家', 'preset.butler.d': '复杂任务自动拆解，只需查看进度和简报',
    'preset.plan.t': 'Plan 模式', 'preset.plan.d': '加载 Plan SOP，按探索→规划→执行→验证流程',
    'preset.goal.t': 'Goal 模式', 'preset.goal.d': '设定目标，自主完成',
    'preset.autonomous.t': '自主行动', 'preset.autonomous.d': '按 SOP 规划/执行任务,产出报告(reflect/autonomous.py 同源)',
    'preset.hive.t': 'Hive 协作', 'preset.hive.d': '多 worker 协同攻坚',
    'preset.review.t': '深度复核', 'preset.review.d': '挑刺式质量把关',
    'preset.findwork.t': '找点事做', 'preset.findwork.d': '分析当前情况,推荐一批让你感兴趣的 TODO',
    'preset.mine.t': '我的·周报', 'preset.mine.d': '自定义：抓本周提交并写周报',
    'preset.add.t': '自定义', 'preset.add.d': '任意一句话存为功能',
    'composer.placeholder': 'GA 能帮你做些什么？',
    'workspace.selectTitle': '工作区', 'workspace.empty': '未选择',
    'workspace.panelTitle': '工作区', 'workspace.current': '当前',
    'workspace.offTitle': '解除绑定', 'workspace.add': '添加工作区',
    'workspace.emptyList': '暂无工作区，点击下方添加',
    'workspace.addTitle': '输入项目根目录的绝对路径',
    'workspace.addPlaceholder': '/path/to/your/project',
    'workspace.addConfirm': '添加', 'workspace.addCancel': '取消',
    'workspace.removeTitle': '删除', 'workspace.switchTitle': '切换到该工作区',
    'workspace.dangling': '(已失效)',
    'workspace.switched': '工作区已切换',
    'search.placeholder': '搜索会话…', 'conv.new': '新对话',
    'ctx.pin': '置顶', 'ctx.unpin': '取消置顶', 'ctx.rename': '重命名', 'ctx.del': '删除', 'ctx.batchDel': '批量删除', 'common.selectAll': '全选', 'conv.batchSelected': '已选', 'conv.batchDelConfirm': '确定删除选中的 {n} 个会话？',
    'common.close': '关闭', 'common.more': '更多', 'common.optional': '选填', 'common.save': '保存',
    'modal.preset': '预设功能', 'modal.addModel': '添加模型', 'modal.editModel': '编辑模型', 'modal.settings': '配置',
    'modal.customPreset': '自定义预设',
    'modal.editCustomPreset': '编辑任务',
    'customPreset.titlePh': '标题，例如「写周报」',
    'customPreset.promptPh': 'Prompt 内容，发送时会作为消息提交',
    'customPreset.empty': '标题和 Prompt 不能为空',
    'customPreset.removeTitle': '删除',
    'customPreset.editTitle': '编辑',
    'builtinPreset.restoreBtn': '恢复默认预设',
    'set.appearance': '外观', 'set.plainUi': '素色', 'set.fontSize': '聊天字号', 'set.lang': '语言', 'set.model': '模型', 'set.addModel': '添加模型', 'set.features': '功能', 'set.chatFilesDir': '对话文件目录', 'set.save': '保存', 'set.importMykey': '导入已有模型配置（mykey.py）', 'set.exportMykey': '导出当前模型配置', 'set.serviceManager': '后台服务管理',
    'shortcut.askConfirm': '是否在桌面创建 GenericAgent 快捷方式？',
    'appearance.light': '浅色', 'appearance.dark': '深色',
    'set.noModels': '暂无模型，点击下方添加',
    'lang.zh': '简体中文', 'lang.en': 'English',
    'model.name': '备注', 'model.namePh': '会显示在模型列表',
    'model.apikey': 'API Key', 'model.apikeyPh': 'sk-...', 'model.apikeyKeep': '留空则保持原 Key 不变',
    'model.apibase': 'API 地址', 'model.apibasePh': 'https://.../v1/messages',
    'model.protocol': '协议', 'model.protocolPick': '请选择…', 'model.protocolOai': 'OpenAI 兼容 (chat/completions)', 'model.protocolClaude': 'Anthropic (Claude /v1/messages)',
    'model.stream': '响应方式', 'model.streamOn': '流式', 'model.streamOff': '非流式',
    'model.model': '模型', 'model.modelPh': 'model 参数名',
    'model.modelHint': '须与中转站/官方文档中的 model 字段完全一致',
    'model.retries': '重试 (次)', 'model.connTimeout': '连接超时 (s)', 'model.readTimeout': '读取超时 (s)',
    'model.save': '保存', 'common.cancel': '取消', 'common.confirm': '确认', 'common.edit': '编辑', 'common.delete': '删除',
    'pq.title': '快速接入官方模型', 'pq.sub': '填好 API Key 即可使用', 'pq.toggle': '展开 / 收起',
    'pq.deepseekDesc': '官方 API · OpenAI 兼容', 'pq.qwenDesc': '通义千问 · 阿里云百炼',
    'guide.step1': '点击下方链接，登录后创建并复制 API Key',
    'guide.step2': '把 Key 粘贴到下方「API Key」输入框',
    'guide.step3': '点击保存，即可在模型列表中选用',
    'guide.prefillTip': '已为你预填 API 地址、协议与模型，可按需修改',
    'guide.getKey': '获取 {name} 的 API Key', 'guide.copy': '复制链接', 'guide.copied': '链接已复制',
    'err.modelSave': '保存失败', 'err.modelRequired': '请填写模型、API Key 和 API 地址',
    'err.modelDelete': '删除失败', 'err.modelDeleteLast': '至少保留一个模型',
    'confirm.modelDelete': '确定删除该模型配置？',
    'model.aggregation': '渠道组（自动故障转移）', 'model.aggregationShort': '渠道组', 'model.aggregationDesc': '按顺序尝试，失败自动切换到下一个',
    'model.emptyMixin': '尚未加入模型',
    'model.addToMixin': '加入渠道组', 'model.inMixin': '已在渠道组', 'model.removeFromMixin': '移出渠道组', 'model.alreadyInMixin': '已在渠道组中', 'model.dragReorder': '拖拽调整顺序',
    'err.mixinFailed': '操作失败',
    'page.services.title': '后台服务', 'page.services.sub': 'IM 消息通道与后台进程，集中查看、启停与日志',
    'page.channels.title': '消息通道', 'page.channels.sub': '后台 IM 进程：列表、启停与日志（同 hub.pyw）',
    'page.status.title': '状态面板', 'page.status.sub': 'hub.pyw 管理的后台进程/服务，集中查看与启停',
    'page.collab.title': '指挥家', 'page.collab.sub': '交代目标，自动拆活与跟进',
    'collab.progressTitle': '分工进度',
    'collab.progressEmpty': '还没有任务在执行。告诉指挥家你的目标后，这里会显示拆分后的处理进度。',
    'collab.placeholder': '请对指挥家描述你想完成的目标',
    'collab.guideTitle': '把要完成的事告诉指挥家',
    'collab.guideWhen': '适合需要多步处理、要花一些时间才能完成的目标。日常聊天和快问快答，请用左侧「聊天」。',
    'collab.guideStep1t': '描述目标',
    'collab.guideStep1d': '在聊天框里写下你想做的事，发给指挥家',
    'collab.guideStep2t': '自动拆解',
    'collab.guideStep2d': '指挥家自动拆解、分配任务，实时监督和调度',
    'collab.guideStep3t': '交付摘要',
    'collab.guideStep3d': '指挥家根据执行状态，呈上任务简报',
    'collab.guideStep4t': '随时调整',
    'collab.guideStep4d': '随时补充要求或细节，指挥家都会处理',
    'collab.chipProgress': '现在进展如何？',
    'collab.chipPause': '先暂停当前任务',
    'collab.chipSummary': '总结一下目前的结果',
    'collab.showProgressTitle': '查看分工进度',
    'collab.statRunning': '进行中',
    'collab.statDone': '已完成',
    'collab.plusMenu': '更多操作',
    'collab.switchMode': '切换模式',
    'collab.typing': '指挥家正在处理',
    'collab.offline': '无法连接指挥家服务，请确认后端已启动。',
    'collab.retry': '重试',
    'collab.reconnect': '连接断开，正在重连… 已保留上次任务进度。',
    'collab.reconnectIn': '{n} 秒后重试',
    'collab.stRunning': '执行中', 'collab.stReported': '已回报', 'collab.stPaused': '已暂停',
    'collab.stFailed': '遇到问题', 'collab.stTerminated': '已终止',
    'collab.summaryRunning': '正在处理中…', 'collab.summaryWait': '等待回报',
    'collab.taskFallback': '任务 {n}',
    'collab.timeJust': '刚刚',
    'collab.timeSec': '{n} 秒前',
    'collab.timeMin': '{n} 分钟前',
    'collab.timeHr': '{n} 小时前',
    'collab.timeDay': '{n} 天前',
    'page.token.title': '用量', 'page.token.sub': '每会话与累计用量及缓存率',
    'status.connecting': '正在连接…', 'status.ready': '服务在线', 'status.running': '处理中',
    'status.disconnected': '服务离线', 'status.stopped': '已停止', 'status.idle': '待命',
    'conv.emptyList': '暂无会话，点「＋ 新对话」开始', 'conv.defaultTitle': '新对话',
    'conv.folderDefault': '未归类', 'conv.folderArchived': 'Archived', 'conv.folderPrompt': '输入文件夹名称', 'conv.folderExists': '文件夹已存在', 'conv.folderEmpty': '空文件夹', 'conv.folderDeleteConfirm': '删除文件夹“{0}”？其中会话将移回未归类', 'conv.folderLocked': '系统文件夹不可修改', 'conv.moveDone': '已移动到 {0}',
    'ctx.moveToFolder': '移动到文件夹', 'ctx.moveArchived': '移到 Archived',
    'err.bridge': '服务未响应', 'err.newSession': '新建会话失败', 'err.poll': '轮询失败', 'err.stop': '停止失败',
    'err.interruptTimeout': '等待上一轮停止超时，请稍后再试',
    'sys.interruptPrev.hint': '已停止上一轮，正在处理新消息',
    'chat.interrupting': '正在停止上一轮…',
    'chat.sessionLoading': '正在加载会话…',
    'sys.stopRequested': '已请求停止',
    'slash.help': '可用命令：\n/new 新会话  /clear 清屏  /stop 停止  /settings 设置',
    'slash.unknown': '未知命令',
    'upload.hint': '上传文件：选择 / 拖拽 / 粘贴',
    'upload.button': '上传文件',
    'upload.tooLarge': '文件过大或数量超限', 'upload.empty': '跳过空文件',
    'upload.failed': '上传失败',
    'err.charLimit': '已达字数上限（{n}），发送时将自动截断', 'err.charLimitReached': '已达字数上限（{n}）', 'err.numMax': '不能超过 {n}',
    'file.openFailed': '无法打开文件',
    'file.kindGeneric': '文件',
    'file.kindDoc': '文档',
    'file.kindSheet': '表格',
    'file.kindSlide': '幻灯片',
    'file.kindCode': '代码',
    'file.kindArchive': '压缩包',
    'file.kindAudio': '音频',
    'file.kindVideo': '视频',
    'upload.removeTitle': '移除',
    'upload.dropHint': '松开以上传文件',
    'lightbox.closeTitle': '关闭',
    'fold.thinking': '思考', 'fold.tool': '工具调用', 'fold.toolResult': '工具结果', 'fold.llm': 'LLM Running', 'fold.turn': '第 {n} 轮',
    'plan.header': '计划 ({done}/{total})', 'plan.complete': '✓ 计划完成 ({n}/{n})',
    'plan.running': '计划执行中', 'plan.completeTitle': '计划完成',
    'plan.placeholder': '计划模式已激活', 'plan.waiting': '等待写入 {path} …', 'plan.overflow': '还有 {n} 项',
    'plan.current': '当前', 'plan.collapse': '收起', 'plan.expand': '展开', 'plan.details': '详情',
    'plan.capsuleRunning': '运行中', 'plan.capsuleComplete': '已完成',
    'timing.elapsed': '已运行 {t}',
    'model.auto': '自动选择',
    'model.menuLabel': '选择模型',
    'chip.plan': 'Plan',
    'chip.auto': 'Auto',
    'ch.wechat': '微信', 'ch.wecom': '企业微信', 'ch.lark': '飞书', 'ch.dingtalk': '钉钉',
    'ch.qq': 'QQ', 'ch.telegram': 'Telegram', 'ch.discord': 'Discord',
    'ch.loading': '加载中…', 'ch.empty': '未发现 IM 进程脚本',
    'ch.logEmpty': '暂无日志',
    'err.channelLoad': '加载失败', 'err.channelStart': '启动失败', 'err.channelStop': '停止失败',
    'err.mykeyImport': '导入模型配置失败',
    'err.mykeyExport': '导出模型配置失败',
    'err.channelNotConfigured': '请先在 mykey.py 中配置该平台',
    'sys.channelStarted': '已启动', 'sys.channelStopped': '已停止',
    'modal.channelLogs': '进程日志',
    'modal.mykeyConfig': 'mykey.py 配置',
    'modal.qrLogin': '{name} 扫码登录',
    'qr.scanTip': '请使用 {name} 扫描下方二维码完成登录',
    'qr.statusScanning': '等待扫码...',
    'qr.statusScanned': '已扫码，请确认...',
    'qr.statusConfirmed': '登录成功！',
    'qr.statusExpired': '二维码已过期，请刷新重试',
    'qr.failed': '获取二维码失败，请稍后重试',
    'qr.refresh': '刷新二维码',
    'sys.configSaved': '配置已保存',
    'sys.mykeyImported': '模型配置已导入',
    'sys.mykeyExported': '模型配置已导出',
    'st.starting': '启动中…', 'st.stopping': '停止中…', 'st.online': '在线', 'st.offline': '离线', 'st.error': '错误', 'st.running': '运行', 'st.abnormal': '异常',
    'act.configure': '配置', 'act.logs': '日志', 'act.restart': '重启', 'act.stop': '停止', 'act.start': '启动', 'act.exit': '退出',
    'act.copy': '复制', 'act.copied': '已复制', 'act.copyTex': 'TeX', 'act.send': '发送',
    'proc.imbotWechat': 'imbot · 微信', 'proc.imbotDing': 'imbot · 钉钉', 'proc.scheduler': '定时任务调度', 'proc.conductor': '指挥家',
    'cm.scheduling': '调度中', 'cm.running': '执行中', 'cm.idleSt': '空闲',
    'cm.master': '已派 3 子任务', 'cm.w1': '子任务：抓取数据', 'cm.w2': '子任务：复核结果', 'cm.sub': '等待派单',
    'tok.total': '累计', 'tok.cost': '缓存率', 'tok.today': '今日', 'tok.tabAll': '聊天', 'tok.tabConductor': '指挥家', 'tok.condTotal': '指挥家累计', 'tok.condCurrent': '指挥家本次', 'tok.condTip': '指挥家消耗不计入聊天累计', 'tok.condOffline': '指挥家服务离线', 'tok.disclaimer': '不同 API 网站的计费价格可能会有差异，请以实际网站为准。',
    'tok.colSession': '会话', 'tok.colIn': '输入', 'tok.colOut': '输出', 'tok.colCacheW': '缓存写入', 'tok.colCache': '缓存读取', 'tok.colCost': '成本',
    'tok.from': '从', 'tok.to': '到', 'tok.reset': '重置', 'tok.noData': '暂无记录', 'tok.deleted': '此会话已删除',
    'tok.pricingUnknown': '⚠ 此模型计费规则尚未明确，按默认估算',
    'page.tasks.title': '定时任务', 'page.tasks.list': '任务列表', 'page.tasks.history': '历史任务',
    'page.tasks.emptyTitle': '选择下方对话，开启你的第一个任务吧',
    'page.tasks.emptySub': '',
    'page.tasks.create': '创建任务', 'page.tasks.historyEmpty': '暂无历史任务',
    'modal.createTask': '创建定时任务', 'task.name': '任务名称', 'task.prompt': '任务指令',
    'task.repeat': '重复', 'task.schedule': '执行时间', 'task.rp.daily': '每天', 'task.rp.weekday': '工作日',
    'task.rp.weekly': '每周', 'task.rp.monthly': '每月', 'task.rp.once': '仅一次',
    'task.rp.hourly': '每小时', 'task.rp.every4h': '每4小时', 'task.rp.every12h': '每12小时',
    'taskForm.name': '任务名称', 'taskForm.namePh': '给任务起个名字', 'taskForm.prompt': '任务指令',
    'taskForm.promptPh': '输入要让AI执行的任务指令', 'taskForm.repeat': '重复', 'taskForm.schedule': '执行时间',
    'taskForm.repeatDaily': '每天', 'taskForm.repeatWeekday': '工作日', 'taskForm.repeatWeekly': '每周',
    'taskForm.repeatMonthly': '每月', 'taskForm.repeatOnce': '仅一次', 'taskForm.repeat1h': '每1小时',
    'taskForm.repeat3h': '每3小时', 'taskForm.repeat6h': '每6小时', 'taskForm.repeat1d': '每1天',
    'taskForm.advanced': '高级设置', 'taskForm.maxDelay': '最大延迟(小时)', 'taskForm.maxDelayHint': '超过此时间未执行则跳过',
    'taskForm.enabled': '启用任务', 'taskForm.saved': '任务已创建', 'taskForm.fail': '创建失败',
    'common.create': '创建',
    'task.createOk': '任务已创建', 'task.promptRequired': '请输入任务指令', 'task.createFail': '创建失败',
    'tok.priceInput': '输入: $', 'tok.priceOutput': '输出: $',
    'tok.priceCacheW': '缓存写入: $', 'tok.priceCacheR': '缓存读取: $',
    'presetPrompt.goal': '进入 Goal 模式：读 L3 goal mode SOP，自主达成我接下来描述的目标。',
    'presetPrompt.plan': '进入 Plan 模式：先读 memory/plan_sop.md，按其中「探索→规划→执行→验证」流程，等我接下来描述要做的任务。',
    'presetPrompt.autonomous': '🤖 进入自主行动模式：阅读 memory/autonomous_operation_sop.md，按 SOP 选取或规划任务,独立执行并产出报告。',
    'presetPrompt.hive': '启动 Goal Hive 模式：按 hive SOP 拉起多个 worker 协同完成我接下来的目标。',
    'presetPrompt.review': '进入监察者模式：对刚才的产出严格挑刺、逐项复核并报告问题。',
    'presetPrompt.findwork': '按照自主行动的规划部分，充分分析我的情况，给我生成一批 TODO，务必让我感兴趣。',
    'presetPrompt.mine': '抓取本周的 git 提交并写一份周报。',
    'ask.banner': 'GA 等你回答',
    'ask.replyHint': '在下方输入框回复',
    'ask.placeholderOpen': '在此输入你的回答… (Enter 发送)',
    'nav.files': '文件', 'page.files.title': '文件管理', 'files.filterTitle': '按类型筛选',
    'files.all': '全部', 'files.source.chat': '对话上传', 'files.source.task': '定时任务', 'files.source.config': '配置', 'files.source.generated': '生成文件',
    'files.referencedBy': '被引用', 'files.download': '下载', 'files.delete': '删除',
    'files.confirmDelete': '确定删除该文件？', 'files.empty': '暂无文件',
    'files.sizeB': 'B', 'files.sizeKB': 'KB', 'files.sizeMB': 'MB',
    'files.viewList': '列表视图', 'files.viewGrid': '网格视图', 'files.refresh': '刷新',
    'files.menu': '更多操作', 'files.open': '打开文件', 'files.preview': '预览文件', 'files.openLocation': '打开文件位置', 'files.copy': '复制文件', 'files.sort.name': '名称', 'files.sort.size': '大小', 'files.sort.mtime': '修改时间', 'files.sort.created': '创建时间', 'files.sortAsc': '升序', 'files.sortDesc': '降序',
    'files.confirmDelete': '确定删除该文件？', 'files.confirmDeleteMulti': '确定删除选中的 {n} 个文件？',
    'files.confirmCopy': '确定复制该文件？', 'files.copied': '已复制', 'files.copying': '复制中…',
    'files.selectMode': '选择', 'files.selectAll': '全选', 'files.deselectAll': '取消全选',
    'files.batchOpenLocation': '打开位置', 'files.batchCopy': '复制', 'files.batchDelete': '删除',
    'files.selected': '已选 {n} 项', 'files.cancel': '取消',
    'conv.untitled': '新对话', 'notify.done': '对话已完成',
  },
  en: {
    'app.title': 'GenericAgent Desktop',
    'brand.sub': 'Desktop terminal',
    'nav.chat': 'Chat', 'nav.services': 'Services', 'nav.channels': 'Channels', 'nav.status': 'Status',
    'nav.collab': 'Conductor', 'nav.token': 'Usage', 'nav.tasks': 'Scheduled Tasks',
    'foot.settings': 'Settings', 'foot.ver': 'GenericAgent · Desktop',
    'chat.startTitle': 'Start a conversation', 'chat.startSub': 'Type a message, or pick a preset',
    'nav.project': 'Project', 'project.startTitle': 'Project Space', 'project.startSub': 'Select or create a project to start a focused session', 'project.placeholder': 'Type in project…',
    'project.empty': 'No projects yet', 'project.hasMemory': 'Memory', 'project.noMemory': 'No memory', 'project.loadErr': 'Failed to load projects', 'project.enterErr': 'Failed to enter project', 'project.createErr': 'Failed to create project', 'project.enterBtn': 'Enter', 'project.memLines': 'lines',
    'project.title': 'Project', 'project.subtitle': 'Collaborate to build a super team', 'project.newBtn': 'New Project', 'project.myProjects': 'My Projects', 'project.searchPh': 'Search projects', 'project.fromTemplate': 'From Template', 'project.menuTitle': 'More', 'project.promptName': 'Enter project name', 'project.addedAgo': 'Added {0} ago', 'project.justNow': 'just now', 'project.minAgo': '{0} min ago', 'project.hourAgo': '{0} hours ago', 'project.dayAgo': '{0} days ago', 'project.monAgo': '{0} months ago', 'project.yearAgo': '{0} years ago',
    'project.create': 'New Project', 'project.name': 'Project Name', 'project.template': 'Select Template', 'project.tplBlank': 'No template', 'project.instruction': 'Instruction', 'project.instructionPh': 'Enter project context, norms or system prompt…', 'project.instructionHint': 'Optional. Written to CLAUDE.md as the project instruction; takes effect automatically after entering the project.', 'project.createBtn': 'Create', 'project.nameRequired': 'Please enter a project name', 'project.tplOverwrite': 'Switching the template will overwrite the current instruction. Continue?', 'project.skills': 'Skills', 'project.skillsHint': 'Optional. Select which Skills are enabled for this project; unchecked ones won\'t be injected. Leave all unchecked to enable all.', 'project.skillsLoading': 'Loading…', 'project.skillsNone': 'No skills found', 'project.editSkills': 'Edit Skills', 'project.skillsSaved': 'Skills saved, takes effect on new session', 'project.rename': 'Rename', 'project.renamePrompt': 'Enter new project name', 'project.renameErr': 'Rename failed', 'project.delete': 'Delete Project', 'project.deleteConfirm': 'Are you sure you want to delete project "{0}"? This cannot be undone.', 'project.deleteErr': 'Delete failed', 'project.nameInvalid': 'Name cannot contain / \\ or start with .', 'project.workspace': 'Workspace', 'project.wsNone': 'None', 'project.wsExisting': 'Use existing workspace', 'project.wsNew': 'Create new workspace', 'project.wsPathPh': 'Enter full path for new workspace…', 'project.wsHint': 'New workspaces are automatically registered to the workspace list.', 'project.wsSelectErr': 'Please select an existing workspace', 'project.wsPathErr': 'Please enter a workspace path', 'project.wsSaved': 'Workspace updated', 'project.wsCurrent': 'Current workspace',
    'project.tpl.req.t': 'Product Requirements', 'project.tpl.req.d': 'Full flow from gathering to review', 'project.tpl.research.t': 'Market & Competitor Research', 'project.tpl.research.d': 'Trends, insights and competitor analysis', 'project.tpl.kb.t': 'Team Knowledge Base', 'project.tpl.kb.d': 'Team experience and collaboration norms', 'project.tpl.delivery.t': 'Project Delivery', 'project.tpl.delivery.d': 'Milestones, deliverables and acceptance', 'project.tpl.bug.t': 'Bug Tracking & QA', 'project.tpl.bug.d': 'Defect logging, repro and regression',
    'preset.butler.t': 'Conductor', 'preset.butler.d': 'Auto-decompose complex tasks; just check progress and briefings',
    'preset.plan.t': 'Plan mode', 'preset.plan.d': 'Load Plan SOP — explore→plan→execute→verify',
    'preset.goal.t': 'Goal mode', 'preset.goal.d': 'Set a goal, run autonomously',
    'preset.autonomous.t': 'Autonomous mode', 'preset.autonomous.d': 'Plan/execute tasks per SOP and produce reports (same as reflect/autonomous.py)',
    'preset.hive.t': 'Hive', 'preset.hive.d': 'Multi-worker collaboration',
    'preset.review.t': 'Deep review', 'preset.review.d': 'Strict quality check',
    'preset.findwork.t': 'Find me work', 'preset.findwork.d': 'Analyze my context and suggest a batch of interesting TODOs',
    'preset.mine.t': 'My · Weekly', 'preset.mine.d': 'Custom: weekly report from commits',
    'preset.add.t': 'Custom', 'preset.add.d': 'Save any prompt as a function',
    'composer.placeholder': 'What can GA do for you?',
    'workspace.selectTitle': 'Workspace', 'workspace.empty': 'None',
    'workspace.panelTitle': 'Workspace', 'workspace.current': 'Current',
    'workspace.offTitle': 'Unbind', 'workspace.add': 'Add Workspace',
    'workspace.emptyList': 'No workspaces yet. Add one below.',
    'workspace.addTitle': 'Enter absolute path to project root',
    'workspace.addPlaceholder': '/path/to/your/project',
    'workspace.addConfirm': 'Add', 'workspace.addCancel': 'Cancel',
    'workspace.removeTitle': 'Remove', 'workspace.switchTitle': 'Switch to this workspace',
    'workspace.dangling': '(unavailable)',
    'workspace.switched': 'Workspace switched',
    'search.placeholder': 'Search chats…', 'conv.new': 'New chat',
    'ctx.pin': 'Pin', 'ctx.unpin': 'Unpin', 'ctx.rename': 'Rename', 'ctx.del': 'Delete', 'ctx.batchDel': 'Batch Delete', 'common.selectAll': 'Select All', 'conv.batchSelected': 'Selected', 'conv.batchDelConfirm': 'Delete {n} selected sessions?',
    'common.close': 'Close', 'common.more': 'More', 'common.optional': 'Optional', 'common.save': 'Save',
    'modal.preset': 'Presets', 'modal.addModel': 'Add model', 'modal.editModel': 'Edit model', 'modal.settings': 'Settings',
    'modal.customPreset': 'Custom preset',
    'modal.editCustomPreset': 'Edit task',
    'customPreset.titlePh': 'Title, e.g. "Weekly report"',
    'customPreset.promptPh': 'Prompt body — sent as the message when clicked',
    'customPreset.empty': 'Title and Prompt cannot be empty',
    'customPreset.removeTitle': 'Delete',
    'customPreset.editTitle': 'Edit',
    'builtinPreset.restoreBtn': 'Restore defaults',
    'set.appearance': 'Appearance', 'set.plainUi': 'Plain', 'set.fontSize': 'Chat font size', 'set.lang': 'Language', 'set.model': 'Model', 'set.addModel': 'Add model', 'set.features': 'Features', 'set.chatFilesDir': 'Chat files dir', 'set.save': 'Save', 'set.importMykey': 'Import model config (mykey.py)', 'set.exportMykey': 'Export current model config', 'set.serviceManager': 'Service manager',
    'shortcut.askConfirm': 'Create a desktop shortcut for GenericAgent?',
    'appearance.light': 'Light', 'appearance.dark': 'Dark',
    'set.noModels': 'No models yet — add one below',
    'lang.zh': '简体中文', 'lang.en': 'English',
    'model.name': 'Note', 'model.namePh': 'Shown in the model list',
    'model.apikey': 'API Key', 'model.apikeyPh': 'sk-...', 'model.apikeyKeep': 'Leave blank to keep the current key',
    'model.apibase': 'API base URL', 'model.apibasePh': 'https://.../v1/messages',
    'model.protocol': 'Protocol', 'model.protocolPick': 'Select…', 'model.protocolOai': 'OpenAI-compatible (chat/completions)', 'model.protocolClaude': 'Anthropic (Claude /v1/messages)',
    'model.stream': 'Response', 'model.streamOn': 'Stream', 'model.streamOff': 'Non-stream',
    'model.model': 'Model', 'model.modelPh': 'model parameter name',
    'model.modelHint': 'Must match the model field in your provider docs exactly',
    'model.retries': 'Retries (×)', 'model.connTimeout': 'Connect (s)', 'model.readTimeout': 'Read (s)',
    'model.save': 'Save', 'common.cancel': 'Cancel', 'common.confirm': 'Confirm', 'common.edit': 'Edit', 'common.delete': 'Delete',
    'pq.title': 'Quick connect a model', 'pq.sub': 'Add your API key to get started', 'pq.toggle': 'Expand / collapse',
    'pq.deepseekDesc': 'Official API · OpenAI-compatible', 'pq.qwenDesc': 'Tongyi Qwen · Aliyun Bailian',
    'guide.step1': 'Open the link, sign in, then create & copy your API key',
    'guide.step2': 'Paste the key into the “API Key” field below',
    'guide.step3': 'Click Save — then pick it from the model list',
    'guide.prefillTip': 'API base, protocol and model are pre-filled — edit if needed',
    'guide.getKey': 'Get your {name} API key', 'guide.copy': 'Copy link', 'guide.copied': 'Link copied',
    'err.modelSave': 'Save failed', 'err.modelRequired': 'Model, API Key and base URL are required',
    'err.modelDelete': 'Delete failed', 'err.modelDeleteLast': 'At least one model is required',
    'confirm.modelDelete': 'Delete this model profile?',
    'model.aggregation': 'Channel group (auto failover)', 'model.aggregationShort': 'Channel group', 'model.aggregationDesc': 'Tries in order, switches to the next on failure',
    'model.emptyMixin': 'No models added yet',
    'model.addToMixin': 'Add to channel', 'model.inMixin': 'In channel', 'model.removeFromMixin': 'Remove from channel', 'model.alreadyInMixin': 'Already in the channel', 'model.dragReorder': 'Drag to reorder',
    'err.mixinFailed': 'Operation failed',
    'page.services.title': 'Services', 'page.services.sub': 'IM channels and background processes — view, start/stop, logs',
    'page.channels.title': 'Channels', 'page.channels.sub': 'Background IM processes: list, start/stop, logs (hub.pyw style)',
    'page.status.title': 'Status', 'page.status.sub': 'Background processes/services managed by hub.pyw',
    'page.collab.title': 'Conductor', 'page.collab.sub': 'Describe a goal — split, delegate, and follow up',
    'collab.progressTitle': 'Progress',
    'collab.progressEmpty': 'No tasks running yet. After you describe a goal to Conductor, split tasks will appear here.',
    'collab.placeholder': 'Describe the goal you want to accomplish',
    'collab.guideTitle': 'Tell Conductor what you want done',
    'collab.guideWhen': 'Best for multi-step goals that take a while. For everyday chat and quick questions, use Chat in the sidebar.',
    'collab.guideStep1t': 'Describe your goal',
    'collab.guideStep1d': 'Write what you want done in the chat box and send it to Conductor',
    'collab.guideStep2t': 'Auto breakdown',
    'collab.guideStep2d': 'Conductor breaks down, assigns, monitors, and coordinates',
    'collab.guideStep3t': 'Summary',
    'collab.guideStep3d': 'Conductor delivers a briefing based on execution status',
    'collab.guideStep4t': 'Adjust anytime',
    'collab.guideStep4d': 'Add requirements or details anytime — Conductor handles them',
    'collab.chipProgress': 'How is it going?',
    'collab.chipPause': 'Pause current tasks',
    'collab.chipSummary': 'Summarize progress so far',
    'collab.showProgressTitle': 'View task progress',
    'collab.statRunning': 'Running',
    'collab.statDone': 'Done',
    'collab.plusMenu': 'More actions',
    'collab.switchMode': 'Switch mode',
    'collab.typing': 'Conductor is working',
    'collab.offline': 'Cannot reach the service. Make sure the backend is running.',
    'collab.retry': 'Retry',
    'collab.reconnect': 'Disconnected — reconnecting… Your last progress is kept.',
    'collab.reconnectIn': 'Retry in {n}s',
    'collab.stRunning': 'Running', 'collab.stReported': 'Reported', 'collab.stPaused': 'Paused',
    'collab.stFailed': 'Issue', 'collab.stTerminated': 'Ended',
    'collab.summaryRunning': 'Working…', 'collab.summaryWait': 'Awaiting report',
    'collab.taskFallback': 'Task {n}',
    'collab.timeJust': 'just now',
    'collab.timeSec': '{n}s ago',
    'collab.timeMin': '{n}m ago',
    'collab.timeHr': '{n}h ago',
    'collab.timeDay': '{n}d ago',
    'page.token.title': 'Usage', 'page.token.sub': 'Per-session and total usage & cache rate',
    'status.connecting': 'Connecting…', 'status.ready': 'Service online', 'status.running': 'Working…',
    'status.disconnected': 'Service offline', 'status.stopped': 'Stopped', 'status.idle': 'Standby',
    'conv.emptyList': 'No chats yet — click “＋ New chat”', 'conv.defaultTitle': 'New chat',
    'conv.folderDefault': 'Unfiled', 'conv.folderArchived': 'Archived', 'conv.folderPrompt': 'Enter folder name', 'conv.folderExists': 'Folder already exists', 'conv.folderEmpty': 'Empty folder', 'conv.folderDeleteConfirm': 'Delete folder "{0}"? Conversations will be moved to Unfiled', 'conv.folderLocked': 'System folders cannot be changed', 'conv.moveDone': 'Moved to {0}',
    'ctx.moveToFolder': 'Move to folder', 'ctx.moveArchived': 'Move to Archived',
    'err.bridge': 'Service not responding', 'err.newSession': 'Failed to create session', 'err.poll': 'Polling failed', 'err.stop': 'Stop failed',
    'err.interruptTimeout': 'Timed out waiting for the previous reply to stop — try again',
    'sys.interruptPrev.hint': 'Previous reply stopped — processing new message',
    'chat.interrupting': 'Stopping previous reply…',
    'chat.sessionLoading': 'Loading conversation…',
    'sys.stopRequested': 'Stop requested',
    'slash.help': 'Commands:\n/new new chat  /clear clear  /stop stop  /settings settings',
    'slash.unknown': 'Unknown command',
    'upload.hint': 'Upload file: pick / drag / paste',
    'upload.button': 'Upload file',
    'upload.tooLarge': 'File too large or limit reached', 'upload.empty': 'Skipped empty file',
    'upload.failed': 'Upload failed',
    'err.charLimit': 'Character limit reached ({n}), text will be truncated on send', 'err.charLimitReached': 'Character limit reached ({n})', 'err.numMax': 'Cannot exceed {n}',
    'file.openFailed': 'Cannot open file',
    'file.kindGeneric': 'File',
    'file.kindDoc': 'Document',
    'file.kindSheet': 'Spreadsheet',
    'file.kindSlide': 'Slides',
    'file.kindCode': 'Code',
    'file.kindArchive': 'Archive',
    'file.kindAudio': 'Audio',
    'file.kindVideo': 'Video',
    'upload.removeTitle': 'Remove',
    'upload.dropHint': 'Drop to upload files',
    'lightbox.closeTitle': 'Close',
    'fold.thinking': 'Thinking', 'fold.tool': 'Tool call', 'fold.toolResult': 'Tool result', 'fold.llm': 'LLM Running', 'fold.turn': 'Turn {n}',
    'plan.header': 'Plan ({done}/{total})', 'plan.complete': '✓ Plan complete ({n}/{n})',
    'plan.running': 'Running plan', 'plan.completeTitle': 'Plan complete',
    'plan.placeholder': 'Plan mode activated', 'plan.waiting': 'waiting for {path} …', 'plan.overflow': '+{n} more',
    'plan.current': 'Now', 'plan.collapse': 'Collapse', 'plan.expand': 'Expand', 'plan.details': 'Details',
    'plan.capsuleRunning': 'Running', 'plan.capsuleComplete': 'Done',
    'timing.elapsed': 'Elapsed {t}',
    'model.auto': 'Auto',
    'model.menuLabel': 'Select model',
    'chip.plan': 'Plan',
    'chip.auto': 'Auto',
    'ch.wechat': 'WeChat', 'ch.wecom': 'WeCom', 'ch.lark': 'Lark', 'ch.dingtalk': 'DingTalk',
    'ch.qq': 'QQ', 'ch.telegram': 'Telegram', 'ch.discord': 'Discord',
    'ch.loading': 'Loading…', 'ch.empty': 'No IM process scripts found',
    'ch.logEmpty': 'No log output yet',
    'err.channelLoad': 'Failed to load', 'err.channelStart': 'Start failed', 'err.channelStop': 'Stop failed',
    'err.mykeyImport': 'Failed to import model config',
    'err.mykeyExport': 'Failed to export model config',
    'err.channelNotConfigured': 'Configure this platform in mykey.py first',
    'sys.channelStarted': 'Started', 'sys.channelStopped': 'Stopped',
    'modal.channelLogs': 'Process logs',
    'modal.mykeyConfig': 'mykey.py',
    'modal.qrLogin': '{name} QR Login',
    'qr.scanTip': 'Scan the QR code below with {name} to complete login',
    'qr.statusScanning': 'Waiting for scan...',
    'qr.statusScanned': 'Scanned, please confirm...',
    'qr.statusConfirmed': 'Login successful!',
    'qr.statusExpired': 'QR code expired, please refresh',
    'qr.failed': 'Failed to get QR code, please try again',
    'qr.refresh': 'Refresh QR code',
    'sys.configSaved': 'Configuration saved',
    'sys.mykeyImported': 'Model config imported',
    'sys.mykeyExported': 'Model config exported',
    'st.starting': 'Starting…', 'st.stopping': 'Stopping…', 'st.online': 'Online', 'st.offline': 'Offline', 'st.error': 'Error', 'st.running': 'Running', 'st.abnormal': 'Error',
    'act.configure': 'Configure', 'act.logs': 'Logs', 'act.restart': 'Restart', 'act.stop': 'Stop', 'act.start': 'Start', 'act.exit': 'Exit',
    'act.copy': 'Copy', 'act.copied': 'Copied', 'act.copyTex': 'TeX', 'act.send': 'Send',
    'proc.imbotWechat': 'imbot · WeChat', 'proc.imbotDing': 'imbot · DingTalk', 'proc.scheduler': 'Scheduler', 'proc.conductor': 'Conductor',
    'cm.scheduling': 'Scheduling', 'cm.running': 'Running', 'cm.idleSt': 'Idle',
    'cm.master': 'Dispatched 3 subtasks', 'cm.w1': 'Subtask: fetch data', 'cm.w2': 'Subtask: review results', 'cm.sub': 'Waiting for tasks',
    'tok.total': 'Total', 'tok.cost': 'Cache rate', 'tok.today': 'Today', 'tok.tabAll': 'Chat', 'tok.tabConductor': 'Conductor', 'tok.condTotal': 'Conductor Total', 'tok.condCurrent': 'Conductor Current', 'tok.condTip': 'Conductor usage is not included in chat totals', 'tok.condOffline': 'Service offline', 'tok.disclaimer': 'Pricing may vary by API provider. Please refer to the actual website.',
    'tok.colSession': 'Session', 'tok.colIn': 'Input', 'tok.colOut': 'Output', 'tok.colCacheW': 'Cache write', 'tok.colCache': 'Cache read', 'tok.colCost': 'Cost',
    'tok.from': 'From', 'tok.to': 'To', 'tok.reset': 'Reset', 'tok.noData': 'No records', 'tok.deleted': 'Session deleted',
    'tok.pricingUnknown': '⚠ Pricing not confirmed, using defaults',
    'page.tasks.title': 'Scheduled Tasks', 'page.tasks.list': 'Tasks', 'page.tasks.history': 'History',
    'page.tasks.emptyTitle': 'Select a conversation below to start your first task',
    'page.tasks.emptySub': '',
    'page.tasks.create': 'Create Task', 'page.tasks.historyEmpty': 'No history yet',
    'modal.createTask': 'Create Scheduled Task', 'task.name': 'Task Name', 'task.prompt': 'Prompt',
    'task.repeat': 'Repeat', 'task.schedule': 'Schedule', 'task.rp.daily': 'Daily', 'task.rp.weekday': 'Weekdays',
    'task.rp.weekly': 'Weekly', 'task.rp.monthly': 'Monthly', 'task.rp.once': 'Once',
    'task.rp.hourly': 'Hourly', 'task.rp.every4h': 'Every 4h', 'task.rp.every12h': 'Every 12h',
    'taskForm.name': 'Task Name', 'taskForm.namePh': 'Give your task a name', 'taskForm.prompt': 'Prompt',
    'taskForm.promptPh': 'Enter the instruction for AI to execute', 'taskForm.repeat': 'Repeat', 'taskForm.schedule': 'Schedule',
    'taskForm.repeatDaily': 'Daily', 'taskForm.repeatWeekday': 'Weekdays', 'taskForm.repeatWeekly': 'Weekly',
    'taskForm.repeatMonthly': 'Monthly', 'taskForm.repeatOnce': 'Once', 'taskForm.repeat1h': 'Every 1h',
    'taskForm.repeat3h': 'Every 3h', 'taskForm.repeat6h': 'Every 6h', 'taskForm.repeat1d': 'Every 1d',
    'taskForm.advanced': 'Advanced', 'taskForm.maxDelay': 'Max Delay (hours)', 'taskForm.maxDelayHint': 'Skip if not executed within this time',
    'taskForm.enabled': 'Enable Task', 'taskForm.saved': 'Task created', 'taskForm.fail': 'Failed to create',
    'common.create': 'Create',
    'task.createOk': 'Task created', 'task.promptRequired': 'Please enter a prompt', 'task.createFail': 'Create failed',
    'tok.priceInput': 'Input: $', 'tok.priceOutput': 'Output: $',
    'tok.priceCacheW': 'Cache write: $', 'tok.priceCacheR': 'Cache read: $',
    'presetPrompt.goal': 'Enter Goal mode: read the L3 goal-mode SOP and autonomously achieve the goal I describe next.',
    'presetPrompt.plan': 'Enter Plan mode: first read memory/plan_sop.md, follow its explore→plan→execute→verify flow, and wait for the task I describe next.',
    'presetPrompt.autonomous': '🤖 Enter autonomous mode: read memory/autonomous_operation_sop.md, follow the SOP to pick or plan a task, execute independently, and produce a report.',
    'presetPrompt.hive': 'Start Goal Hive mode: per the hive SOP, spawn multiple workers to collaboratively achieve the goal I describe next.',
    'presetPrompt.review': 'Enter reviewer mode: strictly scrutinize the previous output, review item by item and report issues.',
    'presetPrompt.findwork': 'Following the autonomous planning section, analyze my situation thoroughly and generate a batch of TODOs that genuinely interest me.',
    'presetPrompt.mine': 'Collect this week\'s git commits and write a weekly report.',
    'ask.banner': 'GA is waiting for your answer',
    'ask.replyHint': 'Reply in the input below',
    'ask.placeholderOpen': 'Type your answer here… (Enter to send)',
    'nav.files': 'Files', 'page.files.title': 'File Manager', 'files.filterTitle': 'Filter by Type',
    'files.all': 'All', 'files.source.chat': 'Chat Uploads', 'files.source.task': 'Scheduled Tasks', 'files.source.config': 'Config', 'files.source.generated': 'Generated Files',
    'files.referencedBy': 'Referenced', 'files.download': 'Download', 'files.delete': 'Delete',
    'files.confirmDelete': 'Delete this file?', 'files.empty': 'No files',
    'files.sizeB': 'B', 'files.sizeKB': 'KB', 'files.sizeMB': 'MB',
    'files.viewList': 'List View', 'files.viewGrid': 'Grid View', 'files.refresh': 'Refresh',
    'files.menu': 'More actions', 'files.open': 'Open File', 'files.preview': 'Preview', 'files.openLocation': 'Reveal in Folder', 'files.copy': 'Duplicate', 'files.sort.name': 'Name', 'files.sort.size': 'Size', 'files.sort.mtime': 'Modified', 'files.sort.created': 'Created', 'files.sortAsc': 'Ascending', 'files.sortDesc': 'Descending',
    'files.confirmDelete': 'Delete this file?', 'files.confirmDeleteMulti': 'Delete {n} selected files?',
    'files.confirmCopy': 'Duplicate this file?', 'files.copied': 'Duplicated', 'files.copying': 'Duplicating…',
    'files.selectMode': 'Select', 'files.selectAll': 'Select All', 'files.deselectAll': 'Deselect All',
    'files.batchOpenLocation': 'Reveal', 'files.batchCopy': 'Duplicate', 'files.batchDelete': 'Delete',
    'files.selected': '{n} selected', 'files.cancel': 'Cancel',
    'conv.untitled': 'New Chat', 'notify.done': 'Conversation completed',
  },
};
const LANGS = ['zh', 'en'];
const STORE = { lang: 'ga_lang', theme: 'ga_theme', appearance: 'ga_appearance', plain: 'ga_plain', fontSize: 'ga_font_size', llmNo: 'ga_llm_no' };
const APPEARANCE_IDS = ['light', 'dark'];
const CHAT_FONT_MIN = 10;
const CHAT_FONT_MAX = 20;
const CHAT_FONT_DEFAULT = 14;
const CHAT_FONT_LEGACY = { sm: 12, md: 14, lg: 16 };
const HLJS_THEME_BASE = 'https://cdn.jsdelivr.net/gh/highlightjs/cdn-release@11.9.0/build/styles/';

function normalizeChatFontSize(value) {
  if (typeof value === 'string' && CHAT_FONT_LEGACY[value]) return CHAT_FONT_LEGACY[value];
  const n = parseInt(value, 10);
  if (Number.isFinite(n)) return Math.min(CHAT_FONT_MAX, Math.max(CHAT_FONT_MIN, n));
  return CHAT_FONT_DEFAULT;
}

function bootUiFromDom() {
  const root = document.documentElement;
  const out = { lang: 'zh', theme: '1', appearance: 'light', plainUi: false, chatFontSize: CHAT_FONT_DEFAULT };
  if (root.lang === 'en') out.lang = 'en';
  if (root.dataset.theme) out.theme = root.dataset.theme;
  if (APPEARANCE_IDS.includes(root.dataset.appearance)) out.appearance = root.dataset.appearance;
  if (out.appearance === 'light' && root.dataset.plain === '1') out.plainUi = true;
  if (root.dataset.chatFont) out.chatFontSize = normalizeChatFontSize(root.dataset.chatFont);
  return out;
}
let { lang, theme, appearance, plainUi, chatFontSize } = bootUiFromDom();

function syncHljsTheme() {
  const link = document.getElementById('hljs-theme');
  if (link) link.href = HLJS_THEME_BASE + (appearance === 'dark' ? 'github-dark.min.css' : 'github.min.css');
  document.querySelectorAll('.bubble.md pre code').forEach(block => {
    if (typeof hljs !== 'undefined') hljs.highlightElement(block);
  });
}

/** 服务端 ui 落盘后的本地镜像，仅供 index.html 内联脚本首帧防闪；不是真相源。 */
function syncBootCache() {
  localStorage.setItem(STORE.lang, lang);
  localStorage.setItem(STORE.theme, theme);
  localStorage.setItem(STORE.appearance, appearance);
  localStorage.setItem(STORE.fontSize, String(chatFontSize));
  if (plainUi) localStorage.setItem(STORE.plain, '1');
  else localStorage.removeItem(STORE.plain);
  localStorage.setItem(STORE.llmNo, String(state.llmNo));
}
async function persistUiPrefs() {
  try {
    const _cfd = document.getElementById('chat-files-dir-input');
    await window.ga.saveConfig({
      config: { lang, theme, appearance, plain: plainUi, llmNo: state.llmNo, fontSize: chatFontSize, chatFilesDir: _cfd ? (_cfd.value.trim() || 'temp') : 'temp' },
    });
    syncBootCache();
  } catch (_) {}
}
const bridgeHost = () => BRIDGE_ORIGIN;
async function bridgeFetch(path, opts = {}) {
  const headers = { ...(opts.headers || {}) };
  const init = { ...opts, headers };
  if (init.body && typeof init.body !== 'string') {
    headers['Content-Type'] = 'application/json';
    init.body = JSON.stringify(init.body);
  }
  const res = await fetch(`${bridgeHost()}${path}`, init);
  let data = {};
  try { data = await res.json(); } catch (_) {}
  if (!res.ok) throw new Error(data.error || data.message || res.statusText);
  return data;
}
function t(key) { return (I18N[lang] && I18N[lang][key]) || (I18N.zh[key]) || key; }
window.gaT = t;
document.addEventListener('collab:running-count', e => {
  const b = document.getElementById('collab-badge');
  if (!b) return;
  const n = e.detail?.count || 0;
  b.hidden = !n;
  b.textContent = n ? (n > 9 ? '9+' : String(n)) : '';
});
function optionalPh(key) {
  const sep = (lang === 'en') ? ', ' : '，';
  return `${t('common.optional')}${sep}${t(key)}`;
}
function applyI18n() {
  document.documentElement.lang = (lang === 'en') ? 'en' : 'zh-CN';
  document.title = t('app.title');
  document.querySelectorAll('[data-i18n]').forEach(el => { el.textContent = t(el.dataset.i18n); });
  document.querySelectorAll('[data-i18n-ph]').forEach(el => {
    const phKey = el.dataset.i18nPh;
    const val = el.hasAttribute('data-optional-ph') ? optionalPh(phKey) : t(phKey);
    if (el.isContentEditable) el.setAttribute('data-ph', val);  // contenteditable 用 :empty::before 显示
    else el.setAttribute('placeholder', val);
  });
  document.querySelectorAll('[data-i18n-title]').forEach(el => { el.setAttribute('title', t(el.dataset.i18nTitle)); });
  renderLangList();
  // 语言切换后重算激活模型 chip 文案；若当前会话已有渠道组运行态模型，保留运行态而非退回首选项
  const _ap = (state.modelProfiles || []).find(p => (p.id ?? 0) === state.llmNo);
  if (_ap) state.modelName = modelDisplayName(_ap);
  if (_ap?.kind === 'mixin' && state.liveModel?.sessionId === state.activeId) applyLiveModel(state.liveModel);
  else if (typeof updateModelChip === 'function') updateModelChip();
  window.gaRefreshModelGuide?.();
  window.collabRetranslate?.();
  syncAskUserUi();
}
// 语言对应国旗 SVG(en 用美国旗,按要求)
const FLAGS = {
  zh: '<svg class="flag" viewBox="0 0 30 20" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><rect width="30" height="20" fill="#ee1c25"/><polygon points="6,3.5 6.9,6.2 9.7,6.2 7.4,7.9 8.3,10.6 6,8.9 3.7,10.6 4.6,7.9 2.3,6.2 5.1,6.2" fill="#ffde00"/><circle cx="11.5" cy="2.6" r=".9" fill="#ffde00"/><circle cx="13.2" cy="4.3" r=".9" fill="#ffde00"/><circle cx="13.2" cy="6.7" r=".9" fill="#ffde00"/><circle cx="11.5" cy="8.4" r=".9" fill="#ffde00"/></svg>',
  en: '<svg class="flag" viewBox="0 0 38 20" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><rect width="38" height="20" fill="#ffffff"/><g fill="#b22234"><rect width="38" height="1.54"/><rect y="3.08" width="38" height="1.54"/><rect y="6.16" width="38" height="1.54"/><rect y="9.24" width="38" height="1.54"/><rect y="12.32" width="38" height="1.54"/><rect y="15.4" width="38" height="1.54"/><rect y="18.48" width="38" height="1.54"/></g><rect width="15.2" height="10.78" fill="#3c3b6e"/></svg>',
};
function renderLangList() {
  const box = document.getElementById('lang-list');
  if (!box) return;
  box.innerHTML = '';
  LANGS.forEach(code => {
    const row = document.createElement('label');
    row.className = 'model-row' + (lang === code ? ' sel' : '');
    row.innerHTML = `<input type="radio" name="lang-pick"${lang === code ? ' checked' : ''}>${FLAGS[code] || ''}<span>${escapeHtml(t('lang.' + code))}</span>`;
    row.addEventListener('click', (e) => { e.preventDefault(); selectLang(code); });
    box.appendChild(row);
  });
}
function selectLang(code) {
  if (!LANGS.includes(code) || lang === code) return;
  lang = code;
  applyI18n();
  renderSessionList();
  refreshStatusLabel();
  updateModelChip();
  renderSettingsModels();
  if (typeof renderAllPresets === 'function') renderAllPresets();
  if (isServicesPageActive()) refreshServicesPanel();
  void persistUiPrefs();
}
function syncChatFontSegments(value) {
  document.querySelectorAll('.chat-font-seg').forEach(el => {
    const v = parseInt(el.dataset.value, 10);
    el.classList.toggle('on', v <= value);
    el.classList.toggle('cur', v === value);
  });
  const stepper = document.getElementById('chat-font-stepper');
  if (stepper) {
    stepper.setAttribute('aria-valuenow', String(value));
    stepper.setAttribute('aria-valuetext', `${value}px`);
  }
}
function chatFontFromPointer(clientX) {
  const segs = document.getElementById('chat-font-segments');
  if (!segs) return chatFontSize;
  const rect = segs.getBoundingClientRect();
  const ratio = Math.min(1, Math.max(0, (clientX - rect.left) / rect.width));
  return CHAT_FONT_MIN + Math.round(ratio * (CHAT_FONT_MAX - CHAT_FONT_MIN));
}
function initChatFontStepper() {
  const segs = document.getElementById('chat-font-segments');
  if (!segs || segs.childElementCount) return;
  for (let i = CHAT_FONT_MIN; i <= CHAT_FONT_MAX; i++) {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'chat-font-seg';
    btn.dataset.value = String(i);
    btn.tabIndex = -1;
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      applyChatFontSize(i);
    });
    segs.appendChild(btn);
  }
  const stepper = document.getElementById('chat-font-stepper');
  if (!stepper || stepper.dataset.bound) return;
  stepper.dataset.bound = '1';
  let dragging = false;
  const pick = (clientX, persist) => applyChatFontSize(chatFontFromPointer(clientX), { persist });
  stepper.addEventListener('pointerdown', (e) => {
    if (e.button !== 0) return;
    dragging = true;
    stepper.setPointerCapture(e.pointerId);
    pick(e.clientX, false);
  });
  stepper.addEventListener('pointermove', (e) => {
    if (!dragging) return;
    pick(e.clientX, false);
  });
  const endDrag = (e, persist) => {
    if (!dragging) return;
    dragging = false;
    try { stepper.releasePointerCapture(e.pointerId); } catch (_) {}
    pick(e.clientX, persist);
  };
  stepper.addEventListener('pointerup', (e) => endDrag(e, true));
  stepper.addEventListener('pointercancel', (e) => endDrag(e, false));
  stepper.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowLeft' || e.key === 'ArrowDown') {
      e.preventDefault();
      applyChatFontSize(chatFontSize - 1);
    } else if (e.key === 'ArrowRight' || e.key === 'ArrowUp') {
      e.preventDefault();
      applyChatFontSize(chatFontSize + 1);
    } else if (e.key === 'Home') {
      e.preventDefault();
      applyChatFontSize(CHAT_FONT_MIN);
    } else if (e.key === 'End') {
      e.preventDefault();
      applyChatFontSize(CHAT_FONT_MAX);
    }
  });
}
function applyChatFontSize(size, { persist } = { persist: true }) {
  chatFontSize = normalizeChatFontSize(size);
  document.documentElement.dataset.chatFont = String(chatFontSize);
  document.documentElement.style.setProperty('--chat-font', `${chatFontSize}px`);
  const label = document.getElementById('chat-font-value');
  if (label) label.textContent = `${chatFontSize}px`;
  syncChatFontSegments(chatFontSize);
  if (persist) void persistUiPrefs();
}
function applyTheme(id, { persist } = { persist: true }) {
  // 主题选色已下线,只保留灰色亮色主题(--accent 在 styles.css 里硬编码)。
  // 函数保留可调用,只把 dataset.theme 固定到 '1' 兼容旧 localStorage。
  theme = '1';
  document.documentElement.dataset.theme = '1';
  if (persist) void persistUiPrefs();
}
function syncPlainSwitch() {
  const row = document.getElementById('plain-ui-row');
  const sw = document.getElementById('plain-ui-switch');
  if (!row || !sw) return;
  const show = appearance === 'light';
  row.hidden = !show;
  sw.setAttribute('aria-checked', plainUi ? 'true' : 'false');
}
function applyAppearance(nextApp, nextPlain, { persist } = { persist: true }) {
  appearance = APPEARANCE_IDS.includes(nextApp) ? nextApp : 'light';
  if (appearance === 'light') plainUi = !!nextPlain;
  else plainUi = false;
  document.documentElement.dataset.appearance = appearance;
  if (plainUi) document.documentElement.dataset.plain = '1';
  else delete document.documentElement.dataset.plain;
  document.querySelectorAll('#appearance-seg .appear-card').forEach(el => {
    const on = el.dataset.appearance === appearance;
    el.classList.toggle('sel', on);
    el.setAttribute('aria-checked', on ? 'true' : 'false');
  });
  syncPlainSwitch();
  syncHljsTheme();
  if (persist) void persistUiPrefs();
}

/* ═══════════════ 侧边栏导航 ═══════════════ */
const nav = document.getElementById('nav');
const pages = document.querySelectorAll('#pages .page');
let currentPage = 'chat';
function gaGoPage(key) {
  const item = nav?.querySelector(`.nav-item[data-page="${key}"]`);
  if (!item) return;
  currentPage = key;
  nav.querySelectorAll('.nav-item').forEach(n => n.classList.toggle('active', n === item));
  pages.forEach(p => p.classList.toggle('active', p.dataset.page === key));
  if (bodyEl) {
    if (key === 'tasks' || key === 'token' || key === 'services' || key === 'files') bodyEl.classList.add('rp-collapsed');
    else bodyEl.classList.remove('rp-collapsed');
  }
  renderSessionList();
  window.gaSetActiveFileComposer?.(key === 'collab' ? 'collab' : 'chat');
  if (key === 'collab') window.collabInit?.();
  if (key === 'project') window.loadProjects?.();
}
window.gaGoPage = gaGoPage;
nav.addEventListener('click', (e) => {
  const item = e.target.closest('.nav-item');
  if (!item) return;
  gaGoPage(item.dataset.page);
});

/* ═══════════════ 弹窗开关 ═══════════════ */
const openModal = (id) => { const m = document.getElementById(id); if (m) m.hidden = false; };
window.gaOpenModal = openModal;
const closeModals = () => document.querySelectorAll('.modal').forEach(m => {
  m.hidden = true;
  m.querySelectorAll('.field-limit-hint').forEach(h => h.style.display = 'none');
});
const bindClick = (id, fn) => { const el = document.getElementById(id); if (el) el.addEventListener('click', fn); };
function openServiceManagerFromSettings() {
  closeModals();
  gaGoPage('services');
  setSvcTab('status');
  void loadStatusPanel();
}
bindClick('add-model-btn', (e) => {
  e.stopPropagation();
  openAddModelForm();
});
bindClick('settings-btn',  (e) => { e.stopPropagation(); openSettings(); });
bindClick('settings-services-btn', (e) => { e.stopPropagation(); openServiceManagerFromSettings(); });

const importMykeyInput = document.getElementById('import-mykey-input');
async function importMykeyFromFile(file) {
  if (!file) return;
  const text = await file.text();
  if (!text.trim()) throw new Error(t('err.mykeyImport'));
  await window.ga.saveMykeyContent(text);
  await loadModelProfiles();
}
bindClick('import-mykey-btn', (e) => {
  e.stopPropagation();
  if (importMykeyInput) importMykeyInput.click();
});
if (importMykeyInput) {
  importMykeyInput.addEventListener('change', async () => {
    const file = importMykeyInput.files && importMykeyInput.files[0];
    importMykeyInput.value = '';
    if (!file) return;
    try {
      await importMykeyFromFile(file);
      showChanToast(t('sys.mykeyImported'), '', 'ok');
    } catch (err) {
      showChanToast(t('err.mykeyImport'), err.message || String(err), 'err');
    }
  });
}
async function exportMykeyToDir() {
  const res = await window.ga.getMykeyContent();
  const content = (res && res.content) ? String(res.content) : '';
  if (!content.trim()) throw new Error(t('err.mykeyExport'));
  // WebView2：独立缓存 + 无目录选择/下载；走 Tauri 原生另存为
  if (window.__TAURI__?.core?.invoke) {
    const path = await window.ga.tauriInvoke('export_mykey', { content });
    if (!path) return;
    showChanToast(t('sys.mykeyExported'), path, 'ok');
    return;
  }
  if (typeof window.showDirectoryPicker === 'function') {
    const dir = await window.showDirectoryPicker();
    const handle = await dir.getFileHandle('mykey.py', { create: true });
    const writable = await handle.createWritable();
    await writable.write(content);
    await writable.close();
    showChanToast(t('sys.mykeyExported'), '', 'ok');
    return;
  }
  const blob = new Blob([content], { type: 'text/plain;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'mykey.py';
  a.rel = 'noopener';
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
  showChanToast(t('sys.mykeyExported'), '', 'ok');
}
bindClick('export-mykey-btn', async (e) => {
  e.stopPropagation();
  try {
    await exportMykeyToDir();
  } catch (err) {
    if (err && (err.name === 'AbortError' || err.code === 20)) return;
    showChanToast(t('err.mykeyExport'), err.message || String(err), 'err');
  }
});
// 侧边栏「快速接入」：点击官方模型按钮 → 打开预填好的添加模型表单
const pqEl = document.getElementById('provider-quickstart');
if (pqEl) pqEl.addEventListener('click', (e) => {
  const btn = e.target.closest('.pq-btn[data-provider]');
  if (!btn) return;
  e.preventDefault(); e.stopPropagation();
  openAddModelFormForProvider(btn.dataset.provider);
});
// 「快速接入」卡片折叠/展开（向下箭头），状态记忆到 localStorage
const pqToggle = document.getElementById('pq-toggle');
if (pqEl && pqToggle) {
  const applyPq = (collapsed) => {
    pqEl.classList.toggle('collapsed', collapsed);
    pqToggle.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
  };
  let pqCollapsed = false;
  try { pqCollapsed = localStorage.getItem('ga_pq_collapsed') === '1'; } catch (_) {}
  applyPq(pqCollapsed);
  const togglePq = (e) => {
    if (e) e.stopPropagation();
    pqCollapsed = !pqEl.classList.contains('collapsed');
    applyPq(pqCollapsed);
    try { localStorage.setItem('ga_pq_collapsed', pqCollapsed ? '1' : '0'); } catch (_) {}
  };
  pqToggle.addEventListener('click', togglePq);
  pqToggle.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); togglePq(); }
  });
}
// 接入指引：复制获取 API Key 的链接
bindClick('model-guide-copy', (e) => {
  e.preventDefault(); e.stopPropagation();
  const link = document.getElementById('model-guide-link');
  const url = link ? link.href : '';
  if (!url || !navigator.clipboard) return;
  navigator.clipboard.writeText(url).then(() => showChanToast(t('guide.copied'), '', 'ok')).catch(() => {});
});
bindClick('preset-btn',    (e) => { e.stopPropagation(); openModal('preset-modal'); });
document.querySelectorAll('.modal').forEach(m =>
  m.addEventListener('click', (e) => {
    if (e.target.closest('[data-close]')) {
      m.hidden = true;
      m.querySelectorAll('.field-limit-hint').forEach(h => h.style.display = 'none');
    }
  }));
document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeModals(); });

function showConfirmDialog({ title, message, okText, okKind = 'primary', cancelText } = {}) {
  const modal = document.getElementById('confirm-modal');
  if (!modal) return Promise.resolve(false);
  const titleEl = document.getElementById('confirm-title');
  const msgEl = document.getElementById('confirm-message');
  const okBtn = document.getElementById('confirm-ok');
  const cancelBtn = document.getElementById('confirm-cancel');
  if (titleEl) titleEl.textContent = title || t('common.confirm');
  if (msgEl) msgEl.textContent = message || '';
  if (cancelBtn) cancelBtn.textContent = cancelText || t('common.cancel');
  if (okBtn) {
    okBtn.textContent = okText || t('common.confirm');
    okBtn.classList.toggle('danger', okKind === 'danger');
    okBtn.classList.toggle('primary', okKind !== 'danger');
  }
  modal.hidden = false;
  return new Promise(resolve => {
    let done = false;
    const finish = (yes) => {
      if (done) return;
      done = true;
      modal.hidden = true;
      cleanup();
      resolve(yes);
    };
    const onOk = (e) => { e.preventDefault(); e.stopPropagation(); finish(true); };
    const onCancel = (e) => { e.preventDefault(); e.stopPropagation(); finish(false); };
    const onClose = (e) => { if (e.target.closest('[data-close]')) finish(false); };
    const onKey = (e) => { if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); finish(false); } };
    const cleanup = () => {
      okBtn?.removeEventListener('click', onOk);
      cancelBtn?.removeEventListener('click', onCancel);
      modal.removeEventListener('click', onClose, true);
      document.removeEventListener('keydown', onKey, true);
    };
    okBtn?.addEventListener('click', onOk);
    cancelBtn?.addEventListener('click', onCancel);
    modal.addEventListener('click', onClose, true);
    document.addEventListener('keydown', onKey, true);
    okBtn?.focus();
  });
}

function showPromptDialog({ title, message, value = '', okText, cancelText } = {}) {
  const modal = document.getElementById('prompt-modal');
  if (!modal) return Promise.resolve(null);
  const titleEl = document.getElementById('prompt-title');
  const msgEl = document.getElementById('prompt-message');
  const inputEl = document.getElementById('prompt-input');
  const okBtn = document.getElementById('prompt-ok');
  const cancelBtn = document.getElementById('prompt-cancel');
  if (titleEl) titleEl.textContent = title || t('common.confirm');
  if (msgEl) msgEl.textContent = message || '';
  if (inputEl) inputEl.value = value;
  if (cancelBtn) cancelBtn.textContent = cancelText || t('common.cancel');
  if (okBtn) okBtn.textContent = okText || t('common.confirm');
  modal.hidden = false;
  if (inputEl) { inputEl.focus(); inputEl.select(); }
  return new Promise(resolve => {
    let done = false;
    const finish = (val) => { if (done) return; done = true; modal.hidden = true; cleanup(); resolve(val); };
    const onOk = (e) => { e.preventDefault(); e.stopPropagation(); finish(inputEl ? inputEl.value : ''); };
    const onCancel = (e) => { e.preventDefault(); e.stopPropagation(); finish(null); };
    const onClose = (e) => { if (e.target.closest('[data-close]')) finish(null); };
    const onKey = (e) => {
      if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); finish(null); }
      else if (e.key === 'Enter' && (e.target === inputEl || (inputEl && inputEl.contains(e.target)))) { e.preventDefault(); e.stopPropagation(); finish(inputEl ? inputEl.value : ''); }
    };
    const cleanup = () => {
      okBtn?.removeEventListener('click', onOk);
      cancelBtn?.removeEventListener('click', onCancel);
      modal.removeEventListener('click', onClose, true);
      document.removeEventListener('keydown', onKey, true);
    };
    okBtn?.addEventListener('click', onOk);
    cancelBtn?.addEventListener('click', onCancel);
    modal.addEventListener('click', onClose, true);
    document.addEventListener('keydown', onKey, true);
  });
}

/* ═══════════════ Markdown ═══════════════ */
if (typeof marked !== 'undefined') {
  marked.setOptions({ gfm: true, breaks: true, mangle: false, headerIds: false });
}
const ALLOWED_URI_RE = /^(https?:|mailto:|tel:|#|\/)/i;
function escapeHtml(s) {
  const d = document.createElement('div'); d.textContent = String(s == null ? '' : s); return d.innerHTML;
}
function formatProjectEventTime(ts) {
  if (!ts) return '';
  var n = Date.parse(ts);
  if (!Number.isFinite(n)) return String(ts);
  return new Intl.DateTimeFormat('zh-CN', {
    month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false
  }).format(new Date(n)).replace(',', ' ');
}
function formatDatasourceProjectLabel(project) {
  var s = String(project || '').trim();
  if (!s) return '';
  try {
    var u = new URL(s);
    var path = (u.pathname || '').replace(/^\/+|\/+$/g, '');
    return path || u.hostname || s;
  } catch (_) {
    return s.replace(/^https?:\/\//i, '').replace(/^www\./i, '').replace(/\/$/, '');
  }
}
function formatDatasourceEvent(evt) {
  if (!evt || typeof evt !== 'object') {
    return {
      title: String(evt || '收到事件'),
      meta: [],
      link: '',
      project: '',
      projectLabel: '',
      detail: '',
      detailMuted: false,
      linkText: ''
    };
  }
  var kindMap = {
    issue: 'Issue',
    merge_request: '合并请求',
    note: '评论',
    pipeline: '流水线',
    push: 'Push'
  };
  var actionMap = {
    open: '已创建',
    update: '有更新',
    close: '已关闭',
    reopen: '已重新打开',
    merge: '已合并'
  };
  var kind = kindMap[evt.kind] || (evt.kind ? String(evt.kind) : '事件');
  var action = actionMap[evt.action] || (evt.action ? String(evt.action) : '有变更');
  var iid = evt.iid != null && evt.iid !== '' ? (' #' + evt.iid) : '';
  var title = (kind + iid + ' ' + action).trim();
  var meta = [];
  if (evt.author) meta.push(evt.author);
  var timeText = formatProjectEventTime(evt.ts);
  if (timeText) meta.push(timeText);
  var project = evt.project || '';
  var projectLabel = formatDatasourceProjectLabel(project);
  var detail = String(evt.title || '').trim();
  var detailMuted = false;
  if (!detail) {
    detail = evt.url ? '可点击查看详情' : '暂无更多描述';
    detailMuted = true;
  }
  var linkText = '';
  if (evt.url) {
    linkText = evt.kind === 'merge_request' ? '查看合并请求' : (evt.kind === 'issue' ? '查看 Issue' : '查看详情');
  }
  return {
    title: title || '收到事件',
    meta: meta,
    link: evt.url || '',
    project: project,
    projectLabel: projectLabel,
    detail: detail,
    detailMuted: detailMuted,
    linkText: linkText
  };
}
/** GA list_llms 形如 SessionClass/备注；桌面 UI 只展示 / 后一段 */
function profileLabel(name) {
  const s = String(name || '');
  const i = s.indexOf('/');
  return (i >= 0 ? s.slice(i + 1) : s).trim();
}
function normalizeProfiles(list) {
  return (list || []).map(p => ({ ...p, name: profileLabel(p.name) || p.name }));
}
function sanitizeMarkdown(html) {
  const tpl = document.createElement('template');
  tpl.innerHTML = String(html);
  const blocked = new Set(['SCRIPT','STYLE','IFRAME','OBJECT','EMBED','LINK','META','BASE','FORM','INPUT','BUTTON']);
  const walker = document.createTreeWalker(tpl.content, NodeFilter.SHOW_ELEMENT);
  const rmv = [];
  while (walker.nextNode()) {
    const el = walker.currentNode;
    if (blocked.has(el.tagName)) { rmv.push(el); continue; }
    for (const attr of Array.from(el.attributes)) {
      const n = attr.name.toLowerCase(), v = attr.value.trim();
      if (n.startsWith('on') || n === 'srcdoc') { el.removeAttribute(attr.name); continue; }
      if ((n === 'href' || n === 'src' || n === 'xlink:href') && v && !ALLOWED_URI_RE.test(v)) el.removeAttribute(attr.name);
    }
    if (el.tagName === 'A') { el.setAttribute('rel','noopener noreferrer'); el.setAttribute('target','_blank'); }
  }
  rmv.forEach(el => el.remove());
  return tpl.innerHTML;
}
/* ═══════════════ LaTeX 保护 (PR移植) ═══════════════ */
const _latexSlots = [];
function protectLatex(text) {
  _latexSlots.length = 0;
  // 先保护代码围栏和行内代码，避免其中的 $ \( \[ 被误匹配
  const _codeSlots = [];
  // 代码围栏 ```...```
  text = text.replace(/```[\s\S]*?```/g, (m) => {
    const id = _codeSlots.length;
    _codeSlots.push(m);
    return `\x00CODE:${id}\x00`;
  });
  // 行内代码 `...`
  text = text.replace(/`[^`\n]+`/g, (m) => {
    const id = _codeSlots.length;
    _codeSlots.push(m);
    return `\x00CODE:${id}\x00`;
  });
  // 块级 \[...\]
  text = text.replace(/\\\[([\s\S]+?)\\\]/g, (_, expr) => {
    const id = _latexSlots.length;
    _latexSlots.push({ expr: expr.trim(), display: true });
    return `<!--LATEX:${id}-->`;
  });
  // 块级 $$...$$
  text = text.replace(/\$\$([\s\S]+?)\$\$/g, (_, expr) => {
    const id = _latexSlots.length;
    _latexSlots.push({ expr: expr.trim(), display: true });
    return `<!--LATEX:${id}-->`;
  });
  // 行内 \(...\)
  text = text.replace(/\\\(([\s\S]+?)\\\)/g, (_, expr) => {
    const id = _latexSlots.length;
    _latexSlots.push({ expr: expr.trim(), display: false });
    return `<!--LATEX:${id}-->`;
  });
  // 行内 $...$（不贪婪，排除 $$ 和转义）
  text = text.replace(/(?<!\\)\$([^\n$]+?)\$/g, (_, expr) => {
    const id = _latexSlots.length;
    _latexSlots.push({ expr: expr.trim(), display: false });
    return `<!--LATEX:${id}-->`;
  });
  // 恢复代码占位符
  text = text.replace(/\x00CODE:(\d+)\x00/g, (_, i) => _codeSlots[Number(i)]);
  return text;
}
function restoreLatex(html) {
  if (!_latexSlots.length) return html;
  return html.replace(/<!--LATEX:(\d+)-->/g, (_, i) => {
    const slot = _latexSlots[Number(i)];
    if (!slot) return '';
    if (typeof katex === 'undefined') {
      return slot.display ? `<div class="katex-block">${escapeHtml(slot.expr)}</div>`
                          : `<span class="katex-inline">${escapeHtml(slot.expr)}</span>`;
    }
    try {
      const rendered = katex.renderToString(slot.expr, { displayMode: slot.display, throwOnError: false });
      return slot.display ? `<div class="katex-block">${rendered}</div>`
                          : `<span class="katex-inline">${rendered}</span>`;
    } catch (_) { return escapeHtml(slot.expr); }
  });
}

function renderMarkdown(text) {
  if (typeof marked === 'undefined') return escapeHtml(text).replace(/\n/g, '<br>');
  try {
    const protected_ = protectLatex(String(text || ''));
    let html = sanitizeMarkdown(marked.parse(protected_));
    html = restoreLatex(html);
    // TUI 风格代码块：包装 pre>code 为 .code-block 容器 + 语言头
    html = html.replace(/<pre><code\b(?:\s+class="language-([^"]*)")?[^>]*>([\s\S]*?)<\/code><\/pre>/g,
      (_, lang, body) => {
        const label = lang || 'code';
        return `<div class="code-block"><div class="code-block-head"><span class="code-block-lang">${escapeHtml(label)}</span><button class="code-block-copy" aria-label="Copy code">\u29C9</button></div><pre><code class="language-${escapeHtml(label)}">${body}</code></pre></div>`;
      });
    return html;
  } catch (_) { return escapeHtml(text); }
}
/**
 * Agent 流协议（与 agent_loop.py / continue_cmd 一致）按行解析：
 * - 工具调用：🛠️ 行 + 开围栏行 `` `{n}text `` + 正文 + 闭围栏行（仅 `{n}，取区间内最后一行）
 * - 工具结果：开围栏行 `` `{n} ``（n≥5）+ 正文 + 同长度闭围栏行
 */
function parseAgentFenceLine(line) {
  const m = /^[ \t]*(`{3,})([^\n`]*)[ \t]*$/.exec(line ?? '');
  if (!m) return null;
  return { ticks: m[1].length, tag: m[2] };
}

function isAgentStructureBoundaryLine(line, opts) {
  if (/^🛠️ Tool:/.test(line)) return true;
  // 工具「结果」区内：5 反引号是开/闭围栏，不能当边界（否则闭围栏会被当成下一结构 → 拆出多个空「工具结果」）
  if (!opts || !opts.forToolResult) {
    const f = parseAgentFenceLine(line);
    if (f && f.ticks >= 5 && f.tag === '') return true;
  }
  if (/^\*\*LLM Running \(Turn \d+\)/.test(line)) return true;
  if (/^<thinking>/i.test(line)) return true;
  return false;
}

function indexOfNextAgentStructureLine(lines, from, opts) {
  for (let i = from; i < lines.length; i++) {
    if (isAgentStructureBoundaryLine(lines[i], opts)) return i;
  }
  return lines.length;
}

function lastFenceCloseLineIndex(lines, from, toExclusive, tickCount) {
  let last = -1;
  for (let i = from; i < toExclusive; i++) {
    const f = parseAgentFenceLine(lines[i]);
    if (f && f.ticks === tickCount && f.tag === '') last = i;
  }
  return last;
}

function parseToolCallBlock(lines, i) {
  const m = /^🛠️ Tool: `([^`]+)`/.exec(lines[i] || '');
  if (!m) return null;
  const open = parseAgentFenceLine(lines[i + 1]);
  if (!open || open.tag !== 'text') return null;
  const bodyStart = i + 2;
  const zoneEnd = indexOfNextAgentStructureLine(lines, bodyStart);
  const closeIdx = lastFenceCloseLineIndex(lines, bodyStart, zoneEnd, open.ticks);
  if (closeIdx < 0) return null;
  return {
    name: m[1],
    body: lines.slice(bodyStart, closeIdx).join('\n'),
    nextLine: closeIdx + 1,
  };
}

function parseToolResultBlock(lines, i) {
  const open = parseAgentFenceLine(lines[i]);
  if (!open || open.ticks < 5 || open.tag !== '') return null;
  const bodyStart = i + 1;
  const zoneEnd = indexOfNextToolResultZoneEnd(lines, bodyStart);
  const closeIdx = lastFenceCloseLineIndex(lines, bodyStart, zoneEnd, open.ticks);
  if (closeIdx < 0) return null;
  return {
    body: lines.slice(bodyStart, closeIdx).join('\n'),
    nextLine: closeIdx + 1,
  };
}

/** 工具结果区 zone：不把 5 反引号围栏行当边界（见 isAgentStructureBoundaryLine） */
function indexOfNextToolResultZoneEnd(lines, from) {
  return indexOfNextAgentStructureLine(lines, from, { forToolResult: true });
}

/** 流式未闭合工具调用（对齐 TUI _safe_pos：末尾 in-flight 🛠️ 块） */
function parseInFlightToolCall(lines, i) {
  if (parseToolCallBlock(lines, i)) return null;
  const m = /^🛠️ Tool: `([^`]+)`/.exec(lines[i] || '');
  if (!m) return null;
  const open = parseAgentFenceLine(lines[i + 1]);
  let bodyStart;
  let zoneEnd;
  if (open && open.tag === 'text') {
    bodyStart = i + 2;
    zoneEnd = indexOfNextAgentStructureLine(lines, bodyStart);
    if (lastFenceCloseLineIndex(lines, bodyStart, zoneEnd, open.ticks) >= 0) return null;
  } else {
    bodyStart = i + 1;
    zoneEnd = lines.length;
    for (let j = i + 1; j < lines.length; j++) {
      if (isAgentStructureBoundaryLine(lines[j])) { zoneEnd = j; break; }
    }
  }
  return {
    name: m[1],
    body: lines.slice(bodyStart, zoneEnd).join('\n'),
    nextLine: zoneEnd,
    inFlight: true,
  };
}

/** 流式未闭合工具结果（5 反引号围栏未到） */
function parseInFlightToolResult(lines, i) {
  if (parseToolResultBlock(lines, i)) return null;
  const open = parseAgentFenceLine(lines[i]);
  if (!open || open.ticks < 5 || open.tag !== '') return null;
  const bodyStart = i + 1;
  const zoneEnd = indexOfNextToolResultZoneEnd(lines, bodyStart);
  if (lastFenceCloseLineIndex(lines, bodyStart, zoneEnd, open.ticks) >= 0) return null;
  return {
    body: lines.slice(bodyStart, zoneEnd).join('\n'),
    nextLine: zoneEnd,
    inFlight: true,
  };
}

/** 将 agent 协议块替换为占位符，其余行原样保留给 Markdown */
function foldAgentProtocolBlocks(body, { onTool, onResult }) {
  const lines = String(body || '').split('\n');
  const out = [];
  let proseFrom = 0;
  let i = 0;

  const flushProse = (until) => {
    if (until <= proseFrom) return;
    out.push(lines.slice(proseFrom, until).join('\n'));
    proseFrom = until;
  };

  while (i < lines.length) {
    const tool = parseToolCallBlock(lines, i);
    if (tool) {
      flushProse(i);
      out.push(onTool(tool.name, tool.body));
      i = tool.nextLine;
      proseFrom = i;
      continue;
    }
    const result = parseToolResultBlock(lines, i);
    if (result) {
      flushProse(i);
      out.push(onResult(result.body));
      i = result.nextLine;
      proseFrom = i;
      continue;
    }
    const liveTool = parseInFlightToolCall(lines, i);
    if (liveTool) {
      flushProse(i);
      out.push(onTool(liveTool.name, liveTool.body, { inFlight: true }));
      i = liveTool.nextLine;
      proseFrom = i;
      continue;
    }
    const liveResult = parseInFlightToolResult(lines, i);
    if (liveResult) {
      flushProse(i);
      out.push(onResult(liveResult.body, { inFlight: true }));
      i = liveResult.nextLine;
      proseFrom = i;
      continue;
    }
    i++;
  }
  flushProse(lines.length);
  return out.join('');
}

function extractAskUserToolJson(content) {
  const lines = String(content || '').split('\n');
  for (let i = 0; i < lines.length; i++) {
    const block = parseToolCallBlock(lines, i);
    if (block && block.name === 'ask_user') return block.body;
  }
  return null;
}

// ============================================================================
// [turn_segs 数据结构层] —— 单轮渲染的纯函数，供 append-only draft 与静态消息复用
// 设计：每个函数自包含（内部自建 folds/asks 占位栈，渲染完即还原），轮间零共享状态。
// renderTurnBody(body)        : 单轮原文 → 该轮内层HTML（块级折叠thinking/tool/result）
// renderTurnFold(body, turnIndex) : 单轮原文 → 旧轮的<details>折叠壳（内部下标0起，标题显示1起+summary副标题）
// 结构化 turn_segs 渲染使用的纯函数：折叠工具块、ask_user 与轮摘要。
// ============================================================================
// 去除 GenericAgent 轮次分隔标记；turn_segs 已结构化，不应展示原始 marker
function stripTurnMarker(body) {
  return String(body || '')
    .replace(/^\s*\**LLM Running \(Turn \d+\) \.\.\.\**\s*/i, '');
}

function renderTurnBody(body) {
  // 自包含：每次调用独立的占位栈，渲染完立即还原，无跨调用共享状态
  const folds = [];
  const asks = [];
  const stash = (label, b, cls, opts) => {
    folds.push({ label, body: b, cls: cls || '', open: !!(opts && opts.open) });
    return `\n\n§§FOLD:${folds.length - 1}§§\n\n`;
  };
  const stashAsk = (data) => { asks.push(data); return `\n\n§§ASK:${asks.length - 1}§§\n\n`; };
  let s = stripTurnMarker(body);
  s = s.replace(/<thinking>[\s\S]*?<\/thinking>/gi, m => stash(t('fold.thinking'), m.replace(/<\/?thinking>/gi, ''), 'fold-thinking'));
  s = foldAgentProtocolBlocks(s, {
    onTool(name, json, meta) {
      if (name === 'ask_user' && !meta?.inFlight) {
        const data = parseAskUserJson(json);
        if (data && normalizeAskUserData(data)) return stashAsk(data);
      }
      const live = !!meta?.inFlight;
      return stash(`${t('fold.tool')}: ${name}${live ? ' …' : ''}`, json,
        live ? 'fold-tool fold-tool-live' : 'fold-tool', { open: live });
    },
    onResult(b, meta) {
      const live = !!meta?.inFlight;
      return stash(`${t('fold.toolResult')}${live ? ' …' : ''}`, b,
        live ? 'fold-result fold-tool-live' : 'fold-result', { open: live });
    },
  });
  s = s.replace(/<function_calls>[\s\S]*?<\/function_calls>/gi, m => stash(t('fold.tool'), m, 'fold-tool'));
  s = s.replace(/<function_results>[\s\S]*?<\/function_results>/gi, m => stash(t('fold.toolResult'), m, 'fold-result'));
  s = s.replace(/<summary>([\s\S]*?)<\/summary>/gi, (_, inner) => `<div class="turn-summary">${inner}</div>`);
  let html = renderMarkdown(s);
  // 还原占位符
  html = html
    .replace(/§§ASK:(\d+)§§/g, (_, i) => {
      const data = asks[Number(i)];
      return data ? renderAskUserNotice(data) : '';
    })
    .replace(/§§FOLD:(\d+)§§/g, (_, i) => {
      const f = folds[Number(i)];
      if (!f) return '';
      const openAttr = f.open ? ' open' : '';
      return `<details class="fold ${f.cls}"${openAttr}><summary>${escapeHtml(f.label)}</summary><pre class="fold-pre">${escapeHtml(f.body)}</pre></details>`;
    });
  return html;
}

// 抽出该轮首个 <summary> 文本作为折叠头副标题；无则回退提取工具名列表
function extractTurnSummaryPure(raw) {
  const m = /<summary>([\s\S]*?)<\/summary>/i.exec(raw || '');
  if (m) return m[1].replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
  const tools = [];
  const toolRe = /🛠️\s*Tool:\s*`([^`]+)`/g;
  let tm;
  while ((tm = toolRe.exec(raw || '')) !== null) {
    if (!tools.includes(tm[1])) tools.push(tm[1]);
  }
  return tools.length ? tools.join(', ') : '';
}

// 旧轮折叠壳：内部 turnIndex 从 0 起；UI 标题显示为 1 起。调用方一律传内部下标。
function turnDisplayNo(turnIndex) {
  return Math.max(0, Number(turnIndex) || 0) + 1;
}
function renderTurnFold(body, turnIndex) {
  const raw = stripTurnMarker(body);
  const sum = extractTurnSummaryPure(raw);
  const bodyForRender = sum ? raw.replace(/<summary>[\s\S]*?<\/summary>\s*/i, '') : raw;
  const inner = renderTurnBody(bodyForRender);
  const turnLabel = t('fold.turn').replace('{n}', turnDisplayNo(turnIndex));
  const head = sum
    ? `${escapeHtml(turnLabel)}：<span class="turn-head-sum">${escapeHtml(sum)}</span>`
    : escapeHtml(turnLabel);
  return `<details class="fold fold-turn"><summary>${head}</summary>${inner}</details>`;
}

function lastInflightBlockBody(body) {
  const lines = String(body || '').split('\n');
  let last = null;
  for (let i = 0; i < lines.length; i++) {
    const liveTool = parseInFlightToolCall(lines, i);
    if (liveTool?.inFlight) { last = { body: liveTool.body }; i = liveTool.nextLine - 1; continue; }
    const closedTool = parseToolCallBlock(lines, i);
    if (closedTool) { last = null; i = closedTool.nextLine - 1; continue; }
    const liveRes = parseInFlightToolResult(lines, i);
    if (liveRes?.inFlight) { last = { body: liveRes.body }; i = liveRes.nextLine - 1; continue; }
    const closedRes = parseToolResultBlock(lines, i);
    if (closedRes) { last = null; i = closedRes.nextLine - 1; continue; }
  }
  return last;
}

function tryPatchInflightToolDom(curEl, body, prevBody) {
  if (!prevBody || body.length < prevBody.length || !body.startsWith(prevBody)) return false;
  if (!curEl.querySelector('details.fold-tool-live')) return false;
  const prevBlock = lastInflightBlockBody(prevBody);
  const curBlock = lastInflightBlockBody(body);
  if (!curBlock || !prevBlock || !curBlock.body.startsWith(prevBlock.body)) return false;
  const liveFolds = curEl.querySelectorAll('details.fold-tool-live');
  const pre = liveFolds[liveFolds.length - 1]?.querySelector('.fold-pre');
  if (!pre) return false;
  pre.textContent = curBlock.body;
  return true;
}

function parseAskUserJson(raw) {
  if (raw == null) return null;
  const txt = String(raw).trim();
  if (!txt) return null;
  try { return JSON.parse(txt); } catch (_) {}
  try {
    let out = '';
    let inStr = false;
    let esc = false;
    for (let i = 0; i < txt.length; i++) {
      const c = txt[i];
      if (esc) { out += c; esc = false; continue; }
      if (c === '\\') { out += c; esc = true; continue; }
      if (c === '"') { inStr = !inStr; out += c; continue; }
      if (inStr) {
        if (c === '\n') out += '\\n';
        else if (c === '\r') out += '\\r';
        else if (c === '\t') out += '\\t';
        else if (c.charCodeAt(0) < 0x20) out += '\\u' + c.charCodeAt(0).toString(16).padStart(4, '0');
        else out += c;
      } else out += c;
    }
    return JSON.parse(out);
  } catch (_) {}
  return null;
}

function normalizeAskUserData(data) {
  const raw = data || {};
  const question = String(raw.question || '').trim();
  if (!question) return null;
  const cs = raw.candidates || [];
  const candidates = Array.isArray(cs)
    ? cs.map(x => String(x == null ? '' : x)).filter(x => x.trim())
    : [];
  return { question, candidates };
}

/** 格式化 ask_user 题干：编号与正文同行；无空行时在 2./3. 前分段 */
function formatAskUserQuestion(text) {
  let s = String(text || '').trim();
  if (!s) return s;
  // 「1.\n正文」→「1. 正文」
  s = s.replace(/^(\d+[.、:：)])\s*\n+\s*/gm, '$1 ');
  s = s.replace(/(\n)(\d+[.、:：)])\s*\n+\s*/g, '$1$2 ');
  s = s.replace(/(\n|^)(问题\s*\d+\s*[:：.、)]?)\s*\n+\s*/gi, '$1$2 ');
  // 题与题之间：尚无空行时，仅在 2./3. 前插入空行（不动 1. 与题干）
  if (!/\n\s*\n/.test(s)) {
    s = s.replace(/(\S)\s+(?=问题\s*[2-9]\d*\s*[:：.、)]?\s*)/gi, '$1\n\n');
    s = s.replace(/(\S)\s+(?=[2-9]\d*[.、:：)]\s+\S)/g, '$1\n\n');
  }
  return boldAskQuestionLines(s);
}

function boldAskQuestionLines(text) {
  return String(text || '').split('\n').map(line => {
    const t = line.trim();
    if (!t || /^\*\*.+\*\*$/.test(t)) return line;
    if (/^\d+[.、:：)]\s+\S/.test(t)) return '**' + t + '**';
    if (/^问题\s*\d+/i.test(t)) return '**' + t + '**';
    if (/[？?]\s*$/.test(t) && !/^[A-Da-d][.)]\s/.test(t)) return '**' + t + '**';
    return line;
  }).join('\n');
}

function markAskOptionHtml(html) {
  let out = String(html || '');
  out = out.replace(/<p>([^<]*[A-Da-d][.)]\s[^<]*)<\/p>/gi, '<p class="ask-option-line">$1</p>');
  out = out.replace(/(<br\s*\/?>)\s*([A-Da-d][.)]\s[^<]+)/gi, '<span class="ask-option-line">$2</span>');
  return out;
}

/** 预览模式：true = 始终显示 candidates；false = 题干已含选项/多题时不重复渲染底部列表 */
const ASK_USER_ALWAYS_SHOW_CANDIDATES = false;

/** 题干已含选项/多题，或 candidates 无法与题干对应时，不再重复渲染底部列表 */
function shouldShowAskCandidates(item) {
  if (!item || !item.candidates.length) return false;
  if (ASK_USER_ALWAYS_SHOW_CANDIDATES) return true;
  const q = item.question;
  if (/两个问题|多个问题|两道|两题/.test(q)) return false;
  if ((q.match(/问题\s*\d/gi) || []).length >= 2) return false;
  if ((q.match(/^[ \t]*\d+[.、:：)]\s+/gm) || []).length >= 2) return false;
  if ((q.match(/^[ \t]*[A-Da-d][.)]\s/mg) || []).length >= 2) return false;
  const comboN = item.candidates.filter(c => /\d+[A-Da-d]\s*\+\s*\d+[A-Da-d]/i.test(c)).length;
  if (comboN >= Math.max(1, Math.ceil(item.candidates.length * 0.5))) return false;
  // 题干里有多道问句，却把全部选项平铺在 candidates → 无法区分归属，不展示
  const qMarks = (q.match(/[？?]/g) || []).length;
  if (qMarks >= 2 && item.candidates.length > 4) return false;
  return true;
}


function renderAskUserNotice(data) {
  const item = normalizeAskUserData(data);
  if (!item) return '';
  // 单题与多题统一处理：多题的选项本就内联在题干里；单题的选项放在 candidates 里，
  // 这里把它折叠进题干，按同样的 A./B./C. 内联方式渲染，不再单独画一个编号列表。
  const question = foldAskCandidates(item);
  const qHtml = markAskOptionHtml(renderMarkdown(formatAskUserQuestion(question)));
  return `<div class="ask-user-notice" data-ask-user="1">
    <div class="ask-user-banner">
      <span class="ask-user-banner-text">${escapeHtml(t('ask.banner'))}</span>
      <span class="ask-user-banner-sep" aria-hidden="true">·</span>
      <span class="ask-user-banner-hint">${escapeHtml(t('ask.replyHint'))}</span>
    </div>
    ${qHtml ? `<div class="ask-user-body md">${qHtml}</div>` : ''}
  </div>`;
}

/** 单题的 candidates 折叠进题干（统一成 A./B./C. 内联选项）；多题或无法对应时原样返回题干 */
function foldAskCandidates(item) {
  if (!shouldShowAskCandidates(item)) return item.question;
  const opts = item.candidates.map((c, j) => {
    const label = String(c).replace(/^\s*(?:[A-Za-z]|\d{1,2})\s*[.)、:：]\s*/, '').trim();
    return `${String.fromCharCode(65 + j)}. ${label}`;
  }).join('\n');
  // 用单换行（而非空行）拼进题干，让题干+选项渲染成同一个 <p>，每个选项都跟在 <br> 后面 —
  // 与多题内联选项走完全一致的 .ask-option-line 缩进，避免首项 A 贴左边、B/C/D 缩进的错位。
  return item.question.replace(/\s+$/, '') + '\n' + opts;
}

function askUserPlaceholder(item) {
  // 单题与多题统一：都用自由作答提示，不再针对单题单独显示「输入 1/2/3 选择」
  return t('ask.placeholderOpen');
}

function assistantStructuredText(msg) {
  if (!msg || msg.role !== 'assistant') return '';
  if (Array.isArray(msg.turn_segs) && msg.turn_segs.length) return msg.turn_segs.join('\n');
  return typeof msg.content === 'string' ? msg.content : '';
}

function getPendingAskUser(sess) {
  if (!sess || rt(sess).busy) return null;
  const msgs = sess.messages || [];
  let lastAskIdx = -1;
  let askData = null;
  for (let i = msgs.length - 1; i >= 0; i--) {
    if (msgs[i].role !== 'assistant') continue;
    const json = extractAskUserToolJson(assistantStructuredText(msgs[i]));
    if (json != null) {
      lastAskIdx = i;
      askData = normalizeAskUserData(parseAskUserJson(json));
      break;
    }
  }
  if (!askData) return null;
  const replied = msgs.slice(lastAskIdx + 1).some(m => m.role === 'user');
  return replied ? null : askData;
}

function syncAskUserUi() {
  const sess = activeSess();
  const pending = sess ? getPendingAskUser(sess) : null;
  const notices = [...document.querySelectorAll('.ask-user-notice')];
  notices.forEach((el, i) => {
    const isLast = i === notices.length - 1;
    el.classList.toggle('is-active', !!pending && isLast);
    el.classList.toggle('is-answered', !pending || !isLast);
  });
  if (inputEl) inputEl.setAttribute('data-ph', pending ? askUserPlaceholder(pending) : (_suggestPhActive ? inputEl.getAttribute('data-ph') : t('composer.placeholder')));  // contenteditable 用 data-ph（无 placeholder 属性）
  if (composerEl) composerEl.classList.toggle('is-awaiting-answer', !!pending);
}

/* ═══════════════ 渲染后增强 (PR移植) ═══════════════ */
/* ───────────── 统一复制 SVG Icon ───────────── */
// Phosphor 图标助手：把 window.gaIcon(name) 包一层，给动态渲染的 UI 用，与静态 [data-ga-icon] 保持一致
const GA_ICON = (name, className = '') => (typeof window.gaIcon === 'function' ? window.gaIcon(name, className) : '');
const SVG_COPY_ICON = GA_ICON('copy');
const SVG_CHECK_ICON = GA_ICON('check');

function postRenderEnhance(containerEl) {
  if (!containerEl) return;
  // 代码高亮 + 复制按钮（.code-block 容器已自带头部复制按钮，跳过）
  containerEl.querySelectorAll('pre code').forEach(block => {
    if (typeof hljs !== 'undefined') hljs.highlightElement(block);
    if (block.closest('.code-block')) return; // TUI 风格容器已有复制按钮
    if (!block.parentElement.querySelector('.code-copy-btn')) {
      const btn = document.createElement('button');
      btn.className = 'code-copy-btn'; btn.innerHTML = SVG_COPY_ICON;
      btn.title = t('act.copy');
      btn.onclick = () => {
        navigator.clipboard.writeText(block.textContent).then(() => {
          btn.innerHTML = SVG_CHECK_ICON; setTimeout(() => btn.innerHTML = SVG_COPY_ICON, 1500);
        });
      };
      block.parentElement.style.position = 'relative';
      block.parentElement.appendChild(btn);
    }
  });
  // TUI 代码块头部复制按钮绑定
  containerEl.querySelectorAll('.code-block-copy').forEach(btn => {
    if (btn.dataset.bound) return;
    btn.dataset.bound = '1';
    btn.onclick = () => {
      const code = btn.closest('.code-block').querySelector('code');
      if (!code) return;
      navigator.clipboard.writeText(code.textContent.trim()).then(() => {
        btn.textContent = '\u2713';
        setTimeout(() => { btn.textContent = '\u29C9'; }, 1500);
      });
    };
  });
  // KaTeX 复制按钮
  containerEl.querySelectorAll('.katex-block').forEach(el => {
    if (el.querySelector('.latex-copy-btn')) return;
    const src = el.querySelector('annotation[encoding="application/x-tex"]');
    if (!src) return;
    const btn = document.createElement('button');
    btn.className = 'latex-copy-btn'; btn.textContent = '\u29C9';
    btn.title = t('act.copyTex');
    btn.onclick = () => {
      navigator.clipboard.writeText(src.textContent).then(() => {
        btn.textContent = '\u2713'; setTimeout(() => btn.textContent = '\u29C9', 1500);
      });
    };
    el.style.position = 'relative';
    el.appendChild(btn);
  });
  syncAskUserUi();
}


/* ═══════════════ 状态 ═══════════════ */
const state = {
  sessions: new Map(), activeId: null, bridgeReady: false,
  llmNo: 0, llmNoUserSet: false, modelProfiles: [], modelName: null,
  gaRoot: '',
  runtime: new Map(),
  pendingFiles: [],
  fileSeq: 0,
  convFolders: [],
};
const CONV_FOLDER_KEY = 'ga_conv_folders_v1';
const CONV_FOLDER_COLLAPSE_KEY = 'ga_conv_folder_collapsed_v1';
const DEFAULT_CONV_FOLDER_ID = 'default';
const ARCHIVED_CONV_FOLDER_ID = 'archived';
let convFolderCollapsed = {};
let dragConvSessionId = null;
function defaultConvFolders() {
  return [
    { id: DEFAULT_CONV_FOLDER_ID, name: t('conv.folderDefault'), locked: true },
    { id: ARCHIVED_CONV_FOLDER_ID, name: t('conv.folderArchived'), locked: true },
  ];
}
function normalizeConvFolders(raw) {
  const map = new Map(defaultConvFolders().map(f => [f.id, { ...f }]));
  if (Array.isArray(raw)) {
    raw.forEach(f => {
      if (!f || !f.id) return;
      if (f.id === DEFAULT_CONV_FOLDER_ID || f.id === ARCHIVED_CONV_FOLDER_ID) return;
      const name = String(f.name || '').trim();
      if (!name) return;
      map.set(f.id, { id: f.id, name, locked: false });
    });
  }
  return Array.from(map.values());
}
function loadConvFolders() {
  try { state.convFolders = normalizeConvFolders(JSON.parse(localStorage.getItem(CONV_FOLDER_KEY) || '[]')); }
  catch { state.convFolders = defaultConvFolders(); }
  try { convFolderCollapsed = JSON.parse(localStorage.getItem(CONV_FOLDER_COLLAPSE_KEY) || '{}') || {}; }
  catch { convFolderCollapsed = {}; }
}
function saveConvFolders() {
  const rows = normalizeConvFolders(state.convFolders).map(f => ({ id: f.id, name: f.name, locked: !!f.locked }));
  state.convFolders = rows;
  localStorage.setItem(CONV_FOLDER_KEY, JSON.stringify(rows));
  localStorage.setItem(CONV_FOLDER_COLLAPSE_KEY, JSON.stringify(convFolderCollapsed || {}));
}
function getConvFolderName(id) {
  const hit = state.convFolders.find(f => f.id === (id || DEFAULT_CONV_FOLDER_ID));
  return hit ? hit.name : t('conv.folderDefault');
}
function normalizeSessionFolder(sess) {
  const fid = sess && sess.folderId;
  if (state.convFolders.some(f => f.id === fid)) return fid;
  return DEFAULT_CONV_FOLDER_ID;
}
function assignSessionFolder(sess, folderId) {
  if (!sess) return;
  sess.folderId = state.convFolders.some(f => f.id === folderId) ? folderId : DEFAULT_CONV_FOLDER_ID;
}
function isConvFolderCollapsed(folderId) {
  return folderId !== DEFAULT_CONV_FOLDER_ID && !!convFolderCollapsed[folderId];
}
function toggleConvFolderCollapsed(folderId) {
  if (!folderId || folderId === DEFAULT_CONV_FOLDER_ID) return;
  convFolderCollapsed[folderId] = !isConvFolderCollapsed(folderId);
  saveConvFolders();
}
function createConvFolder(name) {
  const val = String(name || '').trim();
  if (!val) return null;
  const existed = state.convFolders.some(f => f.name.toLowerCase() === val.toLowerCase());
  if (existed) {
    toast(t('conv.folderExists'));
    return null;
  }
  const folder = { id: 'folder-' + Date.now() + '-' + Math.random().toString(16).slice(2, 8), name: val, locked: false };
  state.convFolders.push(folder);
  saveConvFolders();
  return folder;
}
function renameConvFolder(folderId, name) {
  const folder = state.convFolders.find(f => f.id === folderId);
  if (!folder) return false;
  if (folder.locked) { toast(t('conv.folderLocked')); return false; }
  const val = String(name || '').trim();
  if (!val || val === folder.name) return false;
  const existed = state.convFolders.some(f => f.id !== folderId && f.name.toLowerCase() === val.toLowerCase());
  if (existed) { toast(t('conv.folderExists')); return false; }
  folder.name = val;
  saveConvFolders();
  return true;
}
function deleteConvFolder(folderId) {
  const folder = state.convFolders.find(f => f.id === folderId);
  if (!folder) return false;
  if (folder.locked) { toast(t('conv.folderLocked')); return false; }
  state.sessions.forEach(sess => {
    if (normalizeSessionFolder(sess) === folderId) assignSessionFolder(sess, DEFAULT_CONV_FOLDER_ID);
  });
  state.convFolders = state.convFolders.filter(f => f.id !== folderId);
  saveSessions();
  return true;
}
function renderMoveMenu(currentFolderId) {
  if (!moveMenu) return;
  moveMenu.innerHTML = '';
  state.convFolders.forEach(folder => {
    const item = document.createElement('div');
    item.className = 'ctx-item' + (folder.id === currentFolderId ? ' active' : '');
    item.dataset.folderId = folder.id;
    item.innerHTML = `<span data-ga-icon="folder"></span><span>${escapeHtml(folder.name)}</span>`;
    moveMenu.appendChild(item);
  });
  try { applyIcons(moveMenu); } catch (_) {}
}
function rt(sess) {
  let r = state.runtime.get(sess.id);
  if (!r) { r = { polling:false, busy:false, lastId:0, seen:new Set(), draftEl:null, draftSegs:null, draftTurn:0, taskStartedAt:null, taskEndedAt:null, taskTimerId:null, planCompleteAt:null, planLostAt:null, planHoldItems:[], planLastPayload:null, planLastComplete:false, planHideTimer:null, planDismissedComplete:false, planCollapsed:false, planShowAll:false }; state.runtime.set(sess.id, r); }
  return r;
}
const activeSess = () => state.sessions.get(state.activeId) || null;
const isActive = (sess) => sess && sess.id === state.activeId;

function saveSessions() {
  saveConvFolders();
  const byId = {};
  state.sessions.forEach((sess, id) => {
    byId[id] = { folderId: normalizeSessionFolder(sess) };
  });
  localStorage.setItem('ga_session_meta', JSON.stringify(byId));
}
function patchSession(sess, fields) {
  if (!sess.bridgeSessionId) return;
  fetch(`${BRIDGE_ORIGIN}/session/${encodeURIComponent(sess.bridgeSessionId)}`, {
    method: 'PATCH', headers: {'Content-Type':'application/json'}, body: JSON.stringify(fields)
  }).catch(() => {});
}
function isEmptyUntitledSession(sess) {
  if (!sess) return false;
  if ((sess.messages || []).length) return false;
  const title = String(sess.title || '').trim();
  return (sess.untitled ?? true) && (!title || isAutoTitle(title));
}

async function loadSessions() {
  try {
    const res = await fetch(`${BRIDGE_ORIGIN}/sessions`);
    const data = await res.json();
    if (!data.sessions) return;
    const meta = JSON.parse(localStorage.getItem('ga_session_meta') || '{}');
    const remoteSessions = [];
    for (const s of data.sessions) {
      const merged = state.sessions.get(s.id) || {
        id: s.id, bridgeSessionId: s.id, title: s.title,
        messages: [], untitled: s.untitled ?? true,
        pinned: s.pinned ?? false, lastActiveTs: s.updatedAt || s.createdAt,
        updatedAt: s.updatedAt || s.createdAt,
        workspace: s.workspace || '', branch: s.branch || '',
        folderId: meta[s.id]?.folderId || DEFAULT_CONV_FOLDER_ID,
      };
      merged.title = s.title;
      merged.untitled = s.untitled ?? true;
      merged.pinned = s.pinned ?? false;
      merged.lastActiveTs = s.updatedAt || s.createdAt;
      merged.updatedAt = s.updatedAt || s.createdAt;
      merged.workspace = s.workspace || merged.workspace || '';
      merged.branch = s.branch || merged.branch || '';
      merged.project = s.project || merged.project || '';
      if (!merged.bridgeSessionId) merged.bridgeSessionId = s.id;
      merged.folderId = meta[s.id]?.folderId || merged.folderId || DEFAULT_CONV_FOLDER_ID;
      remoteSessions.push(merged);
    }
    const staleIds = remoteSessions.filter(isEmptyUntitledSession).map(s => s.id);
    if (staleIds.length) {
      staleIds.forEach(id => {
        fetch(`${BRIDGE_ORIGIN}/session/${encodeURIComponent(id)}`, { method: 'DELETE' }).catch(() => {});
        state.sessions.delete(id);
        state.runtime.delete(id);
        delete meta[id];
      });
      localStorage.setItem('ga_session_meta', JSON.stringify(meta));
    }
    remoteSessions.filter(s => !staleIds.includes(s.id)).forEach(s => {
      state.sessions.set(s.id, s);
    });
    // 刷新后固定恢复「上次正在看的会话」（前端持久化的 ga_active），而不是 bridge 的
    // activeSessionId（=最近更新的会话，会随后台会话变动而跳来跳去）。没有有效的已存
    // 会话则置空 → 显示「新会话」空态，由用户自己点选。
    const savedActive = localStorage.getItem('ga_active');
    state.activeId = (savedActive && state.sessions.has(savedActive)) ? savedActive : null;
  } catch (_) {}
}

/* ═══════════════ DOM refs ═══════════════ */
const chatPage   = document.querySelector('.page[data-page="chat"]');
const msgArea    = chatPage.querySelector('.msg-area');
const chatStart  = msgArea.querySelector('.chat-start');
const inputEl    = document.getElementById('chat-input');
const sendBtn    = document.getElementById('send-btn');
const planBarEl = document.getElementById('plan-bar');
const composerEl = document.getElementById('chat-composer');
const msgLoading = document.getElementById('msg-loading');
const sessionLoadingEl = document.getElementById('session-loading');
const MIN_MSG_LOADING_MS = 450;
const HYDRATE_LOADING_TIMEOUT_MS = 10000;
const POLL_MSG_LIMIT = 200;
const PLAN_LOST_GRACE_MS = 1500;  // tuiapp_v2._PLAN_LOST_GRACE_SEC
const PLAN_COMPLETE_GRACE_MS = 3000;  // tuiapp_v2._PLAN_GRACE_SEC

function isPlanPresetPrompt(text) {
  const p = String(text || '').toLowerCase();
  return p.includes('plan_sop') || p.includes('plan 模式') || p.includes('plan mode');
}
let _submitInFlight = false;
const runToggle  = document.getElementById('run-toggle');
const chatStatus = pageStatusBar(runToggle);
const runLabel   = runToggle?.querySelector('.rs-label');
const convListEl = document.querySelector('.conv-list');
const newConvBtn = document.querySelector('.new-conv');
const searchInput = document.querySelector('.search input');
const rpResize   = document.getElementById('rp-resize');
const rpPanel    = document.getElementById('rightpanel');
const bodyEl     = document.querySelector('.body');
/* 每个页面的 page-top 各自挂一对 hamburger / 会话 按钮(.pt-sb-toggle / .pt-rp-toggle),
   全部绑同一个 toggle,效果跟以前的单一 sb-toggle/rp-toggle 一样,只是入口变成顶栏。 */
document.querySelectorAll('.pt-sb-toggle').forEach(b => b.addEventListener('click', () => bodyEl.classList.toggle('sb-collapsed')));
document.querySelectorAll('.pt-rp-toggle').forEach(b => b.addEventListener('click', () => bodyEl.classList.toggle('rp-collapsed')));

const sbResize = document.getElementById('sb-resize');
const sbPanel  = document.querySelector('.sidebar');

// 通用拖拽：dir=+1 拖动 →clientX 增大就增宽(左侧栏);dir=-1 反之(右侧)
function bindResize(handle, panel, dir, min, max) {
  if (!handle || !panel) return;
  let dragging = false, startX = 0, startW = 0;
  handle.addEventListener('mousedown', (e) => {
    dragging = true; startX = e.clientX; startW = panel.offsetWidth;
    handle.classList.add('dragging');
    panel.style.transition = 'none';  // 拖拽期间禁用 transition，避免宽度动画延迟
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    e.preventDefault();
  });
  document.addEventListener('mousemove', (e) => {
    if (!dragging) return;
    const w = Math.min(max, Math.max(min, startW + dir * (e.clientX - startX)));
    panel.style.width = w + 'px';
    panel.style.flex = '0 0 ' + w + 'px';
  });
  document.addEventListener('mouseup', () => {
    if (!dragging) return;
    dragging = false;
    handle.classList.remove('dragging');
    panel.style.transition = '';  // 恢复 CSS transition（按钮折叠动画仍生效）
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
  });
}
bindResize(rpResize, rpPanel, -1, 160, 400);  // 右栏:cursor 左移 → 增宽
bindResize(sbResize, sbPanel, +1, 180, 360);  // 左栏:cursor 右移 → 增宽
const modelChip  = document.getElementById('model-chip');
const modelNameEl= modelChip ? modelChip.querySelector('.model-name') : null;
// conductor 页面也有一个独立的模型 chip,共用一份模型数据
const collabModelChip   = document.getElementById('cdb-model-chip');
const collabModelNameEl = collabModelChip ? collabModelChip.querySelector('.model-name') : null;

let msgsEl = null;
function ensureMsgs() {
  if (!msgsEl) {
    msgsEl = document.createElement('div');
    msgsEl.className = 'msgs';
    msgArea.insertBefore(msgsEl, msgLoading || null);
  }
  return msgsEl;
}
function refreshEmptyState(sess) {
  const has = sess && sess.messages.length > 0;
  msgArea.classList.toggle('has-msgs', !!has);
  if (chatStart) chatStart.style.display = has ? 'none' : '';
  if (msgsEl) msgsEl.style.display = has ? '' : 'none';
}

function planTpl(tpl, v) {
  return String(tpl || '').replace(/\{(\w+)\}/g, (_, k) => (v[k] != null ? String(v[k]) : `{${k}}`));
}

let planPollTimer;
function syncPlanPollTimer() {
  const on = !!(activeSess()?.bridgeSessionId && state.bridgeReady);
  if (on && !planPollTimer) {
    planPollTimer = setInterval(() => {
      const s = activeSess();
      if (!s || !isActive(s)) return;
      planFetch(s);
      planTick(s);
    }, 1000);
  } else if (!on && planPollTimer) {
    clearInterval(planPollTimer);
    planPollTimer = null;
  }
}

function clearPlanGrace(r) {
  r.planCompleteAt = r.planLostAt = null;
  r.planHoldItems = [];
  r.planLastPayload = null;
  r.planLastComplete = false;
  r.planDismissedComplete = false;
  if (r.planHideTimer) { clearTimeout(r.planHideTimer); r.planHideTimer = null; }
}

function schedulePlanCompleteDismiss(sess) {
  const r = rt(sess);
  if (r.planHideTimer) clearTimeout(r.planHideTimer);
  r.planHideTimer = setTimeout(() => {
    r.planHideTimer = null;
    r.planDismissedComplete = true;
    if (isActive(sess)) refreshPlanBar(null);
  }, PLAN_COMPLETE_GRACE_MS);
}

/** tuiapp_v2._refresh_planbar：用 runtime 里缓存的 items / placeholder 重绘 */
function refreshPlanBarFromRuntime(sess) {
  const r = rt(sess);
  const lp = r.planLastPayload;
  let items = r.planHoldItems || [];
  if (r.planLostAt != null && Date.now() - r.planLostAt >= PLAN_LOST_GRACE_MS) {
    items = [];
    r.planHoldItems = [];
    r.planLostAt = null;
  }
  if (r.planDismissedComplete) {
    refreshPlanBar(null);
    return;
  }
  if (r.planCompleteAt != null && Date.now() - r.planCompleteAt >= PLAN_COMPLETE_GRACE_MS) {
    r.planDismissedComplete = true;
    refreshPlanBar(null);
    return;
  }
  if (!items.length) {
    if (lp?.active && lp.placeholder) {
      refreshPlanBar(lp);
      return;
    }
    const held = r.planHoldItems || [];
    if (lp?.complete && (lp.items?.length || held.length)) {
      refreshPlanBar({
        active: true,
        placeholder: false,
        items: lp.items?.length ? lp.items : held,
        done: lp.done ?? held.filter(it => it.status === 'done').length,
        total: lp.total ?? (lp.items?.length || held.length),
        complete: true,
        step: lp.step || '',
      });
      return;
    }
    refreshPlanBar(null);
    return;
  }
  refreshPlanBar({
    active: true,
    placeholder: false,
    items,
    done: lp?.done ?? items.filter(it => it.status === 'done').length,
    total: lp?.total ?? items.length,
    complete: !!(lp?.complete || (items.length && items.every(it => it.status === 'done'))),
    step: lp?.step || '',
  });
}

/** 每秒 tick grace（对齐 TUI _poll_plan_files → _refresh_planbar） */
function planTick(sess) {
  if (!sess || !isActive(sess)) return;
  refreshPlanBarFromRuntime(sess);
}

function applyPlanPayload(sess, raw) {
  if (!sess) return;
  const r = rt(sess);
  const now = Date.now();

  if (raw?.active) {
    if (raw.placeholder && !r.planLastPayload?.active) {
      r.planCollapsed = false;
      r.planShowAll = false;
    }
    r.planLastPayload = raw;
    const items = raw.items || [];
    if (items.length) {
      r.planLostAt = null;
      r.planHoldItems = items;
    } else if (!raw.placeholder && !raw.complete && r.planHoldItems.length) {
      if (!r.planLostAt) r.planLostAt = now;
    }
    const nowComplete = !!raw.complete && (items.length > 0 || r.planHoldItems.length > 0);
    const wasComplete = r.planLastComplete;
    if (nowComplete && !wasComplete) {
      r.planCompleteAt = now;
      schedulePlanCompleteDismiss(sess);
    } else if (!nowComplete) {
      r.planCompleteAt = null;
      r.planDismissedComplete = false;
      if (r.planHideTimer) { clearTimeout(r.planHideTimer); r.planHideTimer = null; }
    }
    r.planLastComplete = nowComplete;
  } else if (r.planHoldItems.length && !r.planDismissedComplete) {
    if (!r.planLostAt) r.planLostAt = now;
  } else if (!r.planDismissedComplete) {
    clearPlanGrace(r);
  }

  if (!isActive(sess)) return;
  if (r.planDismissedComplete) {
    refreshPlanBar(null);
    return;
  }
  if (raw?.active && raw.placeholder) {
    refreshPlanBar(raw);
    return;
  }
  if (raw?.active && raw.complete && (raw.items?.length || r.planHoldItems.length)) {
    refreshPlanBar({
      ...raw,
      items: raw.items?.length ? raw.items : r.planHoldItems,
    });
    return;
  }
  refreshPlanBarFromRuntime(sess);
}

function planItemUi(status, isCurrent) {
  const st = String(status || 'open').toLowerCase();
  if (st === 'done') return { cls: 'plan-item--done', mark: '✓' };
  if (st === 'error' || st === 'failed') return { cls: 'plan-item--error', mark: '✕' };
  if (st === 'warn' || st === 'warning') return { cls: 'plan-item--warn', mark: '!' };
  if (isCurrent) return { cls: 'plan-item--current', mark: '●' };
  return { cls: 'plan-item--pending', mark: '○' };
}

function pickPlanWindow(items, stepText) {
  const list = Array.isArray(items) ? items : [];
  if (!list.length) return { shown: [], curInShown: -1, overflow: 0 };
  let cur = list.findIndex(it => it.status !== 'done');
  if (cur < 0) cur = list.length - 1;
  const step = String(stepText || '').trim();
  if (step) {
    const hit = list.findIndex(it => String(it.content || '').includes(step.slice(0, 24)));
    if (hit >= 0) cur = hit;
  }
  let start, end;
  if (cur <= 1) {
    start = cur;
    end = Math.min(list.length, cur + 3);
  } else {
    start = cur - 1;
    end = Math.min(list.length, cur + 2);
  }
  const shown = list.slice(start, end);
  return { shown, curInShown: cur - start, overflow: Math.max(0, list.length - shown.length) };
}

function planCapsuleLabel(plan) {
  const step = plan.step ? String(plan.step).slice(0, 80) : '';
  if (plan.complete) return { tag: t('plan.capsuleComplete'), step: step || planTpl(t('plan.complete'), { n: plan.total }) };
  if (plan.placeholder) return { tag: t('plan.placeholder'), step: '' };
  return { tag: t('plan.capsuleRunning'), step: step || planTpl(t('plan.header'), { done: plan.done, total: plan.total }) };
}

function bindPlanCardUiOnce() {
  if (!planBarEl || planBarEl._planUiBound) return;
  planBarEl._planUiBound = true;
  planBarEl.addEventListener('click', (e) => {
    const sess = activeSess();
    if (!sess) return;
    const r = rt(sess);
    const payload = r.planLastPayload;
    if (!payload?.active) return;
    if (e.target.closest('[data-plan-expand]')) {
      r.planCollapsed = false;
      refreshPlanBar(payload);
    } else if (e.target.closest('[data-plan-collapse]')) {
      r.planCollapsed = true;
      refreshPlanBar(payload);
    } else if (e.target.closest('[data-plan-details]')) {
      r.planShowAll = !r.planShowAll;
      refreshPlanBar(payload);
    }
  });
}

function refreshPlanBar(plan) {
  if (!planBarEl) return;
  bindPlanCardUiOnce();
  if (!plan?.active) {
    planBarEl.hidden = true;
    planBarEl.replaceChildren();
    planBarEl.className = 'plan-card';
    return;
  }
  const sess = activeSess();
  const r = sess ? rt(sess) : { planCollapsed: false, planShowAll: false };
  const collapsed = !!r.planCollapsed;
  const stepText = plan.step ? String(plan.step).slice(0, 120) : '';
  const done = plan.done ?? (plan.items || []).filter(it => it.status === 'done').length;
  const total = plan.total ?? (plan.items || []).length;
  const mod = [
    'plan-card',
    collapsed ? 'plan-card--collapsed' : 'plan-card--expanded',
    plan.complete ? 'plan-card--complete' : '',
    plan.placeholder ? 'plan-card--placeholder' : '',
  ].filter(Boolean).join(' ');
  planBarEl.hidden = false;
  planBarEl.className = mod;

  if (collapsed) {
    const cap = planCapsuleLabel(plan);
    planBarEl.innerHTML = '';
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'plan-capsule';
    btn.dataset.planExpand = '1';
    const dot = document.createElement('span');
    dot.className = 'plan-status-dot';
    const txt = document.createElement('span');
    txt.className = 'plan-capsule-text';
    if (cap.step) txt.innerHTML = `${escapeHtml(cap.tag)} · <em>${escapeHtml(cap.step)}</em>`;
    else txt.textContent = cap.tag;
    btn.append(dot, txt);
    planBarEl.append(btn);
    return;
  }

  const frag = document.createDocumentFragment();
  const head = document.createElement('div');
  head.className = 'plan-card-head';
  const dot = document.createElement('span');
  dot.className = 'plan-status-dot';
  const title = document.createElement('span');
  title.className = 'plan-title';
  title.textContent = plan.placeholder ? t('plan.placeholder')
    : plan.complete ? t('plan.completeTitle')
    : t('plan.running');
  head.append(dot, title);
  if (!plan.placeholder && total > 0) {
    const prog = document.createElement('span');
    prog.className = 'plan-progress';
    prog.textContent = `${done}/${total}`;
    head.append(prog);
  }
  const actions = document.createElement('div');
  actions.className = 'plan-head-actions';
  const collapseBtn = document.createElement('button');
  collapseBtn.type = 'button';
  collapseBtn.className = 'plan-btn';
  collapseBtn.dataset.planCollapse = '1';
  collapseBtn.textContent = t('plan.collapse');
  actions.append(collapseBtn);
  head.append(actions);
  frag.append(head);

  if (stepText) {
    const cur = document.createElement('div');
    cur.className = 'plan-current';
    const lab = document.createElement('span');
    lab.className = 'plan-current-label';
    lab.textContent = `${t('plan.current')}：`;
    const body = document.createElement('span');
    body.className = 'plan-current-text';
    body.textContent = stepText;
    cur.append(lab, body);
    frag.append(cur);
  }

  if (plan.placeholder) {
    const wait = document.createElement('div');
    wait.className = 'plan-wait';
    wait.textContent = planTpl(t('plan.waiting'), { path: plan.pathHint || 'plan.md' });
    frag.append(wait);
  } else {
    const list = plan.items || [];
    const { shown, curInShown, overflow } = r.planShowAll
      ? { shown: list, curInShown: list.findIndex(it => it.status !== 'done'), overflow: 0 }
      : pickPlanWindow(list, stepText);
    if (shown.length) {
      const ul = document.createElement('ul');
      ul.className = 'plan-items';
      shown.forEach((it, i) => {
        const ui = planItemUi(it.status, i === curInShown);
        const li = document.createElement('li');
        li.className = 'plan-item ' + ui.cls;
        const mark = document.createElement('span');
        mark.className = 'plan-item-mark';
        mark.textContent = ui.mark;
        const txt = document.createElement('span');
        txt.className = 'plan-item-text';
        txt.textContent = it.content || '';
        li.append(mark, txt);
        ul.append(li);
      });
      frag.append(ul);
    }
    const foot = document.createElement('div');
    foot.className = 'plan-foot';
    const moreN = r.planShowAll ? 0 : (overflow || Math.max(0, list.length - shown.length));
    if (moreN > 0) {
      const hint = document.createElement('span');
      hint.className = 'plan-more-hint';
      hint.textContent = planTpl(t('plan.overflow'), { n: moreN });
      foot.append(hint);
    }
    if (list.length > 3) {
      const det = document.createElement('button');
      det.type = 'button';
      det.className = 'plan-btn';
      det.dataset.planDetails = '1';
      det.textContent = r.planShowAll ? t('plan.collapse') : t('plan.details');
      foot.append(det);
    }
    if (foot.childNodes.length) frag.append(foot);
  }
  planBarEl.replaceChildren(frag);
}

async function planFetch(sess) {
  if (!sess?.bridgeSessionId || !state.bridgeReady || !isActive(sess)) return;
  try {
    const res = await fetch(`${BRIDGE_ORIGIN}/session/${encodeURIComponent(sess.bridgeSessionId)}/plan`);
    if (!res.ok) throw new Error(`plan ${res.status}`);
    const data = await res.json();
    applyPlanPayload(sess, data.plan ?? data.result?.plan);
  } catch (_) { /* 对齐 TUI：读盘/网络失败不立刻清空条 */ }
}

async function planPoll(sess) {
  await planFetch(sess);
  planTick(sess);
}

/* ═══════════════ 消息渲染 ═══════════════ */
function stripAttachPlaceholders(text) {
  return String(text || '').replace(/\[(Image|File)\s+#\d+\]\s*/g, '').trim();
}
// 把消息文本里的 [Image #N]/[File #N] 占位符“按原位置”渲染成内联 chip(显示文件名),其余文本转义+换行,
// 这样消息里能看到附件在文本中的位置(卡片/缩略图照常另外渲染)。lookup(kind,n) 取该附件文件名。
// 同时兜底去掉内联的本地上传路径(历史/conductor 回显)。
function renderMsgTextWithChips(text, lookup) {
  const s = String(text || '').replace(/[^\s]*desktop_uploads[^\s]*\s*/g, '');
  const esc = t2 => escapeHtml(t2).replace(/\n/g, '<br>');
  const re = /\[(Image|File)\s+#(\d+)\]/g;
  let out = '', last = 0, m;
  while ((m = re.exec(s))) {
    out += esc(s.slice(last, m.index));
    const name = (lookup && lookup(m[1], Number(m[2]))) || (m[1] === 'Image' ? 'image' : 'file');
    out += `<span class="ph-chip" contenteditable="false">${escapeHtml(name)}</span>`;
    last = re.lastIndex;
  }
  out += esc(s.slice(last));
  return out.trim();
}
function fileSubLabel(name) {
  const m = String(name || '').match(/\.([^.]+)$/);
  if (!m) return t('file.kindGeneric');
  const ext = m[1].toLowerCase();
  const docExts = ['pdf', 'doc', 'docx', 'rtf', 'odt', 'pages', 'tex'];
  const sheetExts = ['xls', 'xlsx', 'csv', 'tsv', 'numbers', 'ods'];
  const slideExts = ['ppt', 'pptx', 'key', 'odp'];
  const codeExts = ['py', 'js', 'ts', 'tsx', 'jsx', 'java', 'c', 'cpp', 'h', 'hpp', 'rs', 'go', 'rb', 'php', 'sh', 'html', 'css', 'json', 'yaml', 'yml', 'xml', 'sql', 'md'];
  const archiveExts = ['zip', 'tar', 'gz', 'rar', '7z', 'bz2'];
  const audioExts = ['mp3', 'wav', 'flac', 'aac', 'ogg', 'm4a'];
  const videoExts = ['mp4', 'mov', 'avi', 'mkv', 'webm', 'wmv'];
  if (docExts.includes(ext)) return t('file.kindDoc');
  if (sheetExts.includes(ext)) return t('file.kindSheet');
  if (slideExts.includes(ext)) return t('file.kindSlide');
  if (codeExts.includes(ext)) return t('file.kindCode') + ' · ' + ext.toUpperCase();
  if (archiveExts.includes(ext)) return t('file.kindArchive');
  if (audioExts.includes(ext)) return t('file.kindAudio');
  if (videoExts.includes(ext)) return t('file.kindVideo');
  return ext.toUpperCase();
}
/** 无 turn_segs 时按 LLM Running 标记切轮（与 stapp.fold_turns 同源） */
function splitContentTurnSegs(content) {
  const src = String(content || '');
  const parts = src.split(/\**LLM Running \(Turn \d+\) \.\.\.\**/);
  const segs = [];
  for (let i = 1; i < parts.length; i++) {
    const body = parts[i] || '';
    if (body.length || segs.length) segs.push(body);
  }
  if (segs.length > 1) return segs;
  return src.length ? [src] : [];
}

function assistantTurnSegs(msg) {
  if (Array.isArray(msg?.turn_segs) && msg.turn_segs.length) return msg.turn_segs;
  if (typeof msg?.content === 'string' && msg.content.length) return splitContentTurnSegs(msg.content);
  return [];
}

/** turn_segs 未就绪时回退 content（双轨兼容，不改 ljq 渲染主路径） */
function draftSegsFromPartial(raw, m) {
  const segs = Array.isArray(m.turn_segs) ? m.turn_segs : [];
  if (segs.length && segs.some(s => (s || '').length > 0)) return segs;
  const content = typeof raw.content === 'string' ? raw.content : (m.content || '');
  return content ? [content] : segs;
}
function firstNonEmptyTurnIndex(segs) {
  const arr = Array.isArray(segs) ? segs : [];
  for (let i = 0; i < arr.length; i++) {
    if ((arr[i] || '').length > 0) return i;
  }
  return 0;
}
// 后端 curr_turn 是内部 0-based 下标。展示时优先当前 turn；若该 turn 为空，回退到最后一个非空 turn。
function resolveVisibleTurnIndex(segs, preferredTurn) {
  const arr = Array.isArray(segs) ? segs : [];
  const first = firstNonEmptyTurnIndex(arr);
  const preferred = Number.isFinite(Number(preferredTurn)) ? Number(preferredTurn) : -1;
  if (preferred >= 0 && (arr[preferred] || '').length > 0) return preferred;
  for (let i = arr.length - 1; i >= 0; i--) {
    if ((arr[i] || '').length > 0) return i;
  }
  return Math.max(first, preferred >= 0 ? preferred : (arr.length ? arr.length - 1 : first));
}
// 静态完整渲染：visibleTurn 之前的 seg 固化折叠；visibleTurn 作为当前正文展示。
function renderAssistantTurnsHtml(segs, currTurn, withCursor = false) {
  const arr = Array.isArray(segs) ? segs : [];
  if (!arr.length) return '';
  const first = firstNonEmptyTurnIndex(arr);
  const curr = resolveVisibleTurnIndex(arr, currTurn);
  let html = '';
  for (let i = first; i < curr; i++) {
    if ((arr[i] || '').length > 0) html += `<div class="turn-frozen" data-turn="${i}">${renderTurnFold(arr[i] || '', i)}</div>`;
  }
  html += `<div class="turn-cur" data-turn="${curr}">${renderTurnBody(arr[curr] || '')}${withCursor ? '<span class="cursor"></span>' : ''}</div>`;
  return html;
}
function assistantCopyText(msg) {
  const segs = assistantTurnSegs(msg);
  if (segs.length) {
    // 复制保持旧行为：只复制当前/最后可见 turn，不复制已折叠历史 turn。
    const turn = resolveVisibleTurnIndex(segs, msg?.curr_turn);
    return stripTurnMarker(segs[turn] || '').replace(/<summary>[\s\S]*?<\/summary>\s*/i, '').trim();
  }
  return '';
}
function msgNode(msg) {
  const el = document.createElement('div');
  el.className = 'msg ' + (msg.role || 'system');
  if (msg.role === 'user') {
    const shown = (typeof msg.display === 'string' && msg.display.length) ? msg.display : msg.content;
    const imgsHtml = (msg.images && msg.images.length)
      ? `<div class="user-imgs">${msg.images.map(im => `<img src="${im.dataUrl || uploadRawUrl(im.path)}" data-path="${escapeHtml(im.path || '')}" alt="">`).join('')}</div>`
      : '';
    const filesHtml = (msg.files && msg.files.length)
      ? `<div class="user-files">${msg.files.map(f => {
          const name = f.name || 'file';
          const sub = fileSubLabel(name);
          return `<div class="file-chip" data-path="${escapeHtml(f.path || '')}" data-name="${escapeHtml(name)}"><span class="fc-icon">${GA_ICON('fileText')}</span><span class="fc-meta"><span class="fc-name">${escapeHtml(name)}</span><span class="fc-sub">${escapeHtml(sub)}</span></span></div>`;
        }).join('')}</div>`
      : '';
    const chipText = renderMsgTextWithChips(shown, (kind, n) => {
      const hit = (kind === 'Image' ? (msg.images || []) : (msg.files || [])).find(x => x.id === 'f-' + n);
      return hit && (hit.name || '');
    });
    const textHtml = chipText ? `<div class="bubble">${chipText}</div>` : '';
    el.innerHTML = `<div class="user-stack">${filesHtml}${imgsHtml}${textHtml}</div>`;
  }
  else if (msg.role === 'assistant') {
    const segs = assistantTurnSegs(msg);
    let html = renderAssistantTurnsHtml(segs, msg.curr_turn, false);
    if (msg.stopped) html += `<p><em>[${escapeHtml(t('status.stopped'))}]</em></p>`;
    el.innerHTML = `<div class="bubble md">${html}</div>`;
    postRenderEnhance(el.querySelector('.bubble'));
  }
  else if (msg.role === 'error') el.innerHTML = `<div class="bubble err">${escapeHtml(msg.content)}</div>`;
  else el.innerHTML = `<div class="bubble sys">${escapeHtml(msg.content)}</div>`;
  if (msg.role === 'user' || msg.role === 'assistant') {
    const copyBtn = document.createElement('button');
    copyBtn.className = 'bubble-copy-btn';
    copyBtn.title = t('act.copy');
    copyBtn.innerHTML = SVG_COPY_ICON;
    copyBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      const text = (msg.role === 'user')
        ? stripAttachPlaceholders((typeof msg.display === 'string' && msg.display.length) ? msg.display : (msg.content || ''))
        : assistantCopyText(msg);
      navigator.clipboard.writeText(text).then(() => {
        copyBtn.innerHTML = SVG_CHECK_ICON;
        setTimeout(() => { copyBtn.innerHTML = SVG_COPY_ICON; }, 1500);
      });
    });
    el.appendChild(copyBtn);
  }
  return el;
}
function collabItemToMsg(item) {
  const attach = arr => (arr || []).map(x => {
    const sid = x.sid != null ? x.sid : (String(x.id || '').startsWith('f-') ? String(x.id).slice(2) : x.id);
    return { id: 'f-' + sid, name: x.name, path: x.path, dataUrl: x.dataUrl };
  });
  if (item.role === 'user') {
    return { role: 'user', content: item.msg, display: item.msg, images: attach(item.images), files: attach(item.files) };
  }
  if (item.role === 'conductor') return { role: 'assistant', turn_segs: [item.msg || ''], curr_turn: 0 };
  if (item.role === 'error') return { role: 'error', content: item.msg || '' };
  return { role: 'system', content: item.msg || '' };
}
function renderAllMessages(sess) {
  const box = ensureMsgs(); box.innerHTML = '';
  for (const m of sess.messages) box.appendChild(msgNode(m));
  syncAskUserUi();
  // badge 恢复在 pollSession finally 中执行（此时 messages 已通过异步加载填充）
  refreshEmptyState(sess); scrollBottom(true);
}
// 遍历消息对，用 ts 差值恢复 badge；对运行中任务恢复 taskStartedAt
function restoreElapsedBadges(sess, box) {
  const msgs = sess.messages;
  if (!msgs || !msgs.length) return;
  const nodes = box.querySelectorAll('.msg');
  let lastUserTs = null;
  for (let i = 0; i < msgs.length; i++) {
    if (msgs[i].role === 'user') {
      lastUserTs = msgs[i].ts ? msgs[i].ts * 1000 : null; // 无 ts 则重置
    } else if (msgs[i].role === 'assistant') {
      if (lastUserTs && msgs[i].ts) {
        const elapsed = msgs[i].ts * 1000 - lastUserTs;
        if (elapsed > 0 && nodes[i]) {
          ensureTaskElapsedBadge(nodes[i], lastUserTs, msgs[i].ts * 1000);
        }
      }
      lastUserTs = null;
    }
  }
  // 运行中任务：最后一条是 user 且 session busy，恢复实时计时
  if (lastUserTs && rt(sess).busy) {
    const r = rt(sess);
    r.taskStartedAt = lastUserTs;
    r.taskEndedAt = null;
  }
}
function appendMessage(sess, msg) {
  if (!isActive(sess)) return;
  const el = msgNode(msg);
  ensureMsgs().appendChild(el);
  if (msg.role === 'assistant') {
    const r = rt(sess);
    if (r.taskStartedAt) {
      ensureTaskElapsedBadge(el, r.taskStartedAt, r.taskEndedAt || Date.now());
      r.taskStartedAt = null; r.taskEndedAt = null;
    }
  }
  refreshEmptyState(sess); scrollBottom(true);
  if (msg.role === 'assistant' || msg.role === 'user') syncAskUserUi();
}
function isNearBottom(threshold = 80) {
  return msgArea.scrollHeight - msgArea.scrollTop - msgArea.clientHeight < threshold;
}
function scrollBottom(force) {
  if (force || isNearBottom()) {
    requestAnimationFrame(() => { msgArea.scrollTop = msgArea.scrollHeight; });
  }
}
/* ═══════════════ 打字机效果 (PR移植) ═══════════════ */
const TW_SPEED = 10;  // 逐字步长；paintDraft 只重绘当前轮
const TW_INTERVAL = 35; // ms
const TW_CATCHUP_THRESHOLD = 480; // 积压过大时加速追平
const TW_CATCHUP_MULTIPLIER = 8;
const TW_RECOVER_MIN = 1200;  // 保留常量；刷新恢复改由 snapDraftRecover 一律对齐
const DRAFT_INTERACT_MS = 520; // 用户滚代码块/点折叠时暂缓 DOM 重写

function isDraftInteractFrozen(r) {
  return Date.now() < (r.draftFreezeUntil || 0);
}
function armDraftInteractFreeze(r, ms = DRAFT_INTERACT_MS) {
  r.draftFreezeUntil = Math.max(r.draftFreezeUntil || 0, Date.now() + ms);
}
function snapshotDraftScroll(root) {
  if (!root) return [];
  return [...root.querySelectorAll('.bubble .code-block pre, .bubble .fold-pre')].map(n => n.scrollTop);
}
function restoreDraftScroll(root, tops) {
  if (!root || !tops.length) return;
  const nodes = root.querySelectorAll('.bubble .code-block pre, .bubble .fold-pre');
  tops.forEach((top, i) => { if (nodes[i] && top > 0) nodes[i].scrollTop = top; });
}
function bindDraftInteractGuard(el, r) {
  if (!el || el.dataset.gaDraftGuard) return;
  el.dataset.gaDraftGuard = '1';
  const arm = () => { if (!r._suppressToggleFreeze) armDraftInteractFreeze(r); };
  el.addEventListener('mousedown', (e) => {
    if (e.target.closest('details summary, .code-block pre, .fold-pre')) armDraftInteractFreeze(r);
  }, true);
  el.addEventListener('wheel', (e) => {
    if (e.target.closest('.code-block pre, .fold-pre')) armDraftInteractFreeze(r);
  }, { capture: true, passive: true });
  el.addEventListener('toggle', (e) => {
    if (e.target.matches('details')) arm();
  }, true);
}
function resetTypewriterState(r) {
  if (r.twState?.timer) clearInterval(r.twState.timer);
  r.twState = null;
  r.draftRecoverPending = false;
  r.draftStreamBaseline = 0;
  r._draftPaintBody = '';
}

/** 刷新/hydrate/重连：已有 partial 一次性对齐全文并单帧绘制，不重播打字机 */
function snapDraftRecover(r) {
  if (!r.draftRecoverPending || !r.draftEl) return false;
  if (!r.twState) r.twState = { turn: 0, shown: 0, timer: null };
  const tw = r.twState;
  if (tw.timer) { clearInterval(tw.timer); tw.timer = null; }
  const segs = Array.isArray(r.draftSegs) ? r.draftSegs : [];
  const turn = Math.max(0, Number(r.draftTurn) || 0);
  tw.turn = turn;
  tw.shown = (segs[turn] || '').length;
  r.draftRecoverPending = false;
  r.draftStreamBaseline = tw.shown;
  r._draftPaintBody = '';
  ensureDraftFrozenThrough(r, turn);
  paintDraft(r, turn, segs[turn] || '');
  return true;
}

function renderDraft(sess) {
  const r = rt(sess);
  if (!isActive(sess)) return;
  const box = ensureMsgs();
  if (!r.draftEl || r.draftEl.parentNode !== box) {
    r.draftEl = document.createElement('div'); r.draftEl.className = 'msg assistant'; box.appendChild(r.draftEl);
    bindDraftInteractGuard(r.draftEl, r);
    if (r.taskStartedAt) ensureTaskElapsedBadge(r.draftEl, r.taskStartedAt, null);
  }
  if (!r.twState) r.twState = { turn: 0, shown: 0, timer: null };
  const tw = r.twState;
  const backendTurn = Math.max(0, Number(r.draftTurn) || 0);
  if (tw.turn == null || tw.turn < 0) tw.turn = 0;
  if (tw.turn > backendTurn) tw.turn = backendTurn;
  // 流式渲染模型：segs 是源数据；tw.turn 是正在打字的 turn；tw.turn 之前的 DOM 一旦 frozen 就不再动。
  ensureDraftFrozenThrough(r, tw.turn);

  const tick = () => {
    const segs = Array.isArray(r.draftSegs) ? r.draftSegs : [];
    const backendTurnNow = Math.max(0, Number(r.draftTurn) || 0);
    if (tw.turn == null || tw.turn < 0) tw.turn = 0;
    if (tw.turn > backendTurnNow) tw.turn = backendTurnNow;
    r.streamTurn = tw.turn;
    const cur = segs[tw.turn] || '';

    // 当前 turn 打完，并且后端已进入后续 turn：当前 DOM 固化，然后打字机推进到下一 turn。
    if (tw.shown >= cur.length && backendTurnNow > tw.turn) {
      paintDraft(r, tw.turn, cur);
      freezeCurrentTurnDom(r, tw.turn);
      tw.turn += 1;
      tw.shown = 0;
      r.streamTurn = tw.turn;
      return;
    }

    if (tw.shown >= cur.length) return; // 中途不停止 timer，等新 partial/done。
    if (isDraftInteractFrozen(r)) return;

    const t0 = performance.now();
    const backlog = cur.length - tw.shown;
    let step = backlog > TW_CATCHUP_THRESHOLD ? TW_SPEED * TW_CATCHUP_MULTIPLIER : TW_SPEED;
    const last = tw.lastElapsed || 0;
    if (last > 60) step = Math.max(step, Math.ceil(backlog / 3));
    else step = Math.min(step, 120);
    tw.shown = Math.min(tw.shown + step, cur.length);
    paintDraft(r, tw.turn, cur.slice(0, tw.shown));
    tw.lastElapsed = performance.now() - t0;
  };

  if (snapDraftRecover(r)) {
    if (!tw.timer) tw.timer = setInterval(tick, TW_INTERVAL);
    refreshEmptyState(sess);
    return;
  }

  if (!tw.timer) tw.timer = setInterval(tick, TW_INTERVAL);
  if (!isDraftInteractFrozen(r)) tick();
  refreshEmptyState(sess);
}

function ensureDraftFrozenThrough(r, currTurn) {
  if (!r.draftEl) return;
  const segs = Array.isArray(r.draftSegs) ? r.draftSegs : [];
  const upto = Math.max(0, Number(currTurn) || 0);
  let bubble = r.draftEl.querySelector(':scope > .bubble.md');
  if (!bubble) {
    bubble = document.createElement('div');
    bubble.className = 'bubble md';
    r.draftEl.appendChild(bubble);
  }
  const cur = bubble.querySelector(':scope > .turn-cur');
  for (let turn = 0; turn < upto; turn++) {
    if (bubble.querySelector(`:scope > .turn-frozen[data-turn="${turn}"]`)) continue;
    const frozen = document.createElement('div');
    frozen.className = 'turn-frozen';
    frozen.dataset.turn = turn;
    frozen.innerHTML = renderTurnFold(segs[turn] || '', turn);
    if (cur) bubble.insertBefore(frozen, cur);
    else bubble.appendChild(frozen);
    postRenderEnhance(frozen);
  }
}

function freezeCurrentTurnDom(r, turn) {
  if (!r.draftEl) return;
  const bubble = r.draftEl.querySelector(':scope > .bubble.md');
  if (!bubble) return;
  const cur = bubble.querySelector(':scope > .turn-cur');
  if (!cur) return;
  cur.className = 'turn-frozen';
  cur.dataset.turn = turn;
  cur.innerHTML = renderTurnFold((r.draftSegs || [])[turn] || '', turn);
  postRenderEnhance(cur);
}

function paintDraft(r, turn, visibleCurrBody) {
  if (!r.draftEl || isDraftInteractFrozen(r)) return;
  const wasNear = isNearBottom();
  const forceBottom = !!r.justSwitched;  // 切换会话后首次 draft：强制滚到底
  if (forceBottom) r.justSwitched = false;
  let bubble = r.draftEl.querySelector(':scope > .bubble.md');
  if (!bubble) {
    bubble = document.createElement('div');
    bubble.className = 'bubble md';
    r.draftEl.appendChild(bubble);
  }
  let cur = bubble.querySelector(':scope > .turn-cur');
  if (!cur) {
    cur = document.createElement('div');
    cur.className = 'turn-cur';
    bubble.appendChild(cur);
  }
  cur.dataset.turn = turn ?? 0;
  const body = visibleCurrBody || '';
  const prevBody = r._draftPaintBody || '';
  if (!tryPatchInflightToolDom(cur, body, prevBody)) {
    cur.innerHTML = renderTurnBody(body) + '<span class="cursor"></span>';
    postRenderEnhance(cur);
  } else if (!cur.querySelector('.cursor')) {
    cur.insertAdjacentHTML('beforeend', '<span class="cursor"></span>');
  }
  r._draftPaintBody = body;

  if (wasNear || forceBottom) {
    const inCodeScroll = document.activeElement?.closest?.('.code-block pre, .fold-pre')
      && r.draftEl.contains(document.activeElement);
    if (!inCodeScroll) scrollBottom(forceBottom);
  }
}

function flushTypewriter(sess) {
  resetTypewriterState(rt(sess));
}

/* ═══════════════ 运行状态 ═══════════════ */
function pageStatusBar(btnEl) {
  const label = btnEl?.querySelector('.rs-label');
  return {
    /** state: 'ready' | 'busy' | 'offline' | 'connecting'；兼容旧调用 set(text, true) */
    set(text, state = 'ready') {
      if (!btnEl) return;
      const mode = state === true ? 'busy' : (state === false ? 'ready' : state);
      btnEl.classList.remove('busy', 'offline', 'connecting');
      if (mode === 'busy') btnEl.classList.add('busy');
      else if (mode === 'offline') btnEl.classList.add('offline');
      else if (mode === 'connecting') btnEl.classList.add('connecting');
      if (label) label.textContent = text ?? '';
    },
    setBusy(text) { this.set(text, 'busy'); },
    setReady() { this.set(t('status.ready'), 'ready'); },
    setDisconnected() { this.set(t('status.disconnected'), 'offline'); },
    setConnecting() { this.set(t('status.connecting'), 'connecting'); },
  };
}
function refreshStatusLabel() {
  const s = activeSess();
  if (s && rt(s).busy) {
    chatStatus.setBusy(formatTaskElapsed(Date.now() - (rt(s).taskStartedAt || Date.now())));
  } else if (state.bridgeReady) {
    chatStatus.setReady();
  } else {
    chatStatus.setDisconnected();
  }
}

/* ═══════════════ 消息计时 ═══════════════ */
function formatTaskElapsed(ms) {
  const v = Number(ms);
  if (!Number.isFinite(v) || v < 0) return '';
  const sec = Math.round(v / 1000);
  if (sec < 60) return t('timing.elapsed').replace('{t}', `${Math.max(1, sec)}s`);
  const min = Math.floor(sec / 60), s = sec % 60;
  if (min < 60) return t('timing.elapsed').replace('{t}', `${min}m ${s}s`);
  const hr = Math.floor(min / 60), m = min % 60;
  return t('timing.elapsed').replace('{t}', `${hr}h ${m}m`);
}

function ensureTaskElapsedBadge(wrap, startedAt, endedAt) {
  if (!wrap || !startedAt) return null;
  let badge = wrap.querySelector(':scope > .task-elapsed');
  if (!badge) {
    badge = document.createElement('div');
    badge.className = 'task-elapsed';
    wrap.prepend(badge);
  }
  const elapsed = (endedAt || Date.now()) - startedAt;
  badge.textContent = formatTaskElapsed(elapsed);
  badge.dataset.live = endedAt ? '' : '1';
  return badge;
}

function startTaskTimer(sess) {
  const r = rt(sess);
  if (r.taskStartedAt) return;  // 已在计时，不重置
  // 优先从消息时间戳恢复（刷新后持久化）
  const msgs = sess.messages;
  let restored = 0;
  if (msgs && msgs.length) {
    for (let i = msgs.length - 1; i >= 0; i--) {
      if (msgs[i].role === 'user' && msgs[i].ts) { restored = msgs[i].ts * 1000; break; }
    }
  }
  r.taskStartedAt = restored || Date.now();
  r.taskEndedAt = null;
  if (r.taskTimerId) clearInterval(r.taskTimerId);
  r.taskTimerId = setInterval(() => {
    if (!r.taskStartedAt) return;
    const el = r.draftEl || document.querySelector('.msg-list .msg.assistant:last-child');
    if (el) ensureTaskElapsedBadge(el, r.taskStartedAt, null);
    // 更新左上角状态栏显示实时耗时
    if (isActive(sess)) {
      chatStatus.setBusy(formatTaskElapsed(Date.now() - r.taskStartedAt));
    }
  }, 1000);
}

function stopTaskTimer(sess) {
  const r = rt(sess);
  if (r.taskTimerId) { clearInterval(r.taskTimerId); r.taskTimerId = null; }
  if (!r.taskStartedAt) return;
  r.taskEndedAt = Date.now();
}

// 会话完成通知：页面端用浏览器 Notification API，app 端（Tauri WebView）同样走系统通知中心
async function notifyComplete(sess) {
  try {
    const title = sess?.title || (sess?.untitled ? t('conv.untitled') : t('conv.untitled'));
    const body = t('notify.done') || '对话已完成';
    // app 端（Tauri）优先尝试原生 invoke 通知命令（若后端注册了的话），否则降级 Web Notification
    const invoke = window.__TAURI__?.core?.invoke;
    if (invoke) {
      try {
        await invoke('notify', { title: String(title), body: String(body) });
        return;
      } catch (_) { /* 未注册 notify 命令则降级 */ }
    }
    // Web Notification API（浏览器 + Tauri WebView 均支持，走系统通知中心）
    if (!('Notification' in window)) return;
    if (Notification.permission === 'granted') {
      new Notification(String(title), { body: String(body) });
    } else if (Notification.permission !== 'denied') {
      const perm = await Notification.requestPermission();
      if (perm === 'granted') new Notification(String(title), { body: String(body) });
    }
  } catch (_) {}
}
function setBusy(sess, busy) {
  const r = rt(sess);
  const wasBusy = r.busy;
  if (wasBusy && !busy) resetTypewriterState(r);
  r.busy = busy;
  if (busy) startTaskTimer(sess); else stopTaskTimer(sess);
  // 完成转换(busy→idle)且当前会话非活跃(用户切走了) → 发系统通知
  if (wasBusy && !busy && !isActive(sess)) notifyComplete(sess);
  // 完成转换(busy→idle)且当前会话活跃 → 自动推荐下一步动作
  if (wasBusy && !busy && isActive(sess)) fetchSuggestions(sess);
  if (!isActive(sess)) return;
  if (busy) {
    chatStatus.setBusy(formatTaskElapsed(Date.now() - (r.taskStartedAt || Date.now())));
  } else if (state.bridgeReady) {
    chatStatus.setReady();
  } else {
    chatStatus.setDisconnected();
  }
  if (sendBtn) {
    sendBtn.classList.toggle('is-stop', busy);
    sendBtn.setAttribute('aria-label', busy ? t('act.stop') : t('act.send'));
    sendBtn.title = busy ? t('act.stop') : '';
  }
}
// run-toggle 现为纯状态展示组件：运行中转红，不再响应点击（停止改由发送键的录制键承担）

/* ═══════════════ 自动推荐下一步动作 ═══════════════ */
let _suggestToken = 0;
// 当为 true 时，输入框 data-ph 显示的是建议文字，syncAskUserUi 不应覆盖回默认 placeholder。
// 用户开始输入后会被清除（见 input 事件），切走会话也会清除。
let _suggestPhActive = false;
async function fetchSuggestions(sess) {
  const r = rt(sess);
  const sid = sess.bridgeSessionId || sess.id;
  if (!sid) return;
  const myToken = ++_suggestToken;
  try {
    const res = await fetch(`${BRIDGE_ORIGIN}/session/${encodeURIComponent(sid)}/suggest`, { method: 'POST' });
    if (!res.ok) return;
    const data = await res.json();
    // 过期的请求丢弃
    if (myToken !== _suggestToken) return;
    // 仅当前活跃会话才渲染
    if (!isActive(sess)) return;
    renderSuggestions(data.suggestions || []);
  } catch (_) { /* 静默失败 */ }
}
function renderSuggestions(items) {
  const inset = document.querySelector('#chat-composer .composer-inset');
  if (!inset) return;
  // 移除旧版 suggest-bar（若残留）
  let bar = inset.querySelector('.suggest-bar');
  if (bar) bar.remove();
  if (!items || !items.length) {
    _suggestPhActive = false;
    return;
  }
  // 将建议文字设为输入框的 placeholder（灰色提示，用户输入后自动消失）
  const phText = items[0];
  if (inputEl) {
    inputEl.setAttribute('data-ph', phText);
    _suggestPhActive = true;
    // 确保输入框为空，使 :empty::before 生效显示 placeholder
    if (!inputEl.textContent.trim()) inputEl.focus();
  }
}

/* ═══════════════ 会话 ═══════════════ */
function isUntitled(x) { return !x || /^(new chat|新对话|新会话)$/i.test(String(x).trim()); }
function sortedSessions() {
  // display order: pinned first, then most-recently-active. [0] is the topmost.
  return [...state.sessions.values()].sort((a, b) => {
    if (a.pinned && !b.pinned) return -1;
    if (!a.pinned && b.pinned) return 1;
    return (b.lastActiveTs || 0) - (a.lastActiveTs || 0);
  });
}
function relativeTime(ts) {
  if (!ts) return '';
  const diff = Date.now() - ts * 1000;
  const s = Math.floor(diff / 1000);
  if (s < 60) return s + 's ago';
  const m = Math.floor(s / 60);
  if (m < 60) return m + 'm ago';
  const h = Math.floor(m / 60);
  if (h < 24) return h + 'h ago';
  const d = Math.floor(h / 24);
  if (d < 7) return d + 'd ago';
  const w = Math.floor(d / 7);
  if (w < 5) return w + 'w ago';
  return new Date(ts * 1000).toLocaleDateString();
}
function renderSessionList() {
  convListEl.innerHTML = '';
  const batchBarEl = document.getElementById('batch-bar');
  if (batchMode) {
    const sessions = sortedSessions().filter(s => normalizeSessionFolder(s) === batchMode.folderId);
    const validIds = new Set(sessions.map(s => s.id));
    batchMode.selected = new Set([...batchMode.selected].filter(id => validIds.has(id)));
    if (!sessions.length) { exitBatch(); }
    else {
      if (batchBarEl) { batchBarEl.hidden = false; batchBarEl.style.display = ''; }
      sessions.forEach(sess => {
        const r = state.runtime.get(sess.id);
        const busy = !!(r && r.busy);
        const sel = batchMode.selected.has(sess.id);
        const pinSvg = sess.pinned ? GA_ICON('pushPinSimple', 'ci-pin') : '';
        const metaParts = [busy ? t('status.running') : t('status.idle')];
        if (sess.updatedAt) metaParts.push(relativeTime(sess.updatedAt));
        if (sess.workspace) metaParts.push(sess.workspace + (sess.branch ? ':' + sess.branch : ''));
        const item = document.createElement('div');
        item.className = 'conv-item batch' + (sel ? ' selected' : '') + (busy ? '' : ' idle');
        item.dataset.id = sess.id;
        item.innerHTML =
          '<input type="checkbox" class="ci-check"' + (sel ? ' checked' : '') + '>' +
          '<span class="ci-dot"></span><div class="ci-main">' +
          '<div class="ci-title">' + pinSvg + escapeHtml(displayTitle(sess)) + '</div>' +
          '<div class="ci-meta">' + escapeHtml(metaParts.join(' · ')) + '</div></div>';
        convListEl.appendChild(item);
      });
      updateBatchBar();
      return;
    }
  }
  if (batchBarEl) { batchBarEl.hidden = true; batchBarEl.style.display = 'none'; }
  const query = (searchInput ? searchInput.value : '').trim().toLowerCase();
  const all = sortedSessions();
  const filtered = query
    ? all.filter(s => {
        const title = displayTitle(s).toLowerCase();
        const ws = (s.workspace || '').toLowerCase();
        const br = (s.branch || '').toLowerCase();
        const fd = getConvFolderName(normalizeSessionFolder(s)).toLowerCase();
        const hasMsg = s.messages && s.messages.some(m => (m.text || '').toLowerCase().includes(query));
        return title.includes(query) || ws.includes(query) || br.includes(query) || fd.includes(query) || hasMsg;
      })
    : all;
  if (filtered.length === 0) {
    const e = document.createElement('div');
    e.className = 'conv-empty'; e.textContent = t('conv.emptyList');
    convListEl.appendChild(e); return;
  }
  const grouped = new Map(state.convFolders.map(f => [f.id, []]));
  filtered.forEach(sess => {
    const folderId = normalizeSessionFolder(sess);
    if (!grouped.has(folderId)) grouped.set(folderId, []);
    grouped.get(folderId).push(sess);
  });
  grouped.forEach((sessions, folderId) => {
    sessions.sort((a, b) => (Date.parse(b.updatedAt || b.lastActiveTs || 0) || 0) - (Date.parse(a.updatedAt || a.lastActiveTs || 0) || 0));
  });
  const orderedFolders = [...state.convFolders].sort((a, b) => {
    const aDefault = a.id === DEFAULT_CONV_FOLDER_ID;
    const bDefault = b.id === DEFAULT_CONV_FOLDER_ID;
    if (aDefault || bDefault) return aDefault ? 1 : -1;
    const aEmpty = !(grouped.get(a.id) || []).length;
    const bEmpty = !(grouped.get(b.id) || []).length;
    if (aEmpty !== bEmpty) return aEmpty ? -1 : 1;
    return 0;
  });
  orderedFolders.forEach(folder => {
    const sessions = grouped.get(folder.id) || [];
    const collapsed = isConvFolderCollapsed(folder.id);
    const head = document.createElement('div');
    head.className = 'conv-folder-head' + (collapsed ? ' is-collapsed' : '');
    head.dataset.folderId = folder.id;
    head.innerHTML =
      `<button class="conv-folder-toggle" data-folder-toggle="1" title="${escapeHtml(collapsed ? t('common.expand') : t('common.collapse'))}"${folder.id === DEFAULT_CONV_FOLDER_ID ? ' hidden' : ''}>${GA_ICON('caretDown')}</button>` +
      `<span class="conv-folder-name">${escapeHtml(folder.name)}</span>` +
      `<span class="conv-folder-count">${sessions.length}</span>` +
      (folder.id === DEFAULT_CONV_FOLDER_ID
        ? `<button class="conv-folder-add" data-folder-add="1" title="${escapeHtml(t('common.new'))}">${GA_ICON('plus')}</button>`
        : '') +
      `<button class="conv-folder-more" data-folder-more="1" title="${escapeHtml(t('common.more'))}"${(folder.locked || folder.id === ARCHIVED_CONV_FOLDER_ID) ? ' hidden' : ''}>${GA_ICON('dotsThreeVertical')}</button>`;
    head.addEventListener('dragover', (e) => {
      if (!dragConvSessionId || folder.id === DEFAULT_CONV_FOLDER_ID) return;
      e.preventDefault();
      head.classList.add('is-drop-target');
    });
    head.addEventListener('dragleave', () => head.classList.remove('is-drop-target'));
    head.addEventListener('drop', (e) => {
      if (!dragConvSessionId) return;
      e.preventDefault();
      head.classList.remove('is-drop-target');
      const sess = state.sessions.get(dragConvSessionId);
      dragConvSessionId = null;
      if (!sess) return;
      assignSessionFolder(sess, folder.id);
      saveSessions();
      renderSessionList();
      toast(t('conv.moveDone').replace('{0}', getConvFolderName(folder.id)));
    });
    convListEl.appendChild(head);
    if (collapsed) return;
    if (!sessions.length) {
      const empty = document.createElement('div');
      empty.className = 'conv-folder-empty';
      empty.textContent = t('conv.folderEmpty');
      convListEl.appendChild(empty);
      return;
    }
    sessions.forEach(sess => {
      const r = state.runtime.get(sess.id);
      const busy = !!(r && r.busy);
      const item = document.createElement('div');
      item.className = 'conv-item' + (currentPage === 'chat' && sess.id === state.activeId ? ' active' : '') + (busy ? '' : ' idle');
      item.dataset.id = sess.id;
      item.title = displayTitle(sess);
      item.draggable = folder.id !== DEFAULT_CONV_FOLDER_ID;
      item.addEventListener('dragstart', () => { dragConvSessionId = sess.id; item.classList.add('is-dragging'); });
      item.addEventListener('dragend', () => { dragConvSessionId = null; item.classList.remove('is-dragging'); document.querySelectorAll('.conv-folder-head.is-drop-target').forEach(el => el.classList.remove('is-drop-target')); });
      const pinSvg = sess.pinned ? GA_ICON('pushPinSimple', 'ci-pin') : '';
      const metaParts = [busy ? t('status.running') : t('status.idle')];
      if (sess.updatedAt) metaParts.push(relativeTime(sess.updatedAt));
      if (sess.workspace) metaParts.push(sess.workspace + (sess.branch ? `:${sess.branch}` : ''));
      item.innerHTML =
        `<span class="ci-dot"></span><div class="ci-main">` +
        `<div class="ci-title">${pinSvg}${escapeHtml(displayTitle(sess))}</div>` +
        `<div class="ci-meta">${escapeHtml(metaParts.join(' · '))}</div></div>` +
        `<button class="ci-more" title="${escapeHtml(t('common.more'))}">${GA_ICON('dotsThreeVertical')}</button>`;
      convListEl.appendChild(item);
    });
  });
}
async function openResumeModal() {
  const listEl = document.getElementById('resume-list');
  if (!listEl) return;
  try { await loadSessions(); } catch (_) {}
  const items = sortedSessions();
  if (!items.length) {
    listEl.innerHTML = '<div style="padding:24px;text-align:center;color:var(--muted);">暂无可恢复会话</div>';
    openModal('resume-modal');
    return;
  }
  listEl.innerHTML = '';
  for (const sess of items) {
    const r = state.runtime.get(sess.id);
    const busy = !!(r && r.busy);
    const metaParts = [busy ? t('status.running') : t('status.idle')];
    if (sess.updatedAt) metaParts.push(relativeTime(sess.updatedAt));
    if (sess.workspace) metaParts.push(sess.workspace + (sess.branch ? `:${sess.branch}` : ''));
    const div = document.createElement('div');
    div.className = 'conv-item' + (busy ? '' : ' idle');
    div.innerHTML =
      `<span class="ci-dot"></span><div class="ci-main">` +
      `<div class="ci-title">${escapeHtml(displayTitle(sess))}</div>` +
      `<div class="ci-meta">${escapeHtml(metaParts.join(' · '))}</div></div>`;
    div.addEventListener('click', () => { closeModals(); setActiveSession(sess.id); });
    listEl.appendChild(div);
  }
  openModal('resume-modal');
}
// ── resume inline session picker (mirrors slash/at panels) ──
let _resumeActive = false, _resumeAll = [], _resumeDisplayed = [], _resumeIdx = -1;
const RESUME_PANEL = () => document.getElementById('resume-panel');
const RESUME_LIST = () => document.getElementById('resume-list');
async function showResumePanel() {
  _resumeActive = true;
  hideSlashPanel(); hideAtPanel();
  _resumeAll = sortedSessions();
  _resumeDisplayed = _resumeAll;
  _resumeIdx = _resumeAll.length > 0 ? 0 : -1;
  renderResumeList();
  const p = RESUME_PANEL(); if (p) p.hidden = false;
  try { await loadSessions(); } catch (_) {}
  _resumeAll = sortedSessions();
  _resumeDisplayed = _resumeAll;
  _resumeIdx = _resumeAll.length > 0 ? 0 : -1;
  renderResumeList();
}
function hideResumePanel() {
  _resumeActive = false;
  const p = RESUME_PANEL(); if (p) p.hidden = true;
}
function renderResumeList() {
  const el = RESUME_LIST(); if (!el) return;
  if (!_resumeDisplayed.length) { el.innerHTML = '<div class="resume-empty">' + t('conv.emptyList') + '</div>'; return; }
  el.innerHTML = '';
  _resumeDisplayed.forEach((sess, i) => {
    const r = state.runtime.get(sess.id);
    const busy = !!(r && r.busy);
    const metaParts = [busy ? t('status.running') : t('status.idle')];
    if (sess.updatedAt) metaParts.push(relativeTime(sess.updatedAt));
    if (sess.workspace) metaParts.push(sess.workspace + (sess.branch ? `:${sess.branch}` : ''));
    const div = document.createElement('div');
    div.className = 'conv-item' + (i === _resumeIdx ? ' active' : '') + (busy ? '' : ' idle');
    div.innerHTML = '<span class="ci-dot"></span><div class="ci-main"><div class="ci-title">' + escapeHtml(displayTitle(sess)) + '</div><div class="ci-meta">' + escapeHtml(metaParts.join(' · ')) + '</div></div>';
    div.addEventListener('mousedown', (e) => { e.preventDefault(); selectResumeItem(i); });
    el.appendChild(div);
  });
  // scroll active item into view (sync, no rAF — rAF is throttled when tab is backgrounded)
  const active = el.querySelector('.conv-item.active');
  if (active) {
    const top = active.offsetTop;
    const bottom = top + active.offsetHeight;
    if (bottom > el.scrollTop + el.clientHeight) el.scrollTop = bottom - el.clientHeight;
    else if (top < el.scrollTop) el.scrollTop = top;
  }
}
function selectResumeItem(i) {
  const idx = (i == null) ? _resumeIdx : i;
  const sess = _resumeDisplayed[idx];
  hideResumePanel();
  if (!sess) return;
  inputEl.innerHTML = '';
  setActiveSession(sess.id);
}
async function invokeResumePicker() {
  // 保存当前会话草稿，防止下面的 /resume 文本覆盖原有输入内容
  const _cur = activeSess();
  if (_cur && inputEl) rt(_cur).inputDraft = inputEl.innerHTML;
  inputEl.innerHTML = '';
  inputEl.appendChild(document.createTextNode('/resume '));
  const sel = window.getSelection();
  const range = document.createRange();
  range.selectNodeContents(inputEl);
  range.collapse(false);
  sel.removeAllRanges(); sel.addRange(range);
  inputEl.focus();
  await showResumePanel();
}
if (searchInput) searchInput.addEventListener('input', () => renderSessionList());
async function ensureBridgeSession(sess) {
  if (sess.bridgeSessionId) return sess.bridgeSessionId;
  const res = await window.ga.rpc('session/new', { cwd: state.gaRoot || '', mcp_servers: [] });
  if (res?.error) throw new Error(res.error.message || res.error);
  sess.bridgeSessionId = res.sessionId || res.result?.sessionId;
  return sess.bridgeSessionId;
}
// 仿 TUI(continue_cmd.py 的 _preview_text 思路):sess.title 只在用户手动 rename 时被填,
// 平时为空;sidebar 显示名实时从消息派生 —— 优先取最后一段 assistant 输出里的 <summary>...</summary>,
// 其次用首条用户消息纯文本,都没有时回退到 t('conv.defaultTitle')。
function isAutoTitle(x) {
  const s = String(x || '').trim();
  if (!s) return true;
  if (/^(new chat|新对话|新会话)$/i.test(s)) return true;
  if (/^agent-\d+$/i.test(s)) return true;  // 兼容上一轮误存的 agent-N
  return false;
}
function displayTitle(sess) {
  if (sess && sess.title && !isAutoTitle(sess.title)) return sess.title;
  const msgs = (sess && sess.messages) || [];
  // 1) 优先:最后一段 assistant 文本里的 <summary>...</summary>
  for (let i = msgs.length - 1; i >= 0; i--) {
    const m = msgs[i];
    if (!m || m.role !== 'assistant') continue;
    const txt = assistantStructuredText(m);
    const sm = /<summary>([\s\S]*?)<\/summary>/i.exec(txt);
    if (sm && sm[1].trim()) {
      const line = sm[1].trim().split('\n')[0].trim();
      if (line) return line.length > 60 ? line.slice(0, 60) + '…' : line;
    }
  }
  // 2) 兜底:最后一条用户消息纯文本(去附件占位符) — 标题随最新指令动态更新
  for (let i = msgs.length - 1; i >= 0; i--) {
    const m = msgs[i];
    if (!m || m.role !== 'user') continue;
    const raw = typeof m.content === 'string' ? m.content : (m.display || '');
    const clean = stripAttachPlaceholders(raw).trim();
    if (clean) return clean.length > 40 ? clean.slice(0, 40) + '…' : clean;
  }
  return t('conv.defaultTitle');
}
async function newSession(folderId = null) {
  // 继承前一活跃会话的 workspace 绑定，使新建会话不丢失工作区上下文
  const prevId = state.activeId;
  let inheritWs = null;
  if (prevId) {
    try {
      const r = await window.ga.getSessionWorkspace(prevId);
      inheritWs = (r && r.workspace && r.workspace.name) || null;
    } catch (_) {}
  }
  // 兜底:无活跃会话或其未绑定workspace时,用最近使用的workspace路径反查注册名
  if (!inheritWs && state.lastWorkspacePath) {
    try {
      const res = await window.ga.listWorkspaces();
      const hit = ((res && res.workspaces) || []).find(w => w.path === state.lastWorkspacePath);
      if (hit && hit.name) inheritWs = hit.name;
    } catch (_) {}
  }
  const localId = 'local-' + Date.now() + '-' + Math.random().toString(16).slice(2);
  const active = activeSess();
  const sess = { id: localId, bridgeSessionId: null, title: '', messages: [], untitled: true, lastActiveTs: Date.now(), folderId: folderId == null ? normalizeSessionFolder(active) : normalizeSessionFolder(folderId) };
  state.sessions.set(localId, sess);
  try {
    await ensureBridgeSession(sess);
    state.sessions.delete(localId);
    sess.id = sess.bridgeSessionId;
    state.sessions.set(sess.id, sess);
    if (inheritWs) {
      try {
        await window.ga.setSessionWorkspace(sess.id, inheritWs);
        sess.workspace = inheritWs;
      } catch (_) {}
    }
  } catch (e) { showError(t('err.newSession') + ': ' + (e.message || e)); }
  setActiveSession(sess.id);
  saveSessions();
  renderSessionList();
}
/* ═══════════════ Project space (entry + create) ═══════════════ */
function formatAgo(ts) {
  if (!ts) return '';
  let diff = Date.now() - ts * 1000;
  if (diff < 0) diff = 0;
  const s = Math.floor(diff / 1000);
  if (s < 60) return t('project.justNow');
  const m = Math.floor(s / 60);
  if (m < 60) return t('project.minAgo').replace('{0}', m);
  const h = Math.floor(m / 60);
  if (h < 24) return t('project.hourAgo').replace('{0}', h);
  const d = Math.floor(h / 24);
  if (d < 30) return t('project.dayAgo').replace('{0}', d);
  const mo = Math.floor(d / 30);
  if (mo < 12) return t('project.monAgo').replace('{0}', mo);
  return t('project.yearAgo').replace('{0}', Math.floor(mo / 12));
}

const PROJECT_TEMPLATES = [
  { key: 'req', icon: 'listChecks', instruction: `# 角色
你是一个产品需求全流程协同助手，服务于产品团队从需求规划到测试上线的完整周期。


## 阶段一：需求规划
当用户在做需求规划时：
- 帮助梳理目标用户、用户痛点、使用场景和业务目标
- 进行竞品调研、行业分析和现有方案对比
- 帮助识别需求价值、影响范围、实现复杂度和优先级
- 输出需求概要、需求池整理、优先级建议和待确认问题清单
- 提醒用户：可以将确定要推进的需求创建为事项，分配给对应负责人，并邀请相关角色关注


## 阶段二：PRD 撰写
当用户在写 PRD 时：
- 遵循 EARS 原则撰写需求描述，确保每条需求表述清晰无歧义：
  - Ubiquitous：系统始终满足的约束（"The system shall…"）
  - Event-driven：由事件触发的行为（"When [event], the system shall…"）
  - Unwanted：异常/故障处理（"If [unwanted condition], then the system shall…"）
  - State-driven：在特定状态下的行为（"While [state], the system shall…"）
  - Optional：可选功能（"Where [feature], the system shall…"）
- 按标准 PRD 结构输出：背景、目标、用户故事、功能清单、流程说明、交互说明、数据指标、验收标准
- 帮助补充边界场景、异常状态、权限规则、数据口径和埋点需求
- 可帮助生成 Word 文档或评审材料供团队讨论
- 提醒用户：完成后可以将 PRD 上传到项目资料库，并创建评审事项分配给技术负责人、设计师和测试同学


## 阶段三：设计与研发评审
当用户在做设计或研发评审时：
- 帮助整理评审议题、待确认问题、关键依赖和风险点
- 协助产品解释需求背景、业务规则、用户路径和验收标准
- 帮助设计师拆解页面清单、状态清单、交互边界和设计验收点
- 帮助研发识别技术依赖、接口问题、数据结构、兼容性和实现风险
- 提醒用户：可以将评审结论沉淀到项目资料库，并将待办问题拆成事项分配给对应负责人


## 阶段四：研发跟进
当用户在跟研发进度时：
- 帮助将需求拆解为研发任务、子任务和依赖项
- 协助查看 TAPD、CNB、GitHub、工蜂等工具中的需求状态、任务状态或代码进展
- 协助评审技术方案，识别范围变更、延期风险和阻塞事项
- 帮助整理每日/每周研发进度摘要和风险清单
- 提醒用户：可以将技术任务拆成子事项分配给开发同学，并附上 PRD、设计稿或评审结论


## 阶段五：测试验收
当用户在做测试或验收时：
- 基于 PRD 生成测试用例、边界场景、异常场景和回归测试范围
- 协助撰写规范的 Bug 描述，包括复现步骤、环境信息、实际结果、期望结果和影响范围
- 帮助产品判断问题是否阻断上线、是否影响核心流程、是否需要调整需求范围
- 输出验收清单、上线风险清单和版本质量总结
- 提醒用户：发现的 Bug 可以创建事项分配给开发同学，并关联回原需求或 PRD 文档


## 阶段六：上线复盘
当用户在做上线复盘时：
- 帮助总结需求目标达成情况、上线效果、遗留问题和后续优化点
- 整理研发、测试、设计和业务侧反馈
- 帮助提炼可复用经验、流程问题和下一版本需求候选项
- 输出上线复盘报告、数据观察清单和后续行动项
- 提醒用户：可以将复盘结论上传到项目资料库，并将后续优化点创建为新事项


# 提醒事项
- 每个阶段完成后，主动提醒用户将成果流转给下一环节的角色
- 提醒用户在事项中附上对应的文档交付物，例如 PRD、设计稿、测试用例、评审结论或复盘报告
- 提醒用户添加关注人，让关键角色及时收到通知
- 提醒用户将重要文档上传到项目资料库，形成项目统一上下文
- 当信息不足时，先追问关键缺口，不要直接编造结论
- 涉及排期、资源、上线范围等决策时，提醒用户找对应负责人确认` },
  { key: 'research', icon: 'chartBar', instruction: `# 角色
你是一个市场调研与竞品分析协同助手，服务于产品、市场、运营、战略和业务决策团队，从调研课题定义、资料收集、竞品分析、报告产出到结论评审的完整周期。


## 阶段一：调研课题定义
当用户在定义调研课题时：
- 帮助澄清本次调研的业务背景、核心问题、目标受众、决策用途和时间范围
- 区分调研类型，例如行业趋势、竞品分析、用户需求、市场规模、商业模式、渠道策略或增长机会
- 帮助拆解调研问题，明确一级问题、二级问题、假设和需要验证的关键信息
- 输出调研 Brief、问题清单、调研范围、信息来源建议和预期交付物
- 提醒用户：可以将调研课题创建为事项，分配给调研负责人，并添加业务方或决策者为关注人


## 阶段二：资料收集
当用户在收集调研资料时：
- 帮助从公开资料、行业报告、新闻资讯、问卷结果、用户访谈、竞品官网、产品体验和内部文档中整理信息
- 协助识别资料来源的可信度、发布时间、适用范围和可能偏差
- 对不同来源的信息进行归类，例如市场数据、用户洞察、产品功能、价格策略、渠道策略、客户案例和商业模式
- 提醒用户：可以将原始资料、链接、问卷结果或访谈记录上传到项目资料库，便于后续复核和追问


## 阶段三：竞品拆解
当用户在分析竞品时：
- 帮助确定竞品范围，区分直接竞品、间接竞品、替代方案和标杆产品
- 从目标用户、核心场景、功能结构、产品流程、商业模式、定价策略、增长渠道、客户案例和优劣势等维度进行拆解
- 帮助输出竞品对比表、功能矩阵、能力差异、体验亮点和风险点
- 避免只罗列功能，要进一步提炼竞品背后的产品策略、用户价值和业务意图
- 提醒用户：可以将竞品分析表或体验截图上传到项目资料库，作为团队统一参考材料


## 阶段四：洞察分析
当用户在提炼调研结论时：
- 帮助从收集到的信息中识别趋势、机会点、风险点、用户痛点和潜在策略方向
- 将事实、判断和建议区分开，避免把未经验证的推测写成确定结论
- 对关键结论补充证据来源、适用条件和不确定性说明
- 帮助形成 SWOT、机会优先级、竞品差距、用户需求洞察或市场进入建议
- 提醒用户：重要洞察可以创建为评审事项，邀请产品、市场、运营或业务负责人讨论确认


## 阶段五：报告产出
当用户需要输出调研报告时：
- 按结构化报告格式输出，包括调研背景、调研目标、方法说明、核心发现、竞品分析、机会判断、风险提示和建议动作
- 根据使用对象调整报告口径，例如给产品团队看时突出需求机会和功能建议，给管理层看时突出市场判断和决策建议
- 帮助生成汇报大纲、PPT 结构、结论页、竞品对比表和行动建议清单
- 对引用的数据、案例和观点标注来源或依据
- 提醒用户：报告完成后可以上传到项目资料库，并创建「调研结论评审」事项，添加业务侧或决策者为关注人


## 阶段六：结论评审
当团队在评审调研结论时：
- 帮助整理评审议题、关键结论、争议点、待确认问题和决策项
- 支持基于已有报告进行追问，例如"这个竞品为什么这么做""这个机会是否适合我们""下一步应该验证什么"
- 将评审反馈整理为行动项，例如继续补充调研、进入需求设计、做用户访谈、拉数据验证或暂不推进
- 提醒用户：可以将评审结论沉淀到项目资料库，并将后续行动创建为事项分配给对应负责人


## 阶段七：持续跟踪
当调研结论需要持续更新时：
- 帮助跟踪竞品动态、行业变化、政策变化、用户反馈和市场数据变化
- 定期整理变化摘要，判断是否影响原有结论
- 帮助维护竞品分析表、市场观察清单和机会池
- 提醒用户：重要变化可以更新到原报告中，并通知关注人重新评估


# 提醒事项
- 调研结论必须区分事实、分析和建议，避免将推测包装成确定结论
- 对关键数据、竞品信息、行业观点和用户反馈，尽量标注来源、时间和适用范围
- 每个阶段完成后，主动提醒用户将资料、分析表、报告和评审结论上传到项目资料库
- 主动提醒用户创建「调研结论评审」事项，并添加业务方、产品负责人或决策者为关注人
- 支持基于已有报告持续追问和深入分析，让 Agent 成为报告生产者与报告消费者之间的桥梁
- 当信息不足、来源不可靠或样本有限时，应明确提示不确定性，并建议补充验证方式` },
  { key: 'kb', icon: 'books', instruction: `# 角色
你是一个团队知识库协同助手，服务于团队成员、知识贡献者、知识消费者和知识库维护人，帮助团队持续沉淀、整理、检索、更新和复用知识。


## 阶段一：知识库初始化
当用户在搭建团队知识库时：
- 帮助明确知识库目标、使用对象、内容范围、维护负责人和更新机制
- 建议知识分类结构，例如团队介绍、业务背景、项目文档、SOP、技术方案、设计规范、常见问题、会议结论、复盘沉淀和踩坑记录
- 帮助生成知识库目录、文档命名规范、标签规范和维护规则
- 提醒用户：可以将已有团队文档、规范、FAQ 和项目资料上传到项目资料库，作为初始知识内容


## 阶段二：知识上传与整理
当成员上传或补充知识时：
- 帮助识别文档类型、适用场景、目标读者和所属分类
- 将零散内容整理成结构化文档，例如背景、适用范围、操作步骤、注意事项、负责人、更新时间和相关链接
- 帮助生成文档摘要、关键词、标签和推荐归档位置
- 对内容不完整的文档，主动提醒补充缺失信息，例如结论、步骤、适用范围、示例、负责人或更新时间
- 提醒用户：可以将整理后的文档上传到项目资料库或乐享知识库，方便团队检索和复用


## 阶段三：知识检索与问答
当团队成员基于知识库提问时：
- 优先基于项目资料库、乐享知识库、腾讯文档或用户上传资料回答
- 回答时尽量引用信息来源，说明答案来自哪份文档或哪个资料
- 如果资料中没有明确依据，要说明"当前知识库未提供"，并建议补充文档或找对应负责人确认
- 对复杂问题，帮助拆解为背景说明、结论、操作步骤、注意事项和相关资料链接
- 提醒用户：如果该问题频繁出现，可以整理成 FAQ 并沉淀到知识库


## 阶段四：知识更新与过期治理
当用户在维护知识库时：
- 帮助识别可能过期、重复、冲突或缺少负责人的文档
- 根据更新时间、业务变化、流程变化或成员反馈，提醒用户检查文档有效性
- 帮助生成待更新文档清单、重复文档合并建议和冲突内容处理建议
- 对过期内容，建议标注状态，例如有效、待更新、已废弃或仅供参考
- 提醒用户：可以将知识更新动作创建为事项，分配给对应维护人，并设置关注人


## 阶段五：知识沉淀与复盘
当团队完成项目、会议或重要工作后：
- 帮助将会议结论、项目复盘、问题处理经验、最佳实践和踩坑记录整理为可复用文档
- 输出结构化复盘文档，包括背景、过程、结论、问题、经验、后续动作和适用场景
- 帮助提炼可复用 SOP、检查清单、模板和 FAQ
- 提醒用户：可以将复盘结论沉淀到知识库，并关联相关项目资料或事项


## 阶段六：知识运营
当知识库需要持续运营时：
- 帮助定期总结新增知识、热门问题、过期内容、待补充内容和高频搜索主题
- 给出知识库运营建议，例如谁负责维护、多久更新一次、哪些内容需要补齐、哪些文档需要合并
- 帮助生成知识库周报/月报、知识缺口清单和维护任务列表
- 提醒用户：可以将维护任务创建为事项，分配给知识库维护人或对应内容负责人


# 提醒事项
- 团队知识库不是一次性上传文档，而是持续运营的知识体系
- 回答问题时优先基于资料库内容，并尽量标注信息来源
- 当资料缺失、过期或冲突时，要明确指出问题，不要编造答案
- 主动引导团队将零散经验、会议结论、SOP、FAQ、技术方案和项目复盘沉淀为结构化文档
- 主动提醒用户为文档补充分类、标签、负责人、更新时间和适用范围
- 主动提醒用户将知识更新、文档补齐、过期治理等动作创建为事项并分配负责人` },
  { key: 'delivery', icon: 'folderSimple', instruction: `# 角色
你是一个项目交付协同助手，服务于销售、售前、交付 PM、研发、客户成功和客户方联系人从售前交接、交付计划到验收复盘的完整周期。


## 阶段一：售前到交付交接
当销售或售前在做项目交接时：
- 帮助整理客户背景、业务诉求、关键联系人、决策链、已承诺事项和待确认问题
- 区分客户明确需求、销售承诺、内部判断和待验证信息
- 输出售前交接清单、客户需求摘要和交付风险初筛
- 提醒用户：可以将交接材料上传项目资料库，并邀请交付 PM、研发和客户成功关注


## 阶段二：客户需求澄清
当团队在澄清客户需求时：
- 帮助梳理客户目标、使用场景、交付范围、验收标准、优先级和约束条件
- 识别范围不清、需求变更、客户待确认、资源冲突和合规风险
- 输出客户需求清单、待确认问题清单和范围边界说明
- 提醒用户：可以将客户待确认问题创建为事项，并分配给客户接口人或内部负责人


## 阶段三：交付计划制定
当交付 PM 在制定计划时：
- 帮助拆解里程碑、任务、负责人、截止时间、依赖项和风险点
- 生成项目交付计划、任务分解表、里程碑计划和风险清单
- 协助区分内部计划和客户可见计划
- 提醒用户：可以将关键任务创建为事项，分配给研发、实施、售前或客户成功负责人


## 阶段四：项目执行跟踪
当团队在推进项目执行时：
- 帮助跟踪任务进度、阻塞事项、客户待确认问题和范围变更
- 整理会议纪要、行动项、负责人和截止时间
- 输出内部项目周报、风险同步和下一步行动清单
- 提醒用户：可以在事项中更新状态、添加关注人，并附上会议纪要或交付文档


## 阶段五：客户沟通与周报
当用户需要对客户同步进展时：
- 帮助生成客户周报、会议纪要、阶段成果说明和待确认事项
- 将内部风险表达转化为客户可理解、可沟通的版本
- 对外输出前，检查是否包含内部敏感信息、成本、人员安排、未确认承诺或内部风险判断
- 提醒用户：客户版材料应与内部版区分存放和使用


## 阶段六：验收交付
当项目进入验收阶段时：
- 帮助整理验收范围、验收标准、交付物清单、遗留问题和客户确认项
- 生成验收清单、交付说明、培训材料或 FAQ
- 协助客户成功从交付阶段接手到持续运营阶段
- 提醒用户：可以将验收结论和交付文档上传项目资料库，并将遗留问题创建为后续事项


## 阶段七：交付复盘
当团队在做交付复盘时：
- 帮助总结项目目标达成情况、范围变更、风险处理、客户反馈和可复用经验
- 输出交付复盘报告、客户成功建议和后续跟进清单
- 帮助沉淀交付 SOP、方案模板、FAQ 和客户案例
- 提醒用户：可以将复盘结论沉淀到乐享知识库或项目资料库，供后续项目复用


# 提醒事项
- 始终区分内部信息和客户可见信息，对外输出前进行敏感信息检查
- 所有交付计划都要明确范围、里程碑、负责人、截止时间、依赖项和风险
- 主动提醒用户将交接材料、需求清单、计划、会议纪要、周报、验收材料和复盘文档上传项目资料库
- 主动提醒用户将任务、风险、客户待确认问题和遗留问题创建为事项并分配负责人
- 不替团队承诺未经确认的交付范围、排期或价格；涉及合同、法务、合规问题时，提醒用户找对应负责人确认` },
  { key: 'bug', icon: 'check', instruction: `# 角色
你是一个 Bug 跟踪与测试验收协同助手，服务于测试、研发和产品从测试准备、Bug 流转到回归验收和版本质量总结的完整周期。


## 阶段一：测试准备
当用户在准备测试时：
- 基于 PRD、需求说明或用户故事生成测试用例
- 覆盖正常流程、边界场景、异常场景、权限场景、兼容性场景和回归范围
- 帮助整理测试计划、测试范围、测试环境、负责人和验收标准
- 输出测试用例表、测试执行清单和风险关注点
- 提醒用户：可以将测试用例上传到项目资料库，并创建测试执行事项分配给测试同学


## 阶段二：Bug 反馈
当用户发现或反馈 Bug 时：
- 帮助将反馈整理为标准 Bug 描述
- 主动追问缺失字段，包括复现环境、账号信息、版本、前置条件、复现步骤、实际结果、期望结果、截图、日志和影响范围
- 协助判断 Bug 严重程度、优先级和是否阻断上线
- 提醒用户：可以将 Bug 创建为事项分配给研发，并关联回原需求、PRD 或测试用例


## 阶段三：研发定位
当研发在定位问题时：
- 帮助研发理解 Bug 背景、复现路径、相关需求和影响范围
- 从日志、报错、代码变更、接口信息或用户反馈中提取问题线索
- 协助整理可能原因、排查路径和待补充信息
- 提醒用户：不要在证据不足时直接断言根因，修复后应补充修复说明和影响范围


## 阶段四：修复验证
当 Bug 修复后需要验证时：
- 根据修复说明生成回归测试清单
- 帮助测试确认原问题是否解决、相关功能是否受影响、边界场景是否覆盖
- 整理回归结果、遗留问题和风险点
- 提醒用户：可以将回归结果更新到 Bug 事项中，并通知产品或测试负责人验收


## 阶段五：测试验收
当产品或测试在做版本验收时：
- 汇总测试通过情况、未解决 Bug、阻塞项、延期风险和上线风险
- 帮助判断版本是否满足验收标准
- 输出验收结论建议、版本质量报告和上线风险清单
- 提醒用户：可以将验收结论上传项目资料库，并将遗留问题创建为后续事项


## 阶段六：质量复盘
当团队在做版本质量复盘时：
- 帮助总结 Bug 分布、问题类型、根因分类、修复效率和遗留风险
- 提炼流程问题、需求问题、研发问题或测试覆盖问题
- 输出质量复盘报告和下一版本改进建议
- 提醒用户：可以将复盘结论沉淀为团队测试规范或研发检查清单


# 提醒事项
- Bug 信息必须结构化，避免研发无法复现或无法判断优先级
- 发现 Bug 后，提醒用户创建事项、分配研发、关联原需求和补充附件
- 修复完成后，提醒用户更新事项状态、补充修复说明并发起回归验证
- 不在缺少证据时断言根因；对代码、日志、接口问题的判断需要标注依据
- 涉及线上事故时，优先提醒团队确认影响范围、止损方案和回滚预案` }
];

function renderProjectList(items) {
  const grid = document.getElementById('project-my-grid');
  if (!grid) return;
  if (!items || !items.length) {
    grid.innerHTML = '<div class="proj-empty">' + escapeHtml(t('project.empty')) + '</div>';
    return;
  }
  grid.innerHTML = items.map(function (p) {
    const ago = p.mtime ? escapeHtml(t('project.addedAgo').replace('{0}', formatAgo(p.mtime))) : '';
    const wsTag = p.workspace ? '<span class="proj-card-ws"><span data-ga-icon="folderSimple"></span> ' + escapeHtml(p.workspace) + '</span>' : '';
    return '<div class="proj-card" data-project="' + escapeHtml(p.name) + '">'
      + '<div class="proj-card-icon"><span data-ga-icon="folderOpen"></span></div>'
      + '<div class="proj-card-body">'
      + '<div class="proj-card-name">' + escapeHtml(p.name) + '</div>'
      + '<div class="proj-card-meta">' + (ago ? ago : '') + (ago && wsTag ? ' · ' : '') + wsTag + '</div>'
      + '</div>'
      + '<button type="button" class="proj-card-menu" aria-label="' + escapeHtml(t('project.menuTitle')) + '"><span data-ga-icon="dotsThree"></span></button>'
      + '</div>';
  }).join('');
  if (window.gaHydrateIcons) gaHydrateIcons(grid);
  grid.querySelectorAll('.proj-card').forEach(function (card) {
    card.addEventListener('click', function (e) {
      const menuBtn = e.target.closest('.proj-card-menu');
      if (menuBtn) { e.stopPropagation(); e.preventDefault(); showProjectCardMenu(menuBtn, card.dataset.project); return; }
      enterProject(card.dataset.project);
    });
  });
}

function renderProjectTemplates() {
  const grid = document.getElementById('project-tpl-grid');
  if (!grid) return;
  grid.innerHTML = PROJECT_TEMPLATES.map(function (tpl) {
    const name = t('project.tpl.' + tpl.key + '.t');
    return '<div class="proj-card proj-card-tpl" data-name="' + escapeHtml(name) + '">'
      + '<div class="proj-card-icon"><span data-ga-icon="' + tpl.icon + '"></span></div>'
      + '<div class="proj-card-body">'
      + '<div class="proj-card-name">' + escapeHtml(name) + '</div>'
      + '<div class="proj-card-meta">' + escapeHtml(t('project.tpl.' + tpl.key + '.d')) + '</div>'
      + '</div>'
      + '</div>';
  }).join('');
  if (window.gaHydrateIcons) gaHydrateIcons(grid);
  grid.querySelectorAll('.proj-card-tpl').forEach(function (card) {
    card.addEventListener('click', function () { createProject(card.dataset.name); });
  });
}

async function loadProjects() {
  renderProjectTemplates();
  try {
    const res = await window.ga.rpc('projects/list', {});
    if (res?.error) throw new Error(res.error.message || res.error);
    renderProjectList(res.projects || res.result?.projects || []);
  } catch (e) { showError(t('project.loadErr') + ': ' + (e.message || e)); }
}

async function enterProject(name) {
  if (!name) return;
  const localId = 'local-' + Date.now() + '-' + Math.random().toString(16).slice(2);
  const sess = { id: localId, bridgeSessionId: null, title: name, messages: [], untitled: true, project: name, lastActiveTs: Date.now() };
  state.sessions.set(localId, sess);
  try {
    const res = await window.ga.rpc('session/new', { cwd: state.gaRoot || '', project: name, mcp_servers: [] });
    if (res?.error) throw new Error(res.error.message || res.error);
    sess.bridgeSessionId = res.sessionId || res.result?.sessionId;
    state.sessions.delete(localId);
    sess.id = sess.bridgeSessionId;
    state.sessions.set(sess.id, sess);
  } catch (e) { state.sessions.delete(localId); showError(t('project.enterErr') + ': ' + (e.message || e)); return; }
  setActiveSession(sess.id);
  saveSessions();
  renderSessionList();
  // project-home 不在侧边栏导航中，gaGoPage会因找不到nav-item提前return，
  // 所以这里手动切换 section active 状态
  currentPage = 'project-home';
  nav?.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  pages.forEach(p => p.classList.toggle('active', p.dataset.page === 'project-home'));
  if (bodyEl) bodyEl.classList.remove('rp-collapsed');
  initProjectHome(name, sess.id);
}

/* ═══════════════ 项目主页 (project-home) ═══════════════ */
let phState = { project: '', sessionId: '', tab: 'feed', filter: 'mine', datasources: [], dsLoadedFor: '', dsLoading: false, todos: [], todosLoadedFor: '', todoQuery: '', ownership: 'all' };
var TODO_STATUS_OPTS = ['todo', 'doing', 'pause', 'done'];
function ntStatusLabel(k) { return t('ph.plan.col.' + k); }

async function loadProjectTodos(projectName) {
  if (!projectName) { phState.todos = []; return; }
  try {
    var res = await fetch('/projects/' + encodeURIComponent(projectName) + '/todos', { headers: { 'Accept': 'application/json' } });
    var data = await res.json();
    if (res.ok && data && Array.isArray(data.todos)) {
      phState.todos = data.todos;
      phState.todosLoadedFor = projectName;
      if (phState.project === projectName && phState.tab === 'plan') renderPhContent();
    }
  } catch (e) { console.warn('loadProjectTodos failed', e); }
}

async function loadProjectDatasources(projectName) {
  if (!projectName) return [];
  phState.dsLoading = true;
  try {
    var resp = await fetch('/projects/' + encodeURIComponent(projectName) + '/datasources');
    var res = await resp.json();
    if (!resp.ok) throw new Error(res.error || '加载失败');
    phState.datasources = Array.isArray(res && res.datasources) ? res.datasources : [];
    phState.dsLoadedFor = projectName;
    return phState.datasources;
  } catch (e) {
    phState.datasources = [];
    phState.dsLoadedFor = projectName;
    showError('加载数据源失败: ' + (e.message || e));
    return [];
  } finally {
    phState.dsLoading = false;
  }
}

function renderPhDatasourceList() {
  if (phState.dsLoading && phState.dsLoadedFor !== phState.project) {
    return '<div class="ph-empty"><div class="ph-empty-text">数据源加载中…</div></div>';
  }
  var list = Array.isArray(phState.datasources) ? phState.datasources : [];
  if (!list.length) {
    return '<div class="ph-empty"><div class="ph-empty-text">暂无已绑定数据源。可前往“计划”页点击“添加来源”。</div></div>';
  }
  return '<div class="ph-ds-list">' + list.map(function (ds) {
    var recent = Array.isArray(ds.event_log) ? ds.event_log.slice(-3).reverse() : [];
    var meta = [ds.type || '', ds.name || '', (ds.events || []).length ? ('订阅 ' + ds.events.length + ' 类事件') : ''].filter(Boolean).join(' · ');
    return '<section class="ph-ds-card">'
      + '<div class="ph-ds-head"><strong>' + escapeHtml(ds.name || ds.id || '未命名数据源') + '</strong><span>' + escapeHtml(ds.type || '') + '</span></div>'
      + '<div class="ph-ds-meta">' + escapeHtml(meta) + '</div>'
      + '<div class="ph-ds-url">' + escapeHtml(ds.webhook_url || '') + '</div>'
      + (recent.length ? ('<ul class="ph-ds-events">' + recent.map(function (evt) {
        var view = formatDatasourceEvent(evt);
        return '<li class="ph-ds-event">'
          + '<div class="ph-ds-event-title">' + escapeHtml(view.title) + '</div>'
          + (view.detail ? ('<div class="ph-ds-event-detail' + (view.detailMuted ? ' is-muted' : '') + '">' + escapeHtml(view.detail) + '</div>') : '')
          + (view.meta.length ? ('<div class="ph-ds-event-meta">' + view.meta.map(function (x) { return '<span>' + escapeHtml(x) + '</span>'; }).join('') + '</div>') : '')
          + (view.projectLabel ? ('<div class="ph-ds-event-project" title="' + escapeHtml(view.project) + '">' + escapeHtml(view.projectLabel) + '</div>') : '')
          + (view.link ? ('<a class="ph-ds-event-link" href="' + escapeHtml(view.link) + '" target="_blank" rel="noopener noreferrer">' + escapeHtml(view.linkText || '查看详情') + '</a>') : '')
          + '</li>';
      }).join('') + '</ul>') : '<div class="ph-ds-empty">暂无最近事件</div>')
      + '</section>';
  }).join('') + '</div>';
}

async function initProjectHome(name, sessionId) {
  phState.project = name;
  phState.sessionId = sessionId;
  phState.tab = 'feed';
  phState.filter = 'mine';
  phState.datasources = [];
  phState.dsLoadedFor = '';
  // 面包屑项目名
  const crumbName = document.getElementById('ph-project-name');
  if (crumbName) crumbName.textContent = name;
  // Tab 切换
  document.querySelectorAll('.ph-tab').forEach(function (tab) {
    tab.onclick = function () {
      document.querySelectorAll('.ph-tab').forEach(function (t) { t.classList.remove('active'); });
      tab.classList.add('active');
      phState.tab = tab.dataset.phTab;
      renderPhContent();
    };
  });
  // 子筛选切换
  document.querySelectorAll('.ph-filter-btn').forEach(function (btn) {
    btn.onclick = function () {
      document.querySelectorAll('.ph-filter-btn').forEach(function (b) { b.classList.remove('active'); });
      btn.classList.add('active');
      phState.filter = btn.dataset.filter;
      renderPhContent();
    };
  });
  // 邀请按钮
  const inviteBtn = document.querySelector('.ph-invite-btn');
  if (inviteBtn) inviteBtn.onclick = function () { alert(t('ph.invite') + ': ' + name); };
  // 配置卡片 + 号按钮
  document.querySelectorAll('.ph-cfg-add').forEach(function (btn) {
    btn.onclick = function (e) { e.stopPropagation(); var card = btn.closest('.ph-cfg-card'); var titleEl = card?.querySelector('.ph-cfg-title'); if (titleEl) alert(titleEl.textContent); };
  });
  // 底部输入框发送
  const input = document.querySelector('.ph-composer-input');
  const sendBtn = document.getElementById('ph-send-btn') || document.querySelector('.ph-composer-send');
  if (input && sendBtn) {
    const doSend = function () {
      var val = input.value.trim();
      if (!val) return;
      var sid = phState.sessionId || state.activeId;
      if (!sid) return;
      setActiveSession(sid);
      if (currentPage !== 'project-home') {
        currentPage = 'project-home';
        syncNavSelection();
        pages.forEach(function (p) { p.classList.toggle('active', p.dataset.page === 'project-home'); });
      }
      if (window.gaComposerSetText) window.gaComposerSetText('chat', val);
      else {
        var ta = document.getElementById('chat-input');
        if (ta) {
          ta.textContent = val;
          ta.dispatchEvent(new Event('input', { bubbles: true }));
        }
      }
      input.value = '';
      var sendChat = document.getElementById('send-btn');
      if (sendChat && !sendChat.disabled) sendChat.click();
    };
    sendBtn.onclick = doSend;
    input.onkeydown = function (e) { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); doSend(); } };
  }
  renderPhContent();
  loadProjectDatasources(name).then(function () { if (phState.project === name) renderPhContent(); });
  loadProjectTodos(name);
  // 渲染专家头像占位
  var expertBox = document.querySelector('.ph-cfg-experts');
  if (expertBox && !expertBox.children.length) {
    ['A', 'B', 'C', 'D'].forEach(function (c) {
      var av = document.createElement('div');
      av.className = 'ph-cfg-expert-avatar';
      av.textContent = c;
      expertBox.appendChild(av);
    });
  }
}

function renderPhTaskList() {
  var project = String(phState.project || '');
  var items = sortedSessions().filter(function (sess) {
    return String(sess && sess.project || '') === project;
  });
  if (!items.length) {
    return '<div class="ph-empty"><div class="ph-empty-icon" data-ga-icon="inbox"></div><div class="ph-empty-text">' + escapeHtml(t('ph.empty.task')) + '</div></div>';
  }
  return '<div class="ph-task-list">' + items.map(function (sess) {
    var r = state.runtime.get(sess.id);
    var busy = !!(r && r.busy);
    var metaParts = [busy ? t('status.running') : t('status.idle')];
    if (sess.updatedAt) metaParts.push(relativeTime(sess.updatedAt));
    if (sess.workspace) metaParts.push(sess.workspace + (sess.branch ? ':' + sess.branch : ''));
    return '<div class="conv-item ph-task-item' + (busy ? '' : ' idle') + '" data-ph-task-open="' + escapeHtml(sess.id) + '" data-id="' + escapeHtml(sess.id) + '" role="button" tabindex="0">'
      + '<span class="ci-dot"></span><div class="ci-main">'
      + '<div class="ci-title">' + escapeHtml(displayTitle(sess)) + '</div>'
      + '<div class="ci-meta">' + escapeHtml(metaParts.join(' · ')) + '</div></div>'
      + '<button type="button" class="ci-more" data-ph-task-menu="' + escapeHtml(sess.id) + '" title="' + escapeHtml(t('common.more')) + '">' + GA_ICON('dotsThreeVertical') + '</button>'
      + '</div>';
  }).join('') + '</div>';
}

function renderPhContent() {
  var box = document.querySelector('.ph-content');
  if (!box) return;
  if (phState.tab === 'plan') {
    box.innerHTML = renderPhPlan();
    bindPhPlan(box);
    if (typeof gaHydrateIcons === 'function') gaHydrateIcons(box);
    return;
  }
  if (phState.tab === 'task') {
    box.innerHTML = renderPhTaskList();
    box.querySelectorAll('[data-ph-task-open]').forEach(function (btn) {
      btn.onclick = function (e) {
        if (e.target.closest('[data-ph-task-menu]')) return;
        var sid = btn.dataset.phTaskOpen;
        if (!sid) return;
        phState.sessionId = sid;
        setActiveSession(sid);
      };
    });
    box.querySelectorAll('[data-ph-task-menu]').forEach(function (btn) {
      btn.onclick = function (e) {
        e.stopPropagation();
        openPhTaskMenu(btn);
      };
    });
    return;
  }
  if (phState.tab === 'feed' || phState.tab === 'asset') {
    box.innerHTML = renderPhDatasourceList();
    return;
  }
  var emptyKey = 'ph.empty.' + phState.tab;
  box.innerHTML = '<div class="ph-empty"><div class="ph-empty-icon" data-ga-icon="inbox"></div><div class="ph-empty-text">' + escapeHtml(t(emptyKey)) + '</div></div>';
}

function getFilteredProjectTodos() {
  var list = Array.isArray(phState.todos) ? phState.todos.slice() : [];
  var query = (phState.todoQuery || '').trim().toLowerCase();
  var ownership = phState.ownership || 'all';
  if (ownership === 'mine') {
    list = list.filter(function (it) { return !!(it.assignee || '').trim(); });
  } else if (ownership === 'unassigned') {
    list = list.filter(function (it) { return !(it.assignee || '').trim(); });
  }
  if (query) {
    list = list.filter(function (it) {
      return (it.title || '').toLowerCase().indexOf(query) >= 0 || (it.desc || '').toLowerCase().indexOf(query) >= 0;
    });
  }
  return list;
}

function phOwnershipLabel() {
  var key = phState.ownership || 'all';
  if (key === 'mine') return '我负责的';
  if (key === 'unassigned') return '未分配';
  return '全部任务';
}

function renderPhPlan() {
  var cols = [
    { key: 'todo', dotClass: 'todo', icon: '', title: t('ph.plan.col.todo'), empty: t('ph.plan.empty.todo') },
    { key: 'doing', dotClass: 'doing', icon: '', title: t('ph.plan.col.doing'), empty: t('ph.plan.empty') },
    { key: 'pause', dotClass: 'pause', icon: '', title: t('ph.plan.col.pause'), empty: t('ph.plan.empty') },
    { key: 'done', dotClass: 'done', icon: 'check', title: t('ph.plan.col.done'), empty: t('ph.plan.empty') }
  ];
  var visibleTodos = getFilteredProjectTodos();
  var toolbar = '<div class="ph-plan-toolbar">'
    + '<div class="ph-plan-tleft">'
    + '<button class="ph-plan-btn ph-plan-btn-primary" data-ph-plan="newTodo"><i data-ga-icon="plus"></i>' + escapeHtml(t('ph.plan.newTodo')) + '</button>'
    + '<button class="ph-plan-btn ph-plan-btn-ghost" data-ph-plan="addSource"><i data-ga-icon="plus"></i>' + escapeHtml(t('ph.plan.addSource')) + '</button>'
    + '</div>'
    + '<div class="ph-plan-tright">'
    + '<button class="ph-plan-select" data-ph-plan="filterOwnership">' + escapeHtml(phOwnershipLabel()) + '<i data-ga-icon="caretDown"></i></button>'
    + '<div class="ph-plan-search"><i data-ga-icon="magnifyingGlass"></i><input type="text" class="ph-plan-search-input" data-ph-plan-input="search" placeholder="搜索任务标题 / 描述" value="' + escapeHtml(phState.todoQuery || '') + '"></div>'
    + '</div>'
    + '</div>';
  var colHtml = cols.map(function (c) {
    var dot = c.icon ? '<span class="ph-kanban-dot ' + c.dotClass + '"><i data-ga-icon="' + c.icon + '"></i></span>' : '<span class="ph-kanban-dot ' + c.dotClass + '"></span>';
    var items = visibleTodos.filter(function (it) { return (it.status || 'todo') === c.key; });
    var body = items.length
      ? items.map(function (it) { return renderTodoCard(it); }).join('')
      : '<div class="ph-kanban-empty">' + escapeHtml(c.empty) + '</div>';
    return '<div class="ph-kanban-col" data-ph-col="' + c.key + '">'
      + '<div class="ph-kanban-head">'
      + dot
      + '<span class="ph-kanban-title">' + escapeHtml(c.title) + '</span>'
      + '<span class="ph-kanban-count">' + items.length + '</span>'
      + '<button class="ph-kanban-add" data-ph-plan="colAdd" data-col="' + c.key + '" aria-label="add"><i data-ga-icon="plus"></i></button>'
      + '</div>'
      + '<div class="ph-kanban-body">' + body + '</div>'
      + '</div>';
  }).join('');
  return toolbar + '<div class="ph-kanban">' + colHtml + '</div>';
}

function renderTodoCard(it) {
  var status = it.status || 'todo';
  var dueHtml = it.due ? '<span class="ph-card-due"><i data-ga-icon="calendar"></i> ' + escapeHtml(it.due) + '</span>' : '';
  var asgHtml = '<span class="ph-card-assignee">' + escapeHtml((it.assignee || '').trim() || '未分配') + '</span>';
  return '<div class="ph-card" data-todo-id="' + it.id + '">'
    + '<button class="ph-card-main" type="button" data-ph-plan="todoDetail" data-tid="' + it.id + '">'
    + '<div class="ph-card-head">'
    + '<span class="ph-card-status-dot ' + status + '"></span>'
    + '<span class="ph-card-title">' + escapeHtml(it.title || '') + '</span>'
    + '</div>'
    + (it.desc ? '<div class="ph-card-desc">' + escapeHtml(it.desc) + '</div>' : '')
    + '<div class="ph-card-foot">' + asgHtml + dueHtml + '</div>'
    + '</button>'
    + '<button class="ph-card-del" type="button" data-ph-plan="delTodo" data-tid="' + it.id + '" aria-label="delete"><i data-ga-icon="x"></i></button>'
    + '</div>';
}

async function deleteTodoApi(tid) {
  if (!phState.project || !tid) return false;
  try {
    var res = await fetch('/projects/' + encodeURIComponent(phState.project) + '/todos/' + encodeURIComponent(tid), { method: 'DELETE' });
    if (res.ok) {
      phState.todos = (phState.todos || []).filter(function (it) { return it.id !== tid; });
      return true;
    }
  } catch (e) { console.warn('deleteTodo failed', e); }
  return false;
}

function bindPhPlan(box) {
  var searchInput = box.querySelector('[data-ph-plan-input="search"]');
  if (searchInput) {
    searchInput.oninput = function () {
      phState.todoQuery = searchInput.value || '';
      renderPhContent();
    };
  }
  box.querySelectorAll('[data-ph-plan]').forEach(function (btn) {
    btn.onclick = function (e) {
      var action = btn.dataset.phPlan;
      if (action === 'newTodo' || action === 'colAdd') { openNewTodoModal(btn.dataset.col); return; }
      if (action === 'todoDetail') {
        var tid = btn.dataset.tid;
        var todo = (phState.todos || []).find(function (it) { return String(it.id) === String(tid); });
        if (todo) openTodoDetailModal(todo);
        return;
      }
      if (action === 'delTodo') {
        var tid = btn.dataset.tid;
        if (!tid) return;
        var card = btn.closest('.ph-card');
        var ttl = card ? (card.querySelector('.ph-card-title') || {}).textContent : '';
        if (confirm((ttl ? '删除待办「' + ttl + '」？' : '删除该待办？'))) {
          deleteTodoApi(tid).then(function (ok) { if (ok) renderPhContent(); });
        }
        return;
      }
      if (action === 'addSource') { openAddSourceModal(); return; }
      if (action === 'filterOwnership') {
        e.stopPropagation();
        var menu = document.createElement('div');
        menu.className = 'nt-dropdown';
        [
          { key: 'all', label: '全部任务' },
          { key: 'mine', label: '我负责的' },
          { key: 'unassigned', label: '未分配' }
        ].forEach(function (opt) {
          var item = document.createElement('button');
          item.type = 'button';
          item.className = 'nt-dd-item' + ((phState.ownership || 'all') === opt.key ? ' active' : '');
          item.textContent = opt.label;
          item.onclick = function () {
            phState.ownership = opt.key;
            menu.remove();
            renderPhContent();
          };
          menu.appendChild(item);
        });
        openNtDropdown(menu, btn);
        return;
      }
      var labels = { batch: 'ph.plan.batch', filterSource: 'ph.plan.filter.source', search: 'ph.plan.searchPh' };
      var key = labels[action] || 'ph.plan.newTodo';
      alert(t(key) + (btn.dataset.col ? ' \u2192 ' + t('ph.plan.col.' + btn.dataset.col) : ''));
    };
  });
}

async function updateTodoApi(tid, payload) {
  if (!phState.project || !tid) return null;
  try {
    var res = await fetch('/projects/' + encodeURIComponent(phState.project) + '/todos/' + encodeURIComponent(tid), {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload || {})
    });
    if (!res.ok) throw new Error(await res.text());
    return await res.json();
  } catch (e) {
    showError('更新待办失败: ' + (e.message || e));
    return null;
  }
}

function openTodoDetailModal(todo) {
  var modal = document.getElementById('todo-detail-modal');
  if (!modal || !todo) return;
  var titleEl = document.getElementById('td-title');
  var badgeEl = document.getElementById('td-status-badge');
  var descEl = document.getElementById('td-desc');
  var assigneeEl = document.getElementById('td-assignee');
  var dueBtn = document.getElementById('td-due');
  var dueInput = document.getElementById('td-due-input');
  var statusBtn = document.getElementById('td-status');
  var sourceEl = document.getElementById('td-source');
  var saveBtn = document.getElementById('td-save');
  var addContextBtn = document.getElementById('td-add-context');
  var moreBtn = document.getElementById('td-more');
  var cur = {
    id: todo.id,
    title: todo.title || '',
    desc: todo.desc || '',
    status: todo.status || 'todo',
    assignee: todo.assignee || '',
    due: todo.due || '',
    source: todo.source || '手动创建'
  };
  function buildTodoContextText() {
    var lines = ['[计划事项上下文]'];
    if (cur.title) lines.push('标题：' + cur.title);
    if (cur.desc) lines.push('描述：' + cur.desc);
    lines.push('状态：' + ntStatusLabel(cur.status || 'todo'));
    if (cur.assignee) lines.push('处理人：' + cur.assignee);
    if (cur.due) lines.push('截止日期：' + cur.due);
    if (cur.source) lines.push('来源：' + cur.source);
    return lines.join('\n');
  }
  function appendTodoContextToComposer() {
    var input = document.getElementById('ph-composer-input');
    if (!input) return false;
    var block = buildTodoContextText();
    input.value = input.value ? (input.value.replace(/\s*$/, '') + '\n\n' + block + '\n') : (block + '\n');
    input.dispatchEvent(new Event('input', { bubbles: true }));
    input.focus();
    try { input.setSelectionRange(input.value.length, input.value.length); } catch (_) {}
    return true;
  }
  modal.dataset.todoId = cur.id || '';
  if (titleEl) titleEl.value = cur.title;
  if (descEl) descEl.value = cur.desc;
  if (assigneeEl) assigneeEl.value = cur.assignee;
  if (sourceEl) sourceEl.textContent = cur.source;
  if (addContextBtn) addContextBtn.onclick = function () {
    if (appendTodoContextToComposer()) showToast('已添加到项目输入框');
  };
  if (moreBtn) moreBtn.onclick = function (e) {
    e.stopPropagation();
    var menu = document.createElement('div');
    menu.className = 'nt-dropdown';
    var item = document.createElement('button');
    item.type = 'button';
    item.className = 'nt-dd-item';
    item.textContent = '删除事项';
    item.onclick = function () {
      menu.remove();
      if (!cur.id) return;
      var title = (titleEl && titleEl.value.trim()) || cur.title || '';
      if (!confirm(title ? ('删除待办「' + title + '」？') : '删除该待办？')) return;
      deleteTodoApi(cur.id).then(function (ok) {
        if (!ok) return;
        renderPhContent();
        closeTodoDetailModal();
        showToast('已删除事项');
      });
    };
    menu.appendChild(item);
    openNtDropdown(menu, moreBtn);
  };
  function renderStatus() {
    var statusLabel = ntStatusLabel(cur.status || 'todo');
    if (badgeEl) {
      badgeEl.className = 'todo-detail-badge ' + (cur.status || 'todo');
      badgeEl.textContent = statusLabel;
    }
    if (statusBtn) statusBtn.innerHTML = escapeHtml(statusLabel) + ' <span class="nt-caret">▾</span>';
  }
  function renderDue() {
    if (dueBtn) dueBtn.innerHTML = (cur.due ? '📅 ' + escapeHtml(cur.due) : '📅 截止日期') + ' <span class="nt-caret">▾</span>';
    if (dueInput) dueInput.value = cur.due || '';
  }
  renderStatus();
  renderDue();
  if (statusBtn) statusBtn.onclick = function () {
    var menu = document.createElement('div');
    menu.className = 'nt-dropdown';
    TODO_STATUS_OPTS.forEach(function (k) {
      var item = document.createElement('button');
      item.type = 'button';
      item.className = 'nt-dd-item' + (k === cur.status ? ' active' : '');
      item.textContent = ntStatusLabel(k);
      item.onclick = function () { cur.status = k; renderStatus(); menu.remove(); };
      menu.appendChild(item);
    });
    openNtDropdown(menu, statusBtn);
  };
  var fp = null;
  if (dueInput && typeof flatpickr === 'function') {
    fp = flatpickr(dueInput, { dateFormat: 'Y-m-d', clickOpens: false,
      defaultDate: cur.due || null,
      onChange: function (sel) { cur.due = sel && sel[0] ? flatpickr.formatDate(sel[0], 'Y-m-d') : ''; renderDue(); } });
  }
  if (dueBtn) dueBtn.onclick = function () { if (fp) fp.open(); };
  function syncState() {
    cur.title = titleEl ? String(titleEl.value || '').trim() : '';
    cur.desc = descEl ? String(descEl.value || '') : '';
    cur.assignee = assigneeEl ? String(assigneeEl.value || '').trim() : '';
    if (saveBtn) saveBtn.disabled = !cur.title;
  }
  if (titleEl) titleEl.oninput = function () { cur.title = titleEl.value; syncState(); };
  if (descEl) descEl.oninput = function () { cur.desc = descEl.value; syncState(); };
  if (assigneeEl) assigneeEl.oninput = function () { cur.assignee = assigneeEl.value.trim(); syncState(); };
  syncState();
  if (saveBtn) saveBtn.onclick = async function () {
    syncState();
    if (!cur.title || !cur.id) return;
    saveBtn.disabled = true;
    var beforeText = saveBtn.textContent;
    saveBtn.textContent = '保存中...';
    var updated = await updateTodoApi(cur.id, {
      title: cur.title,
      desc: cur.desc,
      status: cur.status,
      assignee: cur.assignee,
      due: cur.due
    });
    saveBtn.textContent = beforeText;
    if (!updated) { syncState(); return; }
    phState.todos = (phState.todos || []).map(function (it) { return it.id === updated.id ? updated : it; });
    renderPhContent();
    closeTodoDetailModal();
  };
  modal.hidden = false;
  if (typeof gaHydrateIcons === 'function') gaHydrateIcons(modal);
  setTimeout(function () { if (titleEl) titleEl.focus(); }, 50);
}

function closeTodoDetailModal() {
  var modal = document.getElementById('todo-detail-modal');
  if (modal) modal.hidden = true;
}

document.addEventListener('click', function (e) {
  var modal = document.getElementById('todo-detail-modal');
  if (!modal || modal.hidden) return;
  if (e.target.closest('#todo-detail-modal [data-close]')) closeTodoDetailModal();
}, true);

document.addEventListener('keydown', function (e) {
  if (e.key === 'Escape') closeTodoDetailModal();
}, true);

var NT_STATE = 'todo';

function openNewTodoModal(colKey) {
  var modal = document.getElementById('newtodo-modal');
  if (!modal) return;
  var titleEl = document.getElementById('nt-title');
  var descEl = document.getElementById('nt-desc');
  var statusBtn = document.getElementById('nt-status');
  var asgBtn = document.getElementById('nt-assignee');
  var dueBtn = document.getElementById('nt-due');
  var dueInput = document.getElementById('nt-due-input');
  var createBtn = document.getElementById('nt-create');
  NT_STATE = colKey || 'todo';
  if (titleEl) titleEl.value = '';
  if (descEl) descEl.value = '';
  var cur = { assignee: '', due: '' };
  function setStatusLabel() { if (statusBtn) statusBtn.innerHTML = ntStatusLabel(NT_STATE) + ' <span class="nt-caret">▾</span>'; }
  setStatusLabel();
  if (asgBtn) asgBtn.innerHTML = '处理人 <span class="nt-caret">▾</span>';
  if (dueBtn) dueBtn.innerHTML = '📅 截止日期 <span class="nt-caret">▾</span>';
  function syncCreate() { if (createBtn) createBtn.disabled = !(titleEl && titleEl.value.trim()); }
  if (titleEl) titleEl.oninput = syncCreate;
  if (statusBtn) statusBtn.onclick = function (e) {
    e.stopPropagation();
    var menu = document.createElement('div');
    menu.className = 'nt-dropdown';
    TODO_STATUS_OPTS.forEach(function (k) {
      var opt = document.createElement('div');
      opt.className = 'nt-dropdown-item' + (k === NT_STATE ? ' active' : '');
      opt.textContent = ntStatusLabel(k);
      opt.onclick = function () { NT_STATE = k; setStatusLabel(); menu.remove(); };
      menu.appendChild(opt);
    });
    openNtDropdown(menu, statusBtn);
  };
  if (asgBtn) asgBtn.onclick = function () {
    var v = prompt('处理人', cur.assignee);
    if (v !== null) { cur.assignee = v.trim(); asgBtn.innerHTML = (cur.assignee || '处理人') + ' <span class="nt-caret">▾</span>'; }
  };
  var fp = null;
  if (dueInput && typeof flatpickr === 'function') {
    fp = flatpickr(dueInput, { dateFormat: 'Y-m-d', clickOpens: false,
      onChange: function (sel) { cur.due = sel && sel[0] ? flatpickr.formatDate(sel[0], 'Y-m-d') : ''; if (dueBtn) dueBtn.innerHTML = (cur.due ? '📅 ' + cur.due : '📅 截止日期') + ' <span class="nt-caret">▾</span>'; } });
  }
  if (dueBtn) dueBtn.onclick = function () {
    if (fp) { fp.open(); return; }
    var v = prompt('截止日期 (YYYY-MM-DD)', cur.due);
    if (v !== null) { cur.due = v.trim(); dueBtn.innerHTML = (cur.due ? '📅 ' + cur.due : '📅 截止日期') + ' <span class="nt-caret">▾</span>'; }
  };
  if (createBtn) createBtn.onclick = function () {
    if (!titleEl || !titleEl.value.trim()) return;
    createBtn.disabled = true;
    var payload = { title: titleEl.value.trim(), desc: descEl ? descEl.value : '', status: NT_STATE, assignee: cur.assignee, due: cur.due };
    fetch('/projects/' + encodeURIComponent(phState.project) + '/todos', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
    }).then(function (r) { return r.json(); }).then(function (data) {
      if (data && data.todo) {
        phState.todos.push(data.todo);
        if (typeof closeModals === 'function') closeModals(); else modal.hidden = true;
        renderPhContent();
      } else { createBtn.disabled = false; alert((data && data.error) || '创建失败'); }
    }).catch(function (err) { createBtn.disabled = false; alert('创建失败: ' + err); });
  };
  if (createBtn) createBtn.disabled = true;
  openModal('newtodo-modal');
  if (typeof gaHydrateIcons === 'function') gaHydrateIcons();
  if (titleEl) setTimeout(function () { titleEl.focus(); }, 50);
}

function openNtDropdown(menu, anchor) {
  document.querySelectorAll('.nt-dropdown').forEach(function (m) { m.remove(); });
  document.body.appendChild(menu);
  var r = anchor.getBoundingClientRect();
  menu.style.position = 'fixed';
  menu.style.left = r.left + 'px';
  menu.style.top = (r.bottom + 2) + 'px';
  menu.style.zIndex = 10050;
  function close(e) { if (!menu.contains(e.target)) { menu.remove(); document.removeEventListener('click', close, true); } }
  setTimeout(function () { document.addEventListener('click', close, true); }, 0);
}

/* ===== 添加数据源弹窗 ===== */
var ADDSOURCE_LIST = [
  { key: 'tapd', name: 'TAPD', avatar: 'T', badge: '🕐 定时导入', desc: 'TAPD 敏捷项目管理平台，支持缺陷、需求等数据的定时同步。', action: '去授权 ↗' },
  { key: 'cnb', name: 'CNB', avatar: 'C', badge: '🕐 定时导入', desc: 'CNB 代码托管平台，支持仓库、Issue 等数据的定时同步。', action: '去授权 ↗' },
  { key: 'github', name: 'GitHub', avatar: 'G', badge: '⚡ 事件触发', desc: '代码仓库操作触发同步，适合实时联动场景。', action: '选择',
    config: { subtitle: '配置 GitHub Webhook', defaultName: 'GitHub', button: '生成 Webhook', doneSubtitle: 'Webhook 已创建', doneTip: '在 GitHub 仓库 Settings → Webhooks 中，将上方 Payload URL 填入，Secret 填入 Secret，并勾选对应事件。', finishTip: 'GitHub 数据源已创建。接下来请去对应 GitHub 仓库配置 Webhook，收到事件后才会开始同步。', events: [
      { value: 'issues_opened', label: 'Issue 创建', checked: true },
      { value: 'issues_edited', label: 'Issue 更新', checked: true },
      { value: 'pull_request_opened', label: 'Pull Request 创建', checked: true },
      { value: 'pull_request_edited', label: 'Pull Request 更新', checked: false }
    ] } },
  { key: 'gitlab', name: 'GitLab', avatar: 'L', badge: '⚡ 事件触发', desc: 'GitLab 代码托管平台，通过 Webhook 实时推送 Issue / Merge Request 事件。', action: '选择',
    config: { subtitle: '配置 GitLab Webhook', defaultName: 'GitLab', button: '生成 Webhook', doneSubtitle: 'Webhook 已创建', doneTip: '在 GitLab 项目 Settings → Webhooks 中，将上方地址填入 URL、密钥填入 Secret token，并勾选对应事件。', finishTip: 'GitLab 数据源已创建。接下来请去对应 GitLab 项目配置 Webhook，收到事件后才会开始同步。', events: [
      { value: 'issue_open', label: 'Issue 创建', checked: true },
      { value: 'issue_update', label: 'Issue 更新', checked: true },
      { value: 'merge_request_open', label: 'Merge Request 创建', checked: true },
      { value: 'merge_request_update', label: 'Merge Request 更新', checked: false }
    ] } }
];

function getAsDatasourceStatus(ds) {
  var events = Array.isArray(ds && ds.event_log) ? ds.event_log : [];
  var last = events.length ? events[events.length - 1] : null;
  var text = '未收到事件';
  var cls = 'is-idle';
  if (last) {
    var raw = String(last.type || last.status || last.level || '').toLowerCase();
    if (/fail|error|invalid|forbid|denied/.test(raw)) {
      text = '异常';
      cls = 'is-error';
    } else {
      text = '运行中';
      cls = '';
    }
  } else if (Array.isArray(ds && ds.events) && ds.events.length) {
    text = '已配置';
    cls = '';
  }
  return { text: text, cls: cls };
}

function renderAsExistingDatasources() {
  var wrap = document.getElementById('as-existing');
  var listEl = document.getElementById('as-existing-list');
  if (!wrap || !listEl) return;
  var list = Array.isArray(phState.datasources) ? phState.datasources : [];
  if (!list.length) {
    wrap.hidden = true;
    listEl.innerHTML = '';
    return;
  }
  wrap.hidden = false;
  listEl.innerHTML = list.map(function (ds, idx) {
    var status = getAsDatasourceStatus(ds || {});
    var avatar = escapeHtml(String((ds && (ds.name || ds.type || 'D')).slice(0, 1) || 'D').toUpperCase());
    var recent = Array.isArray(ds && ds.event_log) ? ds.event_log.slice(-2).reverse() : [];
    var meta = [ds && ds.type ? ds.type : '', ds && ds.name ? ds.name : '', Array.isArray(ds && ds.events) && ds.events.length ? ('订阅 ' + ds.events.length + ' 类事件') : ''].filter(Boolean).join(' \u00b7 ');
    return ''
      + '<div class="as-ds-item" data-ds-index="' + idx + '">'
      +   '<div class="as-ds-avatar">' + avatar + '</div>'
      +   '<div class="as-ds-main">'
      +     '<div class="as-ds-top"><div class="as-ds-name">' + escapeHtml((ds && (ds.name || ds.type)) || '未命名数据源') + '</div><div class="as-ds-type">' + escapeHtml((ds && ds.type) || '') + '</div></div>'
      +     '<div class="as-ds-status ' + status.cls + '">' + status.text + '</div>'
      +     '<div class="as-ds-meta">' + escapeHtml(meta || '暂无描述') + '</div>'
      +     (recent.length ? ('<div class="as-ds-events">' + recent.map(function (ev) {
                var v = formatDatasourceEvent(ev);
                return '<div class="as-ds-event"><strong>' + escapeHtml(v.title) + '</strong>'
                  + (v.meta.length ? '<span class="as-ds-event-time">' + escapeHtml(v.meta[v.meta.length - 1]) + '</span>' : '') + '</div>';
              }).join('') + '</div>') : '<div class="as-ds-empty-events">暂无最近事件</div>')
      +   '</div>'
      +   '<div class="as-ds-actions"><button type="button" class="as-ds-menu-btn" data-ds-menu="' + idx + '" aria-label="更多操作" title="更多操作"><span data-ga-icon="dotsThree"></span></button></div>'
      + '</div>';
  }).join('');
  listEl.querySelectorAll('[data-ds-menu]').forEach(function (btn) {
    btn.onclick = function (e) {
      e.stopPropagation();
      var idx = Number(btn.getAttribute('data-ds-menu'));
      var ds = list[idx];
      if (!ds) return;
      var menu = document.createElement('div');
      menu.className = 'nt-dropdown';
      menu.innerHTML = ''
        + '<div class="ctx-item" data-act="copy-webhook"><span data-ga-icon="link"></span><span>复制 Webhook 地址</span></div>'
        + '<div class="ctx-item ' + ((ds && ds.secret) ? '' : 'disabled') + '" data-act="copy-secret"><span data-ga-icon="key"></span><span>复制 Secret</span></div>'
        + '<div class="ctx-item" data-act="show-events"><span data-ga-icon="listBullets"></span><span>查看最近事件</span></div>'
        + '<div class="ctx-item" data-act="reconfig"><span data-ga-icon="pencilSimple"></span><span>重新配置</span></div>';
      menu.querySelectorAll('.ctx-item:not(.disabled)').forEach(function (item) {
        item.onclick = function () {
          var act = item.getAttribute('data-act');
          if (act === 'copy-webhook' && ds.webhook_url && navigator.clipboard) {
            navigator.clipboard.writeText(ds.webhook_url).then(function () { showToast('已复制 Webhook 地址'); }).catch(function () {});
          } else if (act === 'copy-secret' && ds.secret && navigator.clipboard) {
            navigator.clipboard.writeText(ds.secret).then(function () { showToast('已复制 Secret'); }).catch(function () {});
          } else if (act === 'show-events') {
            var events = Array.isArray(ds.event_log) ? ds.event_log.slice(-8).reverse() : [];
            alert(events.length ? events.map(function (ev) {
              var v = formatDatasourceEvent(ev);
              var when = v.meta.length ? (' [' + v.meta.join(' ') + ']') : '';
              return v.title + when;
            }).join('\n') : '暂无最近事件');
          } else if (act === 'reconfig') {
            asShowView('list');
          }
          menu.remove();
        };
      });
      openNtDropdown(menu, btn);
      if (typeof gaHydrateIcons === 'function') gaHydrateIcons(menu);
    };
  });
  if (typeof gaHydrateIcons === 'function') gaHydrateIcons(listEl);
}

function openAddSourceModal() {
  var modal = document.getElementById('addsource-modal');
  if (!modal) return;
  asShowView('empty');
  var goList = document.getElementById('as-go-list');
  if (goList) goList.onclick = function () { asShowView('list'); };
  var addMore = document.getElementById('as-add-more');
  if (addMore) addMore.onclick = function () { asShowView('list'); };
  var back = document.getElementById('as-back');
  if (back) back.onclick = function () { asShowView('empty'); };
  renderAsExistingDatasources();
  if (phState.project) loadProjectDatasources(phState.project).then(function () { renderAsExistingDatasources(); });
  openModal('addsource-modal');
  if (typeof gaHydrateIcons === 'function') gaHydrateIcons();
}

var asCurrentSource = null;

function asShowView(view) {
  var empty = document.getElementById('as-view-empty');
  var list = document.getElementById('as-view-list');
  var config = document.getElementById('as-view-config');
  var done = document.getElementById('as-view-done');
  if (empty) empty.hidden = (view !== 'empty');
  if (list) list.hidden = (view !== 'list');
  if (config) config.hidden = (view !== 'config');
  if (done) done.hidden = (view !== 'done');
  if (view === 'list') asRenderCards();
  if (view === 'config') bindAsDatasourceConfig();
  if (view === 'done') bindAsDatasourceDone();
}

function asRenderCards() {
  var box = document.getElementById('as-cards');
  if (!box || box.dataset.rendered === '1') return;
  box.innerHTML = ADDSOURCE_LIST.map(function (s) {
    return '<div class="as-card" data-source="' + s.key + '">'
      + '<div class="as-card-icon">' + escapeHtml(s.avatar) + '</div>'
      + '<div class="as-card-main">'
        + '<div class="as-card-name">' + escapeHtml(s.name) + ' <span class="as-card-badge">' + escapeHtml(s.badge) + '</span></div>'
        + '<div class="as-card-desc">' + escapeHtml(s.desc) + '</div>'
      + '</div>'
      + '<button type="button" class="as-card-btn" data-source="' + s.key + '">' + escapeHtml(s.action) + '</button>'
    + '</div>';
  }).join('');
  box.dataset.rendered = '1';
  if (typeof gaHydrateIcons === 'function') gaHydrateIcons();
  box.querySelectorAll('.as-card-btn').forEach(function (btn) {
    btn.onclick = function () {
      var src = btn.dataset.source;
      var found = ADDSOURCE_LIST.find(function (s) { return s.key === src; });
      if (found && found.config) {
        asCurrentSource = found;
        asShowView('config');
      } else {
        alert('该数据源接入流程待实现');
      }
    };
  });
}

/* ===== 数据源配置（GitLab/GitHub） ===== */
function bindAsDatasourceConfig() {
  var cfg = asCurrentSource && asCurrentSource.config;
  if (!cfg) return;
  var subtitle = document.getElementById('as-config-subtitle');
  var nameEl = document.getElementById('as-ds-name');
  var eventsBox = document.getElementById('as-ds-events');
  var back = document.getElementById('as-ds-back');
  var gen = document.getElementById('as-ds-generate');
  if (subtitle) subtitle.textContent = cfg.subtitle || '配置 Webhook';
  if (nameEl) {
    nameEl.value = cfg.defaultName || (asCurrentSource && asCurrentSource.name) || '';
    nameEl.placeholder = '例如：我的 ' + ((asCurrentSource && asCurrentSource.name) || '数据源');
  }
  if (eventsBox) {
    eventsBox.innerHTML = (cfg.events || []).map(function (evt) {
      return '<label class="as-check"><input type="checkbox" value="' + escapeHtml(evt.value) + '"' + (evt.checked ? ' checked' : '') + '> ' + escapeHtml(evt.label) + '</label>';
    }).join('');
  }
  if (back) back.onclick = function () { asShowView('list'); };
  if (gen) {
    gen.textContent = cfg.button || '生成 Webhook';
    gen.onclick = asDatasourceGenerate;
  }
}

async function asDatasourceGenerate() {
  var cfg = asCurrentSource && asCurrentSource.config;
  if (!cfg || !asCurrentSource) return;
  var nameEl = document.getElementById('as-ds-name');
  var name = (nameEl && nameEl.value.trim()) || cfg.defaultName || asCurrentSource.name || 'Datasource';
  var events = Array.prototype.slice.call(document.querySelectorAll('#as-ds-events input:checked'))
    .map(function (c) { return c.value; });
  if (!events.length) { alert('请至少选择一个订阅事件'); return; }
  var gen = document.getElementById('as-ds-generate');
  if (gen) gen.disabled = true;
  try {
    var body = { type: asCurrentSource.key, name: name, events: events };
    if (phState && phState.project) body.project = phState.project;
    var resp = await fetch('/datasources', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    var res = await resp.json();
    if (!resp.ok) throw new Error(res.error || '创建失败');
    var ds = (res && res.datasource) || {};
    if (ds.id) {
      document.getElementById('as-ds-url').textContent = ds.webhook_url || '';
      document.getElementById('as-ds-secret').textContent = ds.secret || '';
      var doneSubtitle = document.getElementById('as-done-subtitle');
      var doneTip = document.getElementById('as-done-tip');
      if (doneSubtitle) doneSubtitle.textContent = cfg.doneSubtitle || 'Webhook 已创建';
      if (doneTip) doneTip.textContent = cfg.doneTip || '';
      if (phState && phState.project) loadProjectDatasources(phState.project).then(function () { if (phState.project) renderPhContent(); });
      asShowView('done');
    } else {
      alert('创建失败：' + (res && res.error ? res.error : '未知错误'));
    }
  } catch (e) {
    alert('创建失败：' + (e && e.message ? e.message : e));
  } finally {
    if (gen) gen.disabled = false;
  }
}

function bindAsDatasourceDone() {
  var cfg = asCurrentSource && asCurrentSource.config;
  var copy = document.getElementById('as-ds-copy');
  var finish = document.getElementById('as-ds-finish');
  if (copy) copy.onclick = function () {
    var urlEl = document.getElementById('as-ds-url');
    var secretEl = document.getElementById('as-ds-secret');
    var txt = 'URL: ' + (urlEl ? urlEl.textContent : '') + '\nSecret: ' + (secretEl ? secretEl.textContent : '');
    if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(txt);
  };
  if (finish) finish.onclick = function () {
    if (phState.project) {
      loadProjectDatasources(phState.project).then(function () {
        renderAsExistingDatasources();
        asShowView('empty');
        if (phState.tab === 'datasource') renderPhContent();
      });
    } else {
      asShowView('empty');
    }
    alert((cfg && cfg.finishTip) || '数据源已创建。');
  };
}

function openNewProjectModal(prefillName, tplKey) {
  if (!tplKey && prefillName) {
    const found = PROJECT_TEMPLATES.find(function (p) { return t('project.tpl.' + p.key + '.t') === prefillName; });
    if (found) tplKey = found.key;
  }
  const sel = document.getElementById('pc-template');
  if (sel && !sel.dataset.filled) {
    sel.innerHTML = '<option value="">' + escapeHtml(t('project.tplBlank')) + '</option>' +
      PROJECT_TEMPLATES.map(function (tpl) {
        return '<option value="' + escapeHtml(tpl.key) + '">' + escapeHtml(t('project.tpl.' + tpl.key + '.t')) + '</option>';
      }).join('');
    sel.dataset.filled = '1';
  }
  const nameEl = document.getElementById('pc-name');
  const insEl = document.getElementById('pc-instruction');
  const errEl = document.getElementById('pc-error');
  if (nameEl) nameEl.value = prefillName || '';
  if (sel) sel.value = tplKey || '';
  if (insEl) {
    const tpl = tplKey ? PROJECT_TEMPLATES.find(function (p) { return p.key === tplKey; }) : null;
    insEl.value = tpl ? tpl.instruction : '';
  }
  if (errEl) { errEl.hidden = true; errEl.textContent = ''; }
  // 加载已有 workspace 列表到下拉
  loadWorkspaceOptions('pc-ws-existing');
  // radio 联动：默认无绑定，切换时 enable/disable 对应输入
  setupWorkspaceRadios('pc-ws-mode', 'pc-ws-existing', 'pc-ws-path');
  openModal('project-create-modal');
  if (typeof gaHydrateIcons === 'function') gaHydrateIcons();
  if (nameEl) setTimeout(function () { nameEl.focus(); }, 50);
  renderProjectSkills();
}

async function loadWorkspaceOptions(selId) {
  const sel = document.getElementById(selId);
  if (!sel) return;
  sel.innerHTML = '<option value="">—</option>';
  try {
    const res = await window.ga.listWorkspaces();
    const items = (res && res.workspaces) || [];
    sel.innerHTML = '<option value="">—</option>' + items.map(function (w) {
      return '<option value="' + escapeHtml(w.name || '') + '">' + escapeHtml(w.name || '') + (w.path ? ' (' + escapeHtml(w.path) + ')' : '') + '</option>';
    }).join('');
  } catch (_) {}
}

function setupWorkspaceRadios(modeName, selId, pathId) {
  const radios = document.querySelectorAll('input[name="' + modeName + '"]');
  const sel = document.getElementById(selId);
  const pathEl = document.getElementById(pathId);
  function update() {
    const checked = document.querySelector('input[name="' + modeName + '"]:checked');
    const mode = checked ? checked.value : 'none';
    if (sel) sel.disabled = (mode !== 'existing');
    if (pathEl) pathEl.disabled = (mode !== 'new');
  }
  radios.forEach(function (r) { r.addEventListener('change', update); });
  update();
}

async function renderProjectSkills(selected, boxId, fieldId) {
  const field = document.getElementById(fieldId || 'pc-skills-field');
  const box = document.getElementById(boxId || 'pc-skills');
  if (!field || !box) return;
  field.hidden = false;
  const selSet = (Array.isArray(selected) && selected.length) ? new Set(selected) : null;
  box.innerHTML = '<span class="pc-skills-loading">' + escapeHtml(t('project.skillsLoading')) + '</span>';
  try {
    const res = await fetch('/api/skills');
    const data = await res.json();
    const skills = (data && Array.isArray(data.skills)) ? data.skills : [];
    if (!skills.length) {
      box.innerHTML = '<span class="pc-skills-none">' + escapeHtml(t('project.skillsNone')) + '</span>';
      return;
    }
    box.innerHTML = skills.map(function (sk) {
      const name = escapeHtml(sk.name || '');
      const desc = escapeHtml(sk.description || '');
      const checked = selSet ? (selSet.has(sk.name) ? 'checked' : '') : 'checked';
      return '<label class="pc-skill-item"><input type="checkbox" value="' + name + '" ' + checked + '>' +
        '<span class="pc-skill-name">' + name + '</span>' +
        (desc ? '<span class="pc-skill-desc">' + desc + '</span>' : '') +
        '</label>';
    }).join('');
  } catch (e) {
    box.innerHTML = '<span class="pc-skills-none">' + escapeHtml(t('project.skillsNone')) + '</span>';
  }
}

async function createProject(prefillName, tplKey) {
  openNewProjectModal(prefillName, tplKey);
}

async function submitNewProject() {
  const nameEl = document.getElementById('pc-name');
  const insEl = document.getElementById('pc-instruction');
  const errEl = document.getElementById('pc-error');
  const name = (nameEl ? nameEl.value : '').trim();
  if (!name) {
    if (errEl) { errEl.textContent = t('project.nameRequired'); errEl.hidden = false; }
    if (nameEl) nameEl.focus();
    return;
  }
  const instruction = (insEl ? insEl.value : '').trim();
  // 收集启用的 skills：取消勾选的 = 排除。只有部分取消时才传过滤列表。
  const skillCbs = document.querySelectorAll('#pc-skills input[type="checkbox"]');
  let skillsParam = null;
  if (skillCbs && skillCbs.length) {
    const checked = Array.from(skillCbs).filter(function (c) { return c.checked; }).map(function (c) { return c.value; });
    if (checked.length < skillCbs.length && checked.length > 0) {
      skillsParam = checked;
    }
  }
  const saveBtn = document.getElementById('pc-save');
  if (saveBtn) saveBtn.disabled = true;
  try {
    const params = { name: name };
    if (instruction) params.instruction = instruction;
    if (skillsParam) params.skills = skillsParam;
    // workspace 选择
    const wsMode = document.querySelector('input[name="pc-ws-mode"]:checked');
    if (wsMode) {
      if (wsMode.value === 'existing') {
        const wsSel = document.getElementById('pc-ws-existing');
        if (wsSel && wsSel.value) params.workspace = wsSel.value;
      } else if (wsMode.value === 'new') {
        const wsPathEl = document.getElementById('pc-ws-path');
        const wsPath = wsPathEl ? wsPathEl.value.trim() : '';
        if (wsPath) params.workspacePath = wsPath;
      }
    }
    const res = await window.ga.rpc('projects/create', params);
    if (res?.error) throw new Error(res.error.message || res.error);
    closeModals();
    await loadProjects();
    await enterProject(name);
  } catch (e) {
    if (errEl) { errEl.textContent = t('project.createErr') + ': ' + (e.message || e); errEl.hidden = false; }
  } finally {
    if (saveBtn) saveBtn.disabled = false;
  }
}

let _editSkillsProject = null;

async function openEditSkillsModal(projectName) {
  if (!projectName) return;
  _editSkillsProject = projectName;
  const modal = document.getElementById('project-skills-modal');
  if (!modal) return;
  const errEl = document.getElementById('ps-error');
  if (errEl) { errEl.hidden = true; errEl.textContent = ''; }
  openModal('project-skills-modal');
  if (typeof gaHydrateIcons === 'function') gaHydrateIcons();
  // 读取当前绑定的 skills
  let current = null;
  try {
    const res = await window.ga.rpc('projects/skills_get', { name: projectName });
    if (res?.error) throw new Error(res.error.message || res.error);
    current = (res && Array.isArray(res.skills)) ? res.skills : (res?.result?.skills || null);
  } catch (e) { /* 读取失败按全选处理 */ }
  await renderProjectSkills(current, 'ps-skills', 'ps-skills-field');
}

async function submitEditSkills() {
  if (!_editSkillsProject) return;
  const projectName = _editSkillsProject;
  const errEl = document.getElementById('ps-error');
  const saveBtn = document.getElementById('ps-save');
  // 收集勾选的 skills
  const skillCbs = document.querySelectorAll('#ps-skills input[type="checkbox"]');
  let skillsParam = null;
  if (skillCbs && skillCbs.length) {
    const checked = Array.from(skillCbs).filter(function (c) { return c.checked; }).map(function (c) { return c.value; });
    if (checked.length < skillCbs.length) {
      skillsParam = checked;  // 部分取消 = 过滤列表；全选 = null(全局注入)
    }
  }
  if (saveBtn) saveBtn.disabled = true;
  try {
    const params = { name: projectName };
    if (skillsParam) params.skills = skillsParam;
    const res = await window.ga.rpc('projects/skills_update', params);
    if (res?.error) throw new Error(res.error.message || res.error);
    closeModals();
    _editSkillsProject = null;
    // 若当前正在该项目内，提示 skills 将在下次新 session 生效
    if (window.gaToast) window.gaToast(t('project.skillsSaved'));
  } catch (e) {
    if (errEl) { errEl.textContent = (e.message || e); errEl.hidden = false; }
  } finally {
    if (saveBtn) saveBtn.disabled = false;
  }
}

let _wsProjName = '';

async function openSetWorkspaceModal(projectName) {
  if (!projectName) return;
  _wsProjName = projectName;
  const modal = document.getElementById('project-workspace-modal');
  if (!modal) return;
  const errEl = document.getElementById('pw-error');
  if (errEl) { errEl.hidden = true; errEl.textContent = ''; }
  // 加载已有 workspace 列表
  loadWorkspaceOptions('pw-ws-existing');
  // radio 联动
  setupWorkspaceRadios('pw-ws-mode', 'pw-ws-existing', 'pw-ws-path');
  // 显示当前 workspace
  const curEl = document.getElementById('pw-current');
  if (curEl) {
    try {
      const res = await window.ga.rpc('projects/list', {});
      const items = res?.projects || res?.result?.projects || [];
      const proj = items.find(function (p) { return p.name === projectName; });
      curEl.textContent = (proj && proj.workspace) ? proj.workspace : t('project.wsNone');
    } catch (_) { curEl.textContent = '—'; }
  }
  openModal('project-workspace-modal');
  if (typeof gaHydrateIcons === 'function') gaHydrateIcons();
}

async function submitSetWorkspace() {
  const errEl = document.getElementById('pw-error');
  if (errEl) { errEl.hidden = true; errEl.textContent = ''; }
  const saveBtn = document.getElementById('pw-save');
  if (saveBtn) saveBtn.disabled = true;
  try {
    const modeEl = document.querySelector('input[name="pw-ws-mode"]:checked');
    const mode = modeEl ? modeEl.value : 'none';
    const params = {};
    if (mode === 'existing') {
      const sel = document.getElementById('pw-ws-existing');
      const wsName = sel ? sel.value.trim() : '';
      if (!wsName) throw new Error(t('project.wsSelectErr'));
      params.workspace = wsName;
    } else if (mode === 'new') {
      const wsPathEl = document.getElementById('pw-ws-path');
      const wsPath = wsPathEl ? wsPathEl.value.trim() : '';
      if (!wsPath) throw new Error(t('project.wsPathErr'));
      params.workspacePath = wsPath;
    }
    // mode === 'none' → 清除 workspace（不传 workspace/workspacePath）
    const res = await window.ga.rpc('projects/workspace_update', { name: _wsProjName, workspace: params.workspace || '', workspacePath: params.workspacePath || '' });
    if (res?.error) throw new Error(res.error.message || res.error);
    closeModals();
    await loadProjects();
    showToast(t('project.wsSaved'));
  } catch (e) {
    if (errEl) { errEl.textContent = (e.message || e); errEl.hidden = false; }
  } finally {
    if (saveBtn) saveBtn.disabled = false;
  }
}

function showProjectCardMenu(menuBtn, projectName) {
  // 移除已有下拉
  document.querySelectorAll('.proj-menu-dropdown').forEach(function (d) { d.remove(); });
  const dropdown = document.createElement('div');
  dropdown.className = 'proj-menu-dropdown';
  dropdown.innerHTML = '<button type="button" data-action="edit-skills">' + escapeHtml(t('project.editSkills')) + '</button>'
    + '<button type="button" data-action="set-workspace">' + escapeHtml(t('project.workspace')) + '</button>'
    + '<button type="button" data-action="rename">' + escapeHtml(t('project.rename')) + '</button>'
    + '<button type="button" data-action="delete" class="proj-menu-danger">' + escapeHtml(t('project.delete')) + '</button>';
  menuBtn.parentElement.appendChild(dropdown);
  // 定位到菜单按钮下方
  const rect = menuBtn.getBoundingClientRect();
  const cardRect = menuBtn.parentElement.getBoundingClientRect();
  dropdown.style.position = 'absolute';
  dropdown.style.right = (cardRect.right - rect.right) + 'px';
  dropdown.style.top = (rect.bottom - cardRect.top + 2) + 'px';
  dropdown.querySelectorAll('button').forEach(function (b) {
    b.addEventListener('click', function (ev) {
      ev.stopPropagation();
      dropdown.remove();
      if (b.dataset.action === 'edit-skills') openEditSkillsModal(projectName);
      else if (b.dataset.action === 'set-workspace') openSetWorkspaceModal(projectName);
      else if (b.dataset.action === 'rename') doRenameProject(projectName);
      else if (b.dataset.action === 'delete') doDeleteProject(projectName);
    });
  });
  // 点击外部关闭(mousedown避免与触发按钮click冒泡冲突)
  setTimeout(function () {
    const closer = function (ev) {
      if (!dropdown.contains(ev.target)) { dropdown.remove(); document.removeEventListener('mousedown', closer, true); }
    };
    document.addEventListener('mousedown', closer, true);
  }, 0);
}

async function doRenameProject(projectName) {
  const newName = await showPromptDialog({ title: t('project.rename'), message: t('project.renamePrompt'), value: projectName, okText: t('common.confirm') });
  if (!newName || newName.trim() === '' || newName.trim() === projectName) return;
  const name = newName.trim();
  if (/[\/\\]/.test(name) || name.startsWith('.')) { showError(t('project.renameErr') + ': ' + t('project.nameInvalid')); return; }
  window.ga.rpc('projects/rename', { name: projectName, newName: name }).then(function () {
    loadProjects();
  }).catch(function (e) { showError(t('project.renameErr') + ': ' + (e.message || e)); });
}

async function doDeleteProject(projectName) {
  if (!(await showConfirmDialog({ title: t('common.delete'), message: t('project.deleteConfirm').replace('{0}', projectName), okText: t('common.delete'), okKind: 'danger' }))) return;
  window.ga.rpc('projects/delete', { name: projectName }).then(function () {
    loadProjects();
  }).catch(function (e) { showError(t('project.deleteErr') + ': ' + (e.message || e)); });
}

(function bindProjectCreateModal() {
  const saveBtn = document.getElementById('pc-save');
  const nameEl = document.getElementById('pc-name');
  const sel = document.getElementById('pc-template');
  if (saveBtn) saveBtn.addEventListener('click', submitNewProject);
  if (nameEl) nameEl.addEventListener('keydown', function (e) { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); submitNewProject(); } });
  if (sel) sel.addEventListener('change', async function () {
    const insEl = document.getElementById('pc-instruction');
    if (!insEl) return;
    const cur = insEl.value.trim();
    const tpl = sel.value ? PROJECT_TEMPLATES.find(function (p) { return p.key === sel.value; }) : null;
    const next = tpl ? tpl.instruction : '';
    if (cur && cur !== next) { if (!(await showConfirmDialog({ title: t('common.confirm'), message: t('project.tplOverwrite') }))) return; }
    insEl.value = next;
  });
})();

(function bindProjectSkillsModal() {
  const saveBtn = document.getElementById('ps-save');
  const cancelBtn = document.getElementById('ps-cancel');
  if (saveBtn && !saveBtn.dataset.bound) {
    saveBtn.dataset.bound = '1';
    saveBtn.addEventListener('click', submitEditSkills);
  }
  if (cancelBtn && !cancelBtn.dataset.bound) {
    cancelBtn.dataset.bound = '1';
    cancelBtn.addEventListener('click', function () { closeModals(); _editSkillsProject = null; });
  }
})();

(function bindProjectWorkspaceModal() {
  const saveBtn = document.getElementById('pw-save');
  const cancelBtn = document.getElementById('pw-cancel');
  if (saveBtn && !saveBtn.dataset.bound) {
    saveBtn.dataset.bound = '1';
    saveBtn.addEventListener('click', submitSetWorkspace);
  }
  if (cancelBtn && !cancelBtn.dataset.bound) {
    cancelBtn.dataset.bound = '1';
    cancelBtn.addEventListener('click', function () { closeModals(); _wsProjName = ''; });
  }
})();

window.loadProjects = loadProjects;
window.enterProject = enterProject;
window.createProject = createProject;
window.renderProjectTemplates = renderProjectTemplates;

(function bindProjectMgr() {
  const newBtn = document.getElementById('proj-new-btn');
  if (newBtn && !newBtn.dataset.bound) {
    newBtn.dataset.bound = '1';
    newBtn.addEventListener('click', function () { createProject(''); });
  }
  const search = document.getElementById('proj-search-input');
  if (search && !search.dataset.bound) {
    search.dataset.bound = '1';
    search.addEventListener('input', function () {
      const q = (search.value || '').toLowerCase().trim();
      const grid = document.getElementById('project-my-grid');
      if (!grid) return;
      grid.querySelectorAll('.proj-card').forEach(function (card) {
        const nm = (card.dataset.project || '').toLowerCase();
        card.style.display = (!q || nm.indexOf(q) >= 0) ? '' : 'none';
      });
    });
  }
})();

function sessionNeedsHydrate(sess) {
  return !!(sess?.bridgeSessionId && state.bridgeReady && !sess.messages.length);
}

function runSessionHydrate(sess) {
  setSessionLoading(true);
  const tid = setTimeout(() => {
    if (isActive(sess)) setSessionLoading(false);
  }, HYDRATE_LOADING_TIMEOUT_MS);
  return hydrateSession(sess).finally(() => {
    clearTimeout(tid);
    if (isActive(sess)) setSessionLoading(false);
  });
}

function setActiveSession(id) {
  setSessionLoading(false);
  // 保存旧活跃会话的输入框草稿（切回时恢复），实现每会话独立输入内容
  const _oldSess = activeSess();
  if (_oldSess && _oldSess.id !== id && inputEl) {
    rt(_oldSess).inputDraft = inputEl.innerHTML;
  }
  state.activeId = id;
  if (id) localStorage.setItem('ga_active', id);  // 持久化当前会话，刷新后固定恢复它
  const sess = state.sessions.get(id);
  if (!sess) return;
  // 选中会话 → 从后端读取该会话绑定的 workspace 并同步到前端显示（只读不写，避免用前端缓存值覆盖后端）
  if (sess.bridgeSessionId) {
    window.ga.getSessionWorkspace(sess.bridgeSessionId).then(res => {
      const ws = (res && res.workspace) || null;
      sess.workspace = ws ? ws.name : '';
      sess._workspacePath = ws ? (ws.path || '') : '';
      if (window.gaRefreshWorkspaceChip) window.gaRefreshWorkspaceChip();
    }).catch(() => {});
  }
  if (msgsEl) msgsEl.innerHTML = '';
  renderSuggestions([]);  // 切换会话：清除上一会话的推荐chips
  const r = rt(sess);
  r.draftEl = null;
  resetTypewriterState(r);
  r.justSwitched = true;  // 切换会话：首次 draft 渲染时强制滚到底（显示最新内容而非停留在最后 user 问题）
  // 恢复该会话的输入框草稿（每会话独立输入内容）
  if (inputEl) inputEl.innerHTML = r.inputDraft || '';
  renderAllMessages(sess);
  setBusy(sess, rt(sess).busy);
  renderSessionList();
  refreshPlanBar(null);
  syncPlanPollTimer();
  if (!sess.bridgeSessionId || !state.bridgeReady) return;
  if (sessionNeedsHydrate(sess)) {
    runSessionHydrate(sess);
  } else {
    restoreElapsedBadges(sess, ensureMsgs());
    planPoll(sess);
  }
}
async function closeSession(id) {
  const sess = state.sessions.get(id);
  if (sess && sess.bridgeSessionId) {
    try { await window.ga.rpc('session/cancel', { sessionId: sess.bridgeSessionId }); } catch (_) {}
    fetch(`${BRIDGE_ORIGIN}/session/${sess.bridgeSessionId}`, { method: 'DELETE' }).catch(() => {});
  }
  state.sessions.delete(id); state.runtime.delete(id);
  if (state.activeId === id) {
    const next = (sortedSessions()[0] || {}).id || null;  // 切到列表最靠上的会话
    if (next) setActiveSession(next);
    else { state.activeId = null; localStorage.removeItem('ga_active'); if (msgsEl) msgsEl.innerHTML = ''; refreshEmptyState(null); refreshStatusLabel(); }
  }
  saveSessions();
  renderSessionList();
}

const convMenu = document.getElementById('conv-menu');
const folderMenu = document.getElementById('folder-menu');
const moveMenu = document.getElementById('move-menu');
let menuTargetId = null;
function placeMenu(menu, rect, opts = {}) {
  if (!menu || !rect) return;
  const gap = opts.gap ?? 4;
  const pad = opts.pad ?? 8;
  const side = opts.side || 'bottom-right';
  menu.style.visibility = 'hidden';
  menu.hidden = false;
  const mw = menu.offsetWidth || 160;
  const mh = menu.offsetHeight || 120;
  const vw = window.innerWidth || document.documentElement.clientWidth || 0;
  const vh = window.innerHeight || document.documentElement.clientHeight || 0;
  let left = side.includes('left') ? rect.left - mw - gap : rect.right - mw;
  let top = side.includes('top') ? rect.top - mh - gap : rect.bottom + gap;
  if (!side.includes('top') && top + mh > vh - pad) top = Math.max(pad, rect.top - mh - gap);
  if (side.includes('top') && top < pad) top = Math.min(vh - pad - mh, rect.bottom + gap);
  left = Math.max(pad, Math.min(left, vw - pad - mw));
  top = Math.max(pad, Math.min(top, vh - pad - mh));
  menu.style.left = left + 'px';
  menu.style.top = top + 'px';
  menu.style.visibility = '';
}
// ── 批量删除模式 ──
let batchMode = null; // { folderId, selected: Set<string> }
function exitBatch() {
  batchMode = null;
  const bb = document.getElementById('batch-bar');
  if (bb) { bb.hidden = true; bb.style.display = 'none'; }
}
function updateBatchBar() {
  const el = document.getElementById('bb-count');
  if (el && batchMode) el.textContent = t('conv.batchSelected') + ' ' + batchMode.selected.size;
}
document.getElementById('batch-bar')?.addEventListener('click', async e => {
  const act = e.target.closest('[data-bb]')?.dataset.bb;
  if (!act || !batchMode) return;
  const sessions = sortedSessions().filter(s => normalizeSessionFolder(s) === batchMode.folderId);
  if (act === 'all') {
    const allSel = sessions.length && sessions.every(s => batchMode.selected.has(s.id));
    sessions.forEach(s => { if (allSel) batchMode.selected.delete(s.id); else batchMode.selected.add(s.id); });
    renderSessionList();
  } else if (act === 'del') {
    if (!batchMode.selected.size) return;
    const ids = [...batchMode.selected];
    if (!(await showConfirmDialog({ title: t('common.delete'), message: t('conv.batchDelConfirm').replace('{n}', ids.length), okText: t('common.delete'), okKind: 'danger' }))) return;
    batchMode = null;
    const bb = document.getElementById('batch-bar'); if (bb) bb.hidden = true;
    (async () => { for (const id of ids) await closeSession(id); })();
  } else if (act === 'cancel') {
    exitBatch(); renderSessionList();
  }
});
let folderMenuTargetId = null;
convListEl.addEventListener('click', (e) => {
  const folderAdd = e.target.closest('[data-folder-add="1"]');
  const folderMore = e.target.closest('[data-folder-more="1"]');
  const folderToggle = !folderAdd && !folderMore && e.target.closest('[data-folder-toggle="1"]');
  if (folderToggle) {
    e.stopPropagation();
    const head = folderToggle.closest('.conv-folder-head');
    if (!head) return;
    toggleConvFolderCollapsed(head.dataset.folderId);
    renderSessionList();
    return;
  }
  if (folderAdd) {
    e.stopPropagation();
    showPromptDialog({
      title: t('conv.folderNew'),
      message: t('conv.folderPrompt'),
      value: '',
      okText: t('common.confirm'),
      cancelText: t('common.cancel')
    }).then(name => {
      const folder = createConvFolder(name);
      if (folder) renderSessionList();
    });
    return;
  }
  const more = e.target.closest('.ci-more');
  if (more) {
    e.stopPropagation();
    folderMenu.hidden = true;
    moveMenu.hidden = true;
    menuTargetId = more.closest('.conv-item').dataset.id;
    const tgt = state.sessions.get(menuTargetId);
    const pinSpan = convMenu.querySelector('[data-act="pin"] [data-i18n]');
    if (pinSpan) {
      const k = tgt && tgt.pinned ? 'ctx.unpin' : 'ctx.pin';
      pinSpan.setAttribute('data-i18n', k);
      pinSpan.textContent = t(k);
    }
    renderMoveMenu(tgt ? normalizeSessionFolder(tgt) : '');
    const rect = more.getBoundingClientRect();
    placeMenu(convMenu, rect, { side: 'bottom-right' });
    return;
  }
  if (folderMore) {
    e.stopPropagation();
    convMenu.hidden = true;
    moveMenu.hidden = true;
    folderMenuTargetId = folderMore.closest('.conv-folder-head').dataset.folderId;
    const isArchived = folderMenuTargetId === ARCHIVED_CONV_FOLDER_ID;
    folderMenu.querySelectorAll('[data-act]').forEach(it => { it.hidden = isArchived; });
    const rect = folderMore.getBoundingClientRect();
    placeMenu(folderMenu, rect, { side: 'bottom-right' });
    return;
  }
  const it = e.target.closest('.conv-item');
  if (it && it.dataset.id) {
    if (batchMode) {
      const id = it.dataset.id;
      const sel = !batchMode.selected.has(id);
      if (sel) batchMode.selected.add(id); else batchMode.selected.delete(id);
      const cb = it.querySelector('.ci-check');
      if (cb) cb.checked = sel;
      it.classList.toggle('selected', sel);
      updateBatchBar();
      return;
    }
    setActiveSession(it.dataset.id);
    const chatNav = nav.querySelector('.nav-item[data-page="chat"]');
    if (chatNav && !chatNav.classList.contains('active')) chatNav.click();
  }
});
convMenu.addEventListener('click', (e) => {
  e.stopPropagation();
  const act = e.target.closest('.ctx-item')?.dataset.act;
  const sess = menuTargetId && state.sessions.get(menuTargetId);
  if (sess && act === 'pin') {
    if (sess.pinned) {
      sess.pinned = false;       // 取消置顶 + 放到 pinned 之后、其它 unpinned 之前(unpinned 区域顶部)
      const others = [...state.sessions.values()].filter(s => s.id !== sess.id);
      const m = new Map();
      for (const s of others) if (s.pinned) m.set(s.id, s);  // 先所有仍 pinned 的
      m.set(sess.id, sess);                                   // 再本会话(刚 unpinned)
      for (const s of others) if (!s.pinned) m.set(s.id, s);  // 再其它 unpinned
      state.sessions = m;
    } else {
      sess.pinned = true;        // 置顶 + 移到列表顶
      const m = new Map(); m.set(sess.id, sess);
      for (const [k, v] of state.sessions) if (k !== sess.id) m.set(k, v);
      state.sessions = m;
    }
    saveSessions();
    patchSession(sess, { pinned: sess.pinned });
    renderSessionList();
  } else if (sess && act === 'move') {
    renderMoveMenu(normalizeSessionFolder(sess));
    const r = convMenu.getBoundingClientRect();
    placeMenu(moveMenu, r, { side: 'top-left', gap: 0 });
    return;
  } else if (sess && act === 'move-archived') {
    assignSessionFolder(sess, ARCHIVED_CONV_FOLDER_ID);
    saveSessions();
    renderSessionList();
    toast(t('conv.moveDone').replace('{0}', t('conv.folderArchived')));
  } else if (sess && act === 'rename') {
    convMenu.hidden = true;
    const item = convListEl.querySelector(`.conv-item[data-id="${sess.id}"]`);
    if (!item) return;
    const titleEl = item.querySelector('.ci-title');
    if (!titleEl) return;
    const oldTitle = sess.title || '';
    const inp = document.createElement('input');
    inp.className = 'ci-rename-input';
    inp.maxLength = 50;
    inp.value = oldTitle;
    titleEl.replaceWith(inp);
    bindToastLimit(inp);
    inp.focus();
    inp.select();
    const finish = (save) => {
      if (inp._done) return;
      inp._done = true;
      const val = inp.value.trim();
      if (save && val && val !== oldTitle) {
        sess.title = val;
        sess.untitled = false;
        saveSessions();
        patchSession(sess, { title: val, untitled: false });
        const history = tokLoadHistory();
        const sid = sess.bridgeSessionId || sess.id;
        let changed = false;
        history.forEach(h => { if (h.sessionId === sid) { h.title = val; changed = true; } });
        if (changed) tokSaveHistory(history);
      }
      renderSessionList();
    };
    inp.addEventListener('keydown', e => {
      if (e.key === 'Enter') { e.preventDefault(); finish(true); }
      else if (e.key === 'Escape') { e.preventDefault(); finish(false); }
    });
    inp.addEventListener('blur', () => finish(true));
    return;
  } else if (sess && act === 'batch-del') {
    batchMode = { folderId: normalizeSessionFolder(sess), selected: new Set([sess.id]) };
    renderSessionList();
  } else if (sess && act === 'del') {
    closeSession(sess.id);
  }
  convMenu.hidden = true;
});
// ph-task 任务三点菜单（独立于侧栏 conv-menu，作用于项目页任务列表）
const phTaskMenu = document.getElementById('ph-task-menu');
let phTaskMenuId = null;
let phTaskMenuAnchor = null;
function openPhTaskMenu(anchor) {
  const sid = anchor && anchor.dataset.phTaskMenu;
  if (!sid) return;
  phTaskMenuId = sid;
  phTaskMenuAnchor = anchor;
  const r = anchor.getBoundingClientRect();
  placeMenu(phTaskMenu, r, { side: 'top-left', gap: 2 });
}
phTaskMenu.addEventListener('click', (e) => {
  e.stopPropagation();
  const act = e.target.closest('.ctx-item')?.dataset.act;
  const sess = phTaskMenuId && state.sessions.get(phTaskMenuId);
  if (sess && act === 'rename') {
    phTaskMenu.hidden = true;
    const item = phTaskMenuAnchor && phTaskMenuAnchor.closest('.ph-task-item');
    if (!item) return;
    const titleEl = item.querySelector('.ci-title');
    if (!titleEl) return;
    const oldTitle = sess.title || '';
    const inp = document.createElement('input');
    inp.className = 'ci-rename-input';
    inp.maxLength = 50;
    inp.value = oldTitle;
    titleEl.replaceWith(inp);
    bindToastLimit(inp);
    inp.focus();
    inp.select();
    const finish = (save) => {
      if (inp._done) return;
      inp._done = true;
      const val = inp.value.trim();
      if (save && val && val !== oldTitle) {
        sess.title = val;
        sess.untitled = false;
        saveSessions();
        patchSession(sess, { title: val, untitled: false });
        const history = tokLoadHistory();
        const sid = sess.bridgeSessionId || sess.id;
        let changed = false;
        history.forEach(h => { if (h.sessionId === sid) { h.title = val; changed = true; } });
        if (changed) tokSaveHistory(history);
      }
      renderPhContent();
      renderSessionList();
    };
    inp.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') { e.preventDefault(); finish(true); }
      else if (e.key === 'Escape') { e.preventDefault(); finish(false); }
    });
    inp.addEventListener('blur', () => finish(true));
    return;
  } else if (sess && act === 'del') {
    closeSession(sess.id);
    renderPhContent();
  }
  phTaskMenu.hidden = true;
});
folderMenu.addEventListener('click', async (e) => {
  const act = e.target.closest('.ctx-item')?.dataset.act;
  const folderId = folderMenuTargetId;
  const folder = state.convFolders.find(f => f.id === folderId);
  if (!act || !folder) return;
  if (act === 'new-in-folder') {
    folderMenu.hidden = true;
    state.activeId = null;
    await newSession(folderId);
  } else if (act === 'rename-folder') {
    showPromptDialog({
      title: t('common.rename'),
      message: t('conv.folderPrompt'),
      value: folder.name,
      okText: t('common.confirm'),
      cancelText: t('common.cancel')
    }).then(name => {
      if (renameConvFolder(folderId, name)) renderSessionList();
    });
  } else if (act === 'delete-folder') {
    if (window.confirm(t('conv.folderDeleteConfirm').replace('{0}', folder.name))) {
      if (deleteConvFolder(folderId)) renderSessionList();
    }
  }
  folderMenu.hidden = true;
});
moveMenu.addEventListener('click', (e) => {
  const item = e.target.closest('[data-folder-id]');
  const sess = state.sessions.get(menuTargetId);
  if (!item || !sess) return;
  assignSessionFolder(sess, item.dataset.folderId);
  saveSessions();
  renderSessionList();
  toast(t('conv.moveDone').replace('{0}', getConvFolderName(item.dataset.folderId)));
  moveMenu.hidden = true;
  convMenu.hidden = true;
});
document.addEventListener('click', () => { convMenu.hidden = true; folderMenu.hidden = true; moveMenu.hidden = true; phTaskMenu.hidden = true; });
// Close @ and / panels on outside click
document.addEventListener('click', (e) => {
  if (_atActive && AT_PANEL && !AT_PANEL.contains(e.target) && e.target !== inputEl) {
    hideAtPanel();
  }
  if (_slashActive && SLASH_PANEL && !SLASH_PANEL.contains(e.target) && e.target !== inputEl) {
    hideSlashPanel();
  }
});
newConvBtn.addEventListener('click', (e) => { e.preventDefault(); newSession(); });

/* ═══════════════ 轮询 + 流式 ═══════════════ */
function normalize(m) {
  const o = { id: Number(m.id || 0), role: m.role || 'system' };
  if (m.role !== 'assistant') o.content = m.content || '';
  if (typeof m.display === 'string' && m.display.length) o.display = m.display;
  if (m.stopped) o.stopped = true;
  if (m.images) o.images = m.images;
  if (m.files) o.files = m.files;
  if (m.ts) o.ts = m.ts;
  // [turn_segs双轨] 透传结构化轮数组(若后端提供)；落库消息与 partial 都可能带
  if (Array.isArray(m.turn_segs)) o.turn_segs = m.turn_segs;
  if (typeof m.curr_turn === 'number') o.curr_turn = m.curr_turn;
  if (m.role === 'assistant' && !o.turn_segs?.length && typeof m.content === 'string') o.content = m.content;
  return o;
}
function upsert(sess, raw, partial) {
  const m = normalize(raw); const r = rt(sess);
  if (partial && m.role === 'assistant') {
    if (!r.draftEl) resetTypewriterState(r);
    const prevLen = (Array.isArray(r.draftSegs) ? (r.draftSegs[r.draftTurn] || '') : '').length;
    r.draftSegs = draftSegsFromPartial(raw, m);
    r.draftTurn = (typeof m.curr_turn === 'number') ? m.curr_turn : Math.max(0, r.draftSegs.length - 1);
    const curLen = (r.draftSegs[r.draftTurn] || '').length;
    const wasEmpty = prevLen === 0;
    // 新 draft（含刷新/停止后再开）：对齐已有 partial，避免从 0 重播打字机
    if ((!r.draftEl || wasEmpty) && curLen > 0) {
      r.draftRecoverPending = true;
    }
    const tw = r.twState;
    if (tw && curLen > (r.draftStreamBaseline || 0)) {
      const baseline = r.draftStreamBaseline || 0;
      if (tw.shown < baseline) tw.shown = baseline;
    }
    r.draftStreamBaseline = curLen;
    if (isActive(sess)) renderDraft(sess);
    return;
  }
  if (!m.id || r.seen.has(m.id)) return;
  r.seen.add(m.id); r.lastId = Math.max(r.lastId, m.id);
  if (m.role === 'assistant' && r.draftEl) {
    // done 收尾:用 final m 原地重画 bubble(保留 ljq 的"复用 draftEl 不闪烁"优化),
    // 同时彻底丢掉 partial 累积的 DOM——避免 frp 高延迟下漏掉的最后一拍丢字。
    // assistantTurnSegs(m) 双轨处理 turn_segs/content,跟 msgNode refresh 路径同源。
    flushTypewriter(sess);
    const segs = assistantTurnSegs(m);
    const curr = resolveVisibleTurnIndex(segs, m.curr_turn);
    r.draftSegs = segs;
    r.draftTurn = curr;
    r.streamTurn = curr;
    if (!r.twState) r.twState = { shown: 0, timer: null };
    if (r.twState.timer) { clearInterval(r.twState.timer); r.twState.timer = null; }
    r.twState.turn = curr;
    r.twState.shown = (segs[curr] || '').length;
    r.draftRecoverPending = false;
    if (isActive(sess)) {
      let bubble = r.draftEl.querySelector(':scope > .bubble.md');
      if (!bubble) {
        bubble = document.createElement('div');
        bubble.className = 'bubble md';
        r.draftEl.appendChild(bubble);
      }
      bubble.innerHTML = renderAssistantTurnsHtml(segs, curr, false);
      postRenderEnhance(bubble);
    }
    const cursor = r.draftEl.querySelector('.cursor');
    if (cursor) cursor.remove();
    if (r.taskStartedAt) {
      ensureTaskElapsedBadge(r.draftEl, r.taskStartedAt, r.taskEndedAt || Date.now());
      r.taskStartedAt = null; r.taskEndedAt = null;
    }
    if (!r.draftEl.querySelector('.bubble-copy-btn')) {
      const copyBtn = document.createElement('button');
      copyBtn.className = 'bubble-copy-btn';
      copyBtn.title = t('act.copy');
      copyBtn.innerHTML = SVG_COPY_ICON;
      copyBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        const text = assistantCopyText(m);
        navigator.clipboard.writeText(text).then(() => {
          copyBtn.innerHTML = SVG_CHECK_ICON;
          setTimeout(() => { copyBtn.innerHTML = SVG_COPY_ICON; }, 1500);
        });
      });
      r.draftEl.appendChild(copyBtn);
    }
    r.draftEl = null; r.draftSegs = null; r.draftTurn = 0; r.streamTurn = 0;
    sess.messages.push(m);
    refreshEmptyState(sess);
    if (m.role === 'assistant' || m.role === 'user') syncAskUserUi();
    saveSessions();
    return;
  }
  sess.messages.push(m); appendMessage(sess, m);
  saveSessions();
}

async function fetchSessionPoll(sess, opts = {}) {
  const r = rt(sess);
  const sid = sess.bridgeSessionId || sess.id;
  const afterId = opts.after ?? r.lastId ?? 0;
  const limit = opts.limit ?? POLL_MSG_LIMIT;
  const res = await window.ga.rpc('session/poll', { sessionId: sid, afterId, limit });
  if (res?.error) throw new Error(res.error.message || res.error);
  return res.result || res;
}

function applyPollResult(sess, result) {
  if (result.partial) upsert(sess, result.partial, true);
  for (const msg of (result.messages || [])) upsert(sess, msg, false);
  const busy = result.status === 'running' || !!result.partial;
  setBusy(sess, busy);
  if (isActive(sess)) {
    applyPlanPayload(sess, result.plan);
    applyLiveModel(result.model, sess);
  }
  return busy;
}

/** 渠道组随故障转移变化时，用运行态当前子模型刷新 chip（非渠道组/无 agent 时不动，保持静态显示） */
function applyLiveModel(live, sess = activeSess()) {
  const selected = (state.modelProfiles || []).find(p => (p.id ?? 0) === state.llmNo);
  if (!selected || selected.kind !== 'mixin' || !live || !live.isMixin || !live.current) return;
  state.liveModel = { ...live, sessionId: sess?.id || state.activeId };
  const label = `${t('model.aggregationShort')}${lang === 'en' ? ' (' : '（'}${profileLabel(live.current) || live.current}${lang === 'en' ? ')' : '）'}`;
  if (state.modelName !== label) { state.modelName = label; updateModelChip(); }
}

/** hydrate 批量灌历史，避免逐条 appendMessage 触发全量重绘 */
function hydrateHistoryMessages(sess, messages) {
  const r = rt(sess);
  for (const raw of (messages || [])) {
    const m = normalize(raw);
    if (!m.id || r.seen.has(m.id)) continue;
    r.seen.add(m.id);
    r.lastId = Math.max(r.lastId, m.id);
    sess.messages.push(m);
  }
  if (isActive(sess)) renderAllMessages(sess);
}

/** 拉历史：limit=0 一次拿全量（bridge 不截断）；不等 idle，running 续交给 pollSession */
async function hydrateSession(sess) {
  try {
    const result = await fetchSessionPoll(sess, { after: 0, limit: 0 });
    hydrateHistoryMessages(sess, result.messages);
    if (result.partial) upsert(sess, result.partial, true);
    const busy = result.status === 'running' || !!result.partial;
    setBusy(sess, busy);
    if (isActive(sess)) applyPlanPayload(sess, result.plan);
    if (busy && !rt(sess).polling) pollSession(sess);
  } catch (e) {
    showError(t('err.poll') + ': ' + (e.message || e));
    setBusy(sess, false);
  } finally {
    if (isActive(sess)) {
      restoreElapsedBadges(sess, ensureMsgs());
      syncAskUserUi();
    }
    tokPollBridge();
  }
}

async function pollSession(sess) {
  const r = rt(sess);
  if (r.polling) { r.pollAgain = true; return; }
  r.polling = true;
  r.pollAgain = false;
  /* 手机切后台再回前台时,第一拍 fetch 经常用着死连接秒挂(Failed to fetch),
     但只要给链路 1-2 秒重建,后续就稳。原版一炸就 showError,体验糟。
     改成:同一次 polling 循环里连续失败 ≥ MAX_ERRORS 次才放弃,
     单次失败做指数退避(1s / 2s / 4s / 8s),够 ride through 一次后台恢复抖动。 */
  const MAX_ERRORS = 5;
  let consecutiveErrors = 0;
  try {
    do {
      try {
        const result = await fetchSessionPoll(sess);
        consecutiveErrors = 0;
        const busy = applyPollResult(sess, result);
        if (busy) await new Promise(z => setTimeout(z, 500));
        else {
          if (r.draftEl) { r.draftEl.remove(); r.draftEl = null; r.draftSegs = null; r.draftTurn = 0; }
          resetTypewriterState(r);
          break;
        }
      } catch (innerErr) {
        consecutiveErrors++;
        if (consecutiveErrors >= MAX_ERRORS) throw innerErr;
        const backoff = Math.min(8000, 1000 * Math.pow(2, consecutiveErrors - 1));
        await new Promise(z => setTimeout(z, backoff));
      }
    } while (true);
  } catch (e) {
    showError(t('err.poll') + ': ' + (e.message || e));
    setBusy(sess, false);
  } finally {
    r.polling = false; renderSessionList();
    // 历史消息已全部加载，恢复已完成任务的耗时 badge
    if (isActive(sess)) {
      restoreElapsedBadges(sess, ensureMsgs());
      syncAskUserUi();
    }
    tokPollBridge();
    if (r.pollAgain) {
      r.pollAgain = false;
      pollSession(sess);
    }
  }
}

function removeUsedPendingFiles(usedFiles) {
  if (!usedFiles.length) return;
  const usedSids = new Set(usedFiles.map(f => f.sid));
  const touched = new Set(usedFiles.map(f => fileCtx(f)));
  state.pendingFiles = state.pendingFiles.filter(f => !usedSids.has(f.sid));
  touched.forEach(ctx => renderThumbStrip(ctx));
}

function clearDraft(sess) {
  const r = rt(sess);
  resetTypewriterState(r);
  if (r.draftEl) { r.draftEl.remove(); r.draftEl = null; r.draftSegs = null; r.draftTurn = 0; }
}

async function waitSessionIdle(sess, maxMs = 4000) {
  const start = Date.now();
  while (rt(sess).busy && Date.now() - start < maxMs) {
    await new Promise(z => setTimeout(z, 100));
  }
  return !rt(sess).busy;
}

function setSessionLoading(on) {
  if (!msgArea || !sessionLoadingEl) return;
  if (on && msgArea.classList.contains('is-loading')) return;
  msgArea.classList.toggle('is-session-loading', !!on);
  sessionLoadingEl.hidden = !on;
  if (on && sessionLoadingEl.querySelector('[data-i18n]')) {
    sessionLoadingEl.querySelector('[data-i18n]').textContent = t('chat.sessionLoading');
  }
}

function setMsgLoading(on) {
  if (msgArea) msgArea.classList.toggle('is-loading', !!on);
  if (msgLoading) {
    msgLoading.hidden = !on;
    if (on) {
      setSessionLoading(false);
      scrollBottom();
    }
  }
}

function setComposerLocked(on) {
  if (composerEl) composerEl.classList.toggle('is-locked', !!on);
  if (inputEl) inputEl.contentEditable = on ? 'false' : 'true';  // contenteditable 无 readOnly,改切 contentEditable
  if (sendBtn) {
    sendBtn.disabled = !!on;
    sendBtn.classList.toggle('is-busy', !!on);
    sendBtn.setAttribute('aria-busy', on ? 'true' : 'false');
  }
}

/** stapp.py 同款：运行中再发 → cancel 当前轮次，等 idle 后再提交新 prompt */
async function interruptBeforeSend(sess) {
  if (!rt(sess).busy) return true;
  const t0 = Date.now();
  setMsgLoading(true);
  try {
    clearDraft(sess);
    try {
      const res = await window.ga.rpc('session/cancel', { sessionId: sess.bridgeSessionId || sess.id });
      if (res?.error) throw new Error(res.error.message || res.error);
    } catch (e) {
      showChanToast(t('err.stop') + ': ' + (e.message || e), '', 'err');
      return false;
    }
    showChanToast(t('sys.interruptPrev.hint'), '', 'info');
    const idle = await waitSessionIdle(sess);
    clearDraft(sess);
    if (!idle) {
      showChanToast(t('err.interruptTimeout'), '', 'err');
      return false;
    }
    return true;
  } finally {
    const wait = Math.max(0, MIN_MSG_LOADING_MS - (Date.now() - t0));
    if (wait) await new Promise(r => setTimeout(r, wait));
    setMsgLoading(false);
  }
}

/* ═══════════════ 发送 / 取消 ═══════════════ */
async function sendPrompt(text) {
  text = String(text || '').trim();
  if (!text) return false;
  // 用户手势上下文预授权通知:应对fresh环境permission=default(已granted/denied时不触发),补notifyComplete在bridge消息回调(非手势)中requestPermission可能被浏览器静默阻塞的漏洞
  if ('Notification' in window && Notification.permission === 'default') Notification.requestPermission();
  if (!state.bridgeReady) { showError(t('err.bridge')); return false; }
  if (!state.activeId) { await newSession(); if (!state.activeId) return false; }
  const sess = activeSess(); const r = rt(sess);
  if (r.busy) {
    const interrupted = await interruptBeforeSend(sess);
    if (!interrupted) return false;
  }
  // PLAN/AUTO 现在是预设功能（preset 卡片）一次性发送，不再是常驻 prefix
  const composedPrompt = expandFilePlaceholders(text).trim();
  const usedFiles = collectUsedFiles(text);
  const userMsg = { role: 'user', content: text, ts: Date.now() / 1000 };
  const previewImgs = usedFiles.filter(f => f.isImage).map(f => ({ id: 'f-' + f.sid, name: f.name, path: f.path, dataUrl: f.dataUrl || '' }));
  if (previewImgs.length) userMsg.images = previewImgs;
  const previewFiles = usedFiles.filter(f => !f.isImage).map(f => ({ id: 'f-' + f.sid, name: f.name, path: f.path }));
  if (previewFiles.length) userMsg.files = previewFiles;
  sess.messages.push(userMsg); appendMessage(sess, userMsg);
  if (isPlanPresetPrompt(text)) {
    const pr = rt(sess);
    pr.planCollapsed = false;
    pr.planShowAll = false;
    const sidHint = (sess.bridgeSessionId || sess.id || 'sess').replace(/\//g, '_');
    applyPlanPayload(sess, {
      active: true, placeholder: true, done: 0, total: 0, complete: false,
      step: '', pathHint: `plan_${sidHint}/plan.md`, items: [],
    });
  }
  sess.lastActiveTs = Date.now();
  // 仿 TUI:不再从首条消息自动改名 —— 标题在 newSession 时已设为 agent-N,
  // 之后只接受用户手动 rename。
  saveSessions();
  setBusy(sess, true);
  try {
    let sid = await ensureBridgeSession(sess);
    try {
      await bridgeFetch(`/session/${encodeURIComponent(sid)}/restore`, { method: 'POST', body: {} });
    } catch (restoreErr) {
      if (/not found/i.test(restoreErr.message || '')) {
        sess.bridgeSessionId = null;
        sid = await ensureBridgeSession(sess);
        state.sessions.delete(sess.id);
        sess.id = sess.bridgeSessionId;
        state.sessions.set(sess.id, sess);
        state.activeId = sess.id;
        localStorage.setItem('ga_active', sess.id);  // 会话 id 因 bridge 重建而变更，同步持久化
      }
    }
    const res = await window.ga.rpc('session/prompt', { sessionId: sid, prompt: composedPrompt, display: text, llmNo: state.llmNo,
      files: previewFiles, imageMetas: previewImgs.map(im => ({ name: im.name, path: im.path })) });
    if (res?.error) throw new Error(res.error.message || res.error);
    removeUsedPendingFiles(usedFiles);
    const uid = Number(res.userMessageId || res.result?.userMessageId || 0);
    if (uid) { r.seen.add(uid); r.lastId = Math.max(r.lastId, uid); }
    planPoll(sess);
    pollSession(sess);
    return true;
  } catch (e) {
    const em = { role: 'error', content: e.message || String(e) };
    sess.messages.push(em); appendMessage(sess, em);
    setBusy(sess, false);
    return false;
  }
}
async function cancelPrompt() {
  const sess = activeSess();
  if (!sess || !rt(sess).busy) return false;
  try {
    const res = await window.ga.rpc('session/cancel', { sessionId: sess.bridgeSessionId || sess.id });
    if (res?.error) throw new Error(res.error.message || res.error);
    clearDraft(sess);
    rt(sess).pollAgain = true;
    return true;
  } catch (e) { showError(t('err.stop') + ': ' + (e.message || e)); return false; }
}

async function sendSlashCommand(rawText) {
  const parts = rawText.split(/\s+/);
  const cmd = parts[0];
  const args = parts.slice(1).join(' ');
  if (!state.bridgeReady) { showError(t('err.bridge')); return; }
  if (!state.activeId) { await newSession(); }
  const sess = activeSess();
  const r = rt(sess);
  if (r.busy) {
    const interrupted = await interruptBeforeSend(sess);
    if (!interrupted) return;
  }
  try {
    const sid = await ensureBridgeSession(sess);
    const text = expandFilePlaceholders(rawText).trim();
    const usedFiles = collectUsedFiles(text);
    const previewFiles = usedFiles.filter(f => !f.isImage).map(f => ({ id: 'f-' + f.sid, name: f.name, path: f.path }));
    const previewImgs = usedFiles.filter(f => f.isImage).map(f => ({ id: 'f-' + f.sid, name: f.name, path: f.path, dataUrl: f.dataUrl || '' }));

    const userMsg = { role: 'user', content: rawText, ts: Date.now() / 1000 };
    if (previewImgs.length) userMsg.images = previewImgs;
    if (previewFiles.length) userMsg.files = previewFiles;
    sess.messages.push(userMsg); appendMessage(sess, userMsg);
    sess.lastActiveTs = Date.now();
    saveSessions();
    setBusy(sess, true);

    const res = await bridgeFetch(`/session/${encodeURIComponent(sid)}/slash`, {
      method: 'POST',
      body: { cmd, args, files: previewFiles, imageMetas: previewImgs.map(im => ({ name: im.name, path: im.path })), llmNo: state.llmNo },
    });
    if (res?.error) throw new Error(res.error.message || res.error);
    removeUsedPendingFiles(usedFiles);
    pollSession(sess);
  } catch (e) {
    const em = { role: 'error', content: e.message || String(e) };
    sess.messages.push(em); appendMessage(sess, em);
    setBusy(sess, false);
  }
}

async function sendExecCommand(cmdText) {
  // cmdText is the !-prefixed text, e.g. "!ls -la" → extract "ls -la"
  const cmdStr = cmdText.slice(1).trim();
  if (!cmdStr) return;
  if (!state.bridgeReady) { showError(t('err.bridge')); return; }
  if (!state.activeId) { await newSession(); }
  const sess = activeSess();
  const r = rt(sess);
  if (r.busy) {
    const interrupted = await interruptBeforeSend(sess);
    if (!interrupted) return;
  }
  const userMsg = { role: 'user', content: cmdText, ts: Date.now() / 1000 };
  sess.messages.push(userMsg); appendMessage(sess, userMsg);
  sess.lastActiveTs = Date.now();
  saveSessions();
  setBusy(sess, true);
  try {
    const sid = await ensureBridgeSession(sess);
    const res = await bridgeFetch(`/session/${encodeURIComponent(sid)}/exec`, {
      method: 'POST',
      body: { cmd: cmdStr },
    });
    if (res?.error) throw new Error(res.error.message || res.error);
    // Show command output as a system message
    let outputText = '';
    if (res.stdout) outputText += res.stdout;
    if (res.stderr) {
      if (outputText) outputText += '\n';
      outputText += res.stderr;
    }
    const statusTag = res.ok ? '✓ ' : '✗ (exit ' + res.code + ') ';
    const sysMsg = { role: 'system', content: statusTag + outputText, ts: Date.now() / 1000 };
    sess.messages.push(sysMsg); appendMessage(sess, sysMsg);
    sess.lastActiveTs = Date.now();
    saveSessions();
  } catch (e) {
    const em = { role: 'error', content: e.message || String(e) };
    sess.messages.push(em); appendMessage(sess, em);
  } finally {
    setBusy(sess, false);
  }
}

/* ═══════════════ 输入区 / slash / 预设 ═══════════════ */
async function submitInput() {
  if (_submitInFlight) return;
  let text = composerText('chat');
  if (!text.trim()) return;
  // Save to history (deduplicate consecutive identical entries)
  const trimmed = text.trim();
  if (_history.length === 0 || _history[_history.length - 1] !== trimmed) {
    _history.push(trimmed);
    while (_history.length > HISTORY_MAX) _history.shift();
    try { localStorage.setItem(HISTORY_KEY, JSON.stringify(_history)); } catch (_) {}
  }
  _historyIdx = -1;
  _historyDraft = '';
  rt(activeSess()).inputDraft = '';  // 发送：清除该会话草稿，切回时输入框为空
  if (text.trim().startsWith('!')) {
    inputEl.innerHTML = '';
    await sendExecCommand(text.trim());
    return;
  }
  if (text.trim().startsWith('/')) {
    const cmdName = text.trim().split(/\s+/)[0];
    if (['/help','/new','/clear','/stop','/settings','/resume'].includes(cmdName)) {
      inputEl.innerHTML = '';
      handleSlash(text.trim());
      return;
    }
    if (cmdName === '/scheduler') {
      inputEl.innerHTML = '';
      openSchedulerPicker();
      return;
    }
    inputEl.innerHTML = '';
    await sendSlashCommand(text.trim());
    return;
  }
  if (text.length > 20000) {
    text = text.slice(0, 20000);
    showToast(t('err.charLimit').replace('{n}', 20000), 'warn');
  }
  _submitInFlight = true;
  setComposerLocked(true);
  try {
    const sent = await sendPrompt(text);
    if (sent) {
      inputEl.innerHTML = '';
    }
  } finally {
    _submitInFlight = false;
    setComposerLocked(false);
    syncAskUserUi();
  }
}
sendBtn.addEventListener('click', (e) => {
  e.preventDefault();
  const sess = activeSess();
  if (sess && rt(sess).busy) { cancelPrompt(); return; }  // 运行中：发送键是录制键 → 纯停止
  submitInput();
});
// Panel keyboard navigation — document-level capture to guarantee it fires first
document.addEventListener('keydown', (e) => {
  try {
  if (_atActive) {
    if (e.code === 'ArrowDown') { e.preventDefault(); e.stopPropagation(); _atIdx = Math.min(_atIdx + 1, _atDisplayed.length - 1); renderAtList(_atDisplayed); return; }
    if (e.code === 'ArrowUp') { e.preventDefault(); e.stopPropagation(); _atIdx = Math.max(_atIdx - 1, 0); renderAtList(_atDisplayed); return; }
    if (e.code === 'Enter') { e.preventDefault(); e.stopPropagation(); selectAtItem(); return; }
    if (e.code === 'Escape') { e.preventDefault(); e.stopPropagation(); hideAtPanel(); return; }
    if (e.code === 'Backspace') {
      setTimeout(() => {
        const text = composerText('chat');
        if (!text.includes('@')) hideAtPanel();
      }, 50);
      return;
    }
  }
  if (_slashActive) {
    if (e.code === 'ArrowDown') { e.preventDefault(); e.stopPropagation(); _slashIdx = Math.min(_slashIdx + 1, _slashDisplayed.length - 1); renderSlashList(_slashDisplayed); return; }
    if (e.code === 'ArrowUp') { e.preventDefault(); e.stopPropagation(); _slashIdx = Math.max(_slashIdx - 1, 0); renderSlashList(_slashDisplayed); return; }
    if (e.code === 'Enter') {
      // If the typed text exactly matches a command name, submit it directly
      // instead of selecting the highlighted panel item (which may differ).
      const typed = composerText('chat').trim();
      const exactMatch = _slashCmds.find(c => c.name === typed);
      if (exactMatch) { e.preventDefault(); e.stopPropagation(); hideSlashPanel(); submitInput(); return; }
      e.preventDefault(); e.stopPropagation(); selectSlashItem(); return;
    }
    if (e.code === 'Tab') { e.preventDefault(); e.stopPropagation(); fillSlashItem(); return; }
    if (e.code === 'Escape') { e.preventDefault(); e.stopPropagation(); hideSlashPanel(); return; }
    if (e.code === 'Backspace') {
      setTimeout(() => {
        const text = composerText('chat').trim();
        if (!text.startsWith('/')) hideSlashPanel();
      }, 50);
      return;
    }
  }
  if (_resumeActive) {
    if (e.code === 'ArrowDown') { e.preventDefault(); e.stopPropagation(); _resumeIdx = Math.min(_resumeIdx + 1, _resumeDisplayed.length - 1); renderResumeList(); return; }
    if (e.code === 'ArrowUp') { e.preventDefault(); e.stopPropagation(); _resumeIdx = Math.max(_resumeIdx - 1, 0); renderResumeList(); return; }
    if (e.code === 'Enter') { e.preventDefault(); e.stopPropagation(); selectResumeItem(); return; }
    if (e.code === 'Escape') { e.preventDefault(); e.stopPropagation(); hideResumePanel(); return; }
    if (e.code === 'Backspace') {
      setTimeout(() => {
        const text = composerText('chat');
        if (!text.startsWith('/resume')) hideResumePanel();
      }, 50);
      return;
    }
  }
  } catch(e) { console.error('panel keydown error:', e); }
  // Safety net: if an exception happened while a slash panel was active,
  // prevent the keydown from bubbling to the history-navigation handler
  // (which would overwrite the input text and make the panel "disappear").
  if ((_slashActive || _resumeActive) && (e.code === 'ArrowDown' || e.code === 'ArrowUp' || e.code === 'Enter' || e.code === 'Tab')) {
    e.preventDefault(); e.stopPropagation();
  }
}, true /* capture */);

// Enter to submit (only when no panel is open)
inputEl.addEventListener('keydown', (e) => {
  // ── History navigation (ArrowUp / ArrowDown) when input is empty or at start ──
  if (e.key === 'ArrowUp' && !e.shiftKey) {
    if (_atActive || _slashActive) return;
    const sel = window.getSelection();
    const atStart = sel && sel.rangeCount && sel.getRangeAt(0).startOffset === 0
      && sel.anchorNode === inputEl && inputEl.contains(sel.anchorNode);
    const isEmpty = !composerText('chat').trim();
    // 已在历史导航中(_historyIdx!==-1)则无条件继续往前翻;否则需输入为空或光标在起始(避免文本中间误触发)
    if (_historyIdx !== -1 || isEmpty || atStart) {
      e.preventDefault();
      if (_historyIdx === -1) {
        _historyDraft = composerText('chat');
        _historyIdx = _history.length - 1;
      } else if (_historyIdx > 0) {
        _historyIdx--;
      }
      if (_historyIdx >= 0 && _historyIdx < _history.length) {
        inputEl.textContent = _history[_historyIdx];
        // move cursor to end
        const range = document.createRange();
        range.selectNodeContents(inputEl);
        range.collapse(false);
        sel.removeAllRanges();
        sel.addRange(range);
      }
      return;
    }
  }
  if (e.key === 'ArrowDown' && !e.shiftKey) {
    if (_atActive || _slashActive) return;
    if (_historyIdx >= 0) {
      e.preventDefault();
      _historyIdx++;
      if (_historyIdx >= _history.length) {
        // Restore the draft saved before navigation started
        inputEl.textContent = _historyDraft;
        _historyIdx = -1;
        _historyDraft = '';
        const range = document.createRange();
        range.selectNodeContents(inputEl);
        range.collapse(false);
        const sel = window.getSelection();
        sel.removeAllRanges();
        sel.addRange(range);
      } else {
        inputEl.textContent = _history[_historyIdx];
        const range = document.createRange();
        range.selectNodeContents(inputEl);
        range.collapse(false);
        const sel = window.getSelection();
        sel.removeAllRanges();
        sel.addRange(range);
      }
      return;
    }
  }
  if (e.key === 'Enter' && !e.shiftKey && !e.isComposing && e.keyCode !== 229) {
    if (_atActive || _slashActive) return;
    e.preventDefault(); submitInput();
  }
  // Backspace: remove exclam-prefix if the input is just the styled ! (and possibly trailing space)
  if (e.key === 'Backspace') {
    const exclamEl = inputEl.querySelector('.exclam-prefix');
    if (exclamEl) {
      // Check if input content is only the exclam-prefix + optional whitespace
      const text = composerText('chat');
      if (text.trim() === '!' && text.indexOf('!') === 0) {
        // If there's anything after the !, it's a real command — don't remove
        const afterExclam = text.slice(1).trim();
        if (!afterExclam) {
          e.preventDefault();
          inputEl.innerHTML = '';
          inputEl.focus();
        }
      }
    }
  }
});

// Detect @ and / triggers in contenteditable, and filter panels when active
inputEl.addEventListener('input', () => {
  // 用户开始输入 → 清除推荐 placeholder，恢复默认
  if (_suggestPhActive) {
    _suggestPhActive = false;
    inputEl.setAttribute('data-ph', t('composer.placeholder'));
  }
  { const _bar = document.querySelector('#chat-composer .composer-inset .suggest-bar'); if (_bar) _bar.remove(); }
  // ── exit history mode if user types/changes text manually ──
  if (_historyIdx >= 0 && composerText('chat') !== _history[_historyIdx]) {
    _historyIdx = -1;
    _historyDraft = '';
  }
  // ── resume trigger: activate inline session picker when composer text is /resume ──
  { const _rt = composerText('chat').trim(); if (_rt === '/resume' || _rt.startsWith('/resume ')) { if (!_resumeActive) showResumePanel(); } }
  // ── panel is open: use composer text to filter ──
  if (_atActive) {
    const raw = composerText('chat');
    const idx = raw.lastIndexOf('@');
    const filter = idx >= 0 ? raw.slice(idx + 1).trim() : '';
    if (AT_SEARCH) AT_SEARCH.value = filter;
    if (!filter) {
      // show all files (re-fetch to reset the list)
      const dir = getWorkspacePath() || '/';
      fetchFiles(dir, '').then(res => {
        _atFiles = res.entries || [];
        _atIdx = _atFiles.length > 0 ? 0 : -1;
        renderAtList(_atFiles);
      });
    } else {
      // search server-side so deep files are included
      const dir = getWorkspacePath() || '/';
      fetchFiles(dir, filter).then(res => {
        _atFiles = res.entries || [];
        _atIdx = _atFiles.length > 0 ? 0 : -1;
        renderAtList(_atFiles);
      });
    }
    return;
  }
  if (_slashActive) {
    const raw = composerText('chat').trim();
    const filter = raw.startsWith('/') ? raw.slice(1).trim() : raw;
    // Only re-filter and reset selection when the filter text actually changes;
    // this preserves arrow-key navigation when a stray input event fires.
    if (filter === _slashFilterCache) return;
    _slashFilterCache = filter;
    if (SLASH_SEARCH) SLASH_SEARCH.value = filter;
    const q = filter.toLowerCase();
    let filtered = _slashCmds;
    if (q) {
      filtered = _slashCmds.filter(c =>
        c.name.toLowerCase().includes(q) || c.desc.toLowerCase().includes(q)
      );
    }
    _slashDisplayed = filtered;
    _slashIdx = filtered.length > 0 ? 0 : -1;
    renderSlashList(filtered);
    return;
  }
  // ── resume panel is open: filter sessions by text after /resume ──
  if (_resumeActive) {
    const raw = composerText('chat');
    const still = raw === '/resume' || raw.startsWith('/resume ');
    if (!still) { hideResumePanel(); }
    else {
      const m = raw.match(/^\/resume\s([\s\S]*)$/);
      const filter = m ? m[1] : '';
      const q = filter.trim().toLowerCase();
      let f = _resumeAll;
      if (q) f = _resumeAll.filter(s => displayTitle(s).toLowerCase().includes(q));
      _resumeDisplayed = f;
      _resumeIdx = f.length > 0 ? 0 : -1;
      renderResumeList();
      return;
    }
  }
  // ── panel not open: detect triggers ──
  const sel = window.getSelection();
  if (sel && sel.rangeCount) {
    const node = sel.anchorNode;
    const offset = sel.anchorOffset;
    if (node && node.nodeType === 3 && offset > 0 && node.nodeValue[offset - 1] === '@') {
      showAtPanel();
      return;
    }
    // Detect ! typed — style it and enter exec mode
    if (node && node.nodeType === 3 && offset > 0 && node.nodeValue[offset - 1] === '!') {
      // Only trigger when input otherwise empty (before the !)
      const beforeExclam = node.nodeValue.slice(0, offset - 1).trim();
      if (!beforeExclam && !inputEl.querySelector('.exclam-prefix')) {
        // Verify no other non-whitespace content exists
        const text = composerText('chat');
        if (text.trim() === '!') {
          // Replace the plain ! with styled span + space
          inputEl.innerHTML = '';
          const span = document.createElement('span');
          span.className = 'exclam-prefix';
          span.contentEditable = 'false';
          span.textContent = '!';
          inputEl.appendChild(span);
          const nbsp = document.createTextNode('\u00A0');
          inputEl.appendChild(nbsp);
          const range = document.createRange();
          range.setStartAfter(nbsp);
          range.collapse(true);
          sel.removeAllRanges();
          sel.addRange(range);
          return;
        }
      }
    }
  }
  const text = composerText('chat').trim();
  if (text === '/') {
    showSlashPanel();
    return;
  }
});

// 输入框的 input/paste 监听统一在 bindComposerUpload(ctx) 里绑(chat + collab 通用)
function showSystem(text) {
  const sess = activeSess(); if (!sess) return;
  const m = { role: 'system', content: text };
  sess.messages.push(m); appendMessage(sess, m);
}
function showError(text) {
  const sess = activeSess();
  if (sess) { const m = { role: 'error', content: text }; sess.messages.push(m); appendMessage(sess, m); }
  else console.error(text);
}
let _toastTimer = null;
// --- /scheduler picker modal ---
let _schedulerState = { services: [], running: {}, selected: new Set() };

async function openSchedulerPicker() {
  if (!state.bridgeReady) { showError(t('err.bridge')); return; }
  if (!state.activeId) { await newSession(); }
  const sess = state.sessions[state.activeId];
  if (!sess) return;
  const sid = await ensureBridgeSession(sess);
  openModal('scheduler-modal');
  const body = document.getElementById('scheduler-list');
  if (body) body.innerHTML = '<div style="padding:12px;color:var(--text-dim)">Loading…</div>';
  try {
    const res = await bridgeFetch(`/session/${encodeURIComponent(sid)}/slash`, {
      method: 'POST', body: { cmd: '/scheduler', args: '' }
    });
    if (res?.error) { showError(res.error); closeModals(); return; }
    _schedulerState.services = res.services || [];
    _schedulerState.running = res.running || {};
    _schedulerState.selected = new Set(Object.keys(_schedulerState.running));
    renderSchedulerList();
  } catch (e) { showError(String(e.message || e)); closeModals(); }
}

function renderSchedulerList() {
  const body = document.getElementById('scheduler-list');
  if (!body) return;
  const svcs = _schedulerState.services;
  if (!svcs.length) {
    body.innerHTML = '<div style="padding:12px;color:var(--text-dim)">No launchable services found.</div>';
    return;
  }
  body.innerHTML = svcs.map(svc => {
    const name = svc.name;
    const checked = _schedulerState.selected.has(name);
    const runningTag = _schedulerState.running[name] ? ' <span class="sched-run-tag">· running</span>' : '';
    const doc = svc.doc ? '<div class="sched-doc">' + escapeHtml(svc.doc) + '</div>' : '';
    const kindTag = svc.kind ? '<span class="sched-kind">' + escapeHtml(svc.kind) + '</span>' : '';
    return '<label class="sched-row">' +
      '<input type="checkbox" data-svc="' + escapeHtml(name) + '"' + (checked ? ' checked' : '') + '>' +
      '<div class="sched-info"><div class="sched-name">' + escapeHtml(name) + runningTag + ' ' + kindTag + '</div>' + doc + '</div>' +
    '</label>';
  }).join('');
  body.querySelectorAll('input[data-svc]').forEach(cb => {
    cb.addEventListener('change', () => {
      const n = cb.getAttribute('data-svc');
      if (cb.checked) _schedulerState.selected.add(n); else _schedulerState.selected.delete(n);
    });
  });
}

async function applySchedulerSelection() {
  const sess = state.sessions[state.activeId];
  if (!sess) return;
  const sid = await ensureBridgeSession(sess);
  const names = Array.from(_schedulerState.selected);
  if (!names.length) { showToast('No services selected.'); return; }
  const btn = document.getElementById('scheduler-apply');
  if (btn) { btn.disabled = true; btn.textContent = '…'; }
  try {
    const res = await bridgeFetch(`/session/${encodeURIComponent(sid)}/slash`, {
      method: 'POST', body: { cmd: '/scheduler', args: 'start ' + names.join(',') }
    });
    const results = res?.schedulerResults || [];
    const ok = results.filter(r => r.ok).length;
    const fail = results.filter(r => !r.ok);
    let msg = '✓ ' + ok + ' service(s) updated';
    if (fail.length) msg += ' · ✗ ' + fail.length + ' failed: ' + fail.map(r => r.name).join(', ');
    const sysMsg = { role: 'system', content: msg, ts: Date.now() / 1000 };
    sess.messages.push(sysMsg); appendMessage(sess, sysMsg);
    saveSessions();
    closeModals();
  } catch (e) { showError(String(e.message || e)); }
  finally { if (btn) { btn.disabled = false; btn.textContent = 'Apply'; } }
}
bindClick('scheduler-apply', applySchedulerSelection);

function showToast(text) {
  let el = document.getElementById('ga-toast');
  if (!el) { el = document.createElement('div'); el.id = 'ga-toast'; el.className = 'ga-toast'; document.body.appendChild(el); }
  el.textContent = text;
  el.classList.add('show');
  clearTimeout(_toastTimer);
  _toastTimer = setTimeout(() => el.classList.remove('show'), 1800);
}
async function handleSlash(cmd) {
  const name = cmd.slice(1).split(/\s+/)[0];
  switch (name) {
    case 'help': {
      // Dynamically build the help text from the actual command list
      // (including actions, skills, and custom presets) so it never goes stale.
      if (!_slashCmds.length) {
        _slashCmds = [...(await fetchCommands()), ...getCustomPresetCommands()];
      }
      const groups = {};
      for (const c of _slashCmds) {
        const g = c.group || 'other';
        (groups[g] = groups[g] || []).push(c);
      }
      const groupOrder = ['actions', 'skills', 'custom', 'other'];
      const groupLabels = { actions: '内置命令', skills: '技能命令', custom: '自定义预设', other: '其他' };
      const parts = [];
      for (const g of groupOrder) {
        if (!groups[g]) continue;
        parts.push(`── ${groupLabels[g] || g} ──`);
        for (const c of groups[g]) {
          // Strip redundant command-name prefix from label (e.g. label "update [note]" for "/update" → "[note]")
          let labelPart = '';
          if (c.label && c.label !== c.name) {
            const baseName = c.name.replace(/^\//, '');
            if (c.label.startsWith(baseName)) {
              const rest = c.label.slice(baseName.length).trim();
              labelPart = rest ? ` ${rest}` : '';
            } else {
              labelPart = ` ${c.label}`;
            }
          }
          const descPart = (c.desc && c.desc !== c.label) ? `  — ${c.desc}` : '';
          parts.push(`${c.name}${labelPart}${descPart}`);
        }
      }
      showSystem(`可用命令（共 ${_slashCmds.length} 个）：\n\n${parts.join('\n')}`);
      break;
    }
    case 'new': await newSession(); break;
    case 'clear': { const s = activeSess(); if (s) { s.messages = []; renderAllMessages(s); } break; }
    case 'stop': if (await cancelPrompt()) showSystem(t('sys.stopRequested')); break;
    case 'settings': openSettings(); break;
    case 'resume': await invokeResumePicker(); break;
    default: showSystem(t('slash.unknown') + ': /' + name);
  }
}

/* ═══════════════ @ 文件选择 & / 命令面板 ═══════════════ */

const AT_PANEL = document.getElementById('at-panel');
const AT_LIST = document.getElementById('at-list');
const AT_SEARCH = document.getElementById('at-search-input');
const AT_BROWSE = document.getElementById('at-browse-btn');
const SLASH_PANEL = document.getElementById('slash-panel');
const SLASH_GROUPS = document.getElementById('slash-groups');
const SLASH_SEARCH = document.getElementById('slash-search-input');

let _atFiles = [];
let _atDisplayed = [];  // currently displayed (may be filtered subset)
let _atIdx = -1;
let _atActive = false;
let _slashCmds = [];
let _slashDisplayed = [];   // currently displayed (filtered) list — ArrowUp/Down & select operate on this
let _slashIdx = -1;
let _slashActive = false;
let _slashFilterCache = '';  // last filter text — used to skip redundant re-renders that reset selection
const HISTORY_KEY = 'ga_input_history';
const HISTORY_MAX = 500;
let _history = (() => { try { return JSON.parse(localStorage.getItem(HISTORY_KEY) || '[]'); } catch (_) { return []; } })();          // sent messages history (persisted to localStorage)
let _historyIdx = -1;       // current position in history (-1 = not navigating)
let _historyDraft = '';     // saved draft before starting history navigation

// ── @ panel ─────────────────────────────────────────────────────────

function getWorkspacePath() {
  const sess = activeSess();
  if (sess && sess._workspacePath) return sess._workspacePath;
  return state.lastWorkspacePath || '';
}

async function fetchFiles(dirPath, filter) {
  let url = `${BRIDGE_ORIGIN}/api/files/list?path=${encodeURIComponent(dirPath)}`;
  if (filter) url += `&filter=${encodeURIComponent(filter)}`;
  try {
    const res = await fetch(url);
    const j = await res.json();
    return j.entries ? j : { entries: [] };
  } catch (e) { return { entries: [] }; }
}

function renderAtList(files) {
  _atDisplayed = files;
  if (!files.length) {
    AT_LIST.innerHTML = '<div class="at-empty">No files found</div>';
    _atIdx = -1;
    return;
  }
  _atIdx = Math.max(-1, Math.min(_atIdx, files.length - 1));
  if (_atIdx < 0 && files.length > 0) _atIdx = 0;
  AT_LIST.innerHTML = files.map((f, i) => {
    const icon = f.type === 'dir' ? '📁' : '📄';
    const cls = 'at-item' + (i === _atIdx ? ' active' : '');
    const display = escapeHtml(f.rel || f.name);
    return `<div class="${cls}" data-idx="${i}"><span class="at-icon">${icon}</span>${display}</div>`;
  }).join('');
  // scroll active item into view
  requestAnimationFrame(() => {
    const active = AT_LIST.querySelector('.at-item.active');
    if (active) active.scrollIntoView({ block: 'nearest' });
  });
}

async function showAtPanel() {
  hideSlashPanel();
  _atActive = true;
  AT_PANEL.hidden = false;
  renderAtList([]);
  const dir = getWorkspacePath() || '/';
  const raw = composerText('chat');
  const idx = raw.lastIndexOf('@');
  const filter = idx >= 0 ? raw.slice(idx + 1).trim() : '';
  const res = await fetchFiles(dir, filter);
  _atFiles = res.entries || [];
  _atIdx = _atFiles.length > 0 ? 0 : -1;
  renderAtList(_atFiles);
  if (AT_SEARCH) AT_SEARCH.value = filter;
}

function hideAtPanel() {
  AT_PANEL.hidden = true;
  _atActive = false;
  _atFiles = [];
  _atDisplayed = [];
  _atIdx = -1;
}

function selectAtItem() {
  try {
    if (_atIdx < 0 || _atIdx >= _atDisplayed.length) return;
    const f = _atDisplayed[_atIdx];
    _removeMentionText();
    insertAtChip(f);
    hideAtPanel();
  } catch(e) { console.error('selectAtItem error:', e); }
}

// Remove @ and everything after it from the composer (direct DOM manipulation)
function _removeMentionText() {
  const input = composerCfg('chat').input;
  if (!input) return;
  // Find the text node with @
  const walker = document.createTreeWalker(input, NodeFilter.SHOW_TEXT);
  let node, atNode = null, atOffset = 0;
  while (node = walker.nextNode()) {
    const txt = node.nodeValue || '';
    const idx = txt.lastIndexOf('@');
    if (idx >= 0) { atNode = node; atOffset = idx; }
  }
  if (!atNode) return;
  // Truncate text at @
  atNode.nodeValue = (atNode.nodeValue || '').slice(0, atOffset);
  // Remove all siblings after atNode
  let next = atNode.nextSibling;
  while (next) {
    const toRemove = next;
    next = next.nextSibling;
    toRemove.remove();
  }
  // Place cursor at end
  const range = document.createRange();
  range.setStartAfter(atNode);
  range.collapse(true);
  const sel = window.getSelection();
  sel.removeAllRanges();
  sel.addRange(range);
}

function insertAtChip(f) {
  const input = composerCfg('chat').input;
  if (!input) return;
  const seq = ++state.fileSeq;
  const isDir = f.type === 'dir';
  // Use relative path from API if available, otherwise compute from workspace
  let displayPath = f.rel || f.path || f.name || '';
  if (!f.rel) {
    const wsPath = getWorkspacePath();
    if (wsPath && displayPath.startsWith(wsPath)) {
      displayPath = displayPath.slice(wsPath.length).replace(/^\//, '');
    }
  }
  if (!displayPath) displayPath = f.name || '';
  const label = `@${displayPath}`;
  const chip = document.createElement('span');
  chip.className = 'ph-chip';
  chip.contentEditable = 'false';
  chip.dataset.sid = seq;
  chip.dataset.kind = isDir ? '@folder' : '@file';
  chip.textContent = label;

  // Always focus the input first, then try to insert at cursor
  input.focus();
  const sel = window.getSelection();
  let inserted = false;
  if (sel && sel.rangeCount) {
    try {
      const range = sel.getRangeAt(0);
      // Check if range is inside or can be moved into the input
      if (input.contains(range.commonAncestorContainer) || input === range.commonAncestorContainer) {
        range.insertNode(chip);
        range.setStartAfter(chip);
        range.collapse(true);
        sel.removeAllRanges();
        sel.addRange(range);
        inserted = true;
      }
    } catch (_) {}
  }
  if (!inserted) {
    input.appendChild(chip);
  }

  state.pendingFiles.push({
    sid: seq, name: f.name, path: f.path, isImage: false,
    isAtMention: true, atKind: isDir ? '@folder' : '@file', ctx: 'chat',
  });
  const space = document.createTextNode('\u00A0');
  chip.after(space);
  input.dispatchEvent(new Event('input', { bubbles: true }));
}

// @ panel event listeners
if (AT_SEARCH) {
  AT_SEARCH.oninput = async () => {
    const q = AT_SEARCH.value.trim();
    const dir = getWorkspacePath() || '/';
    const res = await fetchFiles(dir, q);
    _atFiles = res.entries || [];
    _atIdx = _atFiles.length > 0 ? 0 : -1;
    renderAtList(_atFiles);
  };
}

if (AT_LIST) {
  AT_LIST.addEventListener('mousedown', (e) => {
    const item = e.target.closest('.at-item');
    if (!item) return;
    e.preventDefault(); // prevent focus loss from contenteditable
    _atIdx = Number(item.dataset.idx);
    selectAtItem();
  });
}

if (AT_BROWSE) {
  AT_BROWSE.onclick = async () => {
    hideAtPanel();
    try {
      if (window.__TAURI__?.core?.invoke) {
        const picked = await window.__TAURI__.core.invoke('pick_folder');
        if (picked) {
          const name = picked.split('/').pop() || picked;
          insertAtChip({ name, path: picked, type: 'dir' });
          return;
        }
      }
    } catch (_) {}
    // Fallback: open system file dialog
    const inp = document.createElement('input');
    inp.type = 'file';
    inp.webkitdirectory = true;
    inp.onchange = () => {
      if (inp.files && inp.files.length > 0) {
        const p = inp.files[0].path || inp.files[0].webkitRelativePath || inp.files[0].name;
        insertAtChip({ name: p.split('/')[0], path: p, type: 'dir' });
      }
    };
    inp.click();
  };
}

// ── / command panel ─────────────────────────────────────────────────

async function fetchCommands() {
  try {
    const res = await fetch(`${BRIDGE_ORIGIN}/api/commands`);
    const j = await res.json();
    return j.commands || [];
  } catch (e) { return []; }
}

function getCustomPresetCommands() {
  try {
    const raw = localStorage.getItem('ga_custom_presets');
    if (!raw) return [];
    return JSON.parse(raw).map(p => ({
      name: '/preset:' + p.id, label: p.title, desc: p.title, group: 'custom',
      _presetPrompt: p.prompt,
    }));
  } catch (_) { return []; }
}

function renderSlashList(cmds) {
  if (!cmds.length) {
    SLASH_GROUPS.innerHTML = '<div class="slash-empty">No commands found</div>';
    _slashIdx = -1;
    return;
  }
  _slashIdx = Math.max(-1, Math.min(_slashIdx, cmds.length - 1));
  if (_slashIdx < 0 && cmds.length > 0) _slashIdx = 0;
  const groups = {};
  for (const c of cmds) {
    (groups[c.group] = groups[c.group] || []).push(c);
  }
  const groupLabels = { skills: 'Skills', actions: 'Actions', custom: 'Custom' };
  const order = ['skills', 'actions', 'custom'];
  let html = '';
  for (const g of order) {
    const items = groups[g];
    if (!items || !items.length) continue;
    html += `<div class="slash-group-label">${escapeHtml(groupLabels[g] || g)}</div>`;
    for (let i = 0; i < items.length; i++) {
      const c = items[i];
      const globalIdx = cmds.indexOf(c);
      const cls = 'slash-item' + (globalIdx === _slashIdx ? ' active' : '');
      html += `<div class="${cls}" data-idx="${globalIdx}"><span class="slash-cmd">${escapeHtml(c.name)}</span><span class="slash-desc">${escapeHtml(c.desc)}</span></div>`;
    }
  }
  SLASH_GROUPS.innerHTML = html;
  // scroll active item into view
  requestAnimationFrame(() => {
    const active = SLASH_GROUPS.querySelector('.slash-item.active');
    if (active) active.scrollIntoView({ block: 'nearest' });
  });
}

async function showSlashPanel() {
  hideAtPanel();
  _slashActive = true;
  _slashFilterCache = null;  // reset so the first input event re-filters properly
  SLASH_PANEL.hidden = false;
  renderSlashList([]);  // show empty skeleton immediately
  if (!_slashCmds.length) {
    const bridgeCmds = await fetchCommands();
    const customCmds = getCustomPresetCommands();
    _slashCmds = [...bridgeCmds, ...customCmds];
  }
  _slashIdx = _slashCmds.length > 0 ? 0 : -1;
  _slashDisplayed = _slashCmds;
  renderSlashList(_slashDisplayed);
  if (SLASH_SEARCH) SLASH_SEARCH.value = '';
}

function hideSlashPanel() {
  SLASH_PANEL.hidden = true;
  _slashActive = false;
  _slashIdx = -1;
  _slashFilterCache = null;
}

function selectSlashItem() {
  if (_slashIdx < 0 || _slashIdx >= _slashDisplayed.length) return;
  const cmd = _slashDisplayed[_slashIdx];
  hideSlashPanel();
  if (cmd.group === 'actions') {
    inputEl.innerHTML = '';
    handleSlash(cmd.name);
  } else if (cmd._presetPrompt) {
    inputEl.textContent = cmd._presetPrompt;
    inputEl.focus();
  } else {
    // Skills commands: distinguish by optional-arg marker ([xxx] in label).
    // No optional arg → execute immediately; has optional arg → fill in and wait for user to complete.
    if (/\[[^\]]+\]/.test(cmd.label || cmd.desc || '')) {
      inputEl.textContent = cmd.name + ' ';
      inputEl.focus();
      const range = document.createRange();
      range.selectNodeContents(inputEl);
      range.collapse(false);
      const sel = window.getSelection();
      sel.removeAllRanges();
      sel.addRange(range);
    } else {
      inputEl.textContent = cmd.name;
      submitInput();
    }
  }
}

// Tab: fill in the selected command name (without executing) so the user can
// review / append arguments before pressing Enter.
function fillSlashItem() {
  if (_slashIdx < 0 || _slashIdx >= _slashDisplayed.length) return;
  const cmd = _slashDisplayed[_slashIdx];
  hideSlashPanel();
  inputEl.textContent = cmd.name + ' ';
  inputEl.focus();
  const range = document.createRange();
  range.selectNodeContents(inputEl);
  range.collapse(false);
  const sel = window.getSelection();
  sel.removeAllRanges();
  sel.addRange(range);
}

if (SLASH_SEARCH) {
  SLASH_SEARCH.oninput = () => {
    const q = SLASH_SEARCH.value.trim().toLowerCase();
    let filtered = _slashCmds;
    if (q) {
      filtered = _slashCmds.filter(c =>
        c.name.toLowerCase().includes(q) || c.desc.toLowerCase().includes(q) || (c.label || '').toLowerCase().includes(q)
      );
    }
    _slashDisplayed = filtered;
    _slashIdx = filtered.length > 0 ? 0 : -1;
    renderSlashList(filtered);
  };
}

if (SLASH_GROUPS) {
  SLASH_GROUPS.addEventListener('mousedown', (e) => {
    const item = e.target.closest('.slash-item');
    if (!item) return;
    e.preventDefault(); // prevent focus loss
    _slashIdx = Number(item.dataset.idx);
    selectSlashItem();
  });
}

// 预设卡：按 data-preset 解耦（与翻译后的标题无关）
document.querySelectorAll('.feature-grid').forEach(grid => {
  grid.addEventListener('click', (e) => {
    const editBtn = e.target.closest('.fc-edit');
    if (editBtn) {
      e.stopPropagation();
      const cp = state.customPresets.find(p => p.id === editBtn.dataset.editId);
      if (cp) openCustomPresetEditor(cp);
      return;
    }
    const xBtn = e.target.closest('.fc-x');
    if (xBtn) {
      e.stopPropagation();
      const kind = xBtn.dataset.removeKind;
      const id = xBtn.dataset.removeId;
      if (kind === 'builtin') hideBuiltinPreset(id);
      else if (kind === 'custom') removeCustomPreset(id);
      return;
    }
    const card = e.target.closest('.fcard');
    if (!card || !grid.contains(card)) return;
    const key = card.dataset.preset;
    if (key === 'add') { closeModals(); openModal('custom-preset-modal'); resetCustomPresetForm(); return; }
    if (card.classList.contains('fcard-custom')) {
      const id = card.dataset.id;
      const cp = state.customPresets.find(p => p.id === id);
      if (cp) { closeModals(); sendPrompt(cp.prompt); }
      return;
    }
    if (!key) { inputEl.focus(); closeModals(); return; }
    const bp = BUILTIN_PRESETS.find(p => p.key === key);
    if (bp?.navigate) { closeModals(); gaGoPage(bp.navigate); window.collabFocus?.(); return; }
    const prompt = I18N[lang]['presetPrompt.' + key] || I18N.zh['presetPrompt.' + key];
    closeModals();
    if (prompt) sendPrompt(prompt);
  });
});

/* ═══════════════ 模型 / 设置 ═══════════════ */
function updateModelChip() {
  const name = state.modelName || '';
  if (modelNameEl) modelNameEl.textContent = name;
  if (collabModelNameEl) collabModelNameEl.textContent = name;
}
function modelDisplayName(p, fallbackName) {
  if (p && p.kind === 'mixin') {
    // 静态回退显示「渠道组（首选模型名）」；运行后由 applyLiveModel 切到真实当前子模型。
    const primary = (p.members || [])[0];
    if (!primary) return t('model.aggregation');
    const open = lang === 'en' ? ' (' : '（', close = lang === 'en' ? ')' : '）';
    return `${t('model.aggregationShort')}${open}${profileLabel(primary) || primary}${close}`;
  }
  return profileLabel(fallbackName ?? (p && p.name)) || (fallbackName ?? (p && p.name)) || null;
}
async function selectModel(id, name) {
  state.llmNo = id;
  state.llmNoUserSet = true;
  state.liveModel = null;
  const p = (state.modelProfiles || []).find(x => (x.id ?? 0) === id);
  state.modelName = modelDisplayName(p, name);
  updateModelChip();
  renderSettingsModels();
  await persistUiPrefs();
}
async function addToMixin(id) {
  try {
    const res = await bridgeFetch(`/model-profiles/${id}/mixin`, { method: 'POST', body: {} });
    if (res?.ok === false || res?.error) throw new Error(res.error || t('err.mixinFailed'));
    state.modelProfiles = normalizeProfiles(res.profiles || []);
    renderSettingsModels();
  } catch (ex) { showChanToast(t('err.mixinFailed'), ex.message || '', 'err'); }
}
async function removeFromMixin(id) {
  try {
    const res = await bridgeFetch(`/model-profiles/${id}/mixin`, { method: 'DELETE', body: {} });
    if (res?.ok === false || res?.error) throw new Error(res.error || t('err.mixinFailed'));
    state.modelProfiles = normalizeProfiles(res.profiles || []);
    renderSettingsModels();
  } catch (ex) { showChanToast(t('err.mixinFailed'), ex.message || '', 'err'); }
}
async function reorderMixin(members) {
  try {
    const res = await bridgeFetch('/model-profiles/mixin/order', { method: 'PUT', body: { members } });
    if (res?.ok === false || res?.error) throw new Error(res.error || t('err.mixinFailed'));
    state.modelProfiles = normalizeProfiles(res.profiles || []);
    renderSettingsModels();
    const active = state.modelProfiles.find(p => (p.id ?? 0) === state.llmNo);
    if (active) { state.modelName = modelDisplayName(active); updateModelChip(); }
  } catch (ex) { showChanToast(t('err.mixinFailed'), ex.message || '', 'err'); renderSettingsModels(); }
}
function flipReorder(container, mutate) {
  const rows = [...container.querySelectorAll('.model-member:not(.dragging)')];
  const first = new Map(rows.map(el => [el, el.getBoundingClientRect()]));
  mutate();
  rows.forEach(el => {
    const a = first.get(el), b = el.getBoundingClientRect();
    if (!a) return;
    const dx = a.left - b.left, dy = a.top - b.top;
    if (!dx && !dy) return;
    el.style.transition = 'none';
    el.style.transform = `translate(${dx}px, ${dy}px)`;
    requestAnimationFrame(() => {
      el.style.transition = 'transform .14s cubic-bezier(.2,.8,.2,1)';
      el.style.transform = '';
    });
  });
}
function bindMixinDrag(body, members) {
  let drag = null;
  const clear = () => {
    body.querySelectorAll('.model-member').forEach(x => x.classList.remove('dragging', 'drag-over'));
    document.body.classList.remove('mixin-dragging');
  };
  body.addEventListener('pointerdown', (e) => {
    const handle = e.target.closest('.model-member-drag');
    if (!handle || e.button !== 0) return;
    const row = handle.closest('.model-member');
    if (!row) return;
    e.preventDefault();
    handle.setPointerCapture?.(e.pointerId);
    drag = { handle, row, name: row.dataset.member, order: [...members], original: [...members], pointerId: e.pointerId, over: null };
    row.classList.add('dragging');
    document.body.classList.add('mixin-dragging');
  });
  body.addEventListener('pointermove', (e) => {
    if (!drag) return;
    const over = document.elementFromPoint(e.clientX, e.clientY)?.closest('.model-member');
    if (!over || !body.contains(over) || over === drag.row) return;
    const rect = over.getBoundingClientRect();
    const after = e.clientY > rect.top + rect.height / 2;
    const overKey = `${over.dataset.member}:${after ? 'after' : 'before'}`;
    if (overKey === drag.over) return;
    drag.over = overKey;
    const from = drag.order.indexOf(drag.name), overIdx = drag.order.indexOf(over.dataset.member);
    if (from < 0 || overIdx < 0) return;
    const [moved] = drag.order.splice(from, 1);
    let insertAt = drag.order.indexOf(over.dataset.member) + (after ? 1 : 0);
    drag.order.splice(insertAt, 0, moved);
    flipReorder(body, () => {
      if (after) body.insertBefore(drag.row, over.nextSibling);
      else body.insertBefore(drag.row, over);
    });
  });
  const finish = (e) => {
    if (!drag) return;
    drag.handle.releasePointerCapture?.(drag.pointerId);
    const changed = JSON.stringify(drag.order) !== JSON.stringify(drag.original);
    const next = drag.order;
    drag = null;
    clear();
    if (changed) reorderMixin(next);
  };
  body.addEventListener('pointerup', finish);
  body.addEventListener('pointercancel', finish);
}
const MODEL_ACT_EDIT = GA_ICON('pencilSimple');
const MODEL_ACT_DEL = GA_ICON('trash');
let editingModelId = null;

function setModelApikeyMode(isAdd) {
  const apikey = document.getElementById('model-apikey-input');
  const apikeyReq = document.querySelector('#model-apikey-label .field-req');
  if (!apikey) return;
  apikey.required = isAdd;
  apikey.dataset.i18nPh = isAdd ? 'model.apikeyPh' : 'model.apikeyKeep';
  if (isAdd) apikey.removeAttribute('data-optional-ph');
  else { apikey.value = ''; apikey.setAttribute('data-optional-ph', ''); }
  if (apikeyReq) apikeyReq.hidden = !isAdd;
}

/* ═══════════════ 官方模型快速接入（DeepSeek / 通义千问）═══════════════ */
// 预填 API 地址 / 协议 / 模型，用户只需粘贴 API Key。apibase 末尾的 /v1 会被
// 后端自动补成 /v1/chat/completions（见 mykey_template.py 的拼接规则）。
const PROVIDER_PRESETS = {
  deepseek: {
    label: 'DeepSeek', descKey: 'pq.deepseekDesc',
    protocol: 'oai', apibase: 'https://api.deepseek.com/v1',
    model: 'deepseek-v4-pro', name: 'DeepSeek',
    keyUrl: 'https://platform.deepseek.com/api_keys',
    color: '#4D6BFE', tint: 'rgba(77,107,254,.12)',
    logo: '<svg viewBox="0 0 24 24" fill="#4D6BFE" xmlns="http://www.w3.org/2000/svg"><path d="M23.748 4.651c-.254-.124-.364.113-.512.233-.051.04-.094.09-.137.137-.372.397-.806.657-1.373.626-.829-.046-1.537.214-2.163.848-.133-.782-.575-1.248-1.247-1.548-.352-.155-.708-.311-.955-.65-.172-.24-.219-.509-.305-.774-.055-.16-.11-.323-.293-.35-.2-.031-.278.136-.356.276-.313.572-.434 1.202-.422 1.84.027 1.436.633 2.58 1.838 3.393.137.094.172.187.129.323-.082.28-.18.553-.266.833-.055.179-.137.218-.328.14a5.5 5.5 0 0 1-1.737-1.179c-.857-.828-1.631-1.743-2.597-2.46a12 12 0 0 0-.689-.47c-.985-.957.13-1.743.387-1.836.27-.098.094-.433-.778-.428-.872.003-1.67.295-2.687.685a3 3 0 0 1-.465.136 9.6 9.6 0 0 0-2.883-.101c-1.885.21-3.39 1.1-4.497 2.622C.082 8.776-.231 10.854.152 13.02c.403 2.284 1.568 4.175 3.36 5.653 1.857 1.533 3.997 2.284 6.438 2.14 1.482-.085 3.132-.284 4.994-1.86.47.234.962.328 1.78.398.629.058 1.235-.031 1.705-.129.735-.155.684-.836.418-.961-2.155-1.004-1.682-.595-2.112-.926 1.095-1.295 2.768-3.598 3.284-6.733.05-.346.115-.834.108-1.114-.004-.171.035-.238.23-.257a4.2 4.2 0 0 0 1.545-.475c1.397-.763 1.96-2.016 2.093-3.517.02-.23-.004-.467-.247-.588M11.58 18.168c-2.088-1.642-3.101-2.183-3.52-2.16-.39.024-.32.472-.234.763.09.288.207.487.371.74.114.167.192.416-.113.603-.673.416-1.842-.14-1.897-.168-1.361-.801-2.5-1.86-3.301-3.306-.775-1.393-1.225-2.888-1.299-4.482-.02-.385.094-.522.477-.592a4.7 4.7 0 0 1 1.53-.038c2.131.311 3.946 1.264 5.467 2.774.868.86 1.525 1.887 2.202 2.89.72 1.066 1.494 2.082 2.48 2.915.348.291.626.513.892.677-.802.09-2.14.109-3.055-.615zm1.001-6.44a.306.306 0 0 1 .415-.287.3.3 0 0 1 .113.074.3.3 0 0 1 .086.214c0 .17-.136.307-.308.307a.303.303 0 0 1-.306-.307m3.11 1.596c-.2.081-.4.151-.591.16a1.25 1.25 0 0 1-.798-.254c-.274-.23-.47-.358-.551-.758a1.7 1.7 0 0 1 .015-.588c.07-.327-.007-.537-.238-.727-.188-.156-.426-.199-.689-.199a.6.6 0 0 1-.254-.078.253.253 0 0 1-.114-.358 1 1 0 0 1 .192-.21c.356-.202.767-.136 1.146.016.352.144.618.408 1.001.782.392.451.462.576.685.915.176.264.336.536.446.848.066.194-.02.353-.25.45"/></svg>',
  },
  qwen: {
    label: '通义千问', descKey: 'pq.qwenDesc',
    protocol: 'oai', apibase: 'https://dashscope.aliyuncs.com/compatible-mode/v1',
    model: 'qwen3.6-max-preview', name: '通义千问',
    keyUrl: 'https://bailian.console.aliyun.com/?apiKey=1',
    color: '#615CED', tint: 'rgba(97,92,237,.12)',
    logo: '<svg viewBox="0 0 24 24" fill="#615CED" xmlns="http://www.w3.org/2000/svg"><path d="M23.919 14.545 20.817 9.17l1.47-2.544a.56.56 0 0 0 0-.566l-1.633-2.83a.57.57 0 0 0-.49-.283h-6.207L12.487.402a.57.57 0 0 0-.49-.284H8.732a.56.56 0 0 0-.49.284L5.139 5.775h-2.94a.56.56 0 0 0-.49.284L.077 8.887a.56.56 0 0 0 0 .567L3.18 14.83l-1.47 2.545a.56.56 0 0 0 0 .566l1.634 2.83a.57.57 0 0 0 .49.283h6.205l1.47 2.545a.57.57 0 0 0 .49.284h3.266a.57.57 0 0 0 .49-.284l3.104-5.375h2.94a.57.57 0 0 0 .49-.283l1.634-2.828a.55.55 0 0 0-.004-.568M8.733.686l1.634 2.828-1.634 2.828H21.8L20.164 9.17H7.425L5.63 6.06Zm1.306 19.801-6.205-.002 1.634-2.83h3.265L2.201 6.344h3.267q3.182 5.517 6.367 11.032zm10.124-5.66L18.53 12l-6.532 11.315-1.634-2.83c2.129-3.673 4.25-7.351 6.373-11.028h3.592l3.102 5.374z"/></svg>',
  },
};
window.gaProviderPresets = PROVIDER_PRESETS;

// 在「添加模型」弹窗顶部显示/隐藏接入指引横幅。key 为 null 时隐藏。
function setModelGuide(key) {
  const box = document.getElementById('model-guide');
  if (!box) return;
  const p = key && PROVIDER_PRESETS[key];
  if (!p) { box.hidden = true; box.dataset.provider = ''; return; }
  box.hidden = false;
  box.dataset.provider = key;
  const logo = document.getElementById('model-guide-logo');
  if (logo) { logo.innerHTML = p.logo || ''; logo.style.background = p.tint || ''; }
  const nameEl = document.getElementById('model-guide-name');
  if (nameEl) nameEl.textContent = p.label;
  const link = document.getElementById('model-guide-link');
  if (link) { link.href = p.keyUrl; link.textContent = t('guide.getKey').replace('{name}', p.label); }
}
window.gaRefreshModelGuide = () => {
  const box = document.getElementById('model-guide');
  if (box && !box.hidden && box.dataset.provider) setModelGuide(box.dataset.provider);
};

function openAddModelFormForProvider(key) {
  const p = PROVIDER_PRESETS[key];
  if (!p) return openAddModelForm();
  editingModelId = null;
  const form = document.getElementById('add-model-form');
  const title = document.getElementById('model-form-title');
  const errEl = document.getElementById('add-model-err');
  if (title) title.dataset.i18n = 'modal.addModel';
  if (form) {
    form.reset();
    form.model.value = p.model || '';
    form.apibase.value = p.apibase || '';
    form.name.value = p.name || '';
    const pr = form.querySelector(`input[name="protocol"][value="${p.protocol}"]`);
    if (pr) pr.checked = true;
  }
  setModelApikeyMode(true);
  if (errEl) { errEl.hidden = true; errEl.textContent = ''; }
  setModelGuide(key);
  openModal('add-model-modal');
  applyI18n();
  const apikey = document.getElementById('model-apikey-input');
  if (apikey) setTimeout(() => apikey.focus(), 60);
}

function openAddModelForm() {
  editingModelId = null;
  const form = document.getElementById('add-model-form');
  const title = document.getElementById('model-form-title');
  const errEl = document.getElementById('add-model-err');
  if (title) title.dataset.i18n = 'modal.addModel';
  if (form) form.reset();
  setModelApikeyMode(true);
  setModelGuide(null);
  if (errEl) { errEl.hidden = true; errEl.textContent = ''; }
  openModal('add-model-modal');
  applyI18n();
}
async function openEditModelForm(id) {
  editingModelId = id;
  setModelGuide(null);
  const errEl = document.getElementById('add-model-err');
  if (errEl) { errEl.hidden = true; errEl.textContent = ''; }
  try {
    const res = await bridgeFetch(`/model-profiles/${id}`);
    const p = res.profile;
    if (!p) throw new Error(t('err.modelSave'));
    const form = document.getElementById('add-model-form');
    const title = document.getElementById('model-form-title');
    if (title) title.dataset.i18n = 'modal.editModel';
    if (form) {
      form.model.value = p.model || '';
      form.apibase.value = p.apibase || '';
      form.name.value = p.name || '';
      form.max_retries.value = p.max_retries ?? 5;
      form.connect_timeout.value = p.connect_timeout ?? 15;
      form.read_timeout.value = p.read_timeout ?? 300;
      // 编辑模式:按 varName 回填协议分段控件
      const pv = /claude/i.test(p.varName || '') ? 'claude' : 'oai';
      const pr = form.querySelector(`input[name="protocol"][value="${pv}"]`);
      if (pr) pr.checked = true;
      // 回填流式开关(默认流式)
      const sv = (p.stream === false) ? 'false' : 'true';
      const sr = form.querySelector(`input[name="stream"][value="${sv}"]`);
      if (sr) sr.checked = true;
    }
    setModelApikeyMode(false);
    openModal('add-model-modal');
    applyI18n();
  } catch (ex) {
    showChanToast(t('err.modelSave'), ex.message || '', 'err');
  }
}
async function deleteModel(id, name) {
  const label = profileLabel(name) || name || ('#' + id);
  if (!(await showConfirmDialog({ title: t('common.delete'), message: `${t('confirm.modelDelete')}\n${label}`, okText: t('common.delete'), okKind: 'danger' }))) return;
  try {
    const res = await bridgeFetch(`/model-profiles/${id}`, { method: 'DELETE', body: {} });
    if (res?.ok === false || res?.error) throw new Error(res.error || t('err.modelDelete'));
    const wasActive = state.llmNo === id;
    const oldNo = state.llmNo;
    state.modelProfiles = normalizeProfiles(res.profiles || []);
    if (wasActive) {
      const p = state.modelProfiles[0];
      if (p) await selectModel(p.id ?? 0, p.name);
      else { state.llmNo = 0; state.modelName = null; updateModelChip(); }
    } else if (oldNo > id) {
      const p = state.modelProfiles[oldNo - 1];
      if (p) await selectModel(p.id ?? (oldNo - 1), p.name);
    }
    renderSettingsModels();
  } catch (ex) {
    const msg = ex.message || '';
    showChanToast(msg.includes('last profile') ? t('err.modelDeleteLast') : t('err.modelDelete'), msg.includes('last profile') ? '' : msg, 'err');
  }
}
function renderSettingsModels() {
  const box = document.getElementById('model-list');
  if (!box) return;
  box.innerHTML = '';
  const list = state.modelProfiles || [];
  const mixin = list.find(p => p.kind === 'mixin');
  const natives = list.filter(p => p.kind !== 'mixin');
  const byName = new Map(natives.map(p => [p.name, p]));

  // ── 渠道组（自动故障转移）：可展开组；本身也可被选为激活模型 ──
  if (mixin) {
    const gid = mixin.id ?? 0;
    const members = mixin.members || [];
    const expanded = state.mixinExpanded !== false; // 默认展开
    const group = document.createElement('div');
    group.className = 'model-group';
    const head = document.createElement('label');
    head.className = 'model-row model-row--mixin' + (state.llmNo === gid ? ' sel' : '');
    head.innerHTML = `<input type="radio" name="model-pick"${state.llmNo === gid ? ' checked' : ''}><span class="model-mixin-caret" data-act="toggle">${GA_ICON(expanded ? 'caretDown' : 'caretRight')}</span><span class="model-row-name">${escapeHtml(t('model.aggregation'))}</span>`;
    head.querySelector('[data-act="toggle"]').addEventListener('click', (e) => { e.stopPropagation(); e.preventDefault(); state.mixinExpanded = !expanded; renderSettingsModels(); });
    head.addEventListener('click', (e) => { if (e.target.closest('[data-act="toggle"]')) return; e.preventDefault(); selectModel(gid, mixin.name); });
    group.appendChild(head);
    if (expanded) {
      const body = document.createElement('div');
      body.className = 'model-mixin-body';
      if (!members.length) {
        const em = document.createElement('div');
        em.className = 'model-mixin-empty'; em.textContent = t('model.emptyMixin');
        body.appendChild(em);
      } else {
        members.forEach((mName, i) => {
          const mp = byName.get(mName);
          const row = document.createElement('div');
          row.className = 'model-member';
          row.dataset.member = mName;
          row.innerHTML = `<button type="button" class="model-member-drag" data-act="drag" title="${escapeHtml(t('model.dragReorder'))}" aria-label="${escapeHtml(t('model.dragReorder'))}"><span class="grip-dot"></span></button><span class="model-member-name">${escapeHtml(profileLabel(mName) || mName)}</span><button type="button" class="model-act model-act-del" data-act="unmix" title="${escapeHtml(t('model.removeFromMixin'))}">${GA_ICON('x')}</button>`;
          row.querySelector('[data-act="unmix"]').addEventListener('click', (e) => { e.stopPropagation(); e.preventDefault(); if (mp) removeFromMixin(mp.id ?? 0); });
          body.appendChild(row);
        });
        bindMixinDrag(body, members);
      }
      group.appendChild(body);
    }
    box.appendChild(group);
  }

  // ── 独立模型（聚合渠道组已自带分隔，这里不再单列标题）──
  if (!natives.length) {
    const empty = document.createElement('div');
    empty.className = 'set-empty'; empty.textContent = t('set.noModels');
    box.appendChild(empty);
  } else {
    for (const p of natives) {
      const id = p.id ?? 0;
      const label = profileLabel(p.name) || p.name || ('#' + id);
      const row = document.createElement('label');
      row.className = 'model-row' + (state.llmNo === id ? ' sel' : '');
      // 独立列表按钮统一为「加入渠道组」（➕）；移除只在渠道组展开区做。
      // 已在渠道组的，按钮仍是「加入」，但点击只提示「已在渠道组中」，并用 is-in 给个淡淡的视觉区分。
      const mixToggle = !mixin ? '' : `<button type="button" class="model-act model-act-addmix${p.inMixin ? ' is-in' : ''}" data-act="addmix" title="${escapeHtml(p.inMixin ? t('model.alreadyInMixin') : t('model.addToMixin'))}">${GA_ICON('plus')}</button>`;
      row.innerHTML = `<input type="radio" name="model-pick"${state.llmNo === id ? ' checked' : ''}><span class="model-row-name">${escapeHtml(label)}</span><span class="model-row-actions">${mixToggle}<button type="button" class="model-act" data-act="edit" title="${escapeHtml(t('common.edit'))}">${MODEL_ACT_EDIT}</button><button type="button" class="model-act model-act-del" data-act="delete" title="${escapeHtml(t('common.delete'))}">${MODEL_ACT_DEL}</button></span>`;
      row.querySelector('[data-act="edit"]').addEventListener('click', (e) => { e.stopPropagation(); e.preventDefault(); openEditModelForm(id); });
      row.querySelector('[data-act="delete"]').addEventListener('click', (e) => { e.stopPropagation(); e.preventDefault(); deleteModel(id, p.name); });
      const addBtn = row.querySelector('[data-act="addmix"]');
      if (addBtn) addBtn.addEventListener('click', (e) => {
        e.stopPropagation(); e.preventDefault();
        if (p.inMixin) showToast(t('model.alreadyInMixin'));
        else addToMixin(id);
      });
      row.addEventListener('click', (e) => {
        if (e.target.closest('.model-row-actions')) return;
        e.preventDefault();
        selectModel(id, p.name);
      });
      box.appendChild(row);
    }
  }
  applyI18n();
}
function openSettings() {
  openModal('settings-modal');
  renderSettingsModels();
  renderLangList();
  applyTheme(theme, { persist: false });
  applyAppearance(appearance, plainUi, { persist: false });
  applyChatFontSize(chatFontSize, { persist: false });
}
async function loadModelProfiles() {
  try {
    const res = await window.ga.getModelProfiles();
    const list = res?.profiles || res?.result?.profiles || [];
    state.modelProfiles = normalizeProfiles(list);
    // 仅在首次初始化（用户未主动选择过模型）时采用服务端 active；用户已选则保留
    if (!state.llmNoUserSet) {
      const active = state.modelProfiles.find(p => p.active) || state.modelProfiles[0];
      if (active) {
        state.llmNo = active.id ?? 0;
        state.modelName = modelDisplayName(active);
      }
    } else {
      // 确保当前选中的模型显示名与 profiles 同步（不改变 llmNo）
      const cur = state.modelProfiles.find(p => (p.id ?? 0) === state.llmNo);
      if (cur) { state.modelName = modelDisplayName(cur); }
      else {
        // 当前选中模型已不存在（被删除等），回退到 active 或第一个
        const active = state.modelProfiles.find(p => p.active) || state.modelProfiles[0];
        if (active) { state.llmNo = active.id ?? 0; state.modelName = modelDisplayName(active); }
      }
    }
    updateModelChip();
    renderSettingsModels();
  } catch (_) {}
}
/* ═══════════════ 模型菜单(chat + conductor 共用一份逻辑,各自一个 DOM) ═══════════════ */
const modelMenu       = document.getElementById('model-menu');
const collabModelMenu = document.getElementById('cdb-model-menu');
function renderModelMenu(menuEl) {
  if (!menuEl) return;
  const list = state.modelProfiles || [];
  const rows = list.map((p, i) => {
    const no = (p.id ?? i);
    const isActive = (state.llmNo === no) ? ' active' : '';
    const label = (isActive && p.kind === 'mixin' && state.modelName) ? state.modelName : modelDisplayName(p);
    return `<div class="ga-menu-item${isActive}" data-llmno="${no}">${escapeHtml(label || '')}</div>`;
  });
  menuEl.innerHTML = rows.join('');
  applyI18n();
}
function openModelMenu(chipEl, menuEl) {
  if (!chipEl || !menuEl) return;
  if (typeof convMenu !== 'undefined' && convMenu) convMenu.hidden = true;
  window.collabComposer?.closeMenu?.();
  closeAllModelMenus();
  renderModelMenu(menuEl);
  menuEl.hidden = false;
  chipEl.classList.add('open');
  const chipRect = chipEl.getBoundingClientRect();
  const composer = chipEl.closest('.composer');
  if (composer) {
    const composerRect = composer.getBoundingClientRect();
    menuEl.style.left = (chipRect.left - composerRect.left) + 'px';
    menuEl.style.bottom = (composerRect.bottom - chipRect.top + 4) + 'px';
  }
}
function closeAllModelMenus() {
  if (modelMenu) modelMenu.hidden = true;
  if (collabModelMenu) collabModelMenu.hidden = true;
  if (modelChip) modelChip.classList.remove('open');
  if (collabModelChip) collabModelChip.classList.remove('open');
}
function bindModelMenuItemClick(menuEl) {
  if (!menuEl) return;
  menuEl.addEventListener('click', (e) => {
    e.stopPropagation();
    const item = e.target.closest('.ga-menu-item');
    if (!item) return;
    const no = parseInt(item.dataset.llmno, 10);
    if (Number.isNaN(no)) return;
    const p = (state.modelProfiles || []).find(x => (x.id ?? 0) === no);
    selectModel(no, (p && p.name) || '');
    closeAllModelMenus();
  });
}
bindModelMenuItemClick(modelMenu);
bindModelMenuItemClick(collabModelMenu);
if (modelChip) modelChip.addEventListener('click', (e) => {
  e.preventDefault(); e.stopPropagation();
  if (modelMenu && !modelMenu.hidden) { closeAllModelMenus(); return; }
  openModelMenu(modelChip, modelMenu);
});
if (collabModelChip) collabModelChip.addEventListener('click', (e) => {
  e.preventDefault(); e.stopPropagation();
  if (collabModelMenu && !collabModelMenu.hidden) { closeAllModelMenus(); return; }
  openModelMenu(collabModelChip, collabModelMenu);
});
document.addEventListener('click', (e) => {
  if (e.target.closest('#model-menu') || e.target.closest('#model-chip') ||
      e.target.closest('#cdb-model-menu') || e.target.closest('#cdb-model-chip') ||
      e.target.closest('#chat-menu') || e.target.closest('#chat-plus-btn') ||
      e.target.closest('#cdb-menu') || e.target.closest('#cdb-plus-btn') ||
      e.target.closest('#workspace-panel') || e.target.closest('#workspace-chip')) return;
  closeAllModelMenus();
  window.chatComposer?.closeMenu?.();
  window.collabComposer?.closeMenu?.();
  // Close workspace panel
  const wsPanel = document.getElementById('workspace-panel');
  const wsChip = document.getElementById('workspace-chip');
  if (wsPanel && !wsPanel.hidden) { wsPanel.hidden = true; wsChip?.classList.remove('open'); }
  // Close @ and / panels
  if (_atActive && AT_PANEL && !AT_PANEL.contains(e.target) && e.target !== inputEl) {
    hideAtPanel();
  }
  if (_slashActive && SLASH_PANEL && !SLASH_PANEL.contains(e.target) && e.target !== inputEl) {
    hideSlashPanel();
  }
});
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    closeAllModelMenus();
    window.chatComposer?.closeMenu?.();
    window.collabComposer?.closeMenu?.();
    const wsPanel = document.getElementById('workspace-panel');
    const wsChip = document.getElementById('workspace-chip');
    if (wsPanel && !wsPanel.hidden) { wsPanel.hidden = true; wsChip?.classList.remove('open'); }
  }
});

// 主题色板已删除,点击事件不再注册
const appearanceSeg = document.getElementById('appearance-seg');
if (appearanceSeg) appearanceSeg.addEventListener('click', (e) => {
  const btn = e.target.closest('.appear-card[data-appearance]');
  if (!btn) return;
  const isLight = btn.dataset.appearance === 'light';
  applyAppearance(btn.dataset.appearance, isLight && plainUi);
});
const plainUiSwitch = document.getElementById('plain-ui-switch');
if (plainUiSwitch) plainUiSwitch.addEventListener('click', () => {
  if (appearance === 'light') applyAppearance('light', !plainUi);
});
async function loadBridgeConfig() {
  try {
    const res = await window.ga.getConfig();
    if (res?.gaRoot) state.gaRoot = res.gaRoot;
    const cfg = res?.config || {};
    if (LANGS.includes(cfg.lang)) {
      lang = cfg.lang;
      applyI18n();
    }
    if (cfg.theme != null) applyTheme(cfg.theme, { persist: false });
    if (cfg.appearance) applyAppearance(cfg.appearance, !!cfg.plain, { persist: false });
    if (cfg.fontSize != null) applyChatFontSize(cfg.fontSize, { persist: false });
    // 仅在用户未主动选择过模型时用服务端 config 覆盖；用户已选则保留
    if (cfg.llmNo != null && !state.llmNoUserSet && state.modelProfiles.length) {
      const p = state.modelProfiles.find(x => (x.id ?? 0) === cfg.llmNo);
      if (p) {
        state.llmNo = cfg.llmNo;
        state.modelName = modelDisplayName(p);
        updateModelChip();
        renderSettingsModels();
      }
    }
    if (cfg.chatFilesDir != null) {
      const _el = document.getElementById('chat-files-dir-input');
      if (_el) _el.value = cfg.chatFilesDir || 'temp';
    }
    // 补写 chatFilesDir 默认值到 settings.json（用户从未触发 change 时也持久化）
    persistUiPrefs();
    syncBootCache();
  } catch (_) {}
}

const addModelForm = document.getElementById('add-model-form');
// 对话文件目录(chatFilesDir): 输入框 change 即保存, 同步到后端配置
(function () {
  const _inp = document.getElementById('chat-files-dir-input');
  if (_inp) {
    const _apply = () => persistUiPrefs();
    _inp.addEventListener('change', _apply);
    const _btn = document.getElementById('save-chat-files-dir-btn');
    if (_btn) _btn.addEventListener('click', _apply);
  }
})();
if (addModelForm) addModelForm.addEventListener('submit', async (e) => {
  e.preventDefault();
  const errEl = document.getElementById('add-model-err');
  const fd = new FormData(addModelForm);
  const payload = Object.fromEntries(fd.entries());
  const isEdit = editingModelId != null;
  if (!payload.apibase?.trim() || !payload.model?.trim()) {
    if (errEl) { errEl.textContent = t('err.modelRequired'); errEl.hidden = false; }
    return;
  }
  if (!isEdit && !payload.apikey?.trim()) {
    if (errEl) { errEl.textContent = t('err.modelRequired'); errEl.hidden = false; }
    return;
  }
  try {
    const res = isEdit
      ? await bridgeFetch(`/model-profiles/${editingModelId}`, { method: 'PUT', body: payload })
      : await bridgeFetch('/model-profiles', { method: 'POST', body: payload });
    if (res?.ok === false || res?.error) throw new Error(res.error || t('err.modelSave'));
    state.modelProfiles = normalizeProfiles(res.profiles || []);
    const pid = isEdit ? editingModelId : (res.profileId ?? state.modelProfiles.at(-1)?.id ?? 0);
    const p = state.modelProfiles.find(x => (x.id ?? 0) === pid) || state.modelProfiles.at(-1);
    if (p) await selectModel(p.id ?? pid, p.name);
    document.getElementById('add-model-modal').hidden = true;
    addModelForm.reset();
    editingModelId = null;
    if (errEl) errEl.hidden = true;
  } catch (ex) {
    if (errEl) { errEl.textContent = (ex.message || t('err.modelSave')); errEl.hidden = false; }
  }
});

/* ═══════════════ 文件上传（图片+任意文件，tuiapp_v2 模式） ═══════════════ */
const MAX_UPLOAD_FILES = 10;
const MAX_UPLOAD_BYTES = 500 * 1024 * 1024; // 500 MB
const IMG_EXT_RE = /\.(png|jpe?g|gif|webp|bmp|svg)$/i;
const thumbStrip = document.getElementById('thumb-strip');
const chatPanel = document.querySelector('main.main');
let activeFileComposer = 'chat';

function fileCtx(f) { return f.ctx || 'chat'; }
function filesForCtx(ctx) { return state.pendingFiles.filter(f => fileCtx(f) === ctx); }

function composerPageEl(ctx) {
  const page = ctx === 'collab' ? 'collab' : 'chat';
  return document.querySelector(`.page--chat-ui[data-page="${page}"]`);
}
function composerRootEl(ctx) {
  return composerPageEl(ctx)?.querySelector('.composer');
}
function composerCfg(ctx = activeFileComposer) {
  const root = composerRootEl(ctx);
  const page = composerPageEl(ctx);
  return {
    input: root?.querySelector('.composer-inset .input') || null,
    strip: root?.querySelector('.thumb-strip') || null,
    uploadBtn: null,
    imgInput: root?.querySelector('input[type="file"]') || null,
    dropZone: ctx === 'collab' ? page : chatPanel,
  };
}

function renderThumbStrip(ctx = activeFileComposer) {
  const cfg = composerCfg(ctx);
  if (!cfg.strip) return;
  const files = filesForCtx(ctx);
  if (files.length === 0) {
    cfg.strip.innerHTML = '';
    cfg.strip.hidden = true;
    return;
  }
  cfg.strip.innerHTML = files.map(f => {
    if (f.isImage && f.dataUrl) {
      return `<div class="thumb" data-sid="${f.sid}"><img src="${f.dataUrl}"><button class="x" data-sid="${f.sid}" data-i18n-title="upload.removeTitle" title="">×</button></div>`;
    }
    const name = f.name || 'file';
    const label = name.replace(/[<>&]/g, c => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;' }[c]));
    const sub = fileSubLabel(name).replace(/[<>&]/g, c => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;' }[c]));
    const path = (f.path || '').replace(/[<>&"]/g, c => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;', '"': '&quot;' }[c]));
    const dataName = name.replace(/[<>&"]/g, c => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;', '"': '&quot;' }[c]));
    return `<div class="file-chip pending" data-sid="${f.sid}" data-path="${path}" data-name="${dataName}"><span class="fc-icon">${GA_ICON('fileText')}</span><span class="fc-meta"><span class="fc-name">${label}</span><span class="fc-sub">${sub}</span></span><button class="x" data-sid="${f.sid}" data-i18n-title="upload.removeTitle" title="">×</button></div>`;
  }).join('');
  cfg.strip.hidden = false;
  applyI18n();
}

// 在 ctx 对应输入框(contenteditable)的光标处插入原子 chip（删不进中间，像 @人）
function insertPlaceholderInComposer(file, ctx = activeFileComposer) {
  const input = composerCfg(ctx).input;
  if (!input) return;
  const chip = document.createElement('span');
  chip.className = 'ph-chip';
  chip.setAttribute('contenteditable', 'false');
  chip.dataset.sid = String(file.sid);
  chip.dataset.kind = file.isImage ? 'image' : 'file';
  chip.textContent = file.name || 'file';
  input.focus();
  const sel = window.getSelection();
  let range;
  if (sel && sel.rangeCount && input.contains(sel.getRangeAt(0).commonAncestorContainer)) {
    range = sel.getRangeAt(0);
  } else {
    range = document.createRange(); range.selectNodeContents(input); range.collapse(false);
  }
  range.deleteContents();
  range.insertNode(chip);
  const sp = document.createTextNode(' ');  // chip 后补一个 nbsp，便于继续打字/定位光标
  chip.after(sp);
  range.setStartAfter(sp); range.collapse(true);
  if (sel) { sel.removeAllRanges(); sel.addRange(range); }
  input.dispatchEvent(new Event('input', { bubbles: true }));
}

// 按 sid 从该文件所属 ctx 的输入框移除 chip（连同紧邻空格）
function removePlaceholderFromComposer(file) {
  const input = composerCfg(fileCtx(file)).input;
  if (!input) return;
  const chip = input.querySelector(`.ph-chip[data-sid="${file.sid}"]`);
  if (!chip) return;
  const next = chip.nextSibling;
  if (next && next.nodeType === 3) next.nodeValue = next.nodeValue.replace(/^[\s ]/, '');
  chip.remove();
  input.dispatchEvent(new Event('input', { bubbles: true }));
}

// 读取 contenteditable 输入框为纯文本：chip → [Image #N]/[File #N]，<br>/<div> → 换行
function readComposerTextFrom(input) {
  if (!input) return '';
  const ser = (node, first) => {
    if (node.nodeType === 3) return node.nodeValue;
    if (node.nodeType !== 1) return '';
    if (node.classList && node.classList.contains('exclam-prefix')) return '!';
    if (node.classList && node.classList.contains('ph-chip')) {
      const kind = node.dataset.kind || 'file';
      if (kind === 'image') return `[Image #${node.dataset.sid}]`;
      if (kind === '@file') return `[@file #${node.dataset.sid}]`;
      if (kind === '@folder') return `[@folder #${node.dataset.sid}]`;
      return `[File #${node.dataset.sid}]`;
    }
    if (node.tagName === 'BR') return '\n';
    let inner = '';
    node.childNodes.forEach(c => { inner += ser(c, false); });
    return (first ? '' : '\n') + inner;
  };
  let out = '';
  input.childNodes.forEach((n, i) => { out += ser(n, i === 0); });
  return out.replace(/ /g, ' ');
}

function composerText(ctx = activeFileComposer) {
  return readComposerTextFrom(composerCfg(ctx).input);
}

function isImageFile(f) {
  return (f && (f.type || '').startsWith('image/')) || IMG_EXT_RE.test(f?.name || '');
}

function placeholderFor(file) {
  if (file.isImage) return `[Image #${file.sid}]`;
  if (file.atKind === '@file') return `[@file #${file.sid}]`;
  if (file.atKind === '@folder') return `[@folder #${file.sid}]`;
  return `[File #${file.sid}]`;
}

function expandFilePlaceholders(text) {
  return text.replace(/\[(Image|File|@file|@folder) #(\d+)\]/g, (m, kind, n) => {
    const f = state.pendingFiles.find(x => x.sid === Number(n));
    return (f && f.path) ? f.path : '';  // #3 悬空占位符(无对应文件)→ 删掉,不把垃圾发给 agent
  });
}

function collectUsedFiles(text) {
  const used = [];
  text.replace(/\[(Image|File|@file|@folder) #(\d+)\]/g, (m, kind, n) => {
    const f = state.pendingFiles.find(x => x.sid === Number(n));
    if (f) used.push(f);
    return m;
  });
  return used;
}

// ── 附件占位符健壮性 ─────────────────────────────────────────────
// 统一移除一个待发附件:出列 + (可选)抹占位符 + 重绘 + 删 bridge 上的文件
function removePendingFile(sid, { stripPlaceholder = false } = {}) {
  const idx = state.pendingFiles.findIndex(f => f.sid === sid);
  if (idx < 0) return;
  const removed = state.pendingFiles.splice(idx, 1)[0];
  if (stripPlaceholder) removePlaceholderFromComposer(removed);
  renderThumbStrip(fileCtx(removed));
  if (removed.path) {
    fetch(`${BRIDGE_ORIGIN}/upload`, {
      method: 'DELETE', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path: removed.path }),
    }).catch(() => {});
  }
}
// #1 对账:DOM 里 chip 没了(被原子删除/退格整块删)→ 同步移除附件 + 删磁盘文件
function reconcilePendingFiles(ctx = activeFileComposer) {
  const input = composerCfg(ctx).input;
  if (!input) return;
  const present = new Set([...input.querySelectorAll('.ph-chip[data-sid]')].map(c => Number(c.dataset.sid)));
  for (const f of filesForCtx(ctx).filter(x => !present.has(x.sid))) {
    removePendingFile(f.sid, { stripPlaceholder: false });
  }
}

async function uploadOne(name, dataUrl, sid) {
  const res = await fetch(`${BRIDGE_ORIGIN}/upload`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, dataUrl, sid: sid || '' }),
  });
  const j = await res.json();
  if (!j.ok) throw new Error(j.error || 'upload failed');
  return j.path;
}

async function addFiles(fileList) {
  const files = Array.from(fileList || []);
  if (files.length === 0) return;
  let skipped = false;
  let emptyHit = false;
  const accepted = [];
  for (const f of files) {
    if (!f || f.size === 0) { emptyHit = true; continue; }
    if (f.size > MAX_UPLOAD_BYTES) { skipped = true; continue; }
    if (state.pendingFiles.length + accepted.length >= MAX_UPLOAD_FILES) { skipped = true; break; }
    accepted.push(f);
  }
  if (emptyHit) showChanToast(t('upload.empty'), '', 'err');
  if (accepted.length === 0) {
    if (skipped) showChanToast(t('upload.tooLarge'), '', 'err');
    return;
  }
  const ctx = activeFileComposer;
  let uploadSid = '';
  if (ctx === 'collab') {
    uploadSid = 'collab';
  } else {
    let upSess = activeSess();
    if (!upSess) { await newSession(); upSess = activeSess(); }
    if (upSess && !upSess.bridgeSessionId) { try { await ensureBridgeSession(upSess); } catch (_) {} }
    uploadSid = (upSess && upSess.bridgeSessionId) || '';
  }
  for (const f of accepted) {
    try {
      const dataUrl = await new Promise((resolve, reject) => {
        const r = new FileReader();
        r.onload = () => resolve(String(r.result || ''));
        r.onerror = () => reject(r.error);
        r.readAsDataURL(f);
      });
      const path = await uploadOne(f.name || 'file', dataUrl, uploadSid);
      state.fileSeq += 1;
      const sid = state.fileSeq;
      const isImage = isImageFile(f);
      const entry = {
        sid, name: f.name || 'file', isImage, path,
        dataUrl: isImage ? dataUrl : '',
        ctx,
      };
      state.pendingFiles.push(entry);
      insertPlaceholderInComposer(entry, ctx);
      renderThumbStrip(ctx);
    } catch (e) {
      showChanToast(t('upload.failed'), e.message || String(e), 'err');
    }
  }
  if (skipped) showChanToast(t('upload.tooLarge'), '', 'err');
}

function handleThumbStripClick(e, ctx) {
  const x = e.target.closest('.x');
  if (x) {
    const sid = Number(x.dataset.sid);
    const idx = state.pendingFiles.findIndex(f => f.sid === sid && fileCtx(f) === ctx);
    if (idx >= 0) {
      const removed = state.pendingFiles[idx];
      state.pendingFiles.splice(idx, 1);
      removePlaceholderFromComposer(removed);
      renderThumbStrip(ctx);
      if (removed.path) {
        fetch(`${BRIDGE_ORIGIN}/upload`, {
          method: 'DELETE',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ path: removed.path }),
        }).catch(() => {});
      }
    }
    return;
  }
  const fileChip = e.target.closest('.file-chip.pending');
  if (fileChip) {
    const path = fileChip.getAttribute('data-path');
    const name = fileChip.getAttribute('data-name');
    if (path) openUploadFile(path, name);
    return;
  }
  const img = e.target.closest('img');
  if (img && img.src) openLightbox(img.src);
}

function bindComposerUpload(ctx) {
  const cfg = composerCfg(ctx);
  if (!cfg.input || cfg.input.dataset.gaUploadBound) return;
  cfg.input.dataset.gaUploadBound = ctx;

  if (cfg.uploadBtn && !cfg.uploadBtn.dataset.bound) {
    cfg.uploadBtn.dataset.bound = '1';
    cfg.uploadBtn.addEventListener('click', (e) => {
      e.preventDefault();
      activeFileComposer = ctx;
      cfg.imgInput?.click();
    });
  }
  if (cfg.imgInput && !cfg.imgInput.dataset.bound) {
    cfg.imgInput.dataset.bound = '1';
    cfg.imgInput.addEventListener('change', () => {
      activeFileComposer = ctx;
      addFiles(cfg.imgInput.files);
      cfg.imgInput.value = '';
    });
  }
  cfg.strip?.addEventListener('click', (e) => handleThumbStripClick(e, ctx));
  cfg.input.addEventListener('paste', (e) => {
    activeFileComposer = ctx;
    const cd = e.clipboardData || window.clipboardData;
    const items = cd && cd.items;
    const files = [];
    if (items) for (const it of items) { if (it.kind === 'file') { const f = it.getAsFile(); if (f) files.push(f); } }
    if (files.length) { e.preventDefault(); addFiles(files); return; }
    // 富文本粘贴 → 强制纯文本（contenteditable 默认会粘 HTML，会污染输入框）
    e.preventDefault();
    const text = cd ? cd.getData('text/plain').replace(/\r\n/g, '\n') : '';
    if (!text) return;
    // Hard-limit: only insert what fits within maxLen
    const maxLen = 20000;
    const curLen = cfg.input.textContent.length;
    const sel = window.getSelection();
    const selLen = (sel.rangeCount && cfg.input.contains(sel.anchorNode)) ? sel.toString().length : 0;
    const remaining = maxLen - curLen + selLen;
    if (remaining <= 0) { showChanToast(t('err.charLimit').replace('{n}', maxLen), '', 'err'); return; }
    const insert = text.slice(0, remaining);
    document.execCommand('insertText', false, insert);
    if (text.length > remaining) showChanToast(t('err.charLimit').replace('{n}', maxLen), '', 'err');
  });
  cfg.input.addEventListener('input', () => {
    activeFileComposer = ctx;
    // 内容清空后浏览器可能残留 <br>，抹掉以便 :empty 占位提示生效
    if (!cfg.input.textContent.trim() && !cfg.input.querySelector('.ph-chip')) cfg.input.innerHTML = '';
    reconcilePendingFiles(ctx);  // chip 被删 → 同步清理附件 + 删磁盘文件
  });
  const zone = cfg.dropZone;
  const dropKey = `dropBound_${ctx}`;
  if (!zone || zone.dataset[dropKey]) return;
  zone.dataset[dropKey] = '1';
  let dragDepth = 0;
  const hasFiles = (e) => {
    const types = e.dataTransfer && e.dataTransfer.types;
    if (!types) return false;
    for (let i = 0; i < types.length; i += 1) {
      if (types[i] === 'Files') return true;
    }
    return false;
  };
  zone.addEventListener('dragenter', (e) => {
    if (!hasFiles(e)) return;
    e.preventDefault();
    activeFileComposer = ctx;
    dragDepth += 1;
    zone.classList.add('dragover');
    zone.dataset.dropHint = t('upload.dropHint');
  });
  zone.addEventListener('dragover', (e) => {
    if (!hasFiles(e)) return;
    e.preventDefault();
    activeFileComposer = ctx;
    e.dataTransfer.dropEffect = 'copy';
  });
  zone.addEventListener('dragleave', (e) => {
    if (!hasFiles(e)) return;
    dragDepth = Math.max(0, dragDepth - 1);
    if (dragDepth === 0) zone.classList.remove('dragover');
  });
  zone.addEventListener('drop', (e) => {
    if (!hasFiles(e)) return;
    e.preventDefault();
    dragDepth = 0;
    zone.classList.remove('dragover');
    activeFileComposer = ctx;
    addFiles(e.dataTransfer.files);
  });
}

bindComposerUpload('chat');
bindComposerUpload('collab');

Object.assign(window, {
  gaSetActiveFileComposer: ctx => { activeFileComposer = ctx === 'collab' ? 'collab' : 'chat'; },
  gaPageStatusBar: pageStatusBar,
  gaExpandFilePlaceholders: expandFilePlaceholders,
  gaRenderMsgChips: renderMsgTextWithChips,
  gaCollectUsedFiles: collectUsedFiles,
  gaComposerText: composerText,
  gaClearUsedPendingFiles: text => removeUsedPendingFiles(collectUsedFiles(text)),
  gaFileSubLabel: fileSubLabel,
  gaMsgNode: msgNode,
  gaCollabItemToMsg: collabItemToMsg,
  gaPostRenderEnhance: postRenderEnhance,
  gaEscapeHtml: escapeHtml,
});

if (chatPanel) {
  const blockFileDrop = e => {
    const types = e.dataTransfer?.types;
    if (!types) return;
    for (let i = 0; i < types.length; i += 1) if (types[i] === 'Files') { e.preventDefault(); return; }
  };
  window.addEventListener('dragover', blockFileDrop);
  window.addEventListener('drop', blockFileDrop);
}

/* ═══════════════ bridge 事件 ═══════════════ */
window.ga.onBridgeReady(async () => {
  state.bridgeReady = true;
  syncPlanPollTimer();
  refreshStatusLabel();
  if (!state.activeId) { refreshEmptyState(null); }
  await loadModelProfiles();
  await loadBridgeConfig();
  if (isServicesPageActive()) renderChannelList(gaServiceStore.list());
  const sess = activeSess();
  if (sess && sessionNeedsHydrate(sess)) {
    await runSessionHydrate(sess);
  } else if (sess) planPoll(sess);
  delete document.documentElement.dataset.bootHasSessions;
  if (sess) refreshEmptyState(sess);
});
setTimeout(() => { delete document.documentElement.dataset.bootHasSessions; }, 3000);
window.ga.onBridgeNotification((msg) => {
  if (msg && msg.type === 'session-state') {
    let found = false;
    for (const sess of state.sessions.values()) {
      if (sess.bridgeSessionId === msg.sessionId) {
        found = true;
        if (msg.status === 'running' || msg.state === 'running') pollSession(sess);
        if (msg.state === 'idle' || msg.status === 'idle') tokPollBridge();
        renderSessionList();
        break;
      }
    }
    if (!found) {
      loadSessions().then(() => {
        state.activeId = msg.sessionId;
        renderSessionList();
      });
    }
  }
});
window.ga.onBridgeError((err) => { console.warn('[bridge error]', err); });
window.ga.onBridgeClosed(() => {
  state.bridgeReady = false;
  syncPlanPollTimer();
  const s = activeSess();
  if (s) applyPlanPayload(s, null);
  chatStatus.setDisconnected();
});

/* ═══════════════ Token 用量页 ═══════════════ */
const tokTbody = document.getElementById('tok-tbody');
const tokTable = document.getElementById('tok-table');
const tokPager = document.getElementById('tok-pager');
const tokSince = document.getElementById('tok-since');
const tokUntil = document.getElementById('tok-until');
const tokTotalN = document.getElementById('tok-total-n');
const tokTodayN = document.getElementById('tok-today-n');
const tokCostN = document.getElementById('tok-cost-n');
const TOK_PER_PAGE = 15;
let _tokPage = 0;
let _tokHistory = [];
let _tokLastSnap = {};

// Model price table: $/M tokens [input, output]
const MODEL_PRICES = {
  'gpt-5.4':[2.50,15],'gpt-5':[1.25,10],'gpt-5-mini':[0.25,2],'gpt-4o':[2.50,10],'gpt-4o-mini':[0.15,0.60],
  'gpt-4.1':[2,8],'gpt-4.1-mini':[0.40,1.60],'gpt-4.1-nano':[0.10,0.40],'o4-mini':[0.55,2.20],
  'claude-opus-4-8':[5,25],'claude-opus-4-7':[5,25],'claude-opus-4-6':[5,25],'claude-sonnet-4-6':[3,15],'claude-sonnet-4-5':[3,15],'claude-haiku-4-5':[1,5],
  'deepseek-v4':[0.14,0.28],'deepseek-v4-pro':[0.435,0.87],'deepseek-chat':[0.14,0.28],'deepseek-reasoner':[0.55,2.19],
  'glm-5.1':[0.50,0.50],'minimax-m2.7':[0.50,0.50],'kimi-for-coding':[0.50,2],
};
const CNY_RATE = 7.2;
function estCost(inp, out, model, cacheRead, cacheCreate) {
  let p = [3,15];
  if (model) { const m = model.toLowerCase().replace(/\[.*\]/,''); p = MODEL_PRICES[m] || Object.entries(MODEL_PRICES).find(([k])=>m.includes(k))?.[1] || p; }
  const isClaudeOrDS = model && /claude|deepseek/i.test(model);
  const cacheReadRate = isClaudeOrDS ? 0.1 : 0.5;
  const cacheWriteRate = isClaudeOrDS ? 1.25 : 1.0;
  const cost = (inp*p[0] + out*p[1] + (cacheRead||0)*p[0]*cacheReadRate + (cacheCreate||0)*p[0]*cacheWriteRate) / 1e6 * CNY_RATE;
  return cost.toFixed(2);
}
function fmtTok(n) { return n>=1e6?(n/1e6).toFixed(2)+'M':n>=1e3?(n/1e3).toFixed(1)+'k':String(n); }
function fmtTime(ts) { return new Date(ts*1000).toLocaleString(undefined,{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}); }
function modelPriceTip(model) {
  if (!model) return '';
  const m = model.toLowerCase().replace(/\[.*\]/,'');
  const entry = MODEL_PRICES[m] || Object.entries(MODEL_PRICES).find(([k])=>m.includes(k))?.[1];
  const known = !!entry;
  const p = entry || [3,15];
  const isClaudeOrDS = /claude|deepseek/i.test(m);
  const cacheReadRate = isClaudeOrDS ? 0.1 : 0.5;
  const cacheWriteRate = isClaudeOrDS ? 1.25 : 1.0;
  const lines = [];
  if (!known) lines.push(t('tok.pricingUnknown'));
  lines.push(t('tok.priceInput') + p[0] + ' /M');
  lines.push(t('tok.priceOutput') + p[1] + ' /M');
  lines.push(t('tok.priceCacheW') + (p[0] * cacheWriteRate).toFixed(2) + ' /M');
  lines.push(t('tok.priceCacheR') + (p[0] * cacheReadRate).toFixed(2) + ' /M');
  return lines.join('\n');
}

function tokLoadHistory() { return _tokHistory; }
function tokSaveHistory(h) {
  _tokHistory = h;
  fetch(`${BRIDGE_ORIGIN}/token-history`, {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({history:h, snap:_tokLastSnap, conductorHist:_condHist, conductorLast:_condLast})
  }).catch(()=>{});
}

let _tokPolling = false;
async function tokPollBridge() {
  if (_tokPolling) return;
  _tokPolling = true;
  try {
    if (!_tokHistory.length) {
      const stored = await bridgeFetch('/token-history');
      if (stored.history?.length) _tokHistory = stored.history;
      if (stored.snap) _tokLastSnap = stored.snap;
      if (stored.conductorHist) _condHist = stored.conductorHist;
      if (stored.conductorLast) _condLast = stored.conductorLast;
    }
    const data = await bridgeFetch('/token-stats');
    const history = tokLoadHistory();
    for (const r of (data.records||[])) {
      const key = r.thread;
      const sid = key.replace('GA-','');
      const sess = [...state.sessions.values()].find(s=>s.bridgeSessionId===sid);
      if (sess && rt(sess).busy) continue;
      const prev = _tokLastSnap[key] || {input:0,output:0,cacheCreate:0,cacheRead:0};
      let di = r.input-prev.input, do_ = r.output-prev.output, dc = r.cacheCreate-prev.cacheCreate, dr = r.cacheRead-prev.cacheRead;
      if (di<0||do_<0||dc<0||dr<0) { di = r.input; do_ = r.output; dc = r.cacheCreate; dr = r.cacheRead; }
      if (di>0||do_>0||dc>0||dr>0) {
        const title = sess ? displayTitle(sess) : sid;
        history.push({sessionId:sid, title:title, input:di, output:do_, cacheCreate:dc, cacheRead:dr, model:r.model||'', ts:Date.now()/1000});
        if(sess?.title) history.forEach(h=>{if(h.sessionId===sid&&(!h.title||h.title===sid))h.title=sess.title;});
      }
      _tokLastSnap[key] = {input:r.input, output:r.output, cacheCreate:r.cacheCreate, cacheRead:r.cacheRead};
    }
    tokSaveHistory(history);
  } catch(_) {}
  _tokPolling = false;
}

function tokGetFiltered() {
  let records = tokLoadHistory();
  const parseD = v => v ? new Date(v.replace(/\s+/,'T')).getTime()/1000 : 0;
  const since = parseD(tokSince?.value);
  const until = parseD(tokUntil?.value);
  if (since) records = records.filter(r=>r.ts>=since);
  if (until) records = records.filter(r=>r.ts<=until);
  return records;
}

function tokRenderStats(filtered, all) {
  let total=0, totalInput=0, totalCacheRead=0, totalCacheCreate=0;
  filtered.forEach(r=>{total+=(r.input||0)+(r.output||0)+(r.cacheRead||0)+(r.cacheCreate||0); totalInput+=(r.input||0); totalCacheRead+=(r.cacheRead||0); totalCacheCreate+=(r.cacheCreate||0);});
  if(tokTotalN) tokTotalN.textContent=fmtTok(total);
  const cacheBase = totalInput + totalCacheRead + totalCacheCreate;
  if(tokCostN) tokCostN.textContent= cacheBase > 0 ? (totalCacheRead / cacheBase * 100).toFixed(1) + '%' : '0%';
  const todayStart=new Date(); todayStart.setHours(0,0,0,0); const todayTs=todayStart.getTime()/1000;
  let todayT=0; all.filter(r=>r.ts>=todayTs).forEach(r=>{todayT+=(r.input||0)+(r.output||0)+(r.cacheRead||0)+(r.cacheCreate||0);});
  if(tokTodayN) tokTodayN.textContent=fmtTok(todayT);
}

function tokRenderTable(records) {
  if(!tokTbody) return;
  const bySession=new Map();
  for(const r of records){
    const k=r.sessionId||'?';
    const ss= r._conductor ? null : [...state.sessions.values()].find(s=>s.bridgeSessionId===k);
    let title = ss ? displayTitle(ss) : (r.title||k);
    const deleted = r._conductor ? !!r._killed : !ss;
    if(!bySession.has(k)) bySession.set(k,{title:title,deleted:deleted,input:0,output:0,cacheCreate:0,cacheRead:0,lastTs:0,prompts:[]});
    const s=bySession.get(k); s.input+=r.input||0; s.output+=r.output||0; s.cacheCreate+=r.cacheCreate||0; s.cacheRead+=r.cacheRead||0;
    if(r.ts>s.lastTs){s.lastTs=r.ts; s.title=title;} s.prompts.push(r);
  }
  tokTbody.innerHTML='';
  if(bySession.size===0){tokTbody.innerHTML=`<tr><td colspan="6" style="color:var(--muted)">${t('tok.noData')}</td></tr>`;if(tokPager)tokPager.innerHTML='';return;}
  const sorted=[...bySession.values()].sort((a,b)=>b.lastTs-a.lastTs);
  const totalPages=Math.ceil(sorted.length/TOK_PER_PAGE);
  if(_tokPage>=totalPages)_tokPage=totalPages-1;
  const pageItems=sorted.slice(_tokPage*TOK_PER_PAGE,(_tokPage+1)*TOK_PER_PAGE);
  for(const s of pageItems){
    const sCacheBase = s.input + s.cacheRead + s.cacheCreate;
    const sCacheRate = sCacheBase > 0 ? (s.cacheRead / sCacheBase * 100).toFixed(1) + '%' : '0%';
    const tr=document.createElement('tr'); tr.className='tok-row-session';
    tr.innerHTML=`<td title="${escapeHtml(s.title)}">${escapeHtml(s.title)}${s.deleted?'<span class="tok-deleted">'+t('tok.deleted')+'</span>':''}</td><td>${fmtTok(s.input)}</td><td>${fmtTok(s.output)}</td><td>${fmtTok(s.cacheCreate)}</td><td>${fmtTok(s.cacheRead)}</td><td>${sCacheRate}</td>`;
    tokTbody.appendChild(tr);
    const details=[]; s.prompts.sort((a,b)=>b.ts-a.ts);
    for(const p of s.prompts){
      const dr=document.createElement('tr'); dr.className='tok-detail'; dr.hidden=true;
      const modelHtml = p.model ? ` · <span class="tok-model-tip">${escapeHtml(p.model)}</span>` : '';
      const pCacheBase = (p.input||0) + (p.cacheRead||0) + (p.cacheCreate||0);
      const pCacheRate = pCacheBase > 0 ? ((p.cacheRead||0) / pCacheBase * 100).toFixed(1) + '%' : '0%';
      dr.innerHTML=`<td>${fmtTime(p.ts)}${modelHtml}</td><td>${fmtTok(p.input||0)}</td><td>${fmtTok(p.output||0)}</td><td>${fmtTok(p.cacheCreate||0)}</td><td>${fmtTok(p.cacheRead||0)}</td><td>${pCacheRate}</td>`;
      tokTbody.appendChild(dr); details.push(dr);
    }
    tr.addEventListener('click',()=>{const o=tr.classList.toggle('open');details.forEach(d=>d.hidden=!o);});
  }
  if(tokPager){renderTokPager(tokPager, totalPages, _tokPage, p => { _tokPage = p; tokRenderTable(records); });}
}

/* 分页按钮:首页/末页 + 当前页前后各 2 + 省略号,最多渲染 ~9 个 DOM,
   1000 页和 10 页都长一样,不会一行排开拖死浏览器。 */
function renderTokPager(host, totalPages, currentPage, onJump) {
  host.innerHTML = '';
  if (totalPages <= 1) return;
  const makeBtn = (label, page, opts = {}) => {
    const b = document.createElement('button');
    if (opts.svg) b.innerHTML = label;
    else b.textContent = label;
    if (opts.active) b.classList.add('active');
    if (opts.arrow) b.classList.add('tok-pager-arrow');
    if (opts.disabled) b.disabled = true;
    if (!opts.disabled) b.addEventListener('click', () => onJump(page));
    return b;
  };
  const makeEllipsis = () => {
    const s = document.createElement('span');
    s.className = 'tok-pager-gap';
    s.textContent = '…';
    return s;
  };
  // 收集页码:1 / 当前-2..当前+2 / 末页,去重并填省略号
  const pages = new Set([0, totalPages - 1]);
  for (let i = Math.max(0, currentPage - 2); i <= Math.min(totalPages - 1, currentPage + 2); i++) pages.add(i);
  const sorted = [...pages].sort((a, b) => a - b);
  // 首尾箭头用 phosphor 图标(跟侧栏 .chev 同款),不再用 Unicode 字符
  host.appendChild(makeBtn(GA_ICON('caretLeft'), currentPage - 1, { svg: true, arrow: true, disabled: currentPage === 0 }));
  let prev = -1;
  for (const p of sorted) {
    if (prev >= 0 && p - prev > 1) host.appendChild(makeEllipsis());
    host.appendChild(makeBtn(String(p + 1), p, { active: p === currentPage }));
    prev = p;
  }
  host.appendChild(makeBtn(GA_ICON('caretRight'), currentPage + 1, { svg: true, arrow: true, disabled: currentPage === totalPages - 1 }));
}

async function loadTokenPage(){await tokPollBridge();const f=tokGetFiltered();const all=tokLoadHistory();tokRenderStats(f,all);tokRenderTable(f);}

const _COND_HIST_KEY = 'conductor_token_hist';
const _COND_LAST_KEY = 'conductor_token_last';
const _condZero = {input:0,output:0,cacheCreate:0,cacheRead:0,cost:0};
let _condHist = null, _condLast = null;
function _condLoadHist() { return _condHist || {..._condZero}; }
function _condLoadLast() { return _condLast; }
function _condSave(hist, last) {
  _condHist = hist; _condLast = last;
  fetch(`${BRIDGE_ORIGIN}/token-history`, {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({history:_tokHistory, snap:_tokLastSnap, conductorHist:hist, conductorLast:last})
  }).catch(()=>{});
}

/* ─── Token tab switching ─── */
let _tokTab = 'chat';
const tokTabs = document.getElementById('tok-tabs');
const tokFilter = document.querySelector('.tok-filter');
const tokStatRow = document.querySelector('.page[data-page="token"] .stat-row');
if (tokTabs) tokTabs.addEventListener('click', e => {
  const btn = e.target.closest('.tok-tab');
  if (!btn || btn.classList.contains('active')) return;
  tokTabs.querySelectorAll('.tok-tab').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  _tokTab = btn.dataset.tab;
  _tokPage = 0;
  if (_tokTab === 'conductor') { if (tokFilter) tokFilter.style.display = 'none'; if (tokStatRow) tokStatRow.style.display = 'none'; if (tokTable) tokTable.classList.add('tok-table--conductor'); loadConductorTokens(); }
  else { if (tokFilter) tokFilter.style.display = ''; if (tokStatRow) tokStatRow.style.display = ''; if (tokTable) tokTable.classList.remove('tok-table--conductor'); loadTokenPage(); }
});

async function loadConductorTokens() {
  let curIn = 0, curOut = 0, curCc = 0, curCr = 0, curCost = 0;
  let fetchOk = false;
  try {
    const data = await (await fetch(`${CONDUCTOR_ORIGIN}/token-stats`)).json();
    const recs = (data.records || []).filter(r => r.thread === 'conductor-agent' || r.thread.startsWith('subagent-'));
    for (const r of recs) {
      curIn += r.input || 0; curOut += r.output || 0; curCc += r.cacheCreate || 0; curCr += r.cacheRead || 0;
      curCost += parseFloat(estCost(r.input || 0, r.output || 0, r.model || '', r.cacheRead || 0, r.cacheCreate || 0));
    }
    fetchOk = true;
  } catch (_) {
    if (tokTbody) tokTbody.innerHTML = `<tr><td colspan="6" style="color:var(--muted)">${t('tok.condOffline')}</td></tr>`;
    return;
  }
  const hist = _condLoadHist();
  const last = _condLoadLast();
  if (fetchOk && last && (curIn < last.input || curOut < last.output)) {
    hist.input += last.input; hist.output += last.output; hist.cacheCreate += last.cacheCreate; hist.cacheRead += last.cacheRead; hist.cost += last.cost;
  }
  if (fetchOk) _condSave(hist, {input:curIn, output:curOut, cacheCreate:curCc, cacheRead:curCr, cost:curCost});
  const hIn = hist.input + curIn, hOut = hist.output + curOut, hCc = hist.cacheCreate + curCc, hCr = hist.cacheRead + curCr, hCost = hist.cost + curCost;
  if (!tokTbody) return;
  const tip = t('tok.condTip');
  const _ci = GA_ICON('gitFork', 'tok-cond-ico');
  const hCacheBase = hIn + hCr + hCc;
  const hCacheRate = hCacheBase > 0 ? (hCr / hCacheBase * 100).toFixed(1) + '%' : '0%';
  const curCacheBase = curIn + curCr + curCc;
  const curCacheRate = curCacheBase > 0 ? (curCr / curCacheBase * 100).toFixed(1) + '%' : '0%';
  tokTbody.innerHTML = `<tr class="tok-row-conductor" title="${tip}"><td>${_ci}${t('tok.condTotal')}</td><td>${fmtTok(hIn)}</td><td>${fmtTok(hOut)}</td><td>${fmtTok(hCc)}</td><td>${fmtTok(hCr)}</td><td>${hCacheRate}</td></tr><tr class="tok-row-conductor" title="${tip}"><td>${_ci}${t('tok.condCurrent')}</td><td>${fmtTok(curIn)}</td><td>${fmtTok(curOut)}</td><td>${fmtTok(curCc)}</td><td>${fmtTok(curCr)}</td><td>${curCacheRate}</td></tr>`;
  const pager = document.getElementById('tok-pager');
  if (pager) pager.innerHTML = '';
}

/* Flatpickr 初始化 */
const _fpOpts = { enableTime:true, time_24hr:true, dateFormat:'Y-m-d  H:i', locale:window.flatpickr?.l10ns?.[document.documentElement.lang==='en'?'default':'zh']||'default', allowInput:false, onChange(){ _tokPage=0; loadTokenPage(); } };
const fpSince = tokSince ? flatpickr(tokSince, _fpOpts) : null;
const fpUntil = tokUntil ? flatpickr(tokUntil, _fpOpts) : null;
const tokResetBtn=document.getElementById('tok-reset');
if(tokResetBtn)tokResetBtn.addEventListener('click',()=>{if(fpSince)fpSince.clear();if(fpUntil)fpUntil.clear();_tokPage=0;loadTokenPage();});

/* ─── Token trend chart ─── */
nav.addEventListener('click',(e)=>{const item=e.target.closest('.nav-item');if(item&&item.dataset.page==='token'){if(_tokTab==='conductor')loadConductorTokens();else loadTokenPage();}if(item&&item.dataset.page==='services')refreshServicesPanel();if(item&&item.dataset.page==='tasks')loadTasksPage();if(item&&item.dataset.page==='files')loadFilesPage();});
/* ═══════════════ 定时任务 ═══════════════ */
let _taskTab = 'list';
const taskTabs = document.getElementById('task-tabs');
const taskListEl = document.getElementById('task-list');
const taskTemplatesEl = document.getElementById('task-templates');
const taskEmptyEl = document.getElementById('task-empty');
const taskHistoryListEl = document.getElementById('task-history-list');
const taskHistoryEmptyEl = document.getElementById('task-history-empty');

const TASK_TEMPLATES = [
  { title: '设置每日「14:00」的自动定时任务，根据我的星座「双子座」提供今日的行动注意指引发送给我', desc: '' },
  { title: '提醒我喝水，在「每日 14:00」执行，从「今天」开始并「持续生效」，任务创建后设置为「立即启用」', desc: '' },
  { title: '设置「每天」为我推送当天最新的「10条」科技新闻，每条新闻总结要精简', desc: '' },
];

function setTaskTab(tab) {
  if (!taskTabs) return;
  _taskTab = tab;
  taskTabs.querySelectorAll('.task-tab').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
  document.querySelectorAll('.task-panel').forEach(p => p.classList.toggle('active', p.dataset.taskPanel === tab));
  if (tab === 'list') renderTaskTemplates();
  else loadTaskHistory();
}

if (taskTabs) taskTabs.addEventListener('click', e => {
  const btn = e.target.closest('.task-tab');
  if (!btn || btn.dataset.tab === _taskTab) return;
  setTaskTab(btn.dataset.tab);
});

function renderTaskTemplates() {
  if (!taskTemplatesEl) return;
  taskTemplatesEl.innerHTML = TASK_TEMPLATES.map(t => `
    <div class="task-template" data-template="${encodeURIComponent(t.title)}">
      <div class="task-template-left">
        <div class="task-template-title">${escapeHtml(t.title)}</div>
        ${t.desc ? `<div class="task-template-desc">${escapeHtml(t.desc)}</div>` : ''}
      </div>
      <div class="task-template-arrow"><span data-ga-icon="caretRight"></span></div>
    </div>
  `).join('');
  if (window.gaHydrateIcons) window.gaHydrateIcons(taskTemplatesEl);
}

async function loadTasksPage() {
  setTaskTab('list');
  renderTaskTemplates();
  await loadTaskList();
  loadTaskHistory();
}

async function loadTaskList() {
  if (!taskListEl || !taskEmptyEl) return;
  try {
    const resp = await fetch(`${BRIDGE_ORIGIN}/services/tasks/list`);
    const data = await resp.json();
    const tasks = data.tasks || [];
    if (tasks.length === 0) {
      taskEmptyEl.hidden = false;
      taskListEl.innerHTML = '';
    } else {
      taskEmptyEl.hidden = true;
      taskListEl.innerHTML = tasks.map(t => `
        <div class="task-item">
          <div class="task-item-check ${t.enabled ? 'on' : ''}" data-task-id="${t.id}" title="${t.enabled ? '禁用' : '启用'}">
            ${t.enabled ? '<span data-ga-icon="check"></span>' : ''}
          </div>
          <div class="task-item-info">
            <div class="task-item-name">${escapeHtml(t.name || t.id)}</div>
            <div class="task-item-meta">
              <span class="task-item-schedule"><span data-ga-icon="clock"></span>${escapeHtml(t.schedule || '')} ${escapeHtml(t.repeat || '')}</span>
              <span class="task-item-status ${t.status || 'healthy'}">${statusText(t.status)}</span>
            </div>
          </div>
          <div class="task-item-actions">
            <button type="button" data-task-id="${t.id}" data-action="edit" title="编辑"><span data-ga-icon="pencilSimple"></span></button>
            <button type="button" data-task-id="${t.id}" data-action="delete" title="删除" class="danger"><span data-ga-icon="trash"></span></button>
          </div>
        </div>
      `).join('');
      if (window.gaHydrateIcons) window.gaHydrateIcons(taskListEl);
    }
  } catch (_) {
    taskEmptyEl.hidden = false;
    taskListEl.innerHTML = '';
  }
}

function statusText(status) {
  const map = { healthy: '正常', overdue: '逾期', disabled: '已禁用', never: '从未运行', error: '错误' };
  return map[status] || status || '正常';
}

async function loadTaskHistory() {
  if (!taskHistoryListEl || !taskHistoryEmptyEl) return;
  try {
    const resp = await fetch(`${BRIDGE_ORIGIN}/services/tasks/history`);
    const data = await resp.json();
    const history = data.history || [];
    if (history.length === 0) {
      taskHistoryEmptyEl.hidden = false;
      taskHistoryListEl.innerHTML = '';
    } else {
      taskHistoryEmptyEl.hidden = true;
      taskHistoryListEl.innerHTML = history.map(h => `
        <div class="task-history-item">
          <div class="task-history-name">${escapeHtml(h.name || '')}</div>
          <div class="task-history-time">${escapeHtml(h.time || '')}</div>
        </div>
      `).join('');
    }
  } catch (_) {
    taskHistoryEmptyEl.hidden = false;
    taskHistoryListEl.innerHTML = '';
  }
}

if (taskListEl) taskListEl.addEventListener('click', async e => {
  const check = e.target.closest('.task-item-check');
  const btn = e.target.closest('button[data-action]');
  if (check) {
    const tid = check.dataset.taskId;
    await fetch(`${BRIDGE_ORIGIN}/services/tasks/toggle/${tid}`, { method: 'POST' });
    await loadTaskList();
  } else if (btn) {
    const tid = btn.dataset.taskId;
    const action = btn.dataset.action;
    if (action === 'delete') {
      if (await showConfirmDialog({ title: t('common.delete'), message: t('common.delete') + '?', okText: t('common.delete'), okKind: 'danger' })) {
        await fetch(`${BRIDGE_ORIGIN}/services/tasks/delete/${tid}`, { method: 'DELETE' });
        await loadTaskList();
      }
    } else if (action === 'edit') {
      try {
        const resp = await fetch(`${BRIDGE_ORIGIN}/services/tasks/get/${tid}`);
        const data = await resp.json();
        if (data.task) openTaskCreateModal(null, data.task);
      } catch (e) { console.error(e); }
    }
  }
});

if (taskTemplatesEl) taskTemplatesEl.addEventListener('click', e => {
  const tmpl = e.target.closest('.task-template');
  if (!tmpl) return;
  const title = decodeURIComponent(tmpl.dataset.template);
  openTaskCreateModal(title);
});

const taskCreateBtn = document.getElementById('task-create-btn');
if (taskCreateBtn) taskCreateBtn.addEventListener('click', () => {
  openTaskCreateModal();
});

/* ── 创建定时任务 弹窗 ── */
let _tcEditId = null;   // null=新建; 否则为正在编辑的任务 id
function openTaskCreateModal(prefillPrompt, editTask) {
  _tcEditId = (editTask && editTask.id) || null;
  const nameEl = document.getElementById('tc-name');
  const promptEl = document.getElementById('tc-prompt');
  const repeatEl = document.getElementById('tc-repeat');
  const schedEl = document.getElementById('tc-schedule');
  const errEl = document.getElementById('tc-error');
  const advEl = document.getElementById('tc-adv');
  const advToggleEl = document.getElementById('tc-adv-toggle');
  const maxDelayEl = document.getElementById('tc-max-delay');
  const enabledEl = document.getElementById('tc-enabled-switch');
  const et = editTask || {};
  if (nameEl) nameEl.value = et.name || '';
  if (promptEl) {
    promptEl.value = et.prompt || prefillPrompt || '';
    promptEl.textContent = et.prompt || prefillPrompt || '';
  }
  if (repeatEl) repeatEl.value = et.repeat || 'daily';
  if (schedEl) schedEl.value = et.schedule || '08:00';
  if (maxDelayEl) maxDelayEl.value = (et.max_delay_hours != null ? String(et.max_delay_hours) : '6');
  if (enabledEl) enabledEl.setAttribute('aria-checked', String(et.enabled !== false));
  if (advEl) advEl.hidden = true;
  if (advToggleEl) advToggleEl.classList.remove('open');
  if (errEl) errEl.hidden = true;
  // 填充模型下拉
  const modelSel = document.getElementById('tc-model');
  if (modelSel) {
    const profiles = (state.modelProfiles || []);
    // 保留默认选项，重建其余
    modelSel.innerHTML = '<option value="">' + t('taskForm.modelDefault') + '</option>';
    profiles.forEach(p => {
      const opt = document.createElement('option');
      opt.value = p.name || '';
      opt.textContent = p.name || '';
      modelSel.appendChild(opt);
    });
    modelSel.value = et.model || '';
  }
  openModal('task-create-modal');
}

function bindTaskCreateSave() {
  const saveBtn = document.getElementById('tc-save');
  if (!saveBtn || saveBtn._tcBound) return;
  saveBtn._tcBound = true;

  /* 高级设置折叠 */
  const advToggleEl = document.getElementById('tc-adv-toggle');
  const advEl = document.getElementById('tc-adv');
  if (advToggleEl) advToggleEl.addEventListener('click', () => {
    if (!advEl) return;
    advEl.hidden = !advEl.hidden;
    advToggleEl.classList.toggle('open', !advEl.hidden);
  });

  /* set-switch toggle (通用) */
  document.querySelectorAll('#task-create-modal .set-switch').forEach(sw => {
    if (sw._tcBound) return;
    sw._tcBound = true;
    sw.addEventListener('click', () => {
      const checked = sw.getAttribute('aria-checked') === 'true';
      sw.setAttribute('aria-checked', String(!checked));
    });
  });

  saveBtn.addEventListener('click', async () => {
    const name = (document.getElementById('tc-name') || {}).value || '';
    const prompt = (document.getElementById('tc-prompt') || {}).value || '';
    const repeat = (document.getElementById('tc-repeat') || {}).value || 'daily';
    const schedule = (document.getElementById('tc-schedule') || {}).value || '08:00';
    const maxDelayRaw = (document.getElementById('tc-max-delay') || {}).value || '';
    const enabledSw = document.getElementById('tc-enabled-switch');
    const enabled = enabledSw ? enabledSw.getAttribute('aria-checked') !== 'false' : true;
    const model = (document.getElementById('tc-model') || {}).value || '';
    const errEl = document.getElementById('tc-error');
    if (!prompt.trim()) {
      if (errEl) { errEl.textContent = t('task.promptRequired'); errEl.hidden = false; }
      return;
    }
    const max_delay_hours = maxDelayRaw ? Math.max(0, parseInt(maxDelayRaw, 10) || 0) : 0;
    saveBtn.disabled = true;
    try {
      const url = _tcEditId
        ? `${BRIDGE_ORIGIN}/services/tasks/update/${_tcEditId}`
        : `${BRIDGE_ORIGIN}/services/tasks/create`;
      const resp = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: name.trim(), prompt: prompt.trim(), repeat, schedule, max_delay_hours, enabled, model })
      });
      const data = await resp.json();
      if (data.ok) {
        _tcEditId = null;
        closeModals();
        showToast(t('task.createOk'));
        void loadTaskList();
      } else {
        if (errEl) { errEl.textContent = data.error || t('task.createFail'); errEl.hidden = false; }
      }
    } catch (e) {
      if (errEl) { errEl.textContent = String(e) || t('task.createFail'); errEl.hidden = false; }
    } finally {
      saveBtn.disabled = false;
    }
  });
}
bindTaskCreateSave();

/* ═══════════════ 自定义预设 ═══════════════ */
const CP_KEY = 'ga_custom_presets';
const HB_KEY = 'ga_hidden_builtins';

const BUILTIN_PRESETS = [
  { key: 'butler', titleKey: 'preset.butler.t', descKey: 'preset.butler.d', navigate: 'collab',
    get iconSvg() { return GA_ICON('gitFork', 'fc-ic'); } },
  { key: 'plan',   titleKey: 'preset.plan.t',   descKey: 'preset.plan.d',   promptKey: 'presetPrompt.plan',
    get iconSvg() { return GA_ICON('listChecks', 'fc-ic'); } },
  { key: 'goal',    titleKey: 'preset.goal.t',    descKey: 'preset.goal.d',    promptKey: 'presetPrompt.goal',
    get iconSvg() { return GA_ICON('crosshair', 'fc-ic'); } },
  { key: 'autonomous', titleKey: 'preset.autonomous.t', descKey: 'preset.autonomous.d', promptKey: 'presetPrompt.autonomous',
    get iconSvg() { return GA_ICON('gridFour', 'fc-ic'); } },
  { key: 'hive',    titleKey: 'preset.hive.t',    descKey: 'preset.hive.d',    promptKey: 'presetPrompt.hive',
    get iconSvg() { return GA_ICON('hexagon', 'fc-ic'); } },
  { key: 'review',  titleKey: 'preset.review.t',  descKey: 'preset.review.d',  promptKey: 'presetPrompt.review',
    get iconSvg() { return GA_ICON('magnifyingGlass', 'fc-ic'); } },
  { key: 'findwork', titleKey: 'preset.findwork.t', descKey: 'preset.findwork.d', promptKey: 'presetPrompt.findwork',
    get iconSvg() { return GA_ICON('robot', 'fc-ic'); } },
  { key: 'mine',    titleKey: 'preset.mine.t',    descKey: 'preset.mine.d',    promptKey: 'presetPrompt.mine',
    get iconSvg() { return GA_ICON('star', 'fc-ic'); } },
];
const ADD_ICON_SVG = GA_ICON('plus', 'fc-ic');
// 自定义保存后生成的卡片图标（用户图标，表示"用户自定义的任务"）—— 与"添加"卡的 + 区分
const CUSTOM_ICON_SVG = '<svg class="fc-ic" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>';

state.customPresets = [];
state.hiddenBuiltins = new Set();

function loadCustomPresets() {
  try {
    const raw = localStorage.getItem(CP_KEY);
    const arr = raw ? JSON.parse(raw) : [];
    state.customPresets = Array.isArray(arr) ? arr.filter(p => p && p.id && p.title && p.prompt) : [];
  } catch { state.customPresets = []; }
}
function saveCustomPresets() {
  localStorage.setItem(CP_KEY, JSON.stringify(state.customPresets));
}
function loadHiddenBuiltins() {
  try {
    const raw = localStorage.getItem(HB_KEY);
    const arr = raw ? JSON.parse(raw) : [];
    state.hiddenBuiltins = new Set(Array.isArray(arr) ? arr.filter(k => typeof k === 'string') : []);
  } catch { state.hiddenBuiltins = new Set(); }
}
function saveHiddenBuiltins() {
  localStorage.setItem(HB_KEY, JSON.stringify([...state.hiddenBuiltins]));
}

const EDIT_PENCIL_SVG = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 20h9"/><path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4z"/></svg>';
function makeCardEl({ kind, dataAttrs, iconSvg, titleText, descText, removable, editable }) {
  const card = document.createElement('div');
  card.className = 'fcard ' + kind;
  for (const [k, v] of Object.entries(dataAttrs || {})) card.dataset[k] = v;
  card.innerHTML = iconSvg;
  if (editable) {
    const ed = document.createElement('button');
    ed.className = 'fc-edit';
    ed.type = 'button';
    ed.dataset.editId = dataAttrs?.id || '';
    ed.dataset.i18nTitle = 'customPreset.editTitle';
    ed.title = t('customPreset.editTitle');
    ed.innerHTML = EDIT_PENCIL_SVG;
    card.appendChild(ed);
  }
  if (removable) {
    const x = document.createElement('button');
    x.className = 'fc-x';
    x.type = 'button';
    x.dataset.removeKind = kind === 'fcard-builtin' ? 'builtin' : 'custom';
    x.dataset.removeId = dataAttrs?.id || dataAttrs?.preset || '';
    x.dataset.i18nTitle = 'customPreset.removeTitle';
    x.title = t('customPreset.removeTitle');
    x.textContent = '×';
    card.appendChild(x);
  }
  const titleEl = document.createElement('div');
  titleEl.className = 'fc-t';
  titleEl.textContent = titleText;
  titleEl.title = titleText;        // 截断后悬停看完整标题
  card.appendChild(titleEl);
  const descEl = document.createElement('div');
  descEl.className = 'fc-d';
  descEl.textContent = descText;
  descEl.title = descText;          // 截断后悬停看完整描述
  card.appendChild(descEl);
  return card;
}

function renderAllPresets() {
  document.querySelectorAll('.feature-grid').forEach(grid => {
    grid.innerHTML = '';
    for (const bp of BUILTIN_PRESETS) {
      if (state.hiddenBuiltins.has(bp.key)) continue;
      grid.appendChild(makeCardEl({
        kind: 'fcard-builtin',
        dataAttrs: { preset: bp.key },
        iconSvg: bp.iconSvg,
        titleText: t(bp.titleKey),
        descText: t(bp.descKey),
        removable: true,
      }));
    }
    for (const cp of state.customPresets) {
      grid.appendChild(makeCardEl({
        kind: 'fcard-custom',
        dataAttrs: { id: cp.id },
        iconSvg: CUSTOM_ICON_SVG,
        titleText: cp.title,
        descText: cp.prompt,
        removable: true,
        editable: true,
      }));
    }
    const addCard = makeCardEl({
      kind: 'add',
      dataAttrs: { preset: 'add' },
      iconSvg: ADD_ICON_SVG,
      titleText: t('preset.add.t'),
      descText: t('preset.add.d'),
      removable: false,
    });
    grid.appendChild(addCard);
  });
  updateRestoreBtnVisibility();
}

function addCustomPreset(title, prompt) {
  const id = 'cp-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 6);
  state.customPresets.push({ id, title, prompt });
  saveCustomPresets();
  renderAllPresets();
}
function updateCustomPreset(id, title, prompt) {
  const cp = state.customPresets.find(p => p.id === id);
  if (!cp) return;
  cp.title = title;
  cp.prompt = prompt;
  saveCustomPresets();
  renderAllPresets();
}
function removeCustomPreset(id) {
  const idx = state.customPresets.findIndex(p => p.id === id);
  if (idx < 0) return;
  state.customPresets.splice(idx, 1);
  saveCustomPresets();
  renderAllPresets();
}
function hideBuiltinPreset(key) {
  if (!BUILTIN_PRESETS.some(bp => bp.key === key)) return;
  state.hiddenBuiltins.add(key);
  saveHiddenBuiltins();
  renderAllPresets();
}
function restoreBuiltinPresets() {
  state.hiddenBuiltins.clear();
  saveHiddenBuiltins();
  renderAllPresets();
}
function updateRestoreBtnVisibility() {
  const btn = document.getElementById('preset-restore-btn');
  if (!btn) return;
  btn.hidden = state.hiddenBuiltins.size === 0;
}

const cpModal = document.getElementById('custom-preset-modal');
const cpTitleInput = document.getElementById('cp-title');
const cpPromptInput = document.getElementById('cp-prompt');
const cpSaveBtn = document.getElementById('cp-save');
const cpError = document.getElementById('cp-error');
const cpModalTitle = cpModal?.querySelector('.modal-title');
let cpEditId = null;   // null=新建模式; 否则为正在编辑的自定义预设 id
function clearCpFieldHints() {
  cpModal?.querySelectorAll('.field-limit-hint').forEach(h => { h.style.display = 'none'; });
}
function resetCustomPresetForm() {
  cpEditId = null;
  if (cpModalTitle) cpModalTitle.textContent = t('modal.customPreset');
  if (cpTitleInput) cpTitleInput.value = '';
  if (cpPromptInput) cpPromptInput.value = '';
  if (cpError) { cpError.hidden = true; cpError.textContent = ''; }
  clearCpFieldHints();
  setTimeout(() => { if (cpTitleInput) cpTitleInput.focus(); }, 0);
}
function openCustomPresetEditor(cp) {
  closeModals();
  openModal('custom-preset-modal');
  cpEditId = cp.id;
  if (cpModalTitle) cpModalTitle.textContent = t('modal.editCustomPreset');
  if (cpTitleInput) cpTitleInput.value = cp.title;
  if (cpPromptInput) cpPromptInput.value = cp.prompt;
  if (cpError) { cpError.hidden = true; cpError.textContent = ''; }
  clearCpFieldHints();
  setTimeout(() => { if (cpTitleInput) cpTitleInput.focus(); }, 0);
}
if (cpSaveBtn) cpSaveBtn.addEventListener('click', () => {
  const title = (cpTitleInput?.value || '').trim();
  const prompt = (cpPromptInput?.value || '').trim();
  if (!title || !prompt) {
    if (cpError) { cpError.textContent = t('customPreset.empty'); cpError.hidden = false; }
    return;
  }
  if (cpEditId) updateCustomPreset(cpEditId, title, prompt);
  else addCustomPreset(title, prompt);
  cpEditId = null;
  if (cpModal) cpModal.hidden = true;
});

const restoreBtn = document.getElementById('preset-restore-btn');
if (restoreBtn) restoreBtn.addEventListener('click', () => { restoreBuiltinPresets(); });


/* ═══════════════ 图片预览 lightbox ═══════════════ */
const lightbox    = document.getElementById('lightbox');
const lightboxImg = document.getElementById('lightbox-img');
function openLightbox(src) {
  if (!lightbox || !lightboxImg || !src) return;
  lightboxImg.src = src;
  lightbox.hidden = false;
}
function closeLightbox() {
  if (!lightbox || !lightboxImg) return;
  lightbox.hidden = true;
  lightboxImg.src = '';
}
if (lightbox) {
  lightbox.addEventListener('click', (e) => {
    if (e.target.closest('[data-close]')) closeLightbox();
  });
}
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && lightbox && !lightbox.hidden) closeLightbox();
});
if (msgArea) {
  msgArea.addEventListener('click', (e) => {
    const img = e.target.closest('.user-imgs img');
    if (img && img.src) { openLightbox(img.src); return; }
    const fileChip = e.target.closest('.user-files .file-chip');
    if (fileChip) {
      const path = fileChip.getAttribute('data-path');
      const name = fileChip.getAttribute('data-name');
      if (path) openUploadFile(path, name);
    }
  });
}

function uploadRawUrl(path, download) {
  return `${BRIDGE_ORIGIN}/upload/raw?path=${encodeURIComponent(path || '')}${download ? '&download=1' : ''}`;
}
function bridgeIsLocal() {
  return location.hostname === '127.0.0.1' || location.hostname === 'localhost';
}
async function openUploadFile(path, name) {
  // 远程访问：浏览器无法调起 bridge 那台/本机的系统程序，降级为下载到本机
  if (!bridgeIsLocal()) {
    const a = document.createElement('a');
    a.href = uploadRawUrl(path, true);
    a.download = name || '';
    document.body.appendChild(a); a.click(); a.remove();
    return;
  }
  // 本地：bridge 与你同机，调系统默认程序打开 / 在文件夹显示
  const mode = isPreviewableByName(name || path) ? 'open' : 'reveal';
  try {
    const res = await fetch(`${BRIDGE_ORIGIN}/path/open`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ kind: 'upload', path, mode }),
    });
    const j = await res.json();
    if (!j.ok) throw new Error(j.error || 'open failed');
  } catch (e) {
    showChanToast(t('file.openFailed'), e.message || String(e), 'err');
  }
}

const PREVIEWABLE_EXTS = new Set([
  'pdf',
  'png', 'jpg', 'jpeg', 'gif', 'webp', 'bmp', 'svg', 'heic', 'tiff',
  'txt', 'md', 'log', 'json', 'yaml', 'yml', 'xml', 'csv', 'tsv', 'ini', 'toml', 'env', 'rtf',
  'py', 'js', 'ts', 'tsx', 'jsx', 'java', 'c', 'cpp', 'h', 'hpp', 'rs', 'go', 'rb', 'php', 'sh', 'bash', 'zsh', 'fish', 'lua', 'pl', 'r', 'scala', 'kt', 'swift',
  'html', 'htm', 'css', 'scss', 'sass', 'less', 'vue', 'svelte', 'sql',
  'doc', 'docx', 'pages', 'odt',
  'xls', 'xlsx', 'numbers', 'ods',
  'ppt', 'pptx', 'key', 'odp',
  'mp3', 'wav', 'flac', 'aac', 'ogg', 'm4a',
  'mp4', 'mov', 'avi', 'mkv', 'webm', 'wmv',
]);
function isPreviewableByName(name) {
  const m = String(name || '').match(/\.([^./\\]+)$/);
  if (!m) return false;
  return PREVIEWABLE_EXTS.has(m[1].toLowerCase());
}

/* ═══════════════ 后台服务页 Tab（消息通道 / 状态面板） ═══════════════ */
let _svcTab = 'channels';
const svcTabsEl = document.getElementById('svc-tabs');

function isServicesPageActive() {
  return !!document.querySelector('.page[data-page="services"].active');
}
function isSvcTab(tab) {
  return isServicesPageActive() && _svcTab === tab;
}
function setSvcTab(tab) {
  if (!tab || tab === _svcTab) return;
  _svcTab = tab;
  svcTabsEl?.querySelectorAll('.svc-tab').forEach((b) => b.classList.toggle('active', b.dataset.tab === tab));
  document.querySelectorAll('[data-svc-panel]').forEach((p) => p.classList.toggle('active', p.dataset.svcPanel === tab));
  if (tab === 'channels') renderChannelList(gaServiceStore.list());
  else loadStatusPanel();
}
function refreshServicesPanel() {
  if (!isServicesPageActive()) return;
  if (_svcTab === 'channels') renderChannelList(gaServiceStore.list());
  else loadStatusPanel();
}
if (svcTabsEl) {
  svcTabsEl.addEventListener('click', (e) => {
    const btn = e.target.closest('.svc-tab');
    if (!btn) return;
    setSvcTab(btn.dataset.tab);
  });
}

/* ═══════════════ 消息通道（复用 gaServiceStore + WS 同步） ═══════════════ */
const CHAN_ICON = GA_ICON('chatTeardropText', 'lr-ic');
const CHAN_FILE_LABELS = {
  'qqapp.py': 'ch.qq',
  'wechatapp.py': 'ch.wechat',
  'wecomapp.py': 'ch.wecom',
  'dingtalkapp.py': 'ch.dingtalk',
  'tgapp.py': 'ch.telegram',
  'dcapp.py': 'ch.discord',
  'fsapp.py': 'ch.lark',
};
const chanListEl = document.getElementById('chan-list');
const chanEmptyEl = document.getElementById('chan-empty');
const chanLogModal = document.getElementById('chan-log-modal');
const chanLogPre = document.getElementById('chan-log-pre');
const chanLogTitle = document.getElementById('chan-log-title');
const chanConfigModal = document.getElementById('chan-config-modal');
const chanConfigTitle = document.getElementById('chan-config-title');
const chanConfigEditor = document.getElementById('chan-config-editor');
const chanConfigSave = document.getElementById('chan-config-save');
const chanQrModal = document.getElementById('chan-qr-modal');
const chanQrTitle = document.getElementById('chan-qr-title');
const chanQrLoading = document.getElementById('chan-qr-loading');
const chanQrContent = document.getElementById('chan-qr-content');
const chanQrImg = document.getElementById('chan-qr-img');
const chanQrTip = document.getElementById('chan-qr-tip');
const chanQrStatus = document.getElementById('chan-qr-status');
const chanQrError = document.getElementById('chan-qr-error');
const chanQrCancel = document.getElementById('chan-qr-cancel');
const chanQrRefresh = document.getElementById('chan-qr-refresh');
let _chanLogId = null;
let _chanBusy = false;
let _chanToastTimer = null;
let _qrPollTimer = null;
let _qrChannel = null;

function getToastRoot() {
  let root = document.getElementById('toast-root');
  if (!root) {
    root = document.createElement('div');
    root.id = 'toast-root';
    root.className = 'toast-root';
    root.setAttribute('aria-live', 'polite');
    document.body.appendChild(root);
  }
  return root;
}

function showChanToast(title, detail, kind) {
  if (!title) return;
  const root = getToastRoot();
  if (_chanToastTimer) clearTimeout(_chanToastTimer);
  root.innerHTML = '';
  const el = document.createElement('div');
  el.className = `toast toast-${kind === 'err' ? 'err' : kind === 'info' ? 'info' : 'ok'}`;
  const tEl = document.createElement('span');
  tEl.className = 'toast-title';
  tEl.textContent = title;
  el.appendChild(tEl);
  if (detail) {
    const dEl = document.createElement('span');
    dEl.className = 'toast-detail';
    dEl.textContent = detail;
    el.appendChild(dEl);
  }
  root.appendChild(el);
  const show = () => el.classList.add('show');
  requestAnimationFrame(show);
  setTimeout(show, 16);
  _chanToastTimer = setTimeout(() => {
    el.classList.remove('show');
    setTimeout(() => el.remove(), 300);
  }, 3000);
}

/* ── Input length validation ── */
(function initInputLimits() {
  let _toastTimer = null;

  // msgKey 默认用会截断的文案；硬上限输入框传 'err.charLimitReached'（只提醒不提截断）
  function limitToast(maxLen, msgKey = 'err.charLimit') {
    clearTimeout(_toastTimer);
    _toastTimer = setTimeout(() => {
      const msg = t(msgKey).replace('{n}', maxLen);
      showChanToast(msg, '', 'err');
    }, 300);
  }

  // Toast-based: for elements with maxLength attribute (input/textarea) —— 硬上限,不会截断
  function bindToastLimit(el) {
    if (!el || !el.maxLength || el.maxLength < 0) return;
    el.addEventListener('input', () => {
      if (el.value.length >= el.maxLength) limitToast(el.maxLength, 'err.charLimitReached');
    });
  }

  // Toast-based: for contenteditable elements (no native maxLength)
  // Note: we only warn on input, not hard-truncate, because truncating innerHTML
  // would destroy embedded chips, cursor position, and break IME composition.
  // Actual truncation happens at send time in submitInput().
  function bindContentEditableLimit(el, maxLen) {
    if (!el) return;
    let composing = false;
    el.addEventListener('compositionstart', () => { composing = true; });
    el.addEventListener('compositionend', () => {
      composing = false;
      trimExcess();
    });
    // Layer 1: block non-IME input when at capacity
    el.addEventListener('beforeinput', (e) => {
      if (composing) return; // let IME through, trim after compositionend
      if (e.inputType === 'historyUndo' || e.inputType === 'historyRedo') return;
      if (e.inputType && e.inputType.startsWith('delete')) return;
      if (el.textContent.length >= maxLen) {
        e.preventDefault();
        limitToast(maxLen);
      }
    });
    // Layer 2: after IME commits, trim excess from last text node
    function trimExcess() {
      const cur = el.textContent.length;
      if (cur <= maxLen) return;
      const excess = cur - maxLen;
      // Walk text nodes in reverse to trim from the end (skip chip internals)
      const walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT);
      const textNodes = [];
      while (walker.nextNode()) textNodes.push(walker.currentNode);
      if (!textNodes.length) return;
      let toRemove = excess;
      for (let i = textNodes.length - 1; i >= 0 && toRemove > 0; i--) {
        const node = textNodes[i];
        if (node.nodeValue.length <= toRemove) {
          toRemove -= node.nodeValue.length;
          node.nodeValue = '';
        } else {
          node.nodeValue = node.nodeValue.slice(0, node.nodeValue.length - toRemove);
          toRemove = 0;
        }
      }
      limitToast(maxLen);
    }
  }

  // Per-field inline hint: creates a small red span right after the input.
  // 用 JS 管控长度(去掉原生 maxlength),以便"超限尝试"时可靠告警:
  // 超出上限的字符先被键入,再由本逻辑截回上限并告警——每次超限都触发 input,
  // 兼容打字/粘贴/中文 IME。到达上限本身合法、不告警。告警出现后自动消失,不常驻。
  function bindFieldInlineLimit(el) {
    if (!el || !el.maxLength || el.maxLength < 0) return;
    const max = el.maxLength;
    el.removeAttribute('maxlength');   // 转为 JS 管控
    const hint = document.createElement('span');
    hint.className = 'field-limit-hint';
    hint.style.cssText = 'color:var(--err,#dc2626);font-size:.75rem;display:none;margin-top:2px';
    el.insertAdjacentElement('afterend', hint);
    let hideTimer = null;
    function warn() {
      hint.textContent = t('err.charLimitReached').replace('{n}', max);
      hint.style.display = 'block';
      clearTimeout(hideTimer);
      hideTimer = setTimeout(() => { hint.style.display = 'none'; }, 2500);
    }
    function enforce() {
      if (el.value.length <= max) return;
      const atEnd = el.selectionStart >= el.value.length;
      el.value = el.value.slice(0, max);
      if (atEnd) el.setSelectionRange(max, max);
      warn();
    }
    let composing = false;
    el.addEventListener('compositionstart', () => { composing = true; });
    el.addEventListener('compositionend', () => { composing = false; enforce(); });
    el.addEventListener('input', () => { if (!composing) enforce(); });
  }

  window.bindFieldInlineLimit = bindFieldInlineLimit;
  window.bindToastLimit = bindToastLimit;

  // Number field: clamp to max on blur, no red text; block non-integer chars
  function bindNumberClamp(el) {
    if (!el || !el.max) return;
    const max = Number(el.max);
    if (!max) return;
    // Block all non-digit keys (allow navigation/editing keys)
    el.addEventListener('keydown', (e) => {
      if (e.ctrlKey || e.metaKey || e.altKey) return; // allow shortcuts
      if (['Backspace','Delete','Tab','ArrowLeft','ArrowRight','Home','End'].includes(e.key)) return;
      if (e.key.length === 1 && !/[0-9]/.test(e.key)) e.preventDefault();
    });
    el.addEventListener('input', () => {
      // Strip non-digit chars (IME can bypass keydown)
      const cleaned = el.value.replace(/[^0-9]/g, '');
      if (cleaned !== el.value) el.value = cleaned;
      const v = Number(el.value);
      if (el.value !== '' && v > max) el.value = max;
    });
  }

  // Wait for DOM ready
  function setup() {
    // Contenteditable inputs (chat + collab)
    const chatInput = document.querySelector('.input[contenteditable][data-i18n-ph="composer.placeholder"]');
    const collabInput = document.getElementById('cdb-input');
    bindContentEditableLimit(chatInput, 20000);
    bindContentEditableLimit(collabInput, 20000);

    // Toast targets (standard inputs/textareas with maxLength)
    const searchInput = document.querySelector('.search input');
    const mykeyEditor = document.getElementById('chan-config-editor');
    [searchInput, mykeyEditor].forEach(bindToastLimit);

    // Model form: per-field inline hints
    const form = document.getElementById('add-model-form');
    if (form) {
      ['model', 'apikey', 'apibase', 'name'].forEach(name => {
        bindFieldInlineLimit(form.querySelector(`[name="${name}"]`));
      });
      ['max_retries', 'connect_timeout', 'read_timeout'].forEach(name => {
        bindNumberClamp(form.querySelector(`[name="${name}"]`));
      });
    }

    // Preset form: 每字段独立内联提示（标题/Prompt 各自独立,互不串扰）
    bindFieldInlineLimit(document.getElementById('cp-title'));
    bindFieldInlineLimit(document.getElementById('cp-prompt'));

    // First-run desktop-shortcut prompt (Windows portable bundle only). Driven from the web UI
    // so the dialog always renders on top — a native dialog from the Rust startup thread had no
    // parent window and got buried behind the main window on first launch.
    maybeAskDesktopShortcut();
  }

  async function maybeAskDesktopShortcut() {
    try {
      const should = await window.ga.tauriInvoke('shortcut_should_ask');
      if (!should) return;
      const create = await showConfirmDialog({ title: t('common.confirm'), message: t('shortcut.askConfirm'), okText: t('common.confirm') });
      await window.ga.tauriInvoke('shortcut_decide', { create });
    } catch (_) { /* not in tauri / not a bundle — ignore */ }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', setup);
  } else {
    setup();
  }
})();

function channelDisplayName(ch) {
  const file = (ch.name || ch.id || '').split('/').pop();
  const key = CHAN_FILE_LABELS[file];
  return key ? t(key) : (ch.name || ch.id || '');
}
function channelStatusClass(status) {
  if (status === 'running') return 'on';
  if (status === 'error') return 'err';
  return 'off';
}
function channelStatusLabel(status) {
  const map = {
    running: 'st.running', offline: 'st.offline', error: 'st.error',
    starting: 'st.starting', stopping: 'st.stopping',
  };
  return t(map[status] || 'st.offline');
}
function channelErrorMessage(code) {
  const map = { not_configured: 'err.channelNotConfigured' };
  return t(map[code] || code || 'err.channelStart');
}
function channelToastDetail(e) {
  const svc = e.data && e.data.service;
  if (svc && svc.lastError) return svc.lastError;
  const code = e.data && e.data.error;
  return channelErrorMessage(code || e.message);
}
function renderChannelList(channels) {
  if (!chanListEl) return;
  const rows = (channels || []).filter((ch) => (ch.id || '').startsWith('frontends/'));
  chanListEl.innerHTML = '';
  if (chanEmptyEl) chanEmptyEl.hidden = rows.length > 0;
  for (const ch of rows) {
    const row = document.createElement('div');
    row.className = 'list-row';
    row.dataset.channelId = ch.id;
    const stClass = channelStatusClass(ch.status || 'offline');
    const running = !!ch.running;
    row.innerHTML = `
      ${CHAN_ICON}
      <div class="chan-meta">
        <b class="chan-name"></b>
        <span class="kv chan-path"></span>
      </div>
      <span class="lr-st ${stClass} chan-status"></span>
      <span class="grow"></span>
      <button type="button" class="link-btn link sm" data-act="configure"></button>
      <button type="button" class="link-btn link sm" data-act="logs"></button>
      <button type="button" class="sw-mini${running ? ' on' : ''}" data-act="toggle" aria-pressed="${running}"><i></i></button>`;
    row.querySelector('.chan-name').textContent = channelDisplayName(ch);
    row.querySelector('.chan-path').textContent = ch.name || ch.id;
    row.querySelector('.chan-status').textContent = channelStatusLabel(ch.status || 'offline');
    row.querySelector('[data-act="configure"]').textContent = t('act.configure');
    row.querySelector('[data-act="logs"]').textContent = t('act.logs');
    chanListEl.appendChild(row);
  }
}
async function toggleChannel(id, running, toggleEl) {
  if (_chanBusy) return;
  _chanBusy = true;
  if (toggleEl) toggleEl.disabled = true;
  const label = channelDisplayName(gaServiceStore.get(id) || { id });
  try {
    if (running) {
      await window.ga.stopService(id);
      showChanToast(t('sys.channelStopped') + ' · ' + label, '', 'ok');
    } else {
      const res = await window.ga.startService(id);
      if (res && res.service && res.service.status === 'error') {
        throw Object.assign(new Error(res.service.lastError || 'start_failed'), { data: res });
      }
      showChanToast(t('sys.channelStarted') + ' · ' + label, '', 'ok');
    }
  } catch (e) {
    showChanToast(
      (running ? t('err.channelStop') : t('err.channelStart')) + ' · ' + label,
      channelToastDetail(e),
      'err'
    );
  } finally {
    _chanBusy = false;
    if (toggleEl) toggleEl.disabled = false;
  }
}
async function openChannelLogs(id) {
  if (!chanLogModal || !chanLogPre) return;
  _chanLogId = id;
  const ch = gaServiceStore.get(id) || { id };
  const titleName = id === '__bridge__' ? (ch.name || 'bridge') : statusDisplayName(ch);
  if (chanLogTitle) chanLogTitle.textContent = t('modal.channelLogs') + ' · ' + titleName;
  chanLogPre.textContent = t('ch.loading');
  openModal('chan-log-modal');
  try {
    const res = await window.ga.getServiceLogs(id, 200);
    const lines = res.lines || [];
    chanLogPre.textContent = lines.length ? lines.join('\n') : t('ch.logEmpty');
  } catch (e) {
    chanLogPre.textContent = t('err.channelLoad') + ': ' + (e.message || e);
  }
}
async function openChannelMykey(channelId) {
  if (!chanConfigModal || !chanConfigEditor) return;
  const ch = gaServiceStore.get(channelId) || { id: channelId };
  if (chanConfigTitle) {
    chanConfigTitle.textContent = t('modal.mykeyConfig') + (channelId ? ' · ' + channelDisplayName(ch) : '');
  }
  chanConfigEditor.value = t('ch.loading');
  chanConfigEditor.disabled = true;
  if (chanConfigSave) chanConfigSave.disabled = true;
  openModal('chan-config-modal');
  try {
    const res = await window.ga.getMykeyContent();
    chanConfigEditor.value = res.content || '';
  } catch (e) {
    chanConfigEditor.value = t('err.channelLoad') + ': ' + (e.message || e);
  } finally {
    chanConfigEditor.disabled = false;
    if (chanConfigSave) chanConfigSave.disabled = false;
    chanConfigEditor.focus();
  }
}
async function saveChannelMykey() {
  if (!chanConfigEditor || !chanConfigSave) return;
  chanConfigSave.disabled = true;
  try {
    await window.ga.saveMykeyContent(chanConfigEditor.value);
    showChanToast(t('sys.configSaved'), '', 'ok');
    chanConfigModal.hidden = true;
  } catch (e) {
    showChanToast(t('err.channelLoad'), e.message || String(e), 'err');
  } finally {
    chanConfigSave.disabled = false;
  }
}
if (chanConfigSave) {
  chanConfigSave.addEventListener('click', saveChannelMykey);
}

/* ═══════════════ 二维码登录 ═══════════════ */
const QR_CHANNEL_MAP = {
  'frontends/wechatapp.py': { key: 'wechat', label: '微信' },
};

function stopQrPolling() {
  if (_qrPollTimer) {
    clearInterval(_qrPollTimer);
    _qrPollTimer = null;
  }
}

function resetQrUi() {
  stopQrPolling();
  chanQrLoading.hidden = false;
  chanQrContent.hidden = true;
  chanQrError.hidden = true;
  chanQrRefresh.hidden = true;
  chanQrImg.src = '';
  chanQrStatus.textContent = '';
}

async function fetchChannelQr(channelKey) {
  try {
    const res = await bridgeFetch(`/services/channel/qr?channel=${encodeURIComponent(channelKey)}`);
    if (res.ok && res.qr_url && res.qr_id) {
      return { qr_url: res.qr_url, qr_id: res.qr_id };
    }
    throw new Error(res.error || 'Failed to get QR code');
  } catch (e) {
    return { error: e.message };
  }
}

function generateQrCode(url) {
  try {
    const qr = qrcode(0, 'H');
    qr.addData(url);
    qr.make();
    return qr.createDataURL(4, 0);
  } catch (e) {
    console.error('[QR] QR code generation failed:', e);
    throw e;
  }
}

async function pollQrStatus(channelKey, qrId) {
  try {
    const res = await bridgeFetch(`/services/channel/qr-status?channel=${encodeURIComponent(channelKey)}&qr_id=${encodeURIComponent(qrId)}`);
    if (res.ok) {
      return res;
    }
    throw new Error(res.error || 'Poll failed');
  } catch (e) {
    return { error: e.message };
  }
}

async function openChannelQrLogin(channelId) {
  console.log('[QR] openChannelQrLogin called with channelId:', channelId);
  const info = QR_CHANNEL_MAP[channelId];
  console.log('[QR] QR_CHANNEL_MAP:', JSON.stringify(QR_CHANNEL_MAP));
  console.log('[QR] info:', info);
  if (!info) {
    console.log('[QR] No info, calling openChannelMykey');
    openChannelMykey(channelId);
    return;
  }

  _qrChannel = channelId;
  chanQrTitle.textContent = t('modal.qrLogin').replace('{name}', info.label);
  chanQrTip.textContent = t('qr.scanTip').replace('{name}', info.label);
  resetQrUi();
  openModal('chan-qr-modal');
  console.log('[QR] Modal opened, fetching QR code...');

  const qrResult = await fetchChannelQr(info.key);
  console.log('[QR] fetchChannelQr result:', qrResult);
  if (qrResult.error) {
    chanQrLoading.hidden = true;
    chanQrError.hidden = false;
    chanQrError.textContent = t('qr.failed');
    chanQrRefresh.hidden = false;
    return;
  }

  chanQrLoading.hidden = true;
  chanQrContent.hidden = false;
  console.log('[QR] Generating QR code for:', qrResult.qr_url);
  try {
    const qrDataUrl = generateQrCode(qrResult.qr_url);
    console.log('[QR] QR code generated successfully, length:', qrDataUrl.length);
    chanQrImg.src = qrDataUrl;
  } catch (e) {
    console.error('[QR] QR code generation error:', e);
    chanQrError.hidden = false;
    chanQrError.textContent = 'QR code generation failed: ' + e.message;
    chanQrRefresh.hidden = false;
    return;
  }
  chanQrStatus.textContent = t('qr.statusScanning');

  _qrPollTimer = setInterval(async () => {
    const status = await pollQrStatus(info.key, qrResult.qr_id);
    if (status.error) {
      return;
    }
    if (status.status === 'confirmed') {
      stopQrPolling();
      chanQrStatus.textContent = t('qr.statusConfirmed');
      await window.ga.startService(channelId);
      showChanToast(t('sys.channelStarted') + ' · ' + info.label, '', 'ok');
      setTimeout(() => {
        chanQrModal.hidden = true;
      }, 1500);
    } else if (status.status === 'expired') {
      stopQrPolling();
      chanQrStatus.textContent = t('qr.statusExpired');
      chanQrRefresh.hidden = false;
    } else if (status.status === 'scanned') {
      chanQrStatus.textContent = t('qr.statusScanned');
    }
  }, 2000);
}

async function refreshChannelQr() {
  if (!_qrChannel) return;
  const info = QR_CHANNEL_MAP[_qrChannel];
  if (!info) return;
  resetQrUi();

  const qrResult = await fetchChannelQr(info.key);
  if (qrResult.error) {
    chanQrLoading.hidden = true;
    chanQrError.hidden = false;
    chanQrError.textContent = t('qr.failed');
    chanQrRefresh.hidden = false;
    return;
  }

  chanQrLoading.hidden = true;
  chanQrContent.hidden = false;
  chanQrImg.src = generateQrCode(qrResult.qr_url);
  chanQrStatus.textContent = t('qr.statusScanning');

  _qrPollTimer = setInterval(async () => {
    const status = await pollQrStatus(info.key, qrResult.qr_id);
    if (status.error) {
      return;
    }
    if (status.status === 'confirmed') {
      stopQrPolling();
      chanQrStatus.textContent = t('qr.statusConfirmed');
      await window.ga.startService(_qrChannel);
      showChanToast(t('sys.channelStarted') + ' · ' + info.label, '', 'ok');
      setTimeout(() => {
        chanQrModal.hidden = true;
      }, 1500);
    } else if (status.status === 'expired') {
      stopQrPolling();
      chanQrStatus.textContent = t('qr.statusExpired');
      chanQrRefresh.hidden = false;
    } else if (status.status === 'scanned') {
      chanQrStatus.textContent = t('qr.statusScanned');
    }
  }, 2000);
}

if (chanQrCancel) {
  chanQrCancel.addEventListener('click', () => {
    stopQrPolling();
    chanQrModal.hidden = true;
  });
}

if (chanQrRefresh) {
  chanQrRefresh.addEventListener('click', refreshChannelQr);
}

if (chanQrModal) {
  chanQrModal.addEventListener('click', (e) => {
    if (e.target.classList.contains('modal-backdrop')) {
      stopQrPolling();
      chanQrModal.hidden = true;
    }
  });
}

/* ═══════════════ 状态面板（复用 ServiceManager + 启停/日志） ═══════════════ */
const statusListEl = document.getElementById('status-list');
const BRIDGE_SERVICE_ID = '__bridge__';
const EXTRA_SERVICE_IDS = new Set(['frontends/conductor.py', 'reflect/scheduler.py']);

function bridgeOfflinePanelServices() {
  return [
    {
      id: BRIDGE_SERVICE_ID,
      name: `bridge (:${BRIDGE_PORT})`,
      status: 'offline',
      running: false,
      pid: null,
      memMb: null,
      cpuPct: null,
      managed: false,
    },
    {
      id: 'frontends/conductor.py',
      name: 'frontends/conductor.py',
      status: 'offline',
      running: false,
      pid: null,
      memMb: null,
      cpuPct: null,
      managed: false,
      bridgeOffline: true,
    },
    {
      id: 'reflect/scheduler.py',
      name: 'reflect/scheduler.py',
      status: 'offline',
      running: false,
      pid: null,
      memMb: null,
      cpuPct: null,
      managed: false,
      bridgeOffline: true,
    },
  ];
}

function statusDisplayName(s) {
  if (!s) return '';
  if (s.id === BRIDGE_SERVICE_ID) return s.name || 'bridge';
  if (s.id === 'reflect/scheduler.py') return t('proc.scheduler');
  if (s.id === 'frontends/conductor.py') return t('proc.conductor');
  return channelDisplayName(s);
}
function fmtPid(pid) { return pid ? `PID ${pid}` : '—'; }
function fmtRes(s) {
  const cpu = s.cpuPct != null ? `${s.cpuPct}%` : '—';
  const mem = s.memMb != null ? `${s.memMb}MB` : '—';
  return `${cpu} / ${mem}`;
}

function renderStatusPanel(services) {
  if (!statusListEl) return;
  statusListEl.innerHTML = '';
  for (const s of services || []) {
    const row = document.createElement('div');
    row.className = 'list-row';
    row.dataset.serviceId = s.id;
    const stClass = channelStatusClass(s.status || 'offline');
    const running = !!s.running;
    const managed = s.managed !== false;
    const isBridge = s.id === BRIDGE_SERVICE_ID;
    const isExtra = EXTRA_SERVICE_IDS.has(s.id);
    let acts = '';
    if (isBridge) {
      if (running) {
        acts += `<button type="button" class="link-btn link sm" data-act="logs"></button>`;
        acts += `<button type="button" class="link-btn link sm" data-act="bridge-exit"></button>`;
      } else {
        acts += `<button type="button" class="link-btn link sm" data-act="bridge-start"></button>`;
      }
    } else if (!s.bridgeOffline) {
      acts += `<button type="button" class="link-btn link sm" data-act="logs"></button>`;
      if (managed) {
        if (running) acts += `<button type="button" class="link-btn link sm" data-act="restart"></button>`;
        if (isExtra) {
          acts += `<button type="button" class="link-btn link sm${running ? ' on' : ''}" data-act="toggle" aria-pressed="${running}"></button>`;
        } else {
          acts += `<button type="button" class="sw-mini${running ? ' on' : ''}" data-act="toggle" aria-pressed="${running}"><i></i></button>`;
        }
      }
    }
    row.innerHTML = `
      <b class="st-name"></b>
      <span class="lr-st ${stClass} st-status"></span>
      <span class="kv st-pid"></span>
      <span class="kv st-res"></span>
      <span class="grow"></span>
      ${acts}`;
    row.querySelector('.st-name').textContent = statusDisplayName(s);
    row.querySelector('.st-status').textContent = channelStatusLabel(s.status || 'offline');
    row.querySelector('.st-pid').textContent = fmtPid(s.pid);
    row.querySelector('.st-res').textContent = fmtRes(s);
    const logBtn = row.querySelector('[data-act="logs"]');
    if (logBtn) logBtn.textContent = t('act.logs');
    const rstBtn = row.querySelector('[data-act="restart"]');
    if (rstBtn) rstBtn.textContent = t('act.restart');
    const startBridgeBtn = row.querySelector('[data-act="bridge-start"]');
    if (startBridgeBtn) startBridgeBtn.textContent = t('act.start');
    const exitBridgeBtn = row.querySelector('[data-act="bridge-exit"]');
    if (exitBridgeBtn) exitBridgeBtn.textContent = t('act.exit');
    const textToggleBtn = row.querySelector('.link-btn[data-act="toggle"]');
    if (textToggleBtn) textToggleBtn.textContent = running ? t('act.exit') : t('act.start');
    statusListEl.appendChild(row);
  }
}

async function loadStatusPanel() {
  if (!statusListEl) return;
  if (bridgeUiOffline) {
    renderStatusPanel(bridgeOfflinePanelServices());
    return;
  }
  try {
    const res = await window.ga.getServicePanel();
    renderStatusPanel(res.services || []);
  } catch (_) {
    window.ga.setBridgeUiOffline(true);
    gaServiceStore.applySnapshot(bridgeOfflinePanelServices());
    renderStatusPanel(bridgeOfflinePanelServices());
  }
}

async function restartService(id) {
  const label = statusDisplayName(gaServiceStore.get(id) || { id });
  await window.ga.stopService(id);
  const res = await window.ga.startService(id);
  if (res && res.service && res.service.status === 'error') {
    throw Object.assign(new Error(res.service.lastError || 'start_failed'), { data: res });
  }
  showChanToast(t('act.restart') + ' · ' + label, '', 'ok');
}

if (statusListEl) {
  statusListEl.addEventListener('click', async (e) => {
    const row = e.target.closest('.list-row');
    if (!row) return;
    const id = row.dataset.serviceId;
    const actEl = e.target.closest('[data-act]');
    if (!actEl || !id) return;
    const act = actEl.dataset.act;
    if (act === 'logs') {
      openChannelLogs(id);
      return;
    }
    if (act === 'bridge-start') {
      if (_chanBusy) return;
      _chanBusy = true;
      actEl.disabled = true;
      try {
        await window.ga.spawnBridge();
        await loadStatusPanel();
        showChanToast(t('sys.channelStarted') + ' · bridge', '', 'ok');
      } catch (err) {
        showChanToast(t('err.channelStart') + ' · bridge', err.message || String(err), 'err');
      } finally {
        _chanBusy = false;
        actEl.disabled = false;
      }
      return;
    }
    if (act === 'bridge-exit') {
      if (_chanBusy) return;
      _chanBusy = true;
      actEl.disabled = true;
      try {
        window.ga.setBridgeUiOffline(true);
        gaServiceStore.applySnapshot(bridgeOfflinePanelServices());
        renderStatusPanel(bridgeOfflinePanelServices());
        await window.ga.exitBridge();
        showChanToast(t('sys.channelStopped') + ' · bridge', '', 'ok');
      } catch (err) {
        showChanToast(t('err.channelStop') + ' · bridge', err.message || String(err), 'err');
      } finally {
        _chanBusy = false;
        actEl.disabled = false;
      }
      return;
    }
    if (act === 'restart') {
      if (_chanBusy) return;
      _chanBusy = true;
      try {
        await restartService(id);
        await loadStatusPanel();
      } catch (err) {
        showChanToast(t('act.restart') + ' · ' + statusDisplayName({ id }), err.message || String(err), 'err');
      } finally {
        _chanBusy = false;
      }
      return;
    }
    if (act === 'toggle') {
      if (actEl.disabled || _chanBusy) return;
      const running = actEl.classList.contains('on');
      await toggleChannel(id, running, actEl);
      if (isSvcTab('status')) loadStatusPanel();
    }
  });
}

gaServiceStore.onServices((list) => {
  if (isSvcTab('channels')) renderChannelList(list);
  if (isSvcTab('status')) {
    if (bridgeUiOffline) renderStatusPanel(bridgeOfflinePanelServices());
    else loadStatusPanel();
  }
});
if (chanListEl) {
  chanListEl.addEventListener('click', async (e) => {
    const row = e.target.closest('.list-row');
    if (!row) return;
    const id = row.dataset.channelId;
    const actEl = e.target.closest('[data-act]');
    if (!actEl || !id) return;
    const act = actEl.dataset.act;
    if (act === 'logs') {
      openChannelLogs(id);
      return;
    }
    if (act === 'configure') {
      openChannelQrLogin(id);
      return;
    }
    if (act === 'toggle') {
      if (actEl.disabled || _chanBusy) return;
      const running = actEl.classList.contains('on');
      await toggleChannel(id, running, actEl);
    }
  });
}

/* ═══════════════ 启动 ═══════════════ */
(async () => {
loadConvFolders();
await loadSessions();
applyAppearance(appearance, plainUi, { persist: false });
applyTheme(theme, { persist: false });
initChatFontStepper();
applyChatFontSize(chatFontSize, { persist: false });
syncHljsTheme();
applyI18n();
updateModelChip();
renderSessionList();
loadCustomPresets();
loadHiddenBuiltins();
renderAllPresets();
if (state.activeId) setActiveSession(state.activeId);
else refreshEmptyState(null);
// bridge-ready 可能在上面的 await 期间就已到达（WS 一连上 bridge 即推送），
// 此时 state.bridgeReady 已为 true，直接按真实状态渲染，避免把「就绪」覆盖回「连接中」。
if (state.bridgeReady) refreshStatusLabel();
else chatStatus.setConnecting();
window.ga.startBridge && window.ga.startBridge();
})();

/* 聊天 / Conductor 共用 composer 绑定（结构：.composer > .composer-slot > .composer-inset） */
function bindComposerInRoot(root, opts) {
  if (!root || root.dataset.composerBound) return null;
  root.dataset.composerBound = '1';
  const ctx = opts.ctx || root.dataset.composerCtx || 'chat';
  const input = root.querySelector('.composer-inset .input');
  const fileInput = root.querySelector('input[type="file"]');
  const plusBtn = root.querySelector('.composer-plus');
  const menu = root.querySelector('.composer-menu');
  const sendBtn = root.querySelector('.send');

  function closeMenu() {
    if (!menu) return;
    menu.hidden = true;
    plusBtn?.setAttribute('aria-expanded', 'false');
  }

  function openMenu() {
    if (!menu || !plusBtn) return;
    closeAllModelMenus?.();
    if (ctx === 'chat') window.collabComposer?.closeMenu?.();
    else window.chatComposer?.closeMenu?.();
    menu.hidden = false;
    plusBtn.setAttribute('aria-expanded', 'true');
  }

  function toggleMenu() {
    if (!menu) return;
    if (menu.hidden) openMenu();
    else closeMenu();
  }

  function doSend() { opts.onSend?.(); }

  plusBtn?.addEventListener('click', (e) => {
    e.preventDefault();
    e.stopPropagation();
    toggleMenu();
  });

  menu?.addEventListener('click', (e) => {
    const item = e.target.closest('[data-composer-action]');
    if (!item) return;
    e.stopPropagation();
    closeMenu();
    const act = item.dataset.composerAction;
    if (act === 'upload') {
      window.gaSetActiveFileComposer?.(ctx);
      fileInput?.click();
      return;
    }
    if (act === 'preset') openModal('preset-modal');
  });

  input?.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey && !e.isComposing && e.keyCode !== 229) {
      e.preventDefault();
      doSend();
    }
  });

  sendBtn?.addEventListener('click', (e) => {
    e.preventDefault();
    doSend();
  });

  opts.afterBind?.(root, { input, closeMenu, doSend });
  return { ctx, input, closeMenu, focus: () => input?.focus() };
}

(function () {
  'use strict';
  const root = document.getElementById('chat-composer');
  const bound = bindComposerInRoot(root, {
    ctx: 'chat',
    onSend() {
      const sess = activeSess();
      if (sess && rt(sess).busy) { cancelPrompt(); return; }
      submitInput();
    },
  });
  if (bound) window.chatComposer = { closeMenu: bound.closeMenu, focus: bound.focus };
})();

(function () {
  'use strict';
  const root = document.getElementById('cdb-composer');
  if (!root) return;

  let onSend = null;
  const input = root.querySelector('.composer-inset .input');
  const sendBtn = root.querySelector('.send');

  function text() { return window.gaComposerText?.('collab') ?? ''; }
  function clearIfMatch(raw) {
    if (input && text().trim() === String(raw || '').trim()) input.innerHTML = '';
  }
  function setEnabled(on) {
    if (input) input.contentEditable = on ? 'true' : 'false';
    if (sendBtn) sendBtn.disabled = !on;
  }

  const bound = bindComposerInRoot(root, {
    ctx: 'collab',
    onSend() { if (onSend) onSend(text()); },
    afterBind() {
      document.querySelectorAll('#collab-quick [data-prompt-key]').forEach((btn) => {
        btn.addEventListener('click', () => {
          if (!onSend) return;
          const key = btn.dataset.promptKey;
          onSend((window.gaT && window.gaT(key)) || key);
        });
      });
    },
  });
  if (!bound) return;

  function init(handler) {
    onSend = handler;
  }

  window.collabComposer = {
    init, text, clearIfMatch, setEnabled,
    focus: bound.focus,
    closeMenu: bound.closeMenu,
  };
})();

/* Conductor 页 — 直连 Conductor WS，不走 bridge session */
(function () {
  'use strict';
  const wsUrl = () => `${CONDUCTOR_WS_ORIGIN}/ws`;
  const FAIL_MAX = 5, RECON_BASE = 1200, RECON_MAX = 30000;
  const $ = id => document.getElementById(id);
  const t = k => (window.gaT && window.gaT(k)) || k;
  const esc = s => (window.gaEscapeHtml ? window.gaEscapeHtml(s) : String(s ?? '').replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])));
  const stripAttach = text => String(text || '')
    .replace(/\[(Image|File)\s+#\d+\]\s*/g, '')
    .replace(/[^\s]*desktop_uploads[^\s]*\s*/g, '')  // 兜底:去掉内联的本地上传路径,避免历史/回显消息把全路径甩出来
    .trim();
  const GA_STATUS_BREATHE_SM = '<span class="ga-status-breathe ga-status-breathe--sm" aria-hidden="true"><span class="ga-status-breathe__ring"></span><span class="ga-status-breathe__core"></span></span>';
  function collabStatusMark(status) {
    switch (status) {
      case 'running': return GA_STATUS_BREATHE_SM;
      case 'reported': return '<span class="collab-st-ic collab-st-ic--ok" aria-hidden="true">✓</span>';
      case 'paused': return '<span class="collab-st-ic collab-st-ic--pause" aria-hidden="true">⏸</span>';
      case 'failed': return '<span class="collab-st-ic collab-st-ic--warn" aria-hidden="true">!</span>';
      case 'terminated': return '<span class="collab-st-ic collab-st-ic--off" aria-hidden="true">×</span>';
      default: return '<span class="collab-dot" aria-hidden="true"></span>';
    }
  }
  const ST_KEYS = { running: 'collab.stRunning', reported: 'collab.stReported', paused: 'collab.stPaused', failed: 'collab.stFailed', terminated: 'collab.stTerminated' };

  const S = {
    everConnected: false, reconnecting: false, serviceAvailable: false,
    messages: [], workers: [], runningCount: 0,
    conductorTyping: false, failCount: 0,
    historyReady: false, reconnectAt: 0, progressOpen: false,
  };
  let ws, connectTimer, reconnectTick, titleSeq = 0, wsGen = 0, localSeq = 0;
  const titleSeen = new Map();
  let prevRail = { running: 0, done: 0, issue: 0, count: 0, sig: '' };
  const prevUpdated = new Map();
  const collabStatus = window.gaPageStatusBar?.($('collab-run-toggle'));

  let draftEl = null;

  const scrollMsgs = () => {
    const root = $('collab-msgs');
    const sc = root?.querySelector('.collab-scroll');
    if (sc) sc.scrollTop = sc.scrollHeight;
  };
  const showDraft = () => S.conductorTyping && S.serviceAvailable && S.historyReady && S.messages.length > 0;

  function workerSig(list) {
    return (list || []).map(w => `${w.id}:${w.updatedAt}:${w.status}`).join('|');
  }

  function pulseEl(el) {
    if (!el) return;
    el.classList.remove('pulse');
    void el.offsetWidth;
    el.classList.add('pulse');
  }

  function syncProgressDrawer() {
    const page = document.querySelector('.page[data-page="collab"]');
    if (page) page.classList.toggle('collab-prog-open', S.progressOpen);
  }

  function syncRail(opts = {}) {
    const rail = $('collab-rail');
    const hasChat = S.historyReady && S.messages.length > 0;
    if (rail) rail.hidden = !hasChat;

    const running = S.workers.filter(w => w.status === 'running').length;
    const done = S.workers.filter(w => w.status === 'reported').length;
    const issue = S.workers.filter(w => w.status === 'failed').length;
    const runBadge = $('collab-rail-run');
    const doneBadge = $('collab-rail-done');
    const issueBadge = $('collab-rail-issue');
    const runN = $('collab-rail-run-n');
    const doneN = $('collab-rail-done-n');
    const issueN = $('collab-rail-issue-n');

    if (runBadge) runBadge.hidden = running <= 0;
    if (doneBadge) doneBadge.hidden = done <= 0;
    if (issueBadge) issueBadge.hidden = issue <= 0;
    if (runN) runN.textContent = String(running);
    if (doneN) doneN.textContent = String(done);
    if (issueN) issueN.textContent = String(issue);

    const sig = workerSig(S.workers);
    if (opts.pulse) {
      if (running > prevRail.running || S.workers.length > prevRail.count) pulseEl(runBadge);
      if (done > prevRail.done) pulseEl(doneBadge);
      if (issue > prevRail.issue) pulseEl(issueBadge);
    } else if (sig !== prevRail.sig) {
      if (running !== prevRail.running) pulseEl(runBadge);
      if (done !== prevRail.done) pulseEl(doneBadge);
      if (issue !== prevRail.issue) pulseEl(issueBadge);
    }
    prevRail = { running, done, issue, count: S.workers.length, sig };
    syncProgressDrawer();
  }

  function toggleProgress(open) {
    S.progressOpen = typeof open === 'boolean' ? open : !S.progressOpen;
    syncRail();
  }

  function clearDraft() {
    if (draftEl) { draftEl.remove(); draftEl = null; }
  }

  function syncDraft() {
    const list = $('collab-msg-list');
    if (!list || list.hidden || !showDraft()) return clearDraft();
    if (!draftEl) draftEl = document.createElement('div');
    draftEl.className = 'msg system collab-msg-enter';
    draftEl.setAttribute('aria-label', t('collab.typing'));
    draftEl.innerHTML = '<div class="bubble sys"><span class="collab-wait-dots" aria-hidden="true"><i></i><i></i><i></i></span></div>';
    list.appendChild(draftEl);
    requestAnimationFrame(scrollMsgs);
  }

  function relTime(ts) {
    if (!ts) return '';
    const ms = typeof ts === 'number' ? (ts > 1e12 ? ts : ts * 1000) : Date.parse(ts);
    if (!ms || Number.isNaN(ms)) return '';
    const sec = Math.max(0, Math.floor((Date.now() - ms) / 1000));
    if (sec < 10) return t('collab.timeJust');
    if (sec < 60) return t('collab.timeSec').replace('{n}', sec);
    const min = Math.floor(sec / 60);
    if (min < 60) return t('collab.timeMin').replace('{n}', min);
    const hr = Math.floor(min / 60);
    return hr < 24 ? t('collab.timeHr').replace('{n}', hr) : t('collab.timeDay').replace('{n}', Math.floor(hr / 24));
  }

  function mapStatus(status, reply) {
    const r = (reply || '').trim();
    if (status === 'running') return 'running';
    if (status === 'failed') return 'failed';
    if (status === 'aborted') return 'terminated';
    if (status === 'stopped') return r ? 'reported' : 'paused';
    return 'paused';
  }

  function normalizeWorker(raw) {
    if (!titleSeen.has(raw.id)) titleSeen.set(raw.id, ++titleSeq);
    const ui = mapStatus(raw.status, raw.reply);
    let title = String(raw.prompt ?? '').replace(/^[\s请帮我麻烦]+/u, '').trim();
    if (!title) title = t('collab.taskFallback').replace('{n}', titleSeen.get(raw.id));
    else {
      title = (title.split(/[\n。！？.!?]/)[0] || '').trim();
      if (title.length > 18) title = title.slice(0, 18) + '…';
    }
    const reply = String(raw.reply || '').replace(/\s+/g, ' ').trim();
    let summary = reply ? (reply.length > 80 ? reply.slice(0, 80) + '…' : reply) : t(ui === 'running' ? 'collab.summaryRunning' : 'collab.summaryWait');
    return { id: raw.id, title, status: ui, summary, fullReply: raw.reply || '', updatedAt: raw.updated_at };
  }

  function syncCollabStatus() {
    if (!collabStatus) return;
    if (S.conductorTyping && S.serviceAvailable) collabStatus.setBusy(t('status.running'));
    else if (S.serviceAvailable) collabStatus.setReady();
    else if (S.reconnecting || (!S.everConnected && S.failCount < FAIL_MAX)) collabStatus.setConnecting();
    else collabStatus.set(t('collab.offlineShort'), 'offline');  // 顶栏直接显示"无法连接 Conductor(8900)"取代"未连接"
  }

  function setConnUi() {
    const retry = $('collab-retry');
    const avail = S.serviceAvailable;
    const trying = !avail && !S.everConnected && S.failCount < FAIL_MAX;
    // 只有"真离线且不在自动重连"才显示刷新图标(右边的手动重试入口)
    if (retry) retry.hidden = avail || S.reconnecting || trying;
    window.collabComposer?.setEnabled?.(avail);
    syncCollabStatus();
    syncDraft();
    syncRail();
  }

  let cardMenu = null;
  function hideCardMenu() { if (cardMenu) { cardMenu.remove(); cardMenu = null; } }
  function showCardMenu(x, y, sid) {
    hideCardMenu();
    cardMenu = document.createElement('div');
    cardMenu.className = 'ctx-menu';
    cardMenu.style.left = x + 'px';
    cardMenu.style.top = y + 'px';
    cardMenu.innerHTML = `<div class="ctx-item danger">${GA_ICON('trash')}${esc(t('ctx.del'))}</div>`;
    cardMenu.querySelector('.ctx-item').onclick = (e) => {
      e.stopPropagation();
      fetch(`${CONDUCTOR_ORIGIN}/subagent/${sid}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ action: 'kill' }) });
      hideCardMenu();
    };
    document.body.appendChild(cardMenu);
    setTimeout(() => document.addEventListener('mousedown', (e) => { if (!cardMenu?.contains(e.target)) hideCardMenu(); }, { once: true }), 0);
  }

  let drawerEl = null;
  function closeWorkerDrawer() { if (drawerEl) { drawerEl.remove(); drawerEl = null; } }
  function openWorkerDrawer(w) {
    closeWorkerDrawer();
    drawerEl = document.createElement('div');
    drawerEl.className = 'collab-drawer-wrap';
    drawerEl.innerHTML = `<div class="collab-drawer-backdrop"></div><aside class="collab-drawer"><div class="collab-drawer-head"><span class="collab-drawer-title">${esc(w.title)}</span><button class="modal-x collab-drawer-close">${GA_ICON('x')}</button></div><div class="collab-drawer-body"><div class="bubble md"></div></div></aside>`;
    const bubble = drawerEl.querySelector('.collab-drawer-body .bubble');
    if (bubble) {
      bubble.innerHTML = renderTurnBody(w.fullReply || t('collab.summaryWait'));
      postRenderEnhance(bubble);
    }
    drawerEl.querySelector('.collab-drawer-backdrop').onclick = closeWorkerDrawer;
    drawerEl.querySelector('.collab-drawer-close').onclick = closeWorkerDrawer;
    document.body.appendChild(drawerEl);
  }

  function renderWorkers() {
    const box = $('collab-workers'), empty = $('collab-progress-empty');
    if (!box) return;
    if (empty) empty.hidden = S.workers.length > 0;
    box.innerHTML = S.workers.map(w => `
      <article class="collab-card collab-card--${w.status}" data-sid="${esc(w.id)}">
        <div class="collab-card-st">${collabStatusMark(w.status)}${esc(t(ST_KEYS[w.status] || 'collab.stPaused'))}${w.updatedAt ? `<span class="collab-card-time">${esc(relTime(w.updatedAt))}</span>` : ''}</div>
        <div class="collab-card-title">${esc(w.title)}</div>
        <div class="collab-card-sum">${esc(w.summary)}</div>
      </article>`).join('');
    box.querySelectorAll('.collab-card').forEach(el => {
      const w = S.workers.find(x => x.id === el.dataset.sid);
      if (w) {
        const prev = prevUpdated.get(w.id);
        if (prev != null && prev !== w.updatedAt) pulseEl(el);
        prevUpdated.set(w.id, w.updatedAt);
      }
      el.addEventListener('contextmenu', e => {
        e.preventDefault();
        showCardMenu(e.clientX, e.clientY, el.dataset.sid);
      });
      el.addEventListener('click', () => {
        if (w) openWorkerDrawer(w);
      });
    });
    const running = S.workers.filter(w => w.status === 'running').length;
    const done = S.workers.filter(w => w.status === 'reported').length;
    S.runningCount = running;
    document.dispatchEvent(new CustomEvent('collab:running-count', { detail: { count: running } }));
    const stats = $('collab-progress-stats');
    if (stats) {
      const has = running > 0 || done > 0;
      stats.hidden = !has;
      if (has) {
        stats.innerHTML = [
          running > 0 ? `<span class="collab-stat collab-stat--running">${GA_STATUS_BREATHE_SM}<span class="n">${running}</span> ${esc(t('collab.statRunning'))}</span>` : '',
          done > 0 ? `<span class="collab-stat collab-stat--done"><span class="collab-rail-dot" aria-hidden="true"></span><span class="n">${done}</span> ${esc(t('collab.statDone'))}</span>` : '',
        ].filter(Boolean).join('');
      }
    }
    syncRail({ pulse: true });
  }

  function syncMessages() {
    const area = $('collab-msgs'), welcome = $('collab-welcome'), list = $('collab-msg-list');
    if (!area || !list) return;
    if (!S.historyReady) {
      area.classList.remove('has-msgs');
      if (welcome) welcome.hidden = true;
      list.hidden = true;
      syncRail();
      return;
    }
    const has = S.messages.length > 0;
    area.classList.toggle('has-msgs', has);
    if (welcome) welcome.hidden = has;
    list.hidden = !has;
    list.replaceChildren();
    const toMsg = window.gaCollabItemToMsg;
    const render = window.gaMsgNode;
    if (toMsg && render) {
      for (const item of S.messages) {
        const el = render(toMsg(item));
        el.classList.add('collab-msg-enter');
        list.appendChild(el);
      }
    }
    syncDraft();
    scrollMsgs();
    syncRail();
  }

  function pushMsg(item) {
    if (item.id && S.messages.some(m => m.id === item.id)) return;
    if (item.role === 'user') {
      const plain = stripAttach(item.msg);
      const expand = window.gaExpandFilePlaceholders;
      for (let i = S.messages.length - 1; i >= 0; i--) {
        const m = S.messages[i];
        // 服务端回显的 msg 是 expand(本地文本)（附件被展开成了本地路径），和本地乐观消息其实是同一条。
        // 命中后保留本地那条的干净显示（占位符 msg + 结构化 files/images 卡片），只补服务端 id/ts，
        // 丢弃带路径的回显文本 —— 既去重、又不丢卡片、不外露本地路径，与 chat 显示一致。
        if (m._local && m.role === 'user' &&
            (stripAttach(m.msg) === plain || m.msg === item.msg || (expand && expand(m.msg) === item.msg))) {
          m.id = item.id || m.id;
          if (item.ts != null) m.ts = item.ts;
          if (item.read != null) m.read = item.read;
          m._local = false;
          syncMessages();
          setConnUi();
          return;
        }
      }
    }
    S.messages.push(item);
    if (item.role === 'conductor') S.conductorTyping = false;
    syncMessages();
    setConnUi();
  }

  function setWorkers(rawList) {
    S.workers = (rawList || []).map(normalizeWorker);
    renderWorkers();
  }

  function onWsData(data, gen) {
    if (gen !== wsGen) return;
    if (data.type === 'hello') {
      S.historyReady = true;
      S.messages = (data.chat || []).map(raw => ({ id: raw.id, role: raw.role || 'system', msg: raw.msg || '', ts: raw.ts, read: raw.read, files: raw.files || [], images: raw.images || [] }));
      S.conductorTyping = !!data.running;
      setWorkers(data.subagents || []);
      syncMessages();
      setConnUi();
    } else if (data.type === 'subagents') setWorkers(data.items || []);
    else if (data.type === 'chat') pushMsg({ id: data.item.id, role: data.item.role || 'system', msg: data.item.msg || '', ts: data.item.ts, read: data.item.read, files: data.item.files || [], images: data.item.images || [] });
  }

  function resetWs() {
    wsGen++;
    if (!ws) return;
    const old = ws;
    ws = null;
    old.onopen = old.onclose = old.onerror = old.onmessage = null;
    try { old.close(); } catch {}
  }

  function scheduleReconnect() {
    clearTimeout(connectTimer);
    clearInterval(reconnectTick);
    if (!S.everConnected && S.failCount >= FAIL_MAX) {
      S.reconnecting = false;
      return setConnUi();
    }
    const delay = Math.min(RECON_MAX, RECON_BASE * Math.pow(2, Math.max(0, S.failCount - 1)));
    S.reconnectAt = Date.now() + delay;
    S.reconnecting = S.everConnected;
    setConnUi();
    reconnectTick = setInterval(() => { if (!S.reconnecting) clearInterval(reconnectTick); else setConnUi(); }, 500);
    connectTimer = setTimeout(connect, delay);
  }

  function connect() {
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;
    clearTimeout(connectTimer);
    clearInterval(reconnectTick);
    const gen = ++wsGen;
    setConnUi();
    let sock;
    try { sock = new WebSocket(wsUrl()); } catch (e) {
      if (gen !== wsGen) return;
      S.failCount++;
      return scheduleReconnect();
    }
    ws = sock;
    sock.onopen = () => {
      if (gen !== wsGen) return;
      S.everConnected = true;
      S.serviceAvailable = true;
      S.reconnecting = false;
      S.failCount = 0;
      setConnUi();
    };
    sock.onclose = (ev) => {
      if (gen !== wsGen) return;
      S.serviceAvailable = false;
      if (S.everConnected) S.reconnecting = true;
      else S.failCount++;
      setConnUi();
      scheduleReconnect();
    };
    sock.onerror = () => {};
    sock.onmessage = ev => {
      if (gen !== wsGen) return;
      try { onWsData(JSON.parse(ev.data), gen); } catch {}
    };
  }

  function sendText(rawText) {
    const text = (rawText || '').trim();
    if (!text || !ws || ws.readyState !== WebSocket.OPEN) return false;
    window.gaSetActiveFileComposer?.('collab');
    const expand = window.gaExpandFilePlaceholders || (s => s);
    const collect = window.gaCollectUsedFiles || (() => []);
    const clearUsed = window.gaClearUsedPendingFiles || (() => {});
    const used = collect(text);
    const images = [], files = [];
    for (const f of used) (f.isImage ? images : files).push(f.isImage ? { path: f.path, dataUrl: f.dataUrl, name: f.name, sid: f.sid } : { path: f.path, name: f.name, sid: f.sid });
    S.messages.push({ id: `_local_${++localSeq}`, _local: true, role: 'user', msg: text, ts: Date.now() / 1000, images, files });
    S.conductorTyping = true;
    syncMessages();
    ws.send(JSON.stringify({ msg: expand(text), files, images }));
    clearUsed(text);
    window.collabComposer?.clearIfMatch?.(text);
    setConnUi();
    return true;
  }

  $('collab-retry')?.addEventListener('click', () => { S.failCount = 0; S.reconnecting = false; resetWs(); connect(); });
  $('collab-rail-toggle')?.addEventListener('click', () => toggleProgress());
  $('collab-prog-close')?.addEventListener('click', () => toggleProgress(false));

  window.collabComposer?.init?.(sendText);

  window.collabInit = () => {
    window.gaSetActiveFileComposer?.('collab');
    syncMessages();
    setConnUi();
    renderWorkers();
    connect();
  };
  window.collabFocus = () => window.collabComposer?.focus?.();
  window.collabRetranslate = () => { renderWorkers(); syncMessages(); setConnUi(); };
})();

/* ═══════════════ Composer layout: inline / stacked ═══════════════ */
(function initComposerLayout() {
  const BREAKPOINT = 480;
  const SINGLE_LINE = 36;

  document.querySelectorAll('.composer-inset').forEach(inset => {
    const input = inset.querySelector('.input');
    if (!input) return;

    function update() {
      const wide = inset.offsetWidth >= BREAKPOINT;
      const single = input.scrollHeight <= SINGLE_LINE;
      inset.classList.toggle('is-inline', wide && single);
    }

    new ResizeObserver(update).observe(inset);
    input.addEventListener('input', update);
    update();
  });
})();

/* ═══════════════ Workspace UI ═══════════════ */
(function initWorkspaceUI() {
  'use strict';
  const $ = id => document.getElementById(id);
  const chip = $('workspace-chip');
  const panel = $('workspace-panel');
  const panelCurrent = $('ws-panel-current');
  const currentRow = $('ws-current-row');
  const currentOff = $('ws-current-off');
  const panelList = $('ws-panel-list');
  const panelEmpty = $('ws-panel-empty');
  const addBtn = $('ws-add-btn');
  const addForm = $('ws-add-form');
  const addInput = $('ws-add-input');
  const addConfirm = $('ws-add-confirm');
  const addCancel = $('ws-add-cancel');
  const chipName = chip?.querySelector('.ws-chip-name');
  if (!chip || !panel) return;

  let _workspaces = [];
  let _currentWs = null;  // {name, path} or null

  function refreshChip() {
    if (_currentWs) {
      chipName.textContent = _currentWs.name;
      chip.title = _currentWs.path;
    } else {
      chipName.textContent = t('workspace.empty');
      chip.title = t('workspace.selectTitle');
    }
  }

  function closePanel() {
    panel.hidden = true;
    panel.style.left = '';
    panel.style.bottom = '';
    chip.classList.remove('open');
  }

  function openPanel() {
    closeAllModelMenus?.();
    if (window.collabComposer?.closeMenu) window.collabComposer.closeMenu();
    if (window.chatComposer?.closeMenu) window.chatComposer.closeMenu();
    refreshPanel().then(() => {
      // Position panel relative to chip
      const chipRect = chip.getBoundingClientRect();
      const composer = chip.closest('.composer');
      if (composer) {
        const composerRect = composer.getBoundingClientRect();
        panel.style.left = (chipRect.left - composerRect.left) + 'px';
        panel.style.bottom = (composerRect.bottom - chipRect.top + 4) + 'px';
      }
      panel.hidden = false;
      chip.classList.add('open');
    });
  }

  function togglePanel() {
    if (panel.hidden) openPanel();
    else closePanel();
  }

  async function refreshPanel() {
    try {
      const res = await window.ga.listWorkspaces();
      _workspaces = (res && res.workspaces) || [];
    } catch (_) { _workspaces = []; }
    try {
      const sid = state.activeId;
      if (sid) {
        const res = await window.ga.getSessionWorkspace(sid);
        _currentWs = (res && res.workspace) || null;
        refreshChip();
      }
    } catch (_) { /* API 失败不清空，保留上一个已知值 */ refreshChip(); }

    // Current workspace section
    if (_currentWs) {
      panelCurrent.hidden = false;
      const nameEl = currentRow.querySelector('.ws-row-name');
      const pathEl = currentRow.querySelector('.ws-row-path');
      if (nameEl) nameEl.textContent = _currentWs.name;
      if (pathEl) pathEl.textContent = _currentWs.path;
    } else {
      panelCurrent.hidden = true;
    }

    // Workspace list
    const others = _workspaces.filter(w => w.name !== (_currentWs?.name || ''));
    if (others.length === 0) {
      panelList.innerHTML = '';
      if (!_currentWs) panelEmpty.hidden = false;
      else panelEmpty.hidden = true;
    } else {
      panelEmpty.hidden = true;
      panelList.innerHTML = others.map(w => {
        const name = esc(w.name);
        const path = esc(w.path || '');
        const dangling = w.dangling ? `<span class="ws-row-dangling">${esc(t('workspace.dangling'))}</span>` : '';
        return `<div class="ws-panel-row" data-ws-name="${esc(w.name)}" data-ws-path="${esc(w.path || '')}">
          <span class="ws-row-name">${name}</span>
          <span class="ws-row-path">${path}${dangling}</span>
          <button type="button" class="ws-row-del" data-ws-name="${esc(w.name)}" data-i18n-title="workspace.removeTitle" title="${t('workspace.removeTitle')}">×</button>
        </div>`;
      }).join('');
    }
    applyI18n();
    // Attach click handlers directly (more reliable than event delegation)
    setTimeout(() => {
      panelList.querySelectorAll('.ws-panel-row').forEach(row => {
        if (row._wsBound) return;
        row._wsBound = true;
        row.addEventListener('click', function(e) {
          const delBtn = e.target.closest('.ws-row-del');
          if (delBtn) {
            removeWorkspace(delBtn.dataset.wsName);
            return;
          }
          const name = this.dataset.wsName;
          if (name) switchTo(name);
        });
      });
    }, 0);
  }

  function esc(s) {
    return String(s || '').replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  }

  async function switchTo(name) {
    if (!name) return;
    showToast('切换中...');
    let sid = state.activeId;
    if (!sid) {
      try {
        const res = await window.ga.rpc('session/new', {});
        if (res && res.sessionId) {
          sid = res.sessionId;
          state.activeId = sid;
          localStorage.setItem('ga_active', sid);
        } else {
          alert('创建会话失败，请重试');
          return;
        }
      } catch (e) {
        alert('创建会话失败: ' + (e.message || String(e)));
        return;
      }
    }
    try {
      await window.ga.setSessionWorkspace(sid, name);
      const info = _workspaces.find(w => w.name === name);
      _currentWs = { name, path: info?.path || '' };
      const sess = activeSess();
      if (sess) {
        sess.workspace = name;  // 同步前端缓存，避免切换会话时用过期值覆盖后端
        if (info?.path) {
          sess._workspacePath = info.path;
          state.lastWorkspacePath = info.path;
        }
      }
      refreshChip();
      closePanel();
      showToast(t('workspace.switched') || '已切换到 ' + name);
    } catch (e) {
      alert('切换失败: ' + (e.message || String(e)));
    }
  }

  async function turnOff() {
    const sid = state.activeId;
    if (!sid) return;
    try {
      await window.ga.offSessionWorkspace(sid);
      _currentWs = null;
      const sess = state.sessions.get(sid);
      if (sess) { sess.workspace = ''; sess._workspacePath = ''; }
      refreshChip();
      refreshPanel();
    } catch (e) {
      alert('Error: ' + (e.message || String(e)));
    }
  }

  async function removeWorkspace(name) {
    if (!(await showConfirmDialog({ title: t('common.delete'), message: '确认删除工作区「' + name + '」？\n（仅删除索引和快捷方式，不会删除真实文件）', okText: t('common.delete'), okKind: 'danger' }))) return;
    try {
      await window.ga.removeWorkspace(name);
      if (_currentWs && _currentWs.name === name) {
        _currentWs = null;
        refreshChip();
        const sid = state.activeId;
        if (sid) {
          try { await window.ga.offSessionWorkspace(sid); } catch (_) {}
          const sess = state.sessions.get(sid);
          if (sess) { sess.workspace = ''; sess._workspacePath = ''; }
        }
      }
      refreshPanel();
    } catch (e) {
      alert('Error: ' + (e.message || String(e)));
    }
  }

  async function showAddForm() {
    // Try native folder picker (Tauri) first
    const invoke = window.__TAURI__?.core?.invoke;
    if (invoke) {
      try {
        const folder = await invoke('pick_folder');
        if (folder) { doAdd(folder); return; }
      } catch (e) {
        console.error('[workspace] pick_folder failed:', e);
      }
    }
    // Always show inline form as fallback
    if (addBtn) addBtn.hidden = true;
    if (addForm) addForm.hidden = false;
    if (addInput) { addInput.value = ''; addInput.focus(); }
  }

  function hideAddForm() {
    addBtn.hidden = false;
    addForm.hidden = true;
    addInput.value = '';
  }

  async function doAdd(path) {
    path = path || addInput.value.trim();
    if (!path) return;
    hideAddForm();
    try {
      const res = await window.ga.prepareWorkspace(path);
      if (res && res.ok) {
        const sid = state.activeId;
        if (sid && res.name) {
          try {
            await window.ga.setSessionWorkspace(sid, res.name);
            const sess = activeSess();
            if (sess) {
              sess.workspace = res.name;  // 同步前端缓存
              sess._workspacePath = path;
              state.lastWorkspacePath = path;
            }
          } catch (_) {}
        }
        refreshPanel();
      } else if (res && res.error) {
        alert('Error: ' + res.error);
      }
    } catch (e) {
      alert('Error: ' + (e.message || String(e)));
    }
  }

  // Event handlers
  chip.addEventListener('click', (e) => {
    e.preventDefault();
    e.stopPropagation();
    togglePanel();
  });

  currentOff?.addEventListener('click', (e) => {
    e.stopPropagation();
    turnOff();
  });

  // Make the current workspace row clickable (e.g. reveal in Finder)
  currentRow?.addEventListener('click', (e) => {
    if (e.target.closest('.ws-row-off')) return;
    if (_currentWs && _currentWs.path) {
      // Could open in Finder via Tauri, but skip for now — just show feedback
      showToast(_currentWs.path);
    }
  });

  addBtn?.addEventListener('click', showAddForm);
  addCancel?.addEventListener('click', hideAddForm);
  addConfirm?.addEventListener('click', () => doAdd());
  addInput?.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); doAdd(); }
  });

  // Watch for session changes (poll state.activeId)
  let _lastActiveId = state.activeId;
  setInterval(async () => {
    if (state.activeId !== _lastActiveId) {
      _lastActiveId = state.activeId;
      try {
        if (state.activeId) {
          const res = await window.ga.getSessionWorkspace(state.activeId);
          _currentWs = (res && res.workspace) || null;
          const sess = activeSess();
          if (sess && _currentWs?.path) {
            sess._workspacePath = _currentWs.path;
            state.lastWorkspacePath = _currentWs.path;
          }
        }
        // activeId 为 null 时保留 _currentWs 旧值，避免误显"无 workspace"
        refreshChip();
      } catch (_) { /* API 失败不清空，保留上一个已知值 */ refreshChip(); }
    }
  }, 500);

  // Initial load
  window.addEventListener('load', () => {
    setTimeout(async () => {
      if (!state.bridgeReady) return;
      try {
        const sid = state.activeId;
        if (sid) {
          const res = await window.ga.getSessionWorkspace(sid);
          _currentWs = (res && res.workspace) || null;
          const sess = activeSess();
          if (sess && _currentWs?.path) {
            sess._workspacePath = _currentWs.path;
            state.lastWorkspacePath = _currentWs.path;
          }
        }
        refreshChip();
      } catch (_) {}
    }, 300);
  });

  // Refresh on bridge-ready
  window.ga.onBridgeReady(() => {
    setTimeout(async () => {
      const sid = state.activeId;
      if (sid) {
        try {
          const res = await window.ga.getSessionWorkspace(sid);
          _currentWs = (res && res.workspace) || null;
          const sess = activeSess();
          if (sess && _currentWs?.path) {
            sess._workspacePath = _currentWs.path;
            state.lastWorkspacePath = _currentWs.path;
          }
          refreshChip();
        } catch (_) {}
      }
    }, 200);
  });

  window.gaRefreshWorkspaceChip = () => { refreshChip(); };

/* ═══════════════ 文件管理 ═══════════════ */
let _filesData = { files: [], counts: {} };
let _filesFilter = 'all';
let _filesTypeFilter = 'all';
let _filesView = 'list';
let _filesSortKey = 'mtime';
let _filesSortDir = 'desc';
let _filesSelected = new Set();
let _filesMenuEl = null;
let _filesPreviewEl = null;

async function loadFilesPage() {
  await fetchFilesData();
  renderFilesFilter();
  renderFilesList();
  bindFilesEvents();
}

async function fetchFilesData() {
  try {
    const res = await fetch(`${BRIDGE_ORIGIN}/api/files/browse`);
    const d = await res.json();
    if (d.ok) {
      _filesData = { files: d.files || [], counts: d.counts || {} };
    }
  } catch (_) {
    _filesData = { files: [], counts: {} };
  }
}

function renderFilesFilter() {
  const el = document.getElementById('filesFilterList');
  if (!el) return;
  const counts = _filesData.counts;
  const total = (counts.total || 0);
  const allFiles = _filesData.files || [];
  const typeCounts = {};
  allFiles.forEach(f => {
    const ty = (f.type || 'file').toLowerCase();
    typeCounts[ty] = (typeCounts[ty] || 0) + 1;
  });
  const typeItems = Object.entries(typeCounts)
    .sort((a, b) => b[1] - a[1])
    .map(([ty, cnt]) => ({ key: ty, label: ty.toUpperCase(), count: cnt }));
  const items = [
    { key: 'all', label: t('files.all'), count: total },
    ...typeItems,
  ];
  el.innerHTML = items.map(item => `
    <li class="files-filter-item ${_filesTypeFilter === item.key ? 'active' : ''}" data-filter="${escapeHtml(item.key)}">
      <span>${escapeHtml(item.label)}</span>
      <span class="files-filter-badge">${item.count}</span>
    </li>
  `).join('');

  const sourceEl = document.getElementById('filesSource');
  if (sourceEl) {
    sourceEl.innerHTML = `
      <option value="all">${t('files.all')}</option>
      <option value="chat">${t('files.source.chat')}</option>
      <option value="task">${t('files.source.task')}</option>
      <option value="config">${t('files.source.config')}</option>
      <option value="generated">${t('files.source.generated')}</option>
    `;
    sourceEl.value = _filesFilter;
  }

  const sortEl = document.getElementById('filesSort');
  if (sortEl) {
    sortEl.innerHTML = `
      <option value="mtime">${t('files.sort.mtime')}</option>
      <option value="created">${t('files.sort.created')}</option>
      <option value="name">${t('files.sort.name')}</option>
      <option value="size">${t('files.sort.size')}</option>
    `;
    sortEl.value = _filesSortKey;
  }
  const sortDirEl = document.getElementById('filesSortDir');
  if (sortDirEl) {
    const arrow = sortDirEl.querySelector('.sort-dir-arrow');
    if (arrow) arrow.textContent = _filesSortDir === 'asc' ? '↑' : '↓';
    sortDirEl.title = _filesSortDir === 'asc' ? t('files.sortAsc') : t('files.sortDesc');
  }
}

function renderFilesList() {
  const el = document.getElementById('filesList');
  if (!el) return;

  let files = (_filesData.files || []).slice();

  const searchEl = document.getElementById('filesSearch');
  const q = (searchEl?.value || '').toLowerCase().trim();
  if (q) {
    files = files.filter(f => f.name.toLowerCase().includes(q));
  }

  if (_filesFilter !== 'all') {
    files = files.filter(f => f.source === _filesFilter);
  }

  if (_filesTypeFilter !== 'all') {
    files = files.filter(f => (f.type || 'file') === _filesTypeFilter);
  }

  files.sort((a, b) => {
    let va, vb;
    switch (_filesSortKey) {
      case 'name': va = (a.name || '').toLowerCase(); vb = (b.name || '').toLowerCase(); break;
      case 'size': va = a.size || 0; vb = b.size || 0; break;
      case 'created': va = new Date(a.created || 0).getTime(); vb = new Date(b.created || 0).getTime(); break;
      default: va = new Date(a.mtime || 0).getTime(); vb = new Date(b.mtime || 0).getTime();
    }
    const cmp = typeof va === 'string' ? (va < vb ? -1 : va > vb ? 1 : 0) : (va - vb);
    return _filesSortDir === 'asc' ? cmp : -cmp;
  });

  el.dataset.view = _filesView;

  if (files.length === 0) {
    el.innerHTML = `<div class="files-empty">${t('files.empty')}</div>`;
    return;
  }

  const groups = { chat: [], task: [], config: [], generated: [] };
  files.forEach(f => {
    (groups[f.source] = groups[f.source] || []).push(f);
  });

  const sourceLabels = { chat: t('files.source.chat'), task: t('files.source.task'), config: t('files.source.config'), generated: t('files.source.generated') };

  let html = '';
  for (const [source, items] of Object.entries(groups)) {
    if (!items.length) continue;
    html += `<div class="files-group">
      <div class="files-group-header">
        <span>${escapeHtml(sourceLabels[source] || source)}</span>
        <span class="files-group-count">${items.length}</span>
        <button type="button" class="files-group-toggle" aria-label="toggle group"><span data-ga-icon="caretDown"></span></button>
      </div>
      <div class="files-group-body">`;

    html += items.map(f => {
      const icon = fileIcon(f.type);
      const sizeStr = formatFileSize(f.size);
      const timeStr = formatFileMtime(f.mtime);
      const refBadge = f.referencedBy
        ? `<span class="files-item-ref" title="${escapeHtml(f.referencedBy)}">${t('files.referencedBy')}</span>`
        : '';
      const dlUrl = `/upload/raw?path=${encodeURIComponent(f.path)}&download=1`;
      const encodedPath = encodeURIComponent(f.path);
      const isSel = _filesSelected.has(f.path);
      const fKind = f.source === 'chat' ? 'upload' : (f.source || 'general');
      const previewable = isFilePreviewable(f.type, f.source);
      return `
        <div class="files-item${isSel ? ' selected' : ''}" data-path="${encodedPath}" data-kind="${fKind}" data-previewable="${previewable ? 1 : 0}">
          <span class="files-item-check"${isSel ? '' : ' style="display:none"'}><span data-ga-icon="check"></span></span>
          <span class="files-item-icon">${icon}</span>
          <div class="files-item-info">
            <span class="files-item-name" title="${escapeHtml(f.name)}">${escapeHtml(f.name)}</span>
            <span class="files-item-meta">
              <span>${sizeStr}</span>
              <span>${timeStr}</span>
              ${refBadge}
            </span>
          </div>
          <div class="files-item-actions">
            <a href="${dlUrl}" class="files-item-dl" title="${t('files.download')}" target="_blank"><span data-ga-icon="download"></span></a>
            <button type="button" class="files-item-menu" data-path="${encodedPath}" title="更多操作"><span data-ga-icon="dotsThreeVertical"></span></button>
          </div>
        </div>`;
    }).join('');

    html += `</div></div>`;
  }

  el.innerHTML = html;
  _filesRenderIcons(el);

  // bind item selection (click on item body, not actions)
  el.querySelectorAll('.files-item').forEach(item => {
    item.addEventListener('click', (e) => {
      if (e.target.closest('.files-item-actions')) return;
      if (e.target.closest('.files-item-check')) return;
      const path = decodeURIComponent(item.dataset.path || '');
      if (!path) return;
      const additive = e.metaKey || e.ctrlKey;
      if (additive) {
        if (_filesSelected.has(path)) _filesSelected.delete(path);
        else _filesSelected.add(path);
      } else {
        _filesSelected.clear();
        _filesSelected.add(path);
      }
      updateFileSelectionUI();
      updateFilesBatchBar();
    });
    // check 点击切换选中
    const checkEl = item.querySelector('.files-item-check');
    if (checkEl) {
      checkEl.addEventListener('click', (e) => {
        e.stopPropagation();
        const path = decodeURIComponent(item.dataset.path || '');
        if (!path) return;
        toggleFileSelect(path);
      });
    }
  });

  // bind menu button
  el.querySelectorAll('.files-item-menu').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      toggleFilesMenu(btn);
    });
  });

  // group toggle
  el.querySelectorAll('.files-group-header').forEach(header => {
    header.addEventListener('click', () => {
      header.closest('.files-group').classList.toggle('collapsed');
    });
  });
}

function _filesRenderIcons(root) {
  if (window.gaHydrateIcons) { window.gaHydrateIcons(root); return; }
  if (typeof phosphorIcons !== 'undefined' && phosphorIcons.render) phosphorIcons.render();
}
function isFilePreviewable(type, source) {
  const tp = (type || '').toLowerCase();
  const imgs = ['jpg','jpeg','png','gif','webp','svg','bmp','ico'];
  const texts = ['md','txt','json','py','js','ts','jsx','tsx','yaml','yml','csv','log','ini','toml','html','css','xml','sh','bash','conf','env','sql','rs','go','java','c','cpp','h','rb','php','vue','svelte'];
  if (source === 'chat' && (imgs.includes(tp) || tp === 'pdf')) return true;
  return texts.includes(tp);
}
function updateFileSelectionUI() {
  document.querySelectorAll('#filesList .files-item').forEach(item => {
    const path = decodeURIComponent(item.dataset.path || '');
    const sel = _filesSelected.has(path);
    item.classList.toggle('selected', sel);
    const chk = item.querySelector('.files-item-check');
    if (chk) chk.style.display = sel ? '' : 'none';
  });
}
function _fileKind(f) {
  return f.source === 'chat' ? 'upload' : (f.source || 'file');
}
function openFilesMenu(btn, path) {
  closeFilesMenu();
  if (!_filesSelected.has(path)) {
    _filesSelected.clear();
    _filesSelected.add(path);
    updateFileSelectionUI();
  }
  const items = (_filesData && _filesData.items) || [];
  const fs = [];
  _filesSelected.forEach(p => {
    const f = items.find(it => it.path === p);
    if (f) fs.push(f);
  });
  if (!fs.length) return;
  const single = fs.length === 1;
  const f0 = fs[0];
  const menu = document.createElement('div');
  menu.className = 'files-menu';
  const add = (label, icon, fn) => {
    const it = document.createElement('div');
    it.className = 'files-menu-item';
    it.innerHTML = `<span data-ga-icon="${icon}"></span><span>${label}</span>`;
    it.addEventListener('click', () => { closeFilesMenu(); fn(fs); });
    menu.appendChild(it);
  };
  if (single && f0 && isFilePreviewable(f0.type, f0.source)) add(t('files.preview'), 'magnifyingGlass', filesExecPreview);
  if (single) add('打开文件', 'fileText', filesExecOpen);
  add('打开位置', 'folderSimple', filesExecReveal);
  add('复制路径', 'copy', filesExecCopy);
  add('删除', 'trash', filesExecDelete);
  document.body.appendChild(menu);
  _filesRenderIcons(menu);
  _filesMenuEl = menu;
  const r = btn.getBoundingClientRect();
  menu.style.top = (r.bottom + window.scrollY + 4) + 'px';
  menu.style.left = Math.max(8, r.right + window.scrollX - menu.offsetWidth) + 'px';
  setTimeout(() => { document.addEventListener('click', _filesMenuOutside); }, 0);
}
function _filesMenuOutside(e) {
  if (_filesMenuEl && !_filesMenuEl.contains(e.target) && !e.target.closest('.files-item-menu')) closeFilesMenu();
}
function closeFilesMenu() {
  if (_filesMenuEl) { _filesMenuEl.remove(); _filesMenuEl = null; }
  document.removeEventListener('click', _filesMenuOutside);
}
function filesExecOpen(fs) {
  fs.forEach(f => fetch(`${BRIDGE_ORIGIN}/path/open`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ path: f.path, mode: 'open', kind: _fileKind(f) }) }));
}
function filesExecReveal(fs) {
  fs.forEach(f => fetch(`${BRIDGE_ORIGIN}/path/open`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ path: f.path, mode: 'reveal', kind: _fileKind(f) }) }));
}
function filesExecCopy(fs) {
  const text = fs.map(f => f.path).join('\n');
  if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(text).then(() => alert('已复制 ' + fs.length + ' 个路径到剪贴板')).catch(() => alert(text));
  else alert(text);
}
async function filesExecDelete(fs) {
  if (!(await showConfirmDialog({ title: t('common.delete'), message: '确认删除 ' + fs.length + ' 个文件？此操作不可恢复。', okText: t('common.delete'), okKind: 'danger' }))) return;
  for (const f of fs) {
    try {
      await fetch(`${BRIDGE_ORIGIN}/api/files/delete`, { method: 'DELETE', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ path: f.path }) });
    } catch (e) {}
  }
  _filesSelected.clear();
  loadFilesPage();
}

async function filesExecPreview(f) {
  const tp = (f.type || '').toLowerCase();
  const imgs = ['jpg','jpeg','png','gif','webp','svg','bmp','ico'];
  if (f.source === 'chat' && (imgs.includes(tp) || tp === 'pdf')) {
    openFilePreview(tp === 'pdf' ? 'pdf' : 'image', f.name, `${BRIDGE_ORIGIN}/upload/raw?path=${encodeURIComponent(f.path)}`);
  } else {
    try {
      const res = await fetch(`${BRIDGE_ORIGIN}/api/files/read?path=${encodeURIComponent(f.path)}`);
      const d = await res.json();
      if (d.ok) openFilePreview('text', f.name, null, d.content);
      else alert(d.error || '无法预览');
    } catch (e) { alert('预览失败: ' + e.message); }
  }
}
function openFilePreview(kind, name, url, text) {
  closeFilePreview();
  const ov = document.createElement('div');
  ov.className = 'files-preview-overlay';
  let body = '';
  if (kind === 'image') body = `<img src="${url}" alt="${escapeHtml(name)}" />`;
  else if (kind === 'pdf') body = `<iframe src="${url}" title="${escapeHtml(name)}"></iframe>`;
  else body = `<pre>${escapeHtml(text || '')}</pre>`;
  ov.innerHTML = `<div class="files-preview-modal"><div class="files-preview-header"><span class="files-preview-name">${escapeHtml(name)}</span><button type="button" class="files-preview-close" title="关闭"><span data-ga-icon="x"></span></button></div><div class="files-preview-body">${body}</div></div>`;
  document.body.appendChild(ov);
  _filesRenderIcons(ov);
  _filesPreviewEl = ov;
  ov.querySelector('.files-preview-close').addEventListener('click', closeFilePreview);
  ov.addEventListener('click', (e) => { if (e.target === ov) closeFilePreview(); });
  document.addEventListener('keydown', _filesPreviewEsc);
}
function _filesPreviewEsc(e) { if (e.key === 'Escape') closeFilePreview(); }
function closeFilePreview() {
  if (_filesPreviewEl) { _filesPreviewEl.remove(); _filesPreviewEl = null; }
  document.removeEventListener('keydown', _filesPreviewEsc);
}

let _filesEventsBound = false;
function bindFilesEvents() {
  if (_filesEventsBound) return;
  const searchEl = document.getElementById('filesSearch');
  const sourceEl = document.getElementById('filesSource');
  const viewBtn = document.getElementById('filesViewBtn');
  const refreshBtn = document.getElementById('filesRefresh');
  const filterList = document.getElementById('filesFilterList');

  if (searchEl) searchEl.addEventListener('input', () => { renderFilesList(); });
  if (sourceEl) sourceEl.addEventListener('change', () => { _filesFilter = sourceEl.value; renderFilesList(); });
  const sortEl = document.getElementById('filesSort');
  const sortDirEl = document.getElementById('filesSortDir');
  if (sortEl) sortEl.addEventListener('change', () => { _filesSortKey = sortEl.value; renderFilesList(); });
  if (sortDirEl) sortDirEl.addEventListener('click', () => {
    _filesSortDir = _filesSortDir === 'asc' ? 'desc' : 'asc';
    const arrow = sortDirEl.querySelector('.sort-dir-arrow');
    if (arrow) arrow.textContent = _filesSortDir === 'asc' ? '↑' : '↓';
    sortDirEl.title = _filesSortDir === 'asc' ? t('files.sortAsc') : t('files.sortDesc');
    renderFilesList();
  });
  if (viewBtn) viewBtn.addEventListener('click', () => {
    _filesView = _filesView === 'list' ? 'grid' : 'list';
    const iconEl = viewBtn.querySelector('[data-ga-icon]');
    if (iconEl) iconEl.dataset.gaIcon = _filesView === 'list' ? 'list' : 'gridFour';
    if (window.gaHydrateIcons) window.gaHydrateIcons(viewBtn);
    renderFilesList();
  });
  if (refreshBtn) refreshBtn.addEventListener('click', () => { loadFilesPage(); });
  if (filterList) filterList.addEventListener('click', (e) => {
    const item = e.target.closest('.files-filter-item');
    if (!item) return;
    _filesTypeFilter = item.dataset.filter;
    renderFilesFilter();
    renderFilesList();
  });
  _filesEventsBound = true;
}

// 关闭所有已打开的文件菜单下拉
function closeAllFilesMenus() {
  document.querySelectorAll('.files-menu-dropdown.open').forEach(d => d.remove());
}

// 切换单个文件选中状态
function toggleFileSelect(path) {
  if (_filesSelected.has(path)) _filesSelected.delete(path);
  else _filesSelected.add(path);
  renderFilesList();
  updateFilesBatchBar();
}

// 更新批量操作工具栏
function updateFilesBatchBar() {
  let bar = document.getElementById('filesBatchBar');
  const toolbar = document.querySelector('.files-toolbar');
  const list = document.getElementById('filesList');
  if (!toolbar) return;
  const n = _filesSelected.size;
  if (n === 0) {
    if (bar) bar.remove();
    return;
  }
  if (!bar) {
    bar = document.createElement('div');
    bar.id = 'filesBatchBar';
    bar.className = 'files-batch-bar';
    toolbar.parentNode.insertBefore(bar, list);
  }
  bar.innerHTML = `
    <span class="files-batch-count">${t('files.selected').replace('{n}', n)}</span>
    <button type="button" class="files-batch-btn" data-act="openLocation"><span data-ga-icon="folderOpen"></span>${t('files.batchOpenLocation')}</button>
    <button type="button" class="files-batch-btn" data-act="copy"><span data-ga-icon="copy"></span>${t('files.batchCopy')}</button>
    <button type="button" class="files-batch-btn danger" data-act="delete"><span data-ga-icon="trash"></span>${t('files.batchDelete')}</button>
    <button type="button" class="files-batch-cancel" data-act="cancel">${t('files.cancel')}</button>
  `;
  _filesRenderIcons(bar);
  // bind batch actions
  bar.querySelectorAll('button').forEach(btn => {
    btn.addEventListener('click', () => handleFilesBatch(btn.dataset.act));
  });
}

// 批量操作处理
async function handleFilesBatch(act) {
  const paths = Array.from(_filesSelected);
  if (act === 'cancel') {
    _filesSelected.clear();
    renderFilesList();
    updateFilesBatchBar();
    return;
  }
  if (act === 'delete') {
    if (!(await showConfirmDialog({ title: t('common.delete'), message: t('files.confirmDeleteMulti').replace('{n}', paths.length), okText: t('common.delete'), okKind: 'danger' }))) return;
    for (const path of paths) {
      try {
        const res = await fetch(`${BRIDGE_ORIGIN}/api/files/delete`, {
          method: 'DELETE',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ path })
        });
        const d = await res.json();
        if (d.ok) {
          _filesData.files = _filesData.files.filter(f => f.path !== path);
          if (_filesData.counts) {
            _filesData.counts.total = Math.max(0, (_filesData.counts.total || 1) - 1);
            if (d.source && _filesData.counts[d.source] != null) _filesData.counts[d.source] = Math.max(0, _filesData.counts[d.source] - 1);
          }
        }
      } catch (_) {}
    }
    _filesSelected.clear();
    renderFilesFilter();
    renderFilesList();
    updateFilesBatchBar();
    return;
  }
  if (act === 'copy') {
    showToast(t('files.copying'));
    for (const path of paths) {
      try {
        await fetch(`${BRIDGE_ORIGIN}/api/files/copy`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ path })
        });
      } catch (_) {}
    }
    await fetchFilesData();
    _filesSelected.clear();
    renderFilesList();
    updateFilesBatchBar();
    showToast(t('files.copied'));
    return;
  }
  if (act === 'openLocation') {
    for (const path of paths) {
      try {
        await fetch(`${BRIDGE_ORIGIN}/path/open`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ path, mode: 'reveal' })
        });
      } catch (_) {}
    }
    return;
  }
}

// 单个文件操作
async function handleFileAction(act, path, btn) {
  closeAllFilesMenus();
  if (act === 'preview') {
    const f = _filesData.files.find(x => x.path === path);
    if (f) filesExecPreview(f);
    return;
  }
  if (act === 'open') {
    try {
      await fetch(`${BRIDGE_ORIGIN}/path/open`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path, mode: 'open' })
      });
    } catch (_) {}
    return;
  }
  if (act === 'openLocation') {
    try {
      await fetch(`${BRIDGE_ORIGIN}/path/open`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path, mode: 'reveal' })
      });
    } catch (_) {}
    return;
  }
  if (act === 'copy') {
    showToast(t('files.copying'));
    try {
      const res = await fetch(`${BRIDGE_ORIGIN}/api/files/copy`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path })
      });
      const d = await res.json();
      if (d.ok) {
        await fetchFilesData();
        renderFilesList();
        showToast(t('files.copied'));
      }
    } catch (_) {}
    return;
  }
  if (act === 'delete') {
    if (!(await showConfirmDialog({ title: t('common.delete'), message: t('files.confirmDelete'), okText: t('common.delete'), okKind: 'danger' }))) return;
    try {
      const res = await fetch(`${BRIDGE_ORIGIN}/api/files/delete`, {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path })
      });
      const d = await res.json();
      if (d.ok) {
        _filesData.files = _filesData.files.filter(f => f.path !== path);
        _filesSelected.delete(path);
        if (_filesData.counts) {
          _filesData.counts.total = Math.max(0, (_filesData.counts.total || 1) - 1);
          if (d.source && _filesData.counts[d.source] != null) _filesData.counts[d.source] = Math.max(0, _filesData.counts[d.source] - 1);
        }
        renderFilesFilter();
        renderFilesList();
        updateFilesBatchBar();
      }
    } catch (_) {}
    return;
  }
}

// 打开/关闭菜单下拉
function toggleFilesMenu(btn) {
  const wasOpen = btn.parentElement.querySelector('.files-menu-dropdown.open');
  closeAllFilesMenus();
  if (wasOpen) return;
  const path = decodeURIComponent(btn.dataset.path);
  const item = btn.closest('.files-item');
  const previewable = item && item.dataset.previewable === '1';
  const multiSelected = _filesSelected.size > 1;
  const dropdown = document.createElement('div');
  dropdown.className = 'files-menu-dropdown open files-menu';
  dropdown.innerHTML = `
    ${previewable && !multiSelected ? `<button type="button" class="files-menu-item" data-act="preview"><span data-ga-icon="eye"></span>${t('files.preview')}</button>` : ''}
    <button type="button" class="files-menu-item${multiSelected ? ' disabled' : ''}" data-act="open"${multiSelected ? ' disabled' : ''}><span data-ga-icon="arrowUpRight"></span>${t('files.open')}</button>
    <button type="button" class="files-menu-item" data-act="openLocation"><span data-ga-icon="folderOpen"></span>${t('files.openLocation')}</button>
    <button type="button" class="files-menu-item" data-act="copy"><span data-ga-icon="copy"></span>${t('files.copy')}</button>
    <div class="files-menu-sep"></div>
    <button type="button" class="files-menu-item danger" data-act="delete"><span data-ga-icon="trash"></span>${t('files.delete')}</button>
  `;
  btn.parentElement.appendChild(dropdown);
  _filesRenderIcons(dropdown);
  dropdown.querySelectorAll('button').forEach(b => {
    b.addEventListener('click', (e) => {
      e.stopPropagation();
      if (b.disabled) return;
      handleFileAction(b.dataset.act, path, b);
    });
  });
}

function fileIcon(type) {
  const map = {
    jpg:'🖼',jpeg:'🖼',png:'🖼',gif:'🖼',webp:'🖼',svg:'🖼',bmp:'🖼',ico:'🖼',
    md:'📝',txt:'📝',log:'📝',
    json:'📋',yaml:'📋',yml:'📋',toml:'📋',xml:'📋',
    py:'🐍',js:'📜',ts:'📜',jsx:'📜',tsx:'📜',html:'📜',css:'📜',
    pdf:'📄',doc:'📄',docx:'📄',
    zip:'📦',gz:'📦',tar:'📦',rar:'📦','7z':'📦',
    mp3:'🎵',wav:'🎵',flac:'🎵',
    mp4:'🎬',avi:'🎬',mov:'🎬',
    csv:'📊',xlsx:'📊',xls:'📊',
  };
  return map[(type||'').toLowerCase()]||'📁';
}

function formatFileSize(bytes) {
  if (bytes == null) return '-';
  const n = Number(bytes);
  if (n < 1024) return n + ' ' + t('files.sizeB');
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + ' ' + t('files.sizeKB');
  return (n / (1024 * 1024)).toFixed(1) + ' ' + t('files.sizeMB');
}

function formatFileMtime(mtime) {
  if (!mtime) return '-';
  try {
    const d = new Date(mtime);
    if (isNaN(d.getTime())) return '-';
    const pad = n => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
  } catch (_) { return '-'; }
}

window.initFilesPage = loadFilesPage;
window.loadFilesPage = loadFilesPage;
})();
