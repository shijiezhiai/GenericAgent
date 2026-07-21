"""Expert Mode plugin — 零核心代码改动实现 GA「专家」功能。

机制：注册 agent_before hook（agent_loop.py 中每个用户轮触发一次）。
当某专家激活时，把 L1 层（角色人设 + 工作方法 + 工具倾向 + 知识指针 + 收尾纪律）
追加到最后一条 user message（str 直接拼接，多模态 list 追加 text block）。
两层设计：L1 每轮全量注入（轻量、稳定）；L2 = knowledge.md 全文不注入，
由模型按 L1 中的指针与线索（行数/大小）自行判断是否用 file 工具读取。
利用 messages 是 list 引用的事实，直接 mutate 即反映到真正发给 LLM 的内容。

专家配置（版本化资产，类比 skills_external）：
  experts/<name>/expert.md      人设：YAML frontmatter(role/goal/backstory/tools/model) + 正文工作方法
  experts/<name>/knowledge.md   专家知识库（L2 按需读，不每轮注入）

激活态载体 = 文件锚 temp/.active_expert.<宿主pid>（存当前专家名）。PID 键控：
  - 锚只对写它的那个 GA 进程有效 → 多开 GA 各自激活不同专家，互不可见
  - GA 关闭即自动失活（重启后 pid 变，旧锚作废）
  - 进入：agent 用 code_run 调 activate(name)（os.getppid() 即宿主 pid），或直接写锚文件
  - 退出：deactivate() 或删除该文件。插件加载时清扫自己 pid 的前世残留（不碰他进程的锚）

专家 ≈ 「带人设 + 工具白名单 + 知识库的可切换角色模式」，是 project_mode 的泛化：
project_mode 注入"项目"上下文，expert_mode 注入"专家"人设 + 知识库 + 工作方法。
"""
import os
import glob
import plugins.hooks as hooks

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_EXPERTS_DIR = os.path.join(_PROJECT_ROOT, 'experts')
_TEMP = os.path.join(_PROJECT_ROOT, 'temp')
_ANCHOR = os.path.join(_TEMP, f'.active_expert.{os.getpid()}')  # import 时 = GA 主进程 pid


def _cleanup_stale_anchors():
    """清扫自己 pid 的前世残留锚（pid 复用）。他进程的锚一律不碰——故意不做存活探测，
    避免引入杀进程风险（激活判定只认自己 pid 的锚，残留不影响行为）。"""
    for path in glob.glob(os.path.join(_TEMP, '.active_expert*')):
        if path == _ANCHOR:
            try:
                os.remove(path)
            except OSError:
                pass


_cleanup_stale_anchors()


def _active_expert(ctx=None):
    """返回当前激活的专家名；未激活返回 None。

    兼容策略（仿 project_mode）:
      - 新 TUI 多会话可在当前 GenericAgent 实例上设置 _ga_expert_name。
        只要该属性存在，就以它为准；值为 None/空串表示该 agent 普通模式（无专家）。
      - 其它 UI / 旧 SOP 不设置该属性，继续读取 pid 键控文件锚。
    """
    parent = None
    if isinstance(ctx, dict):
        handler = ctx.get('handler')
        parent = getattr(handler, 'parent', None)
    if parent is not None and hasattr(parent, '_ga_expert_name'):
        return getattr(parent, '_ga_expert_name', None) or None
    if not os.path.isfile(_ANCHOR):
        return None
    return open(_ANCHOR, encoding='utf-8').read().strip() or None


def _expert_dir(name):
    return os.path.join(_EXPERTS_DIR, name)


def _persona_path(name):
    return os.path.join(_expert_dir(name), 'expert.md')


def _knowledge_path(name):
    return os.path.join(_expert_dir(name), 'knowledge.md')


def _parse_persona(name):
    """解析 expert.md：返回 (meta_dict, body_text)。
    frontmatter 用 YAML（GA 环境已确认有 PyYAML，不可用时退化为极简 key:value）。
    无 frontmatter 则 meta={}、body=全文。文件不存在返回 (None, None)。"""
    path = _persona_path(name)
    if not os.path.isfile(path):
        return None, None
    text = open(path, encoding='utf-8').read()
    meta, body = {}, text
    if text.startswith('---'):
        parts = text.split('---', 2)
        if len(parts) >= 3:
            try:
                import yaml
                meta = yaml.safe_load(parts[1]) or {}
            except Exception:
                meta = {}
                for line in parts[1].splitlines():
                    if ':' in line:
                        k, v = line.split(':', 1)
                        meta[k.strip()] = v.strip()
            body = parts[2].strip()
    return meta, body


def _knowledge_stat(name):
    """返回 knowledge.md 的 (存在, 行数, 字节数)，供 L1 指针给模型判断依据。"""
    path = _knowledge_path(name)
    if os.path.isfile(path):
        data = open(path, encoding='utf-8').read()
        return True, len(data.splitlines()), len(data.encode('utf-8'))
    return False, 0, 0


def _build_injection(name):
    """构造追加到 user message 末尾的内容（两层设计的 L1 层）。"""
    meta, body = _parse_persona(name)
    if meta is None:
        # 专家配置缺失，提示并自动失活（避免每轮刷错误）
        try:
            os.remove(_ANCHOR)
        except OSError:
            pass
        return f"\n\n---\n[EXPERT MODE] 专家「{name}」配置缺失（{_persona_path(name)}），已自动失活。\n---"
    role = meta.get('role', name)
    goal = meta.get('goal', '')
    backstory = meta.get('backstory', '')
    tools = meta.get('tools', '')
    model_pref = meta.get('model', '')

    k_exists, k_lines, k_bytes = _knowledge_stat(name)
    if k_exists and k_bytes > 0:
        k_hint = (
            f"专家知识库在 {_knowledge_path(name)}（{k_lines} 行 / {k_bytes} 字节）。"
            f"本轮任务若涉及本专业领域的具体方法/checklist/避坑点，先读它再动手；"
            f"若与专业知识无关，可不读。自行判断。"
        )
    else:
        k_hint = f"专家知识库 {_knowledge_path(name)} 暂为空，无需读取。"

    tools_block = ""
    if tools:
        tools_block = (
            f"- 工具倾向：作为本专家，优先使用 [{tools}]；非必要不偏离本专家的工具域"
            f"（软约束，未硬过滤工具集——保持零核心改动）\n"
        )

    model_block = ""
    if model_pref:
        model_block = f"- 模型偏好：{model_pref}（提示，未自动切换）\n"

    # 不硬编码标题：expert.md 正文自带小节结构（"## 工作方法"/"## 诊断流程"…），直接用 body
    body_block = f"\n{body}\n" if body else ""

    return (
        f"\n\n---\n"
        f"[EXPERT MODE: {name}]\n"
        f"你正在「{role}」专家模式中。\n\n"
        f"## 角色\n"
        f"- Role：{role}\n"
        f"- Goal：{goal}\n"
        f"- Backstory：{backstory}\n"
        f"{tools_block}{model_block}"
        f"{body_block}\n"
        f"## 规则\n"
        f"- {k_hint}\n"
        f"## 收尾纪律\n"
        f"干完本轮活后自问：「记忆归零、重新接手本任务的我，缺了本轮哪条专业经验会重复付出"
        f"认知代价——再踩一次坑、再摸索一次？」会的，就用 file 工具把那条追加进 "
        f"{_knowledge_path(name)}，写成未来的自己能直接复用的一句话；不会的，一个字都不写。\n"
        f"---"
    )


@hooks.register('agent_before')
def inject_expert_persona(ctx):
    """每个用户轮起始时，若专家激活，把专家人设追加到 user message。"""
    name = _active_expert(ctx)
    if not name:
        return  # 未激活，普通模式，什么都不做
    um = next((m for m in reversed(ctx.get('messages') or [])
               if isinstance(m, dict) and m.get('role') == 'user'), None)
    if um is None:
        return
    content = um.get('content')
    injection = _build_injection(name)
    if isinstance(content, str):
        um['content'] = content + injection
    elif isinstance(content, list):  # 多模态：追加 text block
        content.append({'type': 'text', 'text': injection})


# ---- 便捷激活/失活/列举（供主 agent 通过 code_run import 调用）----
# 注意：code_run 在子进程执行，os.getppid() = GA 主进程 pid，与模块级 _ANCHOR 的 pid 一致。
def activate(name):
    """激活指定专家。专家需已存在于 experts/<name>/expert.md。通过 code_run 调用。"""
    if not os.path.isfile(_persona_path(name)):
        raise FileNotFoundError(f"专家「{name}」配置不存在：{_persona_path(name)}")
    os.makedirs(_TEMP, exist_ok=True)
    anchor = os.path.join(_TEMP, f'.active_expert.{os.getppid()}')
    open(anchor, 'w', encoding='utf-8').write(name)
    return f"已激活专家「{name}」（宿主 PID {os.getppid()}，锚 {anchor}）"


def deactivate():
    """失活当前专家（回到普通模式）。通过 code_run 调用。"""
    anchor = os.path.join(_TEMP, f'.active_expert.{os.getppid()}')
    try:
        os.remove(anchor)
    except OSError:
        pass
    return "已失活专家（回到普通模式）"


def list_experts():
    """列出所有可用专家（experts/*/expert.md）。"""
    if not os.path.isdir(_EXPERTS_DIR):
        return []
    return [d for d in sorted(os.listdir(_EXPERTS_DIR))
            if os.path.isfile(_persona_path(d))]
