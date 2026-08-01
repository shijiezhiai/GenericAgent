"""Skills Loader plugin — 把外部 Agent Skills（Anthropic 开放规范）安装给 GA。

机制：注册 agent_before hook，每个用户轮起始时把已发现的 skill 索引追加到 user message。
设计为两层渐进加载（与 project_mode 一致）：
  L1（每轮注入）：skill 索引表——name / 触发关键词 / SKILL.md 路径 / 运行基准，轻量稳定。
  L2（按需读取）：模型命中某 skill 后，用 file 工具读对应 SKILL.md 正文获取完整指令；
                  references/ 下的文档由 SKILL.md 正文指明加载条件，再按需读取。

配置（优先级从高到低）：
  1. 环境变量 GA_SKILLS_ROOTS：冒号分隔的 skills 根目录列表，设置即启用
  2. ga_config（DuckDB skill_roots/skill_flags/config_kv 表）——唯一真源
若以上均不存在，默认不注入（零干扰）。

skills_config.json 已退役为一次性导入源，仅 GA_STORAGE=json 回滚时才继续读写。
"""

import os
import re
import json

try:
    import ga_config
except ImportError:  # imported outside a ga_root-on-sys.path context
    import sys as _sys
    _sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    import ga_config

try:
    from plugins import hooks
except Exception:
    hooks = None

_PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
_GA_ROOT = os.path.dirname(_PLUGIN_DIR)            # 实际运行根目录（dev=仓库; bundle=app-support/app）
def _is_bundle_mode():
    # 优先用 Rust 注入的 env 判定；env 若未达 bridge 子进程，退回路径启发式：
    # clone 目标位于 ~/Library/Application Support 或 .app 包内即视为 bundle 部署。
    if os.environ.get("GA_BUILD_ID"):
        return True
    p = _GA_ROOT
    if "Application Support" in p or ".app/Contents/Resources/runtime" in p:
        return True
    return False
_IS_BUNDLE = _is_bundle_mode()   # bundle 模式：配置/数据持久到 app-support（app/ 外）
# 持久根：bundle 下为 app-support（与 app/ 同级，升级不被 clone 覆盖）；dev 下等同仓库根
_SUPPORT_ROOT = os.path.dirname(_GA_ROOT) if _IS_BUNDLE else _GA_ROOT

# bundle 模式下配置放到 app-support 根（app/ 外），随用户持久、升级不被覆盖；
# dev 模式保持原 plugins/skills_config.json。
if _IS_BUNDLE:
    _CONFIG_PATH = os.environ.get("GA_SKILLS_CONFIG", os.path.join(_SUPPORT_ROOT, "skills_config.json"))
else:
    _CONFIG_PATH = os.environ.get("GA_SKILLS_CONFIG", os.path.join(_PLUGIN_DIR, "skills_config.json"))
_injection_cache = None  # (config_mtime, text)


def _resolve_root(entry):
    """把 skills_config.json 里的 root 解析为绝对路径。

    - 绝对路径：原样返回（兼容旧配置）。
    - 相对路径：优先按 _GA_ROOT 解析（自带 skills_external/... 在 app/ 内，随升级刷新）；
      否则按 _SUPPORT_ROOT 解析（用户安装的 ext_plugins/... 在 app/ 外的持久目录）。
    """
    if not entry:
        return entry
    entry = os.path.expanduser(entry)
    if os.path.isabs(entry):
        return entry
    in_app = os.path.join(_GA_ROOT, entry)
    if os.path.isdir(in_app):
        return in_app
    return os.path.join(_SUPPORT_ROOT, entry)


def _effective_config_path():
    """退役的 json 配置路径。真源已是 DuckDB；此函数只服务一次性导入、GA_STORAGE=json
    回滚，以及尚未迁移的 conductor。"""
    if os.path.isfile(_CONFIG_PATH):
        return _CONFIG_PATH
    seed = os.path.join(_GA_ROOT, "plugins", "skills_config.json")
    if os.path.isfile(seed):
        return seed
    return _CONFIG_PATH


_MIGRATE_STASH = ".skills_config_migrate.json"


def _migrate_legacy_config():
    """升级迁移：把旧运行态 app/plugins/skills_config.json 里用户自定义的 plugin_dirs/
    skills_roots（Rust 在 clone 前 stash 到 <support_root>/.skills_config_migrate.json）合并进
    持久配置（现为 DuckDB，经 ga_config）。

    - 只迁移「解析后存在 或 已被新种子覆盖」的条目，丢弃失效的开发机绝对路径，避免污染；
    - 迁移后的条目优先写成相对路径（落在 ga_root/support_root 内），保证可移植；
    - 幂等：stash 用完即删，重复调用无害。
    dev 模式无 stash，直接返回。
    """
    import sys as _sys
    _dbg = (lambda *a: print("[migrate]", *_a, file=_sys.stderr, flush=True)) \
        if os.environ.get("GA_MIGRATE_DEBUG") else (lambda *a: None)
    _dbg("IS_BUNDLE=", _IS_BUNDLE, "GA_ROOT=", _GA_ROOT, "SUPPORT_ROOT=", _SUPPORT_ROOT)
    # stash 由 Rust 写在 app-support 根（clone 目标 dst_app 的父目录）。
    # ga_root 可能因解析差异指向 .app bundle 或 app-support，故多候选根查找。
    cand = []
    if _IS_BUNDLE:
        cand.append(_SUPPORT_ROOT)
    cand.append(os.path.dirname(_GA_ROOT))
    cand.append(os.path.dirname(os.path.dirname(_PLUGIN_DIR)))
    stash = None
    for c in cand:
        if c and os.path.isfile(os.path.join(c, _MIGRATE_STASH)):
            stash = os.path.join(c, _MIGRATE_STASH)
            break
    _dbg("stash=", stash)
    if not stash:
        return
    try:
        with open(stash, "r", encoding="utf-8") as f:
            legacy = json.load(f)
    except Exception:
        legacy = {}

    # 新种子（clone 后 app/plugins/skills_config.json）已解析集合，用于去重
    seed_path = os.path.join(_GA_ROOT, "plugins", "skills_config.json")
    seed = {}
    if os.path.isfile(seed_path):
        try:
            with open(seed_path, "r", encoding="utf-8") as f:
                seed = json.load(f)
        except Exception:
            seed = {}
    seed_roots = {os.path.realpath(_resolve_root(r)) for r in seed.get("skills_roots", []) if r}
    seed_dirs = {os.path.realpath(_resolve_root(d)) for d in seed.get("plugin_dirs", []) if d}

    # 当前持久配置（DuckDB）；首启（库内为空）以新种子为基底，再叠加用户旧条目
    base = ga_config.skills_config()
    if not base.get("skills_roots") and not base.get("plugin_dirs"):
        base = dict(seed) or base
    cur_roots = {os.path.realpath(_resolve_root(r)) for r in base.get("skills_roots", []) if r}
    cur_dirs = {os.path.realpath(_resolve_root(d)) for d in base.get("plugin_dirs", []) if d}

    def _normalize(entry):
        entry = os.path.expanduser(entry)
        if not entry:
            return None
        if not os.path.isabs(entry):
            return entry  # 相对路径，保持可移植
        ab = os.path.realpath(entry)
        for root in (_GA_ROOT, _SUPPORT_ROOT):
            try:
                rel = os.path.relpath(ab, root)
            except Exception:
                continue
            if rel and not rel.startswith(".."):
                return rel  # 落在 ga_root/support_root 内 → 写成相对路径
        return None  # 外部绝对路径（多为开发机残留）→ 丢弃，不污染持久配置

    new_roots = list(base.get("skills_roots", []))
    new_dirs = list(base.get("plugin_dirs", []))
    added = False
    for r in legacy.get("skills_roots", []):
        ra = os.path.realpath(_resolve_root(r)) if r else None
        if not ra or ra in seed_roots or ra in cur_roots or not os.path.exists(ra):
            continue  # 已覆盖或失效，跳过
        nr = _normalize(r)
        if nr and nr not in new_roots:
            new_roots.append(nr)
            added = True
    for d in legacy.get("plugin_dirs", []):
        da = os.path.realpath(_resolve_root(d)) if d else None
        if not da or da in seed_dirs or da in cur_dirs or not os.path.exists(da):
            continue
        nd = _normalize(d)
        if nd and nd not in new_dirs:
            new_dirs.append(nd)
            added = True

    if added:
        base["skills_roots"] = new_roots
        base["plugin_dirs"] = new_dirs
        base.setdefault("enabled", True)
        ga_config.save_skills_config(base)
    # stash 用完即删，避免重复迁移
    try:
        os.remove(stash)
    except Exception:
        pass


def _load_config():
    """读取配置；优先级：env > DuckDB。返回 roots 列表或 None。"""
    _migrate_legacy_config()
    # 1. 环境变量 GA_SKILLS_ROOTS（冒号分隔）
    env_roots = os.environ.get("GA_SKILLS_ROOTS", "").strip()
    if env_roots:
        return [r.strip() for r in env_roots.split(":") if r.strip()]
    # 2. 存储层
    cfg = ga_config.skills_config()
    if not cfg.get("enabled", False):
        return None
    roots = [_resolve_root(r) for r in cfg.get("skills_roots", [])]
    return roots or None


def _parse_frontmatter(text):
    """解析 SKILL.md 的 YAML frontmatter，提取 name / description / version / tags / category /
    permission_level / argument_hint / license。

    仅做轻量行级解析（不依赖 PyYAML），向后兼容：缺失字段返回 None。
    """
    m = re.match(r"^---\s*\n(.*?)\n---", text, re.DOTALL)
    if not m:
        return None
    body = m.group(1)
    name = None
    desc_lines = []
    in_desc = False
    version = None
    tags = []
    in_tags = False
    category = None
    permission_level = None
    argument_hint = None
    license_ = None
    for line in body.splitlines():
        # description 多行块结束判定
        if in_desc:
            if line.strip() == "" or not line.startswith((" ", "\t")):
                in_desc = False
            else:
                desc_lines.append(line.strip())
                continue
        # tags 多行列表块
        if in_tags:
            tm = re.match(r"^-\s+(.+)", line.strip())
            if tm:
                tags.append(tm.group(1).strip().strip('"\''))
                continue
            in_tags = False
        if line.startswith("name:"):
            name = line.split(":", 1)[1].strip() or None
        elif line.startswith("description:"):
            rest = line.split(":", 1)[1].strip()
            if rest in (">", "|"):
                in_desc = True
            elif rest:
                desc_lines.append(rest)
        elif line.startswith("version:"):
            version = line.split(":", 1)[1].strip() or None
        elif line.startswith("category:"):
            category = line.split(":", 1)[1].strip() or None
        elif line.startswith("permission-level:") or line.startswith("permission_level:"):
            permission_level = line.split(":", 1)[1].strip() or None
        elif line.startswith("argument-hint:") or line.startswith("argument_hint:"):
            argument_hint = line.split(":", 1)[1].strip().strip('"\'') or None
        elif line.startswith("license:"):
            license_ = line.split(":", 1)[1].strip() or None
        elif line.startswith("tags:"):
            rest = line.split(":", 1)[1].strip()
            if rest in (">", "|", ""):
                in_tags = True
            else:
                # inline list: [a, b, c] 或 a, b, c
                items = rest.strip("[]").split(",")
                tags = [t.strip().strip('"\'') for t in items if t.strip()]
    desc = " ".join(desc_lines).strip()
    return {
        "name": name,
        "description": desc,
        "version": version,
        "tags": tags,
        "category": category,
        "permission_level": permission_level,
        "argument_hint": argument_hint,
        "license": license_,
    }


def _list_skill_dirs(roots):
    """枚举 (skill_dir, SKILL.md, root, entry)。兼容两种目录结构：
      A) root/<name>/SKILL.md          （根目录直接含 skill 目录）
      B) root/skills/<name>/SKILL.md   （根目录下有 skills/ 子目录，如 flink-skills 仓库）
    """
    out = []
    for root in roots:
        root = os.path.expanduser(root)
        if not os.path.isdir(root):
            continue
        scan_dirs = [root]
        skills_subdir = os.path.join(root, "skills")
        if os.path.isdir(skills_subdir):
            scan_dirs.append(skills_subdir)
        for scan_dir in scan_dirs:
            for entry in sorted(os.listdir(scan_dir)):
                if entry.startswith("."):
                    continue
                d = os.path.join(scan_dir, entry)
                md = os.path.join(d, "SKILL.md")
                if os.path.isfile(md):
                    out.append((d, md, root, entry))
    return out


def _scan_skill(skill_dir, skill_md, root, entry):
    try:
        with open(skill_md, "r", encoding="utf-8") as f:
            text = f.read()
    except Exception:
        return None
    meta = _parse_frontmatter(text)
    if not meta or not meta["name"]:
        meta = {"name": entry, "description": "", "version": None,
                "tags": [], "category": None, "permission_level": None,
                "argument_hint": None, "license": None}
    meta["path"] = skill_md
    meta["root"] = root
    meta["dir"] = skill_dir
    scripts_dir = os.path.join(skill_dir, "scripts")
    meta["has_scripts"] = os.path.isdir(scripts_dir) and any(
        fn.endswith(".js") or fn.endswith(".py") for fn in os.listdir(scripts_dir)
    ) if os.path.isdir(scripts_dir) else False
    return meta


def _discover_skills(roots):
    """扫描各 skills 根目录，返回 skill 列表。frontmatter 解析结果缓存在 skill_meta 表，
    按 SKILL.md mtime 失效——未改动的 skill 不再重复读盘解析。"""
    entries = _list_skill_dirs(roots)
    mtimes = {}
    for d, md, _r, _e in entries:
        try:
            mtimes[d] = os.path.getmtime(md)
        except OSError:
            pass
    cached = ga_config.meta_get(ga_config.SKILLS, mtimes)
    skills, fresh = [], []
    for d, md, root, entry in entries:
        meta = cached.get(d)
        if meta is None:
            meta = _scan_skill(d, md, root, entry)
            if meta is None:
                continue
            if d in mtimes:
                fresh.append({"dir": d, "root": root, "name": meta["name"],
                              "meta": meta, "mtime": mtimes[d]})
        skills.append(meta)
    if fresh:
        ga_config.meta_put(ga_config.SKILLS, fresh)
        ga_config.meta_prune(ga_config.SKILLS, list(mtimes))
    return skills


def _truncate(s, n=120):
    return s if len(s) <= n else s[:n] + "..."


def _load_project_skills(project_name):
    """读取项目绑定的 skill name 列表。返回 None=未绑定(全局), list=已过滤。"""
    if not project_name:
        return None
    try:
        import plugins.project_mode as _pm
        pdir = _pm._project_dir(project_name)
    except Exception:
        return None
    sp = os.path.join(pdir, '.skills.json')
    if not os.path.isfile(sp):
        return None
    try:
        import json as _json
        data = _json.load(open(sp, encoding='utf-8'))
        if isinstance(data, list) and data:
            return [str(s).strip() for s in data if str(s).strip()]
    except Exception:
        pass
    return None


def _build_injection(project_name=None):
    """构建注入文本。带缓存：配置版本戳不变则复用。
    若 project_name 指定且该项目有 .skills.json，则只注入该项目启用的 skill。"""
    global _injection_cache
    allowed = _load_project_skills(project_name)
    cache_key = (ga_config.config_rev(), project_name, tuple(allowed) if allowed else None)
    if _injection_cache and _injection_cache[0] == cache_key:
        return _injection_cache[1]

    roots = _load_config()
    if not roots:
        _injection_cache = (cache_key, None)
        return None

    skills = _discover_skills(roots)
    if allowed is not None:
        allowed_set = set(allowed)
        skills = [sk for sk in skills if sk.get("name") in allowed_set]
    if not skills:
        _injection_cache = (cache_key, None)
        return None

    lines = [
        "\n\n---\n## 已安装的外部 Skills（Agent Skills 规范）",
        "以下 skill 可用。**当用户意图匹配某 skill 的触发场景时**：先 file_read 该 skill 的 SKILL.md 正文获取完整指令，再按指令执行。",
        "脚本运行基准：从对应 skills 根目录运行，命令形如 `cd <root> && node skills/<skill>/scripts/<脚本>.js <参数>`。",
        "认证：共用 `~/.bilibili/config`（cookie），脚本自动读取，无需手动传参。",
        "",
    ]
    for i, sk in enumerate(skills, 1):
        root_rel = sk["root"]
        script_hint = ""
        if sk["has_scripts"]:
            script_hint = " [有可执行脚本]"
        lines.append(f"### {i}. {sk['name']}{script_hint}")
        lines.append(f"- 触发场景：{_truncate(sk['description'], 200)}")
        lines.append(f"- SKILL.md：{sk['path']}")
        lines.append(f"- 根目录：{root_rel}")
        lines.append("")
    lines.append("---")
    text = "\n".join(lines)
    _injection_cache = (cache_key, text)
    return text


if hooks:
    @hooks.register("agent_before")
    def inject_skills_index(ctx):
        """每个用户轮起始时，把 skill 索引追加到 user message。"""
        project_name = None
        try:
            import plugins.project_mode as _pm
            project_name = _pm._active_project(ctx)
        except Exception:
            pass
        text = _build_injection(project_name)
        if not text:
            return
        um = next((m for m in reversed(ctx.get("messages") or [])
                   if isinstance(m, dict) and m.get("role") == "user"), None)
        if um is None:
            return
        content = um.get("content")
        if isinstance(content, str):
            um["content"] = content + text
        elif isinstance(content, list):
            content.append({"type": "text", "text": text})
