"""Plugin Loader — 加载 Claude Code 风格的 Plugin（自包含目录 + .claude-plugin/plugin.json）。

第一层（plugin 容器骨架 + skills 复用）：
  - 发现 plugin 目录，解析 .claude-plugin/plugin.json 清单（name/description/version）
  - 扫描每个 plugin 的 skills/<name>/SKILL.md，namespace 化为 plugin-name:skill-name
  - agent_before hook：注入 plugin skill 索引（与 skills_loader 的裸 skills 索引分列）
  - 公开 API 供 slash_cmds 注册 /plugin:skill 命令并 render 正文（替换 $ARGUMENTS）

配置（优先级从高到低）：
  1. 环境变量 GA_PLUGIN_DIRS：冒号分隔的 plugin 根目录列表，设置即启用
  2. plugins/skills_config.json 的 "plugin_dirs" 字段（数组）
若均不存在，默认不注入（零干扰，与 skills_loader 一致）。

plugin 根目录可为：
  A) 单个 plugin 目录（自身含 .claude-plugin/plugin.json）
  B) 容器目录（其下每个子目录是一个 plugin，各含 .claude-plugin/plugin.json）

与 skills_loader 的关系：
  - skills_loader 处理"裸 skills"（GA 已有 Agent Skills 规范，无 plugin 清单）——保持不变
  - plugin_loader 处理"打包成 plugin 的 skills"（带清单，namespace 化）——本插件新增
  两者独立、互不干扰，各自注入一段索引。
"""

import os
import re
import json
import threading
import subprocess

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
_injection_cache = None   # (key, text)
_plugin_cache = None      # (key, plugins_list)


def _resolve_root(entry):
    """把 skills_config.json 里的 plugin_dir 解析为绝对路径。

    - 绝对路径：原样返回（兼容旧配置）。
    - 相对路径：优先按 _GA_ROOT 解析（自带 skills_external/... 在 app/ 内）；
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
    """退役的 json 配置路径。真源已是 DuckDB；仅一次性导入与 GA_STORAGE=json 回滚会用到。"""
    if os.path.isfile(_CONFIG_PATH):
        return _CONFIG_PATH
    seed = os.path.join(_GA_ROOT, "plugins", "skills_config.json")
    if os.path.isfile(seed):
        return seed
    return _CONFIG_PATH


def _cache_key():
    return (ga_config.config_rev(), os.environ.get("GA_PLUGIN_DIRS", ""))


def _load_plugin_dirs():
    """读取 plugin 目录列表。优先级：env > DuckDB。返回 dirs 列表或 None。"""
    # 升级迁移：把旧运行态里用户自定义的 plugin_dirs 合并进持久配置（幂等）
    try:
        from plugins.skills_loader import _migrate_legacy_config
        _migrate_legacy_config()
    except Exception:
        pass
    env_dirs = os.environ.get("GA_PLUGIN_DIRS", "").strip()
    if env_dirs:
        return [d.strip() for d in env_dirs.split(":") if d.strip()]
    dirs = [_resolve_root(d) for d in ga_config.skills_config().get("plugin_dirs", [])]
    return dirs or None


def _parse_frontmatter(text):
    """解析 SKILL.md 的 YAML frontmatter，提取 name / description / disable-model-invocation。

    与 skills_loader._parse_frontmatter 一致，额外提取 disable-model-invocation。
    """
    m = re.match(r"^---\s*\n(.*?)\n---", text, re.DOTALL)
    if not m:
        return None
    body = m.group(1)
    name = None
    desc_lines = []
    in_desc = False
    disable_model_invocation = False
    for line in body.splitlines():
        if line.startswith("name:"):
            name = line.split(":", 1)[1].strip()
            in_desc = False
        elif line.startswith("description:"):
            rest = line.split(":", 1)[1].strip()
            if rest in (">", "|"):
                in_desc = True
            elif rest:
                desc_lines.append(rest)
                in_desc = False
        elif line.startswith("disable-model-invocation:"):
            val = line.split(":", 1)[1].strip().lower()
            disable_model_invocation = val in ("true", "yes", "1")
            in_desc = False
        elif in_desc:
            if line.strip() == "" or not line.startswith((" ", "\t")):
                in_desc = False
            else:
                desc_lines.append(line.strip())
    desc = " ".join(desc_lines).strip()
    return {
        "name": name,
        "description": desc,
        "disable_model_invocation": disable_model_invocation,
    }


def _parse_manifest(plugin_dir):
    """解析 .claude-plugin/plugin.json，返回 {name, description, version, dir} 或 None。"""
    manifest_path = os.path.join(plugin_dir, ".claude-plugin", "plugin.json")
    if not os.path.isfile(manifest_path):
        return None
    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return None
    name = (data.get("name") or os.path.basename(plugin_dir.rstrip("/")) or "").strip()
    if not name:
        return None
    return {
        "name": name,
        "description": data.get("description", "") or "",
        "version": data.get("version", "") or "",
        "dir": plugin_dir,
        "default_enabled": bool(data.get("defaultEnabled", True)),
        "user_config_schema": data.get("userConfig") if isinstance(data.get("userConfig"), dict) else {},
    }


def _plugin_id(name):
    """标准化 plugin id：非 [a-zA-Z0-9_-] 替换为 -。用于 CLAUDE_PLUGIN_DATA 目录与配置存储键。"""
    return re.sub(r"[^a-zA-Z0-9_-]", "-", name or "")


def _plugin_data_dir(name):
    """${CLAUDE_PLUGIN_DATA} 解析目录：~/.claude/plugins/data/{id}/，自动创建（fail-open）。
    若创建失败（如权限受限），stderr 警告但仍返回路径——由调用方决定是否可用。"""
    import sys
    data_root = os.path.join(os.path.expanduser("~"), ".claude", "plugins", "data")
    d = os.path.join(data_root, _plugin_id(name))
    try:
        os.makedirs(d, exist_ok=True)
    except Exception as e:
        print(f"[plugin] WARN: cannot create CLAUDE_PLUGIN_DATA dir {d}: {e}", file=sys.stderr)
    return d


def _user_config_path():
    """退役的 json 存储位置（现为 DuckDB plugin_configs 表）。保留用于一次性导入、
    GA_STORAGE=json 回滚，以及提示信息里指明来源。"""
    return os.path.join(os.path.dirname(_CONFIG_PATH), "plugin_configs.json")


def _migrate_legacy_plugin_configs():
    """升级迁移：把旧运行态 app/plugins/plugin_configs.json（Rust 在 clone 前 stash 到
    <support_root>/.plugin_configs_migrate.json）合并进持久位置 <support_root>/plugin_configs.json。

    内容是按 plugin-id 聚合的用户选项值；合并规则：持久已有 key 优先，仅补充旧配置里缺失的 key。
    幂等：stash 用完即删。dev 模式无 stash，直接返回。
    """
    import sys as _sys
    _dbg = (lambda *a: print("[migrate-pcfg]", *_a, file=_sys.stderr, flush=True)) \
        if os.environ.get("GA_MIGRATE_DEBUG") else (lambda *a: None)
    _dbg("IS_BUNDLE=", _IS_BUNDLE, "GA_ROOT=", _GA_ROOT, "SUPPORT_ROOT=", _SUPPORT_ROOT)
    cand = []
    if _IS_BUNDLE:
        cand.append(_SUPPORT_ROOT)
    cand.append(os.path.dirname(_GA_ROOT))
    cand.append(os.path.dirname(os.path.dirname(_PLUGIN_DIR)))
    stash = None
    for c in cand:
        if c and os.path.isfile(os.path.join(c, ".plugin_configs_migrate.json")):
            stash = os.path.join(c, ".plugin_configs_migrate.json")
            break
    _dbg("stash=", stash)
    if not stash:
        return
    try:
        with open(stash, "r", encoding="utf-8") as f:
            legacy = json.load(f)
    except Exception:
        legacy = {}
    if not isinstance(legacy, dict):
        legacy = {}
    legacy_cfgs = legacy.get("pluginConfigs") if isinstance(legacy.get("pluginConfigs"), dict) else {}
    cur = ga_config.plugin_configs()
    added = False
    for pid, v in legacy_cfgs.items():
        opts = v.get("options") if isinstance(v, dict) else None
        if pid not in cur and isinstance(opts, dict):
            cur[pid] = opts
            added = True
    if added:
        ga_config.save_plugin_configs(cur)
    try:
        os.remove(stash)
    except Exception:
        pass


def _load_user_configs():
    """读取所有 plugin 的用户配置 options，返回 {plugin_id: {KEY: val}}。fail-open。"""
    _migrate_legacy_plugin_configs()
    try:
        return ga_config.plugin_configs()
    except Exception:
        return {}


def _resolve_user_config(plugin_name, schema, stored):
    """合并 schema.default + 存储值，得到最终 options dict（4.1.4 采集）。
    - required 字段缺失（无 default 且无存储值）→ stderr 警告 + 跳过该字段（fail-open，不阻断 plugin 加载）
    - 用户选 E：敏感值与普通值统一明文处理（sensitive 标记不影响存储路径）
    """
    import sys
    resolved = {}
    for key, spec in (schema or {}).items():
        if not isinstance(spec, dict):
            continue
        val = stored.get(key)
        if (val is None or val == "") and "default" in spec:
            val = spec["default"]
        if val is None or val == "":
            if spec.get("required"):
                print(f"[plugin] {plugin_name}: required userConfig '{key}' missing — "
                      f"set it in {_user_config_path()}", file=sys.stderr)
            continue
        resolved[key] = val
    return resolved


def _substitute_vars(v, plugin_dir, plugin_id, user_cfg, project_dir=None):
    """CC plugin 变量替换（递归作用于 str/list/dict），规范 plugin-ref L400/L454-458/L232。

    支持 5 类变量（按序解析，后者不覆盖已替换结果）：
      ${CLAUDE_PLUGIN_ROOT}   -> plugin_dir 绝对路径
      ${CLAUDE_PLUGIN_DATA}   -> _plugin_data_dir(plugin_id)（含创建+warn+fail-open）
      ${CLAUDE_PROJECT_DIR}   -> project_dir or os.getcwd()
      ${user_config.KEY}      -> str(user_cfg.get(KEY))，缺失为空串
      ${ENV_VAR}              -> os.environ.get(ENV_VAR, '')（兜底，仅匹配纯名无点）
    """
    _uc_pat = re.compile(r"\$\{user_config\.([A-Za-z0-9_]+)\}")
    _env_pat = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

    def _sub_str(s):
        if not isinstance(s, str) or "${" not in s:
            return s
        s = s.replace("${CLAUDE_PLUGIN_ROOT}", str(plugin_dir))
        s = s.replace("${CLAUDE_PLUGIN_DATA}", str(_plugin_data_dir(plugin_id)))
        s = s.replace("${CLAUDE_PROJECT_DIR}", str(project_dir or os.getcwd()))

        def _uc_repl(m):
            val = user_cfg.get(m.group(1)) if isinstance(user_cfg, dict) else None
            return "" if val is None else str(val)
        s = _uc_pat.sub(_uc_repl, s)
        s = _env_pat.sub(lambda m: os.environ.get(m.group(1), ""), s)
        return s

    if isinstance(v, str):
        return _sub_str(v)
    if isinstance(v, list):
        return [_substitute_vars(x, plugin_dir, plugin_id, user_cfg, project_dir) for x in v]
    if isinstance(v, dict):
        return {k: _substitute_vars(val, plugin_dir, plugin_id, user_cfg, project_dir)
                for k, val in v.items()}
    return v


def _build_plugin_env(plugin_dir, plugin_id, user_cfg, project_dir=None):
    """构造 CC plugin subprocess 环境变量（规范 plugin-ref L400/L454-458）。

    基于 os.environ 复制（不污染当前进程），注入：
      CLAUDE_PLUGIN_ROOT / CLAUDE_PLUGIN_DATA / CLAUDE_PROJECT_DIR
      CLAUDE_PLUGIN_OPTION_<KEY> = str(user_cfg[KEY])（每个 user_config 字段）
    """
    env = dict(os.environ)
    env["CLAUDE_PLUGIN_ROOT"] = str(plugin_dir)
    env["CLAUDE_PLUGIN_DATA"] = str(_plugin_data_dir(plugin_id))
    env["CLAUDE_PROJECT_DIR"] = str(project_dir or os.getcwd())
    if isinstance(user_cfg, dict):
        for k, val in user_cfg.items():
            env[f"CLAUDE_PLUGIN_OPTION_{k}"] = "" if val is None else str(val)
    return env


def _discover_plugin_skills(plugin_dir, plugin_name):
    """扫描 plugin_dir/skills/<name>/SKILL.md，返回 skill 列表（带 namespace）。"""
    skills = []
    skills_root = os.path.join(plugin_dir, "skills")
    if not os.path.isdir(skills_root):
        return skills
    for entry in sorted(os.listdir(skills_root)):
        if entry.startswith("."):
            continue
        skill_md = os.path.join(skills_root, entry, "SKILL.md")
        if not os.path.isfile(skill_md):
            continue
        try:
            with open(skill_md, "r", encoding="utf-8") as f:
                text = f.read()
        except Exception:
            continue
        meta = _parse_frontmatter(text)
        if not meta or not meta["name"]:
            skill_name = entry
        else:
            skill_name = meta["name"]
        has_args = "$ARGUMENTS" in text
        skills.append({
            "plugin_name": plugin_name,
            "skill_name": skill_name,
            "ns_name": f"{plugin_name}:{skill_name}",
            "path": skill_md,
            "dir": os.path.join(skills_root, entry),
            "description": meta["description"] if meta else "",
            "disable_model_invocation": meta["disable_model_invocation"] if meta else False,
            "has_args": has_args,
        })
    return skills


def _discover_plugins():
    """扫描 plugin_dirs，返回 plugin 列表（每个含 skills）。"""
    dirs = _load_plugin_dirs()
    if not dirs:
        return []
    stored_cfg = _load_user_configs()
    plugins = []
    for d in dirs:
        d = os.path.expanduser(d)
        if not os.path.isdir(d):
            continue
        manifest = _parse_manifest(d)
        if manifest:
            # A) d 自身是 plugin
            plugin_dirs_to_scan = [d]
        else:
            # B) d 是容器目录，其下每个子目录尝试当 plugin
            plugin_dirs_to_scan = []
            try:
                for entry in sorted(os.listdir(d)):
                    if entry.startswith("."):
                        continue
                    sub = os.path.join(d, entry)
                    if os.path.isdir(sub) and _parse_manifest(sub):
                        plugin_dirs_to_scan.append(sub)
            except Exception:
                continue
        for pdir in plugin_dirs_to_scan:
            m = _parse_manifest(pdir)
            if not m:
                continue
            pname = m["name"]
            schema = m.get("user_config_schema") or {}
            user_cfg = _resolve_user_config(pname, schema, stored_cfg.get(_plugin_id(pname), {})) if schema else {}
            skills = _discover_plugin_skills(pdir, pname)
            agents = _discover_plugin_agents(pdir, pname)
            hooks = _discover_plugin_hooks(pdir, pname)
            mcp = _discover_plugin_mcp(pdir, pname, user_cfg)
            lsp = _discover_plugin_lsp(pdir, pname, user_cfg)
            monitors = _discover_plugin_monitors(pdir, pname, user_cfg)
            plugins.append({
                "name": pname,
                "dir": pdir,
                "description": m["description"],
                "version": m["version"],
                "default_enabled": m["default_enabled"],
                "user_config": user_cfg,
                "skills": skills,
                "agents": agents,
                "hooks": hooks,
                "mcp": mcp,
                "lsp": lsp,
                "monitors": monitors,
            })
    return plugins


def _parse_agent_frontmatter(text):
    """解析 agents/*.md 的 YAML frontmatter，提取 Claude Code plugin agent 字段。

    支持字段：name(必)、description(必)、model、effort、maxTurns、tools、disallowedTools、
    skills、background、isolation。description 支持单行或 > / | 多行（与 _parse_frontmatter 一致）。
    """
    m = re.match(r"^---\s*\n(.*?)\n---", text, re.DOTALL)
    if not m:
        return None
    body = m.group(1)
    fields = {}
    desc_lines = []
    in_desc = False
    for line in body.splitlines():
        if line.startswith("description:"):
            rest = line.split(":", 1)[1].strip()
            if rest in (">", "|"):
                in_desc = True
            elif rest:
                desc_lines.append(rest)
                in_desc = False
            continue
        if in_desc:
            if line.strip() == "" or not line.startswith((" ", "\t")):
                in_desc = False
            else:
                desc_lines.append(line.strip())
                continue
        if ":" in line and not line.startswith((" ", "\t")):
            key, _, val = line.partition(":")
            key = key.strip()
            val = val.strip()
            if key in ("name", "model", "effort", "background", "isolation"):
                fields[key] = val
            elif key == "maxTurns":
                try:
                    fields["maxTurns"] = int(val)
                except Exception:
                    fields["maxTurns"] = val
            elif key in ("tools", "disallowedTools", "skills"):
                fields[key] = [x.strip() for x in val.split(",") if x.strip()] if val else []
    fields["description"] = " ".join(desc_lines).strip()
    return fields


def _discover_plugin_agents(plugin_dir, plugin_name):
    """扫描 plugin_dir/agents/*.md，返回 agent 列表（带 namespace）。

    与 _discover_plugin_skills 对称：解析 frontmatter 的 name/description/tools 等，
    namespace 化为 plugin-name:agent-name。
    """
    agents = []
    agents_root = os.path.join(plugin_dir, "agents")
    if not os.path.isdir(agents_root):
        return agents
    for entry in sorted(os.listdir(agents_root)):
        if entry.startswith(".") or not entry.endswith(".md"):
            continue
        agent_md = os.path.join(agents_root, entry)
        if not os.path.isfile(agent_md):
            continue
        try:
            with open(agent_md, "r", encoding="utf-8") as f:
                text = f.read()
        except Exception:
            continue
        meta = _parse_agent_frontmatter(text)
        if not meta or not meta.get("name"):
            agent_name = entry[:-3]
        else:
            agent_name = meta["name"]
        agents.append({
            "plugin_name": plugin_name,
            "agent_name": agent_name,
            "ns_name": f"{plugin_name}:{agent_name}",
            "path": agent_md,
            "description": meta.get("description", "") if meta else "",
            "model": meta.get("model", "") if meta else "",
            "effort": meta.get("effort", "") if meta else "",
            "maxTurns": meta.get("maxTurns", "") if meta else "",
            "tools": meta.get("tools", []) if meta else [],
            "disallowedTools": meta.get("disallowedTools", []) if meta else [],
        })
    return agents


def _discover_plugin_hooks(plugin_dir, plugin_name):
    """扫描 plugin_dir/hooks/hooks.json，返回扁平化的 hook 配置列表。

    与 _discover_plugin_skills/agents 对称。CC hooks.json 格式:
      {"hooks": {"PreToolUse": [{"matcher": "Write|Edit",
        "hooks": [{"type":"command","command":"${CLAUDE_PLUGIN_ROOT}/x.sh"}]}]}}
    本实现仅支持 type=command（shell 执行），其余 type 记录后跳过。
    返回 [{event, matcher, type, command, plugin_name, plugin_dir}, ...]
    """
    hooks_json = os.path.join(plugin_dir, "hooks", "hooks.json")
    if not os.path.isfile(hooks_json):
        return []
    try:
        with open(hooks_json, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        return []
    out = []
    events = cfg.get("hooks", {}) or {}
    if not isinstance(events, dict):
        return []
    for event, groups in events.items():
        if not isinstance(groups, list):
            continue
        for grp in groups:
            if not isinstance(grp, dict):
                continue
            matcher = grp.get("matcher", "")
            for hk in grp.get("hooks", []) or []:
                if not isinstance(hk, dict):
                    continue
                out.append({
                    "event": event,
                    "matcher": matcher,
                    "type": hk.get("type", "command"),
                    "command": hk.get("command", ""),
                    "plugin_name": plugin_name,
                    "plugin_dir": plugin_dir,
                })
    return out


def _discover_plugin_mcp(plugin_dir, plugin_name, user_cfg=None):
    """解析 plugin 的 MCP server 配置（.mcp.json 或 plugin.json inline mcp）。

    CC plugin MCP 配置位置（优先级）：
      1. plugin 根的 .mcp.json（主流）：{"mcpServers": {name: {command,args,env,cwd} | {url,type}}}
      2. .claude-plugin/plugin.json 的 mcp 字段（inline，格式同上）
    替换 ${CLAUDE_PLUGIN_ROOT} -> plugin_dir 绝对路径（递归作用于 str/list/dict）。
    transport 判断：有 command -> stdio；有 url + type=sse -> sse；有 url -> http。
    返回 {server_name: {<原始字段>, transport, plugin_name, plugin_dir}}，无配置返回 {}。
    fail-open：解析异常/格式不符均返回 {}，不影响 plugin 加载。
    """
    cfg = None
    mcp_json = os.path.join(plugin_dir, ".mcp.json")
    if os.path.isfile(mcp_json):
        try:
            with open(mcp_json, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            cfg = None
    if cfg is None:
        manifest_path = os.path.join(plugin_dir, ".claude-plugin", "plugin.json")
        if os.path.isfile(manifest_path):
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    mdata = json.load(f)
                inline = mdata.get("mcp")
                if isinstance(inline, dict):
                    cfg = inline if "mcpServers" in inline else {"mcpServers": inline}
                elif isinstance(mdata.get("mcpServers"), dict):
                    cfg = {"mcpServers": mdata["mcpServers"]}
            except Exception:
                pass
    if not isinstance(cfg, dict):
        return {}
    servers = cfg.get("mcpServers", {})
    if not isinstance(servers, dict):
        return {}

    plugin_id = _plugin_id(plugin_name)

    def _sub(v):
        return _substitute_vars(v, plugin_dir, plugin_id, user_cfg)

    out = {}
    for sname, sconf in servers.items():
        if not isinstance(sconf, dict):
            continue
        sc = _sub(sconf)
        if "command" in sc:
            sc["transport"] = "stdio"
        elif "url" in sc:
            t = str(sc.get("type", "")).lower()
            sc["transport"] = "sse" if t == "sse" else "http"
        else:
            continue
        sc["plugin_name"] = plugin_name
        sc["plugin_dir"] = plugin_dir
        out[sname] = sc
    return out


def _discover_plugin_lsp(plugin_dir, plugin_name, user_cfg=None):
    """解析 plugin 的 LSP server 配置（.lsp.json）。

    配置格式：
      {"command": "pylsp", "args": [], "env": {},
       "extensionToLanguage": {".py": "python"},
       "workspaceFolder": "...", "initializationOptions": {},
       "settings": {}, "startupTimeout": 30, "maxRestarts": 3, "diagnostics": true}
    也支持 {"lspServers": {name: config}} 多 server 格式。
    返回 {server_name: config}，无配置返回 {}。fail-open。
    """
    lsp_json = os.path.join(plugin_dir, ".lsp.json")
    if not os.path.isfile(lsp_json):
        return {}
    try:
        with open(lsp_json, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        return {}
    if not isinstance(cfg, dict):
        return {}

    plugin_id = _plugin_id(plugin_name)

    def _sub(v):
        return _substitute_vars(v, plugin_dir, plugin_id, user_cfg)

    # 多 server 格式: {"lspServers": {name: config}}
    if "lspServers" in cfg and isinstance(cfg["lspServers"], dict):
        out = {}
        for sname, sconf in cfg["lspServers"].items():
            if not isinstance(sconf, dict) or "command" not in sconf:
                continue
            sc = _sub(sconf)
            sc["plugin_name"] = plugin_name
            sc["plugin_dir"] = plugin_dir
            out[sname] = sc
        return out
    # 单 server 格式: 整个文件就是一个 config
    if "command" in cfg:
        sc = _sub(cfg)
        sc["plugin_name"] = plugin_name
        sc["plugin_dir"] = plugin_dir
        return {plugin_name: sc}
    return {}


def _discover_plugin_monitors(plugin_dir, plugin_name, user_cfg=None):
    """解析 plugin 的 background monitor 配置（4.2）。

    CC plugin monitor 配置位置（优先级）：
      1. plugin 根的 monitors/monitors.json（主流）：[{name, command, description, when?}]
      2. .claude-plugin/plugin.json 的 experimental.monitors（inline array，或相对路径 string）
    command/description 替换变量（同 MCP）：${CLAUDE_PLUGIN_ROOT} 等。
    返回 [{name, command, description, when, plugin_name, plugin_dir}]，无配置返回 []。
    fail-open：解析异常/格式不符均返回 []，不影响 plugin 加载。
    name 在 plugin 内去重（防 reload 重复进程）。
    """
    cfg_list = None
    mon_json = os.path.join(plugin_dir, "monitors", "monitors.json")
    if os.path.isfile(mon_json):
        try:
            with open(mon_json, "r", encoding="utf-8") as f:
                cfg_list = json.load(f)
        except Exception:
            cfg_list = None
    if cfg_list is None:
        manifest_path = os.path.join(plugin_dir, ".claude-plugin", "plugin.json")
        if os.path.isfile(manifest_path):
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    mdata = json.load(f)
                inline = (mdata.get("experimental") or {}).get("monitors")
                if isinstance(inline, str):
                    p = os.path.join(plugin_dir, inline)
                    if os.path.isfile(p):
                        with open(p, "r", encoding="utf-8") as f2:
                            cfg_list = json.load(f2)
                elif isinstance(inline, list):
                    cfg_list = inline
            except Exception:
                pass
    if not isinstance(cfg_list, list):
        return []

    plugin_id = _plugin_id(plugin_name)

    def _sub(v):
        return _substitute_vars(v, plugin_dir, plugin_id, user_cfg)

    out = []
    seen = set()
    for m in cfg_list:
        if not isinstance(m, dict):
            continue
        name = m.get("name")
        command = m.get("command")
        if not name or not command:
            continue
        name = str(name)
        if name in seen:
            continue
        seen.add(name)
        when = str(m.get("when", "always")) or "always"
        if when != "always" and not when.startswith("on-skill-invoke:"):
            when = "always"
        out.append({
            "name": name,
            "command": _sub(str(command)),
            "description": _sub(str(m.get("description", ""))),
            "when": when,
            "plugin_name": plugin_name,
            "plugin_dir": plugin_dir,
        })
    return out


def _get_plugins():
    """获取 plugin 列表，带缓存（配置/env 不变则复用）。"""
    global _plugin_cache
    key = _cache_key()
    if _plugin_cache and _plugin_cache[0] == key:
        return _plugin_cache[1]
    plugins = _discover_plugins()
    _plugin_cache = (key, plugins)
    return plugins


def collect_mcp_tools():
    """启动所有 plugin 的 MCP servers，返回 (tools_schema, tool_map, clients)。

    tools_schema: OpenAI function 数组，name=mcp__<plugin>__<server>__<tool>，合并到 TOOLS_SCHEMA 供 LLM 调用。
    tool_map: {full_name: (McpClient, original_tool_name)}，dispatch else 据此刻路由到对应 client。
    clients: {server_key: McpClient}，由调用方持有生命周期（agent 退出时 stop）。
    fail-open：单 server 启动/列出失败不影响其他 server，仅 stderr 警告。
    """
    try:
        from plugins.mcp_client import McpClient
    except ImportError:
        return [], {}, {}
    tools, tool_map, clients = [], {}, {}
    try:
        plugins = _get_plugins()
    except Exception:
        return tools, tool_map, clients
    for _p in plugins:
        _pn = _p.get("name", "")
        _mcp = _p.get("mcp") or {}
        if not isinstance(_mcp, dict) or not _mcp:
            continue
        for _sn, _cfg in _mcp.items():
            _key = f"{_pn}__{_sn}"
            _c = None
            try:
                try:
                    _pdir = _p.get("dir", "")
                    _pid = _plugin_id(_pn) if _pn else ""
                    _base_env = _build_plugin_env(_pdir, _pid, _p.get("user_config"))
                    _ce = _cfg.get("env")
                    if isinstance(_ce, dict):
                        _base_env.update(_ce)
                    _cfg["env"] = _base_env
                except Exception:
                    pass
                _c = McpClient(_key, _cfg)
                _c.start()
                _tl = _c.list_tools() or []
                for _t in _tl:
                    _tname = _t.get("name", "")
                    _fn = f"mcp__{_pn}__{_sn}__{_tname}"
                    _schema = _t.get("inputSchema") or {"type": "object", "properties": {}}
                    # P3: 将 Tool Annotations 附加到 description 供 LLM 感知工具行为特征
                    _desc = _t.get("description", f"MCP tool {_tname} from {_pn}/{_sn}")
                    _ann = _t.get("annotations")
                    if isinstance(_ann, dict) and _ann:
                        _hints = []
                        if _ann.get("readOnlyHint"):
                            _hints.append("read-only")
                        if _ann.get("destructiveHint"):
                            _hints.append("destructive")
                        if _ann.get("idempotentHint"):
                            _hints.append("idempotent")
                        if _ann.get("openWorldHint"):
                            _hints.append("open-world")
                        if _hints:
                            _desc += f" [{', '.join(_hints)}]"
                    tools.append({
                        "type": "function",
                        "function": {
                            "name": _fn,
                            "description": _desc,
                            "parameters": _schema,
                        },
                    })
                    tool_map[_fn] = (_c, _tname)
                clients[_key] = _c
            except Exception as _e:
                import sys
                print(f"[MCP] skip server {_key}: {_e}", file=sys.stderr)
                try:
                    if _c: _c.stop()
                except Exception:
                    pass
    # --- 全局 MCP servers（非 plugin 绑定，独立配置） ---
    _global_mcp = _load_global_mcp_config()
    for _sn, _cfg in _global_mcp.items():
        _key = f"global__{_sn}"
        if _key in clients:
            continue
        _c = None
        try:
            _c = McpClient(_key, _cfg)
            _c.start()
            _tl = _c.list_tools() or []
            for _t in _tl:
                _tname = _t.get("name", "")
                _fn = f"mcp__global__{_sn}__{_tname}"
                _schema = _t.get("inputSchema") or {"type": "object", "properties": {}}
                _desc = _t.get("description", f"MCP tool {_tname} from global/{_sn}")
                _ann = _t.get("annotations")
                if isinstance(_ann, dict) and _ann:
                    _hints = []
                    if _ann.get("readOnlyHint"): _hints.append("read-only")
                    if _ann.get("destructiveHint"): _hints.append("destructive")
                    if _ann.get("idempotentHint"): _hints.append("idempotent")
                    if _ann.get("openWorldHint"): _hints.append("open-world")
                    if _hints: _desc += f" [{', '.join(_hints)}]"
                tools.append({
                    "type": "function",
                    "function": {"name": _fn, "description": _desc, "parameters": _schema},
                })
                tool_map[_fn] = (_c, _tname)
            clients[_key] = _c
        except Exception as _e:
            import sys
            print(f"[MCP] skip global server {_key}: {_e}", file=sys.stderr)
            try:
                if _c: _c.stop()
            except Exception:
                pass
    return tools, tool_map, clients


def _load_global_mcp_config():
    """加载全局 MCP server 配置（非 plugin 绑定），来自 DuckDB mcp_servers 表。

    条目格式同 Claude Code .mcp.json：{"serverName": {command, args, env, ...}}。
    fail-open：读不到返回 {}。
    """
    try:
        servers = ga_config.mcp_servers()
    except Exception:
        return {}
    return {k: v for k, v in servers.items()
            if isinstance(v, dict) and ("command" in v or "url" in v)}


def collect_lsp_clients():
    """启动所有 plugin 的 LSP servers，返回 {server_key: LspClient}。

    fail-open：单 server 启动失败不影响其他，仅 stderr 警告。
    """
    try:
        from plugins.lsp_client import LspClient
    except ImportError:
        return {}
    clients = {}
    try:
        plugins = _get_plugins()
    except Exception:
        return clients
    for _p in plugins:
        _pn = _p.get("name", "")
        _lsp = _p.get("lsp") or {}
        if not isinstance(_lsp, dict) or not _lsp:
            continue
        for _sn, _cfg in _lsp.items():
            _key = f"{_pn}__{_sn}"
            try:
                _c = LspClient(_key, _cfg)
                _c.start()
                clients[_key] = _c
            except Exception as _e:
                import sys
                print(f"[LSP] skip server {_key}: {_e}", file=sys.stderr)
    return clients


def stop_all_lsp(lsp_clients):
    """停止所有 LSP clients。"""
    for _c in (lsp_clients or {}).values():
        try:
            _c.stop()
        except Exception:
            pass


# ─── 动态 MCP server 热加载/卸载 ───────────────────────────────────────────────

def add_mcp_server(server_name, config, agent_ref=None):
    """运行时动态添加一个 MCP server。

    Returns: (success, message, tools_added)
    """
    try:
        from plugins.mcp_client import McpClient
    except ImportError:
        return False, "mcp_client import failed", []
    _key = f"global__{server_name}"
    if agent_ref and _key in getattr(agent_ref, "mcp_clients", {}):
        return False, f"server '{server_name}' already exists", []
    _c = None
    try:
        _c = McpClient(_key, config)
        _c.start()
        _tl = _c.list_tools() or []
        new_tools, new_map = [], {}
        for _t in _tl:
            _tname = _t.get("name", "")
            _fn = f"mcp__global__{server_name}__{_tname}"
            _schema = _t.get("inputSchema") or {"type": "object", "properties": {}}
            _desc = _t.get("description", f"MCP tool {_tname} from global/{server_name}")
            new_tools.append({
                "type": "function",
                "function": {"name": _fn, "description": _desc, "parameters": _schema},
            })
            new_map[_fn] = (_c, _tname)
        if agent_ref:
            agent_ref.mcp_clients[_key] = _c
            agent_ref.mcp_tool_map.update(new_map)
            if hasattr(agent_ref, "llmclient") and hasattr(agent_ref.llmclient, "backend"):
                be = agent_ref.llmclient.backend
                if hasattr(be, "tools_schema"):
                    be.tools_schema.extend(new_tools)
        return True, f"server '{server_name}' added with {len(new_tools)} tools", [t["function"]["name"] for t in new_tools]
    except Exception as e:
        try:
            if _c: _c.stop()
        except Exception:
            pass
        return False, f"failed to start server '{server_name}': {e}", []


def remove_mcp_server(server_name, agent_ref=None):
    """运行时动态卸载一个 MCP server。

    Returns: (success, message)
    """
    _key = f"global__{server_name}"
    if not agent_ref:
        return False, "no agent_ref provided"
    _c = getattr(agent_ref, "mcp_clients", {}).pop(_key, None)
    if _c is None:
        return False, f"server '{server_name}' not found"
    _prefix = f"mcp__global__{server_name}__"
    _removed = [k for k in getattr(agent_ref, "mcp_tool_map", {}) if k.startswith(_prefix)]
    for k in _removed:
        agent_ref.mcp_tool_map.pop(k, None)
    if hasattr(agent_ref, "llmclient") and hasattr(agent_ref.llmclient, "backend"):
        be = agent_ref.llmclient.backend
        if hasattr(be, "tools_schema"):
            be.tools_schema = [t for t in be.tools_schema
                               if t.get("function", {}).get("name", "") not in _removed]
    try:
        _c.stop()
    except Exception:
        pass
    return True, f"server '{server_name}' removed ({len(_removed)} tools)"


def reload_mcp_servers(agent_ref=None):
    """重新加载所有 MCP servers（stop all → re-collect）。"""
    if agent_ref:
        for _c in list(getattr(agent_ref, "mcp_clients", {}).values()):
            try:
                _c.stop()
            except Exception:
                pass
        agent_ref.mcp_clients = {}
        agent_ref.mcp_tool_map = {}
    tools, tool_map, clients = collect_mcp_tools()
    if agent_ref:
        agent_ref.mcp_clients = clients
        agent_ref.mcp_tool_map = tool_map
        if hasattr(agent_ref, "llmclient") and hasattr(agent_ref.llmclient, "backend"):
            be = agent_ref.llmclient.backend
            if hasattr(be, "tools_schema"):
                be.tools_schema = [t for t in be.tools_schema
                                   if not t.get("function", {}).get("name", "").startswith("mcp__")]
                be.tools_schema.extend(tools)
    return tools, tool_map, clients


def run_cc_hook(event, tool_name, tool_input, **extra):
    """执行 CC plugin 的 PreToolUse/PostToolUse 等 command hooks，返回标准化决策。

    遍历已安装 plugin 的 hooks，筛选 event 匹配 + matcher 命中 tool_name 的
    type=command hook，shell 执行并解析 stdout/exitcode。CC PreToolUse 决策方式：
      exit code 2 -> block(stderr 作反馈)；stdout JSON {continue:false} -> 停止 agent；
      {decision:"block"} -> block；{hookSpecificOutput:{permissionDecision:"deny"}} -> deny；
      {hookSpecificOutput:{updatedInput:{...}}} -> 改写工具参数。
    返回 {block, reason, updated_input, stop, stop_reason}。fail-open：
      hook 执行异常/超时/非 JSON 输出均不阻断工具调用。
    """
    import subprocess
    decision = {"block": False, "reason": "", "updated_input": None,
                "stop": False, "stop_reason": ""}
    try:
        plugins = _get_plugins()
    except Exception:
        return decision
    cwd = extra.get("cwd", os.getcwd())
    for p in plugins:
        for hk in p.get("hooks", []) or []:
            if hk.get("event") != event:
                continue
            matcher = hk.get("matcher", "")
            if matcher:
                try:
                    if not re.search(matcher, tool_name):
                        continue
                except re.error:
                    continue
            if hk.get("type") != "command":
                continue
            command = hk.get("command", "")
            if not command:
                continue
            plugin_dir = hk.get("plugin_dir", "")
            _pname = p.get("name", "")
            _pid = _plugin_id(_pname) if _pname else ""
            _ucfg = p.get("user_config")
            command = _substitute_vars(command, plugin_dir, _pid, _ucfg, cwd)
            stdin_payload = {
                "hook_event_name": event,
                "tool_name": tool_name,
                "tool_input": tool_input,
                "cwd": cwd,
                "permission_mode": "default",
            }
            stdin_payload.update(extra)
            try:
                _hook_env = _build_plugin_env(plugin_dir, _pid, _ucfg, cwd)
                proc = subprocess.run(
                    command, shell=True, input=json.dumps(stdin_payload),
                    capture_output=True, text=True, timeout=30,
                    cwd=plugin_dir or None,
                    env=_hook_env,
                )
            except Exception:
                continue  # fail-open
            rc = proc.returncode
            stdout = (proc.stdout or "").strip()
            stderr = (proc.stderr or "").strip()
            if rc == 2:
                decision["block"] = True
                decision["reason"] = stderr or stdout or "blocked by hook"
                return decision
            if rc != 0:
                continue  # 其他非零退出码：fail-open
            if not stdout:
                continue  # 放行
            try:
                out = json.loads(stdout)
            except Exception:
                continue  # 非 JSON 输出：放行
            if out.get("continue") is False:
                decision["stop"] = True
                decision["stop_reason"] = out.get("stopReason", "stopped by hook")
                return decision
            if out.get("decision") == "block":
                decision["block"] = True
                decision["reason"] = out.get("reason", "blocked by hook")
                return decision
            hso = out.get("hookSpecificOutput") or {}
            if hso.get("hookEventName") == event:
                perm = hso.get("permissionDecision")
                if perm == "deny":
                    decision["block"] = True
                    decision["reason"] = hso.get("permissionDecisionReason",
                                                 "denied by hook")
                    return decision
                if "updatedInput" in hso:
                    decision["updated_input"] = hso["updatedInput"]
    return decision


def _truncate(s, n=120):
    return s if len(s) <= n else s[:n] + "..."


def _build_injection():
    """构建 plugin skill 索引注入文本。带缓存。"""
    global _injection_cache
    key = _cache_key()
    if _injection_cache and _injection_cache[0] == key:
        return _injection_cache[1]
    plugins = _get_plugins()
    if not plugins or not any(p["skills"] for p in plugins):
        _injection_cache = (key, None)
        return None
    lines = [
        "\n\n---\n## 已安装的 Plugins（Claude Code Plugin 规范）",
        "以下 plugin skill 可用，命令格式 `/plugin-name:skill-name`。",
        "**当用户意图匹配某 skill 触发场景时**：先 file_read 该 SKILL.md 正文获取完整指令，再按指令执行。",
        "**若用户直接输入 `/plugin-name:skill-name 参数`**：读取对应 SKILL.md，将其中的 `$ARGUMENTS` 替换为用户参数后执行。",
        "",
    ]
    idx = 0
    for p in plugins:
        if not p["skills"]:
            continue
        idx += 1
        lines.append(f"### Plugin {idx}. {p['name']}")
        if p["description"]:
            lines.append(f"- 说明：{_truncate(p['description'], 200)}")
        for sk in p["skills"]:
            tag = ""
            if sk["disable_model_invocation"]:
                tag += " [仅命令触发]"
            if sk["has_args"]:
                tag += " [接受 $ARGUMENTS]"
            lines.append(f"- skill `{sk['ns_name']}`{tag}：{_truncate(sk['description'], 160)}")
            lines.append(f"  - SKILL.md：{sk['path']}")
        lines.append("")
    lines.append("---")
    text = "\n".join(lines)
    _injection_cache = (key, text)
    return text


# ===== 公开 API（供 slash_cmds 调用） =====

def list_plugin_commands() -> list:
    """返回 plugin skill 命令列表，供 slash 面板注册。"""
    plugins = _get_plugins()
    commands = []
    for p in plugins:
        for sk in p["skills"]:
            cmd = f"/{sk['ns_name']}"
            label = sk["ns_name"]
            if sk["has_args"]:
                label += " [args]"
            commands.append({
                "name": cmd,
                "label": label,
                "desc": sk["description"] or sk["ns_name"],
                "group": "plugins",
            })
    return commands


def _read_skill_md(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return None


def render_plugin_skill(cmd: str, args_text: str = ""):
    """根据 /plugin:skill 命令渲染 SKILL.md 正文，替换 $ARGUMENTS。

    cmd 形如 "/my-plugin:hello"。返回渲染后的文本，未找到返回 None。
    """
    if not cmd or not cmd.startswith("/"):
        return None
    ns = cmd[1:]  # 去掉前导 /
    if ":" not in ns:
        return None
    plugins = _get_plugins()
    for p in plugins:
        for sk in p["skills"]:
            if sk["ns_name"] == ns:
                text = _read_skill_md(sk["path"])
                if text is None:
                    return None
                header = (
                    f"（执行 plugin skill `{ns}`"
                    + (f"，参数：{args_text}" if args_text else "，无参数")
                    + "。以下为 SKILL.md 指令，已将 $ARGUMENTS 替换为用户参数）\n\n"
                )
                if args_text:
                    text = text.replace("$ARGUMENTS", args_text)
                return header + text
    return None


# ===== agent_before hook：注入 plugin skill 索引 =====

if hooks:
    @hooks.register("agent_before")
    def inject_plugin_skills_index(ctx):
        """每个用户轮起始时，把 plugin skill 索引追加到 user message。"""
        text = _build_injection()
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


# ===== 用户显式选中插件（前端 plugin-chip 写入的「【使用插件 X】」前缀） =====

_SELECTED_PLUGIN_RE = re.compile(r"【使用插件 ([^】\n]+)】")


def _build_selected_injection(name):
    """构建选中 plugin 的聚焦注入文本：完整能力清单 + 优先使用指令。

    与 _build_injection 的全量索引互补：即使 plugin 无 skills（仅 agents/MCP）
    也能被选中生效；不缓存（仅在命中前缀的轮次构建，开销可忽略）。
    """
    p = next((x for x in _get_plugins() if x["name"] == name), None)
    if not p:
        return None
    lines = [f"\n\n---\n## 用户已显式选择插件 `{p['name']}`，本轮请优先使用该插件的能力完成任务"]
    if p["description"]:
        lines.append(f"- 说明：{_truncate(p['description'], 300)}")
    lines.append(f"- 目录：{p['dir']}")
    for sk in p["skills"]:
        tag = " [接受 $ARGUMENTS]" if sk["has_args"] else ""
        lines.append(f"- skill `{sk['ns_name']}`{tag}：{_truncate(sk['description'], 160)}")
        lines.append(f"  - SKILL.md：{sk['path']}")
    for ag in p.get("agents", []):
        lines.append(f"- agent `{ag['ns_name']}`：{_truncate(ag['description'], 160)}")
        lines.append(f"  - 正文：{ag['path']}")
    for sn in (p.get("mcp") or {}):
        lines.append(f"- MCP server `{sn}`：工具名前缀 `mcp__{p['name']}__{sn}__`，可直接调用")
    lines.append("执行方式：先 file_read 与用户请求最匹配的 SKILL.md（或 agent 正文）获取完整指令，再按指令执行。")
    lines.append("---")
    return "\n".join(lines)


if hooks:
    @hooks.register("agent_before")
    def inject_selected_plugin(ctx):
        """用户消息带「【使用插件 X】」前缀时，注入该 plugin 的聚焦能力清单。"""
        um = next((m for m in reversed(ctx.get("messages") or [])
                   if isinstance(m, dict) and m.get("role") == "user"), None)
        if um is None:
            return
        content = um.get("content")
        if isinstance(content, str):
            body = content
        elif isinstance(content, list):
            body = next((c.get("text", "") for c in content
                         if isinstance(c, dict) and c.get("type") == "text"), "")
        else:
            return
        m = _SELECTED_PLUGIN_RE.search(body[:300])
        if not m:
            return
        text = _build_selected_injection(m.group(1).strip())
        if not text:
            return
        if isinstance(content, str):
            um["content"] = content + text
        elif isinstance(content, list):
            content.append({"type": "text", "text": text})


def _build_agent_injection():
    """构建 plugin agent 索引注入文本（与 _build_injection 对称，独立于 skill 注入）。

    依赖 _get_plugins 缓存（plugins 已缓存，遍历 agents 开销小，不单独缓存）。
    """
    plugins = _get_plugins()
    if not plugins or not any(p.get("agents") for p in plugins):
        return None
    lines = [
        "\n\n---\n## 已安装的 Plugin Agents（Claude Code Plugin 规范）",
        "以下 plugin agent 可用（scoped name = `plugin:agent-name`）。",
        "**当任务匹配某 agent 的专长描述时**，启动 subagent 执行：",
        "1. file_read 该 agent 正文（路径见下，frontmatter 之后的 body）作为 system prompt",
        "2. 将 system prompt + 用户具体任务写入 temp 下临时文件（如 temp/_plugin_agent_<name>.txt）",
        "3. 执行 `python agentmain.py --func <该临时文件> --nobg`（前台同步等结果；cwd=代码根）",
        "4. 读取 `<临时文件>.out.txt` 获取 subagent 产出（注意：subagent 可能未写指定产物但 out.txt 必有完整轨迹，据工具调用序列+末尾总结判断实质进展）",
        "",
    ]
    idx = 0
    for p in plugins:
        if not p.get("agents"):
            continue
        idx += 1
        lines.append(f"### Plugin Agent {idx}. {p['name']}")
        for ag in p["agents"]:
            tag = ""
            if ag["tools"]:
                tag += f" [专长工具:{','.join(ag['tools'])}]"
            if ag["disallowedTools"]:
                tag += f" [禁用:{','.join(ag['disallowedTools'])}]"
            if ag["model"]:
                tag += f" [model:{ag['model']}]"
            lines.append(f"- agent `{ag['ns_name']}`{tag}：{_truncate(ag['description'], 200)}")
            lines.append(f"  - 正文：{ag['path']}")
        lines.append("")
    lines.append("---")
    return "\n".join(lines)


def list_plugin_agents() -> list:
    """返回 plugin agent 列表（带 namespace/frontmatter 字段），供调试与面板。"""
    plugins = _get_plugins()
    agents = []
    for p in plugins:
        for ag in p.get("agents", []):
            agents.append(ag)
    return agents


def render_plugin_agent(ns: str) -> str:
    """根据 plugin:agent-name 渲染 agent 正文（frontmatter 之后的 body）作为 system prompt。

    ns 形如 "my-plugin:my-agent"。返回正文文本，未找到返回 None。
    与 render_plugin_skill 对称，供主 agent 启动 subagent 时取 system prompt。
    """
    if not ns or ":" not in ns:
        return None
    plugins = _get_plugins()
    for p in plugins:
        for ag in p.get("agents", []):
            if ag["ns_name"] == ns:
                try:
                    with open(ag["path"], "r", encoding="utf-8") as f:
                        text = f.read()
                except Exception:
                    return None
                m = re.match(r"^---\s*\n.*?\n---", text, re.DOTALL)
                body = text[m.end():].strip() if m else text.strip()
                return f"（plugin agent `{ns}` 的 system prompt，以下为正文）\n\n" + body
    return None


if hooks:
    @hooks.register("agent_before")
    def inject_plugin_agents_index(ctx):
        """每个用户轮起始时，把 plugin agent 索引追加到 user message（与 skill 索引并存）。"""
        text = _build_agent_injection()
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


# ============================================================================
# Background Monitors（4.2）— CC plugin 持久后台进程 + stdout 注入 agent context
# ============================================================================
# 每个 monitor = subprocess(持久) + 1 读线程(逐行 stdout → 本地 buffer)。
# 全局 1 个 pump 守护线程，每 0.5s drain 所有 monitor 的 buffer，在
# agent.intervene 为 None（已被 turn_end 消费）时把新 notification 写入
# agent.intervene —— 复用 GA 现有 intervene 注入通道（ga.py L575-579），
# 零侵入核心源码。协议：intervene 唯一写者=pump（仅 None 时写），
# turn_end 只读非 None 后清空 → 无丢消息竞态。每行 stdout = 一个 notification。
_monitors_lock = threading.Lock()
_active_monitors = {}              # key="plugin::name" -> {proc, reader, buffer, stop, label}
_started_skill_monitors = set()   # 已启动的 on-skill-invoke monitor key，防重复
_pump_thread = None
_pump_stop = None
_session_agent_ref = None


def _monitor_key(plugin_name, mon_name):
    return f"{plugin_name}::{mon_name}"


def _reader_loop(proc, mon_buffer, mon_label, stop_flag):
    """daemon 线程：逐行读 monitor stdout → buffer。"""
    try:
        while not stop_flag.is_set():
            line = proc.stdout.readline()
            if not line:
                if proc.poll() is not None:
                    break
                continue
            line = line.rstrip("\n\r")
            if line:
                with _monitors_lock:
                    mon_buffer.append((mon_label, line))
    except Exception:
        pass


def _pump_loop(stop_flag):
    """全局 daemon 线程：每 0.5s drain 所有 monitor buffer，
    在 agent.intervene 为 None 时写入（格式化 notification）。
    使用全局 _session_agent_ref（由 start_session_monitors 设置）。"""
    while not stop_flag.is_set():
        try:
            collected = []
            with _monitors_lock:
                for key, info in _active_monitors.items():
                    buf = info["buffer"]
                    while buf:
                        collected.append(buf.pop(0))
            ag = _session_agent_ref
            if collected and ag is not None and getattr(ag, "intervene", None) is None:
                body = "\n".join(f"[Monitor {lbl}] {ln}" for lbl, ln in collected)
                ag.intervene = body
        except Exception:
            pass
        stop_flag.wait(0.5)


def _ensure_pump_thread():
    """启动全局 pump 线程（幂等）。pump 用全局 _session_agent_ref。"""
    global _pump_thread, _pump_stop
    if _pump_thread is not None and _pump_thread.is_alive():
        return
    _pump_stop = threading.Event()
    _pump_thread = threading.Thread(target=_pump_loop, args=(_pump_stop,), daemon=True)
    _pump_thread.start()


def _start_one_monitor(mon):
    """启动单个 monitor subprocess + 读线程。返回 key 或 None。
    幂等：key 已存在则跳过。fail-open：启动失败返回 None。"""
    key = _monitor_key(mon["plugin_name"], mon["name"])
    with _monitors_lock:
        if key in _active_monitors:
            return key
    try:
        proc = subprocess.Popen(
            mon["command"],
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    except Exception:
        return None
    mon_buffer = []
    stop_flag = threading.Event()
    label = f"{mon['plugin_name']}:{mon['name']}"
    t = threading.Thread(target=_reader_loop, args=(proc, mon_buffer, label, stop_flag), daemon=True)
    t.start()
    with _monitors_lock:
        _active_monitors[key] = {
            "proc": proc, "reader": t, "buffer": mon_buffer,
            "stop": stop_flag, "label": label,
        }
    _ensure_pump_thread()
    return key


def start_session_monitors(agent_ref):
    """session start / plugin reload 时启动所有 when=always 的 monitor。
    幂等（已启动跳过）+ fail-open（单 monitor 失败不影响其他）。
    由 GA 在 session 开始/reload 后调用。设置全局 _session_agent_ref 供 pump 使用。
    """
    global _session_agent_ref
    _session_agent_ref = agent_ref
    try:
        plugins = _get_plugins()
    except Exception:
        return
    for p in plugins:
        for mon in p.get("monitors", []) or []:
            if mon.get("when", "always") == "always":
                try:
                    _start_one_monitor(mon)
                except Exception:
                    pass


def start_skill_monitors(plugin_name, skill_name):
    """某 plugin 的 skill 首次 dispatch 时启动 when=on-skill-invoke:<skill> 的 monitor。
    幂等：同一 monitor 仅在首次 dispatch 启动（_started_skill_monitors 去重）。
    由 GA 在 skill dispatch 入口调用。pump 用全局 _session_agent_ref（需先 start_session）。
    """
    target = f"on-skill-invoke:{skill_name}"
    try:
        plugins = _get_plugins()
    except Exception:
        return
    for p in plugins:
        if p.get("name") != plugin_name:
            continue
        for mon in p.get("monitors", []) or []:
            if mon.get("when", "always") == target:
                key = _monitor_key(plugin_name, mon["name"])
                with _monitors_lock:
                    if key in _started_skill_monitors:
                        continue
                    _started_skill_monitors.add(key)
                try:
                    _start_one_monitor(mon)
                except Exception:
                    pass


def stop_all_monitors():
    """session 结束 / reload 前清理：terminate 所有 monitor subprocess + 停 pump 线程。
    注意：CC 语义为 disable mid-session 不停已运行的；本函数在 session 真正结束时调用。
    """
    global _pump_thread, _pump_stop, _session_agent_ref
    with _monitors_lock:
        items = list(_active_monitors.items())
        _active_monitors.clear()
        _started_skill_monitors.clear()
    for key, info in items:
        info["stop"].set()
        try:
            info["proc"].terminate()
        except Exception:
            pass
        try:
            info["proc"].wait(timeout=1)
        except Exception:
            pass
    if _pump_stop is not None:
        _pump_stop.set()
    _pump_thread = None
    _pump_stop = None
    _session_agent_ref = None
