"""Skills Loader plugin — 把外部 Agent Skills（Anthropic 开放规范）安装给 GA。

机制：注册 agent_before hook，每个用户轮起始时把已发现的 skill 索引追加到 user message。
设计为两层渐进加载（与 project_mode 一致）：
  L1（每轮注入）：skill 索引表——name / 触发关键词 / SKILL.md 路径 / 运行基准，轻量稳定。
  L2（按需读取）：模型命中某 skill 后，用 file 工具读对应 SKILL.md 正文获取完整指令；
                  references/ 下的文档由 SKILL.md 正文指明加载条件，再按需读取。

配置（优先级从高到低）：
  1. 环境变量 GA_SKILLS_ROOTS：冒号分隔的 skills 根目录列表，设置即启用
  2. 配置文件（二选一，首个存在者生效）：
     - $GA_SKILLS_CONFIG 指定的路径
     - plugins/skills_config.json（本插件旁，推荐）
       {
         "skills_roots": ["/abs/path/to/flink-skills"],
         "enabled": true
       }
若以上均不存在，默认不注入（零干扰）。
"""

import os
import re
import json

try:
    from plugins import hooks
except Exception:
    hooks = None

_PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
_CONFIG_PATH = os.environ.get("GA_SKILLS_CONFIG", os.path.join(_PLUGIN_DIR, "skills_config.json"))
_injection_cache = None  # (config_mtime, text)


def _load_config():
    """读取配置；优先级：env > 配置文件。返回 roots 列表或 None。"""
    # 1. 环境变量 GA_SKILLS_ROOTS（冒号分隔）
    env_roots = os.environ.get("GA_SKILLS_ROOTS", "").strip()
    if env_roots:
        return [r.strip() for r in env_roots.split(":") if r.strip()]
    # 2. 配置文件
    if not os.path.isfile(_CONFIG_PATH):
        return None
    try:
        with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        if not cfg.get("enabled", False):
            return None
        roots = cfg.get("skills_roots", [])
        if not roots:
            return None
        return roots
    except Exception:
        return None


def _parse_frontmatter(text):
    """解析 SKILL.md 的 YAML frontmatter，提取 name / description。"""
    m = re.match(r"^---\s*\n(.*?)\n---", text, re.DOTALL)
    if not m:
        return None
    body = m.group(1)
    name = None
    desc_lines = []
    in_desc = False
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
        elif in_desc:
            if line.strip() == "" or not line.startswith((" ", "\t")):
                in_desc = False
            else:
                desc_lines.append(line.strip())
    desc = " ".join(desc_lines).strip()
    return {"name": name, "description": desc}


def _discover_skills(roots):
    """扫描各 skills 根目录，发现所有 <name>/SKILL.md，返回 skill 列表。

    兼容两种目录结构：
      A) root/<name>/SKILL.md          （根目录直接含 skill 目录）
      B) root/skills/<name>/SKILL.md   （根目录下有 skills/ 子目录，如 flink-skills 仓库）
    """
    skills = []
    for root in roots:
        root = os.path.expanduser(root)
        if not os.path.isdir(root):
            continue
        # 候选 skill 父目录：root 本身 + root/skills（若存在）
        scan_dirs = [root]
        skills_subdir = os.path.join(root, "skills")
        if os.path.isdir(skills_subdir):
            scan_dirs.append(skills_subdir)
        for scan_dir in scan_dirs:
            for entry in sorted(os.listdir(scan_dir)):
                if entry.startswith("."):
                    continue
                skill_md = os.path.join(scan_dir, entry, "SKILL.md")
                if not os.path.isfile(skill_md):
                    continue
                try:
                    with open(skill_md, "r", encoding="utf-8") as f:
                        text = f.read()
                except Exception:
                    continue
                meta = _parse_frontmatter(text)
                if not meta or not meta["name"]:
                    meta = {"name": entry, "description": ""}
                meta["path"] = skill_md
                meta["root"] = root
                meta["dir"] = os.path.join(scan_dir, entry)
                # 是否有可执行脚本
                scripts_dir = os.path.join(scan_dir, entry, "scripts")
                meta["has_scripts"] = os.path.isdir(scripts_dir) and any(
                    fn.endswith(".js") or fn.endswith(".py")
                    for fn in os.listdir(scripts_dir)
                ) if os.path.isdir(scripts_dir) else False
                skills.append(meta)
    return skills


def _truncate(s, n=120):
    return s if len(s) <= n else s[:n] + "..."


def _build_injection():
    """构建注入文本。带缓存：配置文件 mtime 不变则复用。"""
    global _injection_cache
    cfg_mtime = os.path.getmtime(_CONFIG_PATH) if os.path.isfile(_CONFIG_PATH) else 0
    if _injection_cache and _injection_cache[0] == cfg_mtime:
        return _injection_cache[1]

    roots = _load_config()
    if not roots:
        _injection_cache = (cfg_mtime, None)
        return None

    skills = _discover_skills(roots)
    if not skills:
        _injection_cache = (cfg_mtime, None)
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
    _injection_cache = (cfg_mtime, text)
    return text


if hooks:
    @hooks.register("agent_before")
    def inject_skills_index(ctx):
        """每个用户轮起始时，把 skill 索引追加到 user message。"""
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
