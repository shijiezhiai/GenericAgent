"""GA permission layer — central decision broker (P1).

The PermissionBroker is the single, fail-closed decision point inserted in
agent_loop.dispatch. It evaluates every tool call (built-in, MCP, or surfaced
through a CC PreToolUse hook) against a deny > ask > allow rule table that is
scoped per permission mode, then falls back to a coarse per-mode default policy.

Design rules (see docs/permission_redesign.md):
* fail-closed: any broker error (DB down, bad rule, etc.) => DENY. The security
  promise must not depend on a healthy DB.
* deny > ask > allow precedence; among equal decisions, the more specific rule
  (exact tool > wildcard tool > "*", explicit target > no target) wins.
* This layer is L1 (decision). Real boundaries (path containment, OS sandbox,
  credential stripping) live in L0 (ga.py / P3) and are NOT enforced here.
"""
from __future__ import annotations
import os, re, fnmatch
from plugins.permission_store import (
    DEFAULT_MODE, _VALID_MODES, load_rules, add_rule, remove_rule,
    get_config, set_config,
)

# P2: permission-confirmation INTERRUPT intent. Distinct from ask_user's
# HUMAN_INTERVENTION so frontends can render an allow/deny prompt (not a free
# question) and feed the decision back via session memory.
PERMISSION_INTENT = "PERMISSION_REQUEST"


def extract_permission_event(ctx):
    """Pull a PERMISSION_REQUEST INTERRUPT out of a turn_end_hook ctx.

    Mirrors the ask_user extraction in tgapp/tuiapp_v2. Returns
    {"tool", "mode", "target", "question"} or None when the run didn't end on a
    permission prompt. Frontends call this from their registered turn_end hook
    (registered on `agent._turn_end_hooks[<key>]`) to know when to show an
    allow/deny prompt.
    """
    if not isinstance(ctx, dict):
        return None
    exit_reason = ctx.get("exit_reason") or {}
    if not isinstance(exit_reason, dict) or exit_reason.get("result") != "EXITED":
        return None
    payload = exit_reason.get("data")
    if not isinstance(payload, dict):
        return None
    if payload.get("status") != "INTERRUPT" or payload.get("intent") != PERMISSION_INTENT:
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    return {
        "tool": str(data.get("tool", "")),
        "mode": str(data.get("mode", "")),
        "target": str(data.get("target") or ""),
        "question": str(data.get("question") or "").strip(),
    }


def answer_permission(agent, tool, decision, mode=None, resume_prompt=None, initiator=None):
    """Persist a user allow/deny decision and resume the run so the broker
    short-circuits the re-issued tool call.

    Returns the display_queue from `agent.put_task` so the frontend can start a
    stream on it (as tgapp's _stream does). `decision` must be "allow"/"deny".
    """
    if decision not in ("allow", "deny"):
        return None
    record_session(agent, tool, decision, mode, initiator=initiator)
    if resume_prompt is None:
        resume_prompt = "继续（权限已决定，请继续执行原任务）"
    return agent.put_task(resume_prompt, source=getattr(agent, "frontend", "user"))


def permission_decision_allowed(agent, requester):
    """Scope permission approvals to the task initiator (P4: IM initiator auth).

    Returns True when no initiator is recorded (back-compat), or when `requester`
    matches the recorded initiator. Frontends pass the command author, e.g.
    "telegram:12345"; a decision from anyone else is rejected.
    """
    ini = getattr(agent, "initiator", None)
    if not ini or not requester:
        return True
    return requester == ini


def record_session(parent, tool, decision, mode=None, initiator=None):
    """Persist a user allow/deny decision for the rest of the session.

    `initiator` (e.g. "telegram:12345") optionally attributes the decision to the
    user who launched the task; see permission_decision_allowed for how it scopes
    who may later flip the decision.

    `parent` is the GenericAgent instance, so the decision survives the
    INTERRUPT-driven re-runs. This is what stops a repeated `ask` from looping:
    the first approval/rejection is remembered and the next identical call is
    short-circuited before it ever reaches the user again. Claude Code parity:
    "Allow once for this session".
    """
    if parent is None or decision not in ("allow", "deny"):
        return
    m = mode or getattr(parent, "permission_mode", DEFAULT_MODE)
    k = f"{m}|{tool}"
    a = getattr(parent, "_perm_allow", None)
    d = getattr(parent, "_perm_deny", None)
    if a is None:
        a = parent._perm_allow = set()
    if d is None:
        d = parent._perm_deny = set()
    if decision == "allow":
        a.add(k); d.discard(k)
    else:
        d.add(k); a.discard(k)
    if initiator:
        parent.initiator = initiator

# ---- tool taxonomy (coarse fallback when no rule matches) -------------------
_READ_TOOLS = {
    "file_read", "code_search", "file_find", "repo_index", "semantic_search",
    "web_scan", "ask_user", "update_working_checkpoint", "start_long_term_update",
}
_WRITE_TOOLS = {"file_write", "file_patch", "web_execute_js"}
_EXEC_TOOLS = {"code_run", "git_checkpoint"}


def _mode_default(tool, mode):
    """Coarse per-mode fallback used only when no rule matches the call."""
    if mode in ("read-only", "plan"):
        # read-only / plan: only read-only introspection tools are allowed.
        return "allow" if tool in _READ_TOOLS else "deny"
    if mode == "full-access":
        return "allow"
    # workspace-write (default): reads + in-workspace writes allowed; code
    # execution allowed (env stripped in P0, OS sandbox = P3); opt into per-tool
    # approval via rules (e.g. `/permissions add code_run ask`).
    return "allow"


def _tool_matches(rule_tool, tool):
    if rule_tool == "*" or rule_tool == tool:
        return True
    if rule_tool.endswith("*") and tool.startswith(rule_tool[:-1]):
        return True
    return False


def _glob_match(glob, target):
    if not glob:
        return True
    target = str(target)
    return fnmatch.fnmatch(target, glob) or fnmatch.fnmatch(os.path.basename(target), glob)


def _specificity(r):
    """Lower == more specific == wins ties. tool exact<wild<*, target explicit<none."""
    tool = r["tool"]
    s = 0
    if tool == "*":
        s += 4
    elif tool.endswith("*"):
        s += 2
    if r["target_glob"]:
        s += 1
    return s


class PermissionBroker:
    def __init__(self, mode=None, frontend="", parent=None):
        self.parent = parent
        m = mode or (getattr(parent, "permission_mode", None) if parent else None)
        m = m or get_config("default_mode", DEFAULT_MODE)
        self.mode = m if m in _VALID_MODES else DEFAULT_MODE
        self.frontend = frontend

    # ---- target extraction -------------------------------------------------
    def _target_of(self, tool, args):
        if not isinstance(args, dict):
            return ""
        for k in ("path", "cwd", "script", "command", "url", "query", "code"):
            if args.get(k):
                return str(args[k])[:512]
        return ""

    # ---- evaluation --------------------------------------------------------
    def _session_key(self, tool):
        return f"{self.mode}|{tool}"

    def evaluate(self, tool, args=None, tool_type="builtin"):
        """Return (decision, reason, rule_id). decision in {allow, deny, ask}."""
        try:
            # P2: session-level memory. A user "allow"/"deny" for this tool in the
            # current session overrides every rule (Claude Code parity: Allow-once
            # for session). Stored on the parent GenericAgent, persists across the
            # INTERRUPT-driven re-runs, which is what stops ask from looping.
            if self.parent is not None:
                _k = self._session_key(tool)
                _allow = getattr(self.parent, "_perm_allow", None)
                _deny = getattr(self.parent, "_perm_deny", None)
                if _allow is not None and _k in _allow:
                    return "allow", "session-allow", None
                if _deny is not None and _k in _deny:
                    return "deny", "session-deny", None
            mode = self.mode
            target = self._target_of(tool, args)
            rules = load_rules()
            matched = []
            for r in rules:
                if r["mode"] not in (mode, "*"):
                    continue
                if not _tool_matches(r["tool"], tool):
                    continue
                if r["target_glob"] and not _glob_match(r["target_glob"], target):
                    continue
                matched.append(r)
            if matched:
                # deny(0) > ask(1) > allow(2); tie -> most specific rule
                order = {"deny": 0, "ask": 1, "allow": 2}
                best = sorted(matched, key=lambda r: (order.get(r["decision"], 3), _specificity(r)))[0]
                return best["decision"], f"rule#{best['id']}", best["id"]
            return _mode_default(tool, mode), "mode-default", None
        except Exception as e:
            # FAIL-CLOSED
            return "deny", f"broker_error:{type(e).__name__}", None


# ---------------------------------------------------------------------------
# /permissions slash command (P1-T4 wires this into agentmain._handle_slash_cmd)
# ---------------------------------------------------------------------------
def permissions_cli(text, agent=None):
    """Parse and execute a `/permissions ...` command; return a display string."""
    text = (text or "").strip()
    parts = text.split()
    sub = parts[1:] if len(parts) > 1 else []
    cmd = sub[0] if sub else "list"

    if cmd in ("list", "show", "ls", ""):
        mode = (getattr(agent, "permission_mode", None) if agent else None) or get_config("default_mode", DEFAULT_MODE)
        lines = [f"当前权限模式 (permission_mode): {mode}", "", "规则表 (deny > ask > allow):", "─" * 46]
        rules = load_rules()
        if not rules:
            lines.append("  (无自定义规则，使用模式默认策略)")
        for r in rules:
            g = f"  [{r['target_glob']}]" if r["target_glob"] else ""
            n = f"  # {r['note']}" if r["note"] else ""
            lines.append(f"  #{r['id']:<3} {r['mode']:<14} {r['tool']:<18} -> {r['decision']}{g}{n}")
        lines += [
            "", "模式默认策略:",
            "  read-only / plan : 仅只读工具允许，其余拒绝",
            "  workspace-write  : 工作区内读写允许；code_run 允许(待 P3 沙箱)；越权写由 L0 拦截",
            "  full-access      : 全部允许",
            "", "用法:",
            "  /permissions                     查看当前模式与规则",
            "  /permissions mode=<mode>         切换模式并持久化",
            "  /permissions add <tool> <dec|ask|allow> [glob]   新增规则(作用于当前模式)",
            "  /permissions rm <id>             删除规则",
        ]
        return "\n".join(lines)

    if cmd == "mode" or cmd.startswith("mode="):
        val = cmd.split("=", 1)[1] if "=" in cmd else None
        if not val:
            m = re.search(r"mode=(\S+)", text)
            val = m.group(1) if m else None
        if not val or val not in _VALID_MODES:
            return "用法: /permissions mode=<full-access|workspace-write|read-only|plan>"
        if agent is not None:
            agent.permission_mode = val
        set_config("default_mode", val)
        return f"✅ 权限模式已设为 `{val}`（已持久化，新会话默认沿用）"

    if cmd in ("add", "set"):
        m = re.match(r"/permissions\s+(?:add|set)\s+(\S+)\s+(allow|deny|ask)(?:\s+(.+))?$", text, re.IGNORECASE)
        if not m:
            return "用法: /permissions add <tool> <allow|deny|ask> [glob]"
        tool, dec = m.group(1), m.group(2).lower()
        glob = (m.group(3) or "").strip().strip('"').strip("'")
        # scope to the current mode so a rule added in workspace-write doesn't
        # silently apply under full-access too (override with tool="*" if desired).
        scope_mode = (getattr(agent, "permission_mode", None) if agent else None) or "*"
        rid = add_rule(mode=scope_mode, tool=tool, decision=dec, target_glob=glob)
        if rid is None:
            return "⚠️ 规则添加失败（存储不可用？）"
        return (f"✅ 已添加规则 #{rid}: {scope_mode} {tool} -> {dec}"
                + (f" [{glob}]" if glob else ""))

    if cmd in ("rm", "remove", "del"):
        m = re.match(r"/permissions\s+(?:rm|remove|del)\s+(\d+)", text)
        if not m:
            return "用法: /permissions rm <id>"
        ok = remove_rule(int(m.group(1)))
        return f"✅ 已删除规则 #{m.group(1)}" if ok else f"⚠️ 未找到规则 #{m.group(1)}"

    return "用法: /permissions [list|mode=|add|rm]"
