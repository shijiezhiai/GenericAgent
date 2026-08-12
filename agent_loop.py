import json, re, os, sys
from dataclasses import dataclass
from typing import Any, Optional
try: from plugins.hooks import trigger as _hook
except ImportError: _hook = lambda *a, **k: None
try: from plugins.plugin_loader import run_cc_hook as _run_cc_hook
except ImportError: _run_cc_hook = None
try:
    from plugins.permission_store import (
        record_audit as _record_audit, DEFAULT_MODE, _VALID_MODES)
except Exception:  # pragma: no cover
    _record_audit = lambda *a, **k: None
    DEFAULT_MODE = "workspace-write"
    _VALID_MODES = ("full-access", "workspace-write", "read-only", "plan")
try:
    from plugins.permissions import PermissionBroker as _PermissionBroker
except Exception:  # pragma: no cover
    _PermissionBroker = None
@dataclass
class StepOutcome:
    data: Any
    next_prompt: Optional[str] = None
    should_exit: bool = False
def try_call_generator(func, *args, **kwargs):
    ret = func(*args, **kwargs)
    if hasattr(ret, '__iter__') and not isinstance(ret, (str, bytes, dict, list)): ret = yield from ret
    return ret

class BaseHandler:
    def turn_end_callback(self, response, tool_calls, tool_results, turn, next_prompt, exit_reason): return next_prompt
    def dispatch(self, tool_name, args, response, index=0, tool_num=1):
        method_name = f"do_{tool_name}"
        _has = hasattr(self, method_name)
        _parent = getattr(self, "parent", None)
        _mode = getattr(_parent, "permission_mode", DEFAULT_MODE)
        if _mode not in _VALID_MODES: _mode = DEFAULT_MODE
        _frontend = getattr(_parent, "frontend", "")
        _target = ""
        if isinstance(args, dict):
            _target = (args.get("path") or args.get("cwd") or args.get("script")
                       or args.get("command") or args.get("url") or args.get("query") or "")
        # P1: centralized permission broker (fail-closed). Runs BEFORE the tool and
        # before the CC PreToolUse hook so a deny short-circuits everything. Covers
        # built-in tools, MCP tools, and CC-hook-surfaced calls uniformly.
        try:
            _broker = _PermissionBroker(mode=_mode, frontend=_frontend, parent=_parent)
            _decision, _reason, _rule_id = _broker.evaluate(
                tool_name, args, tool_type=("mcp" if not _has else "builtin"))
        except Exception as _e:
            _decision, _reason, _rule_id = "deny", f"broker_init_error:{type(_e).__name__}", None
        # audit every dispatch centrally — covers ALL frontends (CLI/TUI/desktop/
        # IM/conductor) that funnel through agent_runner_loop. Best-effort; never blocks.
        _record_audit(tool_name, target=str(_target)[:512], action="decision",
                      decision=_decision, mode=_mode, frontend=_frontend, detail=_reason)
        if _decision == "deny":
            yield f"⛔ 权限拒绝: 工具 `{tool_name}`（mode={_mode}, {_reason}）\n"
            return StepOutcome({"error": "permission_denied", "reason": _reason},
                               next_prompt=f"工具 `{tool_name}` 被权限策略拒绝（{_reason}）。请换一种不越权的方式完成目标。")
        if _decision == "ask":
            # P2: resolve ask -> allow (fall through) / deny (return) / prompt (INTERRUPT).
            # Session memory is already checked inside broker.evaluate, so reaching here
            # means this is the first time this tool is asked this session.
            _ni = bool(os.environ.get("GA_NON_INTERACTIVE")) or getattr(_parent, "non_interactive", False)

            def _ask_deny(reason, remember=True):
                if remember and _parent is not None:
                    try:
                        from plugins.permissions import record_session
                        record_session(_parent, tool_name, "deny", _mode)
                    except Exception:
                        pass
                return StepOutcome({"error": "permission_denied", "reason": reason},
                                   next_prompt=f"工具 `{tool_name}` 需要人工确认但被拒绝（{reason}）。请换一种不越权的方式完成目标。")

            if _ni:
                yield f"⛔ 权限需确认但运行于非交互模式，已拒绝: `{tool_name}`（{_reason}）\n"
                return _ask_deny("non_interactive")
            # interactive frontends
            _cli = (getattr(_parent, "frontend", "cli") == "cli")
            if _cli and os.isatty(0):
                # CLI: synchronous prompt — no INTERRUPT/cross-run needed.
                _q = (f"⚠️ 权限确认: 是否允许执行工具 `{tool_name}`？"
                      + (f"\n   目标: {str(_target)[:200]}" if _target else "")
                      + "\n   允许请回车或输入 y/Y，拒绝输入 n/N: ")
                try:
                    _ans = input(_q)
                except Exception:
                    _ans = "n"
                if str(_ans).strip().lower() in ("y", "yes", ""):
                    try:
                        from plugins.permissions import record_session
                        record_session(_parent, tool_name, "allow", _mode)
                    except Exception:
                        pass
                    yield f"✅ 已授权本次会话执行 `{tool_name}`。\n"
                    # fall through to execution below
                else:
                    yield f"⛔ 已拒绝执行 `{tool_name}`。\n"
                    return _ask_deny("user_denied")
            elif getattr(_parent, "ask_capable", False):
                # TUI/desktop: INTERRUPT -> turn_end_hook -> re-put_task; the user's
                # choice is recorded into session memory by the frontend handler.
                from plugins.permissions import PERMISSION_INTENT
                _payload = {"status": "INTERRUPT", "intent": PERMISSION_INTENT,
                            "data": {"tool": tool_name, "mode": _mode,
                                     "target": str(_target)[:512],
                                     "question": f"是否允许执行工具 `{tool_name}`？"}}
                yield "Waiting for your approval ...\n"
                return StepOutcome(_payload, next_prompt="", should_exit=True)
            else:
                # Frontend is interactive but doesn't implement PERMISSION_REQUEST
                # (IM bots / unhandled). Fail safe: deny + remember, let the model
                # pick a non-privileged path. Full IM interaction = P2.5.
                yield f"⛔ 权限需确认但当前前端不支持交互授权，已拒绝: `{tool_name}`（{_reason}）\n"
                return _ask_deny("frontend_uncapable")
        if _has:
            args['_index'] = index; args['_tool_num'] = tool_num
            _hook('tool_before', locals())
            # Claude Code plugin hooks (PreToolUse): block / 改写参数 / 停止 agent。fail-open：hook 异常不阻断工具。
            if _run_cc_hook is not None:
                _cc_input = {k: v for k, v in args.items() if k not in ('_index', '_tool_num')}
                _d = _run_cc_hook('PreToolUse', tool_name, _cc_input)
                if _d.get('stop'):
                    return StepOutcome(None, next_prompt=_d.get('stop_reason') or 'stopped by plugin hook', should_exit=True)
                if _d.get('block'):
                    yield f"⚠️ 工具 `{tool_name}` 被 plugin hook 阻止: {_d.get('reason','')}\n"
                    return StepOutcome({'error': 'blocked', 'reason': _d.get('reason','')},
                                       next_prompt=f"工具 {tool_name} 被 plugin hook 阻止: {_d.get('reason','')}。请根据反馈调整后重试。")
                _ui = _d.get('updated_input')
                if _ui is not None and isinstance(_ui, dict): args.update(_ui)
            ret = yield from try_call_generator(getattr(self, method_name), args, response)
            _hook('tool_after', locals())
            return ret
        elif tool_name == 'bad_json': return StepOutcome(None, next_prompt=args.get('msg', 'bad_json'), should_exit=False)
        else:
            # MCP fallback：tool_name 形如 mcp__<plugin>__<server>__<tool>，路由到对应 client
            _mcp_map = getattr(getattr(self, "parent", None), "mcp_tool_map", None) or {}
            if tool_name in _mcp_map:
                _c, _orig = _mcp_map[tool_name]
                try:
                    _res = _c.call_tool(_orig, args)
                    _txt = ""
                    if isinstance(_res, dict):
                        # P2: 优先取 structuredContent（2025-11-25 spec），fallback 到 content[].text
                        _sc = _res.get("structuredContent")
                        if _sc is not None:
                            _txt = json.dumps(_sc, ensure_ascii=False) if not isinstance(_sc, str) else _sc
                        else:
                            for _p in (_res.get("content") or []):
                                if isinstance(_p, dict) and _p.get("type") == "text":
                                    _txt += _p.get("text", "")
                        if _res.get("isError"):
                            _txt = f"⚠️ MCP 工具返回错误: {_txt}"
                    else:
                        _txt = str(_res)
                    yield (_txt + "\n") if _txt else f"MCP 工具 `{tool_name}` 执行完成（无输出）\n"
                    return StepOutcome({"result": _txt}, next_prompt=self._get_anchor_prompt(), should_exit=False)
                except Exception as _e:
                    yield f"⚠️ MCP 工具 `{tool_name}` 执行失败: {_e}\n"
                    return StepOutcome({"error": str(_e)},
                                       next_prompt=f"MCP 工具 {tool_name} 执行失败: {_e}。可重试或换方案。")
            yield f"未知工具: {tool_name}\n"
            return StepOutcome(None, next_prompt=f"未知工具 {tool_name}", should_exit=False)

def json_default(o): return list(o) if isinstance(o, set) else str(o)
def exhaust(g):
    try: 
        while True: next(g)
    except StopIteration as e: return e.value

def get_pretty_json(data):
    if isinstance(data, dict) and "script" in data:
        data = data.copy(); data["script"] = data["script"].replace("; ", ";\n  ")
    return json.dumps(data, indent=2, ensure_ascii=False).replace('\\n', '\n')

def agent_runner_loop(client, system_prompt, user_input, handler, tools_schema, 
                      max_turns=40, verbose=True, initial_user_content=None, yield_info=False):
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": initial_user_content if initial_user_content is not None else user_input}
    ]
    turn = 0;  handler.max_turns = max_turns
    _hook('agent_before', locals())
    try:
        from plugins.plugin_loader import start_session_monitors
        start_session_monitors(handler.parent)
    except Exception:
        pass
    while turn < handler.max_turns:
        turn += 1; turnstr = f'LLM Running (Turn {turn}) ...'
        if handler.parent.task_dir: turnstr = f'Turn {turn} ...'
        if verbose: turnstr = f'**{turnstr}**'
        if yield_info: yield {'turn': turn}
        yield f"\n\n{turnstr}\n\n"
        if turn%20 == 0: client.last_tools = ''  # P5: reset tool desc every 20 turns (was 10)
        _hook('turn_before', locals())
        _hook('llm_before', locals())
        response_gen = client.chat(messages=messages, tools=tools_schema)
        if verbose:
            response = yield from response_gen
            yield '\n\n'
        else:
            response = exhaust(response_gen)
            cleaned = _clean_content(response.content)
            if cleaned: yield cleaned + '\n'
        _hook('llm_after', locals())

        if not response.tool_calls: tool_calls = [{'tool_name': 'no_tool', 'args': {}}]
        else: tool_calls = [{'tool_name': tc.function.name, 'args': json.loads(tc.function.arguments), 'id': tc.id}
                          for tc in response.tool_calls]
       
        tool_results = []; next_prompts = set(); exit_reason = {}
        for ii, tc in enumerate(tool_calls):
            tool_name, args, tid = tc['tool_name'], tc['args'], tc.get('id', '')
            if tool_name == 'no_tool': pass
            else: 
                if verbose: yield f"🛠️ Tool: `{tool_name}`  📥 args:\n````text\n{get_pretty_json(args)}\n````\n"
                else: yield f"🛠️ {tool_name}({_compact_tool_args(tool_name, args)})\n\n\n"
            handler.current_turn = turn
            gen = handler.dispatch(tool_name, args, response, index=ii, tool_num=len(tool_calls))
            try:
                v = next(gen)
                def proxy(): yield v; return (yield from gen)
                if verbose: yield '`````\n'
                outcome = (yield from proxy()) if verbose else exhaust(proxy())
                if verbose: yield '`````\n'
            except StopIteration as e: outcome = e.value
            except Exception as e:
                # 工具 generator 抛出的非 StopIteration 异常(如 code_run 的 ValueError)在此兜底,
                # 转成 error 结果反馈给 LLM, 避免异常逃出 agent_loop 导致整个 agent 崩溃退出
                import traceback as _tb
                outcome = StepOutcome({'error': f'{type(e).__name__}: {e}', 'traceback': _tb.format_exc()},
                                      next_prompt=f"工具 {tool_name} 执行异常: {type(e).__name__}: {e}。请根据异常信息调整后重试。", should_exit=False)
            
            if outcome.should_exit: 
                exit_reason = {'result': 'EXITED', 'data': outcome.data}; break
            if not outcome.next_prompt: 
                exit_reason = {'result': 'CURRENT_TASK_DONE', 'data': outcome.data}; break
            if outcome.next_prompt.startswith('未知工具'): client.last_tools = ''
            if outcome.data is not None and tool_name != 'no_tool': 
                datastr = json.dumps(outcome.data, ensure_ascii=False, default=json_default) if type(outcome.data) in [dict, list] else str(outcome.data) 
                datastr = _truncate_tool_result(datastr, tool_name)
                tool_results.append({'tool_use_id': tid, 'content': datastr})
            next_prompts.add(outcome.next_prompt)
        if len(next_prompts) == 0 or exit_reason:
            if len(handler._done_hooks) == 0 or exit_reason.get('result', '') == 'EXITED': break
            next_prompts.add(handler._done_hooks.pop(0))
        next_prompt = handler.turn_end_callback(response, tool_calls, tool_results, turn, '\n'.join(next_prompts), exit_reason)
        # LSP diagnostics：检测文件编辑工具，通知 LSP 并注入诊断信息
        try:
            _lsp_clients = getattr(handler.parent, 'lsp_clients', None) or {}
            if _lsp_clients:
                _edited_files = []
                for _tc in tool_calls:
                    _tn, _ta = _tc['tool_name'], _tc['args']
                    if _tn in ('file_write', 'file_patch') and _ta.get('path'):
                        _edited_files.append(_ta['path'])
                if _edited_files:
                    import os as _os
                    _diag_lines = []
                    for _fp in _edited_files:
                        _ap = _os.path.abspath(_fp)
                        if not _os.path.isfile(_ap):
                            continue
                        try:
                            _text = open(_ap, 'r', encoding='utf-8', errors='replace').read()
                        except Exception:
                            continue
                        for _lc in _lsp_clients.values():
                            if not _lc.handles_extension(_ap):
                                continue
                            _lc.did_change(_ap, _text)
                    import time as _time; _time.sleep(0.3)  # wait for async diagnostics
                    for _lc in _lsp_clients.values():
                        _d = _lc.format_diagnostics()
                        if _d:
                            _diag_lines.append(_d)
                    if _diag_lines:
                        next_prompt += '\n\n' + '\n'.join(_diag_lines)
        except Exception:
            pass
        # Post-Action Verification：文件修改后自动 lint/test，结果反馈给模型
        try:
            from plugins.post_action_verify import run_post_action_verify
            next_prompt = run_post_action_verify(tool_calls, next_prompt)
        except Exception:
            pass
        # Test-Output Verification：模型自行用 code_run 跑测试命令时，识别失败并注入信号
        try:
            from plugins.post_action_verify import run_test_output_verify
            next_prompt = run_test_output_verify(tool_calls, tool_results, next_prompt)
        except Exception:
            pass
        _hook('turn_after', locals())
        messages = [{"role": "user", "content": next_prompt, "tool_results": tool_results}]   # just new message, history is kept in *Session
    if exit_reason: handler.turn_end_callback(response, tool_calls, tool_results, turn, '', exit_reason)
    _hook('agent_after', locals())
    try:
        from plugins.plugin_loader import stop_all_monitors
        stop_all_monitors()
    except Exception:
        pass
    return exit_reason or {'result': 'MAX_TURNS_EXCEEDED'}

def _clean_content(text):
    if not text: return ''
    def _shrink_code(m):
        lines = m.group(0).split('\n')
        lang = lines[0].replace('```','').strip()
        body = [l for l in lines[1:-1] if l.strip()]
        if len(body) <= 6: return m.group(0)
        preview = '\n'.join(body[:5])
        return f'```{lang}\n{preview}\n  ... ({len(body)} lines)\n```'
    text = re.sub(r'```[\s\S]*?```', _shrink_code, text)
    for p in [r'<file_content>[\s\S]*?</file_content>', r'<tool_(?:use|call)>[\s\S]*?</tool_(?:use|call)>', r'(\r?\n){3,}']:
        text = re.sub(p, '\n\n' if '\\n' in p else '', text)
    return text.strip()

def _compact_tool_args(name, args):
    a = {k: v for k, v in args.items() if k != '_index'}
    for k in ('path',): 
        if k in a: a[k] = os.path.basename(a[k])
    if name == 'update_working_checkpoint': s = a.get('key_info', ''); return (s[:60]+'...') if len(s)>60 else s
    if name == 'ask_user':
        q = str(a.get('question', ''))
        cs = a.get('candidates') or []
        if cs: q += '\ncandidates:\n' + '\n'.join(f'- {c}' for c in cs)
        return q
    s = json.dumps(a, ensure_ascii=False); return (s[:120]+'...') if len(s)>120 else s

# ---- P2: truncate long tool results before storing in history (source-level prevention) ----
_TOOL_TRUNCATE_LIMIT = 6000  # max chars per tool_result stored in history

def _truncate_tool_result(datastr, tool_name):
    """Truncate long tool result to prevent history bloat. Keeps head + tail with truncation marker.
    Only affects what gets stored in history (tool_results), not the real-time yield to user."""
    if len(datastr) <= _TOOL_TRUNCATE_LIMIT: return datastr
    head_len = int(_TOOL_TRUNCATE_LIMIT * 0.6)
    tail_len = _TOOL_TRUNCATE_LIMIT - head_len
    skipped = len(datastr) - head_len - tail_len
    return (datastr[:head_len] + f'\n[... truncated {skipped} chars ...]\n' + datastr[-tail_len:])
