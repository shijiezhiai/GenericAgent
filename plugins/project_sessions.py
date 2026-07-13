"""Project Sessions plugin — 会话按项目归档（旁路 hook，零侵入落盘）。

机制：注册 agent_after hook（agent_loop.py 一轮对话结束时触发一次）。
若项目模式激活，往 temp/projects/<名>/sessions/index.md 追加一条机械记录
（时间 / user_input 截断 / log 文件名指针 / 轮数 / exit_reason），并刷新
project_meta.md 的「最后活跃」行。

设计：纯机械落盘，零 LLM 调用——摘要留到 /project status 时由主 agent 现读
现摘（prompt owns logic）。无项目激活时直接 return，零开销。全程 fail-open：
任何异常写 stderr 后吞掉，绝不阻断 agent 主流程。

复用 project_mode 的 _active_project / _project_dir，不重复实现项目激活判定。
"""
import os
import re
import sys
from datetime import datetime

import plugins.hooks as hooks
from plugins.project_mode import _active_project, _project_dir


def _parent(ctx):
    """agent_after 的 ctx=locals()，取 handler.parent（agentmain 实例）。"""
    handler = ctx.get('handler') if isinstance(ctx, dict) else None
    return getattr(handler, 'parent', None)


def _sessions_index_path(name):
    return os.path.join(_project_dir(name), 'sessions', 'index.md')


def _meta_path(name):
    return os.path.join(_project_dir(name), 'project_meta.md')


def _truncate(s, n=80):
    """折叠换行/多空格后截断，避免破坏 index.md 的行结构。"""
    if not s:
        return ''
    s = ' '.join(str(s).split())
    return s[:n] + ('…' if len(s) > n else '')


@hooks.register('agent_after')
def archive_session(ctx):
    """一轮对话结束后，若项目激活，追加一条会话记录 + 刷新最后活跃时间。"""
    try:
        name = _active_project(ctx)
        if not name:
            return  # 普通模式，零开销

        parent = _parent(ctx)
        log_path = getattr(parent, 'log_path', None) or ''
        log_name = os.path.basename(log_path) if log_path else ''

        user_input = ctx.get('user_input') if isinstance(ctx, dict) else None
        turn = ctx.get('turn')
        exit_reason = ctx.get('exit_reason') or {}
        result = exit_reason.get('result', '') if isinstance(exit_reason, dict) else ''

        ts = datetime.now().strftime('%Y-%m-%d %H:%M')

        # 1. 追加会话记录到 sessions/index.md
        idx_path = _sessions_index_path(name)
        os.makedirs(os.path.dirname(idx_path), exist_ok=True)
        entry = (
            f"\n## {ts} | turns={turn if turn is not None else '?'} | result={result or 'n/a'}\n"
            f"- user: {_truncate(user_input)}\n"
            f"- log: {log_name}\n"
        )
        if not os.path.isfile(idx_path):
            with open(idx_path, 'w', encoding='utf-8') as f:
                f.write(
                    f"# {name} — 会话时间线\n"
                    f"（每轮 agent_after 自动追加；摘要用 /project status 现读现摘）\n"
                )
        with open(idx_path, 'a', encoding='utf-8') as f:
            f.write(entry)

        # 2. 刷新 project_meta.md 的「最后活跃」行（meta 不存在则跳过——由 agent 在 /project new 时维护）
        meta_path = _meta_path(name)
        if os.path.isfile(meta_path):
            with open(meta_path, encoding='utf-8') as f:
                data = f.read()
            new_line = f"- 最后活跃：{ts}"
            if re.search(r'^- 最后活跃：.*$', data, re.M):
                data = re.sub(r'^- 最后活跃：.*$', new_line, data, flags=re.M)
            else:
                data = data.rstrip() + '\n' + new_line + '\n'
            with open(meta_path, 'w', encoding='utf-8') as f:
                f.write(data)
    except Exception as e:
        sys.stderr.write(f"[project_sessions] archive failed: {e}\n")
