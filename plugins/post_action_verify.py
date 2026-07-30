"""
Post-Action Verification Hook
==============================
文件修改后自动触发相关测试/lint，将结果反馈给模型。

配置方式（优先级从高到低）：
1. 环境变量 GA_POST_ACTION_VERIFY=1/0 强制开关
2. 项目根目录 ga_config/post_action_verify.json 配置文件
3. 默认关闭

配置文件格式 (ga_config/post_action_verify.json):
{
    "enabled": true,
    "timeout": 30,
    "max_output_chars": 2000,
    "rules": [
        {
            "extensions": [".py"],
            "lint": "python -m py_compile {file}",
            "test": "python -m pytest {test_file} -x -q --tb=short 2>&1 | head -50"
        },
        {
            "extensions": [".js", ".ts"],
            "lint": "npx eslint {file} --no-eslintrc --rule '{{\"no-undef\": \"error\"}}' 2>&1 | head -30"
        }
    ],
    "test_discovery": {
        ".py": ["test_{basename}", "{basename}_test", "tests/test_{basename}"]
    }
}

设计原则：
- 轻量：仅语法检查(py_compile)默认开启，测试发现可选
- 安全：fail-open，任何异常不阻断 agent 流程
- 可配置：规则按扩展名匹配，支持自定义命令
"""

import os
import sys
import json
import re
import subprocess
import fnmatch

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CONFIG_PATH = os.path.join(_PROJECT_ROOT, 'ga_config', 'post_action_verify.json')

# 缓存配置，避免每次 turn 都读文件
_config_cache = None
_config_mtime = 0

# 需要检测的文件修改工具名
_FILE_MODIFY_TOOLS = frozenset({
    'file_write', 'file_patch',
    'mcp__global__filesystem__write_file',
    'mcp__global__filesystem__edit_file',
})

# 默认规则：仅做语法检查（极轻量，几乎无副作用）
_DEFAULT_RULES = [
    {
        "extensions": [".py"],
        "lint": "python -m py_compile {file}",
    },
]

# 跨 turn 的验证状态：filepath(绝对) -> 连续失败次数。用于重试计数与恢复信号。
_verify_state = {}

# P2b: 测试命令识别 + 失败特征（用于 code_run 输出结构化识别）
_TEST_CMD_RE = re.compile(
    r'(pytest|unittest|mocha|jest|vitest|npm\s+test|yarn\s+test|pnpm\s+test|'
    r'go\s+test|cargo\s+test|tox|make\s+test|python\s+-m\s+pytest|rspec)',
    re.I,
)
_TEST_FAIL_RE = [
    re.compile(r'\b\d+\s+(?:failed|errors?)\b', re.I),
    re.compile(r'\bFAILED\b', re.I),
    re.compile(r'\bfailing\b', re.I),
    re.compile(r'Tests:\s+\d+\s+failed', re.I),
    re.compile(r'tests?\s+failed', re.I),
    re.compile(r'✗', re.U),
    re.compile(r'not\s+ok\b', re.I),
]


def _load_config():
    """加载配置，带 mtime 缓存"""
    global _config_cache, _config_mtime

    # 环境变量强制开关
    env_val = os.environ.get('GA_POST_ACTION_VERIFY', '').strip().lower()
    if env_val in ('0', 'false', 'off'):
        return None  # 强制关闭
    if env_val in ('1', 'true', 'on'):
        # 强制开启，仍尝试读配置文件获取规则
        pass

    try:
        if os.path.isfile(_CONFIG_PATH):
            mt = os.path.getmtime(_CONFIG_PATH)
            if _config_cache is not None and mt == _config_mtime:
                cfg = _config_cache
            else:
                with open(_CONFIG_PATH, 'r', encoding='utf-8') as f:
                    cfg = json.load(f)
                _config_cache = cfg
                _config_mtime = mt
        else:
            cfg = {}
    except Exception:
        cfg = {}

    # 判断是否启用
    enabled = cfg.get('enabled', False)
    if env_val in ('1', 'true', 'on'):
        enabled = True
    if not enabled:
        return None

    return cfg


def _find_test_file(filepath, cfg):
    """根据 test_discovery 规则查找对应测试文件"""
    discovery = cfg.get('test_discovery', {})
    ext = os.path.splitext(filepath)[1]
    patterns = discovery.get(ext, [])
    if not patterns:
        return None

    basename = os.path.splitext(os.path.basename(filepath))[0]
    file_dir = os.path.dirname(filepath)
    # 也检查项目根下的 tests/ 目录
    search_dirs = [file_dir, _PROJECT_ROOT, os.path.join(_PROJECT_ROOT, 'tests')]

    for pat in patterns:
        candidate_name = pat.format(basename=basename) + ext
        for d in search_dirs:
            candidate = os.path.join(d, candidate_name)
            if os.path.isfile(candidate):
                return candidate
    return None


def _run_command(cmd, timeout, cwd=None):
    """运行命令，返回 (returncode, output_text)。

    用 `bash -o pipefail -c` 而非 shell=True：确保带 `| head/tail` 的管道
    在中间命令失败时整体返回非 0（否则 pytest/eslint/mvn 失败会被 tail 的
    退出码 0 掩盖，导致验证误判为通过）。
    """
    try:
        proc = subprocess.run(
            ["bash", "-o", "pipefail", "-c", cmd],
            capture_output=True, text=True,
            timeout=timeout, cwd=cwd or _PROJECT_ROOT
        )
        output = (proc.stdout or '') + (proc.stderr or '')
        return proc.returncode, output.strip()
    except subprocess.TimeoutExpired:
        return -1, f"[timeout after {timeout}s]"
    except Exception as e:
        return -1, f"[error: {e}]"


def _match_rule(filepath, rules):
    """找到匹配文件扩展名的规则"""
    ext = os.path.splitext(filepath)[1].lower()
    for rule in rules:
        if ext in [e.lower() for e in rule.get('extensions', [])]:
            return rule
    return None


def verify_edited_files(edited_files, cfg=None):
    """
    对修改的文件执行验证，返回反馈文本（空字符串表示无问题）。
    带跨 turn 的连续失败计数与恢复信号（P2a）。
    
    Args:
        edited_files: 被修改的文件路径列表
        cfg: 配置字典（None 则自动加载）
    
    Returns:
        str: 验证结果反馈文本，空字符串表示全部通过或无需验证
    """
    if cfg is None:
        cfg = _load_config()
    if cfg is None:
        return ''

    rules = cfg.get('rules', _DEFAULT_RULES)
    timeout = cfg.get('timeout', 30)
    max_output = cfg.get('max_output_chars', 2000)

    feedback_parts = []
    recovered_parts = []
    files_checked = set()

    for filepath in edited_files:
        if filepath in files_checked:
            continue
        files_checked.add(filepath)

        ap = os.path.abspath(filepath)
        if not os.path.isfile(ap):
            continue

        rule = _match_rule(ap, rules)
        if rule is None:
            continue

        file_feedback = []

        # 1. Lint / 语法检查
        lint_cmd = rule.get('lint')
        if lint_cmd:
            cmd = lint_cmd.format(file=ap)
            rc, output = _run_command(cmd, timeout)
            if rc != 0:
                file_feedback.append(f"❌ lint failed (exit {rc}):\n{_truncate(output, max_output // 2)}")

        # 2. 测试（可选，仅当配置了 test 命令或 test_discovery）
        test_cmd = rule.get('test')
        if test_cmd:
            test_file = _find_test_file(ap, cfg)
            if test_file:
                cmd = test_cmd.format(file=ap, test_file=test_file)
                rc, output = _run_command(cmd, timeout)
                if rc != 0:
                    file_feedback.append(f"❌ test failed (exit {rc}):\n{_truncate(output, max_output // 2)}")

        rel = os.path.relpath(ap, _PROJECT_ROOT)
        if file_feedback:
            # 连续失败计数 +1，注入 [Verify Failed xN] 强修复指令
            _verify_state[ap] = _verify_state.get(ap, 0) + 1
            n = _verify_state[ap]
            header = f"❌ [Verify Failed x{n}] `{rel}`:"
            feedback_parts.append(header + '\n' + '\n'.join(file_feedback))
        else:
            # 本轮干净：若此前有过失败，给出恢复通过信号并重置计数
            prev = _verify_state.get(ap, 0)
            if prev > 0:
                recovered_parts.append(f"✅ `{rel}` 验证已通过（此前连续 {prev} 次失败现已修复）")
            _verify_state[ap] = 0

    if not feedback_parts and not recovered_parts:
        return ''

    lines = []
    if feedback_parts:
        lines.append("🔍 [Post-Action Verification] 文件修改后自动检查发现问题：")
        lines.append('')
        lines.append('\n\n'.join(feedback_parts))
        lines.append('')
        lines.append("⚠️ 请立即修复上述问题，下一轮会自动重新验证（fix → re-verify 循环）。"
                     "在验证通过前不要判定任务完成、不要跳过验证直接交付。")
    if recovered_parts:
        lines.append('')
        lines.append('\n'.join(recovered_parts))
    result = '\n'.join(lines)
    return _truncate(result, max_output)


def _truncate(text, limit):
    """截断过长输出"""
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n[... truncated, total {len(text)} chars ...]"


def extract_edited_files(tool_calls):
    """
    从 tool_calls 列表中提取被修改的文件路径。
    
    Args:
        tool_calls: [{'tool_name': ..., 'args': {...}}, ...]
    
    Returns:
        list[str]: 被修改的文件路径列表
    """
    edited = []
    for tc in tool_calls:
        tn = tc.get('tool_name', '')
        args = tc.get('args', {})
        if tn in _FILE_MODIFY_TOOLS:
            path = args.get('path') or args.get('source')
            if path:
                edited.append(path)
    return edited


def run_test_output_verify(tool_calls, tool_results, next_prompt):
    """
    P2b: 识别模型用 code_run 自行运行的测试命令输出，若含失败则注入结构化验证信号。
    与 post_action_verify（文件修改触发）互补：此处针对模型主动跑测试的场景。

    Args:
        tool_calls: 当前 turn 的工具调用列表
        tool_results: 当前 turn 的工具结果列表（含 content）
        next_prompt: 当前的 next_prompt 文本

    Returns:
        str: 可能追加了测试失败信号的 next_prompt
    """
    try:
        # 1. 找出运行了测试命令的 code_run 调用
        ran_test = False
        for tc in tool_calls:
            if tc.get('tool_name') != 'code_run':
                continue
            args = tc.get('args', {})
            script = args.get('script') or ''
            if _TEST_CMD_RE.search(script):
                ran_test = True
                break
        if not ran_test:
            return next_prompt

        # 2. 在工具结果文本中查找失败特征
        blob = ''
        for tr in (tool_results or []):
            c = tr.get('content', '') if isinstance(tr, dict) else ''
            if isinstance(c, str):
                blob += '\n' + c
        if not blob:
            return next_prompt

        failed = any(rx.search(blob) for rx in _TEST_FAIL_RE)
        if not failed:
            return next_prompt

        signal = ("\n\n🔍 [Test Output Verification] 检测到你运行了测试命令且输出存在失败用例。"
                  "请分析上述失败输出，定位根因并修复，然后重新运行测试直至全部通过"
                  "（fix → re-verify 循环）。验证通过前不要判定任务完成。")
        return next_prompt + signal
    except Exception:
        return next_prompt


def run_post_action_verify(tool_calls, next_prompt):
    """
    agent_loop 集成入口。检测文件修改并运行验证，返回增强后的 next_prompt。
    
    Args:
        tool_calls: 当前 turn 的工具调用列表
        next_prompt: 当前的 next_prompt 文本
    
    Returns:
        str: 可能追加了验证反馈的 next_prompt
    """
    try:
        cfg = _load_config()
        if cfg is None:
            return next_prompt

        edited_files = extract_edited_files(tool_calls)
        if not edited_files:
            return next_prompt

        feedback = verify_edited_files(edited_files, cfg)
        if feedback:
            next_prompt += '\n\n' + feedback
    except Exception as e:
        # fail-open: 任何异常不阻断流程
        sys.stderr.write(f"[post_action_verify] error: {e}\n")

    return next_prompt
