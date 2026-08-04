import sys, os, re, json, time, threading, importlib, webbrowser
from datetime import datetime
from pathlib import Path
import tempfile, traceback, subprocess, itertools, collections, difflib, shutil
if sys.stdout is None: sys.stdout = open(os.devnull, "w")
if sys.stderr is None: sys.stderr = open(os.devnull, "w")
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from agent_loop import BaseHandler, StepOutcome, json_default
script_dir = os.path.dirname(os.path.abspath(__file__))

def safe_print(*args, **kwargs):
    # 一律输出到 stderr。在桌面内核(kernel_server)中 sys.stdout 是 JSON-RPC 管道，
    # 任何写向 stdout 的内容都会污染协议导致会话中断；kernel 自身日志也走 stderr。
    try: print(*args, file=sys.stderr, **kwargs)
    except: pass

def code_run(code, code_type="python", timeout=60, cwd=None, code_cwd=None, stop_signal=None, maxlen=10000, myprint=safe_print):
    """代码执行器
    python: 运行复杂的 .py 脚本（文件模式）
    powershell/bash: 运行单行指令（命令模式）
    优先使用python，仅在必要系统操作时使用powershell"""
    preview = (code[:60].replace('\n', ' ') + '...') if len(code) > 60 else code.strip()
    cwd = cwd or os.path.join(script_dir, 'temp'); tmp_path = None
    yield f"[Action] Running {code_type} in {os.path.basename(cwd)}: {preview}\n"
    if code_type in ["python", "py"]:
        tmp_dir = code_cwd
        # FDA/TCC 写限制: workspace 可能不可写, 先探测可写性, 不可写则 fallback 到 GA temp
        try:
            with tempfile.NamedTemporaryFile(suffix=".ai.py", dir=tmp_dir):
                pass  # 出 with 自动关闭并删除, 仅探测 tmp_dir 可写性
        except (PermissionError, OSError):
            tmp_dir = os.path.join(script_dir, 'temp')
            os.makedirs(tmp_dir, exist_ok=True)
        # 正式创建脚本文件: 保持打开以便写入 header+code, 写完再 close
        tmp_file = tempfile.NamedTemporaryFile(suffix=".ai.py", delete=False, mode='w', encoding='utf-8', dir=tmp_dir)
        cr_header = os.path.join(script_dir, 'assets', 'code_run_header.py')
        if os.path.exists(cr_header): tmp_file.write(open(cr_header, encoding='utf-8').read())
        tmp_file.write(code)
        tmp_path = tmp_file.name
        tmp_file.close()
        cmd = [sys.executable, "-X", "utf8", "-u", tmp_path]
    elif code_type in ["powershell", "bash", "sh", "shell", "ps1", "pwsh"]:
        if os.name == 'nt':
            _ps = "pwsh" if shutil.which("pwsh") else "powershell"
            utf8_prefix = "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; "
            cmd = [_ps, "-NoProfile", "-NonInteractive", "-Command", utf8_prefix + code]
        else: cmd = ["bash", "-c", code]
    else:
        return {"status": "error", "msg": f"不支持的类型: {code_type}"}
    myprint("code run output:")
    startupinfo = None
    if os.name == 'nt':
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = 0 # SW_HIDE
    full_stdout = []

    def stream_reader(proc, logs):
        try:
            for line_bytes in iter(proc.stdout.readline, b''):
                try: line = line_bytes.decode('utf-8')
                except UnicodeDecodeError: line = line_bytes.decode('gbk', errors='ignore')
                logs.append(line)
                myprint(line, end="")
        except: pass

    try:
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            bufsize=0, cwd=cwd, startupinfo=startupinfo,
            creationflags=0x08000000 if os.name == 'nt' else 0
        )
        start_t = time.time()
        t = threading.Thread(target=stream_reader, args=(process, full_stdout), daemon=True)
        t.start()

        while t.is_alive():
            istimeout = time.time() - start_t > timeout
            if istimeout or stop_signal:
                process.kill()
                myprint("[Debug] Process killed due to timeout or stop signal.")
                if istimeout: full_stdout.append("\n[Timeout Error] 超时强制终止")
                else: full_stdout.append("\n[Stopped] 用户强制终止")
                break
            time.sleep(1)

        t.join(timeout=1)
        exit_code = process.poll()

        stdout_str = "".join(full_stdout)
        status = "success" if exit_code == 0 else "error"
        status_icon = "✅" if exit_code == 0 else "❌"
        if exit_code is None: status_icon = "⏳" 
        output_snippet = smart_format(stdout_str, max_str_len=600, omit_str='\n\n[omitted long output]\n\n')
        output_snippet = re.sub(r'`{4,}', lambda m: m.group(0)[:3] + '\u200b' + m.group(0)[3:], output_snippet)
        yield f"[Status] {status_icon} Exit Code: {exit_code}\n[Stdout]\n{output_snippet}\n"
        if process.stdout: threading.Thread(target=process.stdout.close, daemon=True).start()
        return {
            "status": status,
            "stdout": smart_format(stdout_str, max_str_len=maxlen, omit_str='\n\n[omitted long output]\n\n'),
            "exit_code": exit_code
        }
    except Exception as e:
        if 'process' in locals(): process.kill()
        return {"status": "error", "msg": str(e)}
    finally:
        if code_type == "python" and tmp_path and os.path.exists(tmp_path): os.remove(tmp_path)


def ask_user(question, candidates=None):
    """question: 向用户提出的问题。candidates: 可选的候选项列表"""
    return {"status": "INTERRUPT", "intent": "HUMAN_INTERVENTION",
        "data": {"question": question, "candidates": candidates or []}}

import simphtml
driver = None
def first_init_driver():
    global driver
    from TMWebDriver import TMWebDriver
    driver = TMWebDriver()
    for i in range(7):
        time.sleep(2)
        sess = driver.get_all_sessions()
        if len(sess) > 0: break
        if i == 4: webbrowser.open("https://example.com")

def web_scan(tabs_only=False, switch_tab_id=None, text_only=False, maxlen=35000):
    """获取当前页面的简化HTML内容和标签页列表。注意：简化过程会过滤边栏、浮动元素等非主体内容。
    tabs_only: 仅返回标签页列表，不获取HTML内容（节省token）。
    switch_tab_id: 可选参数，如果提供，则在扫描前切换到该标签页。
    应当多用execute_js，少全量观察html"""
    global driver
    try:
        if driver is None: first_init_driver()
        if len(driver.get_all_sessions()) == 0:
            return {"status": "error", "msg": "没有可用的浏览器标签页，查L3记忆分析原因。"}
        tabs = []
        for sess in driver.get_all_sessions(): 
            sess.pop('connected_at', None)
            sess.pop('type', None)
            sess['url'] = sess.get('url', '')[:50] + ("..." if len(sess.get('url', '')) > 50 else "")
            tabs.append(sess)
        if switch_tab_id: driver.default_session_id = switch_tab_id
        result = {
            "status": "success",
            "metadata": {
                "tabs_count": len(tabs), "tabs": tabs,
                "active_tab": driver.default_session_id
            }
        }
        if not tabs_only: 
            importlib.reload(simphtml); result["content"] = simphtml.get_html(driver, cutlist=True, maxchars=maxlen, text_only=text_only)
            if text_only: result['content'] = smart_format(result['content'], max_str_len=maxlen//3, omit_str='\n\n[omitted long content]\n\n')
        return result
    except Exception as e:
        return {"status": "error", "msg": format_error(e)}
    
def format_error(e):
    exc_type, exc_value, exc_traceback = sys.exc_info()
    tb = traceback.extract_tb(exc_traceback)
    if tb:
        f = tb[-1]
        fname = os.path.basename(f.filename)
        return f"{exc_type.__name__}: {str(e)} @ {fname}:{f.lineno}, {f.name} -> `{f.line}`"
    return f"{exc_type.__name__}: {str(e)}"

def log_memory_access(path):
    if 'memory' not in path: return
    stats_file = os.path.join(script_dir, 'memory/file_access_stats.json')
    try:
        with open(stats_file, 'r', encoding='utf-8') as f: stats = json.load(f)
    except: stats = {}
    fname = os.path.basename(path)
    stats[fname] = {'count': stats.get(fname, {}).get('count', 0) + 1, 'last': datetime.now().strftime('%Y-%m-%d')}
    with open(stats_file, 'w', encoding='utf-8') as f: json.dump(stats, f, indent=2, ensure_ascii=False)

def web_execute_js(script, switch_tab_id=None, no_monitor=False):
    """执行 JS 脚本来控制浏览器，并捕获结果和页面变化"""
    global driver
    try:
        if driver is None: first_init_driver()
        if len(driver.get_all_sessions()) == 0: return {"status": "error", "msg": "没有可用的浏览器标签页，查L3记忆分析原因。"}
        if switch_tab_id: driver.default_session_id = switch_tab_id
        result = simphtml.execute_js_rich(script, driver, no_monitor=no_monitor)
        return result
    except Exception as e: return {"status": "error", "msg": format_error(e)}

def expand_file_refs(text, base_dir=None):
    """展开文本中的 {{file:路径:起始行:结束行}} 引用为实际文件内容。
    可与普通文本混排。展开失败抛 ValueError。
    base_dir: 相对路径的基准目录，默认为进程 cwd"""
    pattern = r'\{\{file:(.+?):(\d+):(\d+)\}\}'
    def replacer(match):
        path, start, end = match.group(1), int(match.group(2)), int(match.group(3))
        path = os.path.abspath(os.path.join(base_dir or '.', path))
        if not os.path.isfile(path): raise ValueError(f"引用文件不存在: {path}")
        with open(path, 'r', encoding='utf-8') as f: lines = f.readlines()
        if start < 1 or end > len(lines) or start > end: raise ValueError(f"行号越界: {path} 共{len(lines)}行, 请求{start}-{end}")
        return ''.join(lines[start-1:end])
    return re.sub(pattern, replacer, text)
    
def file_patch(path: str, old_content: str, new_content: str):
    """在文件中寻找唯一的 old_content 块并替换为 new_content"""
    path = str(Path(path).resolve())
    try:
        if not os.path.exists(path): return {"status": "error", "msg": "文件不存在"}
        with open(path, 'r', encoding='utf-8') as f: full_text = f.read()
        if not old_content: return {"status": "error", "msg": "old_content 为空，请确认 arguments"}
        count = full_text.count(old_content)
        if count == 0: return {"status": "error", "msg": "未找到匹配的旧文本块，建议：先用 file_read 确认当前内容，再分小段进行 patch。若多次失败则询问用户，严禁自行使用 overwrite 或代码替换。"}
        if count > 1: return {"status": "error", "msg": f"找到 {count} 处匹配，无法确定唯一位置。请提供更长、更具体的旧文本块以确保唯一性。建议：包含上下文行来增强特征，或分小段逐个修改。"}
        updated_text = full_text.replace(old_content, new_content)
        with open(path, 'w', encoding='utf-8') as f: f.write(updated_text)
        return {"status": "success", "msg": "文件局部修改成功"}
    except Exception as e: return {"status": "error", "msg": str(e)}

_read_dirs = set()
def _scan_files(base, depth=2):
    try:
        for e in os.scandir(base):
            if e.is_file(): yield (e.name, e.path)
            elif depth > 0 and e.is_dir(follow_symlinks=False): yield from _scan_files(e.path, depth - 1)
    except (PermissionError, OSError): pass
def file_read(path, start=1, keyword=None, count=100, show_linenos=True):
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            stream = ((i, l.rstrip('\r\n')) for i, l in enumerate(f, 1))
            stream = itertools.dropwhile(lambda x: x[0] < start, stream)
            if keyword:
                before = collections.deque(maxlen=count//3)
                for i, l in stream:
                    if keyword.lower() in l.lower():
                        res = list(before) + [(i, l)] + list(itertools.islice(stream, count - len(before) - 1))
                        break
                    before.append((i, l))
                else: return f"Keyword '{keyword}' not found after line {start}. Falling back to content from line {start}:\n\n" \
                               + file_read(path, start, None, count, show_linenos)
            else: res = list(itertools.islice(stream, count))
            realcnt = len(res); L_MAX = min(max(100, 256000//max(realcnt,1)), 8000); TAG = " ... [TRUNCATED]"
            remaining = sum(1 for _ in itertools.islice(stream, 5000))
            total_lines = (res[0][0] - 1 if res else start - 1) + realcnt + remaining
            tl_str = f"{total_lines}+" if remaining >= 5000 else str(total_lines)
            partial = total_lines > realcnt
            total_tag = f"[FILE] {tl_str} lines" + (f" | PARTIAL showing {realcnt}; assess need for more" if partial else "") + "\n"
            res = [(i, l if len(l) <= L_MAX else l[:L_MAX] + TAG) for i, l in res]
            result = "\n".join(f"{i}|{l}" if show_linenos else l for i, l in res)
            if show_linenos: result = total_tag + result
            elif partial: result += f"\n\n[FILE PARTIAL: showing {realcnt}/{tl_str} lines; assess need for more]"
            _read_dirs.add(os.path.dirname(os.path.abspath(path)))
            return result
    except FileNotFoundError:
        msg = f"Error: File not found: {path}"
        try:
            tgt = os.path.basename(path); parent = os.path.dirname(os.path.abspath(path)); scan = os.path.dirname(parent)
            roots = [parent, scan] + [d for d in _read_dirs if not d.startswith(scan)]
            cands = list(dict.fromkeys(itertools.islice((c for base in roots for c in _scan_files(base)), 2000)))
            top = sorted([(difflib.SequenceMatcher(None, tgt.lower(), c[0].lower()).ratio(), c) for c in cands[:2000]], key=lambda x: -x[0])[:5]
            top = [(s, c) for s, c in top if s > 0.3]
            if top: msg += "\n\nDid you mean:\n" + "\n".join(f"  {c[1]}  ({s:.0%})" for s, c in top)
        except Exception: pass
        return msg
    except Exception as e: return f"Error: {str(e)}"

def smart_format(data, max_str_len=100, omit_str=' ... '):
    if not isinstance(data, str): data = str(data)
    if len(data) < max_str_len + len(omit_str)*2: return data
    return f"{data[:max_str_len//2]}{omit_str}{data[-max_str_len//2:]}"

# ── git_checkpoint 辅助函数 ──────────────────────────────────────────────────
_GIT_TIMEOUT = 30
_GIT_CKPT_PREFIX = "ga-ckpt-"

def _git_run(args, repo_dir, timeout=_GIT_TIMEOUT, check=True, input_data=None):
    """Run a git command in repo_dir. Returns (returncode, stdout, stderr)."""
    try:
        proc = subprocess.run(
            ["git"] + args, cwd=repo_dir, capture_output=True,
            text=True, timeout=timeout, input=input_data,
        )
    except FileNotFoundError:
        raise FileNotFoundError("git 可执行文件未找到。请安装 git 并确保在 PATH 中。")
    if check and proc.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed (rc={proc.returncode}): {proc.stderr.strip() or proc.stdout.strip()}")
    return proc.returncode, proc.stdout, proc.stderr

def _git_parse_status_porcelain_v2(text):
    """Parse git status --porcelain=v2 -b output into structured dict."""
    branch, upstream, staged, unstaged, untracked = "HEAD", "", [], [], []
    for line in text.splitlines():
        if not line: continue
        if line.startswith("# branch.head "): branch = line.split(" ", 2)[2]
        elif line.startswith("# branch.upstream "): upstream = line.split(" ", 2)[2]
        elif line.startswith("# branch.ab "):
            parts = line.split(" ")
            if len(parts) >= 4: upstream += f" (ahead {parts[2]}, behind {parts[3]})"
        elif line.startswith("1 ") or line.startswith("2 "):
            parts = line.split(" ")
            xy, path = parts[1], parts[-1]
            entry = {"path": path, "index": xy[0], "work": xy[1]}
            if xy[0] != ".": staged.append(entry)
            if xy[1] != ".": unstaged.append(entry)
        elif line.startswith("? "): untracked.append({"path": line[2:]})
    return {"branch": branch, "upstream": upstream, "staged": staged, "unstaged": unstaged, "untracked": untracked}

def git_status(repo_dir):
    rc, out, err = _git_run(["status", "--porcelain=v2", "-b", "--untracked-files=all"], repo_dir, check=False)
    if rc != 0: return {"status": "error", "msg": err.strip() or out.strip()}
    parsed = _git_parse_status_porcelain_v2(out)
    parsed["summary"] = f"{len(parsed['staged'])} staged, {len(parsed['unstaged'])} modified, {len(parsed['untracked'])} untracked"
    parsed["status"] = "success"
    return parsed

def git_diff(repo_dir, files=None, staged=False):
    args = ["diff", "--no-color", "--stat"]
    if staged: args.insert(1, "--staged")
    if files:
        args += ["--"] + (files if isinstance(files, list) else [files])
    rc, stat_out, err = _git_run(args, repo_dir, check=False)
    if rc != 0: return {"status": "error", "msg": err.strip() or stat_out.strip()}
    body_args = ["diff", "--no-color"]
    if staged: body_args.insert(1, "--staged")
    if files: body_args += ["--"] + (files if isinstance(files, list) else [files])
    _, body_out, _ = _git_run(body_args, repo_dir, check=False)
    if not stat_out.strip():
        return {"status": "success", "stat": "", "diff": "", "msg": "无变更（工作区干净）"}
    return {"status": "success", "stat": stat_out.strip(), "diff": body_out[:6000] + ("\n... [truncated]" if len(body_out) > 6000 else "")}

def _git_block_on_protected_branch(repo_dir, op):
    rc, out, _ = _git_run(["rev-parse", "--abbrev-ref", "HEAD"], repo_dir, check=False)
    branch = (out or "").strip()
    if branch in ("main", "master"):
        return {"status": "error", "msg": f"⚠️ 当前在受保护分支 {branch}！{op} 之前必须先 git_checkpoint(action=branch) 切到工作分支。"}
    return None

def git_commit(repo_dir, message, files=None, add_all=True, no_confirm=False):
    if not message: return {"status": "error", "msg": "message 必填（commit 描述）"}
    guard = _git_block_on_protected_branch(repo_dir, "commit")
    if guard: return guard
    if add_all: _git_run(["add", "-A"], repo_dir)
    elif files: _git_run(["add", "--"] + (files if isinstance(files, list) else [files]), repo_dir)
    rc, out, err = _git_run(["diff", "--cached", "--quiet"], repo_dir, check=False)
    if rc == 0: return {"status": "error", "msg": "无 staged 变更，无需 commit。"}
    rc, out, err = _git_run(["commit", "-m", message], repo_dir)
    sha = ""
    rc2, log_out, _ = _git_run(["log", "-1", "--format=%H"], repo_dir, check=False)
    if rc2 == 0: sha = log_out.strip()
    return {"status": "success", "sha": sha, "short": sha[:7], "msg": out.strip()}

def git_checkpoint_create(repo_dir, message, files=None, add_all=True):
    """Commit + tag with ga-ckpt-<ts>-<shortsha>. message is stored as tag annotation.
    Timestamp precision: microseconds (YYYYMMDD-HHMMSSffffff) so list ordering is deterministic."""
    if not message: message = f"auto checkpoint @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    commit_res = git_commit(repo_dir, message=message, files=files, add_all=add_all)
    if commit_res.get("status") != "success": return commit_res
    sha = commit_res["sha"]
    ts = datetime.now().strftime("%Y%m%d-%H%M%S%f")
    tag_name = f"{_GIT_CKPT_PREFIX}{ts}-{sha[:7]}"
    _git_run(["tag", "-a", tag_name, sha, "-m", message], repo_dir)
    return {"status": "success", "checkpoint_id": tag_name, "sha": sha, "message": message, "created": ts}

def git_checkpoint_list(repo_dir, max_count=20):
    # tag 名前缀是微秒精度时间戳 (ga-ckpt-YYYYMMDD-HHMMSSffffff-)，v:refname 倒序 = 时间倒序
    rc, out, err = _git_run(["tag", "-l", f"{_GIT_CKPT_PREFIX}*", "--sort=-v:refname",
                             f"--format=%(refname:short)|%(creatordate:iso)|%(subject)"], repo_dir, check=False)
    if rc != 0: return {"status": "error", "msg": err.strip() or out.strip()}
    if not out.strip(): return {"status": "success", "checkpoints": [], "msg": "尚无 checkpoint"}
    items = []
    for line in out.splitlines()[:max_count]:
        parts = line.split("|", 2)
        if len(parts) < 3: continue
        tag, ts, subj = parts
        rc2, sha_out, _ = _git_run(["rev-list", "-n", "1", tag], repo_dir, check=False)
        sha = sha_out.strip()
        rc3, files_out, _ = _git_run(["show", "--stat", "--format=", tag], repo_dir, check=False)
        n_files = sum(1 for ln in files_out.splitlines() if ln.strip().startswith("|")) if rc3 == 0 else 0
        items.append({"id": tag, "sha": sha, "short": sha[:7], "time": ts, "message": subj, "files_changed": n_files})
    return {"status": "success", "count": len(items), "checkpoints": items}

def git_checkpoint_restore(repo_dir, ckpt_id, no_confirm=False):
    if not ckpt_id.startswith(_GIT_CKPT_PREFIX): ckpt_id = _GIT_CKPT_PREFIX + ckpt_id
    rc, sha, _ = _git_run(["rev-parse", "--verify", ckpt_id], repo_dir, check=False)
    if rc != 0: return {"status": "error", "msg": f"checkpoint 不存在: {ckpt_id}。先 action=list 看可用 id。"}
    if not no_confirm:
        return {"status": "needs_confirm", "msg": f"⚠️ 危险操作：将 reset --hard 到 {ckpt_id} ({sha.strip()[:7]})。当前未提交变更将丢失。请传 no_confirm=true 确认执行。reflog 仍可恢复: git reflog | grep {sha.strip()[:7]}", "checkpoint_id": ckpt_id, "sha": sha.strip()}
    rc, head_before, _ = _git_run(["rev-parse", "HEAD"], repo_dir, check=False)
    _git_run(["reset", "--hard", ckpt_id], repo_dir)
    log_dir = os.path.join(script_dir, ".workbuddy")
    os.makedirs(log_dir, exist_ok=True)
    with open(os.path.join(log_dir, "git_restore.log"), "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()}\t{repo_dir}\t{ckpt_id}\tfrom={head_before.strip()}\n")
    return {"status": "success", "checkpoint_id": ckpt_id, "restored_to": sha.strip(), "previous_head": head_before.strip(),
            "note": "如需回退本次 restore: git reset --hard " + head_before.strip()}

def git_branch_op(repo_dir, name=None, create_from=None):
    rc, current, _ = _git_run(["rev-parse", "--abbrev-ref", "HEAD"], repo_dir, check=False)
    cur = (current or "").strip()
    if not name: return {"status": "success", "current": cur,
                         "branches": _git_run(["branch", "--format=%(refname:short)|%(upstream:short)"], repo_dir, check=False)[1].strip().splitlines()}
    if name == cur: return {"status": "error", "msg": f"已在分支 {name}"}
    rc, _, _ = _git_run(["rev-parse", "--verify", f"refs/heads/{name}"], repo_dir, check=False)
    if rc == 0:
        _git_run(["checkout", name], repo_dir)
        return {"status": "success", "action": "switched", "branch": name}
    base = create_from or "HEAD"
    rc, out, err = _git_run(["checkout", "-b", name, base], repo_dir, check=False)
    if rc != 0: return {"status": "error", "msg": err.strip() or out.strip()}
    return {"status": "success", "action": "created_and_switched", "branch": name, "from": base}

def git_log(repo_dir, max_count=15):
    rc, out, _ = _git_run(["log", f"-n{max_count}", "--oneline", "--graph", "--decorate"], repo_dir, check=False)
    if rc != 0: return {"status": "error", "msg": "log 失败"}
    return {"status": "success", "log": out.strip() or "（无提交）"}

def _git_pick_remote(repo_dir, preferred=None):
    rc, out, _ = _git_run(["remote"], repo_dir, check=False)
    remotes = [r.strip() for r in (out or "").splitlines() if r.strip()]
    if preferred and preferred in remotes: return preferred
    for r in ("myfork", "origin"):
        if r in remotes: return r
    return remotes[0] if remotes else None

def _github_token():
    try:
        import mykey
        for k in ("github_token", "gh_token", "github_pat"):
            v = getattr(mykey, k, None)
            if v: return v, "github"
        for k in ("gitlab_token", "glab_token"):
            v = getattr(mykey, k, None)
            if v: return v, "gitlab"
    except Exception: pass
    env = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    return (env, "github") if env else (None, None)

def _git_parse_repo_slug(remote_url):
    """Parse 'git@github.com:owner/repo.git' or 'https://github.com/owner/repo.git' -> ('owner/repo', 'github')."""
    s = remote_url.strip()
    if s.startswith("git@"): s = s.split(":", 1)[1]
    elif "://" in s: s = s.split("://", 1)[1]
    s = s.rstrip("/")
    if s.endswith(".git"): s = s[:-4]
    host = "github" if "github" in remote_url.lower() else ("gitlab" if "gitlab" in remote_url.lower() else "unknown")
    return s, host

def git_create_pr(repo_dir, title, body, target=None, remote=None):
    rc, br_out, _ = _git_run(["rev-parse", "--abbrev-ref", "HEAD"], repo_dir, check=False)
    head = (br_out or "").strip()
    if head in ("main", "master", "HEAD", ""): return {"status": "error", "msg": f"当前分支是 {head or '空'}，无法创建 PR。restore 之后 HEAD 可能变成 detached；请先 git_checkpoint(action=branch) 切到工作分支。"}
    if not title: return {"status": "error", "msg": "pr_title 必填"}
    if not target:
        rc, def_out, _ = _git_run(["symbolic-ref", "refs/remotes/origin/HEAD"], repo_dir, check=False)
        target = (def_out or "").strip().split("/")[-1] or "main"
    push_remote = _git_pick_remote(repo_dir, remote)
    if not push_remote: return {"status": "error", "msg": "未配置任何 remote，无法 push。"}
    _git_run(["push", "-u", push_remote, head], repo_dir, timeout=60)
    gh_path = shutil.which("gh")
    if gh_path:
        try:
            proc = subprocess.run([gh_path, "pr", "create", "--title", title, "--body", body or "", "--base", target, "--head", head],
                                  cwd=repo_dir, capture_output=True, text=True, timeout=60)
            if proc.returncode == 0: return {"status": "success", "method": "gh", "url": proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else "", "head": head, "target": target, "remote": push_remote}
        except Exception: pass
    token, host = _github_token()
    if not token: return {"status": "error", "msg": f"已 push 到 {push_remote}，但创建 PR 失败：未找到 gh CLI 也未配置 token。手动访问: https://github.com/<owner>/<repo>/compare/{target}...{head}?expand=1", "head": head, "target": target, "remote": push_remote}
    rc, url_out, _ = _git_run(["remote", "get-url", push_remote], repo_dir, check=False)
    slug, host_parsed = _git_parse_repo_slug(url_out)
    if "github" not in (host_parsed or "") + (host or ""):
        return {"status": "error", "msg": f"仅支持 GitHub/GitLab PR 自动创建，当前 remote host={host_parsed}。请用 gh 或网页手动创建。"}
    import requests as _req
    api = f"https://api.github.com/repos/{slug}/pulls"
    r = _req.post(api, headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}, json={"title": title, "body": body or "", "head": head, "base": target}, timeout=30)
    if r.status_code in (200, 201):
        return {"status": "success", "method": "rest_api", "url": r.json().get("html_url", ""), "number": r.json().get("number"), "head": head, "target": target, "remote": push_remote}
    return {"status": "error", "msg": f"GitHub API 失败 (HTTP {r.status_code}): {r.text[:500]}", "head": head, "target": target}

def consume_file(dr, file):
    if dr and os.path.exists(os.path.join(dr, file)): 
        with open(os.path.join(dr, file), encoding='utf-8', errors='replace') as f: content = f.read()
        os.remove(os.path.join(dr, file))
        return content

class GenericAgentHandler(BaseHandler):
    '''Generic Agent 工具库，包含多种工具的实现。工具函数自动加上了 do_ 前缀。实际工具名没有前缀。'''
    def __init__(self, parent, last_history=None, cwd='./temp'):
        self.parent = parent
        self.working = {}
        self.cwd = cwd;  self.current_turn = 0
        self.history_info = last_history if last_history else []
        self.code_stop_signal = []
        self._done_hooks = []
        self.print = safe_print

    def _get_abs_path(self, path):
        if not path: return ""
        return os.path.abspath(os.path.join(self.cwd, path))   

    def _extract_code_block(self, response, code_type):
        code_type = {'python':'python|py', 'powershell':'powershell|ps1|pwsh', 'bash':'bash|sh|shell'}.get(code_type, re.escape(code_type))
        matches = re.findall(rf"```(?:{code_type})\n(.*?)\n```", response.content, re.DOTALL)
        return matches[-1].strip() if matches else None

    def do_code_run(self, args, response):
        '''执行代码片段，有长度限制，不允许代码中放大量数据，如有需要应当通过文件读取进行。'''
        code_type = args.get("type", "python")
        code = args.get("code") or args.get("script")
        if not code:
            code = self._extract_code_block(response, code_type)
            if not code: return StepOutcome("[Error] Code missing. Must use reply code block or 'script' arg.", next_prompt="\n")
        try: timeout = int(args.get("timeout", 60))
        except: timeout = 60
        raw_path = os.path.join(self.cwd, args.get("cwd", './'))
        cwd = os.path.normpath(os.path.abspath(raw_path))
        code_cwd = os.path.normpath(self.cwd)
        maxlen = 10000 // args.get('_tool_num', 1)
        if code_type == 'python' and args.get("inline_eval"):
            ns = {'handler':self, 'parent':self.parent, 'history':json.dumps(self.parent.llmclient.backend.history)}
            old_cwd = os.getcwd()
            try:
                os.chdir(cwd)
                try:
                    try: result = repr(eval(code, ns))
                    except SyntaxError: exec(code, ns); result = ns.get('_r', 'OK')
                except Exception as e: result = f'Error: {e}'
            finally: os.chdir(old_cwd)
        else: result = yield from code_run(code, code_type, timeout, cwd, code_cwd=code_cwd, stop_signal=self.code_stop_signal, maxlen=maxlen, myprint=self.print)
        next_prompt = self._get_anchor_prompt(skip=args.get('_index', 0) > 0)
        return StepOutcome(result, next_prompt=next_prompt)
    
    def do_ask_user(self, args, response):
        question = args.get("question", "请提供输入：")
        candidates = args.get("candidates", [])
        result = ask_user(question, candidates)
        yield f"Waiting for your answer ...\n"
        return StepOutcome(result, next_prompt="", should_exit=True)
    
    def do_web_scan(self, args, response):
        '''获取当前页面内容和标签页列表。也可用于切换标签页。
        注意：HTML经过简化，边栏/浮动元素等可能被过滤。如需查看被过滤的内容请用execute_js。
        tabs_only=true时仅返回标签页列表，不获取HTML（省token）'''
        tabs_only = args.get("tabs_only", False)
        switch_tab_id = args.get("switch_tab_id", None)
        text_only = args.get("text_only", False)
        maxlen = 35000 // args.get('_tool_num', 1)
        result = web_scan(tabs_only=tabs_only, switch_tab_id=switch_tab_id, text_only=text_only, maxlen=maxlen)
        content = result.pop("content", None)
        yield f'[Info] {str(result)}\n'
        if content: result = json.dumps(result, ensure_ascii=False, default=json_default) + f"\n```html\n{content}\n```"
        next_prompt = "\n"
        return StepOutcome(result, next_prompt=next_prompt)
    
    def do_web_execute_js(self, args, response):
        '''web情况下的优先使用工具，执行任何js达成对浏览器的*完全*控制。支持将结果保存到文件供后续读取分析。'''
        script = args.get("script", "") or self._extract_code_block(response, "javascript")
        if not script: return StepOutcome("[Error] Script missing. Use ```javascript block or 'script' arg.", next_prompt="\n")
        abs_path = self._get_abs_path(script.strip())
        if os.path.isfile(abs_path):
            with open(abs_path, 'r', encoding='utf-8') as f: script = f.read()
        save_to_file = args.get("save_to_file", "")
        switch_tab_id = args.get("switch_tab_id") or args.get("tab_id")
        no_monitor = args.get("no_monitor", False)
        result = web_execute_js(script, switch_tab_id=switch_tab_id, no_monitor=no_monitor)
        if save_to_file and "js_return" in result:
            content = str(result["js_return"] or '')
            abs_path = self._get_abs_path(save_to_file)
            result["js_return"] = smart_format(content, max_str_len=170)
            try:
                with open(abs_path, 'w', encoding='utf-8') as f: f.write(str(content))
                result["js_return"] += f"\n\n[已保存完整内容到 {abs_path}]"
            except: result['js_return'] += f"\n\n[保存失败，无法写入文件 {abs_path}]"
        show = smart_format(json.dumps(result, ensure_ascii=False, indent=2, default=json_default), max_str_len=300)
        self.print("Web Execute JS Result:", show)
        yield f"JS 执行结果:\n{show}\n"
        next_prompt = self._get_anchor_prompt(skip=args.get('_index', 0) > 0)
        result = json.dumps(result, ensure_ascii=False, default=json_default)
        maxlen = 8000 // args.get('_tool_num', 1)
        return StepOutcome(smart_format(result, max_str_len=maxlen), next_prompt=next_prompt)
    
    def do_file_patch(self, args, response):
        path = self._get_abs_path(args.get("path", ""))
        yield f"[Action] Patching file: {path}\n"
        old_content = args.get("old_content", "")
        new_content = args.get("new_content", "")
        try: new_content = expand_file_refs(new_content, base_dir=self.cwd)
        except ValueError as e:
            yield f"[Status] ❌ 引用展开失败: {e}\n"
            return StepOutcome({"status": "error", "msg": str(e)}, next_prompt="\n")
        result = file_patch(path, old_content, new_content)
        yield f"\n{str(result)}\n"
        next_prompt = self._get_anchor_prompt(skip=args.get('_index', 0) > 0)
        return StepOutcome(result, next_prompt=next_prompt)
    
    def do_file_write(self, args, response):
        '''用于对整个文件的大量处理，精细修改要用file_patch。
        需要将要写入的内容放在<file_content>标签内，或者放在代码块中'''
        path = self._get_abs_path(args.get("path", ""))
        mode = args.get("mode", "overwrite")  # overwrite/append/prepend
        action_str = {"prepend": "Prepending to", "append": "Appending to"}.get(mode, "Overwriting")
        yield f"[Action] {action_str} file: {os.path.basename(path)}\n"

        def extract_robust_content(text):
            tags = re.findall(r"<file_content[^>]*>(.*?)</file_content>", text, re.DOTALL)
            if tags: return tags[-1].strip()
            blocks = re.findall(r"```[^\n]*\n([\s\S]*?)```", text)
            if blocks: return blocks[-1].strip()
            return None
        
        content = args.get('content') or extract_robust_content(response.content)
        if not content:
            yield f"[Status] ❌ 失败: 未在回复中找到<file_content>代码块内容\n"
            return StepOutcome({"status": "error", "msg": "No content found. Blank is not supported. Put content inside <file_content>...</file_content> tags in your reply body before call file_write."}, next_prompt="\n")
        try:
            new_content = expand_file_refs(content, base_dir=self.cwd)
            if mode == "prepend":
                old = open(path, 'r', encoding="utf-8").read() if os.path.exists(path) else ""
                open(path, 'w', encoding="utf-8").write(new_content + old)
            else:
                with open(path, 'a' if mode == "append" else 'w', encoding="utf-8") as f: f.write(new_content)
            yield f"[Status] ✅ {mode.capitalize()} 成功 ({len(new_content)} bytes)\n"
            next_prompt = self._get_anchor_prompt(skip=args.get('_index', 0) > 0)
            return StepOutcome({"status": "success", 'writed_bytes': len(new_content)}, next_prompt=next_prompt)
        except Exception as e:
            yield f"[Status] ❌ 写入异常: {str(e)}\n"
            return StepOutcome({"status": "error", "msg": str(e)}, next_prompt="\n")
        
    def do_file_read(self, args, response):
        '''读取文件内容。从第start行开始读取。如有keyword则返回第一个keyword(忽略大小写)周边内容'''
        path = self._get_abs_path(args.get("path", ""))
        yield f"\n[Action] Reading file: {path}\n"
        start = args.get("start", 1)
        count = args.get("count", 100)  # P6: default 100 (was 200) to reduce token cost
        keyword = args.get("keyword")
        show_linenos = args.get("show_linenos", True)
        result = file_read(path, start=start, keyword=keyword,
                           count=count, show_linenos=show_linenos)
        if show_linenos and not result.startswith("Error:"): result = '由于设置了show_linenos，以下返回信息为：(行号|)内容 。\n' + result 
        if ' ... [TRUNCATED]' in result: result += '\n\n（某些行被截断，如需完整内容可改用 code_run 读取）'
        maxlen = 15000 // args.get('_tool_num', 1)
        result = smart_format(result, max_str_len=maxlen, omit_str='\n\n[omitted long content]\n\n')
        next_prompt = self._get_anchor_prompt(skip=args.get('_index', 0) > 0)
        log_memory_access(path)
        if 'memory' in path or 'sop' in path: 
            next_prompt += "\n[SYSTEM TIPS] 正在读取记忆或SOP文件，若决定按sop执行请提取sop中的关键点（特别是靠后的）update working memory."
        return StepOutcome(result, next_prompt=next_prompt)

    def _run_rg(self, cmd_args, cwd, timeout=30):
        """Run ripgrep subprocess, return (stdout, stderr, returncode). Returns rg-not-found error if absent."""
        rg = shutil.which('rg')
        if not rg:
            # 打包桌面 app 的内核继承 launchd 极简 PATH(无 /opt/homebrew/bin)，
            # shutil.which('rg') 会返回 None；直接探测常见安装位置兜底。
            for _cand in ('/opt/homebrew/bin/rg', '/usr/local/bin/rg', '/usr/bin/rg',
                          os.path.expanduser('~/homebrew/bin/rg')):
                if os.path.isfile(_cand) and os.access(_cand, os.X_OK):
                    rg = _cand
                    break
        if rg:
            cmd = [rg] + cmd_args
        else:
            return None, "rg not found", -1
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=cwd,
                                  stdin=subprocess.DEVNULL)
            return proc.stdout, proc.stderr, proc.returncode
        except subprocess.TimeoutExpired:
            return "", "Search timed out (30s)", -1
        except Exception as e:
            return "", str(e), -1

    def do_code_search(self, args, response):
        '''基于ripgrep的代码内容搜索'''
        pattern = args.get("pattern", "")
        if not pattern:
            return StepOutcome("Error: pattern is required")
        path = self._get_abs_path(args.get("path", "."))
        max_results = min(args.get("max_results", 50), 200)
        context_lines = min(args.get("context_lines", 0), 5)
        yield f"\n[Action] Searching: {pattern} in {path}\n"

        cmd = ["--line-number", "--no-heading", "--color", "never", "-m", str(max_results)]
        if args.get("case_insensitive"): cmd.append("-i")
        if args.get("fixed_strings"): cmd.append("-F")
        if context_lines > 0: cmd += ["-C", str(context_lines)]
        # include globs
        inc = args.get("include", "")
        if inc:
            for g in inc.split(","):
                g = g.strip()
                if g: cmd += ["-g", g]
        # exclude globs
        exc = args.get("exclude", "")
        if exc:
            for g in exc.split(","):
                g = g.strip()
                if g: cmd += ["-g", f"!{g}"]
        # file name filter
        fp = args.get("file_pattern", "")
        if fp:
            for g in fp.split(","):
                g = g.strip()
                if g: cmd += ["-g", g]
        cmd += ["-e", pattern, path]

        stdout, stderr, rc = self._run_rg(cmd, cwd=self.working.get("cwd", "."))
        if stdout is None:
            return StepOutcome(f"Error: {stderr}")
        if rc > 1:  # rg error (not "no match")
            return StepOutcome(f"Error: {stderr.strip() or stdout.strip()}")
        if not stdout.strip():
            result = f"No matches found for '{pattern}' in {path}"
        else:
            lines = stdout.splitlines()
            truncated = len(lines) > max_results * 2
            if truncated:
                lines = lines[:max_results * 2]
            result = "\n".join(lines)
            if truncated:
                result += f"\n... (truncated, showing first {max_results*2} lines)"
            result = f"Found matches ({len(stdout.splitlines())} lines total):\n{result}"
        # output cap
        maxlen = 15000 // args.get('_tool_num', 1)
        if len(result) > maxlen:
            result = result[:maxlen] + f"\n... [output truncated at {maxlen} chars]"
        return StepOutcome(result, next_prompt=self._get_anchor_prompt(skip=args.get('_index', 0) > 0))

    def do_file_find(self, args, response):
        '''按文件名模式查找文件路径'''
        pattern = args.get("pattern", "")
        if not pattern:
            return StepOutcome("Error: pattern is required")
        path = self._get_abs_path(args.get("path", "."))
        max_results = min(args.get("max_results", 50), 200)
        sort_by = args.get("sort_by", "path")
        yield f"\n[Action] Finding files: {pattern} in {path}\n"

        cmd = ["--files", "--no-messages"]
        if sort_by == "modified":
            cmd.append("--sort")
            cmd.append("modified")
        # type filter
        tf = args.get("type_filter", "")
        if tf: cmd += ["-t", tf]
        # glob pattern for filename
        cmd += ["-g", pattern, path]

        stdout, stderr, rc = self._run_rg(cmd, cwd=self.working.get("cwd", "."))
        if stdout is None:
            return StepOutcome(f"Error: {stderr}")
        if rc > 1:
            return StepOutcome(f"Error: {stderr.strip() or stdout.strip()}")
        if not stdout.strip():
            result = f"No files matching '{pattern}' found in {path}"
        else:
            lines = stdout.strip().splitlines()
            total = len(lines)
            if total > max_results:
                lines = lines[:max_results]
            result = "\n".join(lines)
            if total > max_results:
                result += f"\n... ({total} total, showing first {max_results})"
            else:
                result = f"{total} file(s) found:\n{result}"
        maxlen = 15000 // args.get('_tool_num', 1)
        if len(result) > maxlen:
            result = result[:maxlen] + f"\n... [output truncated at {maxlen} chars]"
        return StepOutcome(result, next_prompt=self._get_anchor_prompt(skip=args.get('_index', 0) > 0))

    # ── 预加载仓库结构索引（Codex 式：开局吃透整个仓库）──────────────────────
    def do_repo_index(self, args, response):
        '''基于 ripgrep 扫描生成文件清单+符号映射+语言统计，缓存到 .ga_search/repo_index.json。
        大仓库开局先 build 一次，之后用 find/symbols 秒级查询，避免反复走 code_run 遍历。'''
        action = (args.get("action") or "stats").strip().lower()
        root = self._get_abs_path(args.get("path", "."))
        if not os.path.isdir(root): root = os.path.dirname(root) or self.cwd
        cache_dir = os.path.join(root, ".ga_search")
        idx_path = os.path.join(cache_dir, "repo_index.json")
        os.makedirs(cache_dir, exist_ok=True)
        yield f"\n[Action] repo_index({action}) in {root}\n"
        if action in ("build", "rebuild"):
            force = action == "rebuild" or bool(args.get("force"))
            if not force and os.path.exists(idx_path):
                try:
                    ex = json.load(open(idx_path, encoding="utf-8"))
                    if ex.get("root") == os.path.abspath(root):
                        return StepOutcome(f"索引已存在（{ex.get('built_at','?')}，{ex.get('file_count',0)} 文件 / {ex.get('symbol_count',0)} 符号）。如需重建加 force=true。", next_prompt=self._get_anchor_prompt())
                except Exception:
                    pass
            files_out, _, _ = self._run_rg(["--files", "--no-messages"], cwd=root)
            if files_out is None:
                return StepOutcome("Error: rg 不可用，无法建索引。", next_prompt="\n")
            file_list = [l for l in files_out.splitlines() if l.strip()]
            sym_cmd = ["--no-heading", "--color=never", "-n"]
            for p in (
                r'^\s*(?:async\s+)?def\s+([A-Za-z_]\w*)',
                r'^\s*class\s+([A-Za-z_]\w*)',
                r'^\s*(?:export\s+)?(?:async\s+)?function\s+([A-Za-z_]\w*)',
                r'^\s*(?:const|let|var)\s+([A-Za-z_]\w*)\s*=',
                r'^\s*(?:pub(?:\([^)]*\))?\s+)?fn\s+([A-Za-z_]\w*)',
                r'^\s*(?:func|struct|interface|type|enum|trait|impl|mod|namespace|module)\s+([A-Za-z_]\w*)',
                r'^\s*[A-Za-z_][\w.<>\[\],\s*&:?]*?\b([A-Za-z_]\w*)\s*\(',
            ):
                sym_cmd += ["-e", p]
            sym_cmd.append(root)
            sym_out, _, _ = self._run_rg(sym_cmd, cwd=root, timeout=60)
            _SKIP = {'if','for','while','switch','catch','return','sizeof','foreach','lock','using','when','try','do','new','in','out','else','with',
                     'def','class','function','const','let','var','pub','fn','func','struct','interface','type','enum','trait','impl','mod','namespace','module',
                     'export','async','static','public','private','protected','final','override','abstract','virtual','extern','inline',
                     'int','void','char','float','double','bool','long','short','unsigned','signed','auto','self','this'}
            symbol_map, file_syms = {}, {}
            if sym_out:
                for line in sym_out.splitlines():
                    m = re.match(r'^(.+?):(\d+):(.*)$', line)
                    if not m: continue
                    f, rest = m.group(1), m.group(3)
                    name = next((t for t in re.findall(r'[A-Za-z_]\w*', rest) if t not in _SKIP), None)
                    if not name: continue
                    symbol_map.setdefault(name, []).append(f"{f}:{m.group(2)}")
                    file_syms.setdefault(f, []).append(name)
            lang_map = {'.py':'py','.js':'js','.ts':'ts','.tsx':'ts','.jsx':'js','.rs':'rs','.go':'go',
                        '.java':'java','.kt':'kt','.kts':'kt','.c':'c','.h':'c','.cpp':'cpp','.cc':'cpp',
                        '.hpp':'cpp','.rb':'rb','.php':'php','.swift':'swift','.m':'objc','.mm':'objc',
                        '.cs':'cs','.scala':'scala','.sh':'sh','.md':'md','.json':'json','.yaml':'yaml',
                        '.yml':'yaml','.toml':'toml','.html':'html','.css':'css','.sql':'sql','.r':'r','.lua':'lua'}
            ext_count = {}
            for f in file_list:
                _, ext = os.path.splitext(f)
                lang = lang_map.get(ext.lower(), (ext.lower().lstrip('.') or 'other'))
                ext_count[lang] = ext_count.get(lang, 0) + 1
            index = {
                "built_at": datetime.now().isoformat(timespec="seconds"),
                "root": os.path.abspath(root),
                "file_count": len(file_list),
                "languages": dict(sorted(ext_count.items(), key=lambda x: -x[1])[:30]),
                "symbol_count": len(symbol_map),
                "symbols": dict(list(symbol_map.items())[:20000]),
                "files": sorted(os.path.relpath(f, root) for f in file_list)[:20000],
            }
            json.dump(index, open(idx_path, "w", encoding="utf-8"), ensure_ascii=False)
            langs = ", ".join(f"{k}:{v}" for k, v in list(index["languages"].items())[:12])
            return StepOutcome(f"索引已构建：{index['file_count']} 文件 / {index['symbol_count']} 符号。语言：{langs}。缓存于 {idx_path}", next_prompt=self._get_anchor_prompt())
        if not os.path.exists(idx_path):
            return StepOutcome("索引未构建。请先调用 repo_index(action=build)。", next_prompt="\n")
        try:
            index = json.load(open(idx_path, encoding="utf-8"))
        except Exception as e:
            return StepOutcome(f"索引读取失败：{e}。请 rebuild。", next_prompt="\n")
        if action == "stats":
            langs = ", ".join(f"{k}:{v}" for k, v in list(index.get("languages", {}).items())[:15])
            return StepOutcome(f"仓库索引概览\n- 根：{index.get('root')}\n- 文件数：{index.get('file_count')}\n- 符号数：{index.get('symbol_count')}\n- 语言：{langs}\n- 构建于：{index.get('built_at')}", next_prompt=self._get_anchor_prompt())
        q = (args.get("query") or args.get("pattern") or "").lower()
        if not q:
            return StepOutcome(f"{action} 需要 query 参数。", next_prompt="\n")
        maxr = min(int(args.get("max_results", 50 if action == "find" else 30)), 200)
        if action == "find":
            hits = [f for f in index.get("files", []) if q in f.lower()][:maxr]
            if not hits:
                return StepOutcome(f"索引中未找到文件名含 '{q}' 的文件（已索引 {len(index.get('files', []))} 个）。", next_prompt="\n")
            return StepOutcome(f"找到 {len(hits)} 个文件名含 '{q}'（显示前 {maxr}）：\n" + "\n".join(hits), next_prompt=self._get_anchor_prompt())
        if action == "symbols":
            syms = index.get("symbols", {})
            exact = syms.get(q) or next((v for k, v in syms.items() if k.lower() == q), None)
            if exact:
                return StepOutcome(f"符号 '{q}' 精确匹配 → {len(exact)} 处：\n" + "\n".join(exact[:maxr]), next_prompt=self._get_anchor_prompt())
            matched = [(k, v) for k, v in syms.items() if q in k.lower()][:maxr]
            if not matched:
                return StepOutcome(f"索引中未找到含 '{q}' 的符号。可试 repo_index(action=find) 搜文件名，或 code_search 搜内容。", next_prompt=self._get_anchor_prompt())
            return StepOutcome("符号含 '" + q + f"' 的 {len(matched)} 个匹配：\n" + "\n".join(f"  {k} → {len(v)} 处（如 {v[0]}）" for k, v in matched), next_prompt=self._get_anchor_prompt())
        return StepOutcome(f"未知 action: {action}。支持 build/rebuild/stats/find/symbols。", next_prompt="\n")

    # ── 语义/自然语言搜索（Cursor 式：按意图找代码）─────────────────────────
    def _load_embed_cfg(self):
        '''返回 (api_key, api_base, model) 或 None（无 embedding 后端 → 降级）'''
        try:
            if script_dir not in sys.path: sys.path.insert(0, script_dir)
            import mykey
            importlib.reload(mykey)
        except Exception:
            return None
        ak = getattr(mykey, 'embedding_apikey', None) or getattr(mykey, 'emb_key', None)
        if not ak:
            for name in dir(mykey):
                if 'native_oai' in name and name.endswith('_config'):
                    cfg = getattr(mykey, name)
                    if isinstance(cfg, dict) and cfg.get('apikey'):
                        return cfg['apikey'], cfg.get('apibase', 'https://api.openai.com/v1'), 'text-embedding-3-small'
            return None
        ab = getattr(mykey, 'embedding_apibase', None) or getattr(mykey, 'emb_base', None) or 'https://api.openai.com/v1'
        md = getattr(mykey, 'embedding_model', None) or getattr(mykey, 'emb_model', None) or 'text-embedding-3-small'
        return ak, ab.rstrip('/'), md

    def _embed(self, texts, cfg):
        import requests
        ak, ab, md = cfg
        base = ab.rstrip('/')
        url = base + ('/v1' if '/v1' not in base else '') + '/embeddings'
        headers = {'Authorization': f'Bearer {ak}', 'Content-Type': 'application/json'}
        out = []
        for t in texts:
            r = requests.post(url, headers=headers, json={'model': md, 'input': t}, timeout=30)
            r.raise_for_status()
            out.append(r.json()['data'][0]['embedding'])
        return out

    def _cosine(self, a, b):
        dot = sum(x * y for x, y in zip(a, b))
        na = sum(x * x for x in a) ** 0.5
        nb = sum(x * x for x in b) ** 0.5
        return dot / (na * nb) if na and nb else 0.0

    def _extract_keywords(self, query):
        STOP = set('a an the of to in on for with and or is are be do does how what where which who when why this that these those i we you they it their our your my me he she as at by from into about can could should would want need find search code function class method use using implement add create new get set make show list all any some each per'.split())
        toks = re.findall(r'[A-Za-z_][A-Za-z0-9_]*', query.lower())
        out, syn = [], {'auth': 'authentication', 'login': 'authentication', 'db': 'database', 'sql': 'database',
                        'cli': 'command', 'ui': 'interface', 'api': 'interface', 'config': 'configuration',
                        'test': 'testing', 'bug': 'error', 'parse': 'parsing', 'init': 'initialize', 'util': 'utility'}
        for t in toks:
            if t in STOP or len(t) < 2:
                continue
            out.append(t)
            for sub in re.findall(r'[a-z]+|[A-Z][a-z]+', t):
                if sub not in STOP and len(sub) >= 2:
                    out.append(sub.lower())
        for k in list(out):
            if k in syn:
                out.append(syn[k])
        return list(dict.fromkeys(out))[:20]

    def _build_chunks(self, root, max_files=4000, chunk_lines=40, overlap=8):
        out, _, _ = self._run_rg(["--files", "--no-messages"], cwd=root)
        if not out:
            return []
        files = [l for l in out.splitlines() if l.strip()][:max_files]
        exts = ('.py', '.js', '.ts', '.tsx', '.jsx', '.go', '.rs', '.java', '.kt', '.kts', '.c', '.cpp',
                '.h', '.hpp', '.rb', '.php', '.swift', '.cs', '.scala', '.sh', '.lua', '.r', '.m', '.mm', '.sql')
        chunks = []
        for f in files:
            if not f.lower().endswith(exts):
                continue
            try:
                lines = open(f, encoding='utf-8', errors='replace').read().splitlines()
            except Exception:
                continue
            if not lines:
                continue
            for i in range(0, len(lines), chunk_lines - overlap):
                seg = lines[i:i + chunk_lines]
                if not seg:
                    break
                chunks.append({'file': os.path.relpath(f, root), 'start': i + 1, 'end': i + len(seg),
                               'text': "\n".join(seg)})
                if i + chunk_lines >= len(lines):
                    break
        return chunks[:20000]

    def do_semantic_search(self, args, response):
        '''语义/自然语言搜索代码。有 embedding 后端时向量检索；否则降级为智能 ripgrep（关键词+同义词扩展）。'''
        query = (args.get("query") or "").strip()
        if not query:
            return StepOutcome("需要 query 参数（自然语言描述要找什么）。", next_prompt="\n")
        root = self._get_abs_path(args.get("path", "."))
        if not os.path.isdir(root):
            root = os.path.dirname(root) or self.cwd
        cache_dir = os.path.join(root, ".ga_search")
        db_path = os.path.join(cache_dir, "semantic.db")
        os.makedirs(cache_dir, exist_ok=True)
        maxr = min(int(args.get("max_results", 10)), 50)
        kws = self._extract_keywords(query)
        yield f"\n[Action] semantic_search: {query}\n"
        cfg = self._load_embed_cfg()
        if cfg:
            try:
                force = bool(args.get("force"))
                if force or not os.path.exists(db_path):
                    yield "  构建语义索引（分块+向量化）...\n"
                    chunks = self._build_chunks(root)
                    if not chunks:
                        return StepOutcome("无源码文件可索引。", next_prompt="\n")
                    embs = []
                    for i in range(0, len(chunks), 32):
                        embs += self._embed([c['text'] for c in chunks[i:i + 32]], cfg)
                    import sqlite3
                    con = sqlite3.connect(db_path)
                    con.execute("CREATE TABLE IF NOT EXISTS chunks(file TEXT, start INTEGER, text TEXT, emb TEXT)")
                    con.execute("DELETE FROM chunks")
                    for c, e in zip(chunks, embs):
                        con.execute("INSERT INTO chunks VALUES(?,?,?,?)", (c['file'], c['start'], c['text'], json.dumps(e)))
                    con.commit()
                    con.close()
                q_emb = self._embed([query], cfg)[0]
                import sqlite3
                con = sqlite3.connect(db_path)
                rows = con.execute("SELECT file,start,text,emb FROM chunks").fetchall()
                con.close()
                scored = [(self._cosine(q_emb, json.loads(e)), f, s, t) for f, s, t, e in rows]
                scored.sort(reverse=True)
                top = scored[:maxr]
                if not top or top[0][0] < 0.05:
                    return StepOutcome(f"[语义搜索·向量模式] 未找到高相关片段（top 相似度<0.05）。可换关键词或 force 重建索引。", next_prompt=self._get_anchor_prompt())
                out = [f"[语义搜索·向量模式] query: {query}  top {len(top)}："]
                for s, f, st, t in top:
                    snip = t if len(t) <= 400 else t[:400] + "..."
                    out.append(f"\n◆ {f}:{st}  (sim={s:.3f})\n{snip}")
                return StepOutcome("\n".join(out), next_prompt=self._get_anchor_prompt())
            except Exception as e:
                yield f"  embedding 失败（{e}），降级为关键词搜索。\n"
        if not kws:
            return StepOutcome("无法从查询抽取关键词，请改用 code_search 直接给正则。", next_prompt="\n")
        pat = "|".join(re.escape(k) for k in kws)
        out, _, _ = self._run_rg(["--no-heading", "--color=never", "-n", "-i", "-e", pat, root], cwd=root, timeout=30)
        if out is None:
            return StepOutcome("Error: rg 不可用。", next_prompt="\n")
        lines = out.splitlines()[:maxr]
        if not lines:
            return StepOutcome(f"[语义搜索·降级模式] 关键词 {kws} 未命中。可换 code_search 给更精确正则。", next_prompt="\n")
        return StepOutcome(f"[语义搜索·降级模式·无 embedding 后端] 关键词：{kws}\n找到 {len(lines)} 行：\n" + "\n".join(lines), next_prompt=self._get_anchor_prompt())

    def export_history(self, fn): 
        with open(fn, 'w', encoding='utf-8') as f: json.dump(self.parent.llmclient.backend.history, f, ensure_ascii=False)
    def _in_plan_mode(self): return self.working.get('in_plan_mode')
    def _exit_plan_mode(self): self.working.pop('in_plan_mode', None)
    def enter_plan_mode(self, plan_path): 
        self.working['in_plan_mode'] = plan_path; self.max_turns = 100
        self.print(f"[Info] Entered plan mode with plan file: {plan_path}")
        return plan_path
    def _check_plan_completion(self):
        if not os.path.isfile(p:=self._in_plan_mode() or ''): return None
        try: return len(re.findall(r'\[ \]', open(p, encoding='utf-8', errors='replace').read()))
        except: return None
    
    def do_update_working_checkpoint(self, args, response):
        '''为整个任务设定后续需要临时记忆的重点。'''
        key_info = args.get("key_info", "")
        related_sop = args.get("related_sop", "")
        if "key_info" in args: self.working['key_info'] = key_info
        if "related_sop" in args: self.working['related_sop'] = related_sop
        self.working['passed_sessions'] = 0
        yield f"[Info] Updated key_info and related_sop.\n"
        next_prompt = self._get_anchor_prompt(skip=args.get('_index', 0) > 0)
        #next_prompt += '\n[SYSTEM TIPS] 此函数一般在任务开始或中间时调用，如果任务已成功完成应该是start_long_term_update用于结算长期记忆。\n'
        return StepOutcome({"result": "working key_info updated"}, next_prompt=next_prompt)

    def _retry_or_exit(self, prompt):
        self._empty_ct = getattr(self, '_empty_ct', 0) + 1
        if self._empty_ct >= 3: return StepOutcome({}, should_exit=True)
        return StepOutcome({}, next_prompt=prompt)

    def do_no_tool(self, args, response):
        '''这是一个特殊工具，由引擎自主调用，不要包含在TOOLS_SCHEMA里。
        当模型在一轮中未显式调用任何工具时，由引擎自动触发。
        二次确认仅在回复几乎只包含<thinking>/<summary>和一段大代码块时触发。'''
        content = getattr(response, 'content', '') or ""
        thinking = getattr(response, 'thinking', '') or ""
        if not response or (not content.strip() and not thinking.strip()):
            yield "[Warn] LLM returned an empty response. Retrying...\n"
            return self._retry_or_exit("[System] Blank response, regenerate and tooluse")
        if '[!!! 流异常中断' in content[-100:] or '!!!Error:' in content[-100:]:
            return self._retry_or_exit("[System] Incomplete response. Regenerate and tooluse.")
        if 'max_tokens !!!]' in content[-100:]:
            return self._retry_or_exit("[System] max_tokens limit reached. Use multi small steps to do it.")
        
        if self._in_plan_mode() and any(kw in content for kw in ['任务完成', '全部完成', '已完成所有', '🏁']):
            if 'VERDICT' not in content and '[VERIFY]' not in content and '验证subagent' not in content:
                yield "[Warn] Plan模式完成声明拦截。\n"
                return StepOutcome({}, next_prompt="⛔ [验证拦截] 检测到你在plan模式下声称完成，但未执行[VERIFY]验证步骤。请先按plan_sop §四启动验证subagent，获得VERDICT后才能声称完成。")
            
        # 2. 检测"包含较大代码块但未调用工具"的情况
        # 关键特征：恰好1个大代码块 + 代码块直接结尾（后面只有空白）
        code_block_pattern = r"```[a-zA-Z0-9_]*\n[\s\S]{50,}?```"
        blocks = re.findall(code_block_pattern, content)
        if len(blocks) == 1:
            m = re.search(code_block_pattern, content)
            after_block = content[m.end():]
            if not after_block.strip():
                residual = content.replace(m.group(0), "")
                residual = re.sub(r"<thinking>[\s\S]*?</thinking>", "", residual, flags=re.IGNORECASE)
                residual = re.sub(r"<summary>[\s\S]*?</summary>", "", residual, flags=re.IGNORECASE)
                clean_residual = re.sub(r"\s+", "", residual)
                if len(clean_residual) <= 30:
                    yield "[Info] Detected large code block without tool call and no extra natural language. Requesting clarification.\n"
                    next_prompt = (
                        "[System] 检测到你在上一轮回复中主要内容是较大代码块，且本轮未调用任何工具。\n"
                        "如果这些代码需要执行、写入文件或进一步分析，请重新组织回复并显式调用相应工具"
                        "（例如：code_run、file_write、file_patch 等）；\n"
                        "如果只是向用户展示或讲解代码片段，请在回复中补充自然语言说明，"
                        "并明确是否还需要额外的实际操作。"
                    )
                    return StepOutcome({}, next_prompt=next_prompt)
                
        if self._in_plan_mode():
            remaining = self._check_plan_completion()
            if remaining == 0:
                self._exit_plan_mode(); yield "[Info] Plan完成：plan.md中0个[ ]残留，退出plan模式。\n"
        
        #yield "[Info] Final response to user.\n"
        return StepOutcome(response, next_prompt=None)
    
    def do_start_long_term_update(self, args, response):
        '''Agent觉得当前任务完成后有重要信息需要记忆时调用此工具。'''
        prompt = '''### [总结提炼经验] 既然你觉得当前任务有重要信息需要记忆，请提取最近一次任务中【事实验证成功且长期有效】的环境事实、用户偏好、重要步骤，更新记忆。
本工具是标记开启结算过程，若已在更新记忆过程或没有值得记忆的点，忽略本次调用。
**如果没有经验证的，未来能用上的信息，忽略本次调用！**
**只能提取行动验证成功的信息**：
- **环境事实**（路径/凭证/配置）→ `file_patch` 更新 L2，同步 L1
- **复杂任务经验**（关键坑点/前置条件/重要步骤）→ L3 精简 SOP（只记你被坑得多次重试的核心要点）
**禁止**：临时变量、具体推理过程、未验证信息、通用常识、你可以轻松复现的细节、只是做了但没有验证的信息
**操作**：严格遵循提供的L0的记忆更新SOP。先 `file_read` 看现有 → 判断类型 → 最小化更新 → 无新内容跳过，保证对记忆库最小局部修改。\n
''' + get_global_memory(getattr(self, 'cwd', None))
        # 项目模式激活时路由写侧：项目专属记忆写项目记忆文件（经 junction 落真实项目根），
        # 全局 memory/ 只收跨项目通用事实。检测两条路径：前端绑定的 agent 属性、pid 键控锚文件。
        _proj = getattr(self, '_ga_project_mode_name', None) or None
        if not _proj:
            try:
                _temp_dir = os.path.join(script_dir, 'temp')
                for _f in os.listdir(_temp_dir):
                    if _f.startswith('.active_project.') and os.path.isfile(os.path.join(_temp_dir, _f)):
                        with open(os.path.join(_temp_dir, _f), encoding='utf-8', errors='ignore') as _fh:
                            _proj = _fh.read().strip() or None
                        if _proj:
                            break
            except OSError:
                pass
        if _proj:
            _pmem = os.path.join(script_dir, 'temp', 'projects', _proj, 'project_memory.md')
            prompt += f'''
[项目模式激活: {_proj}] 项目记忆文件: {_pmem}
- 本项目专属的决策/约定/踩坑/进度 → 只写该文件（用 file_patch 增量更新，禁整文件重写）；
- 与项目无关的全局事实/用户偏好 → 才写全局 memory/；
- 拿不准归宿的信息默认不写。
'''
        yield "[Info] Start distilling good memory for long-term storage.\n"
        path = './memory/memory_management_sop.md'
        if os.path.exists(path): result = 'This is L0:\n' + file_read(path, show_linenos=False)
        else: result = "Memory Management SOP not found. Do not update memory."
        return StepOutcome(result, next_prompt=prompt)

    def do_git_checkpoint(self, args, response):
        '''结构化 Git 工作流（替代裸 git 命令）。提供 9 个动作：
status/diff/commit/checkpoint/list/restore/branch/log/pr。
checkpoint = commit + 打 ga-ckpt-<ts> tag，可一键 restore；PR 优先 gh CLI，失败回退 GitHub REST API。
所有动作 cwd 默认 self.cwd（git 仓库根），可在 args.cwd 覆盖。'''
        action = (args.get("action") or "").strip().lower()
        repo = self._resolve_git_repo(args.get("cwd"))
        if not action: return StepOutcome({"status": "error", "msg": "action 必填。可选: status/diff/commit/checkpoint/list/restore/branch/log/pr"}, next_prompt="\n")
        if repo.get("status") == "error": return StepOutcome(repo, next_prompt="\n")
        repo_dir = repo["repo_dir"]
        try:
            if action == "status":   result = git_status(repo_dir)
            elif action == "diff":   result = git_diff(repo_dir, files=args.get("files"), staged=args.get("staged", False))
            elif action == "commit":
                result = git_commit(repo_dir, message=args.get("message", ""), files=args.get("files"), add_all=args.get("all", True), no_confirm=args.get("no_confirm", False))
            elif action == "checkpoint":
                result = git_checkpoint_create(repo_dir, message=args.get("message", ""), files=args.get("files"), add_all=args.get("all", True))
            elif action == "list":   result = git_checkpoint_list(repo_dir, max_count=args.get("max_count", 20))
            elif action == "restore":
                ckpt = args.get("checkpoint_id") or args.get("id")
                if not ckpt: return StepOutcome({"status": "error", "msg": "restore 必须提供 checkpoint_id（ga-ckpt-...）"}, next_prompt="\n")
                result = git_checkpoint_restore(repo_dir, ckpt_id=ckpt, no_confirm=args.get("no_confirm", False))
            elif action == "branch":
                result = git_branch_op(repo_dir, name=args.get("branch"), create_from=args.get("create_from"))
            elif action == "log":    result = git_log(repo_dir, max_count=args.get("max_count", 15))
            elif action == "pr":
                result = git_create_pr(repo_dir, title=args.get("pr_title", ""), body=args.get("pr_body", ""), target=args.get("pr_target"), remote=args.get("remote"))
            else: return StepOutcome({"status": "error", "msg": f"未知 action: {action}"}, next_prompt="\n")
        except subprocess.TimeoutExpired:
            return StepOutcome({"status": "error", "msg": "git 命令超时（>30s）。请检查 repo 状态或网络。"}, next_prompt="\n")
        except FileNotFoundError as e:
            return StepOutcome({"status": "error", "msg": f"git 未安装或不可执行: {e}"}, next_prompt="\n")
        except Exception as e:
            return StepOutcome({"status": "error", "msg": format_error(e)}, next_prompt="\n")
        yield f"[Action] git_checkpoint {action} @ {repo_dir}\n"
        out = json.dumps(result, ensure_ascii=False, indent=2, default=json_default) if isinstance(result, (dict, list)) else str(result)
        if len(out) > 8000: out = out[:8000] + "\n... [truncated]"
        yield out + "\n"
        next_prompt = self._get_anchor_prompt(skip=args.get('_index', 0) > 0)
        return StepOutcome(result, next_prompt=next_prompt)

    def _resolve_git_repo(self, cwd):
        if cwd: target = os.path.abspath(cwd)
        else: target = os.path.abspath(self.cwd)
        cur = target
        for _ in range(8):
            if os.path.isdir(os.path.join(cur, ".git")) or os.path.isfile(os.path.join(cur, ".git")):
                return {"status": "success", "repo_dir": cur}
            parent = os.path.dirname(cur)
            if parent == cur: break
            cur = parent
        return {"status": "error", "msg": f"找不到 git 仓库（向上搜了 8 层）: {target}。请在 git 仓库内调用，或显式传 cwd。"}

    def _fold_earlier(self, lines):
        FALLBACK = '直接回答了用户问题'
        parts, cnt, last = [], 0, ''
        def flush():
            if cnt:
                if FALLBACK in last: parts.append(f'[Agent]（{cnt} turns）')
                else: parts.append(f'{last}（{cnt} turns）')
        for line in lines:
            if line.startswith('[USER]'):
                flush(); parts.append(line); cnt = 0; last = ''
            else: cnt += 1; last = line
        flush()
        return "\n".join(parts[-70:])

    def _get_anchor_prompt(self, skip=False):
        if skip: return "\n"
        h = self.history_info; W = 15  # P3: reduced from 30 to cut working memory token cost
        earlier = f'<earlier_context>\n{self._fold_earlier(h[:-W])}\n</earlier_context>\n' if len(h) > W else ""
        h_str = "\n".join(h[-W:])
        prompt = f"\n### [WORKING MEMORY]\n{earlier}<history>\n{h_str}\n</history>"
        prompt += f"\nCurrent turn: {self.current_turn}\n"
        if self.working.get('key_info'): prompt += f"\n<key_info>{self.working.get('key_info')}</key_info>"
        if self.working.get('related_sop'): prompt += f"\n有不清晰的地方请再次读取{self.working.get('related_sop')}"
        if getattr(self.parent, 'verbose', False): self.print(prompt)
        return prompt
    
    def turn_end_callback(self, response, tool_calls, tool_results, turn, next_prompt, exit_reason):
        _c = re.sub(r'```.*?```|<thinking>.*?</thinking>', '', response.content, flags=re.DOTALL)
        rsumm = re.search(r"<summary>(.*?)</summary>", _c, re.DOTALL)
        if rsumm: summary = rsumm.group(1).strip()
        else:
            tc = tool_calls[0]; clean_args = {k: v for k, v in tc['args'].items() if not k.startswith('_')}   # at least one because no_tool
            summary = _c.strip() or smart_format("直接回答了用户问题" if tc['tool_name'] == 'no_tool' else f"{tc['tool_name']}, args: {clean_args}", max_str_len=40)
            next_prompt += "\n\n\n[SYSTEM] 必须在回复文本中包含<summary>！\n\n"
        summary = smart_format(summary.replace('\n', ''), max_str_len=80)
        self.history_info.append(f'[Agent] {summary}')
        _plan = self._in_plan_mode()

        if turn % 175 == 0 and (not _plan):
            next_prompt += f"\n\n[DANGER] Turn {turn}. Must call ask_user to summarize progress and get direction. No more blind retries."
        elif turn % 7 == 0:
            next_prompt += f"\n\n[SYSTEM] Turn {turn}. Call update_working_checkpoint to save key context. Stop ineffective retries; if no progress, switch strategy: 1) Probe physical boundaries 2) **Re-read relevant SOPs**"
        elif turn % 25 == 0:
            next_prompt += f"\n\n[SYSTEM] Turn {turn}. Write checkpoints/key findings/tried approaches to a **file** for future reference (not only working_checkpoint!). Avoid losing critical info."
        elif turn % 20 == 0: next_prompt += get_global_memory(getattr(self, 'cwd', None))  # P4: inject every 20 turns (was 10)

        if _plan and turn >= 10 and turn % 5 == 0:
            next_prompt = f"[Plan Hint] 正在计划模式。必须 file_read({_plan}) 确认当前步骤，回复开头引用：📌 当前步骤：...\n\n" + next_prompt
        if _plan and turn >= 190: next_prompt += f"\n\n[DANGER] Plan模式已运行 {turn} 轮，已达上限。必须 ask_user 汇报进度并确认是否继续。"

        injkeyinfo = self.parent.extrakeyinfo or consume_file(self.parent.task_dir, '_keyinfo')
        injprompt = self.parent.intervene or consume_file(self.parent.task_dir, '_intervene')
        if injkeyinfo: self.working['key_info'] = self.working.get('key_info', '') + f"\n[MASTER] {injkeyinfo}"
        if injprompt: next_prompt += f"\n\n[MASTER] {injprompt}\n"
        self.parent.intervene = self.parent.extrakeyinfo = None
        for hook in list(getattr(self.parent, '_turn_end_hooks', {}).values()): hook(locals())  # current readonly
        return next_prompt

def get_global_memory(cwd=None):
    prompt = "\n"
    try:
        suffix = '_en' if os.environ.get('GA_LANG', '') == 'en' else ''
        with open(os.path.join(script_dir, 'memory/global_mem_insight.txt'), 'r', encoding='utf-8', errors='replace') as f: insight = f.read()
        with open(os.path.join(script_dir, f'assets/insight_fixed_structure{suffix}.txt'), 'r', encoding='utf-8') as f: structure = f.read()
        _cwd = cwd or os.path.join(script_dir, 'temp')
        prompt += f'cwd = {_cwd} (./)\n'
        prompt += f"\n[Memory] (../memory)\n"
        prompt += structure + '\n../memory/global_mem_insight.txt:\n'
        prompt += insight + "\n"
    except FileNotFoundError: pass
    return prompt
