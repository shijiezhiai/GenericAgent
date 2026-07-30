import os, json, time as _time, socket as _socket, logging
from datetime import datetime, timedelta

# 端口锁：防止重复启动，bind失败时agentmain会直接崩溃退出
# reload时mod.__dict__保留_lock，跳过重复绑定
try: _lock
except NameError:
    _lock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    _lock.bind(('127.0.0.1', int(os.environ.get('GA_SCHEDULER_LOCK_PORT', '45762')))); _lock.listen(1)

INTERVAL = 120
ONCE = False

_dir = os.path.dirname(os.path.abspath(__file__))
TASKS = os.path.join(_dir, '../sche_tasks')
DONE  = os.path.join(_dir, '../sche_tasks/done')
_LOG  = os.path.join(_dir, '../sche_tasks/scheduler.log')

os.makedirs(DONE, exist_ok=True)
_logger = logging.getLogger('scheduler')

# 通知：on_done 时按任务配置的 notify_channels 发送结果通知
# notify.json 存接收者ID: {"wechat": to_user_id, "feishu": open_id}
# 任务JSON: notify_channels=["wechat","feishu"](空=不通知), notify_content="status_only"|"with_result"
NOTIFY_CFG = os.path.join(_dir, '../sche_tasks/notify.json')
_pending = {}  # {tid: {"channels":[...], "content": "...", "rpt": "..."}}

if not _logger.handlers:
    _logger.setLevel(logging.INFO)
    _fh = logging.FileHandler(_LOG, encoding='utf-8')
    _fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s',
                                        datefmt='%Y-%m-%d %H:%M'))
    _logger.addHandler(_fh)

# 默认最大延迟窗口（小时），超过此时间不触发
DEFAULT_MAX_DELAY = 6
_l4_t = 0  # last L4 archive time

def _parse_cooldown(repeat):
    """解析repeat为冷却时间(比实际周期略短,防漂移)"""
    if repeat == 'once': return timedelta(days=999999)
    if repeat in ('daily', 'weekday'): return timedelta(hours=20)
    if repeat == 'weekly': return timedelta(days=6)
    if repeat == 'monthly': return timedelta(days=27)
    if repeat.startswith('every_'):
        try:
            parts = repeat.split('_')
            n = int(parts[1].rstrip('hdm'))
            u = parts[1][-1]
            if u == 'h': return timedelta(hours=n)
            if u == 'm': return timedelta(minutes=n)
            if u == 'd': return timedelta(days=n)
        except (ValueError, IndexError):
            pass  # fall through to warning below
    _logger.warning(f'Unknown repeat type: {repeat}, fallback to 20h cooldown')
    return timedelta(hours=20)

def _last_run(tid, done_files):
    """找最近一次执行时间"""
    latest = None
    for df in done_files:
        if not df.endswith(f'_{tid}.md'): continue
        try:
            t = datetime.strptime(df[:15], '%Y-%m-%d_%H%M')
            if latest is None or t > latest: latest = t
        except: continue
    return latest

def check():
    # L4 archive cron (silent, every 12h)
    global _l4_t
    if _time.time() - _l4_t > 43200:
        _l4_t = _time.time()
        try:
            import sys; sys.path.insert(0, os.path.join(_dir, '../memory/L4_raw_sessions'))
            from compress_session import batch_process
            raw_dir = os.path.join(_dir, '../temp/model_responses')
            r = batch_process(raw_dir, dry_run=False)
            print(f'[L4 cron] {r}')
        except Exception as e:
            _logger.error(f'L4 archive failed: {e}')

    if not os.path.isdir(TASKS): return None
    now = datetime.now()
    os.makedirs(DONE, exist_ok=True)
    done_files = set(os.listdir(DONE))
    for f in sorted(os.listdir(TASKS)):
        if not f.endswith('.json'): continue
        tid = f[:-5]
        try:
            with open(os.path.join(TASKS, f), encoding='utf-8') as fp:
                task = json.loads(fp.read())
        except Exception as e:
            _logger.error(f'JSON parse error for {f}: {e}')
            continue
        if not task.get('enabled', False): continue
        
        repeat = task.get('repeat', 'daily')
        sched = task.get('schedule', '00:00')
        try:
            h, m = map(int, sched.split(':'))
        except Exception as e:
            _logger.error(f'Invalid schedule format in {f}: {sched!r} ({e})')
            continue
        
        # weekday任务：周末跳过
        if repeat == 'weekday' and now.weekday() >= 5: continue
        
        # 还没到schedule时间就跳过
        if now.hour < h or (now.hour == h and now.minute < m): continue
        
        # 执行窗口检查：超过max_delay小时则跳过（防止开机太晚触发过时任务）
        max_delay = task.get('max_delay_hours', DEFAULT_MAX_DELAY)
        sched_minutes = h * 60 + m
        now_minutes = now.hour * 60 + now.minute
        if (now_minutes - sched_minutes) > max_delay * 60:
            _logger.info(f'SKIP {tid}: {now_minutes - sched_minutes}min past schedule, '
                         f'exceeds max_delay={max_delay}h')
            continue
        
        # 检查冷却
        last = _last_run(tid, done_files)
        cooldown = _parse_cooldown(repeat)
        if last and (now - last) < cooldown: continue
        
        # 触发
        _logger.info(f'TRIGGER {tid} (repeat={repeat}, schedule={sched}, '
                     f'last_run={last})')
        ts = now.strftime('%Y-%m-%d_%H%M')
        rpt = os.path.join(DONE, f'{ts}_{tid}.md')
        prompt = task.get('prompt', '')
        model = task.get('model', '')
        model_tag = f'[MODEL:{model}]\n' if model else ''
        # 记录通知配置，on_done 时读取发送
        channels = task.get('notify_channels', [])
        if channels:
            _pending[tid] = {
                'channels': [c for c in channels if c in ('wechat', 'feishu')],
                'content': task.get('notify_content', 'status_only'),
                'rpt': rpt,
            }
        return (f'[定时任务] {tid}\n'
                f'[报告路径] {rpt}\n\n'
                f'先读 scheduled_task_sop 了解执行流程，然后执行以下任务：\n\n'
                f'{model_tag}'
                f'{prompt}\n\n'
                f'完成后将执行报告写入 {rpt}。')

    return None


def _load_notify_cfg():
    """读取 notify.json: {"wechat": to_user_id, "feishu": open_id}"""
    try:
        with open(NOTIFY_CFG, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        _logger.error(f'load notify.json failed: {e}')
        return {}


def _send_wechat(to_user_id, text):
    """通过 wechatapp.py 的 WxBotClient 主动发消息"""
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            'wechatapp', os.path.join(_dir, '../frontends/wechatapp.py'))
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        client = m.WxBotClient()
        client._save = lambda **kw: None  # patch 掉 TCC 拦截的 token 写入
        resp = client.send_text(to_user_id, text)
        return bool(resp and resp.get('message_id'))
    except Exception as e:
        _logger.error(f'send_wechat failed: {e}')
        return False


def _send_feishu(open_id, text):
    """通过 fsapp.py 的 send_message 主动发消息（用卡片支持长文本/换行）"""
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            'fsapp', os.path.join(_dir, '../frontends/fsapp.py'))
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        mid = m.send_message(open_id, text, use_card=True, receive_id_type='open_id')
        return bool(mid)
    except Exception as e:
        _logger.error(f'send_feishu failed: {e}')
        return False


def on_done(result):
    """任务执行完成后被 agentmain 调用，按任务配置发送通知。
    result: agent 完整输出。失败时以 [ERROR] 开头。
    """
    if not _pending:
        return
    tid, info = next(iter(_pending.items()))
    _pending.pop(tid, None)  # 取出即清，避免重复
    channels = info.get('channels', [])
    content_mode = info.get('content', 'status_only')
    rpt = info.get('rpt', '')
    if not channels:
        return

    result = result or ''
    is_error = result.strip().startswith('[ERROR]')
    status = '❌ 失败' if is_error else '✅ 成功'

    # 组装消息内容
    if content_mode == 'with_result':
        # 带执行结果（截断防止超长）
        body = result.strip()
        if len(body) > 1800:
            body = body[:1800] + '\n...(结果已截断)'
        msg = f'定时任务 {tid} {status}\n\n{body}'
    else:
        # 仅状态（失败含原因，剥离 [ERROR] 前缀）
        if is_error:
            parts = result.strip().split('\n', 1)
            reason = parts[1][:500].strip() if len(parts) > 1 else parts[0][7:][:500].strip()
            msg = f'定时任务 {tid} {status}\n失败原因: {reason}'
        else:
            msg = f'定时任务 {tid} {status}'

    cfg = _load_notify_cfg()
    for ch in channels:
        try:
            if ch == 'wechat':
                uid = cfg.get('wechat', '')
                if uid:
                    _send_wechat(uid, msg)
            elif ch == 'feishu':
                oid = cfg.get('feishu', '')
                if oid:
                    _send_feishu(oid, msg)
        except Exception as e:
            _logger.error(f'notify {ch} failed: {e}')
