"""Persistent input-history (Up/Down arrow recall) for TUI v2 sessions.

JSON sidecar at ``temp/model_responses/input_histories.json`` maps a log-file
basename → list[str] of user inputs.  Mirrors ``session_names.py`` in spirit:
same directory, same basename keying, thread-locked, atomic write.

Lifecycle
---------
* **Write**: every user submission in ``on_input_area_submitted`` appends to
  the in-memory list *and* persists the trimmed (≤ ``_HISTORY_MAX``) snapshot.
* **Read on /continue**: ``_continue_restore_apply._finish`` loads the
  persisted list into ``AgentSession.input_history`` so Up-arrow recall works
  immediately after restoring a historical session.
* **In-session switch** (``/switch``): the existing in-memory flow
  (``prev.input_history = inp._input_history`` …) already carries history
  between live sessions, so the store is only consulted at *restore* time.
"""
import json, os, threading

_LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        'temp', 'model_responses')
_REG_PATH = os.path.join(_LOG_DIR, 'input_histories.json')
_lock = threading.Lock()

# Must match InputArea._HISTORY_MAX so the persisted list never exceeds what
# the UI is willing to hold.  A mismatch would let the file grow unbounded
# across many sessions (each only ever trims its own in-memory copy).
HISTORY_MAX = 200


def _load() -> dict:
    try:
        with open(_REG_PATH, encoding='utf-8') as f:
            d = json.load(f)
            return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _save(d: dict) -> None:
    os.makedirs(_LOG_DIR, exist_ok=True)
    tmp = _REG_PATH + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(d, f, ensure_ascii=False, indent=2)
    os.replace(tmp, _REG_PATH)


def _key(log_path: str) -> str:
    return os.path.basename(log_path or '')


def get(log_path: str) -> list:
    """Return the persisted history list for ``log_path`` (empty if none)."""
    with _lock:
        d = _load()
    v = d.get(_key(log_path))
    return list(v) if isinstance(v, list) else []


def append(log_path: str, text: str) -> None:
    """Append ``text`` to the persisted history for ``log_path``.

    Dedupes consecutive duplicates (same rule as ``InputArea.record_history``)
    and trims to ``HISTORY_MAX`` entries.  No-op when ``log_path`` is empty
    (agent not yet booted) or ``text`` is blank.
    """
    key = _key(log_path)
    stripped = (text or '').strip()
    if not key or not stripped:
        return
    with _lock:
        d = _load()
        lst = d.get(key)
        if not isinstance(lst, list):
            lst = []
        if not (lst and lst[-1] == stripped):
            lst.append(stripped)
            if len(lst) > HISTORY_MAX:
                lst = lst[-HISTORY_MAX:]
            d[key] = lst
            _save(d)


def set_list(log_path: str, history: list) -> None:
    """Overwrite the persisted history for ``log_path`` wholesale.

    Used when restoring a session that already has a populated in-memory list
    and we want the file to match (e.g. after a dedup/trim on the live copy).
    """
    key = _key(log_path)
    if not key:
        return
    lst = [s for s in (history or []) if isinstance(s, str) and s.strip()]
    if len(lst) > HISTORY_MAX:
        lst = lst[-HISTORY_MAX:]
    with _lock:
        d = _load()
        if lst:
            d[key] = lst
        else:
            d.pop(key, None)
        _save(d)


def migrate(old_path: str, new_path: str) -> None:
    """Move the history entry from old basename to new after /continue copy."""
    old_key, new_key = _key(old_path), _key(new_path)
    if not old_key or not new_key or old_key == new_key:
        return
    with _lock:
        d = _load()
        if old_key in d:
            d[new_key] = d.pop(old_key)
            _save(d)


def gc(log_paths_alive: set) -> int:
    """Drop entries whose basename is no longer among ``log_paths_alive``.

    Called opportunistically (e.g. on /sessions) to keep the sidecar from
    accumulating dead sessions.  Returns count removed.
    """
    alive = {os.path.basename(p or '') for p in (log_paths_alive or set())}
    with _lock:
        d = _load()
        bad = [k for k in d if k not in alive]
        for k in bad:
            d.pop(k, None)
        if bad:
            _save(d)
        return len(bad)
