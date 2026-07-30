#!/usr/bin/env python3
"""Contract test for the GA desktop kernel (JSON-RPC over stdio).

Spawns `frontends/kernel_server.py` as a subprocess, drives it through the protocol
defined in `frontends/kernel_protocol.md`, and asserts behavior is consistent with the
legacy `desktop_bridge.py` business logic (which the kernel reuses unchanged).

Run:  python assets/kernel_protocol_smoketest.py
"""
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
KERNEL = REPO / "frontends" / "kernel_server.py"


def find_python():
    for cand in [REPO / ".venv" / "bin" / "python", REPO / ".venv" / "Scripts" / "python.exe"]:
        if cand.exists():
            return str(cand)
    return sys.executable


class KernelClient:
    def __init__(self, proc):
        self.proc = proc
        self.lock = threading.Lock()
        self.responses = {}
        self.events = {}
        self.notifications = []
        self._id = 0
        self.reader = threading.Thread(target=self._reader, daemon=True)
        self.reader.start()

    def _reader(self):
        for line in self.proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except Exception:
                continue  # ignore non-JSON (e.g. stray stdout)
            if "method" in msg and "id" not in msg:
                with self.lock:
                    self.notifications.append(msg)
            elif "id" in msg:
                rid = msg["id"]
                with self.lock:
                    self.responses[rid] = msg
                    ev = self.events.get(rid)
                if ev:
                    ev.set()

    def call(self, method, params=None, timeout=15):
        self._id += 1
        rid = self._id
        ev = threading.Event()
        with self.lock:
            self.events[rid] = ev
        req = {"jsonrpc": "2.0", "id": rid, "method": method, "params": params or {}}
        self.proc.stdin.write(json.dumps(req, ensure_ascii=False) + "\n")
        self.proc.stdin.flush()
        if not ev.wait(timeout):
            raise TimeoutError(f"no response for {method}")
        with self.lock:
            msg = self.responses.pop(rid)
        if "error" in msg:
            return ("error", msg["error"])
        return ("ok", msg.get("result"))

    def notifications_for(self, method=None):
        with self.lock:
            return [n for n in self.notifications if method is None or n.get("method") == method]


def main():
    py = find_python()
    # isolate persistent state under a throwaway root
    tmp = tempfile.mkdtemp(prefix="ga_kernel_test_")
    env = dict(os.environ)
    env["GA_KERNEL_ROOT"] = tmp

    proc = subprocess.Popen(
        [py, str(KERNEL)], cwd=str(REPO), stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, text=True, bufsize=1, env=env,
    )
    client = KernelClient(proc)
    fails = 0

    def check(name, cond, detail=""):
        nonlocal fails
        status = "PASS" if cond else "FAIL"
        print(f"[{status}] {name} {detail}")
        if not cond:
            fails += 1

    try:
        # give the kernel a moment to emit kernel.ready / import
        time.sleep(1.0)
        if proc.poll() is not None:
            print("[FAIL] kernel exited early; stderr:")
            print(proc.stderr.read() if proc.stderr else "")
            sys.exit(1)

        st, res = client.call("status")
        check("status", st == "ok" and isinstance(res.get("sessionCount"), int), f"{st}:{res}")

        st, res = client.call("config.get")
        check("config.get", st == "ok" and "gaRoot" in res, f"{st}")

        st, res = client.call("conv_folders.get")
        check("conv_folders.get", st == "ok" and "folders" in res, f"{st}")

        st, res = client.call("session.create", {"cwd": str(REPO)})
        check("session.create", st == "ok" and res.get("sessionId"), f"{st}:{res}")
        sid = res.get("sessionId") if st == "ok" else None

        if sid:
            st, res = client.call("session.list")
            ids = [s.get("id") for s in res.get("sessions", [])] if st == "ok" else []
            check("session.list.contains", sid in ids, f"{st}")

            st, res = client.call("session.patch", {"sid": sid, "fields": {"folder_id": "default"}})
            check("session.patch", st == "ok", f"{st}:{res}")

            st, res = client.call("session.get", {"sid": sid})
            # snapshot() emits camelCase folderId (per kernel_protocol.md)
            check("session.get.folderId", st == "ok" and res.get("session", {}).get("folderId") == "default", f"{st}")

            st, res = client.call("conv_folders.set", {
                "folders": [
                    {"id": "default", "name": "default", "locked": True},
                    {"id": "archived", "name": "archived", "locked": True},
                ],
                "assignments": {sid: "default"},
            })
            check("conv_folders.set", st == "ok", f"{st}:{res}")
            assigns = res.get("assignments", {}) if st == "ok" else {}
            check("conv_folders.set.assignment", assigns.get(sid) == "default", f"{assigns}")

            # prompt → must accept and start streaming notifications
            st, res = client.call("session.prompt", {"sid": sid, "prompt": "say hi in one word"})
            check("session.prompt.accepted", st == "ok" and res.get("accepted"), f"{st}:{res}")

            got = []
            for _ in range(60):  # up to 30s
                got = client.notifications_for("session.stream")
                if got:
                    break
                time.sleep(0.5)
            states = [n["params"].get("state") for n in got]
            check("session.stream.received", len(got) >= 1, f"states={states}")

            st, res = client.call("session.messages", {"sid": sid})
            msgs = res.get("messages") if st == "ok" else None
            check("session.messages", st == "ok" and isinstance(msgs, list) and len(msgs) >= 1, f"{st}")

            st, res = client.call("session.cancel", {"sid": sid}, timeout=5)
            check("session.cancel", st == "ok", f"{st}:{res}")

            st, res = client.call("session.delete", {"sid": sid})
            check("session.delete", st == "ok", f"{st}")
        else:
            check("session.create", False, "no sid returned")

        # model profiles read path
        st, res = client.call("model_profiles.list")
        check("model_profiles.list", st == "ok" and "profiles" in res, f"{st}")
    except Exception as e:
        check("exception", False, repr(e))
    finally:
        try:
            client.call("kernel.shutdown", timeout=5)
        except Exception:
            pass
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()

    print()
    if fails == 0:
        print("ALL PASS")
        sys.exit(0)
    else:
        print(f"{fails} FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
