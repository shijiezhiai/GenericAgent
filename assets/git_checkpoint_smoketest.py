"""Smoke test for git_checkpoint helper functions.
在临时目录建一个独立 git 仓库，模拟跑完 9 个 action 的核心闭环：
status → branch → commit → checkpoint → list → restore → log → diff。
PR 动作不实跑（需要 push + token），只校验分支保护 + 参数解析。
"""
import os, sys, subprocess, tempfile, shutil, json, traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ga import (
    git_status, git_diff, git_commit, git_checkpoint_create,
    git_checkpoint_list, git_checkpoint_restore, git_branch_op, git_log,
    git_create_pr,
)

def run(cwd, cmd, check=True, env=None):
    p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, env=env or os.environ.copy())
    if check and p.returncode != 0:
        raise RuntimeError(f"cmd {cmd} failed: {p.stderr}")
    return p

def main():
    work = tempfile.mkdtemp(prefix="ga_ckpt_test_")
    try:
        env = os.environ.copy()
        env.update({"GIT_AUTHOR_NAME": "Test", "GIT_AUTHOR_EMAIL": "t@e", "GIT_COMMITTER_NAME": "Test", "GIT_COMMITTER_EMAIL": "t@e"})
        env.pop("GIT_DIR", None); env.pop("GIT_WORK_TREE", None)

        run(work, ["git", "init", "-b", "main"], env=env)
        run(work, ["git", "config", "user.email", "t@e"], env=env)
        run(work, ["git", "config", "user.name", "Test"], env=env)
        # 给 main 一个空提交，便于后续 commit 有 parent
        with open(os.path.join(work, "README.md"), "w") as f: f.write("# test\n")
        run(work, ["git", "add", "README.md"], env=env)
        run(work, ["git", "commit", "-m", "init"], env=env)

        print("=" * 60)
        print("1) status (clean)")
        r = git_status(work)
        assert r["status"] == "success", r
        assert r["branch"] == "main", r
        assert len(r["staged"]) == 0 and len(r["unstaged"]) == 0 and len(r["untracked"]) == 0, r
        print("   OK:", r["summary"])

        print("=" * 60)
        print("2) commit on main → 应被拒绝")
        with open(os.path.join(work, "forbidden.txt"), "w") as f: f.write("x\n")
        r = git_commit(work, message="bad", add_all=True)
        assert r["status"] == "error" and "受保护分支" in r["msg"], f"应拒绝，got: {r}"
        print("   OK: 被拒绝 ->", r["msg"][:60], "...")

        print("=" * 60)
        print("3) branch create work")
        r = git_branch_op(work, name="work", create_from="HEAD")
        assert r["status"] == "success" and r["action"] == "created_and_switched", r
        print("   OK:", r)

        print("=" * 60)
        print("4) status after switch (with untracked)")
        r = git_status(work)
        assert r["branch"] == "work", r
        assert any(u["path"] == "forbidden.txt" for u in r["untracked"]), r
        print("   OK: branch=%s, untracked=%d" % (r["branch"], len(r["untracked"])))

        print("=" * 60)
        print("5) commit on work")
        r = git_commit(work, message="first feature commit", add_all=True)
        assert r["status"] == "success" and r["short"], r
        sha1 = r["sha"]
        print("   OK: short=%s" % r["short"])

        print("=" * 60)
        print("6) checkpoint #1")
        with open(os.path.join(work, "extra.py"), "w") as f: f.write("print('hi')\n")
        r = git_checkpoint_create(work, message="pre-refactor checkpoint")
        assert r["status"] == "success", r
        ckpt_id_1 = r["checkpoint_id"]
        print("   OK: id=%s" % ckpt_id_1)

        print("=" * 60)
        print("6b) checkpoint #2 (to test list with multiple)")
        with open(os.path.join(work, "extra2.py"), "w") as f: f.write("print('hi2')\n")
        r = git_checkpoint_create(work, message="second checkpoint")
        assert r["status"] == "success", r
        ckpt_id_2 = r["checkpoint_id"]
        print("   OK: id=%s" % ckpt_id_2)

        print("=" * 60)
        print("7) list")
        r = git_checkpoint_list(work)
        assert r["status"] == "success" and r["count"] >= 2, r
        ids = {c["id"] for c in r["checkpoints"]}
        assert ckpt_id_1 in ids and ckpt_id_2 in ids, r
        # 最新在前
        assert r["checkpoints"][0]["id"] == ckpt_id_2, r
        print("   OK: %d checkpoints, latest=%s" % (r["count"], r["checkpoints"][0]["id"]))

        print("=" * 60)
        print("8) log")
        r = git_log(work, max_count=5)
        assert r["status"] == "success" and "init" in r["log"], r
        print("   OK:\n", r["log"])

        print("=" * 60)
        print("9) diff (after new uncommitted edit)")
        with open(os.path.join(work, "extra.py"), "a") as f: f.write("# more\n")
        r = git_diff(work)
        assert r["status"] == "success" and "extra.py" in r["stat"], r
        print("   OK: stat=\n", r["stat"])

        print("=" * 60)
        print("10) restore without confirm → needs_confirm")
        r = git_checkpoint_restore(work, ckpt_id=ckpt_id_1, no_confirm=False)
        assert r["status"] == "needs_confirm", r
        print("   OK:", r["msg"][:80], "...")

        print("=" * 60)
        print("11) restore with confirm")
        # 在 restore 之前先做一个无用 commit, 以便 reflog 显示回退路径
        with open(os.path.join(work, "temp.py"), "w") as f: f.write("temp\n")
        git_commit(work, message="junk before restore", add_all=True)
        r = git_checkpoint_restore(work, ckpt_id=ckpt_id_1, no_confirm=True)
        assert r["status"] == "success", r
        # checkpoint_1 之前没有 extra2.py（extra2.py 是 checkpoint_2 引入的）
        assert os.path.exists(os.path.join(work, "extra.py")), "extra.py 应在 checkpoint_1 中"
        assert not os.path.exists(os.path.join(work, "extra2.py")), "extra2.py 应被回退（属于 checkpoint_2）"
        assert not os.path.exists(os.path.join(work, "temp.py")), "temp.py 应被回退"
        log_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".workbuddy", "git_restore.log")
        assert os.path.exists(log_path), "restore log 应被写入"
        print("   OK: restored, previous_head=%s" % r["previous_head"][:7])
        print("   restore log:", open(log_path).read().strip().splitlines()[-1])

        print("=" * 60)
        print("12) pr without remote → 应报未配置 remote")
        r = git_create_pr(work, title="x", body="y")
        assert r["status"] == "error" and "remote" in r["msg"], r
        print("   OK:", r["msg"][:80])

        print("=" * 60)
        print("13) branch list")
        git_branch_op(work, name="main", create_from="HEAD")
        r = git_branch_op(work)
        assert r["status"] == "success" and "main" in r["branches"][0] and "work" in r["branches"][1], r
        print("   OK: branches=%s" % r["branches"])

        print("\n" + "=" * 60)
        print("ALL 13 SMOKE TESTS PASSED ✅")
    finally:
        shutil.rmtree(work, ignore_errors=True)
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".workbuddy", "git_restore.log")
        if os.path.exists(log_path):
            with open(log_path) as f: lines = f.readlines()
            keep = [l for l in lines if "ga_ckpt_test_" not in l]
            with open(log_path, "w") as f: f.writelines(keep)

if __name__ == "__main__":
    try: main()
    except Exception as e:
        print("❌ FAILED:", e)
        traceback.print_exc()
        sys.exit(1)
