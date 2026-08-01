#!/usr/bin/env python3
"""构建 GenericAgent 的自包含 macOS .app 所需的 runtime/ 资源。

产出目录：frontends/desktop/runtime/
  runtime/python/      python-build-standalone (relocatable, 无 venv)
  runtime/app/        GA 核心源码 (= GA_ROOT, 只读)
  runtime/wheels/     第三方依赖 wheel (离线安装用)
  runtime/install_macos.sh  首次运行把 wheels 装进嵌入式 python

用法：
  python3 bundle_build.py                 # 全量构建
  python3 bundle_build.py --only app      # 仅复制源码
  python3 bundle_build.py --only python   # 仅下载嵌入式 python
  python3 bundle_build.py --only wheels   # 仅收集 wheels
  python3 bundle_build.py --only bake     # 仅把已收集 wheels 预装进 bundle python
  python3 bundle_build.py --clean         # 清掉 runtime/ 重来

设计约束（与 src-tauri/src/lib.rs 对齐）：
  - lib.rs 的 bundle_python() 找 runtime/python/bin/python3
  - find_project_dir()   找 runtime/app/agentmain.py (作为 GA_ROOT)
  - run_offline_prepare() 调 runtime/install_macos.sh --python-path ... --project-dir ... \
        --wheel-dir ... --extra-packages "fastapi uvicorn websockets" --mode PrepareOnly --no-venv
"""
from __future__ import annotations
import argparse, os, shutil, subprocess, sys, tarfile, tempfile, urllib.request

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RUNTIME = os.path.join(REPO_ROOT, "frontends", "desktop", "src-tauri", "runtime")

# python-build-standalone 已迁移到 astral-sh。仅 3.10/3.11/3.12/3.13 等 tag；
# GA 要求 >=3.10,<3.14，取最新稳定的 3.10.20（依赖生态最全）。
STANDALONE_TAG = "20260728"
STANDALONE_URL = (
    "https://github.com/astral-sh/python-build-standalone/releases/download/"
    f"{STANDALONE_TAG}/cpython-3.10.20+{STANDALONE_TAG}-aarch64-apple-darwin-install_only.tar.gz"
)

# 核心运行时依赖白名单（让 网关+内核+legacy+conductor 跑起来）。
# 仅 pyproject core + desktop_bridge 实际 import 的必需项；IM/GUI 依赖(telegram/streamlit/...)
# 暂不打，保持体积与离线范围可控，真机验证缺啥补啥。
CORE_DEPS = [
    "requests", "beautifulsoup4", "bottle", "simple-websocket-server",
    "aiohttp", "fastapi", "uvicorn", "websockets",
    "psutil", "pillow", "pyyaml", "aiofiles", "python-dotenv", "markdown", "duckdb",
]

# 复制源码时的排除项
EXCLUDE_DIRS = {".git", ".venv", "temp", "sche_tasks", "ARCHIVED",
                "migrated_from_src", "node_modules", "target", "__pycache__",
                ".workbuddy", ".idea", ".vscode", "build", "dist"}
EXCLUDE_FILES = {"mykey.py"}  # 密钥绝不打包
EXCLUDE_SUFFIX = (".pyc", ".pyo", ".egg-info", ".DS_Store", ".so" + "_tmp")


def log(msg: str):
    print(f"[bundle_build] {msg}", flush=True)


def step_python():
    py = os.path.join(RUNTIME, "python", "bin", "python3")
    if os.path.exists(py):
        log("python 已存在，跳过下载")
        _ensure_pip(py)
        return
    os.makedirs(RUNTIME, exist_ok=True)
    url = STANDALONE_URL
    log(f"下载 standalone python: {url}")
    fd, tmp = tempfile.mkstemp(suffix=".tar.gz")
    os.close(fd)
    try:
        urllib.request.urlretrieve(url, tmp)
        log(f"下载完成 ({os.path.getsize(tmp)//1024} KB)，解压到 runtime/python")
        # tarball 顶层是 python/ 目录
        with tarfile.open(tmp) as tf:
            tf.extractall(RUNTIME)
    finally:
        os.remove(tmp)
    _ensure_pip(py)
    log("python 就绪: " + py)


def _ensure_pip(py: str):
    try:
        subprocess.run([py, "-m", "pip", "--version"], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return
    except subprocess.CalledProcessError:
        pass
    log("bootstrap pip via ensurepip")
    subprocess.run([py, "-m", "ensurepip", "--upgrade"], check=True)


def _skipped(path: str) -> bool:
    base = os.path.basename(path)
    if base in EXCLUDE_DIRS or base in EXCLUDE_FILES:
        return True
    if base.endswith(EXCLUDE_SUFFIX):
        return True
    # 排除特定大/无关目录
    parts = path.split(os.sep)
    if "skills_external" in parts or "frontends" in parts and "node_modules" in parts:
        return True
    return False


def step_app():
    dst = os.path.join(RUNTIME, "app")
    # 注意：不要用 shutil.rmtree 清空旧目录——会触发构建环境的安全删除护栏
    # (删除文件数超阈值需确认)。改为覆盖复制；残留的极少数旧文件对 Python
    # import 无影响（同名文件优先当前源码，且 GA 仓库极少删除模块）。
    if os.path.isdir(dst):
        log("runtime/app 已存在，覆盖复制（不删旧文件）")
    os.makedirs(dst, exist_ok=True)
    log(f"复制 GA 核心源码 -> {dst}")

    copied = 0
    for root, dirs, files in os.walk(REPO_ROOT):
        # 原地过滤 dirs，避免深入无关目录
        dirs[:] = [d for d in dirs if not _skipped(os.path.join(root, d))]
        # 也跳过 runtime 自身（正在构建的目录）
        rel = os.path.relpath(root, REPO_ROOT)
        if rel.startswith("frontends") and "runtime" in rel.split(os.sep):
            continue
        if rel == "frontends" and "runtime" in dirs:
            dirs.remove("runtime")
        for f in files:
            src = os.path.join(root, f)
            if _skipped(src):
                continue
            # 跳过特殊文件（socket/pipe/device 等），它们不可用 copy2 复制
            try:
                st = os.lstat(src)
                import stat as _stat
                if _stat.S_ISSOCK(st.st_mode) or _stat.S_ISFIFO(st.st_mode) \
                        or _stat.S_ISCHR(st.st_mode) or _stat.S_ISBLK(st.st_mode):
                    continue
            except OSError:
                continue
            tgt = os.path.join(dst, rel, f)
            os.makedirs(os.path.dirname(tgt), exist_ok=True)
            shutil.copy2(src, tgt)
            copied += 1
    log(f"复制完成，共 {copied} 个文件")


def step_wheels():
    py = os.path.join(RUNTIME, "python", "bin", "python3")
    if not os.path.exists(py):
        step_python()
    wheels_dir = os.path.join(RUNTIME, "wheels")
    # 关键：先清掉旧 wheels（含 _dupes 子目录），避免跨次构建累积同名多版本
    # 包，导致 install_macos.sh 里 `pip install *.whl` 出现 ResolutionImpossible。
    if os.path.isdir(wheels_dir):
        for name in os.listdir(wheels_dir):
            p = os.path.join(wheels_dir, name)
            if name.endswith(".whl") or name == "_dupes":
                if os.path.isdir(p):
                    shutil.rmtree(p)
                else:
                    os.remove(p)
    os.makedirs(wheels_dir, exist_ok=True)
    log("收集 wheels: " + ", ".join(CORE_DEPS))
    subprocess.run(
        [py, "-m", "pip", "download", "--no-cache-dir",
         "-d", wheels_dir, *CORE_DEPS],
        check=True,
    )
    n = len([f for f in os.listdir(wheels_dir) if f.endswith(".whl")])
    log(f"wheels 就绪: {n} 个")


def _parse_wheel(fname: str):
    """解析 wheel 文件名 -> (规范包名小写, 版本)，纯标准库，不依赖 packaging。"""
    base = fname[:-4] if fname.endswith(".whl") else fname
    parts = base.split("-")
    if len(parts) < 5:
        return None
    return parts[0].lower(), parts[1]


def _verkey(v: str):
    out = []
    for x in v.split("."):
        out.append((0, x.zfill(8)) if x.isdigit() else (1, x))
    return out


def _dedup_wheels(wheels_dir: str):
    """同一包只保留最新版本，旧版本挪到 _dupes/，避免 pip install *.whl 报 ResolutionImpossible。"""
    wheels = [os.path.join(wheels_dir, f)
              for f in os.listdir(wheels_dir) if f.endswith(".whl")]
    best = {}
    for f in wheels:
        p = _parse_wheel(os.path.basename(f))
        if not p:
            continue
        name, ver = p
        if name not in best or _verkey(ver) > _verkey(best[name]):
            best[name] = ver
    keep = set()
    for f in wheels:
        p = _parse_wheel(os.path.basename(f))
        if not p:
            keep.add(f)
            continue
        name, ver = p
        if ver == best[name]:
            keep.add(f)
    dup = os.path.join(wheels_dir, "_dupes")
    for f in wheels:
        if f not in keep:
            os.makedirs(dup, exist_ok=True)
            os.rename(f, os.path.join(dup, os.path.basename(f)))
    log(f"wheels 去重后保留 {len(keep)} 个")


def step_bake():
    """构建期把 wheels 离线装进 bundle 自带 python，并写 .prepared 标记。

    真机首次打开时 clone_dir(tar) 会把「已装好包」的 python 整体克隆到
    app-support/python，故不再触发 run_offline_prepare —— 没有首次启动的 pip
    子进程，也就没有 SIGKILL(137) / encodings  relocation 这类脆点。
    install_macos.sh 仍保留作为强制重装的兜底（用户删掉 .prepared 时）。"""
    py = os.path.join(RUNTIME, "python", "bin", "python3")
    if not os.path.exists(py):
        step_python()
    wheels_dir = os.path.join(RUNTIME, "wheels")
    if not os.path.isdir(wheels_dir) or not any(
        f.endswith(".whl") for f in os.listdir(wheels_dir)
    ):
        step_wheels()
    _dedup_wheels(wheels_dir)
    wheels = [os.path.join(wheels_dir, f)
              for f in os.listdir(wheels_dir) if f.endswith(".whl")]
    log(f"构建期预装 {len(wheels)} 个 wheel 进 bundle python (免去首次启动装包)")
    subprocess.run(
        [py, "-m", "pip", "install", "--no-index", "--upgrade", *wheels],
        check=True,
    )
    marker = os.path.join(RUNTIME, "python", ".prepared")
    with open(marker, "w") as f:
        f.write("baked-at-build\n")
    log("bundle python 已预装并写 .prepared 标记")


INSTALL_SH = r"""#!/usr/bin/env bash
# 首次运行：把随包分发的 wheels 离线安装进嵌入式 python（无 venv，可重定位）。
set -euo pipefail
PY=""; PROJECT=""; WHEELDIR=""; EXTRA=""; MODE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --python-path) PY="$2"; shift 2;;
    --project-dir) PROJECT="$2"; shift 2;;
    --wheel-dir) WHEELDIR="$2"; shift 2;;
    --extra-packages) EXTRA="$2"; shift 2;;
    --mode) MODE="$2"; shift 2;;
    --no-venv) shift;;
    *) shift;;
  esac
done
echo "GAPROGRESS|deps"
# 去重：同一包只保留最新版本，避免 wheels/ 残留多版本同名包导致 ResolutionImpossible。
# 用纯标准库解析 wheel 文件名（嵌入式 python 没有顶层 packaging 模块），
# 版本按数字段比较，兼容 0.140.13 vs 0.141.1 这类比较。
"$PY" - "$WHEELDIR" <<'PYEOF'
import os, glob, sys
def parse(fname):
    base = fname[:-4] if fname.endswith(".whl") else fname
    parts = base.split("-")
    if len(parts) < 5:
        return None
    return parts[0].lower(), parts[1]
def verkey(v):
    out = []
    for x in v.split("."):
        out.append((0, x.zfill(8)) if x.isdigit() else (1, x))
    return out
wheels = glob.glob(os.path.join(sys.argv[1], "*.whl"))
best = {}
for f in wheels:
    p = parse(os.path.basename(f))
    if not p:
        continue
    name, ver = p
    if name not in best or verkey(ver) > verkey(best[name]):
        best[name] = ver
keep = set()
for f in wheels:
    p = parse(os.path.basename(f))
    if not p:
        keep.add(f); continue
    name, ver = p
    if ver == best[name]:
        keep.add(f)
dup = os.path.join(sys.argv[1], "_dupes")
for f in wheels:
    if f not in keep:
        os.makedirs(dup, exist_ok=True)
        os.rename(f, os.path.join(dup, os.path.basename(f)))
PYEOF
"$PY" -m pip install --no-index --upgrade "$WHEELDIR"/*.whl
echo "GAPROGRESS|done"
"""


def gen_install():
    p = os.path.join(RUNTIME, "install_macos.sh")
    with open(p, "w") as f:
        f.write(INSTALL_SH)
    os.chmod(p, 0o755)
    log("生成 install_macos.sh")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["app", "python", "wheels", "bake"], default=None)
    ap.add_argument("--clean", action="store_true")
    args = ap.parse_args()

    if args.clean and os.path.isdir(RUNTIME):
        log("clean: 删除 runtime/")
        shutil.rmtree(RUNTIME)

    only = args.only
    if only in (None, "python"):
        step_python()
    if only in (None, "app"):
        step_app()
    if only in (None, "wheels"):
        step_wheels()
    if only in (None, "bake"):
        step_bake()
    gen_install()
    log("全部完成 -> " + RUNTIME)


if __name__ == "__main__":
    main()
