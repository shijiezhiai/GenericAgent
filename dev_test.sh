#!/bin/zsh
# GenericAgent 源码 dev 测试实例（与日常 .app 并行共存）
#
# 用法:
#   ./dev_test.sh            前台启动 dev 实例（Ctrl-C 停止）
#   ./dev_test.sh status     查看 dev 实例是否在跑
#   ./dev_test.sh stop       停掉 dev 实例（含孤儿 worker）
#
# 单端口网关 dev 实例（与日常 .app 并行共存）
# 网关收口 desktop_bridge(legacy) + kernel + conductor，并反向代理 conductor/TMWebDriver/supergrok_proxy
# 到同一端口（生产同构）。dev 也走这条路径，方便验证单端口。
#
# 用法:
#   ./dev_test.sh            前台启动 dev 实例（网关单端口，Ctrl-C 停止）
#   ./dev_test.sh bridge     前台启动旧 bare desktop_bridge.py（仅调试，不走网关）
#   ./dev_test.sh status     查看 dev 实例是否在跑
#   ./dev_test.sh stop       停掉 dev 实例（含孤儿 worker）
#
# 端口约定（全部避开日常 .app 的 14168/8900/45762）:
#   gateway 25168(单端口) / legacy-bridge 25169(仅内部) / conductor 29900 / cdp 28766 / supergrok 25433
# 环境:
#   GA_NO_IM_AUTOSTART=1  不拉起 IM 机器人，避免与日常 .app 重复收发
# 验证提醒: 本机 shell 有 HTTP_PROXY，curl 必须 --noproxy '*'

set -u
cd "$(dirname "$0")"
SRC_DIR="$(pwd)"

DEV_GATEWAY_PORT="${DEV_GATEWAY_PORT:-25168}"
DEV_LEGACY_PORT="${DEV_LEGACY_PORT:-25169}"
DEV_CONDUCTOR_PORT="${DEV_CONDUCTOR_PORT:-29900}"
DEV_CDP_PORT="${DEV_CDP_PORT:-28766}"
DEV_GROK_PORT="${DEV_GROK_PORT:-25433}"
DEV_SCHED_LOCK_PORT="${DEV_SCHED_LOCK_PORT:-45764}"
PY="$SRC_DIR/.venv/bin/python"

if [[ ! -x "$PY" ]]; then
  echo "ERROR: 找不到 $PY（源码 venv），先创建 .venv 并装依赖" >&2
  exit 1
fi

# 网关监听单端口；以此判定 dev 实例是否在跑
dev_pids() {
  lsof -ti tcp:"$DEV_GATEWAY_PORT" -s tcp:LISTEN 2>/dev/null
}

case "${1:-start}" in
  status)
    pids=$(dev_pids)
    if [[ -n "$pids" ]]; then
      echo "dev 实例在跑 (pid: $pids)  http://127.0.0.1:$DEV_GATEWAY_PORT/"
      curl -s --noproxy '*' -m 3 "http://127.0.0.1:$DEV_GATEWAY_PORT/status" | head -c 300; echo
    else
      echo "dev 实例未运行"
    fi
    ;;

  stop)
    pids=$(dev_pids)
    [[ -n "$pids" ]] && kill ${=pids} 2>/dev/null && echo "已停止网关 (pid: $pids)"
    # 补杀孤儿 worker（legacy bridge / kernel / conductor 不随网关退出）。
    # 判据必须窄：命令行显式引用「本源码目录/frontends/*.py」的 python 进程，
    # 不能用 lsof +D（会误伤只是打开了源码文件的编辑器/LSP 进程）。
    sleep 1
    orphans=$(pgrep -f "python.*$SRC_DIR/frontends/.*\.py" | grep -v "^$$\$" || true)
    if [[ -n "$orphans" ]]; then
      echo "补杀孤儿 worker:"
      pgrep -fl "python.*$SRC_DIR/frontends/.*\.py" || true
      kill ${=orphans} 2>/dev/null  # zsh 不自动分词，${=} 强制按空白拆多个 pid
    fi
    echo done
    ;;

  # 旧路径：直接起 bare desktop_bridge.py（仅调试用，不走网关单端口）
  bridge)
    if [[ -n "$(dev_pids)" ]]; then
      echo "ERROR: 端口 $DEV_GATEWAY_PORT 已被占用（dev 实例已在跑？用 ./dev_test.sh status 查看）" >&2
      exit 1
    fi
    echo "启动 bare bridge dev 实例: http://127.0.0.1:$DEV_GATEWAY_PORT/  (Ctrl-C 停止)"
    export BRIDGE_PORT="$DEV_GATEWAY_PORT"
    export CONDUCTOR_PORT="$DEV_CONDUCTOR_PORT"
    export GA_SCHEDULER_LOCK_PORT="$DEV_SCHED_LOCK_PORT"
    export GA_NO_IM_AUTOSTART=1
    exec "$PY" frontends/desktop_bridge.py
    ;;

  # 默认：单端口网关 dev 实例（生产同构，可验证 conductor/TMWebDriver/supergrok_proxy 同端口）
  start|*)
    if [[ -n "$(dev_pids)" ]]; then
      echo "ERROR: 端口 $DEV_GATEWAY_PORT 已被占用（dev 实例已在跑？用 ./dev_test.sh status 查看）" >&2
      exit 1
    fi
    GATEWAY_DIR="$SRC_DIR/frontends/desktop_gateway"
    GA_BIN="$GATEWAY_DIR/target/debug/ga-desktop-gateway"
    if [[ ! -x "$GA_BIN" ]]; then
      echo "==> 网关二进制缺失，开始 cargo build（首次较慢，需 Apple clang 工具链）..."
      ( cd "$GATEWAY_DIR" && \
        export SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk \
                DEVELOPER_DIR=/Library/Developer/CommandLineTools \
                CC=/Library/Developer/CommandLineTools/usr/bin/clang \
                CXX=/Library/Developer/CommandLineTools/usr/bin/clang++ \
                CARGO_TARGET_AARCH64_APPLE_DARWIN_LINKER=/Library/Developer/CommandLineTools/usr/bin/clang && \
        export PATH=/Library/Developer/CommandLineTools/usr/bin:$PATH && \
        cargo build ) || { echo "ERROR: 网关构建失败" >&2; exit 1; }
    fi
    echo "启动 dev 实例（单端口网关）: http://127.0.0.1:$DEV_GATEWAY_PORT/  (Ctrl-C 停止)"
    echo "验证:"
    echo "  status:    curl --noproxy '*' http://127.0.0.1:$DEV_GATEWAY_PORT/status"
    echo "  conductor: curl --noproxy '*' http://127.0.0.1:$DEV_GATEWAY_PORT/conductor/"
    echo "  cdp:       curl --noproxy '*' http://127.0.0.1:$DEV_GATEWAY_PORT/cdp/"
    echo "  proxy:     curl --noproxy '*' http://127.0.0.1:$DEV_GATEWAY_PORT/proxy/"
    export GA_GATEWAY_ROOT="$SRC_DIR"
    export GA_KERNEL_PYTHON="$PY"
    export GA_GATEWAY_PORT="$DEV_GATEWAY_PORT"
    export GA_LEGACY_PORT="$DEV_LEGACY_PORT"
    export GA_CONDUCTOR_PORT="$DEV_CONDUCTOR_PORT"
    export GA_CDP_PORT="$DEV_CDP_PORT"
    export GA_GROK_PORT="$DEV_GROK_PORT"
    export GA_SCHEDULER_LOCK_PORT="$DEV_SCHED_LOCK_PORT"
    export GA_NO_IM_AUTOSTART=1
    exec "$GA_BIN"
    ;;
esac
