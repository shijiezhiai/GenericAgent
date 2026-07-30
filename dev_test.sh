#!/bin/zsh
# GenericAgent 源码 dev 测试实例（与日常 .app 并行共存）
#
# 用法:
#   ./dev_test.sh            前台启动 dev 实例（Ctrl-C 停止）
#   ./dev_test.sh status     查看 dev 实例是否在跑
#   ./dev_test.sh stop       停掉 dev 实例（含孤儿 worker）
#
# 端口约定（全部避开日常 .app 的 14168/8900/45762）:
#   bridge 25168 / conductor 29900 / scheduler 锁 45764
# 环境:
#   GA_NO_IM_AUTOSTART=1  不拉起 IM 机器人，避免与日常 .app 重复收发
# 验证提醒: 本机 shell 有 HTTP_PROXY，curl 必须 --noproxy '*'

set -u
cd "$(dirname "$0")"
SRC_DIR="$(pwd)"

DEV_BRIDGE_PORT="${DEV_BRIDGE_PORT:-25168}"
DEV_CONDUCTOR_PORT="${DEV_CONDUCTOR_PORT:-29900}"
DEV_SCHED_LOCK_PORT="${DEV_SCHED_LOCK_PORT:-45764}"
PY="$SRC_DIR/.venv/bin/python"

if [[ ! -x "$PY" ]]; then
  echo "ERROR: 找不到 $PY（源码 venv），先创建 .venv 并装依赖" >&2
  exit 1
fi

# 找出 cwd 在本源码目录、监听 dev 端口或由 dev bridge 派生的进程
dev_pids() {
  lsof -ti tcp:"$DEV_BRIDGE_PORT" -s tcp:LISTEN 2>/dev/null
}

case "${1:-start}" in
  status)
    pids=$(dev_pids)
    if [[ -n "$pids" ]]; then
      echo "dev 实例在跑 (pid: $pids)  http://127.0.0.1:$DEV_BRIDGE_PORT/"
      curl -s --noproxy '*' -m 3 "http://127.0.0.1:$DEV_BRIDGE_PORT/status" | head -c 300; echo
    else
      echo "dev 实例未运行"
    fi
    ;;

  stop)
    pids=$(dev_pids)
    [[ -n "$pids" ]] && kill ${=pids} 2>/dev/null && echo "已停止 bridge (pid: $pids)"
    # 补杀孤儿 worker（wechatapp/fsapp 等不随 bridge 退出）。
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

  start|*)
    # 端口占用检查
    if [[ -n "$(dev_pids)" ]]; then
      echo "ERROR: 端口 $DEV_BRIDGE_PORT 已被占用（dev 实例已在跑？用 ./dev_test.sh status 查看）" >&2
      exit 1
    fi
    echo "启动 dev 实例: http://127.0.0.1:$DEV_BRIDGE_PORT/  (Ctrl-C 停止)"
    echo "验证: curl --noproxy '*' http://127.0.0.1:$DEV_BRIDGE_PORT/status"
    export BRIDGE_PORT="$DEV_BRIDGE_PORT"
    export CONDUCTOR_PORT="$DEV_CONDUCTOR_PORT"
    export GA_SCHEDULER_LOCK_PORT="$DEV_SCHED_LOCK_PORT"
    export GA_NO_IM_AUTOSTART=1
    exec "$PY" frontends/desktop_bridge.py
    ;;
esac
