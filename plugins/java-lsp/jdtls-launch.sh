#!/usr/bin/env bash
# Eclipse JDT Language Server 启动器 —— 供 GA 的 LSP 集成调用。
#
# GA 调用方式:  jdtls-launch.sh -data <data-dir>
# 依赖:
#   1) JDK 17+ (java 在 PATH)
#   2) 已下载的 jdtls 发行包，含 plugins/ 与 config_mac/ (macOS) 目录
#      下载: https://github.com/eclipse-jdtls/eclipse.jdt.ls  (或各发行版提供的 build)
#
# 使用前请编辑下面 JDTLS_HOME 为你的 jdtls 发行包根目录。
# 也可通过环境变量覆盖: JDTLS_HOME=/your/path 启动 GA。
set -e

JDTLS_HOME="${JDTLS_HOME:-/opt/jdtls}"
LAUNCHER_JAR="$(ls "$JDTLS_HOME"/plugins/org.eclipse.equinox.launcher_*.jar 2>/dev/null | head -1)"
CONFIG_DIR="$JDTLS_HOME/config_mac"
DATA_DIR="$1"

if [ -z "$LAUNCHER_JAR" ] || [ ! -f "$LAUNCHER_JAR" ]; then
  echo "ERROR: 未找到 jdtls launcher jar，请设置 JDTLS_HOME (当前: $JDTLS_HOME)" >&2
  exit 1
fi
if [ -z "$DATA_DIR" ]; then
  DATA_DIR="$(mktemp -d)"
fi

exec java \
  -Declipse.application=org.eclipse.jdt.ls.core.id1 \
  -Dosgi.bundles.defaultStartLevel=4 \
  -Declipse.product=org.eclipse.jdt.ls.core.product \
  -Dlog.level=ALL \
  -Xmx2G \
  -jar "$LAUNCHER_JAR" \
  -configuration "$CONFIG_DIR" \
  -data "$DATA_DIR" \
  --add-modules=ALL-SYSTEM
