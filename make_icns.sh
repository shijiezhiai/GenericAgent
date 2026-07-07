#!/bin/bash
# Generate macOS .icns icon from logo.jpg
# Requires: sips (built-in macOS), iconutil (built-in macOS)

set -e
cd "$(dirname "$0")"

SRC="assets/images/logo.jpg"
SRC_PNG="/tmp/ga_logo_src.png"
ICONSET="GenericAgent.iconset"
ICNS="GenericAgent.icns"

if [ ! -f "$SRC" ]; then
    echo "Error: $SRC not found"
    exit 1
fi

# Convert source to PNG first
sips -s format png "$SRC" --out "$SRC_PNG" >/dev/null

rm -rf "$ICONSET"
mkdir "$ICONSET"

# Generate all required icon sizes from PNG source
for size in 16 32 128 256 512; do
    sips -z $size $size "$SRC_PNG" --out "$ICONSET/icon_${size}x${size}.png" >/dev/null
    double=$((size * 2))
    sips -z $double $double "$SRC_PNG" --out "$ICONSET/icon_${size}x${size}@2x.png" >/dev/null
done

iconutil -c icns "$ICONSET" -o "$ICNS"
rm -rf "$ICONSET"

echo "Created $ICNS"
