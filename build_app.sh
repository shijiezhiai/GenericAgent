#!/bin/bash
# Build a macOS .app bundle for GenericAgent
set -e
cd "$(dirname "$0")"

APP_NAME="GenericAgent"
APP_DIR="dist/${APP_NAME}.app"
CONTENTS="${APP_DIR}/Contents"
MACOS="${CONTENTS}/MacOS"
RESOURCES="${CONTENTS}/Resources"
GA_DIR="$(pwd)"

# Find Python
PYTHON_PATH=$(which python3)
echo "=== Building ${APP_NAME}.app ==="
echo "    Python: ${PYTHON_PATH}"
echo "    Project: ${GA_DIR}"

# Step 1: Generate icon
echo "[1/4] Generating icon..."
bash make_icns.sh

# Step 2: Create .app structure
echo "[2/4] Creating app bundle..."
rm -rf "$APP_DIR"
mkdir -p "$MACOS" "$RESOURCES"
cp GenericAgent.icns "$RESOURCES/"

# Step 3: Create a native launcher binary (avoids macOS script restrictions)
echo "[3/4] Compiling native launcher..."
cat > /tmp/ga_launcher.c << 'EOF'
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <mach-o/dyld.h>

int main(int argc, char *argv[]) {
    char exe[4096], script[4096];
    unsigned int size = 4096;
    _NSGetExecutablePath(exe, &size);

    char *macos = strstr(exe, "/MacOS/");
    if (!macos) return 1;
    *macos = '\0';
    snprintf(script, 4096, "%s/Resources/run.sh", exe);

    execl("/bin/bash", "bash", script, (char *)NULL);
    return 1;
}
EOF

clang --sysroot /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk \
    -o "${MACOS}/GenericAgent" /tmp/ga_launcher.c
rm /tmp/ga_launcher.c

# Step 4: Create the actual launch script in Resources
cat > "${RESOURCES}/run.sh" << SCRIPT
#!/bin/bash
export GA_PROJECT_DIR="${GA_DIR}"
cd "\$GA_PROJECT_DIR"
exec "${PYTHON_PATH}" "\$GA_PROJECT_DIR/launch.pyw" "\$@"
SCRIPT
chmod +x "${RESOURCES}/run.sh"

# Info.plist
cat > "${CONTENTS}/Info.plist" << PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key>
    <string>GenericAgent</string>
    <key>CFBundleDisplayName</key>
    <string>GenericAgent</string>
    <key>CFBundleIdentifier</key>
    <string>com.genericagent.app</string>
    <key>CFBundleVersion</key>
    <string>0.1.0</string>
    <key>CFBundleShortVersionString</key>
    <string>0.1.0</string>
    <key>CFBundleExecutable</key>
    <string>GenericAgent</string>
    <key>CFBundleIconFile</key>
    <string>GenericAgent</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>LSMinimumSystemVersion</key>
    <string>11.0</string>
    <key>NSHighResolutionCapable</key>
    <true/>
</dict>
</plist>
PLIST

# Sign ad-hoc
codesign --force --deep --sign - "$APP_DIR" 2>/dev/null || true

echo "[4/4] Done!"
echo ""
echo "✅ ${APP_DIR} created ($(du -sh "$APP_DIR" | cut -f1))"
echo ""
echo "   Install:  cp -r ${APP_DIR} /Applications/"
echo "   Launch:   open ${APP_DIR}"
