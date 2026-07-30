use std::process::{Command, Stdio};
use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::time::{Duration, Instant};
use std::thread;
use std::path::{Path, PathBuf};
use tauri::Manager;

use ga_desktop_gateway::GatewayConfig;

#[cfg(windows)]
use std::os::windows::process::CommandExt;

/// Get project root (parent of frontends/)
fn project_root() -> PathBuf {
    std::env::current_exe()
        .expect("cannot get exe path")
        .parent().expect("cannot get exe dir")   // frontends/
        .parent().expect("cannot get project root") // project root
        .to_path_buf()
}

/// Directory next to which a self-contained bundle keeps its runtime/ folder.
/// Windows: the exe's folder. Linux: the .AppImage's folder ($APPIMAGE) when launched as an
/// AppImage (current_exe would otherwise point inside the read-only squashfs mount).
/// macOS portable package: the folder containing GenericAgent.app and runtime/.
fn bundle_anchor_dir() -> Option<PathBuf> {
    #[cfg(not(windows))]
    {
        if let Some(p) = std::env::var_os("APPIMAGE") {
            if let Some(d) = PathBuf::from(p).parent() {
                return Some(d.to_path_buf());
            }
        }
    }

    let exe = std::env::current_exe().ok()?;

    #[cfg(target_os = "macos")]
    {
        // current_exe() inside a standard bundle is:
        //   <package>/GenericAgent.app/Contents/MacOS/GenericAgent
        // Prefer the standard macOS layout where runtime is embedded in the app:
        //   GenericAgent.app/Contents/Resources/runtime/app/agentmain.py
        // Fall back to the old portable layout for compatibility:
        //   <package>/runtime/app/agentmain.py
        let mut d = exe.parent();
        while let Some(dir) = d {
            if dir.extension().and_then(|s| s.to_str()) == Some("app") {
                let resources = dir.join("Contents").join("Resources");
                if resources.join("runtime").join("app").join("agentmain.py").exists() {
                    return Some(resources);
                }
                if let Some(parent) = dir.parent() {
                    return Some(parent.to_path_buf());
                }
            }
            // App Translocation 友好：macOS 把带 quarantine 的 .app 挂载到
            // /Volumes/<name>/ 并以它为卷根运行，exe = /Volumes/<name>/Contents/MacOS/...
            // 此时路径里没有 <name>.app 目录，直接检查 <dir>/Resources 即可。
            let resources = dir.join("Resources");
            if resources.join("runtime").join("app").join("agentmain.py").exists() {
                return Some(resources);
            }
            d = dir.parent();
        }
    }

    Some(exe.parent()?.to_path_buf())
}

/// 可写的 app-support 根：~/Library/Application Support/GenericAgent (macOS)。
fn app_support_dir() -> Option<PathBuf> {
    dirs::data_dir().map(|d| d.join("GenericAgent"))
}

/// 递归复制目录（把只读的包内 runtime/ 克隆到可写的 app-support）。
fn clone_dir(src: &Path, dst: &Path) -> bool {
    // dst 的父目录（如 ~/Library/Application Support/GenericAgent）可能不存在，
    // 必须先创建，否则 cp -R src dst 因父缺失直接失败。
    if let Some(parent) = dst.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::remove_dir_all(dst);
    Command::new("cp")
        .arg("-R")
        .arg(src)
        .arg(dst)
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Embedded interpreter inside the bundle. Prefer the **writable** app-support/python
/// (cloned from runtime/python and pip-installed with wheels by run_offline_prepare);
/// fall back to the read-only in-bundle runtime/python only as a last resort.
fn bundle_python() -> Option<PathBuf> {
    if let Some(sup) = app_support_dir() {
        #[cfg(windows)]
        let p = sup.join("python").join("python.exe");
        #[cfg(not(windows))]
        let p = sup.join("python").join("bin").join("python3");
        if p.exists() {
            return Some(p);
        }
    }
    let root = bundle_root()?;
    #[cfg(windows)]
    let p = root.join("python").join("python.exe");
    #[cfg(not(windows))]
    let p = root.join("python").join("bin").join("python3");
    if p.exists() { Some(p) } else { None }
}

/// Find python executable:
/// 1. The embedded bundle python (runtime/python) — deps are installed directly into it
///    (no venv), and its path is resolved relative to the bundle anchor at runtime, so the
///    package stays relocatable (moving the folder doesn't break absolute venv paths).
/// 2. .portable/uv-python/ 下找 python.exe (Windows) 或 python3 (Unix)
/// 3. Fallback to system PATH
fn find_python() -> String {
    if let Some(p) = bundle_python() {
        return p.to_string_lossy().to_string();
    }
    let root = project_root();
    let portable_python_dir = root.join(".portable").join("uv-python");

    if portable_python_dir.exists() {
        // uv installs python like: uv-python/cpython-3.12.x-windows-x86_64/python.exe
        // We need to search for python.exe inside subdirectories
        if let Ok(entries) = std::fs::read_dir(&portable_python_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    #[cfg(windows)]
                    {
                        let py = path.join("python.exe");
                        if py.exists() {
                            return py.to_string_lossy().to_string();
                        }
                    }
                    #[cfg(not(windows))]
                    {
                        let py = path.join("bin").join("python3");
                        if py.exists() {
                            return py.to_string_lossy().to_string();
                        }
                    }
                }
            }
        }
    }

    // Fallback: system PATH
    #[cfg(windows)]
    { "python".to_string() }
    #[cfg(not(windows))]
    { "python3".to_string() }
}

/// When auto-discovery falls back to a bare `python`/`python3` (system interpreter, which
/// lacks our dependencies), and the discovered project dir bundles a `.venv`, prefer that
/// venv python so the kernel/legacy subprocesses can import their dependencies.
fn resolve_python(python: String, project: &str) -> String {
    if (python == "python" || python == "python3") && !project.is_empty() {
        let venv = PathBuf::from(project).join(".venv").join("bin").join("python3");
        if venv.exists() {
            return venv.to_string_lossy().to_string();
        }
    }
    python
}

/// Bundle 模式下，包内 runtime/ 是只读的，而 Python 端要在 ga_root 下写 temp/sche_tasks/mykey
/// 等、且首次需把 wheels 装进嵌入式 python。这里在 ~/Library/Application Support/GenericAgent/
/// 维护可写副本：app/(源码) 与 python/(解释器)。仅在构建版本 (GA_BUILD_ID) 变化或缺失时重新克隆。
/// 非 bundle 构建返回 None（走 dev/source 路径，ga_root 本身是可读写的仓库目录）。
fn ensure_writable_runtime() -> Option<PathBuf> {
    if bundle_root().is_none() {
        return None;
    }
    let anchor = bundle_anchor_dir()?;
    let src_app = anchor.join("runtime").join("app");
    let src_py = anchor.join("runtime").join("python");
    if !src_app.join("agentmain.py").exists()
        || !src_py.join("bin").join("python3").exists()
    {
        return None;
    }
    let base = app_support_dir()?;
    let dst_app = base.join("app");
    let dst_py = base.join("python");
    let version_file = base.join(".app-clone-version");
    let need_copy = match std::fs::read_to_string(&version_file) {
        Ok(v) if v.trim() == env!("GA_BUILD_ID") => false,
        _ => true,
    };
    if need_copy {
        // 升级重克隆会整体删掉 dst_app，但其中的用户数据（会话历史 temp/、定时任务、
        // 运行期演化的 memory/、mykey.py 等）必须跨版本保留：先挪到 hold 目录，克隆后挪回。
        let hold = base.join(".upgrade-hold");
        let _ = std::fs::remove_dir_all(&hold);
        let preserved = stash_user_data(&dst_app, &hold);
        if !clone_dir(&src_app, &dst_app) || !clone_dir(&src_py, &dst_py) {
            restore_user_data(&hold, &dst_app, &preserved);
            return None;
        }
        restore_user_data(&hold, &dst_app, &preserved);
        let _ = std::fs::remove_dir_all(&hold);
        let _ = std::fs::write(&version_file, env!("GA_BUILD_ID"));
    }
    Some(dst_app)
}

/// app 根目录下属于「用户数据」、升级时必须原样保留的条目。
const USER_DATA_ENTRIES: &[&str] = &[
    "temp",
    "sche_tasks",
    "memory",
    "mykey.py",
    ".file_favorites.json",
];

/// 把 dst_app 中存在的用户数据条目 rename 到 hold/ 下，返回成功挪走的条目名。
fn stash_user_data(dst_app: &Path, hold: &Path) -> Vec<&'static str> {
    let mut moved = Vec::new();
    if !dst_app.exists() {
        return moved;
    }
    let _ = std::fs::create_dir_all(hold);
    for name in USER_DATA_ENTRIES {
        let src = dst_app.join(name);
        if src.exists() && std::fs::rename(&src, hold.join(name)).is_ok() {
            moved.push(*name);
        }
    }
    moved
}

/// 把 hold/ 中的用户数据条目挪回新克隆的 dst_app（覆盖包内自带的同名内容）。
fn restore_user_data(hold: &Path, dst_app: &Path, preserved: &[&'static str]) {
    for name in preserved {
        let dst = dst_app.join(name);
        let _ = std::fs::remove_dir_all(&dst);
        let _ = std::fs::remove_file(&dst);
        if std::fs::rename(hold.join(name), &dst).is_err() {
            eprintln!("[ga-desktop] WARN: failed to restore user data entry: {}", name);
        }
    }
}

/// Find the project directory (folder containing agentmain.py).
/// Bundle layout: a writable copy under <app support>/GenericAgent/app (cloned from
/// runtime/app, see ensure_writable_runtime). Dev layout: walk up from the exe.
fn find_project_dir() -> Option<String> {
    // Bundle 模式：ga_root 必须可写，返回 app-support 副本
    if let Some(w) = ensure_writable_runtime() {
        return Some(w.to_string_lossy().to_string());
    }

    // Dev/source layout: walk up to 8 levels from the exe location.
    let exe = std::env::current_exe().ok()?;
    let mut dir = Some(exe.parent()?);
    for _ in 0..8 {
        match dir {
            Some(d) => {
                if d.join("agentmain.py").exists() {
                    return Some(d.to_string_lossy().to_string());
                }
                dir = d.parent();
            }
            None => break,
        }
    }
    None
}

/// Settings file path: ~/.ga_desktop_settings.json
fn settings_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".ga_desktop_settings.json")
}

/// Read the settings file as a JSON object (empty object when missing/unparseable).
fn read_settings() -> serde_json::Map<String, serde_json::Value> {
    let path = settings_path();
    if let Ok(content) = std::fs::read_to_string(&path) {
        if let Ok(serde_json::Value::Object(m)) = serde_json::from_str(&content) {
            return m;
        }
    }
    serde_json::Map::new()
}

/// Merge `updates` into the existing settings file and write it back, preserving any keys
/// we don't touch. The old code rewrote the file with only python_path/project_dir, which
/// would silently drop sibling keys like `desktop_shortcut`. Always go through here.
fn merge_settings(updates: serde_json::Value) {
    let mut obj = read_settings();
    if let serde_json::Value::Object(m) = updates {
        for (k, v) in m {
            obj.insert(k, v);
        }
    }
    let val = serde_json::Value::Object(obj);
    if let Ok(text) = serde_json::to_string_pretty(&val) {
        let _ = std::fs::write(settings_path(), text);
    }
}

/// Desktop-shortcut preference stored in settings under `desktop_shortcut`.
/// None  = never asked (first run)
/// Some(true)/Some(false) = user's remembered choice.
fn read_shortcut_pref() -> Option<bool> {
    read_settings().get("desktop_shortcut").and_then(|v| v.as_bool())
}

fn write_shortcut_pref(enabled: bool) {
    merge_settings(serde_json::json!({ "desktop_shortcut": enabled }));
}

/// Create (or overwrite) a desktop shortcut pointing at the CURRENT exe. Overwriting on every
/// enabled launch is what makes the portable bundle relocatable: move the folder, relaunch, and
/// the shortcut is rewritten to the new path. Windows-only (uses a .lnk via WScript.Shell).
#[cfg(windows)]
fn ensure_desktop_shortcut() {
    let Ok(exe) = std::env::current_exe() else { return; };
    let Some(desktop) = dirs::desktop_dir() else { return; };
    let lnk = desktop.join("GenericAgent.lnk");
    let work_dir = exe.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| exe.clone());

    let exe_s = exe.to_string_lossy().replace('\'', "''");
    let lnk_s = lnk.to_string_lossy().replace('\'', "''");
    let work_s = work_dir.to_string_lossy().replace('\'', "''");

    // Build the shortcut via WScript.Shell COM, consistent with the existing powershell usage
    // elsewhere in this file. No extra crate needed.
    let script = format!(
        "$ws = New-Object -ComObject WScript.Shell; \
         $sc = $ws.CreateShortcut('{lnk}'); \
         $sc.TargetPath = '{exe}'; \
         $sc.WorkingDirectory = '{work}'; \
         $sc.IconLocation = '{exe}'; \
         $sc.Save()",
        lnk = lnk_s, exe = exe_s, work = work_s
    );

    let mut cmd = Command::new("powershell.exe");
    cmd.args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", &script]);
    cmd.creation_flags(0x08000000); // CREATE_NO_WINDOW
    let _ = cmd.status();
}

#[cfg(target_os = "linux")]
fn ensure_desktop_shortcut() {
    // Launch target: the AppImage path when running as one, else the current exe. Writing the
    // current path on every enabled launch keeps a relocated bundle's launcher valid.
    let Some(target) = std::env::var_os("APPIMAGE").map(PathBuf::from)
        .or_else(|| std::env::current_exe().ok()) else { return; };
    let exec = target.to_string_lossy().replace('"', "");
    // Linux .desktop Icon= needs an image file (or themed name), not the AppImage path. The CI
    // ships GenericAgent.png next to the AppImage; fall back to a generic themed icon otherwise.
    let icon = bundle_anchor_dir()
        .map(|d| d.join("GenericAgent.png"))
        .filter(|p| p.exists())
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|| "application-x-executable".to_string());
    let entry = format!(
        "[Desktop Entry]\nType=Application\nName=GenericAgent\nComment=GenericAgent Desktop\n\
         Exec=\"{exec}\"\nIcon={icon}\nTerminal=false\nCategories=Utility;Development;\n",
        exec = exec, icon = icon
    );
    let write_desktop = |path: &std::path::Path| {
        if std::fs::write(path, &entry).is_ok() {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755));
        }
    };
    if let Some(home) = dirs::home_dir() {
        let apps = home.join(".local/share/applications");
        let _ = std::fs::create_dir_all(&apps);
        write_desktop(&apps.join("GenericAgent.desktop"));
    }
    if let Some(desktop) = dirs::desktop_dir() {
        let _ = std::fs::create_dir_all(&desktop);
        let f = desktop.join("GenericAgent.desktop");
        write_desktop(&f);
        // GNOME marks unknown launchers "untrusted"; flag ours so it runs on double-click. Best effort.
        let _ = Command::new("gio")
            .args(["set", &f.to_string_lossy(), "metadata::trusted", "true"])
            .status();
    }
}

#[cfg(target_os = "macos")]
fn ensure_desktop_shortcut() {
    // The .app is the launchable unit; drop a symlink to it on the Desktop.
    let Ok(exe) = std::env::current_exe() else { return; };
    let mut app: Option<PathBuf> = None;
    let mut d = exe.parent();
    while let Some(dir) = d {
        if dir.extension().and_then(|s| s.to_str()) == Some("app") { app = Some(dir.to_path_buf()); break; }
        d = dir.parent();
    }
    let (Some(app), Some(desktop)) = (app, dirs::desktop_dir()) else { return; };
    let link = desktop.join("GenericAgent.app");
    let _ = std::fs::remove_file(&link);
    let _ = std::os::unix::fs::symlink(&app, &link);
}

#[cfg(all(not(windows), not(target_os = "linux"), not(target_os = "macos")))]
fn ensure_desktop_shortcut() {}

/// First-run shortcut handling for portable bundles (all platforms). Self-heals the shortcut
/// path on every enabled launch (cheap, no UI). The first-run ASK is driven by the frontend
/// (see the `shortcut_should_ask` / `shortcut_decide` commands): a native dialog from this
/// background startup thread has no parent window and gets buried behind the main window on
/// first launch, so the prompt is owned by the web UI instead, which always renders on top.
fn maybe_setup_shortcut() {
    if bundle_root().is_none() {
        return;
    }
    // Only self-heal when the user already opted in. Never prompt here.
    if read_shortcut_pref() == Some(true) {
        ensure_desktop_shortcut();
    }
}

/// Frontend asks whether to show the first-run "create desktop shortcut?" prompt.
/// True only on a portable bundle whose preference has never been set.
#[tauri::command]
fn shortcut_should_ask() -> bool {
    bundle_root().is_some() && read_shortcut_pref().is_none()
}

/// Frontend reports the user's choice. Persists it and creates the shortcut when enabled.
#[tauri::command]
fn shortcut_decide(create: bool) {
    write_shortcut_pref(create);
    if create {
        ensure_desktop_shortcut();
    }
}

/// True when this binary is running from inside a macOS .app bundle (packaged build).
/// Used to refuse stale ~/.ga_desktop_settings.json that could point at an old checkout
/// when App Translocation hides our own runtime/ from current_exe().
#[cfg(target_os = "macos")]
fn running_inside_app_bundle() -> bool {
    std::env::current_exe()
        .ok()
        .map(|p| {
            p.components().any(|c| {
                c.as_os_str().to_string_lossy().ends_with(".app")
            })
        })
        .unwrap_or(false)
}

/// Read config from settings file, or auto-discover and save.
/// Self-contained bundles always prefer their own runtime/app over stale user settings,
/// otherwise an old ~/.ga_desktop_settings.json can silently point the UI at a different checkout.
pub fn get_or_discover_config() -> (String, String) {
    let path = settings_path();

    if bundle_root().is_some() {
        let python = find_python();
        let project = find_project_dir().unwrap_or_default();
        if !python.is_empty() && !project.is_empty() {
            merge_settings(serde_json::json!({
                "python_path": python,
                "project_dir": project
            }));
            return (resolve_python(python, &project), project);
        }
    }

    // Try reading existing settings.
    // On macOS, a packaged .app must never trust ~/.ga_desktop_settings.json: App
    // Translocation can run the bundle from a random read-only copy where bundle_root()
    // fails to see our own runtime/, and an old settings file would then silently point
    // the bridge at a previously installed checkout. In that case fall through to
    // auto-discovery (which still resolves the bundle via .app-relative search below).
    #[cfg(target_os = "macos")]
    let trust_settings = !running_inside_app_bundle();
    #[cfg(not(target_os = "macos"))]
    let trust_settings = true;

    if trust_settings && path.exists() {
        if let Ok(content) = std::fs::read_to_string(&path) {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&content) {
                let python = val.get("python_path")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let project = val.get("project_dir")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if !python.is_empty() && !project.is_empty() {
                    return (resolve_python(python, &project), project);
                }
            }
        }
    }

    // Auto-discover
    let python = find_python();
    let project = find_project_dir().unwrap_or_default();

    // Save discovered config
    if !python.is_empty() && !project.is_empty() {
        merge_settings(serde_json::json!({
            "python_path": python,
            "project_dir": project
        }));
    }

    (resolve_python(python, &project), project)
}

/// Self-contained bundle support dir: holds python/, wheels/, install_windows.ps1 and app/.
/// Typical portable layout keeps only the exe (+README) at the top level and tucks everything
/// else under <exe dir>/runtime/. Returns None when this is not a bundle (e.g. dev build).
fn bundle_root() -> Option<PathBuf> {
    let runtime = bundle_anchor_dir()?.join("runtime");
    if runtime.join("app").join("agentmain.py").exists() {
        return Some(runtime);
    }
    None
}

/// Marker written after a successful offline prepare. Must live in a writable dir: the
/// in-bundle runtime/ is read-only, so writing .prepared there silently fails and
/// needs_first_run_prepare() stays true (re-running prepare on every launch). We put it
/// under the app-support dir (same place as the writable app clone).
fn prepared_marker() -> Option<PathBuf> {
    Some(dirs::data_dir()?.join("GenericAgent").join(".prepared"))
}

/// True when this is a self-contained bundle whose python env has not been prepared yet
/// (embedded python present but deps not yet installed into it).
fn needs_first_run_prepare(project_dir: &str) -> bool {
    if project_dir.is_empty() { return false; }
    bundle_python().is_some() && prepared_marker().map(|m| !m.exists()).unwrap_or(false)
}

/// Clear env vars a host launcher injects pointing at its own runtime. The Linux AppImage exports
/// PYTHONHOME/PYTHONPATH (-> bundled python crashes with "No module named 'encodings'") and
/// LD_LIBRARY_PATH (-> wrong shared libs). Our bundled python / prepare / bridge must run clean.
fn sanitize_bundle_env(cmd: &mut Command) {
    cmd.env_remove("PYTHONHOME");
    cmd.env_remove("PYTHONPATH");
    cmd.env_remove("LD_LIBRARY_PATH");
    // Stamp the gateway/kernel we spawn with this build's id (used by the bundle prepare flow
    // and any future identity checks).
    cmd.env("GA_BUILD_ID", env!("GA_BUILD_ID"));
}

/// Run the offline prepare (install_windows.ps1 -Mode PrepareOnly) using bundled python + wheels.
/// Streams the script's stdout and forwards GAPROGRESS markers to `report(pct, message)`.
/// Blocking; intended to run on a background thread. Writes ~/.ga_desktop_settings.json.
fn run_offline_prepare(project_dir: &str, report: &dyn Fn(i32, &str)) -> Result<(), String> {
    let root = bundle_root().ok_or("cannot locate bundle root")?;
    let wheels = root.join("wheels");

    // 脚本(wheels)仍在包内 runtime/ 读取（只读，OK）；python 必须用可写的 app-support 副本，
    // 否则 pip install 写包内 site-packages 会因只读失败。ensure_writable_runtime() 已先把
    // runtime/python 克隆到 app-support/python，故这里 find_python() 在 bundle 模式返回可写副本。
    let script = if cfg!(windows) {
        root.join("install_windows.ps1")
    } else if cfg!(target_os = "macos") {
        root.join("install_macos.sh")
    } else {
        root.join("install_linux.sh")
    };
    let py = PathBuf::from(find_python());

    if !script.exists() || !py.exists() || !wheels.exists() {
        return Err(format!("prepare resources missing under {:?}", root));
    }

    #[cfg(windows)]
    let mut cmd = {
        let mut c = Command::new("powershell.exe");
        c.args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"])
            .arg(&script)
            .arg("-PythonPath").arg(&py)
            .arg("-ProjectDir").arg(project_dir)
            .arg("-WheelDir").arg(&wheels)
            .arg("-ExtraPipPackages").arg("fastapi uvicorn websockets")
            // -NoVenv: install deps straight into the embedded python (no venv) so the
            // bundle is relocatable. See prepared_marker / find_python.
            .args(["-Mode", "PrepareOnly", "-SkipNpmInstall", "-NoVenv"]);
        c
    };
    #[cfg(not(windows))]
    let mut cmd = {
        let mut c = Command::new("bash");
        c.arg(&script)
            .arg("--python-path").arg(&py)
            .arg("--project-dir").arg(project_dir)
            .arg("--wheel-dir").arg(&wheels)
            .arg("--extra-packages").arg("fastapi uvicorn websockets")
            // --no-venv: install deps straight into the embedded python (no venv) so the
            // bundle is relocatable. See prepared_marker / find_python.
            .args(["--mode", "PrepareOnly", "--no-venv"]);
        c
    };

    cmd.stdout(Stdio::piped()).stderr(Stdio::null());
    sanitize_bundle_env(&mut cmd);
    #[cfg(windows)]
    cmd.creation_flags(0x08000000); // CREATE_NO_WINDOW
    let mut child = cmd.spawn().map_err(|e| format!("failed to launch prepare: {}", e))?;

    // Forward the script's ASCII progress keys to the loading window, which localizes them
    // (window.gaProgress maps key -> zh/en by navigator.language).
    if let Some(out) = child.stdout.take() {
        for line in BufReader::new(out).lines().flatten() {
            if let Some(key) = line.trim().strip_prefix("GAPROGRESS|") {
                match key.trim() {
                    "venv" => report(15, "venv"),
                    "deps" => report(45, "deps"),
                    "done" => report(90, "done"),
                    _ => {}
                }
            }
        }
    }

    let status = child.wait().map_err(|e| format!("prepare wait failed: {}", e))?;
    if !status.success() {
        return Err(format!("prepare exited with status {:?}", status.code()));
    }
    // Record success so later launches (and relocated copies) skip the prepare step.
    if let Some(marker) = prepared_marker() {
        let _ = std::fs::write(&marker, b"ok\n");
    }
    Ok(())
}

/// Last resort when a stale listener on :14168 (from a crashed previous run) holds the port:
/// force-kill whatever process is listening so our in-process gateway can bind it.
/// Phase 2: there is no separate "bridge" process — the gateway IS the :14168 server — but a
/// leftover listener from a prior crash must still be cleared.
fn force_free_bridge_port() {
    #[cfg(windows)]
    {
        // netstat -ano: last column is the PID for the :14168 LISTENING row.
        if let Ok(out) = Command::new("netstat").args(["-ano", "-p", "tcp"]).output() {
            let text = String::from_utf8_lossy(&out.stdout);
            for line in text.lines() {
                if line.contains(":14168") && line.to_uppercase().contains("LISTENING") {
                    if let Some(pid) = line.split_whitespace().last() {
                        let mut c = Command::new("taskkill");
                        c.args(["/F", "/PID", pid]);
                        c.creation_flags(0x08000000);
                        let _ = c.status();
                    }
                }
            }
        }
    }
    #[cfg(not(windows))]
    {
        // lsof prints the listening PIDs; kill -9 each.
        if let Ok(out) = Command::new("lsof").args(["-ti", "tcp:14168", "-sTCP:LISTEN"]).output() {
            for pid in String::from_utf8_lossy(&out.stdout).split_whitespace() {
                let _ = Command::new("kill").args(["-9", pid]).status();
            }
        }
    }
}

fn is_bridge_running() -> bool {
    TcpStream::connect(("127.0.0.1", 14168)).is_ok()
}

fn wait_for_port(port: u16, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

/// Phase 2: run the Rust/axum gateway in-process (Tauri's async runtime) and supervise only the
/// Python kernel subprocess. The kernel spawns the legacy `desktop_bridge.py` on `GA_LEGACY_PORT`
/// for any not-yet-migrated routes (strangler fallback), so the kernel is the only Python
/// subprocess Tauri manages directly. Once every route is migrated (plan 2.1) the legacy fallback
/// is removed and the kernel becomes the sole subprocess.
fn spawn_gateway(python_path: &str, project_dir: &str) {
    let cfg = GatewayConfig {
        root: PathBuf::from(project_dir),
        port: 14168,
        conductor_port: 8900,
        fallback: String::new(),
        legacy_port: 14169,
        kernel_python: python_path.to_string(),
        kernel_data_dir: String::new(), // production: kernel shares the real project root
    };
    tauri::async_runtime::spawn(async move {
        if let Err(e) = ga_desktop_gateway::serve(cfg).await {
            eprintln!("[tauri] gateway exited with error: {}", e);
        }
    });
}

fn show_bridge_window(app_handle: &tauri::AppHandle) {
    if let Some(main_win) = app_handle.get_webview_window("main") {
        let url = tauri::Url::parse("http://127.0.0.1:14168/").unwrap();
        let _ = main_win.navigate(url);
        let _ = main_win.show();
        let _ = main_win.set_focus();
    }
    if let Some(setup_win) = app_handle.get_webview_window("setup") {
        let _ = setup_win.hide();
    }
}

#[tauri::command]
fn start_bridge_with_config(app_handle: tauri::AppHandle, python_path: String, project_dir: String) -> Result<(), String> {
    // Save to settings (merge so sibling keys like desktop_shortcut survive).
    merge_settings(serde_json::json!({"python_path": python_path, "project_dir": project_dir}));

    spawn_gateway(&python_path, &project_dir);

    // Wait for port
    if !wait_for_port(14168, Duration::from_secs(20)) {
        return Err("Bridge did not become ready within 20s".into());
    }

    show_bridge_window(&app_handle);
    Ok(())
}

#[tauri::command]
fn start_bridge(app_handle: tauri::AppHandle) -> Result<(), String> {
    let (python_path, project_dir) = get_or_discover_config();
    spawn_gateway(&python_path, &project_dir);
    if !wait_for_port(14168, Duration::from_secs(20)) {
        return Err("Bridge did not become ready within 20s".into());
    }
    show_bridge_window(&app_handle);
    Ok(())
}

#[tauri::command]
fn get_config() -> (String, String) {
    get_or_discover_config()
}

#[tauri::command]
fn export_mykey(content: String) -> Result<Option<String>, String> {
    let path = rfd::FileDialog::new()
        .set_file_name("mykey.py")
        .add_filter("Python", &["py"])
        .save_file();
    match path {
        Some(p) => {
            std::fs::write(&p, content.as_bytes()).map_err(|e| e.to_string())?;
            Ok(Some(p.to_string_lossy().into_owned()))
        }
        None => Ok(None),
    }
}

#[tauri::command]
fn pick_folder() -> Option<String> {
    rfd::FileDialog::new().pick_folder().map(|p| p.to_string_lossy().into_owned())
}

// 原生系统通知(Tauri桌面端):WKWebView不支持Web Notification API,由Rust端通过tauri-plugin-notification发出
#[tauri::command]
fn notify(app: tauri::AppHandle, title: String, body: String) -> Result<(), String> {
    use tauri_plugin_notification::NotificationExt;
    app.notification()
        .builder()
        .title(&title)
        .body(&body)
        .show()
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let args: Vec<String> = std::env::args().collect();
    let no_autostart = args.iter().any(|a| a == "--no-autostart");
    let dev_mode = args.iter().any(|a| a == "--dev");

    let project_dir = find_project_dir().unwrap_or_default();
    let needs_prepare = needs_first_run_prepare(&project_dir);

    // Phase 2: the gateway serves :14168 in-process. Free any stale listener from a previous
    // crash so our fresh gateway can bind it.
    if is_bridge_running() {
        eprintln!("[tauri] :14168 already held; freeing stale listener");
        force_free_bridge_port();
    }

    let bridge_ok = is_bridge_running();
    let mut spawned_bridge = false;
    // Skip the early spawn when a first-run prepare is required (no venv yet);
    // the setup thread prepares the env first and then starts the bridge.
    if !bridge_ok && !no_autostart && !needs_prepare {
        let (py_str, dir_str) = get_or_discover_config();
        spawn_gateway(&py_str, &dir_str);
        spawned_bridge = true;
    }

    tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(|app, _args, _cwd| {
            if let Some(w) = app.get_webview_window("main") {
                let _ = w.unminimize();
                let _ = w.show();
                let _ = w.set_focus();
            }
        }))
        .plugin(tauri_plugin_notification::init())
        .invoke_handler(tauri::generate_handler![start_bridge_with_config, start_bridge, get_config, export_mykey, shortcut_should_ask, shortcut_decide, pick_folder, notify])
        .setup(move |app| {
            // Show the loading window immediately so the first-run prepare isn't a blank screen.
            // The window starts on loading.html (a local page), so no "connection refused" flash.
            if let Some(w) = app.get_webview_window("main") {
                let _ = w.show();
            }

            let handle = app.handle().clone();
            let project_dir = project_dir.clone();
            thread::spawn(move || {
                // Progress reporter: push status into the loading window (window.gaProgress).
                let main_win = handle.get_webview_window("main");

                let report = |pct: i32, msg: &str| {
                    if let Some(w) = &main_win {
                        let js = format!(
                            "window.gaProgress && window.gaProgress({}, {})",
                            pct,
                            serde_json::to_string(msg).unwrap_or_else(|_| "\"\"".to_string())
                        );
                        let _ = w.eval(&js);
                    }
                };

                // First-run (self-contained bundle): prepare the embedded python env offline,
                // then start the bridge with the freshly created venv.
                if needs_prepare {
                    report(5, "start");
                    if let Err(e) = run_offline_prepare(&project_dir, &report) {
                        eprintln!("[tauri] first-run prepare failed: {}", e);
                        if let Some(sw) = handle.get_webview_window("setup") { let _ = sw.show(); }
                        if let Some(mw) = handle.get_webview_window("main") { let _ = mw.hide(); }
                        return;
                    }
                    report(95, "starting");
                    if !is_bridge_running() {
                        let (py_str, dir_str) = get_or_discover_config();
                        spawn_gateway(&py_str, &dir_str);
                    }
                }

                // First run (prepare) and cold bridge start may take a while; allow up to 60s.
                let wait = if needs_prepare || spawned_bridge {
                    Duration::from_secs(60)
                } else {
                    Duration::from_secs(2)
                };
                let bridge_ready = wait_for_port(14168, wait);

                if bridge_ready {
                    // The bridge auto-starts conductor + scheduler itself (on_startup), so we do
                    // NOT probe their ports here: that would self-detect the bridge's own
                    // just-started extras and falsely report "ports busy".
                    if !wait_for_port(14168, Duration::from_secs(15)) {
                        eprintln!("[tauri] bridge not reachable before navigate");
                        if let Some(w) = &main_win {
                            let msg = "无法连接 bridge (127.0.0.1:14168)，请关闭程序后重试。";
                            let js = format!(
                                "alert({})",
                                serde_json::to_string(msg).unwrap_or_else(|_| "\"\"".to_string())
                            );
                            let _ = w.eval(&js);
                        }
                        return;
                    }
                    // Navigate to the bridge HTTP only after it is ready.
                    if let Some(w) = handle.get_webview_window("main") {
                        if let Ok(url) = tauri::Url::parse("http://127.0.0.1:14168/") {
                            let _ = w.navigate(url);
                        }
                        if dev_mode {
                            w.open_devtools();
                        } else {
                            // Disable F5/F12/Ctrl+R/right-click in production
                            let _ = w.eval(r#"
                                document.addEventListener('keydown', function(e) {
                                    if (e.key === 'F12' || e.key === 'F5' ||
                                        (e.ctrlKey && e.key === 'r') ||
                                        (e.ctrlKey && e.shiftKey && e.key === 'I')) {
                                        e.preventDefault();
                                    }
                                });
                                document.addEventListener('contextmenu', function(e) {
                                    e.preventDefault();
                                });
                            "#);
                        }
                        let _ = w.show();
                        let _ = w.set_focus();
                    }
                    if let Some(sw) = handle.get_webview_window("setup") { let _ = sw.hide(); }
                    // App is up and reachable: ask-once / self-heal the desktop shortcut.
                    // Runs last so it never blocks the loading/navigation path.
                    maybe_setup_shortcut();
                } else {
                    // Bridge never came up -> let the user fix paths in the setup window.
                    if let Some(sw) = handle.get_webview_window("setup") {
                        if dev_mode { sw.open_devtools(); }
                        let _ = sw.show();
                    }
                    if let Some(mw) = handle.get_webview_window("main") { let _ = mw.hide(); }
                }
            });
            Ok(())
        })
        .on_window_event(|window, event| {
            if let tauri::WindowEvent::CloseRequested { .. } = event {
                let label = window.label();
                if label == "main" {
                    // Persistent backend: closing the window does NOT stop the bridge or its
                    // services, so relaunching attaches to the warm backend on 14168.
                    window.app_handle().exit(0);
                } else if label == "setup" {
                    // Setup closed -> exit if main is not visible
                    if let Some(main_win) = window.app_handle().get_webview_window("main") {
                        if !main_win.is_visible().unwrap_or(false) {
                            window.app_handle().exit(0);
                        }
                    } else {
                        window.app_handle().exit(0);
                    }
                }
            }
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
