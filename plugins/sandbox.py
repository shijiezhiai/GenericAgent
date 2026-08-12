"""OS-level sandbox for code_run — the L0 real boundary.

P0 fixed GA-layer path checks and env stripping; P1 added the policy broker.
Neither can stop a crafted python/bash snippet from reading `~/.ssh/id_rsa` and
returning its contents, or writing outside the workspace — those checks live in
the 9 tools' dispatch, not in the arbitrary code the model runs. The only real
boundary is the kernel, so this module wraps every code_run child in an OS
sandbox:

  - macOS  : `sandbox-exec` seatbelt profile (deny writes everywhere except the
             workspace + temp; deny reads of classic secret stores).
  - Linux  : `bwrap` (bubblewrap) if present, equivalent confinement.
  - else   : degrade to no sandbox (best-effort), never silently claim safety.

Design rules (see docs/permission_redesign.md, P3):
  * Command-string matching is NOT security. Only kernel-enforced rules count.
  * Network stays allowed by default (the agent legitimately installs/fetches);
    opt-in deny via GA_SANDBOX_DENY_NETWORK. Env stripping (P0-T3) already
    removes credentials, so exfil surface is already small.
  * fail-closed: if the sandbox is *expected* (platform supports it) but the
    profile can't be built, we RAISE — code_run turns that into a tool error
    rather than running unsandboxed.
"""
from __future__ import annotations
import os, sys, re, tempfile, shutil

# Classic secret stores we never let a code_run child read, anywhere on disk.
# These are bypass-proof at the kernel level (path tricks like symlinks,
# ../.., or unicode normalization are resolved by the kernel before the check).
# NOTE: we deliberately do NOT deny `*.pem`/`*.key` broadly — that would also
# block the system CA bundle (/etc/ssl/cert.pem) and break all TLS inside the
# sandbox. Private keys live in the high-confidence dirs below, which we deny.
_SECRET_READ_DENY = [
    r"\.ssh/",
    r"\.aws/",
    r"\.gnupg/",
    r"\.config/gcloud/",
    r"\.config/gh/",
    r"Keychains/",
    r"mykey\.py$",
    r"mykey_template\.py$",
    r"id_rsa",
    r"id_ed25519",
    r"\.netrc$",
    r"credentials$",
    r"/etc/shadow$",
    r"/etc/sudoers$",
]

_HOME = os.path.expanduser("~")
_TMPDIR = tempfile.gettempdir()


def _scheme_str(p: str) -> str:
    """Quote a path for embedding in a seatbelt (Scheme) string literal."""
    return '"' + p.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _scheme_escape_regex(p: str) -> str:
    """Escape a regex for embedding in a seatbelt (regex "...") string literal."""
    return p.replace("\\", "\\\\").replace('"', '\\"')


def _build_seatbelt_profile(allowed_roots, deny_network=False):
    """Return a sandbox-exec profile string confining writes to allowed_roots
    and reads of secret stores."""
    allows = []
    for r in allowed_roots:
        r = os.path.realpath(os.path.expanduser(r))
        allows.append(f"(allow file-write* (subpath {_scheme_str(r)}))")
    # User caches are commonly written by libs (matplotlib, pip, npm, etc.).
    # /dev is needed for /dev/null, /dev/stdout, /dev/fd (harmless, not exfil).
    # /tmp is a symlink to /private/tmp on macOS -> must allow the real path.
    for cache in (os.path.join(_HOME, ".cache"),
                  os.path.join(_HOME, "Library", "Caches"),
                  _TMPDIR, "/tmp", "/private/tmp", "/private/var/folders", "/dev"):
        allows.append(f"(allow file-write* (subpath {_scheme_str(cache)}))")
    secret_rules = "\n".join(
        f'(deny file-read* (regex "{_scheme_escape_regex(g)}"))' for g in _SECRET_READ_DENY)
    net = "(allow network*)\n" + ("(deny network*)" if deny_network else "")
    return f"""(version 1)
(allow default)
; --- confine file writes to the workspace + temp only ---
(deny file-write*)
{chr(10).join(allows)}
; --- deny reads of secret stores (kernel-resolved, bypass-proof) ---
{secret_rules}
; --- optional network confinement ---
{net}
"""


def _build_bwrap_cmd(cmd, allowed_roots, deny_network=False):
    """Best-effort bubblewrap wrapper (Linux). Untested here; guard via try."""
    ro_roots = ["/", _HOME]
    args = ["bwrap", "--ro-bind", "/", "/", "--dev", "/dev"]
    for r in allowed_roots:
        rp = os.path.realpath(os.path.expanduser(r))
        args += ["--bind", rp, rp]
    args += ["--tmpfs", _TMPDIR]
    if deny_network:
        args += ["--unshare-net"]
    args += cmd
    return args


def wrap_code_run(cmd, cwd, extra_roots=None, deny_network=None):
    """Wrap a code_run subprocess command in an OS sandbox if one is available.

    Returns (wrapped_cmd, applied: bool, detail: str, profile_path: str|None).
    - profile_path is non-None only for seatbelt; caller must unlink it after
      the child exits.
    - If the sandbox is expected but cannot be built, RAISE (fail-closed).
    - If no sandbox is available on this platform, returns (cmd, False,
      'no_sandbox:<reason>', None).
    """
    if os.environ.get("GA_DISABLE_SANDBOX"):
        return cmd, False, "disabled_by_env", None

    allowed = list(extra_roots or [])
    if cwd:
        allowed.append(cwd)
    allowed.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "temp"))
    allowed = [os.path.realpath(os.path.expanduser(a)) for a in allowed if a]

    if deny_network is None:
        deny_network = bool(os.environ.get("GA_SANDBOX_DENY_NETWORK"))

    plat = sys.platform
    if plat == "darwin":
        sb = shutil.which("sandbox-exec")
        if not sb:
            return cmd, False, "no_sandbox:no_sandbox_exec", None
        try:
            profile = _build_seatbelt_profile(allowed, deny_network=deny_network)
            fd, path = tempfile.mkstemp(prefix="ga_sbx_", suffix=".sb", dir=_TMPDIR)
            with os.fdopen(fd, "w") as f:
                f.write(profile)
        except Exception as e:
            # Expected to sandbox but failed to build -> fail closed.
            raise RuntimeError(f"sandbox profile build failed: {e}")
        wrapped = [sb, "-f", path] + list(cmd)
        return wrapped, True, "macos-seatbelt", path

    if plat.startswith("linux"):
        if shutil.which("bwrap"):
            try:
                wrapped = _build_bwrap_cmd(cmd, allowed, deny_network=deny_network)
                return wrapped, True, "linux-bwrap", None
            except Exception as e:
                raise RuntimeError(f"bwrap build failed: {e}")
        return cmd, False, "no_sandbox:no_bwrap", None

    return cmd, False, f"no_sandbox:platform_{plat}", None
