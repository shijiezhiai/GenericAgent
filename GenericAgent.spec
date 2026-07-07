# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_all

datas = [('assets', 'assets'), ('frontends', 'frontends'), ('ga', 'ga'), ('plugins', 'plugins'), ('reflect', 'reflect'), ('memory', 'memory'), ('agentmain.py', '.'), ('llmcore.py', '.'), ('agent_loop.py', '.'), ('ga.py', '.'), ('simphtml.py', '.'), ('mykey.py', '.')]
binaries = []
hiddenimports = ['webview', 'streamlit', 'bottle', 'requests', 'bs4']
tmp_ret = collect_all('streamlit')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]


a = Analysis(
    ['launch.pyw'],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='GenericAgent',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['GenericAgent.icns'],
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='GenericAgent',
)
app = BUNDLE(
    coll,
    name='GenericAgent.app',
    icon='GenericAgent.icns',
    bundle_identifier=None,
)
