"""
py2app setup script for GenericAgent macOS .app bundle.

Usage:
    pip install py2app
    python setup_app.py py2app

The built app will be in ./dist/GenericAgent.app
"""
from setuptools import setup

APP = ['launch.pyw']
DATA_FILES = [
    ('assets', [
        'assets/images/logo.jpg',
        'assets/sys_prompt.txt',
        'assets/sys_prompt_en.txt',
        'assets/tools_schema.json',
        'assets/tools_schema_cn.json',
        'assets/global_mem_insight_template.txt',
        'assets/global_mem_insight_template_en.txt',
        'assets/insight_fixed_structure.txt',
        'assets/insight_fixed_structure_en.txt',
    ]),
    ('frontends', []),  # will be populated by include_packages
]

OPTIONS = {
    'argv_emulation': False,
    'iconfile': 'GenericAgent.icns',  # will create below
    'plist': {
        'CFBundleName': 'GenericAgent',
        'CFBundleDisplayName': 'GenericAgent',
        'CFBundleIdentifier': 'com.genericagent.app',
        'CFBundleVersion': '0.1.0',
        'CFBundleShortVersionString': '0.1.0',
        'LSMinimumSystemVersion': '11.0',
        'NSHighResolutionCapable': True,
    },
    'includes': [
        'webview', 'streamlit', 'bottle', 'aiohttp',
        'requests', 'bs4',
    ],
    'packages': [
        'streamlit', 'webview', 'ga', 'frontends',
        'certifi', 'altair', 'pandas', 'numpy',
    ],
    'excludes': ['tkinter', 'test'],
    'resources': ['assets', 'frontends', 'ga', 'plugins', 'reflect', 'memory'],
    'semi_standalone': False,
}

setup(
    app=APP,
    name='GenericAgent',
    data_files=DATA_FILES,
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
)
