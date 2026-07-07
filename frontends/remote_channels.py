"""远控通道注册表与生命周期管理。

各社交平台 frontend 的元数据注册，以及通道进程的启动/停止/状态管理。
Qt 主界面通过 ChannelManager 实例管理所有远控通道的生命周期。
"""

import json, os, signal, subprocess, sys, time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHANNELS_STATE_FILE = Path.home() / ".ga_channels.json"

# ── SVG 图标 (用于 Qt 卡片展示) ─────────────────────────────────────────────
_SVG_WECHAT = '<svg viewBox="0 0 24 24" fill="{c}"><path d="M8.691 2.188C3.891 2.188 0 5.476 0 9.53c0 2.212 1.17 4.203 3.002 5.55a.59.59 0 0 1 .213.665l-.39 1.48c-.019.07-.048.141-.048.213 0 .163.13.295.29.295a.326.326 0 0 0 .167-.054l1.903-1.114a.864.864 0 0 1 .717-.098 10.16 10.16 0 0 0 2.837.403c.276 0 .543-.027.811-.05-.857-2.578.157-4.972 1.932-6.446 1.703-1.415 3.882-1.98 5.853-1.838-.576-3.583-4.196-6.348-8.596-6.348zM5.785 5.991c.642 0 1.162.529 1.162 1.18a1.17 1.17 0 0 1-1.162 1.178A1.17 1.17 0 0 1 4.623 7.17c0-.651.52-1.18 1.162-1.18zm5.813 0c.642 0 1.162.529 1.162 1.18a1.17 1.17 0 0 1-1.162 1.178 1.17 1.17 0 0 1-1.162-1.178c0-.651.52-1.18 1.162-1.18zm5.34 2.867c-1.797-.052-3.746.512-5.28 1.786-1.72 1.428-2.687 3.72-1.78 6.22.942 2.453 3.666 4.229 6.884 4.229.826 0 1.622-.12 2.361-.336a.722.722 0 0 1 .598.082l1.584.926a.272.272 0 0 0 .14.047c.134 0 .24-.111.24-.247 0-.06-.023-.12-.038-.177l-.327-1.233a.582.582 0 0 1-.023-.156.49.49 0 0 1 .201-.398C23.024 18.48 24 16.82 24 14.98c0-3.21-2.931-5.837-7.062-6.122zm-2.036 2.87c.535 0 .969.44.969.982a.976.976 0 0 1-.969.983.976.976 0 0 1-.969-.983c0-.542.434-.983.97-.983zm4.072 0c.535 0 .969.44.969.982a.976.976 0 0 1-.969.983.976.976 0 0 1-.969-.983c0-.542.434-.983.97-.983z"/></svg>'
_SVG_WECOM = '<svg viewBox="0 0 24 24" fill="{c}"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 15h-2v-2h2v2zm0-4h-2V7h2v6zm4 4h-2v-2h2v2zm0-4h-2V7h2v6z"/></svg>'
_SVG_QQ = '<svg viewBox="0 0 24 24" fill="{c}"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm3.61 14.15c-.53.17-1.07-.11-1.72-.48-.37.67-.97 1.33-1.89 1.33s-1.52-.66-1.89-1.33c-.65.37-1.19.65-1.72.48-.78-.25-.6-1.26-.22-2.17-.86-.67-1.48-1.56-1.48-2.75 0-2.62 2.38-4.74 5.31-4.74s5.31 2.12 5.31 4.74c0 1.19-.62 2.08-1.48 2.75.38.91.56 1.92-.22 2.17z"/></svg>'
_SVG_FEISHU = '<svg viewBox="0 0 24 24" fill="{c}"><path d="M3.474 4.95a.477.477 0 0 1 .19-.611L12 .062l8.336 4.277a.477.477 0 0 1 .19.611L12.472 23.23a.477.477 0 0 1-.944 0L3.474 4.95z"/></svg>'
_SVG_DINGTALK = '<svg viewBox="0 0 24 24" fill="{c}"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm4.64 6.8c-.15 1.58-.8 5.42-.8 5.42s-.04.55-.44.64c-.25.06-.63-.14-.89-.33-.2-.15-3.04-1.98-3.48-2.3-.12-.08-.25-.25-.04-.44l3.33-3.18c.17-.16.2-.42-.04-.3-1.2.6-5.08 3.26-5.56 3.57-.48.31-1.07.22-1.07.22l-2.15-.7s-.5-.22-.1-.49c0 0 4.72-2.1 6.42-2.81 1.7-.72 5.86-2.56 5.86-2.56s.8-.36.74.5z"/></svg>'
_SVG_DISCORD = '<svg viewBox="0 0 24 24" fill="{c}"><path d="M20.317 4.37a19.791 19.791 0 0 0-4.885-1.515.074.074 0 0 0-.079.037c-.21.375-.444.864-.608 1.25a18.27 18.27 0 0 0-5.487 0 12.64 12.64 0 0 0-.617-1.25.077.077 0 0 0-.079-.037A19.736 19.736 0 0 0 3.677 4.37a.07.07 0 0 0-.032.027C.533 9.046-.32 13.58.099 18.057a.082.082 0 0 0 .031.057 19.9 19.9 0 0 0 5.993 3.03.078.078 0 0 0 .084-.028c.462-.63.874-1.295 1.226-1.994a.076.076 0 0 0-.041-.106 13.107 13.107 0 0 1-1.872-.892.077.077 0 0 1-.008-.128 10.2 10.2 0 0 0 .372-.292.074.074 0 0 1 .077-.01c3.928 1.793 8.18 1.793 12.062 0a.074.074 0 0 1 .078.01c.12.098.246.198.373.292a.077.077 0 0 1-.006.127 12.299 12.299 0 0 1-1.873.892.077.077 0 0 0-.041.107c.36.698.772 1.362 1.225 1.993a.076.076 0 0 0 .084.028 19.839 19.839 0 0 0 6.002-3.03.077.077 0 0 0 .032-.054c.5-5.177-.838-9.674-3.549-13.66a.061.061 0 0 0-.031-.03z"/></svg>'
_SVG_TELEGRAM = '<svg viewBox="0 0 24 24" fill="{c}"><path d="M11.944 0A12 12 0 0 0 0 12a12 12 0 0 0 12 12 12 12 0 0 0 12-12A12 12 0 0 0 12 0a12 12 0 0 0-.056 0zm4.962 7.224c.1-.002.321.023.465.14a.506.506 0 0 1 .171.325c.016.093.036.306.02.472-.18 1.898-.962 6.502-1.36 8.627-.168.9-.499 1.201-.82 1.23-.696.065-1.225-.46-1.9-.902-1.056-.693-1.653-1.124-2.678-1.8-1.185-.78-.417-1.21.258-1.91.177-.184 3.247-2.977 3.307-3.23.007-.032.014-.15-.056-.212s-.174-.041-.249-.024c-.106.024-1.793 1.14-5.061 3.345-.479.33-.913.49-1.302.48-.428-.008-1.252-.241-1.865-.44-.752-.245-1.349-.374-1.297-.789.027-.216.325-.437.893-.663 3.498-1.524 5.83-2.529 6.998-3.014 3.332-1.386 4.025-1.627 4.476-1.635z"/></svg>'

# placeholder for channel status display
_SVG_LINK = '<svg viewBox="0 0 24 24" fill="none" stroke="{c}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/></svg>'


@dataclass
class ChannelDef:
    """远控通道定义。"""
    key: str                # 唯一标识 e.g. "wechat"
    label: str              # 显示名称
    svg: str                # SVG 模板 (含 {c} 占位)
    icon_color: str         # 图标填充色
    description: str        # 卡片描述文字
    config_keys: list       # 需要用户提供的配置项 (空=扫码登录)
    module_name: str        # frontends 模块名 (无 .py)
    recommended: bool = True
CHANNEL_REGISTRY: list[ChannelDef] = [
    ChannelDef(
        key="wechat", label="微信", svg=_SVG_WECHAT, icon_color="#07C160",
        description="一键扫码关联微信，在微信联系人中与 Agent 对话，AI协作和沟通更简单。",
        config_keys=[],  # 扫码登录，无需手动填key
        module_name="wechatapp",
    ),
    ChannelDef(
        key="wecom", label="企业微信", svg=_SVG_WECOM, icon_color="#0082EF",
        description="扫码一键关联企微，后续可在企微群聊或私聊中直接与 Agent 沟通交互，实现高效协作。",
        config_keys=["wecom_bot_id", "wecom_secret"],
        module_name="wecomapp",
    ),
    ChannelDef(
        key="qq", label="QQ", svg=_SVG_QQ, icon_color="#12B7F5",
        description="极简配置流程，快速将 Agent 接入 QQ 中，后续可在 QQ 内与 Agent 方便对话交互。",
        config_keys=["qq_app_id", "qq_app_secret"],
        module_name="qqapp",
    ),
    ChannelDef(
        key="feishu", label="飞书", svg=_SVG_FEISHU, icon_color="#3370FF",
        description="将 Agent 接入飞书机器人，团队成员在飞书群聊或私聊中即可直接对话。",
        config_keys=["feishu_app_id", "feishu_app_secret"],
        module_name="fsapp",
    ),
    ChannelDef(
        key="dingtalk", label="钉钉", svg=_SVG_DINGTALK, icon_color="#0089FF",
        description="将 Agent 接入钉钉机器人，团队成员可在钉钉群聊或私聊中直接与 Agent 交互。",
        config_keys=["dingtalk_client_id", "dingtalk_client_secret"],
        module_name="dingtalkapp",
    ),
    ChannelDef(
        key="discord", label="Discord", svg=_SVG_DISCORD, icon_color="#5865F2",
        description="Discord Bot 接入，在 Discord 频道或私聊中与 Agent 对话交互。",
        config_keys=["discord_bot_token"],
        module_name="dcapp",
    ),
    ChannelDef(
        key="telegram", label="Telegram", svg=_SVG_TELEGRAM, icon_color="#26A5E4",
        description="Telegram Bot 接入，在 Telegram 中与 Agent 进行对话，支持私聊和群组。",
        config_keys=["tg_bot_token"],
        module_name="tgapp",
    ),
]


def get_channel(key: str) -> Optional[ChannelDef]:
    for ch in CHANNEL_REGISTRY:
        if ch.key == key:
            return ch
    return None


# ── 通道状态持久化 ───────────────────────────────────────────────────────────

def _load_state() -> dict:
    if CHANNELS_STATE_FILE.exists():
        try:
            return json.loads(CHANNELS_STATE_FILE.read_text("utf-8"))
        except Exception:
            pass
    return {}


def _save_state(state: dict):
    CHANNELS_STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), "utf-8")


# ── ChannelManager ───────────────────────────────────────────────────────────

class ChannelManager:
    """管理远控通道子进程的启动、停止与状态。"""

    def __init__(self):
        self._processes: dict[str, subprocess.Popen] = {}
        self._state = _load_state()

    def is_configured(self, key: str) -> bool:
        return key in self._state and self._state[key].get("configured", False)

    def is_running(self, key: str) -> bool:
        proc = self._processes.get(key)
        if proc is None:
            return False
        if proc.poll() is not None:
            del self._processes[key]
            return False
        return True

    def get_status(self, key: str) -> str:
        """返回 'running' | 'stopped' | 'unconfigured'"""
        if not self.is_configured(key):
            return "unconfigured"
        return "running" if self.is_running(key) else "stopped"

    def configure(self, key: str, config: dict):
        """保存通道配置。"""
        self._state[key] = {"configured": True, "config": config, "auto_start": True}
        _save_state(self._state)

    def start(self, key: str) -> tuple[bool, str]:
        """启动通道子进程。返回 (success, message)。"""
        if self.is_running(key):
            return True, "已在运行中"
        ch = get_channel(key)
        if not ch:
            return False, f"未知通道: {key}"
        if not self.is_configured(key):
            return False, "通道尚未配置"

        module_path = os.path.join(PROJECT_ROOT, "frontends", f"{ch.module_name}.py")
        if not os.path.exists(module_path):
            return False, f"模块文件不存在: {module_path}"

        try:
            proc = subprocess.Popen(
                [sys.executable, module_path],
                cwd=PROJECT_ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
            self._processes[key] = proc
            # 记住启用状态：用户主动启动的通道，重启后自动恢复
            if key in self._state:
                self._state[key]["auto_start"] = True
                _save_state(self._state)
            return True, f"{ch.label} 已启动 (PID: {proc.pid})"
        except Exception as e:
            return False, f"启动失败: {e}"

    def stop(self, key: str) -> tuple[bool, str]:
        """停止通道子进程。"""
        proc = self._processes.get(key)
        if proc is None or proc.poll() is not None:
            self._processes.pop(key, None)
            return True, "未在运行"
        try:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            del self._processes[key]
            # 记住停用状态：用户主动停止的通道，重启后不再自动拉起
            if key in self._state:
                self._state[key]["auto_start"] = False
                _save_state(self._state)
            return True, "已停止"
        except Exception as e:
            return False, f"停止失败: {e}"

    def remove(self, key: str):
        """移除通道配置并停止进程。"""
        self.stop(key)
        self._state.pop(key, None)
        _save_state(self._state)

    def auto_start_all(self):
        """启动所有标记为 auto_start 的通道。"""
        for key, info in self._state.items():
            if info.get("auto_start") and info.get("configured"):
                self.start(key)

    def stop_all(self):
        """停止所有运行中的通道。"""
        for key in list(self._processes.keys()):
            self.stop(key)
