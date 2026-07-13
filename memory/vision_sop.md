# Vision API SOP

## ⚠️ 前置规则（必须遵守）

1. **先枚举窗口**：调用 vision 前必须先用 `pygetwindow` 枚举窗口标题，确认目标窗口存在且已激活到前台。窗口不存在就不要截图。
2. **🚫 禁止全屏截图**：必须先利用ljqCtrl截取窗口区域。能截局部（如标题栏）就不截整窗口，能截窗口就绝不全屏。全屏截图在任何场景下都不允许。
3. **能不用 vision 就不用**：如果窗口标题/本地 OCR（`ocr_utils.py`）能获取所需信息，就不要调用 vision API，省 token 且更可靠。Vision 是最后手段。

## 快速用法

```python
from vision_api import ask_vision
result = ask_vision(image, prompt="描述图片内容", timeout=60, max_pixels=1_440_000)
# image: 文件路径(str/Path) 或 PIL Image
# backend: 'claude'(默认) | 'openai' | 'modelscope'
# 返回 str：成功为模型回复，失败为 'Error: ...'
```

## 在 code_run 中调用（输出回显）

`ask_vision` 含网络 IO，在 `code_run` 用 `type=python` 执行时可能拿不到输出（不报错但无回显，多次踩坑）。可靠做法：用 `type=bash` + heredoc + `.venv/bin/python`，stdout 正常回显：

```bash
.venv/bin/python - <<'PY'
from vision_api import ask_vision
print(ask_vision("path/to/img.png", prompt="描述内容"))
PY
```

> ⚠️ **code_run PYTHONPATH 陷阱**：`type=bash` 启动的子 python（`.venv/bin/python` 或 `/usr/bin/python3`）**不继承主会话 python 的 PYTHONPATH**，`memory` 不在 `sys.path` → 直接 `import vision_api` / `ocr_utils` 等 memory 模块会 `ModuleNotFoundError`（已实测确认）。heredoc 内须先补：`import sys; sys.path.insert(0, "memory")`（相对 GA 仓库根 cwd，已验证可行）。

## 如果没有 `vision_api.py`，初次构建vision能力

1. 复制 `memory/vision_api.template.py` → `memory/vision_api.py`
2. 只改头部"用户配置区"：去 `mykey.py` 里扫描变量名（⚠️ 只看名字，禁止输出 apikey 值），尝试找能用配置名填入 `CLAUDE_CONFIG_KEY` / `OPENAI_CONFIG_KEY`，`DEFAULT_BACKEND` 选后端，并测试
3. 保底：没有可用 config 时去 `https://modelscope.cn/my/myaccesstoken` 申请 token 填入 `MODELSCOPE_API_KEY`
