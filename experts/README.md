# GA 专家（Experts）

专家 = 「带人设 + 工具白名单 + 知识库」的可切换角色模式。**零核心代码改动**，由 `plugins/expert_mode.py`（仿 `plugins/project_mode.py`）通过 `agent_before` hook 每轮注入。

## 目录结构
```
experts/<name>/
  expert.md       人设：YAML frontmatter(role/goal/backstory/tools/model) + 正文「工作方法」
  knowledge.md    专家知识库（按需读，不每轮注入；如审查 checklist / 避坑点）
```

## 内置专家
- `code-reviewer` — 资深代码审查工程师

## 激活 / 切换 / 失活
agent 通过 code_run 调用：
```python
from plugins.expert_mode import activate, deactivate, list_experts
print(list_experts())        # 列出可用专家
activate('code-reviewer')    # 激活
deactivate()                 # 失活，回到普通模式
```
激活后，每轮用户消息末尾会自动追加该专家的人设 + 工作方法 + 知识指针 + 收尾纪律。专家隔离靠 PID 键控文件锚（`temp/.active_expert.<pid>`），多开 GA 互不可见；GA 重启自动失活。

## 新增专家
1. `experts/<新名>/expert.md`：写 frontmatter（role/goal/backstory，可选 tools/model）+ 正文工作方法。
2. （可选）`experts/<新名>/knowledge.md`：写该领域 checklist / 避坑点。
3. `activate('<新名>')` 即可。

## 设计要点
- **两层注入**：L1 每轮注入人设 + 方法 + 知识指针（行数/大小线索）；L2 = knowledge.md 全文不注入，模型按指针自行 file_read。
- **工具白名单为软约束**：frontmatter `tools` 仅在人设里提示优先使用，未硬过滤工具集（保持零核心改动）。如需硬过滤，可在 `inject_expert_persona` 中改写 `ctx['tools_schema']`。
- **模型偏好为提示**：frontmatter `model` 仅提示，未自动切换模型。
