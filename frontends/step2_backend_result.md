# Step2 Backend Result: 文件管理API

## SCHE_TASKS结构
```
sche_tasks/
├── flink_weekly_activity.json      ← 任务配置(JSON), taskId=flink_weekly_activity
├── flink_weekly_activity.py        ← 脚本文件
├── github_trending.py              ← 脚本文件
├── github_trending_daily.json      ← 任务配置(JSON), taskId=github_trending_daily
├── scheduler.log                   ← 调度日志(不在browse范围内)
├── done/                           ← 定时报告输出
│   ├── 2026-07-02_1400_github_trending_daily.md
│   ├── 2026-07-02_github_trending_daily.md
│   ├── 2026-07-03_1026_github_trending_daily.md
│   ├── 2026-07-03_1541_flink_weekly_activity.md
│   ├── 2026-07-03_flink_weekly_activity.md
│   └── 2026-07-03_github_trending_daily.md
```
Done文件命名: {date}_{可选time}_{taskId}.md, taskId通过匹配sche_tasks/*.json的stem提取。

## 新增端点

| 端点 | 方法 | Handler | Handler行号 | Route行号 |
|------|------|---------|------------|-----------|
| /api/files/browse | GET | files_browse_handler | L1621-1728 | L2289 |
| /api/files/delete | POST | files_delete_handler | L1731-1750 | L2290 |

文件总行数: 2289 → 2423 (+134行)

## 契约确认

### GET /api/files/browse ✅
- 聚合3源: chat(递归扫描temp/desktop_uploads/sess-*), task(sche_tasks/done/*.md), config(sche_tasks/*.{json,py})
- 每文件返回: name, path, size, mtime(ms整数), source, type(扩展名小写), session?, taskId?, referencedBy
- referencedBy逻辑:
  - chat: {type:"session", id:sess-xxx, alive:目录是否存在}
  - task: {type:"task", id:tid, alive:对应.json是否存在且enabled}
  - config: null
- counts: {all, chat, task, config}
- 路径安全: 限定在ga_root下, no ..穿越

### DELETE /api/files/delete ✅
- body {path}: 限定upload_root或sche_root, 拒绝越权路径(403)
- 空路径/外部路径正确拒绝
- 文件存在则unlink, 不存在静默成功
- 复用upload_delete_handler的Path.resolve()+parents in检查模式

## TEST
✅ 语法检查通过(py_compile)  
✅ 启动bridge(port 14170) → GET /api/files/browse → 200, 12 files (2 chat + 6 task + 4 config)  
✅ POST /api/files/delete {path:"/etc/passwd"} → 403 Forbidden  
✅ POST /api/files/delete {path:""} → 403 Forbidden  
✅ POST /api/files/delete {path:".../done/nonexistent.md"} → 200 ok (静默成功)

VERDICT: DONE
