# QQ Image Collector for Linux

一个运行在 Linux/Docker 上的 QQ 群 AI 原图采集工具。它通过 NapCat、
OneBot 和 QQ Chat Exporter（QCE）读取群消息并触发原图下载，只保留能够解析出
生成参数的图片。

## 功能

- 持续监听多个 QQ 群，并在重启后从 SQLite 游标继续补漏。
- 自动回填当前账号可见的群历史消息。
- 触发 OneBot `get_image` 获取原图，避免只处理缩略图缓存。
- 识别 NovelAI、A1111 兼容参数、NovelAI stealth/zTXt 和 ComfyUI 工作流；
  NovelAI 可读性按普通 PNG 文本、EXIF、Alpha 的官网回退顺序判定。
- 无参数图片和 GIF 立即淘汰；临时文件在每轮结束时清理。
- 按 SHA-256 去重，最终图片保存到 `NovelAI`、`ComfyUI`、
  `NAI含参但不可直接读取的` 或 `其他模型生成`。
- 保存群号、发送者 QQ、原始消息 ID、发送时间和完整参数等来源信息。
- 提供 FastAPI + React 本地控制台，管理群聊、回填任务和运行状态。
- Docker、NapCat 和采集 Worker 支持服务器重启后自动恢复。

## 处理链路

```text
QCE 读取实时消息与历史消息
  -> 保存可跨重启续跑的群游标
  -> 使用原始消息 ID 让 OneBot 重建图片 file token
  -> OneBot get_image 触发原图下载
  -> 在临时目录解析图片元数据
  -> 无参数/GIF：记录结果并删除
  -> 参数结构校验和来源分类
  -> 有效图片计算 SHA-256 并去重
  -> 保存到四类来源目录
  -> 更新 SQLite 来源、统计与回填进度
```

实时补漏、失败重试、手动任务和历史回填始终由同一个 Worker 串行执行，避免
并发下载、重复采集或游标互相覆盖。

## 组成

- `collector.py`：消息发现、原图下载、去重、回填和运行循环。
- `metadata_reader.py`：NovelAI、A1111 和 ComfyUI 参数解析。
- `METADATA_CLASSIFICATION.md`：最终四分类、接受条件和通道优先级。
- `qq_image_console/`：管理 API、单实例 Worker 和状态管理。
- `frontend/`：React 控制台。
- `linux/`：Docker Compose、初始化、登录探测和缓存清理工具。
- `linux/reclassify_repository.py`：先预演、再备份 SQLite 并前向修正现有图库，
  可恢复上次隔离但被新版确认有效的图片。

## 最小启动

需要预先安装 Docker 和 Docker Compose。

```bash
cd linux
cp .env.example .env
chmod 600 .env
python3 bootstrap.py
./manage.sh start
./manage.sh login
```

登录成功后可运行 `./manage.sh probe` 验证 OneBot 与 QCE。账号密码、WebUI
Token、运行数据库、QQ 缓存和最终图片均保存在被 `.gitignore` 排除的持久化目录，
不应提交到版本库。

## 测试

```bash
python -m unittest -v
```

本项目使用目标仓库中的 MIT License。
