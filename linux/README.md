# Linux deployment

## Services and ports

Compose 运行三个服务：官方 `napcat`、管理界面/事件 Worker
`collector-console`、每 6 小时运行一轮的 `cache-cleaner`。

| Endpoint | Host binding | Purpose |
| --- | --- | --- |
| NapCat WebUI | `0.0.0.0:10058` | 用户扫码与 NapCat 管理，使用独立强 Token |
| NapCat WebUI rescue | `127.0.0.1:16099` | SSH 本机救援 |
| Collector | `127.0.0.1:17890` | 由现有 Nginx `18080` 反代并进行 Token 校验 |
| OneBot HTTP | 仅 Compose `napcat:3000` | 健康、群列表、有限断档恢复 |
| OneBot WS | 仅 Compose `napcat:3001` | 生产消息事件 |

不存在 QCE 服务或 `40653`。Docker 日志限制为 10 MiB × 3。

## First start

```bash
cp .env.example .env
chmod 600 .env
./manage.sh prepare
./manage.sh start
./manage.sh login
```

扫码登录后，NapCat 可能生成账号专属的 `onebot11_<QQ>.json`。运行：

```bash
./manage.sh activate-account
./manage.sh status
```

该命令把专用账号配置同步成 HTTP + 正向 WS `debug raw`，然后只重启 NapCat。
控制台容器启动时会等待 WebUI、登录信息和 WS 端口健康；全部就绪后才启动唯一
Worker。

## Production gate

正式监听前，把一张已知 SHA-256 的 NAI 或 ComfyUI 原图放到宿主机持久目录的
`diagnostics/`，再从另一个 QQ 账号勾选“原图”发到只含测试账号的群。随后运行：

```bash
./manage.sh diagnose-original known.png 100000001
```

这个明确隔离的工具会监听下一张测试图片，比较源文件、`data.url`、raw URL 和
一次性 `get_image` 的 SHA-256。只有 `production_gate=pass`，即 `data.url` 与源
文件完全一致，才可在控制台解除暂停并启用生产群。工具不输出 rkey/Token，生产
Worker 本身仍硬拒绝 `get_image`。

同一文件再不勾“原图”发送，用来确认 QQ 重编码确实破坏元数据；数小时后可观察
旧 URL 的 403 行为。首次灰度只启用一个群，运行两周后再逐群增加。

## Routine commands

```bash
./manage.sh status
./manage.sh logs
./manage.sh console-url
./manage.sh probe-event 100000001
./manage.sh purge-cache
./manage.sh restart
```

`probe-event` 只调用白名单接口并等待一条 WS 事件，不下载图片。

## Upgrade and rollback

升级时先构建新采集镜像，不必复制图库。切换前停止旧 Worker、执行 SQLite WAL
checkpoint，并只复制一份：

```text
repository/state/collector_state.pre-event-v1.<timestamp>.sqlite3
```

数据库迁移会保留 `images`、`assets`、`monitored_groups`、最终路径和群配置，
新增队列/计数/断档状态，把旧失败标记为 `legacy_failed`，并删除旧历史游标表。
切换后确认：

- `40653` 未监听；
- 宿主机没有 `3000/3001`；
- 四类文件数与切换前一致；
- 控制台、NapCat、WS、Worker、清理容器均为 running/healthy；
- 未登录时 Worker 保持停止，图库不被修改。

如果迁移后的服务不能启动，停止新容器、恢复唯一数据库副本并使用上一个 Git
提交重新构建。图库从未被迁移，因此不需要回滚图片。回滚副本和旧 QCE 数据由
清理服务在 7 天后删除。

## Cache safety

`cache-cleaner` 的删除白名单固定在 `cache_cleanup.py`。它每 6 小时删除：

- 超过 2 小时的 Pic、Emoji、`nt_temp`、采集 `.part`；
- 超过 24 小时的 Video、File、Ptt；
- 超过 48 小时的 QQ/NapCat 日志；
- 超过 7 天的旧 QCE 数据与事件版数据库回滚副本。

`nt_db`、登录会话、NapCat 配置、OneBot Token 和最终图库不在遍历范围内。
