# QQ AI 原图采集器：Linux Docker 适配

这是 Windows 生产项目的独立 Linux 副本。它不会读取或修改
`D:\qq-image-collector`，也不会自动启动服务器上的 QQ。

## 独立数据盘与全新监听账号

宿主机运行数据目录由 `.env` 中的 `QQAI_RUNTIME_ROOT` 控制，例如：

```dotenv
QQAI_RUNTIME_ROOT=/mnt/disk-1/qq-ai-image-collector
```

Compose 会把 QQ 会话、NapCat/QCE 配置、SQLite、日志和最终图片全部放在该
目录。`bootstrap.py` 与 `manage.sh purge-cache` 使用同一设置，不会误写
项目目录下的旧 `runtime/`。

全新部署使用全新数据库，不导入 Windows 的旧图片、消息记录或游标。首次
启动先补抓最近一小时，并启用标准 QCE 历史回填，适合刚加入目标群的新监听
账号；回填追到该账号可见历史的末端后，自动转为日常增量监听。官方镜像不
提供 Windows 旧环境中的私有深层接口，因此
`deep_backfill_enabled=false` 保持关闭。

## 架构

- `napcat-qce`：官方 `ghcr.io/shuakami/napcat-qce` 一体化镜像；服务器
  配置固定到已验证的镜像摘要，避免无人值守时自动换版本。
- `collector-console`：原有采集核心、SQLite、FastAPI 和 React 控制台。
- 两个容器共享网络命名空间和 QQ 缓存目录，因此 OneBot `get_image`
  返回的 `/app/.config/QQ/...` 路径可直接读取，不需要跨机器传图片。
- NapCat WebUI、QCE 和采集控制台默认只绑定宿主机 `127.0.0.1`。
- 公网控制台使用显式启用的独立端口与管理 Token，不包含 Cloudflare 组件。

新增图片按 `NovelAI`、`ComfyUI`、`NAI含参但不可直接读取的`、
`其他模型生成` 四类保存。Linux 默认配置
`storage.migrate_existing_accepted_on_start=false`，因此普通升级和重启不会搬动
已有图片；需要主动整理历史目录时再显式运行来源信息迁移命令。

## 服务器首次准备（不会启动 QQ）

```bash
cd /opt/qq-ai-image-collector-linux/linux
python3 bootstrap.py
docker compose config
docker compose build collector-console
docker compose pull napcat-qce
```

`bootstrap.py` 会创建独立的 `runtime/`，生成随机 OneBot Token，但不会
显示 Token，也不会覆盖已经存在的配置。不要在生产切换后使用
`--force`。

## 登录与本地访问

切换前必须先停止 Windows 上的采集 Worker 和 NapCat，避免同一个 QQ
同时运行两套 NapCat。然后在服务器启动：

```bash
./manage.sh start
./manage.sh login
```

`login` 会显示二维码。手机扫码成功后按 `Ctrl+C` 只会退出日志查看，
不会关闭容器。

Compose、Docker 和两个容器均可设置为开机自启，但腾讯仍可能使已保存的
“快速登录身份”失效。若日志提示身份失效，NapCat 会退回二维码登录，此时
无法在没有账号凭据的情况下做到完全无人值守。需要无人值守回退时，可在
权限为 `600` 的 `.env` 中只配置下列一个变量：

```dotenv
NAPCAT_QUICK_PASSWORD=QQ明文密码
# 或
NAPCAT_QUICK_PASSWORD_MD5=QQ密码的MD5
```

Compose 会把该值只传给 NapCat 容器；不要提交 `.env`、打印该值或将 NapCat
WebUI 暴露到公网。首次扫码或配置回退密码后，应实际重启宿主机并运行
`./manage.sh probe`，不能只依据容器的 `restart` 策略判断自动恢复成功。

从自己的电脑建立 SSH 隧道：

```powershell
ssh -L 16099:127.0.0.1:16099 `
    -L 40653:127.0.0.1:40653 `
    -L 17891:127.0.0.1:17890 `
    your-server
```

随后访问：

- NapCat WebUI：`http://127.0.0.1:16099/webui`
- QCE：`http://127.0.0.1:40653/qce`
- 采集控制台：先执行 `./manage.sh console-url` 获取使用本地 `17891`
  转发端口且带本地会话的 URL

如确需临时从公网处理 QQ 登录，可在轮换
`runtime/napcat-config/webui.json` 中的随机 Token 后显式配置：

```dotenv
NAPCAT_PUBLIC_BIND=0.0.0.0
NAPCAT_PUBLIC_WEBUI_PORT=10058
```

然后只在宿主机防火墙开放该 TCP 端口。原来的 `16099` 回环映射会保留作为
SSH 救援入口；OneBot 和 QCE 仍不得公开。NapCat 公网入口是普通 HTTP，
Token、二维码和管理操作没有传输层加密，只应在受信网络中临时使用，并在
登录完成后优先改回 `127.0.0.1` 或使用 VPN/SSH Tunnel。

生产服务器可使用下列命令原子更新 `.env`、轮换 256 位随机 Token、开放
UFW 并重建容器；命令只在终端显示一次新 Token：

```bash
./enable_public_napcat.sh 10058
```

## 可选：独立公网端口与 Token

默认不会公开控制台。只有同时完成以下三项才会允许直连：

1. `.env` 设置 `COLLECTOR_PUBLIC_BIND=0.0.0.0` 和独立公网端口。
2. `manager_config.json` 显式设置 `direct_public_enabled=true`、准确的
   `direct_public_hosts` 以及相同的 `direct_public_port`。
3. 宿主机防火墙只开放上述端口。

直连模式继续要求随机管理 Token、同源 Origin 与准确 Host，其他 Host、
端口和无 Token API 请求仍会被拒绝。它提供与本机控制台相同的管理能力，
不要与 NapCat WebUI、QCE 或 OneBot 共用公网端口。

此模式是普通 HTTP，Token 和页面内容没有传输层加密；不要在不可信网络中
使用。需要传输加密时应改用 VPN、SSH Tunnel 或受身份验证的 HTTPS 反向代理。

## QCE/OneBot 兼容性验证

登录完成后运行：

```bash
./manage.sh probe
./manage.sh probe 10000003
```

首次登录后，NapCat 会创建账号专属的 `onebot11_<QQ号>.json`。运行：

```bash
./manage.sh activate-account
```

脚本会自动识别唯一登录账号，把随机 Token 模板写入账号专属配置，并兼容
NapCat 4.18.13 的对象式超时配置。多账号目录必须显式传入
`./manage.sh activate-account <QQ号>`。如果 OneBot 在首次登录时尚未初始化，
应等待登录状态写盘后正常重启一次 `napcat-qce`，再重启
`collector-console` 以重新挂接共享网络命名空间。

第一条只检查登录信息、QCE `/health` 和凭据位置。第二条额外请求最近
五分钟内最多两条消息，用于验证新版 QCE 的 `/api/messages/fetch`
响应结构，不下载图片。

官方 QCE 不保证 Windows 环境中曾经添加的私有
`/api/messages/fetch-before` 扩展，因此 Linux 默认
`deep_backfill_enabled=false`，日常运行只进行新消息补漏。

## 从 Windows 部署迁移（可选）

只有从旧的 Windows 部署切换过来时才需要这一步。先在 Windows 侧导出快照：

```powershell
python linux\export_windows_snapshot.py `
  --config '<Windows 仓库>\config\collector_config.json' `
  --output '<快照输出目录>'
```

服务器如果没有同时容纳“导入快照”和“最终仓库”两份图片的空间，应把快照
内容直接传进持久化仓库（Compose 尚未启动）：

```bash
cd /opt/qq-ai-image-collector-linux/linux
# 从 Windows 传输 snapshot/final、snapshot/state 和 manifest 到 ./runtime/repository/

python3 migrate_windows_snapshot.py \
  --snapshot ./runtime/repository \
  --destination ./runtime/repository \
  --source-prefix '<Windows 仓库根目录>' \
  --replace-database
```

脚本会复制 `final/` 下全部分类图片、使用 SQLite Online Backup、改写 Windows
绝对路径、逐文件校验 SHA-256，并保留群配置、实时游标、历史游标、
发送人和消息来源。

这种“原地安装”不会再复制一份图片，数据库只产生一个短期临时副本。
正式切换时应先停止 Windows Worker 与 NapCat，再重复导出和增量传输最后一次
快照并重新运行上述命令。

## 缓存清理

清理器只遍历 QQ 会话中的 `nt_data/Pic`、`nt_data/Emoji` 和
`nt_temp`，不会进入 `nt_db`，也不会接触最终仓库：

```bash
python3 cache_cleanup.py
python3 cache_cleanup.py --apply
```

专用采集服务器默认只保留 1 天原图、缩略图、表情与临时媒体缓存。
每 6 小时清理一次，因此文件通常会在超过 24 小时后的 6 小时内删除。
最终四分类图片仓库、SQLite 和 QQ `nt_db` 不受影响：

清理器兼容 `账号/nt_qq` 与 Linux QQ 实际使用的 `nt_qq_<哈希>` 两种账号目录。

```cron
17 */6 * * * cd /opt/qq-ai-image-collector-linux/linux && ./manage.sh purge-cache --apply >> /mnt/disk-1/qq-ai-image-collector/repository/logs/purge_qq_cache.log 2>&1
```

## 风控与恢复策略

- 默认轮询为 90 秒并增加最多 20 秒随机抖动。
- 新消息每页最多 20 条，失败重试每周期最多 3 个。
- 新账号安装默认启用标准历史回填；到达账号可见历史末端后只保留增量监听。
- 不要让 Windows 与 Linux 同时登录同一 QQ。
- 若腾讯连续要求重新登录，应先 `docker compose stop napcat-qce`
  排查，不要反复扫码或无限手动重启。
- `docker compose down` 不会删除 `runtime/`；不要使用 `down -v` 或
  手工删除会话、仓库目录。

## 常用命令

```bash
./manage.sh status
./manage.sh logs
./manage.sh login
./manage.sh qce-token
./manage.sh console-url
./manage.sh stop
```
