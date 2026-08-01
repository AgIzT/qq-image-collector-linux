# QQ Image Collector for Linux

面向低实时性、低接口频率场景的 QQ 群 AI 原图采集器。生产链路只接收 NapCat
标准 OneBot 正向 WebSocket 事件，先把图片任务写入 SQLite，再由单并发 Worker
低频直连 QQ CDN。它不轮询群历史、不自动回填、不使用 QCE，生产代码也不能调用
`get_image`。

```text
NapCat OneBot WS（debug raw）
  -> SQLite 持久图片队列
  -> 单并发、15±3 秒 QQ CDN 下载
  -> 严格元数据解析 + SHA-256 去重
  -> NovelAI / ComfyUI / NAI含参但不可直接读取的 / 其他模型生成
```

## 主要行为

- 每条群消息事件都持久化 raw 消息 ID 和时间；图片段额外保存 URL、原图标志、
  尺寸、MD5、群号、发送者 QQ 和消息来源。
- `original=true`、未知、非原图/小图/疑似表情只影响优先级，不直接误杀。
- 下载并发恒为 1，默认间隔 15 秒；最老队列超过 30 分钟后降到 5 秒，回落到
  15 分钟以内才恢复正常速率。每日最多 600 次 CDN 请求。
- 只允许 `gchat.qpic.cn` 与 `multimedia.nt.qq.com.cn` HTTPS 地址，单文件最大
  128 MiB；URL 在终态会删除查询参数，只留下 CDN 主机。
- GIF 通过文件魔数确认，并在读取到头部后立即终止下载；普通评论、作者名、
  链接、水印和无结构参数会被淘汰。
- WebSocket 真断档才允许调用 `get_group_msg_history`：每页 20 条、每群最多
  5 页、每小时最多 6 次、每天最多 20 次。
- 每张图片遇到 403 最多刷新一次 URL；10 分钟 3 次 403 或一次 429 会熔断
  1 小时。生产 `get_image` 调用会在发包前被拦截、记严重告警并停止 Worker。
- SQLite、队列、群游标和最终图库跨容器/服务器重启保留；Compose 服务均使用
  `restart: unless-stopped`。

## 元数据与四分类

解析器覆盖 PNG `tEXt`、`iTXt`、`zTXt`、EXIF `UserComment` 与 NovelAI
Alpha `stealth_pngcomp`，详细规则见 [METADATA_CLASSIFICATION.md](METADATA_CLASSIFICATION.md)。
新图片直接进入四个平级目录：

- `NovelAI`
- `ComfyUI`
- `NAI含参但不可直接读取的`
- `其他模型生成`

现有图库在升级时不会重分类、重哈希或复制。

## 部署

```bash
cd linux
cp .env.example .env
chmod 600 .env
./manage.sh prepare
./manage.sh start
./manage.sh login
```

NapCat WebUI 使用公网 `10058`；控制台由宿主机 Nginx 的 `18080` 转发到
loopback `17890`。OneBot HTTP/WS 的 `3000/3001` 不映射到宿主机，旧 QCE
`40653` 不再存在。详细切换、测试门禁和缓存规则见 [linux/README.md](linux/README.md)。

## 测试

测试数据只使用合成群号、QQ 号、Token 和文件名：

```bash
python -m unittest discover -v
cd frontend && npm ci --no-audit --no-fund && npm run build
cd ../linux && docker compose config -q
```

架构状态机和安全不变量见 [EVENT_PIPELINE.md](EVENT_PIPELINE.md)。项目使用 MIT License。
