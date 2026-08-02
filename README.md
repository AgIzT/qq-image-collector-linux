# QQ Image Collector for Linux

面向“账号会话接口最小化”的 QQ 群 AI 原图采集器。生产链路只接收 NapCat
标准 OneBot 正向 WebSocket 事件，先把图片任务写入 SQLite，再由单并发 Worker
直连 QQ CDN。它不轮询群历史、不自动回填、不使用 QCE，生产代码也不能调用
`get_image`。

```text
NapCat OneBot WS（debug raw）
  -> SQLite 持久图片队列
  -> 单并发 QQ CDN 下载
  -> 严格元数据解析 + SHA-256 去重
  -> NovelAI / ComfyUI / NAI含参但不可直接读取的 / 其他模型生成
```

## 主要行为

- 每条群消息事件都持久化 live raw `msgId/msgSeq` 和时间；图片段保存 URL、原图
  标志、尺寸、MD5、群号、发送者 QQ 和消息来源。标准段与 raw `picElement` 按
  文件名/MD5 一对一匹配；转换失败留下的 raw-only 图片也会入队而不是静默丢失。
- `original=true`、未知、非原图/小图/疑似表情只影响优先级，不直接淘汰。若
  `original` 缺失，尺寸、扩展名与表情信号共同决定优先级。
- 下载并发恒为 1，默认间隔 15 秒；队列最老任务超过 30 分钟时降到 5 秒。
  每日 3000 次 CDN 请求上限只是本地防失控护栏，不是账号风控措施；请求尝试
  与完整下载分别统计。
- 只允许 `gchat.qpic.cn` 与 `multimedia.nt.qq.com.cn` HTTPS 地址，单文件最大
  128 MiB。首选 URL 无效或返回 403/404/410 时，先尝试事件自带的第二条合法
  CDN URL；5xx/408/425 最多退避三次。
- rkey URL 带一个明确标注“尚未实测”的 6 小时调度提示，临期项可越过普通
  优先级。`expired` 独立告警；后续事件带来新 URL 时可原地复活，无需会话 API。
- GIF 以文件魔数最终确认，并在读到头部后立即中止；普通评论、作者名、链接、
  水印和无结构参数会被淘汰。
- 常驻 Worker 的普通断档恢复和过期 URL 历史刷新均被生产策略硬禁用；即使误改
  小时/每日额度也不会调用 `get_group_msg_history`。
- 唯一例外是内部一次性时间窗口恢复器：从窗口前已保存的 raw NT `msgId` 向当前
  低频翻页，先探测方向，只把硬时间范围内的图片入队；它不开放公网 API、不覆盖
  live raw 游标，并使用独立的每小时 6、每天 20 次额度。
- 403 默认不调用历史刷新。生产 `get_image` 在发包前被拦截、记录严重告警并
  停止 Worker；账号会话 API 次数才是主要安全边界，CDN 字节量不是。
- 终态删除 URL/rkey，只保留主机、URL 指纹和诊断结论。Compose 服务使用
  `restart: unless-stopped`，SQLite、队列、群游标和图库跨重启保留。

## 元数据与四分类

解析器覆盖 PNG `tEXt`、`iTXt`、`zTXt`、EXIF `UserComment` 与 NovelAI
Alpha `stealth_pngcomp`。规则见 [METADATA_CLASSIFICATION.md](METADATA_CLASSIFICATION.md)。
新图片进入四个平级目录：

- `NovelAI`
- `ComfyUI`
- `NAI含参但不可直接读取的`
- `其他模型生成`

现有图库升级时不会重分类、重哈希或复制。

## 部署与验收

```bash
cd linux
cp .env.example .env
chmod 600 .env
./manage.sh prepare
./manage.sh start
./manage.sh login
```

NapCat WebUI 使用公网 `10058`；控制台由宿主机 Nginx 的 `18080` 转发到
loopback `17890`。OneBot HTTP/WS `3000/3001` 不映射到宿主机，旧 QCE
`40653` 不再存在。

在 URL 形态、字节、元数据、生命周期、`original` 标志与 rkey 外联完成 Test
A–F 前，Worker 必须保持暂停且六个生产群全部禁用。随后只启用一个群运行满
72 小时；`history_calls` 和 `get_image_blocked` 均为 0 才能逐群扩展。完整命令、
原始输出字段、切换与缓存规则见 [linux/README.md](linux/README.md)，架构不变量见
[EVENT_PIPELINE.md](EVENT_PIPELINE.md)。

## 测试

仓库测试只使用合成群号、QQ 号、Token 和文件名：

```bash
python -m unittest discover -v
cd frontend && npm ci --no-audit --no-fund && npm run build
cd ../linux && docker compose config -q
```

项目使用 MIT License。
