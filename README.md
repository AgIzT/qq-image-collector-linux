# QQ Image Collector for Linux

QQ 群 AI 原图采集器。生产链路接收 NapCat 标准 OneBot 正向 WebSocket 事件，
先把每个图片任务写入 SQLite，再由单并发 Worker 直连 QQ CDN。它不轮询历史、
不做任意日期回填、不使用 QCE，生产代码也不能调用 `get_image`；WS 断线、
服务器重启和 CDN URL 失效时，会按持久游标自动恢复必要的消息或 URL。

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
- 下载并发恒为 1，生产图片间隔为 0；完成一张后立即领取下一张，没有人为等待。
  没有每日数量、小时次数或队列总量上限；请求尝试与完整下载分别统计。
- 只允许 `gchat.qpic.cn`、`multimedia.nt.qq.com.cn` 与 QQ 表情资源主机
  `gxh.vip.qq.com` 的 HTTPS 地址，单文件最大
  128 MiB。首选 URL 无效时先尝试事件自带的第二条合法 CDN URL；400/403/404/410
  会用对应消息游标刷新 URL，429 只延后当前图片，网络错误和 5xx 持续退避重试，
  不会因为次数达到阈值而永久放弃。
- 带 rkey URL 使用生产实测后的保守 30 分钟调度窗口，临期项可越过普通优先级；
  该窗口不是腾讯声明的精确 TTL。`expired` 独立告警；后续事件带来新 URL 时可
  原地复活，无需会话 API。
- GIF 以文件魔数最终确认，并在读到头部后立即中止；普通评论、作者名、链接、
  水印和无结构参数会被淘汰。
- 已下架的 QQ parcel 表情只有在事件表情信号、固定腾讯主机和
  `/club/item/parcel/item/.../raw300.gif` 路径同时命中时直接淘汰，不进行永久
  302/404 重试；其他带 GIF 后缀的资源仍必须读取魔数。
- WS 中断超过 3 秒后，常驻 Worker 从每群最后一个持久 raw `msgId` 向前补到
  重连时刻；没有历史页数或调用次数上限，但不会向该游标之前倒扫。
- 过期 CDN URL 可反复刷新；若历史接口仍返回同一个失效 URL，任务按指数退避
  留在持久队列，不形成紧密循环，也不转成静默终止。
- 内部一次性时间窗口恢复器只处理固定 `not_before/not_after` 范围，不开放任意
  历史日期；它同样没有小时、每日、每群页数或“队列必须清空”的停止条件。
- 生产 `get_image` 在发包前被拦截并记录严重告警，但不会因此暂停其他事件采集。
- 终态删除 URL/rkey，只保留主机、URL 指纹和诊断结论。Compose 服务使用
  `restart: unless-stopped`，SQLite、队列、群游标和图库跨重启保留。Worker、WS
  和下载循环均有独立心跳；任一后台协程异常退出都会触发进程内重建，避免“PID
  仍在但采集已停”的假活。

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

控制台会分别显示 Worker、WS 连接心跳、下载循环、队列和自动断档恢复。完整命令、
诊断工具、切换与缓存规则见 [linux/README.md](linux/README.md)，当前生产不变量见
[EVENT_PIPELINE.md](EVENT_PIPELINE.md)。

## 测试

仓库测试只使用合成群号、QQ 号、Token 和文件名：

```bash
python -m unittest discover -v
cd frontend && npm ci --no-audit --no-fund && npm run build
cd ../linux && docker compose config -q
```

项目使用 MIT License。
