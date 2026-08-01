# Event-driven collection contract

本文记录生产链路的不可变约束。流程、解析或风险模型更新时，必须同时更新本文、
测试和控制台字段。

## 风险模型

现有对照数据表明，账号存活差异与 `get_image/downloadRichMedia`、批量
`get_group_msg_history` 等账号会话动作相关，而不是 CDN 下载字节量或境外 IP。
因此安全边界是 OneBot 动作白名单、历史调用配额和可审计计数；15 秒下载间隔与
每日 3000 次请求上限仅防止本地队列失控，不能被描述为防封手段。

## 事件与图片身份

NapCat HTTP/WS 均为 `messagePostFormat=array`、`debug=true`，且只存在于 Compose
私有网络。实时 raw 提供 NT `msgId/msgSeq`、`picElement.original`、宽高、MD5、
文件名与 `originImageUrl`，标准段提供 `data.url`、`file`、`file_size`、summary
和 subtype。

- 标准图片与 raw `picElement` 先按文件名、再按 MD5 一对一匹配。
- 有标识但不匹配时禁止按数组位置回退；双方均无标识且只有一个候选时才允许
  `position-unverified`。
- 未匹配 raw `picElement` 仍以 `raw-only` 入队。即使没有可用 URL，也必须形成
  明确的 `expired` 告警，不能静默消失。
- live raw `msgId` 是持久实时锚点，raw `msgSeq`/历史 `real_seq` 用于跨实时与
  历史去重。NapCat 历史短 `message_id/message_seq` 仅作当次响应标识，不得覆盖
  live 锚点。
- 唯一记录仍为群、消息键和图片序号；同群同稳定 msgSeq/序号会合并到已有行。

事件必须先写 SQLite，随后才允许下载。`downloading` 在异常重启后恢复为
`queued`。首次没有 live 游标时只从当前事件开始，不执行历史回填。

## 队列、URL 与终态

```text
queued/deferred -> downloading
  -> accepted | rejected_no_metadata | filtered_gif
  -> expired | failed_terminal
```

`original` 只参与优先级；NULL 时使用大小、宽高、扩展名和表情信号降级判断。
rkey URL 记录 `url_expires_at` 为“保守 6 小时、尚未实测”的调度提示；只有进入
最后一小时才越过普通优先级。Test D 完成后才能把该提示改成真实 TTL。

候选 URL 按配置顺序逐个做 HTTPS/主机白名单验证。首选非法、403、404 或 410
时先尝试事件已有的第二个合法候选，不产生账号会话请求。5xx/408/425 最多退避
三次；429 熔断一小时。`expired` 与普通失败分开统计。终态保存两路 URL 指纹，
后续直接事件或已经发生的有限断档若带来不同 URL，可原地复活；同一 URL 不会
循环复活。

终态从 `resolver_json` 删除完整 URL/rkey，只保留主机、URL 指纹、HTTP 状态与
诊断字段。错误字符串不得包含 URL。

## 账号会话断路器

生产 OneBot 客户端只允许：

- `get_login_info`
- `get_group_list`
- `get_version_info`
- `get_group_msg_history`

`get_image` 在网络前硬拒绝，递增 `get_image_blocked`、设置
`collector_paused=true`、写 `critical_alarm` 并停止 Worker。403 默认不会刷新
历史 URL；若将来根据实测显式启用，每图也只允许一次且受每小时 6、每日 20 次
历史总配额约束。WS 断线超过 3 秒只进行有限断档恢复；历史结果不能更新 live
游标。任意日期回填、连续回填、启动轮询和 QCE 均不存在。

`linux/diagnostic_compare.py` 与生产 Worker 隔离：Test B 只有显式危险参数才调用
一次 `get_image`，并在请求前写入不可重复 sentinel；Test C 默认只比较源文件与
CDN 字节/元数据，不调用账号 API。

## 计数与灰度门禁

`hourly_counters` 分开记录：

- `events`、`image_segments`、`queued_high/medium/low`；
- `cdn_requests`（所有尝试）、`cdn_downloads`（完整 200）、`cdn_bytes`、403/429；
- `history_calls`、`get_image_blocked`；
- accepted、rejected、duplicates、failed、expired、filtered_gif。

Test A 直接独立统计标准段与 raw，不依赖生产解析器；Test D 的完整 URL 只暂存在
chmod 0600 的诊断文件，T+24h 后删除。Test E 只统计灰度开始后、
`resolver='event-cdn'` 的记录。`telemetry_report.py --hours 72` 只有在实际观察时间
达到 72 小时且 `history_calls=get_image_blocked=0` 时才返回通过。

## 镜像、rkey 与清理

NapCat 镜像固定摘要，Compose 将当前已知的两个第三方 rkey 域名
`ss.xingzhige.com`、`secret-service.bietiaop.com` 解析到 `127.0.0.1`。固定摘要
与源码提交的映射必须在上线后通过实际 `get_version_info`/构建信息确认；每次更新
镜像都要按对应源码重新审查。短时无外联只能表述为“该窗口未观察到”。

当前固定多架构摘要的 SLSA provenance 已确认来自官方
`NapNeko/NapCat-Docker@f0599fb2eef4e9007aed72501849e2ca3eeaccdf`，amd64
子清单为 `sha256:11d72e50b6edc01b20f1a7611a250720e61412fe96184e3c70c3b8cf976744e1`。
这只是 Docker 封装级映射；镜像内 NapCatQQ 版本/源码仍属于 Test F 的登录后门禁。

最终文件按 SHA-256 去重并原子移动。清理容器每 6 小时仅遍历白名单：Pic、Emoji、
`nt_temp`、`.part` 保留 2 小时；Video、File、Ptt 保留 24 小时；QQ/NapCat 日志
保留 48 小时。它不会遍历 `nt_db`、会话、配置或 `final`。旧 QCE 数据与唯一数据库
回滚副本保留 7 天。
