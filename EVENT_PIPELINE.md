# Event-driven collection contract

本文档记录生产链路的不可变约束。调整采集流程时，应同时更新本文、测试和控制台
字段，避免重新引入高频历史请求。

## Event ingestion

NapCat 同时启用一个 HTTP Server 与一个 WebSocket Server，均为
`messagePostFormat=array`、`debug=true`。两者只存在于 Compose 私有网络。
WS `debug raw` 提供 NT 消息的 `msgId`、`msgSeq`、`picElement.original`、宽高、
MD5 与 `originImageUrl`；标准图片段提供 `data.url`、`file`、`file_size`、
`summary` 与 `sub_type`。

Worker 不在内存中暂存等待下载的事件。唯一键
`(group_id, raw_message_id, image_index)` 首先写入 `images`，随后才允许下载。
终态任务不会因为重复事件或重启而重新排队。`downloading` 在异常重启后恢复为
`queued`。

首次没有群游标时只从当前事件开始。已有游标且 Worker/WS 确实中断超过 3 秒，
才从最后 raw `msgId` 向后进行有限恢复。任意日期回填、连续历史回填和启动轮询
均不存在。

## Queue states

```text
queued/deferred -> downloading
  -> accepted
  -> rejected_no_metadata
  -> filtered_gif
  -> expired
  -> failed_terminal
```

旧 OneBot/QCE 失败在迁移时一次性变为 `legacy_failed`，新版不会重试。终态会从
`resolver_json` 删除 `url`、`origin_url` 和 raw 对象，记录 `url_host`、HTTP 状态
和诊断结论。任何错误字符串不得包含 rkey URL。

## Tencent-interface circuit breakers

OneBot 客户端只有四个允许动作：

- `get_login_info`
- `get_group_list`
- `get_version_info`
- `get_group_msg_history`

其余动作在网络请求前失败。若动作名为 `get_image`，还会递增
`get_image_blocked`、设置 `collector_paused=true`、写入 `critical_alarm` 并停止
Worker。仓库中的 `linux/diagnostic_compare.py` 是隔离的一次性验收工具，不会被
生产包导入；只有明确传入危险确认参数时才会做一次字节对比。

403 的 URL 刷新标记与历史调用配额在同一个异步临界区提交，确保每张图片最多
调用一次。429 不调用历史。NapCat 镜像固定摘要，且 Compose 把当前源码中两个
已知第三方 rkey 服务域名解析到 `127.0.0.1`；更新镜像前必须重新审查相关源码。

## Counters and observability

`hourly_counters` 记录事件、图片段、三个优先级、CDN 请求/字节、403/429、
历史调用、被阻止的 `get_image`、接受、淘汰、重复、失败和 GIF。控制台 SSE
每 2 秒显示：

- WS 连接与最后事件；
- 队列深度、最老任务和优先级；
- 下载器、每日配额与熔断截止时间；
- 今日历史调用和 `get_image` 拦截；
- 每群最后消息、最后图片和断档恢复状态。

正常稳态的 `history_calls` 与 `get_image_blocked` 必须为 0。

## Storage and cleanup

最终图片仅由解析成功后的原子移动写入，并按 SHA-256 去重。清理容器每 6 小时
只遍历明确白名单：Pic、Emoji、`nt_temp` 与 `.part` 保留 2 小时；Video、File、
Ptt 保留 24 小时；QQ/NapCat 日志保留 48 小时。它不会遍历 `nt_db`、登录配置、
会话或 `final`。旧 QCE 数据与 `pre-event-v1` 数据库回滚副本保留 7 天。
