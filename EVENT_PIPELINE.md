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

候选 URL 按配置顺序逐个做 HTTPS/主机白名单验证。首选非法、400、403、404 或 410
时先尝试事件已有的第二个合法候选，不产生账号会话请求。5xx/408/425 最多退避
三次；429 熔断一小时。`expired` 与普通失败分开统计。终态保存两路 URL 指纹，
后续直接事件或已经发生的有限断档若带来不同 URL，可原地复活；同一 URL 不会
循环复活。

终态从 `resolver_json` 删除完整 URL/rkey，只保留主机、URL 指纹、HTTP 状态与
诊断字段。错误字符串不得包含 URL。

下载中的文件名和后缀不构成格式证据。CDN 临时文件可能以 `.bin` 或 `.part` 保存，
PNG 原始 `tEXt/iTXt/zTXt` 扫描必须由八字节 PNG 魔数
`89 50 4E 47 0D 0A 1A 0A`（并结合实际解码格式）驱动，禁止因为临时后缀不是 `.png`
而跳过。最终扩展名只能在识别真实字节格式后确定。

## 账号会话断路器

生产 OneBot 客户端只允许：

- `get_login_info`
- `get_group_list`
- `get_version_info`
- `get_group_msg_history`

`get_image` 在网络前硬拒绝，递增 `get_image_blocked`、设置
`collector_paused=true`、写 `critical_alarm` 并停止 Worker。400/403 默认不会刷新
历史 URL；只有显式启用、且 400 同时具备 event-cdn、原始消息序号与 rkey/到期证据
时才允许尝试。生产 live-only 标记存在时，常驻 Worker 在预算检查的最前面硬拒绝
所有普通 history 调用；手动 `recover-gap` 与任意 `backfill` API 都返回 410。即使误把
小时/每日额度改成非零，WS 重连和过期 URL 也不能发出请求。历史结果不能更新 live
游标。任意日期回填、连续回填、启动轮询和 QCE 运行时均不存在。

`linux/diagnostic_compare.py` 与生产 Worker 隔离：Test B 只有显式危险参数才调用
一次 `get_image`，并在请求前写入不可重复 sentinel；Test C 默认只比较源文件与
CDN 字节/元数据，不调用账号 API。

## 计数与灰度门禁

`hourly_counters` 分开记录：

- `events`、`image_segments`、`queued_high/medium/low`；
- `cdn_requests`（所有尝试）、`cdn_downloads`（完整 200）、`cdn_bytes`、400/403/429；
- `history_calls`、`window_history_calls`、`get_image_blocked`；
- accepted、rejected、duplicates、failed、expired、filtered_gif。

Test A 直接独立统计标准段与 raw，不依赖生产解析器；Test D 的完整 URL 只暂存在
chmod 0600 的诊断文件，T+24h 后删除。Test D 对每条 URL、每个时间点顺序发送成对的
Range GET（`bytes=0-0`）与不带 Range 的普通 GET；两者都以 streaming 模式只取得响应
头后立即关闭，应用不得消费响应体。Test E 只统计灰度开始后、
`resolver='event-cdn'` 的记录。`telemetry_report.py --hours 72` 只有在实际观察时间
达到 72 小时且 `history_calls=get_image_blocked=0` 时才返回通过。

生命周期公开报告只允许响应状态和严格白名单化的长度、Content-Range、是否支持 bytes、
MIME、是否 chunked、ETag SHA-256 与脱敏 Location 形态；不得记录任意响应头、完整
Location、Cookie、文件名、认证信息或正文。schema 1 的历史 Range-only check 可读取并
标记为 legacy，但禁止伪造缺失的普通 GET 结果。TTL 与生产可用性主要依据普通 GET，
Range 只作为辅助证据。

## 一次性严格时间窗口恢复

已经明确审计的生产断档可以使用内部 `window-recovery` 容器；它不是常驻 Worker 的
回填功能，也没有公网创建接口。上下界必须与 SQLite 的
`production_history_floor`、`production_live_only_started_at` 完全一致，否则失败关闭。

每群起点是窗口下界之前最后一条旧 QCE 图片记录中的 19 位 raw NT `msgId`，不是
raw `msgSeq`。固定 NapCat 的 `get_group_msg_history.message_seq` 参数实际接受 short
message ID 或 raw `msgId`；返回的稳定持久身份是 `real_seq`。恢复器使用
`reverse_order=false` 从旧 raw msgId 向当前时间移动，每页 20 条且包含锚点：

1. 第一页先完整验证 `real_seq`、时间、群边界、包含锚点和前进方向；验证完成前不入队，
   通过后才处理同一响应，禁止为“探测”重复请求一遍；
2. 后续锚点取本页最大 `real_seq` 对应的 short `message_id`；入队去重仍使用
   `(group_id, real_seq, image_index)`；
3. 只在 `not_before <= time <= not_after` 时才调用共享事件解析和入队，并在解析后再次
   校验图片时间；页外消息从不下载；
4. 历史中的 `message_sent` 强制规范化为 `message`，避免漏掉登录账号自己发送的图；
5. `real_seq` 前进且时间随序号不倒退时，首个 `max(page_time)>not_after` 的跨界页处理
   完成后即可结束；短页、空页、锚点不进、方向异常或字段缺失均不能伪装完成；
6. 每 10 分钟至多一页、每小时 6 次、每天 20 次、每群最多 200 次；全局图片队列未
   清空时不取下一页，避免历史 URL 等待过久失效；
7. NapCat short ID 映射失效时回到不可变 raw 起点重扫，依赖 `real_seq` 去重；绝不把
   raw `msgSeq` 冒充 API 锚点；
8. 普通 history 预算始终为 0，恢复调用另计 `window_history_calls`；完成后停止并移除
   profile 容器，实时 WS 采集全程继续。

Test A 与主动发图诊断分开，以降低用户负担。它必须在 Worker 暂停、所有生产群停用
时，对一个明确指定的目标群进行只读 WS 被动累计，直到取得不少于 200 个图片段；必须
启用 group 过滤，只输出脱敏聚合，不调用 OneBot HTTP、不下载图片、不写生产数据库。
账号加入的群较多，因此绝对禁止不带 group 过滤的全群普查。

正式 Test A 使用独立的、权限为 0600 的 `url_probe.sqlite3` 持久状态，不得复用或
连接生产 SQLite。目标 scope 和事件身份只保存 SHA-256，正文、群号、账号、消息 ID、
文件名、完整 URL 和查询参数均不落盘；可持久化内容仅限去重指纹与 host/rkey/original
等脱敏聚合。每处理一个事件都必须在独立数据库事务提交后原子刷新 JSON checkpoint，
事件 SHA-256 去重保证重连重放不重复计数。只有显式 `--reset` 才允许开始一轮全新
统计，普通进程或服务器重启必须续接原状态。

WS 断线按 1–60 秒指数退避重连。单次 6 小时等待结束而样本不足时状态为 partial，
保留 SQLite 和 JSON 并可继续运行，不能清零或伪装完成。服务器正式普查由持久
systemd 服务绑定一个明确目标群被动累计 200；达到门槛后正常退出并保持 completed，
systemd 不得再次重启该任务。

Test B、C、D、F 可以同时连接同一个隔离测试群并共享一次 10 张图片的发送窗口：前三
张分别为勾选“原图”的 NovelAI tEXt、ComfyUI workflow、NAI Alpha stealth 已知源，
另 7 张用于补足 Test D/F 的 URL 与网络样本，其中建议一部分不勾选“原图”。Test B/C
按 MD5 独立匹配，Test D 捕获 10 条 URL，Test F 同步观察网络。并行不合并原始输出或
降低门禁；Test B 的 `get_image` 仍然只能执行一次。Test E 另行发送三张已知源的原图
与非原图各一轮，共 6 个图片实例。

## 当前上线门禁记录（2026-08-02）

本轮隔离诊断已经确认：标准 `data.url` 下载字节与已知源文件 SHA-256 一致；唯一一次
`get_image` 诊断也与源文件一致，其 sentinel 已消费，禁止删除或重跑；raw
`originImageUrl` 被 CDN 白名单拒绝或不可用，因此当前唯一可用生产候选是 `data`。
ComfyUI 工作流样本的字节和元数据对比通过。

NAI 样本的 Test C 失败已经确认为诊断实现假失败：下载字节保存在 `.bin/.part` 临时
文件时，PNG 原始文本扫描错误地按文件后缀跳过，不是 NAI 元数据在 CDN 中丢失。生产
必须继续暂停；完成 PNG 魔数驱动修复后，只重跑 Test C 的三张已知源做完整回归，不
重跑 Test B，也绝不再次调用 `get_image`。

旧 Test D 的 T+0 原始记录为 10 条 Range 请求全部返回 HTTP 206；这只证明当时的
Range 请求可用。随后已确认旧 10 条 URL 在 T+1 时 Range GET 与普通 GET 均返回 400。
由于旧 T+0 没有普通 GET 数据，该轮只能作为 legacy 调查证据，不能满足严格 Test D。
schema 2 上线后必须重新捕获 10 条 URL，从新的成对 T+0 开始完整执行
T+1h/T+6h/T+24h。记录中不保存或公开账号、群、消息、文件名、完整 URL 或具体哈希值。

## 镜像、rkey 与清理

NapCat 镜像固定摘要，Compose 将当前已知的两个第三方 rkey 域名
`ss.xingzhige.com`、`secret-service.bietiaop.com` 解析到 `127.0.0.1`。固定摘要
与源码提交的映射必须在上线后通过实际 `get_version_info`/构建信息确认；每次更新
镜像都要按对应源码重新审查。短时无外联只能表述为“该窗口未观察到”。

当前固定多架构摘要的 SLSA provenance 已确认来自官方
`NapNeko/NapCat-Docker@f0599fb2eef4e9007aed72501849e2ca3eeaccdf`，amd64
子清单为 `sha256:11d72e50b6edc01b20f1a7611a250720e61412fe96184e3c70c3b8cf976744e1`。
镜像内 `NapCat.Shell.zip` 的 SHA-256 为
`85bb5b889caa61a5e671bf1b07ddb27d8b0a69f5a68016a3480aeff2ae220d03`，与官方
NapCatQQ `v4.18.13` 发布资产一致；该标签源码提交为
`216d0c7c6b60474298394044b9114074b8f131cf`。登录后仍须用 `get_version_info` 确认
实际运行构建没有偏离持久卷中的内容。

该精确源码还确认：debug 模式是在 OneBot 消息构造完成后附加完整 raw；标准图片段
生成 `data.url` 时会调用 `FileApi.getImageUrl()`。若 raw URL 为 NTV2，NapCat 会先
尝试带缓存的原生 `FetchRkey`，失败后才尝试两个第三方 rkey 服务，再使用 fallback。
因此“收到事件完全不产生任何账号动作”不是可假定的事实。生产仍严禁逐图
`get_image` 和连续历史调用；NapCat 内部 rkey 刷新行为必须由 Test F 的源码、日志及
网络窗口共同验收。两个第三方服务必须始终保持阻断，禁止为了制造“阻断前后对照”而
临时解除；Test F 只接受固定阻断状态下的源码、域名解析、日志、socket 与 DNS/SYN
观察证据，Test A 也只运行这一种网络状态。

最终文件按 SHA-256 去重并原子移动。清理容器每 6 小时仅遍历白名单：Pic、Emoji、
`nt_temp`、`.part` 保留 2 小时；Video、File、Ptt 保留 24 小时；QQ/NapCat 日志
保留 48 小时。它不会遍历 `nt_db`、会话、配置或 `final`。旧 QCE 数据与唯一数据库
回滚副本保留 7 天。
