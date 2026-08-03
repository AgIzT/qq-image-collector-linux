# Linux 部署与事件链验收

本目录部署官方 NapCat、事件驱动采集器/控制台和缓存清理器。正式链路只消费
OneBot 正向 WS 事件中的 CDN URL；生产 Worker 不调用 `get_image`，不做连续历史
回填，也不依赖 QCE。

当前生产版本为 v1.1.6：每个图片事件先进入 SQLite，没有每日下载、历史小时/每日、
每群页数或队列总量额度；400/403/404/410 会刷新对应消息 URL，429 只延后当前图片，
网络错误持续退避。WS 断线或服务器重启后按每群 live raw 游标向前补到重连时刻，
不会倒扫游标之前的长期历史。Worker、WS 和下载循环都有独立心跳，任一后台任务退出
都会触发自动重建。

## 服务与端口

| Endpoint | 宿主机绑定 | 用途 |
| --- | --- | --- |
| NapCat WebUI | `0.0.0.0:10058` | 扫码与 NapCat 管理，使用独立强 Token |
| NapCat WebUI rescue | `127.0.0.1:16099` | SSH 本机救援 |
| Collector | `127.0.0.1:17890` | 由现有 Nginx `18080` 反代并验证 Token |
| OneBot HTTP | 仅 Compose `napcat:3000` | 健康、群列表、断档与 URL 恢复 |
| OneBot WS | 仅 Compose `napcat:3001` | 消息事件 |

不存在 QCE 服务或 `40653`，OneBot `3000/3001` 不映射到宿主机。Docker 日志限制
为 10 MiB × 3。

## 首次启动

```bash
cp .env.example .env
chmod 600 .env
./manage.sh prepare
./manage.sh start
./manage.sh login
```

扫码登录后，NapCat 可能生成账号专属的 `onebot11_<QQ>.json`。执行：

```bash
./manage.sh activate-account
./manage.sh status
```

`activate-account` 会把账号配置同步为 HTTP + 正向 WS、`messagePostFormat=array`、
`debug=true`，然后只重启 NapCat。控制台会等待 WebUI、登录信息和 WS 健康后再运行
唯一 Worker。

## 可选隔离诊断

重新验证 NapCat/CDN 行为时，可在隔离测试窗口临时满足：

- `collector_paused=true`；
- 原有六个生产群全部停用；
- 只使用自己的账号和隔离测试群；
- 不触发手动断档恢复。

`allow_403_history_refresh` 及历史/CDN 额度设置已从 v1.1.6 删除，旧配置值在 prepare
和控制台启动时清除，不能再用它们暂停生产队列。

测试图片放到宿主持久目录 `${QQAI_RUNTIME_ROOT}/diagnostics/`；容器内只读路径为
`/diagnostics/`。每项测试必须保留命令的完整原始 JSON/终端输出，不能只记录结论。
输出中不得补写账号、群号、文件名、完整 CDN URL、rkey 或 Prompt。

### 低负担分组验收

Test A 不要求用户集中发送 200 张图。它在 Worker 暂停、所有生产群停用时，对一个
明确指定的目标群进行只读 WS 被动累计；账号加入的群较多，命令必须显式提供 group，
绝对禁止无过滤监听全部群。该诊断只读取 WS，只输出脱敏聚合，不调用 OneBot HTTP、
不下载图片、不写生产 SQLite。

正式 Test A 由持久 systemd 服务运行，绑定一个明确目标群被动累计 200 个图片槽位。
服务使用独立 0600 `url_probe.sqlite3`，普通重启续跑；达到完成门槛后正常退出且不再
重启。诊断状态与生产数据库完全隔离。

Test B、C、D、F 使用另一个隔离测试群，并共享一次 10 张图片的发送窗口。先准备三张
已知 SHA-256 的源文件：NovelAI PNG `tEXt`、ComfyUI workflow、NAI Alpha stealth。
运维方同时启动一次性 `diagnose-original`、三项 `diagnose-metadata`、
`url-lifecycle-capture` 和 `audit-rkey-network`；用户随后发送 10 张图片：前三张是上述
三张已知源且都勾选“原图”，另 7 张用于补足 URL/网络样本，其中建议一部分不勾选
“原图”。可以批量发送，不要求塞进一条 QQ 消息。

Test B/C 按源文件 MD5 独立匹配，Test D 捕获前 10 条 URL，Test F 同步观察网络。
并行不合并原始输出，也不放宽任何通过条件；Test B 的隔离 `get_image` 仍然只允许
一次。Test E 需要临时启用测试群验证接受率，必须另行发送 6 个图片实例。

## Test A：URL 形态普查

保持生产 Worker 暂停、所有生产群停用。选择一个明确指定的目标群，运行下列带 group
过滤的命令并被动等待该群自然出现图片，直至独立采集至少 200 个估算图片槽位；用户
无需为了 Test A 集中发送 200 张。槽位数按标准段与 raw 图片候选的并集计算，即
`standard + raw - independently_matched_pairs`，不是取两边较大值，也不是把同一图片
重复计数：

```bash
./manage.sh probe-event <目标群号> 200
```

`<目标群号>` 不得省略；禁止运行无 group 的全群普查。脚本只读 WS，直接检查标准
图片段与 raw `picElement`，不复用生产事件解析结果，不调用 OneBot HTTP、不下载图片、
不写生产数据库。它只写权限为 0600 的独立 `url_probe.sqlite3` 和脱敏 JSON
checkpoint；目标 scope 与事件只保存 SHA-256，不能保存群号、账号、消息 ID、文件名、
正文、完整 URL 或查询参数。必须原样回报以下字段：

- `complete` 必须为 `true`，`captured_estimated_image_slots >= 200`；
- `standard_image_segments`、`raw_pic_elements`、`independently_matched_pairs`；
- `standard_without_raw_match`、`raw_without_standard_match`；
- `data_url_host_distribution`、`origin_url_host_distribution`；
- `data_url_rkey`、`origin_url_rkey`、`combined_rkey_ratio_percent`；
- `gchat_path_samples_redacted` 三条结构样例；
- `raw_original_flag_distribution`。

`timed_out=true`、样本不足或只有概述均不通过。Test A 决定生产应优先 `data.url`
还是 `originImageUrl`，不能预先假定 raw 与标准数组严格同序。

### Test A 可靠性与续跑

- 每个事件先以事件 SHA-256 去重，再在独立 SQLite 的单个事务中更新聚合；事务提交后
  原子刷新 JSON checkpoint。重连重放不能重复计数。
- WS 断线按 1–60 秒指数退避重连；进程、容器或服务器重启后从独立数据库继续。
- 单次最长 6 小时仍不足 200 时记录为 partial，保留状态；下次启动继续累计，不把
  partial 当作通过，也不清零。
- `--reset` 是唯一允许清空旧统计并开始新 scope 的操作，执行前必须确认目标群；普通
  启停严禁附带 `--reset`。
- 正式服务器任务由 systemd 保持运行；只在失败或未完成时恢复。达到 200 后状态为
  completed、进程正常退出，服务不得重启。
- 0600 SQLite/JSON 中也只能出现 SHA-256 scope、事件指纹和脱敏聚合；任何生产数据库
  表、真实标识或完整 URL 都不属于 Test A 状态。

## Test B：一次性字节一致性

将一张已知 SHA-256 的 PNG 放入 `diagnostics/`，从自己的另一个账号勾选“原图”发到
测试群。先启动诊断，再发送该文件：

```bash
./manage.sh diagnose-original known.png <测试群号> <发送者QQ>
```

诊断按源文件 MD5 匹配事件，不会把“下一张任意图片”误当样本。它比较源文件、
`data.url`、raw URL 和仅此一次的 `get_image`；`get_image` 请求前会写入不可重复
sentinel，生产 Worker 从未获得这项能力。

必须保留完整 JSON，并确认：

- `matched_event_md5` 与源文件一致；
- `source.sha256`、选中 CDN 候选的 `sha256`、
  `get_image_one_time_diagnostic.sha256` 三者完全一致；
- `raw_debug_present=true`，并记录 `raw_picture_match`；
- `production_gate=pass`。

任何哈希不一致都保持暂停。不要删除 sentinel 后重复 Test B，也不要用生产账号反复
试验 `get_image`。

## Test C：三类元数据存活

分别准备 NovelAI PNG `tEXt`、ComfyUI workflow、NAI Alpha stealth 三张已知源文件。
每个命令启动后，再发送与文件 MD5 对应的原图：

```bash
./manage.sh diagnose-metadata novelai-text.png <测试群号> <发送者QQ>
./manage.sh diagnose-metadata comfyui-workflow.png <测试群号> <发送者QQ>
./manage.sh diagnose-metadata nai-alpha.png <测试群号> <发送者QQ>
```

该模式只比较源文件和两路事件 CDN，不调用 `get_image`。三份原始 JSON 都必须显示：

- 生产候选 `matches_source=true`；
- 生产候选 `metadata_matches_source=true`；
- 源文件与候选的 `metadata.source`、`metadata.field_names` 和
  `metadata.fields_sha256` 一致；
- `production_gate=pass`。

任一类型失败都不能进入灰度。

临时下载路径可能以 `.bin` 或 `.part` 结尾。Test C 和生产解析不得用该后缀判断图片
类型；PNG 原始文本扫描必须先检查八字节 PNG 魔数
`89 50 4E 47 0D 0A 1A 0A`，并结合实际解码格式。只有真实字节格式确定后才能选择最终
扩展名。

## Test D：URL 生命周期

先从测试群事件采集 10 条 URL。完整 URL 仅写入权限为 0600 的 secret 文件，终端只
显示哈希 ID、类型、主机和是否带 rkey：

```bash
./manage.sh url-lifecycle-capture <测试群号>
```

该命令会同时完成新的 schema 2 T+0。每条 URL、每个时间点都顺序执行两次 GET：

- `range_get`：带 `Range: bytes=0-0`；
- `plain_get`：不带 Range，与生产下载的请求形态一致。

两种请求都用 streaming 模式，只等待响应头并立即关闭；代码不调用 body read/iterator，
不会主动下载完整响应体。操作系统缓冲可能收到少量已经在途的字节，因此不能声称线上
绝对传输 0 body。之后必须在实际时间点运行：

```bash
./manage.sh url-lifecycle-check T+1h
./manage.sh url-lifecycle-check T+6h
./manage.sh url-lifecycle-check T+24h --finalize
```

完整状态矩阵保存在 `repository/state/url_lifecycle.report.json`。每个结果分别包含
`range_get` 与 `plain_get`，以及两次请求的时间；公开字段只允许 HTTP 状态和白名单化的
长度、Content-Range、bytes 支持、MIME、chunked、ETag 哈希与脱敏 Location 形态。
完整 Location、Cookie、Content-Disposition、认证头、未知响应头和正文均不得进入报告。

脚本可以读取旧 schema 1 报告，但旧 check 必须标为 `legacy=true`、
`plain_get_recorded=false`，不得补造普通 GET。最后一步必须输出
`secret_urls_deleted=true`。不得提前连续执行四次伪造生命周期。10 条 URL 的完整四时点
矩阵共 80 次直连 CDN GET，不调用 OneBot HTTP 或账号会话接口。

## 当前门禁进度（2026-08-02）

本轮隔离测试的脱敏结论如下；不得在文档或提交中补入账号、群、消息、真实文件名、
完整 URL、rkey 或具体哈希值：

- `data.url` 下载字节与已知源 SHA-256 一致；
- 唯一一次 `get_image` 诊断也与源 SHA-256 一致，sentinel 已消费，不得删除或重跑；
- raw `originImageUrl` 被白名单拒绝或不可用，当前生产候选只能使用 `data`；
- ComfyUI workflow 样本的字节和元数据比较通过；
- NAI 的 Test C 结果是假失败：临时 `.bin/.part` 后缀使 PNG 原始文本扫描被跳过，
  不是 CDN 破坏了 NAI 元数据；
- 旧 Test D 的 T+0 为 10 条 Range 探测全部 HTTP 206；旧 10 条 URL 在 T+1 时，Range
  GET 与普通 GET 均为 HTTP 400。旧 T+0 缺少普通 GET，只能标为 legacy。

生产保持暂停，六个生产群保持停用。修复为 PNG 魔数驱动后，只重跑 Test C 的三张
已知源文件，连同已经通过的 ComfyUI 再做一次回归；不重跑 Test B，不再次调用
`get_image`。严格 Test D 必须用 schema 2 重新捕获 10 条 URL，从新的成对 T+0 开始，
再按实际 T+1h/T+6h/T+24h 时间点完成。

## URL preference 与 TTL 决策

只有 Test A–D 的原始结果齐全后才作决定：

| 实测结果 | 决策 |
| --- | --- |
| 仅 `data.url` 字节一致 | `url_preference=data` |
| 仅 raw `originImageUrl` 字节一致 | `url_preference=raw` |
| 两路均一致 | 优先无 rkey 且为 `gchat.qpic.cn` 的一路；否则按 Test D 生命周期更长者 |
| 两路均不一致 | 保持暂停，重新设计链路，不启用生产群 |
| 无 rkey 的 gchat URL 的 `plain_get` 到 T+24h 各时点均为 200 | 视为当前样本至少存活 24h；Range 结果只作辅助 |
| rkey URL 的 `plain_get` 到 T+24h 各时点均为 200 | 只能证明 TTL 下限为 24h；保留保守调度并继续观察 |
| Range 仍为 206、但同时间点 `plain_get` 失败 | 对生产普通 GET 不可用，不能通过门禁 |
| T+6h 前出现失效 | 当前 6h 提示不安全；先按最早失效时间减安全余量改实现，再灰度 |
| T+1h 或 T+0 已失效 | 不进入灰度 |

结论确定后可在控制台“CDN 首选通道”中选择 `data` 或 `raw`；值写入 SQLite，
Worker 下一循环生效并跨重启保留。

`cdn_requests` 统计所有尝试，`cdn_downloads` 只统计完整成功的 200。生产没有每日
请求额度；下载仍保持单并发与默认 15 秒节奏。

## Test E：original 分布与接受率

Test A 的 `raw_original_flag_distribution` 是第一份原始证据。要验证真实字节的接受率，
仅临时启用隔离测试群并解除暂停：把 Test B/C 的三张已知源各勾选“原图”发送一次，
再把同三张各不勾选“原图”发送一次，共 6 个图片实例。NULL 不能由用户主动制造，只
观察 NapCat 是否返回。采集完成后立即重新暂停并停用测试群，等待至少一小时后输出：

```bash
./manage.sh telemetry 1
```

必须原样保存 `original_flag_distribution` 与 `original_flag_by_status`。脚本只统计
`rollout_started_at` 之后、`resolver='event-cdn'` 的记录，旧库数据不得混入。如果
NULL 占多数，优先级必须主要依据宽高、大小、扩展名和表情信号；即使
`original=false`，也只降低优先级，不能在下载前淘汰。

## Test F：第三方 rkey 与网络观察

Compose 当前将以下两个已知第三方服务解析到 `127.0.0.1`：

```text
ss.xingzhige.com
secret-service.bietiaop.com
```

在 Test A 正在接收图片、预计发生原生 rkey 刷新的窗口运行：

```bash
./manage.sh audit-rkey-network 300
```

必须保留域名解析、NapCat 网络命名空间当前 TCP socket、以及可用时 300 秒 DNS/SYN
观察的完整输出。短窗口未见外联只能写成“该窗口未观察到”，不能证明永远不会连接。
两个第三方域名必须在整个测试和生产周期始终保持阻断，禁止为了对照临时解除。Test F
只要求固定阻断状态下的源码审查、域名解析、NapCat 日志、socket 与 DNS/SYN 观察证据；
Test A 也只在这一种网络状态下运行，不再要求或允许“屏蔽前后”两轮。还必须用
`get_version_info`/构建信息确认固定镜像摘要对应的 NapCat 版本和源码提交；映射不明
则门禁失败。

当前已核验到的镜像级 provenance 为：多架构摘要
`sha256:e66a6e52dc5dd63a2b8537651bafad50255d021584c113a8bbb2cc0ff94bd772`，
amd64 子清单为
`sha256:11d72e50b6edc01b20f1a7611a250720e61412fe96184e3c70c3b8cf976744e1`，
SLSA 构建上下文指向官方 `NapNeko/NapCat-Docker` 提交
`f0599fb2eef4e9007aed72501849e2ca3eeaccdf`（2026-07-19）。这只能证明 Docker
封装来源。镜像内 `NapCat.Shell.zip` 的 SHA-256
`85bb5b889caa61a5e671bf1b07ddb27d8b0a69f5a68016a3480aeff2ae220d03` 已与官方
NapCatQQ `v4.18.13` 发布资产比对一致，对应标签源码提交为
`216d0c7c6b60474298394044b9114074b8f131cf`。登录后仍须用 `get_version_info`
确认实际运行构建。

精确源码显示，debug raw 是在标准 OneBot 消息构造完成后附加的；标准图片段生成
`data.url` 时仍会调用 NapCat `FileApi.getImageUrl()`。NTV2 图片会先尝试缓存的原生
`FetchRkey`，失败后尝试两个第三方 rkey 服务，再回退。因此 Test F 不只是验证域名
阻断，也要观察原生 rkey 刷新；不能把“使用事件 URL”简化成“NapCat 内部绝无账号
动作”。这条不改变生产禁令：Worker 仍不得逐图调用 `get_image`。`history_calls` 在
WS 断档或 CDN URL 失效时可以非零，必须结合原因与队列状态判断，不能再把非零本身
当作故障。

## Test G：单群 72 小时稳态门禁

A–F 全部通过后，只启用一个灰度群，记录 `rollout_started_at`，连续运行至少 72 小时：

```bash
./manage.sh telemetry 72
```

必须保留完整 JSON，包括：

```text
events / image_segments / queued_high / queued_medium / queued_low
cdn_requests / cdn_downloads / cdn_bytes / cdn_400 / cdn_403 / cdn_429
history_calls / get_image_blocked
accepted / rejected / duplicates / failed / expired / filtered_gif
```

持续观察项为：

- `duration_requirement_met=true`；
- `get_image_blocked=0`。

`history_calls` 应能对应到明确的 WS 断档或 URL 刷新；无原因持续增长时再定位路径。

## 明确禁止的回退方案

- 不增加国内中继、双机下载或 CDN 代理；现有证据不支持“境外 IP/字节量导致风控”。
- 不恢复 QCE、启动轮询、任意日期回填或连续历史回填。
- 不允许生产代码调用 `get_image/downloadRichMedia`。
- 不放宽 OneBot 动作白名单；历史接口只用于游标后的断档与单张 URL 刷新。

## 一次性时间窗口补漏

普通任意日期 `backfill` 永久返回 410；`recover-gap` 按 live 游标向前恢复并返回 202。
若已有明确的旧断档需要固定时间窗恢复，可在 `.env` 写入闭区间 Unix 时间：

```text
WINDOW_RECOVERY_NOT_BEFORE=<审计后的下界>
WINDOW_RECOVERY_NOT_AFTER=<生产切换时间>
```

这两个值必须与数据库生产标记完全一致。启动、查看和停止内部 profile：

```bash
./manage.sh window-recovery-start
./manage.sh window-recovery-status
./manage.sh window-recovery-stop
```

恢复器优先从切换点之后的当前 WS raw `msgId` 向旧消息翻页；安静群没有当前锚点时只用
最新一页建立起点。旧 QCE `msgId` 不会复用。每群首屏先完整验证后退方向，验证前不
入队，通过后才处理同一响应，不重复拉取探测页。随后默认每 2 秒一页，没有小时、
每日、每群页数或队列清空限制。只有硬时间窗内图片会进入原有事件 CDN 队列。它不更新
实时游标、不调用 `get_image`。脱敏汇总写入
`repository/state/diagnostics/window-recovery-report.json`，完整群号与锚点只留在 SQLite。

## 日常命令

```bash
./manage.sh status
./manage.sh logs
./manage.sh console-url
./manage.sh purge-cache
./manage.sh restart
```

## 升级与回滚

升级时先构建新镜像，不复制图库。切换前停止旧 Worker、执行 SQLite WAL checkpoint，
只保存一份数据库回滚副本：

```text
repository/state/collector_state.pre-event-v1.<timestamp>.sqlite3
```

迁移保留 `images`、`assets`、`monitored_groups`、最终路径和群配置；旧非事件队列标记为
`legacy_failed`，旧任务取消，旧历史游标表删除。切换后确认：

- `40653` 未监听，宿主机没有 `3000/3001`；
- 四类文件数与切换前一致；
- 控制台、NapCat、WS、Worker、清理容器健康；
- 六个生产群仍停用且 Worker 仍暂停；
- 未登录或门禁未通过时图库不被修改。

若迁移后的服务不能启动，停止新容器、恢复唯一数据库副本，并从上一 Git 提交重建。
图库未迁移，不需要回滚图片。回滚副本和旧 QCE 数据 7 天后清除。

## 缓存安全

`cache-cleaner` 每 6 小时只遍历固定白名单：

- Pic、Emoji、`nt_temp`、采集 `.part` 保留 2 小时；
- Video、File、Ptt 保留 24 小时；
- QQ/NapCat 日志保留 48 小时；
- 旧 QCE 数据与唯一数据库回滚副本保留 7 天。

`nt_db`、登录会话、NapCat 配置、OneBot Token 和最终图库不在遍历范围内。
