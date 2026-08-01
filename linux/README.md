# Linux 部署与事件链验收

本目录部署官方 NapCat、事件驱动采集器/控制台和缓存清理器。正式链路只消费
OneBot 正向 WS 事件中的 CDN URL；生产 Worker 不调用 `get_image`，不做连续历史
回填，也不依赖 QCE。

## 服务与端口

| Endpoint | 宿主机绑定 | 用途 |
| --- | --- | --- |
| NapCat WebUI | `0.0.0.0:10058` | 扫码与 NapCat 管理，使用独立强 Token |
| NapCat WebUI rescue | `127.0.0.1:16099` | SSH 本机救援 |
| Collector | `127.0.0.1:17890` | 由现有 Nginx `18080` 反代并验证 Token |
| OneBot HTTP | 仅 Compose `napcat:3000` | 健康、群列表和受限断档恢复 |
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

## 上线前冻结状态

在完成 Test A–F 前，必须在控制台同时满足：

- `collector_paused=true`；
- 原有六个生产群全部停用；
- 只使用自己的账号和隔离测试群；
- 不触发“恢复本次断档”，不创建任何历史任务；
- `allow_403_history_refresh=false`。

测试图片放到宿主持久目录 `${QQAI_RUNTIME_ROOT}/diagnostics/`；容器内只读路径为
`/diagnostics/`。每项测试必须保留命令的完整原始 JSON/终端输出，不能只记录结论。
输出中不得补写账号、群号、文件名、完整 CDN URL、rkey 或 Prompt。

## Test A：URL 形态普查

保持生产 Worker 暂停。先运行命令，再在隔离测试群发送图片，直至独立采集至少
200 个估算图片槽位。槽位数按标准段与 raw 图片候选的并集计算，即
`standard + raw - independently_matched_pairs`，不是取两边较大值，也不是把同一图片
重复计数：

```bash
./manage.sh probe-event <测试群号> 200
```

脚本直接检查标准图片段与 raw `picElement`，不复用生产事件解析结果；输出同时保存到
`repository/state/url_probe.json`。必须原样回报以下字段：

- `complete` 必须为 `true`，`captured_estimated_image_slots >= 200`；
- `standard_image_segments`、`raw_pic_elements`、`independently_matched_pairs`；
- `standard_without_raw_match`、`raw_without_standard_match`；
- `data_url_host_distribution`、`origin_url_host_distribution`；
- `data_url_rkey`、`origin_url_rkey`、`combined_rkey_ratio_percent`；
- `gchat_path_samples_redacted` 三条结构样例；
- `raw_original_flag_distribution`。

`timed_out=true`、样本不足或只有概述均不通过。Test A 决定生产应优先 `data.url`
还是 `originImageUrl`，不能预先假定 raw 与标准数组严格同序。

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

## Test D：URL 生命周期

先从测试群事件采集 10 条 URL。完整 URL 仅写入权限为 0600 的 secret 文件，终端只
显示哈希 ID、类型、主机和是否带 rkey：

```bash
./manage.sh url-lifecycle-capture <测试群号>
```

该命令会同时完成 T+0。之后必须在实际时间点运行：

```bash
./manage.sh url-lifecycle-check T+1h
./manage.sh url-lifecycle-check T+6h
./manage.sh url-lifecycle-check T+24h --finalize
```

完整状态矩阵保存在 `repository/state/url_lifecycle.report.json`。最后一步必须输出
`secret_urls_deleted=true`。不得提前连续执行四次伪造生命周期。

## URL preference 与 TTL 决策

只有 Test A–D 的原始结果齐全后才作决定：

| 实测结果 | 决策 |
| --- | --- |
| 仅 `data.url` 字节一致 | `url_preference=data` |
| 仅 raw `originImageUrl` 字节一致 | `url_preference=raw` |
| 两路均一致 | 优先无 rkey 且为 `gchat.qpic.cn` 的一路；否则按 Test D 生命周期更长者 |
| 两路均不一致 | 保持暂停，重新设计链路，不启用生产群 |
| 无 rkey 的 gchat URL 到 T+24h 均为 200 | 视为当前样本至少存活 24h，不需要 rkey 过期抢救 |
| rkey URL 到 T+24h 均为 200 | 只能证明 TTL 下限为 24h；保留保守调度并继续观察 |
| T+6h 前出现失效 | 当前 6h 提示不安全；先按最早失效时间减安全余量改实现，再灰度 |
| T+1h 或 T+0 已失效 | 不进入灰度 |

结论确定后可在控制台“CDN 首选通道”中选择 `data` 或 `raw`；值写入 SQLite，
Worker 下一循环生效并跨重启保留。

`daily_download_limit=3000` 表示 CDN 请求失控保护，不是防封阈值；`cdn_requests`
统计所有尝试，`cdn_downloads` 只统计完整成功的 200。账号风险边界是 OneBot 动作，
不是 CDN 字节量。

## Test E：original 分布与接受率

Test A 的 `raw_original_flag_distribution` 是第一份原始证据。要验证真实字节的接受率，
仅临时启用隔离测试群并解除暂停，投放一组原图/非原图/NULL 标志样本；采集完成后立即
重新暂停并停用测试群。等待至少一小时后输出：

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
还必须保留 Test A 屏蔽域名前后的 URL 分布对照，并用 `get_version_info`/构建信息确认
固定镜像摘要对应的 NapCat 版本和源码提交；映射不明则门禁失败。

当前已核验到的镜像级 provenance 为：多架构摘要
`sha256:e66a6e52dc5dd63a2b8537651bafad50255d021584c113a8bbb2cc0ff94bd772`，
amd64 子清单为
`sha256:11d72e50b6edc01b20f1a7611a250720e61412fe96184e3c70c3b8cf976744e1`，
SLSA 构建上下文指向官方 `NapNeko/NapCat-Docker` 提交
`f0599fb2eef4e9007aed72501849e2ca3eeaccdf`（2026-07-19）。这只能证明 Docker
封装来源，不能替代对镜像内 NapCatQQ 版本/源码提交的登录后核验。

## Test G：单群 72 小时稳态门禁

A–F 全部通过后，只启用一个灰度群，记录 `rollout_started_at`，连续运行至少 72 小时：

```bash
./manage.sh telemetry 72
```

必须保留完整 JSON，包括：

```text
events / image_segments / queued_high / queued_medium / queued_low
cdn_requests / cdn_downloads / cdn_bytes / cdn_403 / cdn_429
history_calls / get_image_blocked
accepted / rejected / duplicates / failed / expired / filtered_gif
```

硬门禁为：

- `duration_requirement_met=true`；
- `steady_state_gate=pass`；
- `history_calls=0`；
- `get_image_blocked=0`。

任一不满足就暂停扩群并定位路径。通过后每次只增加一个群，每次至少观察 48 小时。

## 明确禁止的回退方案

- 不增加国内中继、双机下载或 CDN 代理；现有证据不支持“境外 IP/字节量导致风控”。
- 不恢复 QCE、启动轮询、任意日期回填或连续历史回填。
- 不允许生产代码调用 `get_image/downloadRichMedia`。
- 不放宽 OneBot 动作白名单，不把 403 默认转换为历史调用。
- 不在 A–F 原始证据不完整时启用原有六群。

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
