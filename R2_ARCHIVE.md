# 图库归档到 R2

图库卷只有 49 GB，日增约 1.5 GB，写满不会报错——ENOSPC 走通用重试分支，退避到
一小时一次，心跳照常、控制台全绿、入库数悄悄归零。所以归档不是"有空再说"的事，
它是让采集能一直跑下去的前提。

`linux/ops/archive_to_r2.py` 每天从 cron 跑一次：把新图传到 R2，再删掉本地那些
**已经确认存在于 R2 的**老图。本地稳定保留最近 14 天。

---

## 1. 三条决定了对象布局的理由

### 对象键用 sha256，不用 `分类/日期/文件名`

仓库文件名里嵌着群号和发送者 QQ 号：

```
2026-08-25_01-23-48_g100000001_u200000002_669f941cf5.png
                     └ 群号 ┘  └ QQ 号 ┘
```

对象键是归档里唯一会出现在每一条 URL、每一行日志、每一次 listing 里的东西。
所以键是内容寻址的：

```
originals/<sha256[0:2]>/<sha256><ext>
```

分类和日期没有丢，它们在日索引里——那才是它们该待的地方。顺带两个好处：
同一张图天然只存一份（`assets` 本来就按 sha256 去重），本地的 Windows 旧收藏
里那 474 张与服务器重复的图也会自动落在同一个键上，不会存两遍。

### 元数据按大小分三处

`metadata_json` 总共 648 MB，ComfyUI 的工作流平均 165 KB、最大 2.4 MB。全塞进
日索引的话，一天的索引就是 30 MB，前端没法用。所以：

| 键 | 内容 | 大小 |
|---|---|---|
| `data/days/<YYYY-MM-DD>.json` | 列表页要的：提示词、负向、采样器、种子、模型族 | 每天约 500 KB（gzip） |
| `meta/<sha[0:2]>/<sha>.json` | 原始 `metadata_json` 全文，点开某张图才取 | 单个 KB~MB 级 |
| `private/days/<YYYY-MM-DD>.json` | 群号、QQ 号、消息 ID、原文件名 | 每天几十 KB |

`private/` 不该被任何公开路由映射出去。把它和公开数据放在不同前缀，是为了让
"别把群号发出去"成为结构上的事实，而不是一条需要记住的规矩。

> **这个桶不要开公开访问。** 不要给 `qqai-image-archive` 开 r2.dev 公开 URL，
> 也不要直接绑自定义域——两者都会让整个桶可读，而 `private/days/2026-08-21.json`
> 这种键是猜得出来的，等于把群号和 QQ 号挂到公网上。
>
> 展示端要读图，正确做法是走 Worker 绑定这个桶，并且**按前缀白名单**放行：
> 只放 `originals/`、`meta/`、`data/`。站内 `ATLAS_DATA_BUCKET` 就是这个模式。
> 如果哪天真的需要整桶公开，先把 `private/` 挪到另一个桶再说。

JSON 都带 `Content-Encoding: gzip`（图片不压——PNG/WebP 已压过，收益 0-2%）。
浏览器 `fetch` 透明解压；`curl` 要加 `--compressed`。

### 删除前必须现场核对

删本地文件之前，脚本对那一天的每个对象发一次 HEAD，比对 `content-length`。
只要有一个对不上，整天不删。

这个核对故意不信任自己的记账表：`archive_state.sqlite3` 说传过了，不等于 R2
现在还有。核对的价值就在于它独立于那张表。

---

## 2. 一天什么时候才算"可以删"

补漏会往**过去**的日期目录写图——断线补漏和定窗补漏都按消息的 `sent_at` 决定
落盘目录，所以今天跑一次补漏，可能往 `2026-08-13/` 里加文件。
**"这天已经过完了" ≠ "这天不会再变"。** 四个条件全满足才删：

1. 日期比今天早 `keep_days` 天（默认 14）
2. `images` 表里那一天没有 `queued` / `deferred` / `downloading` 的记录
3. 那天的目录里最新的文件 mtime 已经 `seal_hours` 小时（默认 6）没动
4. 那天每个 asset 都通过了 HEAD 核对

删完只在目录真正空了的时候才 `rmdir`。如果某个文件在 HEAD 和删除之间又冒出来，
它的目录会留着——`store_asset` 发现 `local_path` 指向的文件不存在时会重新下载
一份（这是有意的自愈行为），所以已归档的日期目录偶尔重新出现文件是正常的，
不是 bug。下一轮会把它当作待传项处理。

---

## 3. 用法

```bash
# 看进度：每天多少张、传了多少、哪些已清理
python3 linux/ops/archive_to_r2.py --status

# 传一天试试（不会删任何东西）
python3 linux/ops/archive_to_r2.py --upload --day 2026-07-20

# 看看会删什么，但别真删
python3 linux/ops/archive_to_r2.py --purge --dry-run

# cron 跑的就是这一条
python3 linux/ops/archive_to_r2.py --upload --purge --index --max-seconds 14400
```

`--max-seconds` / `--max-bytes` 让任务可以被安全打断：传到哪算哪，进度在
`/var/lib/qqai-state/archive_state.sqlite3` 里，下一轮接着传。图库卷单线程
只有 3 MB/s、八线程约 10 MB/s，**全量首传是小时级的，别指望一次跑完**，也别在
前台 SSH 里跑（会话空闲会断）。

归档状态单独一个数据库，不往 `assets` / `images` 加列——采集侧是那两张表的
唯一写入方。

### 凭证

```bash
qqai-set-r2 set <account_id> <access_key_id> <secret_access_key> [bucket]
qqai-set-r2 keep 14      # 本地保留天数
qqai-set-r2 test         # 连一次，确认读写都通
```

写进 `/etc/qqai-archive.json`（0600），不进仓库。换 token 只需要跑一次 `set`。

---

## 4. 数据格式

日索引的一条记录，字段名对齐站内法典的词条格式（`id` / `title` / `path` /
`tags` / `negative`），这样展示端可以基本照搬：

```json
{
  "id": "ac1ea1c756c180e0372c9bcc7b82975e07a0fc2109dd19413658a24e03528e4f",
  "title": "1.3::artist:suzumi_(ccroquette)::",
  "path": ["NovelAI", "2026-07-24"],
  "tags": "1.3::artist:suzumi_(ccroquette)::,artist:dino_(dinoartforame),...",
  "negative": "lowres, artistic error, film grain, ...",
  "params": {"steps": 28, "sampler": "k_dpmpp_2m", "seed": 2315686423,
             "scale": 5.0, "noiseSchedule": "exponential", "cfgRescale": 0.0},
  "model": "DiffusionModelMetaName.NAIv5 A7FFBC5D",
  "modelFamily": "NAI-V5",
  "category": "NovelAI",
  "metadataSource": "novelai",
  "ext": ".png", "width": 1024, "height": 1536, "size": 3381904,
  "sentAt": 1784824924,
  "origin": "server"
}
```

图片地址由 `id` 和 `ext` 拼出来，不需要另存一列：

```
<base>/originals/<id[0:2]>/<id><ext>
```

`tags` / `negative` 怎么来，取决于 `metadataSource`，因为四个通道的形状完全不同：

| `metadataSource` | 提示词来自 |
|---|---|
| `novelai`、`novelai-unreadable` | `Description` 是正向；`Comment` 是**字符串形式的 JSON**，再 `json.loads` 一次才拿到 `uc`（负向）和采样参数。少数记录走 EXIF 通道，顶层是 `UserComment` |
| `a1111-compatible` | `parameters` 是 A1111 那种纯文本：正向 → `Negative prompt:` → `Steps: ...` 键值尾巴 |
| `comfyui` | 没有提示词字段。从节点图里 `CLIPTextEncode` 的 `inputs.text` 猜，猜出来的标 `promptSource: "node-graph-heuristic"` |
| `unknown-generator` | 多半是 ComfyUI 的分支，`prompt` 常常是序列化的节点图而非文本，同上处理。认不出结构就宁可留空，也不把一坨 JSON 当提示词 |

**写消费端时不要假设某个键一定存在。** `tags`、`negative`、`params`、`model`
都可能缺；ComfyUI 的图经常只有 `hasWorkflow: true`。

顶层 `data/index.json` 列出所有天（对应站内的 `codexes.json`）：

```json
{
  "id": "qqai-archive",
  "entryCount": 19195, "bytes": 43635072487, "dayCount": 44,
  "categories": {"NovelAI": 13663, "ComfyUI": 3224, "...": 0},
  "modelFamilies": {"NAI-V5": 7681, "NAI-V4": 5159, "...": 0},
  "days": [{"day": "2026-07-20", "origin": "server",
            "entryCount": 9, "bytes": 19299604,
            "path": "data/days/2026-07-20.json"}]
}
```

`origin` 区分两批来源不同的数据：`server` 是采集器入库的（数据库里有行），
`legacy` 是部署服务器之前 Windows 时代的本地收藏（**数据库里没有对应行**，
`assets` / `images` 查不到它们）。两批共用一个 `originals/` 空间（同 sha256
自然合并），但索引分开，因为它们的数据库状态不一样。

---

## 5. 服务器之前的那批（`origin: "legacy"`）

Windows 时代收的 5,090 张（11.2 GB，2026-06-08 ~ 07-24）不在数据库里——
`assets` / `images` 两张表都没有对应行，所以 IMAGE_LIBRARY_SPEC.md 里那些
SQL 查不到它们，元数据只能从文件里重新读。

`linux/ops/archive_legacy_to_r2.py` 在**存着这些文件的那台 Windows 机器上**跑，
它 import `archive_to_r2.py` 拿上传、键布局和记录格式，只有"字段从哪来"是自己的：

```bash
python linux/ops/archive_legacy_to_r2.py \n    --root "D:/program/群聊图片获取/final" \n    --config r2_archive_config.json --bucket qqai-image-archive \n    --state legacy_archive.sqlite3
```

传完会写一份 `legacy_days.json`，拿到服务器上合进索引：

```bash
scp legacy_days.json <server>:/tmp/
ssh <server> 'python3 .../archive_to_r2.py --merge-days /tmp/legacy_days.json'
```

两批共用 `originals/` 空间——内容寻址让两边都有的图落在同一个键上，只存一份，
所以那 474 张"服务器已有同 sha256"的不用特殊处理。索引分开，因为
**数据库状态不同**：`origin: "server"` 的能用 SQL 查出来源，`origin: "legacy"`
的查不到。消费端要能区分这一点。

`_rejected/`（243 张，确认无价值）和 `_prompt_only/`（91 张，见下）不传。

## 6. 还没做的

- **缩略图。** 原图平均 2.3 MB，19,000 张的列表页不能直接加载原图。键位留了
  `thumbs/<sha[0:2]>/<sha>.webp`，但服务器是 2 核 2 GB 且没装 Pillow，在上面
  批量生成会和采集抢 CPU、触发心跳告警。R2 出口免费，更合适的做法是从 R2
  拉下来在别的机器上生成，或者交给 Cloudflare Images 按需缩放。
- **展示端。** 数据格式是照着站内法典的词条格式设计的，前端还没有。
- **本地那批的删除。** 脚本不删任何本地文件。磁盘压力在服务器上，不在这台
  Windows 机器上，所以什么时候删是另一个决定。
- **`_prompt_only` 那 91 张。** 有实质提示词但缺 Steps / Sampler / CFG，按当前
  契约不算"可复现的生成参数"，所以既没进 `final/` 也没上传。等决定。
