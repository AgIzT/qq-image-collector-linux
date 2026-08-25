#!/usr/bin/env python3
"""把每张图的模型标识抽进独立索引表。只新增，不修改任何既有数据。

安全约束：
  - 仅 CREATE TABLE / INSERT 到 asset_model；对 assets 只读
  - 每批小事务提交，批间休眠，避免长时间持锁拖垮采集 worker
  - 以 sha256 为主键，重复运行只补新增（可随时中断续跑）
"""
import json, os, re, sqlite3, sys, time

DB = os.environ.get(
    "QQAI_DATABASE",
    # The state volume moved to fast storage; images stayed on the big one.
    "/var/lib/qqai-state/collector_state.sqlite3",
)
CHUNK = 200
SLEEP = 0.3

NAI = re.compile(r"(?:NovelAI\s+Diffusion\s+|DiffusionModelMetaName\.NAI)v?([0-9]+)", re.I)

def extract(blob, category):
    """返回 (原始模型串, 归一族名)。"""
    if not blob:
        return None, "unknown"
    raw = None
    try:
        d = json.loads(blob)
        if isinstance(d, dict):
            for k in ("Source", "source", "Model", "model", "sd_model_name"):
                v = d.get(k)
                if isinstance(v, str) and v.strip():
                    raw = v.strip()
                    break
            if raw is None:
                c = d.get("Comment") or d.get("comment")
                if isinstance(c, str):
                    try:
                        cj = json.loads(c)
                        if isinstance(cj, dict):
                            v = cj.get("Source") or cj.get("source")
                            if isinstance(v, str):
                                raw = v.strip()
                    except Exception:
                        pass
    except Exception:
        pass
    hay = raw if raw else blob[:4000]
    m = NAI.search(hay) or NAI.search(blob[:8000])
    if m:
        if raw is None:
            s = m.group(0)
            raw = s
        return raw[:200], "NAI-V" + m.group(1)
    if category == "ComfyUI":
        return (raw or "")[:200] or None, "ComfyUI"
    return (raw or "")[:200] or None, (category or "unknown")

con = sqlite3.connect(DB, timeout=60)
con.execute("PRAGMA busy_timeout=60000")
con.row_factory = sqlite3.Row
con.execute("""CREATE TABLE IF NOT EXISTS asset_model (
    sha256 TEXT PRIMARY KEY, model TEXT, model_family TEXT,
    sent_at INTEGER, category TEXT, indexed_at INTEGER)""")
con.execute("CREATE INDEX IF NOT EXISTS idx_am_family_sent ON asset_model(model_family, sent_at)")
con.execute("CREATE INDEX IF NOT EXISTS idx_am_sent ON asset_model(sent_at)")
con.commit()

total = con.execute("SELECT count(*) FROM assets").fetchone()[0]
done0 = con.execute("SELECT count(*) FROM asset_model").fetchone()[0]
print(f"assets={total} 已索引={done0} 待处理={total-done0}", flush=True)

last = ""
added = 0
while True:
    rows = con.execute(
        "SELECT a.sha256, a.metadata_json, a.category, a.canonical_sent_at "
        "FROM assets a LEFT JOIN asset_model m ON m.sha256=a.sha256 "
        "WHERE m.sha256 IS NULL AND a.sha256>? ORDER BY a.sha256 LIMIT ?",
        (last, CHUNK)).fetchall()
    if not rows:
        break
    batch = []
    for r in rows:
        model, fam = extract(r["metadata_json"], r["category"])
        batch.append((r["sha256"], model, fam, r["canonical_sent_at"], r["category"], int(time.time())))
        last = r["sha256"]
    con.executemany("INSERT OR IGNORE INTO asset_model VALUES (?,?,?,?,?,?)", batch)
    con.commit()
    added += len(batch)
    print(f"  +{len(batch)} 累计 {added}/{total-done0}", flush=True)
    time.sleep(SLEEP)

print("=== 完成，按模型族统计 ===", flush=True)
for r in con.execute("SELECT model_family, count(*) n FROM asset_model GROUP BY 1 ORDER BY n DESC"):
    print(f"  {r[1]:>6}  {r[0]}", flush=True)
con.close()
