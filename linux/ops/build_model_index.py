#!/usr/bin/env python3
"""把每张图的模型标识抽进独立索引表。不修改 assets 等既有数据。

分类规则在归档目录的 model_taxonomy.py（与归档、图库共用同一份）：
  NAI系：NAI-V5 / NAI-V4.5 / NAI-V4 / NAI-V3 / NAI-V1/V2 / NAI-未知版本
  本地模型：Illustrious / NoobAI / Anima / Krea2 / MOSS-Image / ... / 本地-未识别

安全约束：
  - 仅 CREATE/ALTER TABLE、INSERT 到 asset_model；对 assets 只读
  - 每批小事务提交，批间短暂让出，避免长时间持锁拖垮采集 worker
  - 批大小与间隔可用 QQAI_INDEX_CHUNK / QQAI_INDEX_SLEEP 覆盖
  - 以 sha256 为主键，可随时中断续跑；新图补索引，分类器版本（TAXONOMY）
    升级后旧行按批重算，不需要清表
"""
import json, os, sqlite3, sys, time

DB = os.environ.get(
    "QQAI_DATABASE",
    # The state volume moved to fast storage; images stayed on the big one.
    "/var/lib/qqai-state/collector_state.sqlite3",
)
# cron runs a copy at /opt/build_model_index.py, away from model_taxonomy.py;
# the archive scripts import it from the ops directory itself.
OPS = os.environ.get("QQAI_OPS_DIR", "/opt/qq-ai-image-collector-linux-event-a1cfe1a/linux/ops")
sys.path[:0] = [OPS, os.path.dirname(os.path.abspath(__file__))]
import model_taxonomy  # noqa: E402

# The batch size and the pause between batches were chosen when the database
# sat on a volume that read at 26 MB/s, where a long-running index build
# starved the collector.  On the current volume a full rebuild does about nine
# seconds of actual work, so a 0.3s pause per batch was adding twenty-one
# seconds of pure waiting to it.  Keep a small pause - it still hands the
# writer a gap between transactions - and let both be tuned without an edit.
CHUNK = int(os.environ.get("QQAI_INDEX_CHUNK", "500"))
SLEEP = float(os.environ.get("QQAI_INDEX_SLEEP", "0.05"))

# Bump whenever model_taxonomy changes what it returns; rows indexed by an
# older version are re-derived on the next runs.
TAXONOMY = 2


def extract(blob, category, source=None):
    """返回 (模型名, 模型族)。"""
    try:
        fields = json.loads(blob) if blob else {}
    except ValueError:
        fields = {}
    family, model = model_taxonomy.classify(source, fields, category)
    return (model[:200] if model else None), family


def main():
    """建索引。放在函数里，归档脚本才能只 import extract 而不连数据库。"""
    con = sqlite3.connect(DB, timeout=60)
    con.execute("PRAGMA busy_timeout=60000")
    con.row_factory = sqlite3.Row
    con.execute("""CREATE TABLE IF NOT EXISTS asset_model (
        sha256 TEXT PRIMARY KEY, model TEXT, model_family TEXT,
        sent_at INTEGER, category TEXT, indexed_at INTEGER)""")
    if "taxonomy" not in {r[1] for r in con.execute("PRAGMA table_info(asset_model)")}:
        con.execute("ALTER TABLE asset_model ADD COLUMN taxonomy INTEGER NOT NULL DEFAULT 0")
    con.execute("CREATE INDEX IF NOT EXISTS idx_am_family_sent ON asset_model(model_family, sent_at)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_am_sent ON asset_model(sent_at)")
    con.commit()

    total = con.execute("SELECT count(*) FROM assets").fetchone()[0]
    current = con.execute("SELECT count(*) FROM asset_model WHERE taxonomy >= ?", (TAXONOMY,)).fetchone()[0]
    print(f"assets={total} 已是当前分类={current} 待处理={total-current}", flush=True)

    last = ""
    added = 0
    while True:
        rows = con.execute(
            "SELECT a.sha256, a.metadata_json, a.metadata_source, a.category, a.canonical_sent_at "
            "FROM assets a LEFT JOIN asset_model m ON m.sha256=a.sha256 "
            "WHERE (m.sha256 IS NULL OR m.taxonomy < ?) AND a.sha256>? ORDER BY a.sha256 LIMIT ?",
            (TAXONOMY, last, CHUNK)).fetchall()
        if not rows:
            break
        batch = []
        for r in rows:
            model, fam = extract(r["metadata_json"], r["category"], r["metadata_source"])
            batch.append((r["sha256"], model, fam, r["canonical_sent_at"], r["category"], int(time.time()), TAXONOMY))
            last = r["sha256"]
        con.executemany(
            "INSERT OR REPLACE INTO asset_model (sha256, model, model_family, sent_at, category, indexed_at, taxonomy) "
            "VALUES (?,?,?,?,?,?,?)", batch)
        con.commit()
        added += len(batch)
        print(f"  +{len(batch)} 累计 {added}/{total-current}", flush=True)
        time.sleep(SLEEP)

    print("=== 完成，按模型族统计 ===", flush=True)
    for r in con.execute("SELECT model_family, count(*) n FROM asset_model GROUP BY 1 ORDER BY n DESC"):
        print(f"  {r[1]:>6}  {r[0]}", flush=True)
    con.close()


if __name__ == "__main__":
    main()
