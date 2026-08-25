#!/usr/bin/env python3
"""按条件从图库挑出一批图片，用符号链接组成一个选集目录（不复制、不占空间）。"""
import argparse, datetime as dt, json, os, sqlite3, sys, shutil
from pathlib import Path

DB = os.environ.get(
    "QQAI_DATABASE",
    # The state volume moved to fast storage; images stayed on the big one.
    "/var/lib/qqai-state/collector_state.sqlite3",
)
SEL = Path(os.environ.get(
    "QQAI_SELECTION_ROOT",
    "/mnt/disk-1/qq-ai-image-collector/repository/selections",
))
CST = dt.timezone(dt.timedelta(hours=8))

def day_bounds(s):
    d = dt.datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=CST)
    return int(d.timestamp()), int((d + dt.timedelta(days=1)).timestamp())

ap = argparse.ArgumentParser()
ap.add_argument("--name", required=True, help="选集目录名")
ap.add_argument("--since", help="起始日期 YYYY-MM-DD (CST)")
ap.add_argument("--until", help="结束日期 YYYY-MM-DD (CST, 含当天)")
ap.add_argument("--model", action="append", default=[], help="模型关键字，可多次；任一命中即选")
ap.add_argument("--category", action="append", default=[], help="分类目录名，可多次")
ap.add_argument("--replace", action="store_true", help="已存在则先清空")
a = ap.parse_args()

lo = day_bounds(a.since)[0] if a.since else 0
hi = day_bounds(a.until)[1] if a.until else 1 << 62

con = sqlite3.connect(DB, timeout=60)
con.row_factory = sqlite3.Row
rows = con.execute(
    "SELECT sha256, local_path, category, metadata_json, canonical_sent_at "
    "FROM assets WHERE canonical_sent_at>=? AND canonical_sent_at<?", (lo, hi))

out = SEL / a.name
if out.exists() and a.replace:
    shutil.rmtree(out)
out.mkdir(parents=True, exist_ok=True)

seen = total = linked = missing = 0
by_cat = {}
for r in rows:
    total += 1
    if a.category and r["category"] not in a.category:
        continue
    if a.model:
        blob = r["metadata_json"] or ""
        if not any(m.lower() in blob.lower() for m in a.model):
            continue
    seen += 1
    src = Path(r["local_path"])
    if not src.is_file():
        missing += 1
        continue
    dst = out / src.name
    if not dst.exists():
        try:
            dst.symlink_to(src)
            linked += 1
        except OSError:
            missing += 1
    by_cat[r["category"]] = by_cat.get(r["category"], 0) + 1
con.close()
print(f"扫描 {total} 条，命中 {seen}，建链 {linked}，缺失 {missing}")
for k, v in sorted(by_cat.items(), key=lambda x: -x[1]):
    print(f"  {v:>6}  {k}")
print(f"选集目录: {out}")
