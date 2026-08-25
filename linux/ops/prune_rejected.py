#!/usr/bin/env python3
"""删除超过保留期的淘汰记录。

淘汰记录的唯一用途是让补漏重读历史时跳过重复下载；补漏窗口最多几小时，
因此远早于保留期的记录不可能再被用到。分批提交并在批间让出写锁，避免
拖垮采集 worker。
"""
import os, sqlite3, sys, time

DB = os.environ.get(
    "QQAI_DATABASE",
    # The state volume moved to fast storage; images stayed on the big one.
    "/var/lib/qqai-state/collector_state.sqlite3",
)
KEEP_DAYS = 7
BATCH = 2000
SLEEP = 0.4
STATUSES = ("rejected_no_metadata", "filtered_gif")

apply = "--apply" in sys.argv
cutoff = int(time.time()) - KEEP_DAYS * 86400
con = sqlite3.connect(DB, timeout=120)
con.execute("PRAGMA busy_timeout=120000")

marks = ",".join("?" * len(STATUSES))
total = con.execute(
    f"SELECT count(*) FROM images WHERE status IN ({marks}) AND sent_at>0 AND sent_at<?",
    (*STATUSES, cutoff),
).fetchone()[0]
stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(cutoff))
print("保留 {} 天，截止 {}".format(KEEP_DAYS, stamp), flush=True)
print(f"待删除 {total} 行  apply={apply}", flush=True)
if not apply or total == 0:
    print("(未执行)", flush=True)
    raise SystemExit

done = 0
while True:
    cur = con.execute(
        f"DELETE FROM images WHERE rowid IN ("
        f"  SELECT rowid FROM images WHERE status IN ({marks}) AND sent_at>0 AND sent_at<? LIMIT ?)",
        (*STATUSES, cutoff, BATCH),
    )
    con.commit()
    if not cur.rowcount:
        break
    done += cur.rowcount
    print(f"  已删 {done}/{total}", flush=True)
    time.sleep(SLEEP)

rest = con.execute("SELECT status, count(*) FROM images GROUP BY 1 ORDER BY 2 DESC").fetchall()
print("=== 剩余分布 ===", flush=True)
for s, n in rest:
    print(f"  {n:>8}  {s}", flush=True)
print(f"总行数 {sum(n for _, n in rest)}", flush=True)
con.close()
