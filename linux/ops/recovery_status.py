#!/usr/bin/env python3
"""One-screen progress report for the 2026-09-14 outage recovery."""

import json
import os
import sqlite3
import subprocess
import time

DB = os.environ.get("QQAI_DATABASE", "/var/lib/qqai-state/collector_state.sqlite3")
NOT_BEFORE, NOT_AFTER = 1789361700, 1789743600
# The outage itself, which is the part worth counting as recovered.
GAP_START, GAP_END = 1789361700, 1789743000

connection = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=20)
connection.row_factory = sqlite3.Row

alive = subprocess.run(["ps", "-eo", "args"], capture_output=True, text=True, check=False)
running = sum(
    1 for line in alive.stdout.splitlines()
    if "qq_image_collector.window_recovery" in line and "-eo args" not in line
)

row = connection.execute(
    "SELECT value_json FROM runtime_state WHERE key='window_recovery'"
).fetchone()
state = json.loads(row[0]) if row else {}

recovered = connection.execute(
    "SELECT COUNT(*) FROM images WHERE status='accepted' AND sent_at BETWEEN ? AND ?",
    (GAP_START, GAP_END),
).fetchone()[0]

calls_hour = connection.execute(
    "SELECT COUNT(*) FROM window_recovery_calls WHERE not_before=? AND called_at > ?",
    (NOT_BEFORE, int(time.time()) - 3600),
).fetchone()[0]

jobs = connection.execute(
    """
    SELECT status, COUNT(*) AS groups, SUM(history_calls) AS calls,
           SUM(images_enqueued) AS enqueued
    FROM window_recovery_jobs WHERE not_before=? GROUP BY status ORDER BY 2 DESC
    """,
    (NOT_BEFORE,),
).fetchall()

if running:
    note = ""
elif state.get("phase") in ("finished", "failed_policy"):
    note = "   <- 已结束，不会再拉起"
else:
    note = "   <- 已停；守护会在 5 分钟内拉起"
print(f"进程          {running} 个{note}")
print(f"状态          {state.get('phase')}")
print(f"群进度        {state.get('groups_terminal')}/{state.get('groups_total')}")
print(f"history 调用  {state.get('history_calls')}   最近一小时 {calls_hour}（上限 60）")
print(f"窗内消息      {state.get('messages_in_window')}")
print(f"入队图片      {state.get('images_enqueued')}   重复 {state.get('duplicates')}")
print(f"断流期已入库  {recovered} 张")
if state.get("error"):
    print(f"错误          {state['error']}")
print()
for job in jobs:
    print(f"  {job['status']:<12} {job['groups']:>2} 个群  调用 {job['calls'] or 0:>5}  入队 {job['enqueued'] or 0:>5}")
