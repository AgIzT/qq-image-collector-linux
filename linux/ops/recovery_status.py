#!/usr/bin/env python3
"""One-screen progress report for the history recovery the guard is watching."""

import json
import os
import sqlite3
import subprocess
import time
from pathlib import Path

DB = os.environ.get("QQAI_DATABASE", "/var/lib/qqai-state/collector_state.sqlite3")
TARGET_FILE = Path(os.environ.get("QQAI_RECOVERY_TARGET", "/etc/qqai-recovery-guard.json"))
# Same target as recovery_guard.py; without the file, the September recovery.
target = {"not_before": 1789361700, "not_after": 1789743600, "groups": []}
if TARGET_FILE.is_file():
    target.update(json.loads(TARGET_FILE.read_text(encoding="utf-8")))
NOT_BEFORE, NOT_AFTER = int(target["not_before"]), int(target["not_after"])
GROUPS = [str(group) for group in target.get("groups") or []]

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

group_filter = f" AND group_id IN ({','.join('?' * len(GROUPS))})" if GROUPS else ""
recovered = connection.execute(
    "SELECT COUNT(*) FROM images WHERE status='accepted' AND sent_at BETWEEN ? AND ?" + group_filter,
    (NOT_BEFORE, NOT_AFTER, *GROUPS),
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
span = lambda v: time.strftime("%m-%d %H:%M", time.localtime(v))
print(f"窗口          {span(NOT_BEFORE)} 到 {span(NOT_AFTER)}" + (f"  群 {','.join(GROUPS)}" if GROUPS else "  全部启用的群"))
print(f"状态          {state.get('phase')}")
print(f"群进度        {state.get('groups_terminal')}/{state.get('groups_total')}")
print(f"history 调用  {state.get('history_calls')}   最近一小时 {calls_hour}（上限 60）")
print(f"窗内消息      {state.get('messages_in_window')}")
print(f"入队图片      {state.get('images_enqueued')}   重复 {state.get('duplicates')}")
print(f"窗口内已入库  {recovered} 张")
if state.get("error"):
    print(f"错误          {state['error']}")
print()
for job in jobs:
    print(f"  {job['status']:<12} {job['groups']:>2} 个群  调用 {job['calls'] or 0:>5}  入队 {job['enqueued'] or 0:>5}")
