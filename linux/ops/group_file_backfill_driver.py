#!/usr/bin/env python3
"""Run the one-time group-file backfill in chunks the image volume can hold.

The volume keeps 14 days of images locally and sits a few GB from full; a
full disk does not crash the collector, it silently stops storing images (see
R2_ARCHIVE.md).  The backfill's images mostly land on dates older than that,
which the archive moves to R2 and deletes locally -- but only once a date has
been quiet for seal_hours, so gallery maintenance has had time to build
thumbnails from the local file.

So this loops on the host:

1. Days the backfill wrote to, once quiet for seal_hours and older than
   keep_days, get the normal ``archive_to_r2.py --upload --purge`` for just
   those days, under the archive's own lock and with its normal checks.
2. If free space is comfortably above the reserve, one backfill chunk runs in
   the collector container, stopping before free space reaches the reserve.
3. Sleep and repeat until the backfill reports nothing left and every day it
   touched has been handed to the archive.

Progress: /var/log/qqai-group-file-backfill.log and the backfill's
/var/lib/qqai-state/group_file_backfill.json.  Safe to stop and start again.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

CONTAINER = os.environ.get("QQAI_CONTAINER", "qqai-collector-console")
STATE = Path("/var/lib/qqai-state/group_file_backfill.json")
HANDED = Path("/var/lib/qqai-state/group_file_backfill_archived.json")
IMAGE_ROOT = Path("/mnt/disk-1/qq-ai-image-collector/repository")
ARCHIVE = "/opt/qq-ai-image-collector-linux-event-a1cfe1a/linux/ops/archive_to_r2.py"
ARCHIVE_CONFIG = Path("/etc/qqai-archive.json")
ARCHIVE_LOCK = "/run/lock/qqai-archive.lock"
LOG = Path("/var/log/qqai-group-file-backfill.log")
# A day of live intake is about 2.3 GB and the nightly archive frees about the
# same, so the volume normally bottoms out near 3.5 GB just before 04:30.  The
# backfill's old-day files are archived away about six hours after a chunk,
# and live intake keeps arriving meanwhile; four keeps that dip above where
# the volume already goes every night.
RESERVE = int(float(os.environ.get("QQAI_BACKFILL_RESERVE_GB", "4")) * 1024**3)
MIN_CHUNK = 512 * 1024**2
SLEEP_SECONDS = 15 * 60
TZ = dt.timezone(dt.timedelta(hours=8))


def log(message: str) -> None:
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(line, flush=True)
    with LOG.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def write_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(temporary, path)


def day_age_days(day: str, now: float) -> float:
    try:
        start = dt.datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=TZ)
    except ValueError:
        return -1
    return (now - start.timestamp()) / 86400


def archive_ready_days(keep_days: int, seal_hours: float) -> list[str]:
    touched = read_json(STATE).get("touched_days") or {}
    handed = read_json(HANDED)
    now = time.time()
    ready = []
    changed = False
    for day, written in sorted(touched.items()):
        if int(written) <= int(handed.get(day) or 0):
            continue
        if day_age_days(day, now) <= keep_days + 1:
            # Recent days stay local by design; the nightly run takes them
            # when they age out.  Nothing for the driver to do.
            handed[day] = int(written)
            changed = True
            continue
        if now - int(written) >= seal_hours * 3600 + 900:
            ready.append(day)
    if changed:
        write_json(HANDED, handed)
    return ready


def run_archive(days: list[str]) -> bool:
    command = ["flock", ARCHIVE_LOCK, "python3", "-u", ARCHIVE, "--upload", "--purge", "--workers", "3"]
    for day in days:
        command += ["--day", day]
    log(f"archive {len(days)} days: {days[0]} .. {days[-1]}")
    with LOG.open("a", encoding="utf-8") as handle:
        result = subprocess.run(command, stdout=handle, stderr=subprocess.STDOUT, check=False)
    if result.returncode != 0:
        log(f"archive exited {result.returncode}; will retry these days")
        return False
    handed = read_json(HANDED)
    touched = read_json(STATE).get("touched_days") or {}
    for day in days:
        handed[day] = int(touched.get(day) or time.time())
    write_json(HANDED, handed)
    return True


def run_chunk(budget: int, min_age_days: int) -> dict | None:
    command = [
        "docker", "exec", "-w", "/app", CONTAINER,
        "python", "-u", "-m", "qq_image_collector.group_file_backfill",
        "--min-free-bytes", str(RESERVE),
        "--max-stored-bytes", str(budget),
        "--min-age-days", str(min_age_days),
    ]
    log(f"backfill chunk, up to {budget / 1024**3:.1f} GB of new files")
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    last = ""
    with LOG.open("a", encoding="utf-8") as handle:
        for line in process.stdout:  # type: ignore[union-attr]
            handle.write(line)
            handle.flush()
            if line.strip():
                last = line.strip()
    code = process.wait()
    if code != 0:
        log(f"backfill chunk exited {code}")
        return None
    try:
        summary = json.loads(last)
    except ValueError:
        log("backfill chunk printed no summary")
        return None
    stats = summary.get("stats") or {}
    log(
        "chunk done: stop={stop_reason} remaining={remaining} processed={processed} "
        "accepted={accepted} duplicate={duplicate} rejected={rejected} expired={expired} "
        "deferred_recent={deferred} stored={stored:.2f}GB".format(
            stop_reason=summary.get("stop_reason"),
            remaining=summary.get("remaining"),
            processed=stats.get("processed"),
            accepted=stats.get("accepted"),
            duplicate=stats.get("duplicate"),
            rejected=stats.get("rejected"),
            expired=stats.get("expired"),
            deferred=stats.get("deferred_recent"),
            stored=int(stats.get("stored_bytes") or 0) / 1024**3,
        )
    )
    return summary


def main() -> int:
    config = read_json(ARCHIVE_CONFIG)
    keep_days = int(config.get("keep_days", 14))
    seal_hours = float(config.get("seal_hours", 6))
    min_age_days = keep_days + 1
    log(
        f"driver start: reserve {RESERVE / 1024**3:.1f} GB, keep {keep_days} d, "
        f"seal {seal_hours} h, files newer than {min_age_days} d wait"
    )
    finished = False
    # Once only files too recent to archive are left, one pass a day picks
    # them up as they age instead of listing every group every 15 minutes.
    slow = False
    last_chunk = 0.0
    last_wait_log = 0.0
    failures = 0
    idle = 0
    while True:
        ready = archive_ready_days(keep_days, seal_hours)
        if ready:
            run_archive(ready)
        free = shutil.disk_usage(IMAGE_ROOT).free
        if not finished:
            budget = free - RESERVE
            due = not slow or time.time() - last_chunk >= 86400
            if due and budget >= MIN_CHUNK:
                summary = run_chunk(budget, min_age_days)
                last_chunk = time.time()
                if summary is None:
                    failures += 1
                    if failures >= 3:
                        log("three chunks in a row failed; stopping for a look")
                        return 1
                else:
                    failures = 0
                    stats = summary.get("stats") or {}
                    idle = idle + 1 if int(stats.get("processed") or 0) == 0 else 0
                    # Two passes that fetch nothing mean what is left cannot
                    # be fetched (unlistable group, vanished file).
                    exhausted = summary.get("stop_reason") == "done" and (
                        not summary.get("remaining") or idle >= 2
                    )
                    if exhausted and int(stats.get("deferred_recent") or 0) > 0:
                        if not slow:
                            log(f"only files from the last {min_age_days} days remain; one pass a day from now")
                        slow = True
                    elif exhausted:
                        finished = True
                        log("backfill has nothing left; waiting for the archive to take the last days")
            elif due and time.time() - last_wait_log >= 3600:
                last_wait_log = time.time()
                log(f"free {free / 1024**3:.1f} GB; waiting for the archive to make room")
        else:
            handed = read_json(HANDED)
            pending = [
                day for day, written in (read_json(STATE).get("touched_days") or {}).items()
                if int(written) > int(handed.get(day) or 0)
            ]
            if not pending:
                log("all done")
                return 0
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    sys.exit(main())
