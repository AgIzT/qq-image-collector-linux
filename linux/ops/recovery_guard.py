#!/usr/bin/env python3
"""Keep a bounded history-window recovery running until it actually finishes.

The recovery for the 2026-09-14 outage is a ~70 hour job at 60 history calls an
hour, run detached inside the collector container.  It is resumable - every page
anchor lives in window_recovery_jobs - but nothing restarts it, and a container
restart or an unhandled error would leave it stopped with no sign.  That is the
same shape of failure as the outage it is recovering from: quiet, and only
visible if somebody happens to look.

So this checks every few minutes and restarts it, unless the run has reached a
state where restarting is wrong:

  finished       the window is fully paged; nothing to do
  failed_policy  a safety check refused the run, and repeating the same command
                 would just refuse again, loudly, forever

Restarts are logged and capped.  A run that keeps dying is a fault to look at,
not something to relaunch several hundred times.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

DB = os.environ.get("QQAI_DATABASE", "/var/lib/qqai-state/collector_state.sqlite3")
STATE = Path(os.environ.get("QQAI_RECOVERY_GUARD_STATE", "/var/lib/qqai-recovery-guard.json"))
TARGET_FILE = Path(os.environ.get("QQAI_RECOVERY_TARGET", "/etc/qqai-recovery-guard.json"))
CONTAINER = os.environ.get("QQAI_COLLECTOR_CONTAINER", "qqai-collector-console")
MAX_RESTARTS = 40

# The run this guard keeps alive.  TARGET_FILE replaces any of these for a new
# window; without it the guard watches the September outage recovery.
DEFAULT_TARGET = {
    "not_before": 1789361700,
    "not_after": 1789743600,
    "expected_groups": 15,
    # Empty means every enabled group.
    "groups": [],
    # The per-group ceiling stops one group whose paging never converges from
    # spending the whole budget.  It counts every call ever made for the group
    # in this window, so it has to cover what was already spent as well.  1500
    # was set for the first run from an estimate that left the biggest group
    # almost no margin, and hitting it was silent; replays burned it on
    # 519382846 and 726877770.  For the 2026-09-26 re-run of 519382846: 1,500
    # already spent, and its window is ~15,900 messages, about 840 pages from
    # the top anchor, so 3000 leaves ~1.8x.  A group that reaches it now shows
    # up in the watchdog.
    "max_calls_per_group": 3000,
    "log": "/data/qq-image-collector/window-recovery-0918.log",
}


def load_target() -> dict:
    target = dict(DEFAULT_TARGET)
    try:
        target.update(json.loads(TARGET_FILE.read_text(encoding="utf-8")))
    except FileNotFoundError:
        pass
    groups = [str(group) for group in target.get("groups") or []]
    # These end up in a shell command line.
    if any(not re.fullmatch(r"\d{5,12}", group) for group in groups):
        raise ValueError(f"{TARGET_FILE}: group ids must be plain numbers")
    target["groups"] = groups
    return target


def build_command(target: dict) -> str:
    groups = ",".join(target["groups"])
    return (
        "python3 -m qq_image_collector.window_recovery"
        " --config /data/qq-image-collector/config/collector_config.json"
        f" --not-before {int(target['not_before'])} --not-after {int(target['not_after'])}"
        f" --expected-groups {int(target['expected_groups'])} --page-size 20"
        " --hourly-limit 60 --daily-limit 1440"
        f" --max-calls-per-group {int(target['max_calls_per_group'])}"
        + (f" --groups {groups}" if groups else "")
        + " --window-override"
        f" >> {shlex.quote(str(target['log']))} 2>&1"
    )


TARGET = load_target()
WINDOW = (int(TARGET["not_before"]), int(TARGET["not_after"]))
COMMAND = build_command(TARGET)


def say(message: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {message}", flush=True)


def read_state() -> dict:
    try:
        return json.loads(STATE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def write_state(state: dict) -> None:
    temporary = STATE.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    temporary.replace(STATE)


def recovery_phase() -> tuple[str, dict]:
    connection = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=20)
    try:
        row = connection.execute(
            "SELECT value_json FROM runtime_state WHERE key='window_recovery'"
        ).fetchone()
    finally:
        connection.close()
    if not row:
        return "unknown", {}
    try:
        value = json.loads(str(row[0]))
    except ValueError:
        return "unknown", {}
    if not isinstance(value, dict):
        return "unknown", {}
    # State published for some other window says nothing about this one.
    if int(value.get("not_before") or 0) != WINDOW[0]:
        return "other_window", value
    return str(value.get("phase") or "unknown"), value


def unfinished_jobs() -> int:
    """Jobs in this window that still have pages to fetch.

    The published phase alone is not enough: before 2026-09-29 the runner
    also said "finished" while a job waited between pages, and a process that
    died then was left for dead with its job still queued.
    """
    connection = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=20)
    try:
        return int(connection.execute(
            """
            SELECT count(*) FROM window_recovery_jobs
            WHERE not_before=? AND not_after=? AND status IN ('probe','queued','deferred')
            """,
            WINDOW,
        ).fetchone()[0])
    finally:
        connection.close()


def running() -> bool:
    result = subprocess.run(
        ["ps", "-eo", "args"], capture_output=True, text=True, check=False
    )
    return any(
        "qq_image_collector.window_recovery" in line and f"--not-before {WINDOW[0]}" in line
        for line in result.stdout.splitlines()
    )


def main() -> int:
    phase, value = recovery_phase()
    if phase == "failed_policy" or (phase == "finished" and not unfinished_jobs()):
        say(f"recovery {phase}; nothing to restart")
        return 0
    if phase == "other_window":
        say("runtime state belongs to a different window; leaving it alone")
        return 0
    if running():
        return 0

    state = read_state()
    # Restarts are counted per window; a new run starts with a clean slate.
    if state.get("window") != list(WINDOW):
        state = {"window": list(WINDOW)}
    restarts = int(state.get("restarts") or 0)
    if restarts >= MAX_RESTARTS:
        say(f"refusing to restart again after {restarts} attempts; investigate the log")
        return 1

    command = ["docker", "exec", "-d", CONTAINER, "sh", "-lc", COMMAND]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        say(f"restart failed: {result.stderr.strip()[:200]}")
        return 1
    state["restarts"] = restarts + 1
    state["last_restart_at"] = int(time.time())
    state["last_phase"] = phase
    state["calls"] = value.get("history_calls")
    state["images_enqueued"] = value.get("images_enqueued")
    write_state(state)
    say(
        f"restarted recovery (attempt {restarts + 1}); was phase={phase} "
        f"calls={value.get('history_calls')} enqueued={value.get('images_enqueued')}"
    )
    say(f"command: {shlex.join(command[:4])} ...")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
