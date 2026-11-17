#!/usr/bin/env python3
"""Out-of-band health watch for the collector.

This deliberately shares nothing with the console.  The console has frozen on
its own status refresh before, and on 2026-08-13 the thing that actually died
was the QQ process inside the NapCat container: the container stayed up, its
restart policy never fired because QQ is not its PID 1, the collector worker
kept heartbeating because it was alive and merely idle, and collection was
silently down for fourteen hours.  Anything that watches for that has to run
outside all of it - own process, own cron entry, own read-only handle on the
database - and it has to look at whether images are still arriving rather than
at whether the services claim to be healthy.

Stdlib only, so it keeps working when the container does not.

    watchdog.py --config /etc/qqai-watchdog.json [--dry-run] [--force-notify]
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


DEFAULT_CONFIG_PATH = "/etc/qqai-watchdog.json"

DEFAULTS: dict[str, Any] = {
    "database": "/var/lib/qqai-state/collector_state.sqlite3",
    "image_root": "/mnt/disk-1/qq-ai-image-collector/repository",
    "napcat_container": "qqai-napcat",
    "console_container": "qqai-collector-console",
    "state_file": "/var/lib/qqai-watchdog-state.json",
    # A heartbeat is written every ten seconds, so a minute of silence is
    # already far outside normal.
    "heartbeat_max_age_seconds": 180,
    # Silence on the socket means the transport itself died.  NapCat sends an
    # OneBot heartbeat every 30s whether or not the account is signed in, so
    # this threshold is about the connection, never about whether data flows.
    "no_event_warn_seconds": 3600,
    "no_event_critical_seconds": 10800,
    # Group traffic is the thing the collector exists to receive.  Over the 601
    # hours before the 2026-09-14 outage the quietest hour still carried two
    # group messages and no hour carried none, so an hour of true silence is
    # already outside anything this account has ever done.
    "no_message_warn_seconds": 3600,
    "no_message_critical_seconds": 10800,
    "disk_warn_percent": 85,
    "disk_critical_percent": 93,
    # Repeat interval while a problem persists.  A first push always goes out
    # on the spot; this only governs the nagging afterwards, and its real job is
    # covering a push that never arrived.  Four days of silence in September
    # were not caused by these intervals, but twelve hours between reminders was
    # far too long to be the backstop for a dropped notification.
    "notify_cooldown_seconds": 1800,
    "warn_cooldown_seconds": 14400,
    # A disk filling over days is the one thing an operator legitimately parks
    # until the weekend, and it was explicitly asked to stop shouting hourly.
    # Everything else gets the interval above.
    "quiet_check_prefixes": ["disk_", "gallery_storage", "archive_slow", "download_stuck", "group_silence"],
    "quiet_cooldown_seconds": 43200,
    "recovery_cooldown_seconds": 3600,
    # The gallery's server maintenance writes this after every run.  It is what
    # gives new images a thumbnail and the privacy approval that lets anyone
    # open them, and on 2026-09-23 it hit its storage cap and stopped for three
    # days with nothing but a JSON field to say so.  A missing file means the
    # gallery is not installed on this host, and the check stays out of the way.
    "gallery_maintenance_report": "/var/lib/enter-gallery/maintenance-latest.json",
    # Runs every six hours and may take up to 45 minutes.
    "gallery_maintenance_warn_seconds": 8 * 3600,
    "gallery_maintenance_critical_seconds": 24 * 3600,
    # A run that finishes normally leaves a handful pending at most; hundreds
    # means images are failing one by one without stopping the run.
    "gallery_pending_warn": 200,
    # Images the last run could not approve; one oversized file is expected.
    "gallery_blocked_warn": 10,
    "gallery_storage_warn_percent": 90,
    # New images reach the site only after the archive publishes its index.
    # If no maintenance run has started since the archive last finished, they
    # are on the site without thumbnails; allow a run this long to get going.
    "archive_log": "/opt/qqai-archive.log",
    "gallery_after_archive_grace_seconds": 3600,
    # The archive starts at 04:30 and the 05:00 maintenance run waits for it
    # for an hour, so past about 90 minutes the morning run is skipped.  It
    # crept there a minute a day through October and skipped one on 2026-10-26
    # before anything said so; a normal run takes a few minutes.
    "archive_warn_seconds": 3600,
    # History recovery gaps that are known and accepted as permanent, as
    # "<not_before>:<group_id>".  Anything else a recovery run left partial or
    # failed is reported until it is recovered or listed here.
    "recovery_acknowledged": [],
    # The ceiling history recovery runs are approved for, in calls per day
    # (60 an hour).  The live collector keeps its own 300 a day.
    "recovery_daily_calls": 1440,
    # The live collector's daily history ceiling when its database carries no
    # history_daily_limit override; it matches the collector's own default.
    "history_daily_limit": 300,
    # How long NapCat may keep failing history calls before it is a warning.
    # The collector pauses refreshes meanwhile, so images wait, not vanish.
    "history_failing_warn_seconds": 6 * 3600,
    # Images still queued after this long, and images given up on in a day.
    # October averaged about one CDN failure a day; ten means something broke.
    "download_stuck_seconds": 3 * 86400,
    "download_lost_warn_per_day": 10,
    # How long an enabled group may go without a message before it is
    # reported: two days for one that averaged an image a day over the past
    # month, two weeks for a quieter one.  Groups known to be gone are listed
    # in group_silence_acknowledged; private_groups are named only generically.
    "group_silence_busy_seconds": 2 * 86400,
    "group_silence_quiet_seconds": 14 * 86400,
    "group_silence_acknowledged": [],
    "private_groups": [],
    # Restarting NapCat re-launches QQ.  Off by default: turning it on is a
    # decision about whether an unattended restart is preferable to waiting for
    # a person, and that depends on whether the session can re-login on its own.
    "auto_restart_napcat": False,
    "notify": {
        # Fill one of these in.  Examples:
        #   Bark:       {"url": "https://api.day.app/<key>/{title}/{body}"}
        #   Telegram:   {"url": "https://api.telegram.org/bot<token>/sendMessage",
        #                "method": "POST",
        #                "json": {"chat_id": "<id>", "text": "{title}\n{body}"}}
        #   ServerChan: {"url": "https://sctapi.ftqq.com/<key>.send",
        #                "method": "POST",
        #                "form": {"title": "{title}", "desp": "{body}"}}
        "url": "",
        "method": "GET",
        "timeout_seconds": 15,
    },
}

OK = "ok"
WARN = "warn"
CRITICAL = "critical"
RANK = {OK: 0, WARN: 1, CRITICAL: 2}

# The collector's calendar days, and so its daily history budget, run in UTC+8.
COLLECTOR_TIMEZONE = dt.timezone(dt.timedelta(hours=8))


class Finding:
    __slots__ = ("key", "level", "message")

    def __init__(self, key: str, level: str, message: str) -> None:
        self.key = key
        self.level = level
        self.message = message

    def __repr__(self) -> str:  # pragma: no cover - diagnostic only
        return f"<{self.level} {self.key}: {self.message}>"


def load_config(path: Path) -> dict[str, Any]:
    config = json.loads(json.dumps(DEFAULTS))
    if path.is_file():
        supplied = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(supplied, dict):
            raise ValueError("watchdog configuration must be a JSON object")
        notify = dict(config["notify"])
        notify.update(supplied.pop("notify", {}) or {})
        config.update(supplied)
        config["notify"] = notify
    return config


def read_state(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def write_state(path: Path, state: dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
        os.replace(temporary, path)
    except OSError as exc:
        print(f"warning: could not persist watchdog state: {exc}", file=sys.stderr)


def _runtime_state(connection: sqlite3.Connection, key: str) -> dict[str, Any]:
    row = connection.execute(
        "SELECT value_json FROM runtime_state WHERE key=?", (key,)
    ).fetchone()
    if not row:
        return {}
    try:
        value = json.loads(str(row[0]))
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _docker(*args: str, timeout: int = 30) -> tuple[int, str]:
    if shutil.which("docker") is None:
        return 127, "docker not available"
    try:
        result = subprocess.run(
            ["docker", *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        return 1, f"{type(exc).__name__}: {exc}"
    return result.returncode, (result.stdout or result.stderr or "").strip()


def check_qq_alive(config: dict[str, Any]) -> Finding:
    """The 2026-08-13 check.

    QQ runs as a grandchild of the container's init, so the kernel can kill it
    without the container exiting and without the restart policy noticing.
    """

    container = str(config["napcat_container"])
    code, output = _docker(
        "inspect", "--format", "{{.State.Running}}", container, timeout=20
    )
    if code != 0:
        return Finding("qq", CRITICAL, f"NapCat 容器状态未知：{output[:200]}")
    if output.strip().casefold() != "true":
        return Finding("qq", CRITICAL, f"NapCat 容器未运行（{output.strip()}）")
    code, output = _docker("exec", container, "pgrep", "-x", "qq", timeout=25)
    if code != 0:
        return Finding(
            "qq",
            CRITICAL,
            "NapCat 容器在运行，但里面的 QQ 进程不见了。"
            "容器不会自行退出，重启策略也不会触发 —— 这正是 8-13 那次静默断采。",
        )
    return Finding("qq", OK, "QQ 进程在")


def check_heartbeats(connection: sqlite3.Connection, config: dict[str, Any]) -> list[Finding]:
    findings: list[Finding] = []
    now = int(time.time())
    limit = int(config["heartbeat_max_age_seconds"])
    for key, label in (("worker", "采集 Worker"), ("event_stream", "事件流")):
        state = _runtime_state(connection, key)
        beat = int(state.get("heartbeat_at") or 0)
        if beat <= 0:
            findings.append(Finding(f"hb_{key}", CRITICAL, f"{label} 从未写过心跳"))
            continue
        age = now - beat
        if age > limit:
            findings.append(
                Finding(f"hb_{key}", CRITICAL, f"{label} 心跳停了 {age} 秒")
            )
        else:
            findings.append(Finding(f"hb_{key}", OK, f"{label} 心跳 {age} 秒"))
    stream = _runtime_state(connection, "event_stream")
    if not stream.get("connected"):
        findings.append(
            Finding(
                "ws",
                CRITICAL,
                f"事件 WebSocket 未连接：{str(stream.get('last_error') or '未知原因')[:200]}",
            )
        )
    else:
        findings.append(Finding("ws", OK, "事件 WebSocket 已连接"))
    return findings


def check_intake(connection: sqlite3.Connection, config: dict[str, Any]) -> list[Finding]:
    """Is group traffic still arriving?

    This used to watch `last_event_at` and call it "no group messages".  It is
    not: the collector stamps that field on every frame the WebSocket delivers,
    and NapCat keeps sending an OneBot heartbeat every 30 seconds while the QQ
    account is signed out.  On 2026-09-14 the account dropped at about 12:40 and
    the counters recorded exactly 120 events an hour - heartbeat and nothing
    else - with zero group messages for three days, while every check here,
    including this one, reported healthy.

    So the two are measured apart now: one finding for the socket being alive,
    and one for messages actually coming down it.
    """

    now = int(time.time())
    findings: list[Finding] = []

    stream = _runtime_state(connection, "event_stream")
    last_event = int(stream.get("last_event_at") or 0)
    if last_event <= 0:
        findings.append(Finding("stream", WARN, "事件流尚未收到过任何帧"))
    else:
        age = now - last_event
        if age >= int(config["no_event_critical_seconds"]):
            findings.append(Finding("stream", CRITICAL, f"已经 {age // 60} 分钟连心跳都没有，事件流已断"))
        elif age >= int(config["no_event_warn_seconds"]):
            findings.append(Finding("stream", WARN, f"{age // 60} 分钟没有任何帧（含心跳）"))
        else:
            findings.append(Finding("stream", OK, f"事件流 {age} 秒前还在收帧（含心跳）"))

    row = connection.execute(
        "SELECT MAX(bucket_start) FROM hourly_counters WHERE group_messages > 0"
    ).fetchone()
    last_bucket = int(row[0]) if row and row[0] else 0
    if last_bucket <= 0:
        findings.append(Finding("messages", WARN, "从未统计到群消息"))
        return findings

    # Counters are hourly buckets; the newest one with traffic may still be
    # filling, so silence is measured from the end of that hour.
    quiet = now - (last_bucket + 3600)
    if quiet >= int(config["no_message_critical_seconds"]):
        findings.append(Finding(
            "messages",
            CRITICAL,
            f"已经 {quiet // 3600} 小时没有任何群消息，心跳却正常——多半是 QQ 账号掉线",
        ))
    elif quiet >= int(config["no_message_warn_seconds"]):
        findings.append(Finding("messages", WARN, f"{quiet // 60} 分钟没有群消息"))
    else:
        findings.append(Finding("messages", OK, "群消息仍在进来"))
    return findings


def check_disks(config: dict[str, Any]) -> list[Finding]:
    findings: list[Finding] = []
    targets = {
        "images": str(config["image_root"]),
        "state": str(Path(str(config["database"])).parent),
    }
    warn = float(config["disk_warn_percent"])
    critical = float(config["disk_critical_percent"])
    for key, path in targets.items():
        try:
            usage = shutil.disk_usage(path)
        except OSError as exc:
            findings.append(Finding(f"disk_{key}", WARN, f"{path} 无法读取：{exc}"))
            continue
        used_percent = 100.0 * (usage.total - usage.free) / max(1, usage.total)
        free_gb = usage.free / (1024**3)
        detail = f"{path} 已用 {used_percent:.0f}%，剩 {free_gb:.1f} GB"
        if used_percent >= critical:
            # A full volume does not crash the worker; writes fail, images get
            # deferred with a growing backoff and everything keeps reporting
            # healthy.  That silence is the reason this is critical early.
            findings.append(Finding(f"disk_{key}", CRITICAL, detail))
        elif used_percent >= warn:
            findings.append(Finding(f"disk_{key}", WARN, detail))
        else:
            findings.append(Finding(f"disk_{key}", OK, detail))
    return findings


def _last_archive_run(path: Path) -> tuple[float | None, float | None]:
    """When the last finished archive run started and ended, from its own log."""
    try:
        with path.open("rb") as handle:
            handle.seek(max(0, path.stat().st_size - 65536))
            tail = handle.read().decode("utf-8", errors="replace")
    except OSError:
        return None, None
    ended = None
    for line in reversed(tail.splitlines()):
        match = re.match(r"===== (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (开始|结束)", line)
        if not match:
            continue
        stamp = time.mktime(time.strptime(match.group(1), "%Y-%m-%d %H:%M:%S"))
        # A run still in progress has a start and no end yet; skip past it.
        if ended is None:
            if match.group(2) == "结束":
                ended = stamp
        elif match.group(2) == "开始":
            return stamp, ended
    return None, ended


def check_gallery(config: dict[str, Any]) -> list[Finding]:
    path = Path(str(config.get("gallery_maintenance_report") or ""))
    if not str(path) or not path.is_file():
        return []
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
        finished = dt.datetime.fromisoformat(str(report["finishedAt"]).replace("Z", "+00:00"))
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return [Finding("gallery", WARN, f"图库维护报告读不了：{type(exc).__name__}")]

    findings: list[Finding] = []
    age = time.time() - finished.timestamp()
    pending = int(report.get("thumbnailPending") or 0)
    stopped = str(report.get("stopped") or "")
    hours = age / 3600
    if stopped:
        # The run itself says it gave up.  New images stay without a thumbnail
        # and without approval until someone changes a limit, so this does not
        # get better by waiting.
        findings.append(Finding(
            "gallery", CRITICAL,
            f"图库维护被 {stopped} 挡住，{pending} 张新图没有缩略图也打不开",
        ))
    elif age >= int(config["gallery_maintenance_critical_seconds"]):
        findings.append(Finding("gallery", CRITICAL, f"图库维护 {hours:.0f} 小时没有完成过"))
    elif age >= int(config["gallery_maintenance_warn_seconds"]):
        findings.append(Finding("gallery", WARN, f"图库维护 {hours:.0f} 小时没有完成过"))
    elif pending >= int(config["gallery_pending_warn"]):
        findings.append(Finding(
            "gallery", WARN,
            f"图库维护跑完了但还有 {pending} 张缺缩略图，失败 {report.get('failed', 0)} 张",
        ))
    else:
        findings.append(Finding("gallery", OK, f"图库维护 {hours:.1f} 小时前完成，缺缩略图 {pending} 张"))

    # An image in the catalog without a privacy approval is listed on the site
    # and cannot be opened.  Right after a run that is only what failed it:
    # quarantined until the processing policy changes, or deferred for retry.
    try:
        blocked = int(report["catalog"]) - int(report["approvalsPublished"])
    except (KeyError, TypeError, ValueError):
        blocked = 0
    if blocked >= int(config["gallery_blocked_warn"]):
        reasons = "、".join(f"{code} {count}" for code, count in (report.get("failureSummary") or {}).items())
        findings.append(Finding(
            "gallery_blocked", WARN,
            f"图库有 {blocked} 张图没通过隐私检查，站上打不开" + (f"（{reasons}）" if reasons else ""),
        ))

    # On late-September mornings the 05:00 run read the index before the
    # archive finished writing it, so every run looked clean while the newest
    # images sat on the site without thumbnails until 11:00.  Maintenance now
    # waits for the archive; this catches it if that ever stops holding.
    began, archived = _last_archive_run(Path(str(config.get("archive_log") or "")))
    try:
        started = dt.datetime.fromisoformat(str(report["startedAt"]).replace("Z", "+00:00")).timestamp()
    except (KeyError, ValueError, TypeError):
        started = 0.0
    grace = int(config["gallery_after_archive_grace_seconds"])
    if archived and started and archived > started and time.time() - archived > grace:
        findings.append(Finding(
            "gallery_order", WARN,
            f"归档 {time.strftime('%m-%d %H:%M', time.localtime(archived))} 写完后还没有图库维护跑过，新图没有缩略图",
        ))
    if began and archived and archived - began >= int(config["archive_warn_seconds"]):
        findings.append(Finding(
            "archive_slow", WARN,
            f"归档 {time.strftime('%m-%d', time.localtime(began))} 用了 {(archived - began) / 60:.0f} 分钟"
            f"（{time.strftime('%H:%M', time.localtime(began))}–{time.strftime('%H:%M', time.localtime(archived))}），"
            "早上的图库维护最多等到 6 点左右，再慢就会跳过那一轮",
        ))

    used = report.get("retainedBytesAfter")
    limit = report.get("storageLimit")
    if isinstance(used, int) and isinstance(limit, int) and limit > 0:
        percent = 100.0 * used / limit
        detail = f"图库衍生存储 {used / 1e9:.2f} / {limit / 1e9:.0f} GB（{percent:.0f}%）"
        warn = float(config["gallery_storage_warn_percent"])
        findings.append(Finding("gallery_storage", WARN if percent >= warn else OK, detail))
    return findings


def check_disabled_groups(connection: sqlite3.Connection, config: dict[str, Any]) -> list[Finding]:
    """A group switched off for a reason that has since gone away.

    627217018 was disabled on 8/17 because the account then collecting was not
    in it.  The account joined later and the group ran for weeks with every
    message reaching QQ and none reaching the collector, which drops events
    from disabled groups without a trace.  NapCat's own log still shows them.
    """
    disabled = {
        str(group_id): str(name or "")
        for group_id, name in connection.execute(
            "SELECT group_id, display_name FROM monitored_groups WHERE enabled=0"
        ).fetchall()
    }
    if not disabled:
        return [Finding("disabled_groups", OK, "没有停用的群")]
    if shutil.which("docker") is None:
        return []
    try:
        result = subprocess.run(
            ["docker", "logs", "--since", "1h", str(config["napcat_container"])],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, errors="replace", timeout=60, check=False,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        return [Finding("disabled_groups", WARN, f"读不了 NapCat 日志，没法核对停用的群：{type(exc).__name__}")]
    received: dict[str, int] = {}
    for group_id in re.findall(r"接收 <- 群聊 \[[^\]]*\((\d+)\)\]", result.stdout or ""):
        if group_id in disabled:
            received[group_id] = received.get(group_id, 0) + 1
    if not received:
        return [Finding("disabled_groups", OK, f"{len(disabled)} 个停用的群过去一小时没有消息")]
    detail = "；".join(
        f"{disabled[group_id] or group_id}（{group_id}）{count} 条"
        for group_id, count in sorted(received.items(), key=lambda item: -item[1])
    )
    return [Finding("disabled_groups", WARN, f"停用的群还在收消息，要不要重新启用：{detail}")]


def check_recovery_gaps(connection: sqlite3.Connection, config: dict[str, Any]) -> list[Finding]:
    """A recovery that stopped short is data that will not come back on its own.

    The September run ended "finished" with two groups at their per-group call
    limit, and the only trace was a status in a table.  QQ keeps group history
    for a limited time, so a gap nobody hears about is a gap that becomes
    permanent.
    """
    try:
        rows = connection.execute(
            """
            SELECT group_id, not_before, not_after, status, next_anchor_time, messages_in_window
            FROM window_recovery_jobs
            WHERE status LIKE 'partial%' OR status LIKE 'failed%'
            ORDER BY not_before, group_id
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return []  # no recovery has ever been run on this database
    acknowledged = {str(value) for value in config.get("recovery_acknowledged") or []}
    stamp = lambda value: time.strftime("%m-%d %H:%M", time.localtime(int(value)))
    gaps = []
    for group_id, not_before, not_after, status, anchor_time, in_window in rows:
        if f"{not_before}:{group_id}" in acknowledged:
            continue
        if not in_window:
            # Images an earlier run found in the same window say nothing about
            # how far this one got; on 2026-10-28 they turned a replay that
            # never arrived into "09-15 21:27 到 09-15 21:27 没翻到".
            gaps.append(f"群 {group_id}（{status}）{stamp(not_before)} 到 {stamp(not_after)} 一页都没翻进去")
            continue
        # Only images the recovery itself paged through say how deep it got;
        # live images from the edges of the window would hide a hole between.
        deepest = connection.execute(
            """
            SELECT MIN(sent_at) FROM images
            WHERE group_id=? AND sent_at BETWEEN ? AND ?
              AND resolver_json LIKE '%window-recovery%'
            """,
            (group_id, not_before, not_after),
        ).fetchone()[0]
        reached = min(int(v) for v in (anchor_time, deepest, not_after) if v)
        gaps.append(f"群 {group_id}（{status}）{stamp(not_before)} 到 {stamp(reached)} 没翻到")
    if not gaps:
        return [Finding("recovery", OK, "历史回捞没有未补完的群")]
    return [Finding("recovery", WARN, f"历史回捞有 {len(gaps)} 个群没补完：" + "；".join(gaps))]


# Prefix of the error recorded when an operator gives up on images by hand.
OPERATOR_CLOSED = "closed by operator"


def check_download_outcomes(connection: sqlite3.Connection, config: dict[str, Any]) -> list[Finding]:
    """Images found but never stored: stuck in the queue, or given up on.

    502 images from the September recovery sat "deferred" for a month, each
    waiting for a page replay that nothing was going to run, while the
    recovery itself reported success.  A queued image is normally stored
    within minutes and a backlog clears within a day, so anything still
    queued after days is not going to resolve by waiting.
    """
    now = time.time()
    stuck_seconds = int(config["download_stuck_seconds"])
    count, oldest, attempts, error = connection.execute(
        """
        SELECT count(*), min(discovered_at), max(attempts),
               (SELECT substr(coalesce(error, ''), 1, 60) FROM images
                WHERE status IN ('queued','deferred','downloading') AND discovered_at < ?
                GROUP BY 1 ORDER BY count(*) DESC LIMIT 1)
        FROM images
        WHERE status IN ('queued','deferred','downloading') AND discovered_at < ?
        """,
        (now - stuck_seconds, now - stuck_seconds),
    ).fetchone()
    findings: list[Finding] = []
    if count:
        findings.append(Finding(
            "download_stuck", WARN,
            f"{count} 张图排队超过 {stuck_seconds // 86400} 天还没下载下来"
            f"（最早 {time.strftime('%m-%d', time.localtime(int(oldest)))} 发现，重试最多 {attempts} 次"
            + (f"，多为：{error}" if error else "") + "）",
        ))
    # Images an operator has written off were decided on, not lost; counting
    # them turned the closing of 413 unrecoverable images into a day of alarms.
    lost = connection.execute(
        """
        SELECT count(*) FROM images
        WHERE status IN ('failed_terminal','expired') AND updated_at > ?
          AND coalesce(error, '') NOT LIKE ?
        """,
        (now - 86400, OPERATOR_CLOSED + "%"),
    ).fetchone()[0]
    level = WARN if lost >= int(config["download_lost_warn_per_day"]) else OK
    findings.append(Finding("download_lost", level, f"24 小时内放弃下载 {lost} 张图"))
    return findings


def check_group_silence(connection: sqlite3.Connection, config: dict[str, Any]) -> list[Finding]:
    """Enabled groups that have stopped delivering messages.

    In October six enabled groups had gone quiet for one to eight weeks with
    nothing said: one had dropped the account from its member list, and the
    rest were still listed while not one of their messages reached NapCat.
    Nothing downstream can tell a quiet group from a lost one, so this weighs
    each group's silence against how busy it was over the past month.
    """
    now = time.time()
    acknowledged = {str(value) for value in config.get("group_silence_acknowledged") or []}
    private = {str(value) for value in config.get("private_groups") or []}
    rows = connection.execute(
        """
        SELECT g.group_id, coalesce(r.last_message_time, 0), coalesce(g.created_at, 0)
        FROM monitored_groups g LEFT JOIN group_runtime r ON r.group_id = g.group_id
        WHERE g.enabled = 1
        """
    ).fetchall()
    silent = []
    for group_id, last_message, added in rows:
        if str(group_id) in acknowledged:
            continue
        images = connection.execute(
            "SELECT count(*) FROM images WHERE group_id=? AND sent_at > ?",
            (group_id, now - 30 * 86400),
        ).fetchone()[0]
        # About one image a day or more counts as busy.
        busy = images >= 30
        limit = int(config["group_silence_busy_seconds" if busy else "group_silence_quiet_seconds"])
        # A group added an hour ago has not been silent since 1970.
        quiet_for = now - max(int(last_message), int(added))
        if quiet_for >= limit:
            label = "不公开的群" if str(group_id) in private else f"群 {group_id}"
            if last_message:
                silent.append((quiet_for, f"{label} {quiet_for / 86400:.0f} 天没有消息"))
            else:
                silent.append((quiet_for, f"{label} 加入监控 {quiet_for / 86400:.0f} 天一直没有消息"))
    known = sum(1 for row in rows if str(row[0]) in acknowledged)
    if not silent:
        return [Finding(
            "group_silence", OK,
            f"{len(rows) - known} 个启用的群都在正常收消息" + (f"，另有 {known} 个已知静默" if known else ""),
        )]
    silent.sort(reverse=True)
    return [Finding(
        "group_silence", WARN,
        f"{len(silent)} 个启用的群异常安静（可能被踢或收不到推送）：" + "；".join(text for _, text in silent),
    )]


def _history_daily_limit(connection: sqlite3.Connection, config: dict[str, Any]) -> int:
    """The collector's daily ceiling: its app_settings override, else ours."""
    try:
        row = connection.execute(
            "SELECT value_json FROM app_settings WHERE key='history_daily_limit'"
        ).fetchone()
        value = int(json.loads(str(row[0]))) if row else 0
    except (sqlite3.OperationalError, TypeError, ValueError):
        value = 0
    return value if value > 0 else int(config.get("history_daily_limit", 300))


def check_history_breaker(connection: sqlite3.Connection, config: dict[str, Any]) -> list[Finding]:
    """Say why image refreshes are waiting when NapCat keeps failing history calls.

    The collector pauses all history calls for an hour once NapCat fails a few
    in a row, then probes with one.  Images wait rather than being given up,
    so a pause that clears within hours is normal - NapCat failed every call
    from 02:00 to 07:40 on 2026-11-17.  Only failures that keep coming for longer are a warning;
    the span is measured between real failures, so a pause nobody has probed
    yet does not grow into one by itself.
    """
    breaker = _runtime_state(connection, "history_breaker")
    failures = int(breaker.get("failures") or 0)
    threshold = int(breaker.get("threshold") or 0)
    if not threshold or failures < threshold:
        return []
    stamp = lambda value: time.strftime("%m-%d %H:%M", time.localtime(int(value)))
    first = int(breaker.get("first_failed_at") or 0)
    last = int(breaker.get("last_failed_at") or first)
    until = int(breaker.get("paused_until") or 0)
    waiting = f"暂停到 {stamp(until)}" if until > time.time() else "等下一次探测"
    error = str(breaker.get("last_error") or "")[:120]
    message = (
        f"NapCat 历史接口自 {stamp(first)} 起连续 {failures} 次出错，图片链接刷新{waiting}"
        + (f"（最近：{error}）" if error else "")
    )
    level = WARN if last - first >= int(config["history_failing_warn_seconds"]) else OK
    return [Finding("history_paused", level, message)]


def check_policy_alarms(connection: sqlite3.Connection, config: dict[str, Any]) -> list[Finding]:
    findings: list[Finding] = []
    alarm = _runtime_state(connection, "critical_alarm")
    if alarm.get("active"):
        findings.append(
            Finding(
                "policy_alarm",
                CRITICAL,
                f"策略告警：{str(alarm.get('reason') or 'unknown')[:200]}",
            )
        )
    else:
        findings.append(Finding("policy_alarm", OK, "无策略告警"))
    now = int(time.time())
    blocked = int(connection.execute(
        "SELECT coalesce(sum(get_image_blocked), 0) FROM hourly_counters WHERE bucket_start>=?",
        (now - 86400,),
    ).fetchone()[0] or 0)
    # The collector holds history calls to its daily limit per calendar day in
    # UTC+8, so this counts from the same midnight.  A rolling 24 hours added
    # the previous evening to a day that stopped exactly at its limit: on
    # 2026-11-17 the collector halted at 300 by 07:40 and this reported "320,
    # over quota" every five minutes until 19:00 although the ceiling had held.
    day_start = int(
        dt.datetime.now(COLLECTOR_TIMEZONE)
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .timestamp()
    )
    row = connection.execute(
        "SELECT coalesce(sum(history_calls), 0), coalesce(sum(window_history_calls), 0) "
        "FROM hourly_counters WHERE bucket_start>=?",
        (day_start,),
    ).fetchone()
    total = int(row[0] or 0)
    # Recovery calls are counted in both columns.  They run under their own
    # operator-approved ceiling (60 an hour, 1,440 a day), so holding them to
    # the live collector's 300 a day warned every five minutes for the whole of
    # every approved recovery - early October's pushes were mostly this.
    history = total - int(row[1] or 0)
    limit = _history_daily_limit(connection, config)
    # Counted from the call log rather than hourly buckets: ">= 24 hours ago"
    # takes in 25 buckets, which put a recovery running exactly at its ceiling
    # 14 calls over it.
    try:
        recovery = int(connection.execute(
            "SELECT count(*) FROM window_recovery_calls WHERE called_at>?",
            (int(time.time()) - 86400,),
        ).fetchone()[0])
    except sqlite3.OperationalError:
        recovery = 0
    if blocked:
        findings.append(
            Finding(
                "get_image",
                CRITICAL,
                f"24 小时内有 {blocked} 次 get_image 调用被拦截 —— 有代码路径在碰禁用接口",
            )
        )
    else:
        findings.append(Finding("get_image", OK, "get_image 拦截计数为 0"))
    # The collector refuses a call once the day's total reaches the limit, so
    # only a broken ceiling can take the live count past it.  Reaching it is
    # reported but is the ceiling working: refreshes wait for midnight.
    if history > limit:
        findings.append(Finding(
            "history", WARN,
            f"今日历史调用 {history} 次，超过采集器日配额 {limit}——额度限制没有拦住",
        ))
    elif total >= limit:
        findings.append(Finding(
            "history", OK,
            f"今日账号会话历史调用 {history}/{limit} 次，额度已用完，图片链接刷新等到 0 点",
        ))
    else:
        findings.append(Finding("history", OK, f"今日账号会话历史调用 {history}/{limit} 次"))
    findings.extend(check_history_breaker(connection, config))
    if recovery:
        findings.append(
            Finding("recovery_calls", OK, f"24 小时历史回捞调用 {recovery} 次")
            if recovery <= int(config.get("recovery_daily_calls", 1440))
            else Finding("recovery_calls", WARN, f"24 小时历史回捞调用 {recovery} 次，超过批准的每天 {config.get('recovery_daily_calls', 1440)} 次")
        )
    return findings


def notify(config: dict[str, Any], title: str, body: str) -> bool:
    settings = config.get("notify") or {}
    url_template = str(settings.get("url") or "").strip()
    if not url_template:
        print("notify: no webhook configured, skipping push", file=sys.stderr)
        return False
    timeout = int(settings.get("timeout_seconds") or 15)
    method = str(settings.get("method") or "GET").upper()

    def fill(value: Any, *, quote: bool) -> Any:
        if isinstance(value, str):
            title_value = urllib.parse.quote(title, safe="") if quote else title
            body_value = urllib.parse.quote(body, safe="") if quote else body
            return value.replace("{title}", title_value).replace("{body}", body_value)
        if isinstance(value, dict):
            return {key: fill(item, quote=quote) for key, item in value.items()}
        return value

    url = fill(url_template, quote=True)
    data: bytes | None = None
    headers = {"User-Agent": "qqai-watchdog/1"}
    if settings.get("json") is not None:
        data = json.dumps(fill(settings["json"], quote=False), ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
        method = "POST"
    elif settings.get("form") is not None:
        data = urllib.parse.urlencode(fill(settings["form"], quote=False)).encode("utf-8")
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        method = "POST"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            reply = response.read(2048)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        print(f"notify failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return False
    # Push services answer 200 to a message they refuse and say so in the body:
    # ServerChan with a non-zero "code" (its free tier caps messages a day),
    # Telegram with "ok": false.  Bark reports success as code 200.  Taking the
    # status alone as delivered marks an alert sent that never arrived.
    try:
        answer = json.loads(reply.decode("utf-8", errors="replace"))
    except ValueError:
        answer = None
    if isinstance(answer, dict):
        code = answer.get("code", 0)
        if answer.get("ok") is False or (isinstance(code, int) and code not in (0, 200)):
            reason = answer.get("message") or answer.get("description") or answer
            print(f"notify rejected: {str(reason)[:200]}", file=sys.stderr)
            return False
    return True


def maybe_restart_napcat(
    config: dict[str, Any],
    state: dict[str, Any],
    findings: list[Finding],
    *,
    dry_run: bool,
) -> str | None:
    if not bool(config.get("auto_restart_napcat")):
        return None
    dead = next((f for f in findings if f.key == "qq" and f.level == CRITICAL), None)
    if dead is None:
        return None
    now = int(time.time())
    last = int(state.get("napcat_restarted_at") or 0)
    cooldown = int(config["recovery_cooldown_seconds"])
    if now - last < cooldown:
        return f"跳过自动重启：距上次仅 {now - last} 秒（冷却 {cooldown} 秒）"
    if dry_run:
        return "dry-run：本应重启 NapCat 容器"
    code, output = _docker("restart", str(config["napcat_container"]), timeout=120)
    state["napcat_restarted_at"] = now
    if code == 0:
        return "已自动重启 NapCat 容器以拉起 QQ"
    return f"自动重启 NapCat 失败：{output[:200]}"


def run_checks(config: dict[str, Any]) -> list[Finding]:
    findings: list[Finding] = []
    database = Path(str(config["database"]))
    if not database.is_file():
        findings.append(Finding("database", CRITICAL, f"数据库不存在：{database}"))
    else:
        # Read-only and short-lived: the watchdog must never be the thing that
        # blocks a writer on the database it is supposed to be watching.
        connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=20)
        try:
            connection.execute("PRAGMA busy_timeout=15000")
            findings.extend(check_heartbeats(connection, config))
            findings.extend(check_intake(connection, config))
            findings.extend(check_policy_alarms(connection, config))
            findings.extend(check_recovery_gaps(connection, config))
            findings.extend(check_download_outcomes(connection, config))
            findings.extend(check_group_silence(connection, config))
            findings.extend(check_disabled_groups(connection, config))
        finally:
            connection.close()
    findings.append(check_qq_alive(config))
    findings.extend(check_disks(config))
    findings.extend(check_gallery(config))
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path(DEFAULT_CONFIG_PATH))
    parser.add_argument("--dry-run", action="store_true", help="检查并打印，不推送也不自愈")
    parser.add_argument("--force-notify", action="store_true", help="忽略冷却，强制推送一次")
    args = parser.parse_args()

    config = load_config(args.config)
    state_path = Path(str(config["state_file"]))
    state = read_state(state_path)

    try:
        findings = run_checks(config)
    except Exception as exc:  # a crashed watchdog must still be able to shout
        findings = [Finding("watchdog", CRITICAL, f"巡检自身异常：{type(exc).__name__}: {exc}")]

    worst = max((RANK[f.level] for f in findings), default=0)
    problems = [f for f in findings if f.level != OK]
    problems.sort(key=lambda f: RANK[f.level], reverse=True)

    recovery = maybe_restart_napcat(config, state, findings, dry_run=args.dry_run)

    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
    for finding in findings:
        print(f"{stamp} [{finding.level:8}] {finding.key}: {finding.message}")
    if recovery:
        print(f"{stamp} [recovery] {recovery}")

    now = int(time.time())
    signature = "|".join(f"{f.key}:{f.level}" for f in problems)
    changed = signature != str(state.get("last_signature") or "")
    def repeat_interval(finding: Finding) -> int:
        quiet = tuple(config.get("quiet_check_prefixes") or ())
        if finding.level != CRITICAL and finding.key.startswith(quiet):
            return int(config.get("quiet_cooldown_seconds", config["warn_cooldown_seconds"]))
        if finding.level == CRITICAL:
            return int(config["notify_cooldown_seconds"])
        return int(config.get("warn_cooldown_seconds", config["notify_cooldown_seconds"]))

    # The most urgent problem sets the pace, so a parked disk warning cannot
    # slow down the reminder about a dead collector sitting next to it.
    cooldown = min((repeat_interval(f) for f in problems), default=int(config["notify_cooldown_seconds"]))
    # `changed` still bypasses this, so a warn escalating to critical pushes at
    # once rather than waiting out the gentler interval it was throttled by.
    cooled = now - int(state.get("last_notified_at") or 0) >= cooldown
    should_notify = bool(problems) and (changed or cooled)
    recovered = not problems and bool(state.get("last_signature"))

    # --force-notify has to push even when nothing is wrong.  Its whole purpose
    # is proving the channel works before it is needed, and a healthy system is
    # exactly when someone sets one up - gating it behind "are there problems"
    # made the one command for testing a push silently do nothing.
    if not args.dry_run and (should_notify or recovered or args.force_notify):
        if problems:
            level = CRITICAL if worst == RANK[CRITICAL] else WARN
            title = f"QQ采集 {'严重' if level == CRITICAL else '警告'}：{problems[0].message[:40]}"
            lines = [f"{'✖' if f.level == CRITICAL else '▲'} {f.message}" for f in problems]
        elif recovered:
            title = "QQ采集 已恢复"
            lines = ["之前的告警项目前全部正常。"]
        else:
            title = "QQ采集 巡检正常"
            lines = ["测试推送：所有检查项正常。收到这条就说明告警通道是通的。"]
        if recovery:
            lines.append(f"↻ {recovery}")
        lines.append("")
        lines.extend(f"· {f.message}" for f in findings if f.level == OK)
        if notify(config, title, "\n".join(lines)):
            state["last_notified_at"] = now
            print(f"{stamp} [push    ] {title}")
    state["last_signature"] = signature
    state["last_run_at"] = now
    if not args.dry_run:
        write_state(state_path, state)
    return 2 if worst == RANK[CRITICAL] else (1 if worst == RANK[WARN] else 0)


if __name__ == "__main__":
    raise SystemExit(main())
