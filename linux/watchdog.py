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
import json
import os
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
    # Quiet groups genuinely go quiet at night, so silence alone is not a
    # fault.  This only fires alongside a hard signal (QQ gone, stream
    # disconnected) or once it passes the point where every group being quiet
    # stops being plausible.
    "no_event_warn_seconds": 3600,
    "no_event_critical_seconds": 10800,
    "disk_warn_percent": 85,
    "disk_critical_percent": 93,
    # Repeat interval while a problem persists, chosen by severity.  One
    # interval for both meant a slow, known condition - a disk filling over
    # days - pushed as often as a dead QQ, and an alert that repeats hourly
    # about something the operator has already decided to handle on Thursday
    # is how people learn to swipe the channel away.
    "notify_cooldown_seconds": 3600,
    "warn_cooldown_seconds": 43200,
    "recovery_cooldown_seconds": 3600,
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


def check_intake(connection: sqlite3.Connection, config: dict[str, Any]) -> Finding:
    """Are messages still arriving?

    Every service can report healthy while nothing flows, which is what a dead
    QQ looks like from the collector's side, and what a full disk looks like
    once downloads start deferring.  This is the only check that measures the
    thing the system exists to do.
    """

    now = int(time.time())
    stream = _runtime_state(connection, "event_stream")
    last_event = int(stream.get("last_event_at") or 0)
    if last_event <= 0:
        return Finding("intake", WARN, "尚未收到过任何事件")
    age = now - last_event
    if age >= int(config["no_event_critical_seconds"]):
        return Finding(
            "intake",
            CRITICAL,
            f"已经 {age // 60} 分钟没有收到任何群消息事件",
        )
    if age >= int(config["no_event_warn_seconds"]):
        return Finding("intake", WARN, f"{age // 60} 分钟没有新事件")
    return Finding("intake", OK, f"最后一条事件在 {age} 秒前")


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


def check_policy_alarms(connection: sqlite3.Connection) -> list[Finding]:
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
    row = connection.execute(
        "SELECT coalesce(sum(get_image_blocked), 0), coalesce(sum(history_calls), 0) "
        "FROM hourly_counters WHERE bucket_start>=?",
        (int(time.time()) - 86400,),
    ).fetchone()
    blocked = int(row[0] or 0)
    history = int(row[1] or 0)
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
    findings.append(
        Finding("history", OK, f"24 小时账号会话历史调用 {history} 次")
        if history <= 300
        else Finding("history", WARN, f"24 小时历史调用 {history} 次，已超过日配额 300")
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
            response.read(2048)
        return True
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        print(f"notify failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return False


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
            findings.append(check_intake(connection, config))
            findings.extend(check_policy_alarms(connection))
        finally:
            connection.close()
    findings.append(check_qq_alive(config))
    findings.extend(check_disks(config))
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

    for finding in findings:
        print(f"[{finding.level:8}] {finding.key}: {finding.message}")
    if recovery:
        print(f"[recovery] {recovery}")

    now = int(time.time())
    signature = "|".join(f"{f.key}:{f.level}" for f in problems)
    changed = signature != str(state.get("last_signature") or "")
    cooldown = int(
        config["notify_cooldown_seconds"]
        if worst == RANK[CRITICAL]
        else config.get("warn_cooldown_seconds", config["notify_cooldown_seconds"])
    )
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
    state["last_signature"] = signature
    state["last_run_at"] = now
    if not args.dry_run:
        write_state(state_path, state)
    return 2 if worst == RANK[CRITICAL] else (1 if worst == RANK[WARN] else 0)


if __name__ == "__main__":
    raise SystemExit(main())
