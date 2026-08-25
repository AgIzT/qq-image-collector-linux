"""Single-instance PID files that survive container PID reuse.

A container starts numbering processes from 1 again, so a PID file left behind
by a previous container routinely names a PID that exists in the new one and
belongs to something else entirely.  Checking only that the PID is alive then
reports the collector as already running against an unrelated process, which is
why every ``docker compose up -d`` threw

    RuntimeError: collector is already running with PID 8

and burned a start before the restart policy retried into a working one.

Recording the process start time alongside the PID settles it: the kernel's
value for a reused PID is a different number, so a stale file is recognisable
without guessing from command lines.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def pid_start_time(pid: int) -> int | None:
    """Start time in clock ticks since boot, or None where /proc is absent.

    Field 22 of /proc/<pid>/stat.  The comm field can contain spaces and
    parentheses, so everything up to the final ')' is skipped rather than split.
    """

    try:
        with open(f"/proc/{int(pid)}/stat", "rb") as handle:
            raw = handle.read()
    except (OSError, ValueError):
        return None
    close = raw.rfind(b")")
    if close < 0:
        return None
    fields = raw[close + 2 :].split()
    # stat field 22 is the 20th entry after the state field that follows comm.
    if len(fields) < 20:
        return None
    try:
        return int(fields[19])
    except (TypeError, ValueError):
        return None


def _encode(pid: int) -> str:
    started = pid_start_time(pid)
    return f"{pid} {started}" if started is not None else str(pid)


def _decode(text: str) -> tuple[int, int | None]:
    parts = text.strip().split()
    if not parts:
        raise ValueError("empty pid file")
    pid = int(parts[0])
    if len(parts) < 2:
        return pid, None
    try:
        return pid, int(parts[1])
    except ValueError:
        return pid, None


def holder_is_live(text: str) -> int | None:
    """Return the PID still holding this file, or None when it is stale."""

    try:
        pid, started = _decode(text)
    except ValueError:
        return None
    if pid <= 0 or pid == os.getpid() or not pid_is_alive(pid):
        return None
    current = pid_start_time(pid)
    if started is None or current is None:
        # Either the file predates start-time recording or /proc is unavailable.
        # Treat it as stale: this deployment runs one instance per container, so
        # a leftover file is far more likely than a genuine second process, and
        # O_EXCL below still refuses to hand two live processes the same file.
        return None
    return pid if current == started else None


class PidFile:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def __enter__(self) -> "PidFile":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.is_file():
            try:
                text = self.path.read_text(encoding="ascii")
            except OSError:
                text = ""
            holder = holder_is_live(text)
            if holder is not None:
                raise RuntimeError(f"collector is already running with PID {holder}")
            self.path.unlink(missing_ok=True)
        descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "w", encoding="ascii") as handle:
            handle.write(_encode(os.getpid()))
        return self

    def __exit__(self, *_args: Any) -> None:
        try:
            pid, _started = _decode(self.path.read_text(encoding="ascii"))
            if pid == os.getpid():
                self.path.unlink(missing_ok=True)
        except (OSError, ValueError):
            pass
