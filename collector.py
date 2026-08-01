"""Compatibility CLI for the event-driven QQ image collector."""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
from typing import Any

from qq_image_collector import (
    FINAL_CATEGORIES,
    OneBotClient,
    OneBotError,
    OneBotPolicyError,
    category_for_source,
    connect_database,
)
from qq_image_collector.config import load_settings
from qq_image_collector.database import queue_snapshot
from qq_image_collector.worker import run_worker


def pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


class PidFile:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def __enter__(self) -> "PidFile":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.is_file():
            try:
                existing = int(self.path.read_text(encoding="ascii").strip())
            except (OSError, ValueError):
                existing = 0
            if existing and existing != os.getpid() and pid_is_alive(existing):
                raise RuntimeError(f"collector is already running with PID {existing}")
            self.path.unlink(missing_ok=True)
        descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "w", encoding="ascii") as handle:
            handle.write(str(os.getpid()))
        return self

    def __exit__(self, *_args: Any) -> None:
        try:
            if self.path.read_text(encoding="ascii").strip() == str(os.getpid()):
                self.path.unlink(missing_ok=True)
        except OSError:
            pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="QQ event-driven AI image collector")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("command", choices=("run", "status"), nargs="?", default="run")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(args.config)
    connection = connect_database(settings["storage"]["database"])
    if args.command == "status":
        print(queue_snapshot(connection))
        connection.close()
        return 0
    connection.close()
    pid_file = Path(settings["runtime"]["pid_file"])
    with PidFile(pid_file):
        asyncio.run(run_worker(args.config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
