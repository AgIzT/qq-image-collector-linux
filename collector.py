"""Compatibility CLI for the event-driven QQ image collector."""

from __future__ import annotations

import argparse
import asyncio
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
from qq_image_collector.pidfile import PidFile, pid_is_alive
from qq_image_collector.database import queue_snapshot
from qq_image_collector.worker import run_worker


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
