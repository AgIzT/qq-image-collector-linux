from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from collector_control import get_setting
from qq_image_collector.config import load_settings
from qq_image_collector.database import (
    COUNTER_COLUMNS,
    connect_database,
    counter_sum,
)


REPORT_COUNTERS = (
    "events",
    "image_segments",
    "images_seen",
    "queued_high",
    "queued_medium",
    "queued_low",
    "cdn_requests",
    "cdn_downloads",
    "cdn_bytes",
    "cdn_400",
    "cdn_403",
    "cdn_429",
    "history_calls",
    "get_image_blocked",
    "accepted",
    "rejected",
    "duplicates",
    "failed",
    "expired",
    "filtered_gif",
)


def report(config: Path, hours: int) -> tuple[dict[str, object], bool]:
    settings = load_settings(config)
    connection = connect_database(settings["storage"]["database"])
    try:
        now = int(time.time())
        configured_start = get_setting(connection, "rollout_started_at", None)
        rollout_started_at = int(configured_start or now)
        observation_seconds = max(0, now - rollout_started_at)
        since = max(rollout_started_at, now - max(1, hours) * 3600)
        counters = {
            key: counter_sum(connection, key, since)
            for key in REPORT_COUNTERS
            if key in COUNTER_COLUMNS
        }
        original = [
            {
                "original_flag": "null" if row[0] is None else str(int(row[0])),
                "count": int(row[1]),
            }
            for row in connection.execute(
                """
                SELECT original_flag, count(*) FROM images
                WHERE resolver='event-cdn' AND discovered_at>=?
                GROUP BY original_flag ORDER BY original_flag
                """,
                (rollout_started_at,),
            )
        ]
        original_status = [
            {
                "original_flag": "null" if row[0] is None else str(int(row[0])),
                "status": str(row[1]),
                "count": int(row[2]),
            }
            for row in connection.execute(
                """
                SELECT original_flag, status, count(*) FROM images
                WHERE resolver='event-cdn' AND discovered_at>=?
                GROUP BY original_flag, status ORDER BY original_flag, status
                """,
                (rollout_started_at,),
            )
        ]
    finally:
        connection.close()
    duration_met = observation_seconds >= max(1, hours) * 3600
    gate = duration_met and counters.get("get_image_blocked", 0) == 0
    return (
        {
            "schema": 1,
            "hours": max(1, hours),
            "since": since,
            "rollout_started_at": rollout_started_at if configured_start else None,
            "observation_seconds": observation_seconds,
            "required_observation_seconds": max(1, hours) * 3600,
            "duration_requirement_met": duration_met,
            "generated_at": int(time.time()),
            "counters": counters,
            "original_flag_distribution": original,
            "original_flag_by_status": original_status,
            "steady_state_gate": "pass" if gate else "fail",
            "gate_rule": "get_image_blocked == 0; history is diagnostic only",
            "privacy": "no account, group, sender, filename or URL values are emitted",
        },
        gate,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Emit the bounded steady-state counter report.")
    parser.add_argument("--hours", type=int, default=72)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("/data/qq-image-collector/config/collector_config.json"),
    )
    args = parser.parse_args()
    payload, gate = report(args.config, max(1, args.hours))
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if gate else 2


if __name__ == "__main__":
    raise SystemExit(main())
