from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

import websockets

from qq_image_collector.config import load_settings
from qq_image_collector.events import parse_group_event
from qq_image_collector.onebot import OneBotClient, websocket_settings


async def receive_one(config: Path, group_id: str | None, timeout: int) -> int:
    settings = load_settings(config)
    client = OneBotClient.from_settings(settings["onebot"])
    login = client.call("get_login_info", {})
    version = client.call("get_version_info", {})
    groups = client.call("get_group_list", {}) or []
    print(
        json.dumps(
            {
                "login": {"user_id": str((login or {}).get("user_id") or "")},
                "version": version,
                "groups": len(groups),
                "production_get_image": "blocked",
            },
            ensure_ascii=False,
        )
    )
    ws_url, token = websocket_settings(settings["onebot"])
    headers = {"Authorization": f"Bearer {token}"} if token else None
    async with websockets.connect(ws_url, additional_headers=headers, ping_interval=30) as websocket:
        while True:
            payload = await asyncio.wait_for(websocket.recv(), timeout=timeout)
            event = json.loads(payload)
            cursor, items = parse_group_event(event)
            if cursor is None or (group_id and cursor["group_id"] != group_id):
                continue
            print(
                json.dumps(
                    {
                        "group_id": cursor["group_id"],
                        "raw_message_id": cursor["message_id"],
                        "images": len(items),
                        "raw_debug": bool(event.get("raw")),
                    },
                    ensure_ascii=False,
                )
            )
            return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only OneBot HTTP and WS event probe.")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("/data/qq-image-collector/config/collector_config.json"),
    )
    parser.add_argument("--group")
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    return asyncio.run(receive_one(args.config, args.group, max(5, args.timeout)))


if __name__ == "__main__":
    raise SystemExit(main())
