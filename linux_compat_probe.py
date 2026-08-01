from __future__ import annotations

import argparse
import json
import time
import urllib.request
from pathlib import Path
from typing import Any

from collector import OneBotClient, QCEClient, load_settings


def get_json(url: str, timeout: int = 10) -> dict[str, Any]:
    request = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        value = json.load(response)
    if not isinstance(value, dict):
        raise ValueError(f"{url} did not return an object")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read-only compatibility probe for Linux NapCat/QCE."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("/data/qq-image-collector/config/collector_config.json"),
    )
    parser.add_argument("--group", help="Optionally fetch at most two recent messages.")
    args = parser.parse_args()
    settings = load_settings(args.config)

    onebot = OneBotClient.from_settings(settings["onebot"])
    account = onebot.call("get_login_info", {}) or {}
    qce_settings = settings["qce"]
    qce = QCEClient.from_settings(qce_settings)
    health = get_json(str(qce_settings["base_url"]).rstrip("/") + "/health")

    result: dict[str, Any] = {
        "onebot": {
            "ok": True,
            "user_id": str(account.get("user_id") or ""),
            "nickname": str(account.get("nickname") or ""),
        },
        "qce": {
            "ok": True,
            "health_shape": sorted(health.keys()),
            "security_file": str(qce.security_config),
        },
        "message_fetch": {"tested": False},
    }
    if args.group:
        now = int(time.time())
        messages, has_next = qce.fetch_page(str(args.group), now - 300, now, 2)
        result["message_fetch"] = {
            "tested": True,
            "ok": True,
            "count": len(messages),
            "has_next": has_next,
            "message_keys": sorted(messages[0].keys()) if messages else [],
        }

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
