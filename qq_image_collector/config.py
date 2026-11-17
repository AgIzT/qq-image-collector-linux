from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_RUNTIME: dict[str, Any] = {
    "pid_file": "/data/qq-image-collector/state/collector.pid",
    "collector_paused": False,
    "download_interval_seconds": 0,
    "download_jitter_seconds": 0,
    "max_download_bytes": 128 * 1024 * 1024,
    "url_preference": "data",
    "url_expiry_urgent_seconds": 3600,
    "url_refresh_max_failures": 5,
    # get_group_msg_history is the only per-image Tencent interface left, so it
    # carries a hard ceiling.  Steady-state live collection needs zero calls;
    # these budgets exist to cover reconnect gap recovery and to stop any single
    # code path from sustaining thousands of calls a day unnoticed.
    "history_hourly_limit": 60,
    "history_daily_limit": 300,
    # When NapCat fails this many history calls in a row, all history calls
    # pause for history_failure_pause_seconds, then one call probes whether it
    # has recovered.  On 2026-11-17 it failed every call from 02:00 to 07:40,
    # and retrying each expired image five times spent the day's 300 for
    # nothing while fifty images were given up.
    "history_failure_pause_after": 3,
    "history_failure_pause_seconds": 3600,
    "ws_ping_interval_seconds": 30,
    "event_state_heartbeat_seconds": 10,
    "ws_disconnect_gap_seconds": 3,
    "history_page_size": 20,
    "history_page_interval_seconds": 2,
    "cdn_429_pause_seconds": 300,
    "worker_restart_delay_seconds": 5,
    "worker_heartbeat_seconds": 10,
}


def load_settings(path: str | Path) -> dict[str, Any]:
    config_path = Path(path)
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("collector configuration must be a JSON object")
    payload.pop("qce", None)
    payload.setdefault("groups", [])
    payload.setdefault("onebot", {})
    storage = payload.setdefault("storage", {})
    storage.setdefault("root", "/data/qq-image-collector")
    storage.setdefault(
        "database",
        str(Path(str(storage["root"])) / "state" / "collector_state.sqlite3"),
    )
    runtime = payload.setdefault("runtime", {})
    for key, value in DEFAULT_RUNTIME.items():
        runtime.setdefault(key, value)
    return payload


def runtime_value(settings: dict[str, Any], key: str) -> Any:
    return settings.get("runtime", {}).get(key, DEFAULT_RUNTIME[key])
