from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_RUNTIME: dict[str, Any] = {
    "pid_file": "/data/qq-image-collector/state/collector.pid",
    "collector_paused": False,
    "download_interval_seconds": 15,
    "download_jitter_seconds": 3,
    "accelerated_interval_seconds": 5,
    "accelerate_queue_age_seconds": 1800,
    "resume_normal_queue_age_seconds": 900,
    "max_download_bytes": 128 * 1024 * 1024,
    "url_preference": "data",
    "url_expiry_urgent_seconds": 3600,
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
