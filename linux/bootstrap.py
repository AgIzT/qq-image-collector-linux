from __future__ import annotations

import argparse
import json
import os
import secrets
from pathlib import Path
from typing import Any


DEFAULT_GROUPS: list[str] = []


def env_value(path: Path, key: str) -> str | None:
    if not path.is_file():
        return None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        if name.strip() == key:
            return value.strip().strip("\"'")
    return None


def configured_runtime_root(root: Path, explicit: Path | None = None) -> Path:
    value: str | Path | None = explicit or os.environ.get("QQAI_RUNTIME_ROOT")
    if value is None:
        value = env_value(root / ".env", "QQAI_RUNTIME_ROOT")
    candidate = Path(value) if value else Path("runtime")
    if not candidate.is_absolute():
        candidate = root / candidate
    return candidate.resolve()


def atomic_json(path: Path, payload: dict[str, Any], *, force: bool = False) -> bool:
    if path.exists() and not force:
        return False
    serialized = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    if path.is_file() and path.read_text(encoding="utf-8") == serialized:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(serialized, encoding="utf-8")
    os.replace(temporary, path)
    try:
        path.chmod(0o600)
    except OSError:
        pass
    return True


def atomic_text(path: Path, value: str, *, replace: bool = False) -> bool:
    if path.is_file() and not replace:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)
    try:
        path.chmod(0o600)
    except OSError:
        pass
    return True


def onebot_config(token: str) -> dict[str, Any]:
    return {
        "network": {
            "httpServers": [
                {
                    "enable": True,
                    "name": "qq-image-collector-http",
                    "host": "0.0.0.0",
                    "port": 3000,
                    "enableCors": False,
                    "enableWebsocket": False,
                    "messagePostFormat": "array",
                    "token": token,
                    "debug": True,
                }
            ],
            "httpSseServers": [],
            "httpClients": [],
            "websocketServers": [
                {
                    "enable": True,
                    "name": "qq-image-collector-events",
                    "host": "0.0.0.0",
                    "port": 3001,
                    "messagePostFormat": "array",
                    "reportSelfMessage": False,
                    "token": token,
                    "enableForcePushEvent": True,
                    "debug": True,
                    "heartInterval": 30000,
                }
            ],
            "websocketClients": [],
            "plugins": [],
        },
        "musicSignUrl": "",
        "enableLocalFile2Url": False,
        "parseMultMsg": False,
        "imageDownloadProxy": "",
        "timeout": {
            "baseTimeout": 10000,
            "uploadSpeedKBps": 256,
            "downloadSpeedKBps": 256,
            "maxTimeout": 180000,
        },
    }


def collector_config(groups: list[str]) -> dict[str, Any]:
    return {
        "onebot": {
            "base_url": "http://napcat:3000",
            "ws_url": "ws://napcat:3001",
            "webui_url": "http://napcat:6099",
            "token_file": "/app/napcat/config/collector.onebot.token",
            "timeout_seconds": 20,
        },
        "groups": groups,
        "storage": {
            "root": "/data/qq-image-collector",
            "database": "/data/qq-image-collector/state/collector_state.sqlite3",
        },
        "runtime": {
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
        },
    }


def manager_config() -> dict[str, Any]:
    return {
        "data_dir": "/data/manager",
        "collector_config": "/data/qq-image-collector/config/collector_config.json",
        "qq_path": "/opt/QQ/qq",
        "napcat_root": "/app/napcat",
        "deployment_mode": "linux-docker",
        "launcher_kind": "external",
        "shell_launcher": None,
        "host": "0.0.0.0",
        "port": 17890,
        "open_browser": False,
        "remote_enabled": False,
        "remote_public_origin": None,
        "remote_access_issuer": None,
        "remote_access_audience": None,
        "remote_allowed_email": None,
        "snapshot_ingest_url": None,
        "snapshot_secret_file": None,
        "snapshot_interval_seconds": 60,
        "trusted_proxy_cidrs": ["127.0.0.0/8", "::1/128", "172.16.0.0/12"],
        "local_forward_ports": [],
        "direct_public_enabled": False,
        "direct_public_hosts": [],
        "direct_public_port": 17890,
        "external_service_detail": "NapCat 与事件采集器由 Docker Compose 管理",
    }


def _load_object(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def reconcile_collector(path: Path, requested_groups: list[str]) -> bool:
    existing = _load_object(path)
    groups = requested_groups or [str(value) for value in existing.get("groups", [])]
    payload = collector_config(groups)
    existing_storage = existing.get("storage")
    if isinstance(existing_storage, dict):
        for key in ("root", "database"):
            if existing_storage.get(key):
                payload["storage"][key] = existing_storage[key]
    existing_runtime = existing.get("runtime")
    if isinstance(existing_runtime, dict):
        forced = {
            "event_state_heartbeat_seconds",
            "worker_restart_delay_seconds",
            "worker_heartbeat_seconds",
        }
        for key in tuple(payload["runtime"]):
            if key in existing_runtime:
                if key not in forced:
                    payload["runtime"][key] = existing_runtime[key]
    return atomic_json(path, payload, force=True)


def reconcile_manager(path: Path) -> bool:
    payload = manager_config()
    existing = _load_object(path)
    if existing:
        payload.update(existing)
        payload.update(
            {
                "collector_config": "/data/qq-image-collector/config/collector_config.json",
                "deployment_mode": "linux-docker",
                "launcher_kind": "external",
                "host": "0.0.0.0",
                "port": 17890,
                "external_service_detail": "NapCat 与事件采集器由 Docker Compose 管理",
            }
        )
    return atomic_json(path, payload, force=True)


def prepare(
    root: Path,
    groups: list[str],
    *,
    force: bool = False,
    runtime_root: Path | None = None,
) -> dict[str, bool]:
    del force
    runtime = configured_runtime_root(root, runtime_root)
    for relative in (
        "qq-session",
        "napcat-config",
        "napcat-logs",
        "diagnostics",
        "qce-data",
        "repository/config",
        "repository/final/NovelAI",
        "repository/final/ComfyUI",
        "repository/final/NAI含参但不可直接读取的",
        "repository/final/其他模型生成",
        "repository/state",
        "repository/temp",
        "repository/logs",
        "manager",
    ):
        (runtime / relative).mkdir(parents=True, exist_ok=True)

    token_path = runtime / "napcat-config" / "collector.onebot.token"
    if token_path.is_file():
        token = token_path.read_text(encoding="utf-8").strip()
        if len(token) < 24:
            raise ValueError("existing OneBot token is unexpectedly short")
        token_created = False
    else:
        token = secrets.token_urlsafe(32)
        token_created = atomic_text(token_path, token + "\n")

    onebot = onebot_config(token)
    onebot_changed = atomic_json(
        runtime / "napcat-config" / "onebot11.json", onebot, force=True
    )
    account_changed = False
    for account_config in (runtime / "napcat-config").glob("onebot11_[0-9]*.json"):
        account_changed = atomic_json(account_config, onebot, force=True) or account_changed

    plugins_path = runtime / "napcat-config" / "plugins.json"
    plugins_changed = False
    if plugins_path.is_file():
        plugins = _load_object(plugins_path)
        if "napcat-plugin-qce" in plugins:
            plugins.pop("napcat-plugin-qce", None)
            plugins_changed = atomic_json(plugins_path, plugins, force=True)

    results = {
        "onebot_token": token_created,
        "onebot": onebot_changed or account_changed,
        "plugins": plugins_changed,
        "collector": reconcile_collector(
            runtime / "repository" / "config" / "collector_config.json", groups
        ),
        "manager": reconcile_manager(runtime / "manager" / "manager_config.json"),
    }
    env_file = root / ".env"
    env_example = root / ".env.example"
    if not env_file.exists() and env_example.is_file():
        env_file.write_text(env_example.read_text(encoding="utf-8"), encoding="utf-8")
        try:
            env_file.chmod(0o600)
        except OSError:
            pass
        results["environment"] = True
    else:
        results["environment"] = False
    return results


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Initialize or migrate the event-driven NapCat deployment."
    )
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--group", action="append", default=[])
    parser.add_argument("--runtime-root", type=Path)
    parser.add_argument("--force", action="store_true", help="Compatibility flag; secrets are never rotated.")
    args = parser.parse_args()
    root = args.root.resolve()
    groups = [str(value) for value in args.group] or list(DEFAULT_GROUPS)
    invalid = [value for value in groups if not value.isdigit()]
    if invalid:
        parser.error("Group IDs must be numeric: " + ", ".join(invalid))
    runtime = configured_runtime_root(root, args.runtime_root)
    result = prepare(root, groups, force=args.force, runtime_root=runtime)
    print("Runtime reconciled: " + ", ".join(key for key, changed in result.items() if changed))
    print(f"Runtime root: {runtime}")
    print("No credential value was printed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
