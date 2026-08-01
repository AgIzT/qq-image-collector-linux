from __future__ import annotations

import argparse
import json
import os
import secrets
from pathlib import Path
from typing import Any


# No groups are monitored until they are added with --group or from the console.
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
    value: str | Path | None = explicit
    if value is None:
        value = os.environ.get("QQAI_RUNTIME_ROOT")
    if value is None:
        value = env_value(root / ".env", "QQAI_RUNTIME_ROOT")
    candidate = Path(value) if value else Path("runtime")
    if not candidate.is_absolute():
        candidate = root / candidate
    return candidate.resolve()


def atomic_json(path: Path, payload: dict[str, Any], *, force: bool = False) -> bool:
    if path.exists() and not force:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
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
                    "name": "qq-image-collector-local",
                    "host": "127.0.0.1",
                    "port": 3000,
                    "enableCors": False,
                    "enableWebsocket": False,
                    "messagePostFormat": "array",
                    "token": token,
                    "debug": False,
                }
            ],
            "httpSseServers": [],
            "httpClients": [],
            "websocketServers": [],
            "websocketClients": [],
            "plugins": [],
        },
        "musicSignUrl": "",
        "enableLocalFile2Url": True,
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
            "config_dir": "/app/napcat/config",
            "server_name": "qq-image-collector-local",
            "timeout_seconds": 180,
        },
        "qce": {
            "base_url": "http://127.0.0.1:40653",
            "security_config": "/app/.qq-chat-exporter/security.json",
            "security_configs": [
                "/app/.qq-chat-exporter/security.json",
                "/root/.qq-chat-exporter/security.json",
            ],
            "timeout_seconds": 180,
        },
        "groups": groups,
        "storage": {
            "root": "/data/qq-image-collector",
            "database": "/data/qq-image-collector/state/collector_state.sqlite3",
            "legacy_roots": [],
            "keep_rejected": False,
            "migrate_existing_accepted_on_start": False,
        },
        "runtime": {
            "pid_file": "/data/qq-image-collector/state/collector.pid",
            "poll_interval_seconds": 90,
            "poll_jitter_seconds": 20,
            "catchup_page_size": 20,
            "catchup_initial_lookback_seconds": 3600,
            "backfill_page_size": 20,
            "backfill_pages_per_cycle": 1,
            "backfill_paused": False,
            "retry_limit_per_cycle": 3,
            "deep_backfill_enabled": False,
        },
    }


def manager_config() -> dict[str, Any]:
    return {
        "data_dir": "/data/manager",
        "collector_config": "/data/qq-image-collector/config/collector_config.json",
        "qq_path": "/app/QQ/qq",
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
        "trusted_proxy_cidrs": [
            "127.0.0.0/8",
            "::1/128",
            "172.16.0.0/12",
        ],
        "local_forward_ports": [17891],
        "direct_public_enabled": False,
        "direct_public_hosts": [],
        "direct_public_port": 17891,
        "external_service_detail": "NapCat/QCE 由 Docker Compose 管理",
    }


def prepare(
    root: Path,
    groups: list[str],
    *,
    force: bool = False,
    runtime_root: Path | None = None,
) -> dict[str, bool]:
    runtime = configured_runtime_root(root, runtime_root)
    for relative in (
        "qq-session",
        "napcat-config",
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

    token = secrets.token_urlsafe(24)
    results = {
        "onebot": atomic_json(
            runtime / "napcat-config" / "onebot11.json",
            onebot_config(token),
            force=force,
        ),
        "plugins": atomic_json(
            runtime / "napcat-config" / "plugins.json",
            {
                "napcat-plugin-builtin": True,
                "napcat-plugin-qce": True,
            },
            force=force,
        ),
        "collector": atomic_json(
            runtime / "repository" / "config" / "collector_config.json",
            collector_config(groups),
            force=force,
        ),
        "manager": atomic_json(
            runtime / "manager" / "manager_config.json",
            manager_config(),
            force=force,
        ),
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
        description="Initialize the isolated Linux NapCat/QCE collector deployment."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parent,
        help="Directory containing docker-compose.yml",
    )
    parser.add_argument("--group", action="append", default=[])
    parser.add_argument(
        "--runtime-root",
        type=Path,
        help=(
            "Host persistence directory. Defaults to QQAI_RUNTIME_ROOT from "
            ".env, then ./runtime."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Replace generated configuration. Never use after production cutover.",
    )
    args = parser.parse_args()
    root = args.root.resolve()
    runtime_root = configured_runtime_root(root, args.runtime_root)
    groups = [str(value) for value in args.group] or list(DEFAULT_GROUPS)
    invalid = [value for value in groups if not value.isdigit()]
    if invalid:
        parser.error("Group IDs must be numeric: " + ", ".join(invalid))
    result = prepare(
        root,
        groups,
        force=args.force,
        runtime_root=runtime_root,
    )
    print(
        "Linux runtime initialized. Generated files: "
        + ", ".join(key for key, created in result.items() if created)
    )
    print(f"Runtime root: {runtime_root}")
    print("No service was started and no credential value was printed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
