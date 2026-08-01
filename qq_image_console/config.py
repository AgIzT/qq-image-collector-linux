from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


APP_NAME = "QQImageCollectorConsole"


def _default_data_dir() -> Path:
    override = os.environ.get("QQ_IMAGE_CONSOLE_HOME")
    if override:
        return Path(override).expanduser().resolve()
    return Path.home() / ".local" / "share" / "qq-image-collector-console"


def _application_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _detect_qq() -> Path:
    return Path("/app/QQ/qq")


def _detect_napcat() -> Path:
    candidates = [Path("/app/napcat"), Path("/opt/napcat")]
    return next((path for path in candidates if path.is_dir()), candidates[0])


def _detect_collector_config() -> Path | None:
    explicit = os.environ.get("QQ_IMAGE_COLLECTOR_CONFIG")
    candidates = [
        Path(explicit).expanduser() if explicit else None,
        Path("/data/qq-image-collector/config/collector_config.json"),
        _application_root() / "collector_config.json",
    ]
    return next((path for path in candidates if path is not None and path.is_file()), None)


@dataclass
class ConsoleConfig:
    data_dir: str
    collector_config: str
    qq_path: str
    napcat_root: str
    deployment_mode: str = "linux-docker"
    launcher_kind: str = "external"
    shell_launcher: str | None = None
    host: str = "127.0.0.1"
    port: int = 17890
    open_browser: bool = False
    remote_enabled: bool = False
    remote_public_origin: str | None = None
    remote_access_issuer: str | None = None
    remote_access_audience: str | None = None
    remote_allowed_email: str | None = None
    snapshot_ingest_url: str | None = None
    snapshot_secret_file: str | None = None
    snapshot_interval_seconds: int = 60
    trusted_proxy_cidrs: list[str] = field(default_factory=list)
    local_forward_ports: list[int] = field(default_factory=list)
    direct_public_enabled: bool = False
    direct_public_hosts: list[str] = field(default_factory=list)
    direct_public_port: int = 17891
    external_service_detail: str = "NapCat/QCE 由 Docker Compose 管理"

    @property
    def data_path(self) -> Path:
        return Path(self.data_dir)

    @property
    def collector_config_path(self) -> Path:
        return Path(self.collector_config)

    @property
    def qq_executable(self) -> Path:
        return Path(self.qq_path)

    @property
    def napcat_path(self) -> Path:
        return Path(self.napcat_root)

    @property
    def token_file(self) -> Path:
        return self.data_path / "manager.token"

    @property
    def manager_config_file(self) -> Path:
        return self.data_path / "manager_config.json"

    @property
    def manager_pid_file(self) -> Path:
        return self.data_path / "manager.pid"

    @property
    def manager_log_file(self) -> Path:
        return self.data_path / "manager.log"

    @property
    def snapshot_secret_path(self) -> Path:
        if self.snapshot_secret_file:
            return Path(self.snapshot_secret_file)
        return self.data_path / "snapshot.secret"

    def collector_settings(self) -> dict[str, Any]:
        return json.loads(self.collector_config_path.read_text(encoding="utf-8"))

    def storage_root(self) -> Path:
        return Path(self.collector_settings()["storage"]["root"])

    def database_path(self) -> Path:
        return Path(self.collector_settings()["storage"]["database"])

    @property
    def external_services(self) -> bool:
        return self.deployment_mode == "linux-docker" or self.launcher_kind == "external"

    def save(self) -> None:
        self.data_path.mkdir(parents=True, exist_ok=True)
        temporary = self.manager_config_file.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(asdict(self), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(temporary, self.manager_config_file)


def _create_collector_config(path: Path, napcat_root: Path, data_dir: Path) -> None:
    storage_root = Path("/data/qq-image-collector")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "onebot": {
            "config_dir": str(napcat_root / "config"),
            "server_name": "qq-image-collector-local",
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
        "groups": [],
        "storage": {
            "root": str(storage_root),
            "database": str(storage_root / "state" / "collector_state.sqlite3"),
            "legacy_roots": [],
            "keep_rejected": False,
            "migrate_existing_accepted_on_start": False,
        },
        "runtime": {
            "pid_file": str(storage_root / "state" / "collector.pid"),
            "poll_interval_seconds": 60,
            "catchup_page_size": 50,
            "catchup_initial_lookback_seconds": 3600,
            "backfill_page_size": 50,
            "backfill_pages_per_cycle": 1,
            "retry_limit_per_cycle": 5,
            "deep_backfill_enabled": False,
        },
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_console_config(explicit_file: Path | None = None) -> ConsoleConfig:
    data_dir = _default_data_dir()
    config_file = explicit_file or data_dir / "manager_config.json"
    if config_file.is_file():
        payload = json.loads(config_file.read_text(encoding="utf-8"))
        payload.pop("start_with_windows", None)
        payload.setdefault("deployment_mode", "linux-docker")
        payload.setdefault("launcher_kind", "external")
        payload.setdefault("host", "0.0.0.0")
        payload.setdefault(
            "trusted_proxy_cidrs",
            ["127.0.0.0/8", "::1/128", "172.16.0.0/12"],
        )
        config = ConsoleConfig(**payload)
        config.data_path.mkdir(parents=True, exist_ok=True)
        return config

    data_dir.mkdir(parents=True, exist_ok=True)
    napcat_root = _detect_napcat()
    collector_config = _detect_collector_config()
    if collector_config is None:
        collector_config = data_dir / "collector_config.json"
        _create_collector_config(collector_config, napcat_root, data_dir)
    config = ConsoleConfig(
        data_dir=str(data_dir),
        collector_config=str(collector_config),
        qq_path=str(_detect_qq()),
        napcat_root=str(napcat_root),
        host="0.0.0.0",
        trusted_proxy_cidrs=["127.0.0.0/8", "::1/128", "172.16.0.0/12"],
    )
    config.save()
    return config
