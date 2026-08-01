from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import threading
import time
import uuid
from contextlib import closing
from pathlib import Path
from typing import Any, Callable

from .config import ConsoleConfig


ProgressCallback = Callable[[str, int, int], None]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _existing_parent(path: Path) -> Path:
    current = path
    while not current.exists() and current.parent != current:
        current = current.parent
    return current


def _relative_to(path: Path, root: Path) -> Path | None:
    try:
        return path.resolve().relative_to(root.resolve())
    except ValueError:
        return None


def migrate_storage(
    config: ConsoleConfig,
    destination: Path,
    progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Copy and verify a repository, then atomically switch the manager config.

    The original repository is intentionally retained as the rollback copy.
    """

    source = config.storage_root().resolve()
    destination = destination.expanduser().resolve()
    if destination == source:
        raise ValueError("新仓库位置与当前仓库相同")
    if _relative_to(destination, source) is not None or _relative_to(source, destination) is not None:
        raise ValueError("新旧仓库不能互相嵌套")
    if destination.exists() and any(destination.iterdir()):
        raise ValueError("目标文件夹必须不存在或为空")
    source_db = config.database_path().resolve()
    db_relative = _relative_to(source_db, source)
    if db_relative is None:
        raise ValueError("当前 SQLite 必须位于仓库目录内，才能执行安全迁移")

    files = [path for path in source.rglob("*") if path.is_file()]
    total_bytes = sum(path.stat().st_size for path in files)
    disk = shutil.disk_usage(_existing_parent(destination))
    if disk.free < total_bytes + 64 * 1024 * 1024:
        raise OSError("目标磁盘可用空间不足（需要仓库大小外加 64 MiB 安全余量）")

    backup_dir = source / "state" / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"pre-migration-{time.strftime('%Y%m%d-%H%M%S')}.sqlite3"
    with closing(sqlite3.connect(source_db)) as input_db, closing(
        sqlite3.connect(backup_path)
    ) as backup_db:
        input_db.backup(backup_db)
        input_db.execute("PRAGMA wal_checkpoint(FULL)")

    # Include the fresh database backup in the copy and verification set.
    files = [path for path in source.rglob("*") if path.is_file()]
    total_files = len(files)
    total_bytes = sum(path.stat().st_size for path in files)
    staging = destination.parent / f".{destination.name}.migration-{uuid.uuid4().hex}.tmp"
    if staging.exists():
        raise FileExistsError(f"迁移暂存目录已存在：{staging}")
    staging.mkdir(parents=True)
    original_collector_config = config.collector_config
    copied = 0
    copied_bytes = 0
    try:
        for index, source_file in enumerate(files, start=1):
            relative = source_file.relative_to(source)
            target_file = staging / relative
            target_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_file, target_file)
            copied += 1
            copied_bytes += source_file.stat().st_size
            if progress:
                progress("copy", index, total_files)

        for index, source_file in enumerate(files, start=1):
            target_file = staging / source_file.relative_to(source)
            if _sha256(source_file) != _sha256(target_file):
                raise OSError(f"SHA-256 校验失败：{source_file.relative_to(source)}")
            if progress:
                progress("verify", index, total_files)

        collector_payload = config.collector_settings()
        target_db = staging / db_relative
        with closing(sqlite3.connect(target_db)) as connection:
            rows = connection.execute(
                "SELECT group_id, message_id, image_index, local_path FROM images "
                "WHERE local_path IS NOT NULL"
            ).fetchall()
            for group_id, message_id, image_index, local_path in rows:
                relative = _relative_to(Path(str(local_path)), source)
                if relative is None:
                    continue
                connection.execute(
                    "UPDATE images SET local_path=? WHERE group_id=? AND message_id=? AND image_index=?",
                    (str(destination / relative), group_id, message_id, image_index),
                )
            asset_rows = connection.execute(
                "SELECT sha256, local_path FROM assets WHERE local_path IS NOT NULL"
            ).fetchall()
            for digest, local_path in asset_rows:
                relative = _relative_to(Path(str(local_path)), source)
                if relative is not None:
                    connection.execute(
                        "UPDATE assets SET local_path=? WHERE sha256=?",
                        (str(destination / relative), digest),
                    )
            connection.commit()

        storage = collector_payload.setdefault("storage", {})
        storage["root"] = str(destination)
        storage["database"] = str(destination / db_relative)
        runtime = collector_payload.setdefault("runtime", {})
        runtime["pid_file"] = str(destination / "state" / "collector.pid")

        source_collector_path = config.collector_config_path.resolve()
        config_relative = _relative_to(source_collector_path, source)
        if config_relative is None:
            target_collector_path = staging / "config" / "collector_config.json"
        else:
            target_collector_path = staging / config_relative
        target_collector_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_config = target_collector_path.with_suffix(target_collector_path.suffix + ".tmp")
        temporary_config.write_text(
            json.dumps(collector_payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        os.replace(temporary_config, target_collector_path)

        # Validate each accepted image before switching the manager to the new root.
        with closing(sqlite3.connect(target_db)) as connection:
            accepted = connection.execute(
                "SELECT DISTINCT sha256, local_path FROM images "
                "WHERE status='accepted' AND sha256 IS NOT NULL AND local_path IS NOT NULL"
            ).fetchall()
        for index, (expected, local_path) in enumerate(accepted, start=1):
            relative = _relative_to(Path(str(local_path)), destination)
            if relative is None:
                image_path = Path(str(local_path))
            else:
                image_path = staging / relative
            if not image_path.is_file() or _sha256(image_path) != str(expected):
                raise OSError(f"迁移后图片校验失败：{local_path}")
            if progress:
                progress("images", index, len(accepted))

        if destination.exists():
            destination.rmdir()
        os.replace(staging, destination)
        final_collector_path = destination / target_collector_path.relative_to(staging)
        config.collector_config = str(final_collector_path)
        try:
            config.save()
        except Exception:
            config.collector_config = original_collector_config
            try:
                config.save()
            except Exception:
                pass
            raise

        return {
            "source": str(source),
            "destination": str(destination),
            "database_backup": str(backup_path),
            "files": copied,
            "bytes": copied_bytes,
            "accepted_images_verified": len(accepted),
        }
    except Exception:
        config.collector_config = original_collector_config
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
        raise


class StorageMigrationManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state: dict[str, Any] = {
            "status": "idle",
            "stage": None,
            "current": 0,
            "total": 0,
            "error": None,
            "result": None,
            "started_at": None,
            "finished_at": None,
        }

    def state(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._state)

    def start(self, destination: Path, config: ConsoleConfig, supervisor: Any) -> dict[str, Any]:
        with self._lock:
            if self._state["status"] == "running":
                raise RuntimeError("已有仓库迁移正在执行")
            self._state = {
                "status": "running",
                "stage": "stop_worker",
                "current": 0,
                "total": 0,
                "error": None,
                "result": None,
                "started_at": int(time.time()),
                "finished_at": None,
            }
        worker_was_running = bool(supervisor.worker_pid())

        def update(stage: str, current: int, total: int) -> None:
            with self._lock:
                self._state.update(stage=stage, current=current, total=total)

        def run() -> None:
            try:
                supervisor.stop_worker()
                result = migrate_storage(config, destination, update)
                if worker_was_running:
                    update("restart_worker", 0, 0)
                    supervisor.start_worker()
                with self._lock:
                    self._state.update(
                        status="completed",
                        stage="done",
                        result=result,
                        finished_at=int(time.time()),
                    )
            except Exception as exc:
                # The old repository and manager config are still valid on all copy-stage failures.
                if worker_was_running and not supervisor.worker_pid():
                    try:
                        supervisor.start_worker()
                    except Exception:
                        pass
                with self._lock:
                    self._state.update(
                        status="failed",
                        error=f"{type(exc).__name__}: {exc}",
                        finished_at=int(time.time()),
                    )

        threading.Thread(target=run, name="storage-migration", daemon=True).start()
        return self.state()
