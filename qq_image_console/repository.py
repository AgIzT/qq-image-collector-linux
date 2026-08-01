from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
import threading
import time
from contextlib import closing
from pathlib import Path
from typing import Any

from collector import category_for_source, connect_database
from collector_control import (
    enqueue_job,
    enabled_groups,
    get_setting,
    request_job_cancel,
    seed_monitored_groups,
    set_group_enabled,
    set_setting,
)

from .config import ConsoleConfig


class Repository:
    def __init__(self, config: ConsoleConfig) -> None:
        self.config = config
        self._cache_lock = threading.Lock()
        self._stats_cache: tuple[float, dict[str, Any]] | None = None
        self.bootstrap()

    def connect(self) -> sqlite3.Connection:
        connection = connect_database(
            self.config.database_path(),
            initialize=False,
        )
        connection.row_factory = sqlite3.Row
        return connection

    def bootstrap(self) -> None:
        settings = self.config.collector_settings()
        with closing(connect_database(self.config.database_path())) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS remote_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at INTEGER NOT NULL,
                    identity TEXT NOT NULL,
                    action TEXT NOT NULL,
                    status_code INTEGER NOT NULL,
                    source_ip TEXT
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_remote_audit_created_at "
                "ON remote_audit(created_at DESC)"
            )
            connection.commit()
            seed_monitored_groups(connection, [str(value) for value in settings.get("groups", [])])
            if get_setting(connection, "setup_completed", None) is None and settings.get("groups"):
                # Existing installations are imported without forcing the new-user wizard.
                set_setting(connection, "setup_completed", True)

    def list_groups(self) -> list[dict[str, Any]]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                """
                SELECT g.group_id, g.display_name, g.enabled,
                       r.recent_status, r.recent_last_success, r.recent_last_error,
                       q.last_time AS recent_cursor_time,
                       CASE
                           WHEN d.group_id IS NOT NULL AND d.completed=1 THEN 'completed'
                           WHEN d.group_id IS NOT NULL
                                AND r.backfill_status IN ('error', 'paused')
                               THEN r.backfill_status
                           WHEN d.group_id IS NOT NULL THEN 'running'
                           WHEN max(coalesce(r.backfill_completed, 0), coalesce(c.completed, 0))=1
                               THEN 'completed'
                           ELSE r.backfill_status
                       END AS backfill_status,
                       CASE WHEN d.group_id IS NOT NULL
                            THEN d.oldest_time
                            ELSE coalesce(r.backfill_cursor_time, c.oldest_time)
                       END AS backfill_cursor_time,
                       CASE WHEN d.group_id IS NOT NULL
                            THEN d.completed
                            ELSE max(coalesce(r.backfill_completed, 0), coalesce(c.completed, 0))
                       END AS backfill_completed,
                       r.backfill_last_success,
                       r.backfill_last_error,
                       coalesce(count(DISTINCT CASE WHEN i.status='accepted' THEN i.sha256 END), 0) accepted,
                       coalesce(sum(CASE WHEN i.status='accepted' THEN 1 ELSE 0 END), 0)
                         - coalesce(count(DISTINCT CASE WHEN i.status='accepted' THEN i.sha256 END), 0) duplicates,
                       coalesce(sum(CASE WHEN i.status='rejected_no_metadata' THEN 1 ELSE 0 END), 0) rejected,
                       coalesce(sum(CASE WHEN i.status='failed' THEN 1 ELSE 0 END), 0) failed
                FROM monitored_groups g
                LEFT JOIN group_runtime r ON r.group_id=g.group_id
                LEFT JOIN group_cursors c ON c.group_id=g.group_id
                LEFT JOIN deep_history_cursors d ON d.group_id=g.group_id
                LEFT JOIN qce_recent_cursors q ON q.group_id=g.group_id
                LEFT JOIN images i ON i.group_id=g.group_id
                GROUP BY g.group_id
                ORDER BY g.enabled DESC, g.created_at, g.group_id
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def enabled_group_ids(self) -> list[str]:
        with closing(self.connect()) as connection:
            return enabled_groups(connection)

    def upsert_group(self, group_id: str, display_name: str | None = None) -> None:
        if not group_id.isdigit() or not 5 <= len(group_id) <= 20:
            raise ValueError("群号必须是 5 到 20 位数字")
        with closing(self.connect()) as connection:
            set_group_enabled(connection, group_id, True, display_name)
        self._sync_config_groups()

    def disable_group(self, group_id: str) -> None:
        with closing(self.connect()) as connection:
            if not connection.execute(
                "SELECT 1 FROM monitored_groups WHERE group_id=?", (group_id,)
            ).fetchone():
                raise ValueError("监听群不存在")
            set_group_enabled(connection, group_id, False)
            now = int(time.time())
            connection.execute(
                """
                UPDATE jobs SET status='cancelled', cancel_requested=1,
                    finished_at=?, updated_at=?
                WHERE group_id=? AND status='queued'
                """,
                (now, now, group_id),
            )
            connection.execute(
                """
                UPDATE jobs SET cancel_requested=1, updated_at=?
                WHERE group_id=? AND status='running'
                """,
                (now, group_id),
            )
            connection.commit()
        self._sync_config_groups()

    def update_group_names(self, groups: list[dict[str, Any]]) -> None:
        with closing(self.connect()) as connection:
            for group in groups:
                group_id = str(group.get("group_id") or "")
                name = str(group.get("group_name") or "").strip() or None
                if group_id:
                    row = connection.execute(
                        "SELECT enabled FROM monitored_groups WHERE group_id=?", (group_id,)
                    ).fetchone()
                    if row:
                        set_group_enabled(connection, group_id, bool(row[0]), name)

    def _sync_config_groups(self) -> None:
        payload = self.config.collector_settings()
        payload["groups"] = self.enabled_group_ids()
        target = self.config.collector_config_path
        temporary = target.with_suffix(target.suffix + ".tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        os.replace(temporary, target)

    def create_job(self, group_id: str, kind: str) -> int:
        if group_id not in self.enabled_group_ids():
            raise ValueError("该群未启用监听")
        with closing(self.connect()) as connection:
            return enqueue_job(connection, group_id, kind)

    def cancel_job(self, job_id: int) -> bool:
        with closing(self.connect()) as connection:
            return request_job_cancel(connection, job_id)

    def list_jobs(self, limit: int = 100) -> list[dict[str, Any]]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                """
                SELECT id, kind, group_id, status, progress_pages,
                       cancel_requested, created_at, started_at, updated_at,
                       finished_at, error
                FROM jobs ORDER BY id DESC LIMIT ?
                """,
                (max(1, min(limit, 500)),),
            ).fetchall()
            return [dict(row) for row in rows]

    def record_remote_audit(
        self,
        identity: str,
        action: str,
        status_code: int,
        source_ip: str | None,
    ) -> None:
        with closing(self.connect()) as connection:
            connection.execute(
                """
                INSERT INTO remote_audit(created_at, identity, action, status_code, source_ip)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    int(time.time()),
                    identity[:320],
                    action[:500],
                    int(status_code),
                    (source_ip or "")[:80] or None,
                ),
            )
            connection.execute(
                "DELETE FROM remote_audit WHERE id NOT IN "
                "(SELECT id FROM remote_audit ORDER BY id DESC LIMIT 5000)"
            )
            connection.commit()

    def list_remote_audit(self, limit: int = 100) -> list[dict[str, Any]]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                """
                SELECT id, created_at, identity, action, status_code, source_ip
                FROM remote_audit ORDER BY id DESC LIMIT ?
                """,
                (max(1, min(int(limit), 500)),),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_app_settings(self) -> dict[str, Any]:
        collector = self.config.collector_settings()
        runtime = collector.get("runtime", {})
        with closing(self.connect()) as connection:
            return {
                "storage_root": str(self.config.storage_root()),
                "deployment_mode": self.config.deployment_mode,
                "external_services": self.config.external_services,
                "qq_path": self.config.qq_path,
                "napcat_root": self.config.napcat_root,
                "launcher_kind": self.config.launcher_kind,
                "shell_launcher": self.config.shell_launcher,
                "poll_interval_seconds": get_setting(
                    connection,
                    "poll_interval_seconds",
                    runtime.get("poll_interval_seconds", 60),
                ),
                "catchup_page_size": get_setting(
                    connection,
                    "catchup_page_size",
                    runtime.get("catchup_page_size", 50),
                ),
                "backfill_page_size": get_setting(
                    connection,
                    "backfill_page_size",
                    runtime.get("backfill_page_size", 50),
                ),
                "collector_paused": get_setting(connection, "collector_paused", False),
                "backfill_paused": get_setting(connection, "backfill_paused", False),
                "deep_backfill_enabled": get_setting(
                    connection,
                    "deep_backfill_enabled",
                    runtime.get("deep_backfill_enabled", True),
                ),
            }

    def patch_app_settings(self, values: dict[str, Any]) -> dict[str, Any]:
        database_keys = {
            "poll_interval_seconds",
            "catchup_page_size",
            "backfill_page_size",
            "collector_paused",
            "backfill_paused",
            "deep_backfill_enabled",
        }
        with closing(self.connect()) as connection:
            for key in database_keys & values.keys():
                set_setting(connection, key, values[key])
        for key in ("qq_path", "napcat_root", "launcher_kind", "shell_launcher"):
            if key in values:
                setattr(self.config, key, values[key])
        self.config.save()
        return self.get_app_settings()

    def setup_status(self) -> dict[str, Any]:
        collector = self.config.collector_settings()
        onebot = collector.get("onebot", {})
        qce = collector.get("qce", {})
        onebot_dir = Path(str(onebot.get("config_dir") or ""))
        security_files = [
            Path(str(value))
            for value in (
                qce.get("security_configs") or [qce.get("security_config")]
            )
            if value
        ]
        security_file = next((path for path in security_files if path.is_file()), None)
        napcat_root = self.config.napcat_path
        account_config_ready = onebot_dir.is_dir() and any(
            onebot_dir.glob("onebot11_*.json")
        )
        if self.config.external_services:
            launcher_ok = True
            launcher_detail = self.config.external_service_detail
        elif self.config.launcher_kind == "shell":
            launcher_ok = bool(self.config.shell_launcher) and Path(
                str(self.config.shell_launcher)
            ).is_file()
            launcher_detail = str(self.config.shell_launcher or "尚未配置 Shell 启动器")
        else:
            required = [
                napcat_root / "napimain.exe",
                napcat_root / "napiloader.dll",
                napcat_root / "nativeLoader.cjs",
            ]
            launcher_ok = all(path.is_file() for path in required)
            launcher_detail = str(napcat_root)
        checks = [
            {
                "key": "qq",
                "label": (
                    "Linux QQ 会话配置已生成"
                    if self.config.external_services
                    else "QQ 已安装"
                ),
                "ok": (
                    account_config_ready
                    if self.config.external_services
                    else self.config.qq_executable.is_file()
                ),
                "detail": (
                    str(onebot_dir)
                    if self.config.external_services
                    else str(self.config.qq_executable)
                ),
            },
            {
                "key": "napcat",
                "label": "NapCat 启动器可用",
                "ok": launcher_ok,
                "detail": launcher_detail,
            },
            {
                "key": "onebot",
                "label": "OneBot HTTP 已配置",
                "ok": account_config_ready,
                "detail": str(onebot_dir),
            },
            {
                "key": "qce",
                "label": "QCE 插件凭据可用",
                "ok": security_file is not None,
                "detail": str(security_file or security_files[0] if security_files else "未配置"),
            },
            {
                "key": "storage",
                "label": "图片仓库可写",
                "ok": self.config.storage_root().is_dir()
                and os.access(self.config.storage_root(), os.W_OK),
                "detail": str(self.config.storage_root()),
            },
            {
                "key": "groups",
                "label": "至少选择一个监听群",
                "ok": bool(self.enabled_group_ids()),
                "detail": f"当前 {len(self.enabled_group_ids())} 个",
            },
        ]
        with closing(self.connect()) as connection:
            completed = bool(get_setting(connection, "setup_completed", False))
        return {
            "completed": completed,
            "ready": all(bool(item["ok"]) for item in checks),
            "checks": checks,
            "links": {
                "qq": "https://im.qq.com/pcqq/index.shtml",
                "napcat": "https://napneko.github.io/guide/boot/Shell",
                "qce": "https://github.com/shuakami/qq-chat-exporter",
            },
        }

    def complete_setup(self) -> dict[str, Any]:
        current = self.setup_status()
        if not current["ready"]:
            missing = [item["label"] for item in current["checks"] if not item["ok"]]
            raise ValueError("尚未完成：" + "、".join(missing))
        with closing(self.connect()) as connection:
            set_setting(connection, "setup_completed", True)
        return self.setup_status()

    def stats(self, force: bool = False) -> dict[str, Any]:
        with self._cache_lock:
            if not force and self._stats_cache and time.time() - self._stats_cache[0] < 5:
                return dict(self._stats_cache[1])

        local_midnight = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        today_epoch = int(local_midnight.timestamp())
        with self.connect() as connection:
            status_counts = dict(
                connection.execute(
                    "SELECT status, count(*) FROM images GROUP BY status"
                ).fetchall()
            )
            accepted_records = int(status_counts.get("accepted", 0))
            unique_rows = connection.execute(
                """
                SELECT sha256, min(local_path), min(metadata_source), min(sent_at)
                FROM images
                WHERE status='accepted' AND sha256 IS NOT NULL AND local_path IS NOT NULL
                GROUP BY sha256
                """
            ).fetchall()
            today_new = connection.execute(
                "SELECT count(DISTINCT sha256) FROM images WHERE status='accepted' AND sent_at>=?",
                (today_epoch,),
            ).fetchone()[0]
            gif_excluded = connection.execute(
                "SELECT count(*) FROM images WHERE error='excluded image format: GIF'"
            ).fetchone()[0]
            asset_count = connection.execute("SELECT count(*) FROM assets").fetchone()[0]
            provenance_missing = connection.execute(
                """
                SELECT count(*) FROM images
                WHERE status='accepted' AND coalesce(sender_uin, '')=''
                """
            ).fetchone()[0]

        categories = {
            "NovelAI": 0,
            "ComfyUI": 0,
            "NAI含参但不可直接读取的": 0,
            "其他模型生成": 0,
        }
        disk_bytes = 0
        seen_paths: set[str] = set()
        for _digest, local_path, source, _sent_at in unique_rows:
            category = category_for_source(source)
            categories[category] = categories.get(category, 0) + 1
            normalized = str(local_path)
            if normalized in seen_paths:
                continue
            seen_paths.add(normalized)
            try:
                disk_bytes += Path(normalized).stat().st_size
            except OSError:
                pass
        result = {
            "unique_images": len(unique_rows),
            "accepted_records": accepted_records,
            "novelai": categories.get("NovelAI", 0),
            "comfyui": categories.get("ComfyUI", 0),
            "novelai_unreadable": categories.get("NAI含参但不可直接读取的", 0),
            "other_models": categories.get("其他模型生成", 0),
            "today_new": int(today_new or 0),
            "disk_bytes": disk_bytes,
            "gif_excluded": int(gif_excluded or 0),
            "failed": int(status_counts.get("failed", 0)),
            "rejected": int(status_counts.get("rejected_no_metadata", 0)),
            "resolving": int(status_counts.get("resolving", 0)),
            "assets": int(asset_count or 0),
            "provenance_missing": int(provenance_missing or 0),
            "provenance_complete": max(
                0, accepted_records - int(provenance_missing or 0)
            ),
        }
        with self._cache_lock:
            self._stats_cache = (time.time(), result)
        return dict(result)
