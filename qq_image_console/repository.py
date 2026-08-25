from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import closing
from pathlib import Path
from typing import Any

from collector_control import (
    enqueue_job,
    get_setting,
    request_job_cancel,
    seed_monitored_groups,
    set_group_enabled,
    set_setting,
)
from qq_image_collector.config import DEFAULT_RUNTIME
from qq_image_collector.database import (
    connect_database,
    counter_sum,
    get_runtime_state,
    local_day_start,
    queue_snapshot,
)

from .config import ConsoleConfig


STATUS_CACHE_SECONDS = 15.0


class Repository:
    def __init__(self, config: ConsoleConfig) -> None:
        self.config = config
        self._cache_lock = threading.Lock()
        self._stats_compute_lock = threading.Lock()
        self._groups_compute_lock = threading.Lock()
        self._stats_cache: tuple[float, dict[str, Any]] | None = None
        self._groups_cache: tuple[float, list[dict[str, Any]]] | None = None
        self.bootstrap()

    def connect(self) -> sqlite3.Connection:
        connection = connect_database(self.config.database_path(), initialize=False)
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
                "CREATE INDEX IF NOT EXISTS idx_remote_audit_created_at ON remote_audit(created_at DESC)"
            )
            connection.commit()
            seed_monitored_groups(connection, [str(value) for value in settings.get("groups", [])])
            runtime = settings.get("runtime", {})
            for key in (
                "download_interval_seconds",
                "download_jitter_seconds",
                "url_preference",
                "collector_paused",
            ):
                current = get_setting(connection, key, None)
                if current is None:
                    value = runtime.get(key, DEFAULT_RUNTIME[key])
                    set_setting(connection, key, value)
            connection.execute(
                """
                -- Settings from designs that no longer exist.  history_hourly_limit
                -- and history_daily_limit deliberately are NOT in this list: they
                -- came back as the live account-session ceiling, and deleting them
                -- on every console start silently discarded any operator override,
                -- leaving the limit adjustable only by editing Python defaults.
                DELETE FROM app_settings
                WHERE key IN (
                    'daily_download_limit', 'history_max_pages_per_gap',
                    'allow_403_history_refresh', 'cdn_403_window_seconds',
                    'cdn_403_trip_count', 'cdn_circuit_seconds'
                )
                """
            )
            connection.commit()
            if get_setting(connection, "setup_completed", None) is None:
                set_setting(connection, "setup_completed", bool(settings.get("groups")))

    def _invalidate_groups(self) -> None:
        with self._groups_compute_lock:
            with self._cache_lock:
                self._groups_cache = None

    def list_groups(self, force: bool = False) -> list[dict[str, Any]]:
        with self._groups_compute_lock:
            return self._list_groups_locked(force)

    def _list_groups_locked(self, force: bool = False) -> list[dict[str, Any]]:
        with self._cache_lock:
            if (
                not force
                and self._groups_cache
                and time.time() - self._groups_cache[0] < STATUS_CACHE_SECONDS
            ):
                return [dict(row) for row in self._groups_cache[1]]
        with closing(self.connect()) as connection:
            rows = connection.execute(
                """
                SELECT g.group_id, g.display_name, g.enabled,
                       r.event_status, r.last_message_id, r.last_message_time,
                       r.last_event_at, r.last_image_at, r.gap_status,
                       r.gap_started_at, r.gap_finished_at, r.gap_error,
                       coalesce(s.accepted, 0) AS accepted,
                       coalesce(s.accepted_today, 0) AS accepted_today,
                       coalesce(s.rejected, 0) AS rejected,
                       coalesce(s.failed, 0) AS failed,
                       coalesce(s.expired, 0) AS expired,
                       coalesce(s.queued, 0) AS queued,
                       coalesce(s.accepted - s.unique_accepted, 0) AS duplicates
                FROM monitored_groups g
                LEFT JOIN group_runtime r ON r.group_id=g.group_id
                LEFT JOIN (
                    SELECT group_id,
                           sum(CASE WHEN status='accepted' THEN 1 ELSE 0 END) AS accepted,
                           -- 同一次扫描里多带一个累加器，不增加任何成本
                           sum(CASE WHEN status='accepted' AND sent_at>=:today THEN 1 ELSE 0 END) AS accepted_today,
                           count(DISTINCT CASE WHEN status='accepted' THEN sha256 END) AS unique_accepted,
                           sum(CASE WHEN status IN ('rejected_no_metadata','filtered_gif') THEN 1 ELSE 0 END) AS rejected,
                           sum(CASE WHEN status IN ('failed_terminal','legacy_failed') THEN 1 ELSE 0 END) AS failed,
                           sum(CASE WHEN status='expired' THEN 1 ELSE 0 END) AS expired,
                           sum(CASE WHEN status IN ('queued','deferred','downloading') THEN 1 ELSE 0 END) AS queued
                    FROM images GROUP BY group_id
                ) s ON s.group_id=g.group_id
                ORDER BY g.enabled DESC, g.created_at, g.group_id
                """,
                {"today": local_day_start()},
            ).fetchall()
            result = [dict(row) for row in rows]
        with self._cache_lock:
            self._groups_cache = (time.time(), result)
        return [dict(row) for row in result]

    def upsert_group(self, group_id: str, display_name: str | None = None) -> None:
        with closing(self.connect()) as connection:
            enabled_before = int(
                connection.execute(
                    "SELECT count(*) FROM monitored_groups WHERE enabled=1"
                ).fetchone()[0]
            )
            set_group_enabled(connection, group_id, True, display_name)
            if enabled_before == 0:
                set_setting(connection, "rollout_started_at", int(time.time()))
        self._invalidate_groups()

    def disable_group(self, group_id: str) -> None:
        with closing(self.connect()) as connection:
            exists = connection.execute(
                "SELECT 1 FROM monitored_groups WHERE group_id=?", (str(group_id),)
            ).fetchone()
            if not exists:
                raise ValueError("group is not configured")
            set_group_enabled(connection, str(group_id), False)
            enabled_after = int(
                connection.execute(
                    "SELECT count(*) FROM monitored_groups WHERE enabled=1"
                ).fetchone()[0]
            )
            if enabled_after == 0:
                set_setting(connection, "rollout_started_at", None)
        self._invalidate_groups()

    def update_group_names(self, rows: list[dict[str, Any]]) -> None:
        now = int(time.time())
        with closing(self.connect()) as connection:
            for row in rows:
                group_id = str(row.get("group_id") or "")
                name = str(row.get("group_name") or "").strip()
                if group_id and name:
                    connection.execute(
                        "UPDATE monitored_groups SET display_name=?, updated_at=? WHERE group_id=?",
                        (name, now, group_id),
                    )
            connection.commit()
        self._invalidate_groups()

    def create_job(self, group_id: str, _mode: str = "gap_recovery") -> int:
        with closing(self.connect()) as connection:
            configured = connection.execute(
                """
                SELECT g.enabled, r.last_message_id, r.last_message_seq,
                       r.last_message_time
                FROM monitored_groups g
                LEFT JOIN group_runtime r ON r.group_id=g.group_id
                WHERE g.group_id=?
                """,
                (str(group_id),),
            ).fetchone()
            if not configured or not int(configured[0]):
                raise ValueError("group is not enabled")
            if (
                not str(configured[1] or "")
                or not str(configured[2] or "")
                or int(configured[3] or 0) <= 0
            ):
                raise ValueError("group has not received an event cursor yet")
            return enqueue_job(connection, "gap_recovery", str(group_id))

    def list_jobs(self, limit: int = 100) -> list[dict[str, Any]]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                """
                SELECT id, kind, group_id, status, progress_pages, cancel_requested,
                       created_at, started_at, updated_at, finished_at, error
                FROM jobs ORDER BY id DESC LIMIT ?
                """,
                (max(1, min(int(limit), 500)),),
            ).fetchall()
            return [dict(row) for row in rows]

    def cancel_job(self, job_id: int) -> bool:
        with closing(self.connect()) as connection:
            return request_job_cancel(connection, int(job_id))

    def get_app_settings(self) -> dict[str, Any]:
        collector = self.config.collector_settings()
        runtime = collector.get("runtime", {})
        with closing(self.connect()) as connection:
            values = {
                key: get_setting(connection, key, runtime.get(key, DEFAULT_RUNTIME.get(key)))
                for key in (
                    "download_interval_seconds",
                    "download_jitter_seconds",
                    "url_preference",
                    "collector_paused",
                )
            }
        return values | {
            "unlimited_collection": True,
            "storage_root": str(self.config.storage_root()),
            "deployment_mode": self.config.deployment_mode,
            "external_services": self.config.external_services,
        }

    def patch_app_settings(self, values: dict[str, Any]) -> dict[str, Any]:
        allowed = {
            "download_interval_seconds",
            "download_jitter_seconds",
            "url_preference",
            "collector_paused",
        }
        invalid = set(values) - allowed
        if invalid:
            raise ValueError(f"unsupported settings: {sorted(invalid)}")
        with closing(self.connect()) as connection:
            for key, value in values.items():
                set_setting(connection, key, value)
        return self.get_app_settings()

    def _model_stats(self, connection: sqlite3.Connection) -> dict[str, Any]:
        """Model-family breakdown from the side index.

        asset_model is small and indexed on sent_at, unlike images, so this
        stays in the millisecond range and is safe on the status path.  The
        index is rebuilt incrementally by a daily cron, so "today" here trails
        until that run.
        """

        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='asset_model'"
        ).fetchone()
        if not exists:
            return {"available": False, "total": [], "days": {}}
        today = local_day_start()
        yesterday = today - 86400
        total = [
            {"family": str(row[0] or "unknown"), "count": int(row[1])}
            for row in connection.execute(
                "SELECT model_family, count(*) FROM asset_model GROUP BY 1 ORDER BY 2 DESC"
            )
        ]
        days: dict[str, list[dict[str, Any]]] = {"today": [], "yesterday": []}
        for label, lo, hi in (("today", today, today + 86400), ("yesterday", yesterday, today)):
            days[label] = [
                {"family": str(row[0] or "unknown"), "count": int(row[1])}
                for row in connection.execute(
                    "SELECT model_family, count(*) FROM asset_model "
                    "WHERE sent_at>=? AND sent_at<? GROUP BY 1 ORDER BY 2 DESC",
                    (lo, hi),
                )
            ]
        indexed = int(connection.execute("SELECT count(*) FROM asset_model").fetchone()[0])
        return {"available": True, "indexed": indexed, "total": total, "days": days}

    def stats(self, force: bool = False) -> dict[str, Any]:
        with self._stats_compute_lock:
            return self._stats_locked(force)

    def _stats_locked(self, force: bool = False) -> dict[str, Any]:
        with self._cache_lock:
            if (
                not force
                and self._stats_cache
                and time.time() - self._stats_cache[0] < STATUS_CACHE_SECONDS
            ):
                return dict(self._stats_cache[1])
        with closing(self.connect()) as connection:
            unique_images = int(connection.execute("SELECT count(*) FROM assets").fetchone()[0])
            accepted_records = int(
                connection.execute("SELECT count(*) FROM images WHERE status='accepted'").fetchone()[0]
            )
            categories = {
                str(row[0]): int(row[1])
                for row in connection.execute(
                    "SELECT category, count(*) FROM assets GROUP BY category"
                ).fetchall()
            }
            queue = queue_snapshot(connection)
            today = local_day_start()
            counters = {
                key: counter_sum(connection, key, today)
                for key in (
                    "events",
                    "images_seen",
                    "image_segments",
                    "cdn_requests",
                    "cdn_downloads",
                    "cdn_bytes",
                    "cdn_400",
                    "cdn_403",
                    "cdn_429",
                    "history_calls",
                    "window_history_calls",
                    "get_image_blocked",
                    "accepted",
                    "rejected",
                    "duplicates",
                    "failed",
                    "expired",
                    "filtered_gif",
                )
            }
            payload = {
                "unique_images": unique_images,
                "accepted_records": accepted_records,
                "novelai": categories.get("NovelAI", 0),
                "comfyui": categories.get("ComfyUI", 0),
                "novelai_unreadable": categories.get("NAI含参但不可直接读取的", 0),
                "other_models": categories.get("其他模型生成", 0),
                "disk_bytes": int(
                    connection.execute("SELECT coalesce(sum(file_size), 0) FROM assets").fetchone()[0]
                ),
                "models": self._model_stats(connection),
                "queue": queue,
                "today": counters,
                "events": get_runtime_state(connection, "event_stream", {}),
                "downloader": get_runtime_state(connection, "downloader", {}),
                "worker": get_runtime_state(connection, "worker", {}),
                "window_recovery": get_runtime_state(connection, "window_recovery", {}),
                # The blocked-action alarm used to be written and never read by
                # anything, so a policy violation raised a flag nobody looked
                # at.  linux/watchdog.py is the out-of-band consumer; this puts
                # it in front of anyone with the console open too.
                "critical_alarm": get_runtime_state(connection, "critical_alarm", {}),
            }
        with self._cache_lock:
            self._stats_cache = (time.time(), payload)
        return dict(payload)

    def setup_status(self, groups: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        settings = self.config.collector_settings()
        onebot = settings.get("onebot", {})
        configured_groups = groups if groups is not None else self.list_groups()
        checks = [
            {
                "key": "collector_config",
                "label": "采集配置",
                "ok": self.config.collector_config_path.is_file(),
                "detail": str(self.config.collector_config_path),
            },
            {
                "key": "storage",
                "label": "图片仓库",
                "ok": self.config.storage_root().is_dir(),
                "detail": str(self.config.storage_root()),
            },
            {
                "key": "onebot_http",
                "label": "OneBot HTTP 配置",
                "ok": bool(onebot.get("base_url")),
                "detail": str(onebot.get("base_url") or "未配置"),
            },
            {
                "key": "onebot_ws",
                "label": "OneBot 事件流配置",
                "ok": bool(onebot.get("ws_url")),
                "detail": str(onebot.get("ws_url") or "未配置"),
            },
            {
                "key": "groups",
                "label": "监听群聊",
                "ok": any(bool(row.get("enabled")) for row in configured_groups),
                "detail": "已配置" if configured_groups else "尚未选择",
            },
        ]
        with closing(self.connect()) as connection:
            completed = bool(get_setting(connection, "setup_completed", False))
        return {"completed": completed and all(row["ok"] for row in checks), "checks": checks}

    def complete_setup(self) -> dict[str, Any]:
        status = self.setup_status()
        if not all(row["ok"] for row in status["checks"]):
            raise ValueError("setup checks have not passed")
        with closing(self.connect()) as connection:
            set_setting(connection, "setup_completed", True)
        return self.setup_status()

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
                (int(time.time()), identity[:320], action[:512], int(status_code), source_ip),
            )
            connection.commit()

    def list_remote_audit(self, limit: int = 100) -> list[dict[str, Any]]:
        with closing(self.connect()) as connection:
            rows = connection.execute(
                """
                SELECT id, created_at, identity, action, status_code, source_ip
                FROM remote_audit ORDER BY id DESC LIMIT ?
                """,
                (max(1, min(int(limit), 1000)),),
            ).fetchall()
            return [dict(row) for row in rows]
