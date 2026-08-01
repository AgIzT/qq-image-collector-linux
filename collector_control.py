"""Persistent control-plane state shared by the collector and local console."""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Iterable


RUNTIME_FIELDS = {
    "recent_status",
    "recent_last_success",
    "recent_last_error",
    "recent_messages",
    "recent_images",
    "recent_accepted",
    "recent_rejected",
    "recent_failed",
    "recent_skipped",
    "backfill_status",
    "backfill_cursor_time",
    "backfill_completed",
    "backfill_last_success",
    "backfill_last_error",
    "backfill_messages",
    "backfill_images",
    "backfill_accepted",
    "backfill_rejected",
    "backfill_failed",
    "backfill_skipped",
}


def initialize_control_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS monitored_groups (
            group_id TEXT PRIMARY KEY,
            display_name TEXT,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS group_runtime (
            group_id TEXT PRIMARY KEY,
            recent_status TEXT NOT NULL DEFAULT 'idle',
            recent_last_success INTEGER,
            recent_last_error TEXT,
            recent_messages INTEGER NOT NULL DEFAULT 0,
            recent_images INTEGER NOT NULL DEFAULT 0,
            recent_accepted INTEGER NOT NULL DEFAULT 0,
            recent_rejected INTEGER NOT NULL DEFAULT 0,
            recent_failed INTEGER NOT NULL DEFAULT 0,
            recent_skipped INTEGER NOT NULL DEFAULT 0,
            backfill_status TEXT NOT NULL DEFAULT 'idle',
            backfill_cursor_time INTEGER,
            backfill_completed INTEGER NOT NULL DEFAULT 0,
            backfill_last_success INTEGER,
            backfill_last_error TEXT,
            backfill_messages INTEGER NOT NULL DEFAULT 0,
            backfill_images INTEGER NOT NULL DEFAULT 0,
            backfill_accepted INTEGER NOT NULL DEFAULT 0,
            backfill_rejected INTEGER NOT NULL DEFAULT 0,
            backfill_failed INTEGER NOT NULL DEFAULT 0,
            backfill_skipped INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,
            group_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            progress_pages INTEGER NOT NULL DEFAULT 0,
            cancel_requested INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            started_at INTEGER,
            updated_at INTEGER NOT NULL,
            finished_at INTEGER,
            error TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_jobs_active
        ON jobs(status, created_at)
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    connection.commit()


def seed_monitored_groups(connection: sqlite3.Connection, groups: Iterable[str]) -> None:
    now = int(time.time())
    for group_id in dict.fromkeys(str(value) for value in groups if str(value)):
        connection.execute(
            """
            INSERT INTO monitored_groups (group_id, enabled, created_at, updated_at)
            VALUES (?, 1, ?, ?)
            ON CONFLICT(group_id) DO NOTHING
            """,
            (group_id, now, now),
        )
        connection.execute(
            """
            INSERT INTO group_runtime (group_id, updated_at)
            VALUES (?, ?)
            ON CONFLICT(group_id) DO NOTHING
            """,
            (group_id, now),
        )
    connection.commit()


def enabled_groups(connection: sqlite3.Connection, fallback: Iterable[str] = ()) -> list[str]:
    rows = connection.execute(
        "SELECT group_id FROM monitored_groups WHERE enabled=1 ORDER BY created_at, group_id"
    ).fetchall()
    configured_count = int(
        connection.execute("SELECT count(*) FROM monitored_groups").fetchone()[0]
    )
    if configured_count:
        return [str(row[0]) for row in rows]
    return list(dict.fromkeys(str(value) for value in fallback if str(value)))


def set_group_enabled(
    connection: sqlite3.Connection,
    group_id: str,
    enabled: bool,
    display_name: str | None = None,
) -> None:
    now = int(time.time())
    connection.execute(
        """
        INSERT INTO monitored_groups (
            group_id, display_name, enabled, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(group_id) DO UPDATE SET
            display_name=coalesce(excluded.display_name, monitored_groups.display_name),
            enabled=excluded.enabled,
            updated_at=excluded.updated_at
        """,
        (group_id, display_name, int(enabled), now, now),
    )
    connection.execute(
        """
        INSERT INTO group_runtime (group_id, updated_at)
        VALUES (?, ?)
        ON CONFLICT(group_id) DO NOTHING
        """,
        (group_id, now),
    )
    connection.commit()


def update_group_runtime(
    connection: sqlite3.Connection,
    group_id: str,
    **values: Any,
) -> None:
    invalid = set(values) - RUNTIME_FIELDS
    if invalid:
        raise ValueError(f"Unsupported group runtime fields: {sorted(invalid)}")
    now = int(time.time())
    connection.execute(
        """
        INSERT INTO group_runtime (group_id, updated_at)
        VALUES (?, ?)
        ON CONFLICT(group_id) DO NOTHING
        """,
        (group_id, now),
    )
    if values:
        assignments = ", ".join(f"{field}=?" for field in values)
        connection.execute(
            f"UPDATE group_runtime SET {assignments}, updated_at=? WHERE group_id=?",
            (*values.values(), now, group_id),
        )
    connection.commit()


def get_setting(connection: sqlite3.Connection, key: str, default: Any = None) -> Any:
    row = connection.execute(
        "SELECT value_json FROM app_settings WHERE key=?", (key,)
    ).fetchone()
    if not row:
        return default
    try:
        return json.loads(str(row[0]))
    except json.JSONDecodeError:
        return default


def set_setting(connection: sqlite3.Connection, key: str, value: Any) -> None:
    now = int(time.time())
    connection.execute(
        """
        INSERT INTO app_settings (key, value_json, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value_json=excluded.value_json,
            updated_at=excluded.updated_at
        """,
        (key, json.dumps(value, ensure_ascii=False), now),
    )
    connection.commit()


def enqueue_job(connection: sqlite3.Connection, group_id: str, kind: str) -> int:
    if kind not in {"page", "continuous", "rescan"}:
        raise ValueError(f"Unsupported backfill job kind: {kind}")
    now = int(time.time())
    existing = connection.execute(
        """
        SELECT id FROM jobs
        WHERE group_id=? AND status IN ('queued', 'running')
        ORDER BY id LIMIT 1
        """,
        (group_id,),
    ).fetchone()
    if existing:
        raise ValueError(f"Group {group_id} already has an active job")
    cursor = connection.execute(
        """
        INSERT INTO jobs (kind, group_id, status, created_at, updated_at)
        VALUES (?, ?, 'queued', ?, ?)
        """,
        (kind, group_id, now, now),
    )
    connection.commit()
    return int(cursor.lastrowid)


def request_job_cancel(connection: sqlite3.Connection, job_id: int) -> bool:
    cursor = connection.execute(
        """
        UPDATE jobs SET cancel_requested=1, updated_at=?
        WHERE id=? AND status IN ('queued', 'running')
        """,
        (int(time.time()), job_id),
    )
    connection.commit()
    return bool(cursor.rowcount)


def next_active_job(connection: sqlite3.Connection) -> sqlite3.Row | tuple[Any, ...] | None:
    return connection.execute(
        """
        SELECT id, kind, group_id, status, progress_pages, cancel_requested
        FROM jobs
        WHERE status IN ('queued', 'running')
        ORDER BY CASE status WHEN 'running' THEN 0 ELSE 1 END, created_at, id
        LIMIT 1
        """
    ).fetchone()


def update_job(connection: sqlite3.Connection, job_id: int, **values: Any) -> None:
    allowed = {
        "status",
        "progress_pages",
        "cancel_requested",
        "started_at",
        "finished_at",
        "error",
    }
    invalid = set(values) - allowed
    if invalid:
        raise ValueError(f"Unsupported job fields: {sorted(invalid)}")
    if not values:
        return
    assignments = ", ".join(f"{field}=?" for field in values)
    connection.execute(
        f"UPDATE jobs SET {assignments}, updated_at=? WHERE id=?",
        (*values.values(), int(time.time()), job_id),
    )
    connection.commit()
