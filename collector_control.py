from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Iterable
from typing import Any


RUNTIME_FIELDS = frozenset(
    {
        "event_status",
        "last_message_id",
        "last_message_time",
        "last_event_at",
        "last_image_at",
        "gap_status",
        "gap_started_at",
        "gap_finished_at",
        "gap_error",
    }
)


def initialize_control_schema(connection: sqlite3.Connection) -> None:
    # The canonical schema is initialized by qq_image_collector.database.
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
            INSERT INTO monitored_groups(group_id, enabled, created_at, updated_at)
            VALUES (?, 1, ?, ?)
            ON CONFLICT(group_id) DO NOTHING
            """,
            (group_id, now, now),
        )
        connection.execute(
            """
            INSERT INTO group_runtime(group_id, updated_at) VALUES (?, ?)
            ON CONFLICT(group_id) DO NOTHING
            """,
            (group_id, now),
        )
    connection.commit()


def enabled_groups(connection: sqlite3.Connection, fallback: Iterable[str] = ()) -> list[str]:
    rows = connection.execute(
        "SELECT group_id FROM monitored_groups WHERE enabled=1 ORDER BY created_at, group_id"
    ).fetchall()
    if rows:
        return [str(row[0]) for row in rows]
    values = [str(value) for value in fallback if str(value)]
    if values:
        seed_monitored_groups(connection, values)
    return values


def set_group_enabled(
    connection: sqlite3.Connection,
    group_id: str,
    enabled: bool,
    display_name: str | None = None,
) -> None:
    group_id = str(group_id).strip()
    if not group_id.isdigit():
        raise ValueError("group_id must contain digits only")
    now = int(time.time())
    connection.execute(
        """
        INSERT INTO monitored_groups(group_id, display_name, enabled, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(group_id) DO UPDATE SET
            display_name=coalesce(excluded.display_name, monitored_groups.display_name),
            enabled=excluded.enabled,
            updated_at=excluded.updated_at
        """,
        (group_id, display_name, int(bool(enabled)), now, now),
    )
    connection.execute(
        """
        INSERT INTO group_runtime(group_id, updated_at) VALUES (?, ?)
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
        raise ValueError(f"unsupported group runtime fields: {sorted(invalid)}")
    now = int(time.time())
    connection.execute(
        """
        INSERT INTO group_runtime(group_id, updated_at) VALUES (?, ?)
        ON CONFLICT(group_id) DO NOTHING
        """,
        (str(group_id), now),
    )
    if values:
        assignments = ", ".join(f"{key}=?" for key in values)
        connection.execute(
            f"UPDATE group_runtime SET {assignments}, updated_at=? WHERE group_id=?",
            (*values.values(), now, str(group_id)),
        )
    connection.commit()


def get_setting(connection: sqlite3.Connection, key: str, default: Any = None) -> Any:
    row = connection.execute("SELECT value_json FROM app_settings WHERE key=?", (key,)).fetchone()
    if not row:
        return default
    try:
        return json.loads(str(row[0]))
    except (TypeError, ValueError):
        return default


def set_setting(connection: sqlite3.Connection, key: str, value: Any) -> None:
    now = int(time.time())
    connection.execute(
        """
        INSERT INTO app_settings(key, value_json, updated_at) VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at
        """,
        (key, json.dumps(value, ensure_ascii=False), now),
    )
    connection.commit()


def enqueue_job(connection: sqlite3.Connection, kind: str, group_id: str) -> int:
    if kind != "gap_recovery":
        raise ValueError("only bounded gap_recovery jobs are supported")
    active = connection.execute(
        """
        SELECT id FROM jobs
        WHERE kind='gap_recovery' AND group_id=? AND status IN ('queued','running')
        LIMIT 1
        """,
        (str(group_id),),
    ).fetchone()
    if active:
        raise ValueError("this group already has an active gap recovery job")
    now = int(time.time())
    cursor = connection.execute(
        """
        INSERT INTO jobs(kind, group_id, status, created_at, updated_at)
        VALUES ('gap_recovery', ?, 'queued', ?, ?)
        """,
        (str(group_id), now, now),
    )
    connection.commit()
    return int(cursor.lastrowid)


def request_job_cancel(connection: sqlite3.Connection, job_id: int) -> bool:
    now = int(time.time())
    cursor = connection.execute(
        """
        UPDATE jobs SET cancel_requested=1,
            status=CASE WHEN status='queued' THEN 'cancelled' ELSE status END,
            finished_at=CASE WHEN status='queued' THEN ? ELSE finished_at END,
            updated_at=?
        WHERE id=? AND status IN ('queued','running')
        """,
        (now, now, int(job_id)),
    )
    connection.commit()
    return int(cursor.rowcount or 0) > 0


def next_active_job(connection: sqlite3.Connection) -> sqlite3.Row | None:
    connection.row_factory = sqlite3.Row
    return connection.execute(
        "SELECT * FROM jobs WHERE status='queued' ORDER BY created_at, id LIMIT 1"
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
        raise ValueError(f"unsupported job fields: {sorted(invalid)}")
    if not values:
        return
    now = int(time.time())
    assignments = ", ".join(f"{key}=?" for key in values)
    connection.execute(
        f"UPDATE jobs SET {assignments}, updated_at=? WHERE id=?",
        (*values.values(), now, int(job_id)),
    )
    connection.commit()
