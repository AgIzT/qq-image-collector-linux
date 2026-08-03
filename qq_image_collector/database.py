from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlsplit


FILENAME_TIMEZONE = dt.timezone(dt.timedelta(hours=8))
ASSET_PARSER_VERSION = "5"
FINAL_CATEGORIES = (
    "NovelAI",
    "ComfyUI",
    "NAI含参但不可直接读取的",
    "其他模型生成",
)
TERMINAL_IMAGE_STATUSES = frozenset(
    {
        "accepted",
        "rejected_no_metadata",
        "filtered_gif",
        "failed_terminal",
        "expired",
        "legacy_failed",
    }
)
COUNTER_COLUMNS = frozenset(
    {
        "events",
        "group_messages",
        "images_seen",
        "image_segments",
        "queued_high",
        "queued_medium",
        "queued_low",
        "filtered_gif",
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
    }
)


def category_for_source(source: str | None) -> str:
    if source == "comfyui":
        return "ComfyUI"
    if source == "novelai":
        return "NovelAI"
    if source in {"novelai-unreadable", "novelai-stealth", "novelai-ztxt"}:
        return "NAI含参但不可直接读取的"
    return "其他模型生成"


def _ensure_columns(
    connection: sqlite3.Connection,
    table: str,
    columns: dict[str, str],
) -> set[str]:
    existing = {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}
    added: set[str] = set()
    for name, declaration in columns.items():
        if name not in existing:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")
            added.add(name)
    return added


def connect_database(
    path: str | Path,
    *,
    initialize: bool = True,
) -> sqlite3.Connection:
    database = Path(path)
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database, timeout=5)
    connection.execute("PRAGMA busy_timeout=5000")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA temp_store=MEMORY")
    if not initialize:
        return connection
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA wal_autocheckpoint=10000")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS images (
            group_id TEXT NOT NULL,
            message_id TEXT NOT NULL,
            message_seq TEXT,
            image_index INTEGER NOT NULL,
            sent_at INTEGER,
            file_token TEXT,
            declared_size INTEGER,
            status TEXT NOT NULL,
            sha256 TEXT,
            local_path TEXT,
            metadata_source TEXT,
            metadata_json TEXT,
            error TEXT,
            updated_at INTEGER NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_retry_at INTEGER NOT NULL DEFAULT 0,
            resolver TEXT NOT NULL DEFAULT 'event-cdn',
            resolver_json TEXT,
            group_uin TEXT,
            group_name TEXT,
            sender_uin TEXT,
            sender_uid TEXT,
            sender_member_name TEXT,
            sender_nickname TEXT,
            sender_remark_name TEXT,
            message_text TEXT,
            is_imported INTEGER,
            is_online INTEGER,
            original_flag INTEGER,
            queue_priority INTEGER NOT NULL DEFAULT 2,
            url_expires_at INTEGER NOT NULL DEFAULT 0,
            discovered_at INTEGER,
            collected_at INTEGER,
            PRIMARY KEY (group_id, message_id, image_index)
        )
        """
    )
    added_image_columns = _ensure_columns(
        connection,
        "images",
        {
            "message_seq": "TEXT",
            "sent_at": "INTEGER",
            "file_token": "TEXT",
            "declared_size": "INTEGER",
            "sha256": "TEXT",
            "local_path": "TEXT",
            "metadata_source": "TEXT",
            "metadata_json": "TEXT",
            "error": "TEXT",
            "attempts": "INTEGER NOT NULL DEFAULT 0",
            "next_retry_at": "INTEGER NOT NULL DEFAULT 0",
            "resolver": "TEXT NOT NULL DEFAULT 'event-cdn'",
            "resolver_json": "TEXT",
            "group_uin": "TEXT",
            "group_name": "TEXT",
            "sender_uin": "TEXT",
            "sender_uid": "TEXT",
            "sender_member_name": "TEXT",
            "sender_nickname": "TEXT",
            "sender_remark_name": "TEXT",
            "message_text": "TEXT",
            "is_imported": "INTEGER",
            "is_online": "INTEGER",
            "original_flag": "INTEGER",
            "queue_priority": "INTEGER NOT NULL DEFAULT 2",
            "url_expires_at": "INTEGER NOT NULL DEFAULT 0",
            "discovered_at": "INTEGER",
            "collected_at": "INTEGER",
        },
    )
    if {"queue_priority", "url_expires_at"} & added_image_columns:
        connection.execute(
            """
            UPDATE images SET
                queue_priority=coalesce(
                    CAST(json_extract(resolver_json, '$.priority') AS INTEGER), 2
                ),
                url_expires_at=coalesce(
                    CAST(json_extract(resolver_json, '$.url_expires_at') AS INTEGER), 0
                ),
                discovered_at=coalesce(discovered_at, updated_at)
            WHERE status IN ('queued','deferred','downloading')
            """
        )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS assets (
            sha256 TEXT PRIMARY KEY,
            local_path TEXT NOT NULL UNIQUE,
            category TEXT NOT NULL,
            file_extension TEXT,
            file_size INTEGER,
            width INTEGER,
            height INTEGER,
            metadata_source TEXT,
            metadata_json TEXT,
            parser_version TEXT,
            canonical_group_id TEXT,
            canonical_sender_uin TEXT,
            canonical_message_id TEXT,
            canonical_image_index INTEGER,
            canonical_sent_at INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    _ensure_columns(
        connection,
        "assets",
        {
            "category": "TEXT NOT NULL DEFAULT '其他模型生成'",
            "file_extension": "TEXT",
            "file_size": "INTEGER",
            "width": "INTEGER",
            "height": "INTEGER",
            "metadata_source": "TEXT",
            "metadata_json": "TEXT",
            "parser_version": "TEXT",
            "canonical_group_id": "TEXT",
            "canonical_sender_uin": "TEXT",
            "canonical_message_id": "TEXT",
            "canonical_image_index": "INTEGER",
            "canonical_sent_at": "INTEGER",
            "updated_at": "INTEGER",
        },
    )
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
            event_status TEXT NOT NULL DEFAULT 'idle',
            last_message_id TEXT,
            last_message_seq TEXT,
            last_message_time INTEGER,
            last_event_at INTEGER,
            last_image_at INTEGER,
            gap_status TEXT NOT NULL DEFAULT 'idle',
            gap_started_at INTEGER,
            gap_finished_at INTEGER,
            gap_error TEXT,
            updated_at INTEGER NOT NULL
        )
        """
    )
    _ensure_columns(
        connection,
        "group_runtime",
        {
            "event_status": "TEXT NOT NULL DEFAULT 'idle'",
            "last_message_id": "TEXT",
            "last_message_seq": "TEXT",
            "last_message_time": "INTEGER",
            "last_event_at": "INTEGER",
            "last_image_at": "INTEGER",
            "gap_status": "TEXT NOT NULL DEFAULT 'idle'",
            "gap_started_at": "INTEGER",
            "gap_finished_at": "INTEGER",
            "gap_error": "TEXT",
        },
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
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS runtime_state (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS hourly_counters (
            bucket_start INTEGER PRIMARY KEY,
            events INTEGER NOT NULL DEFAULT 0,
            group_messages INTEGER NOT NULL DEFAULT 0,
            images_seen INTEGER NOT NULL DEFAULT 0,
            image_segments INTEGER NOT NULL DEFAULT 0,
            queued_high INTEGER NOT NULL DEFAULT 0,
            queued_medium INTEGER NOT NULL DEFAULT 0,
            queued_low INTEGER NOT NULL DEFAULT 0,
            filtered_gif INTEGER NOT NULL DEFAULT 0,
            cdn_requests INTEGER NOT NULL DEFAULT 0,
            cdn_downloads INTEGER NOT NULL DEFAULT 0,
            cdn_bytes INTEGER NOT NULL DEFAULT 0,
            cdn_400 INTEGER NOT NULL DEFAULT 0,
            cdn_403 INTEGER NOT NULL DEFAULT 0,
            cdn_429 INTEGER NOT NULL DEFAULT 0,
            history_calls INTEGER NOT NULL DEFAULT 0,
            window_history_calls INTEGER NOT NULL DEFAULT 0,
            get_image_blocked INTEGER NOT NULL DEFAULT 0,
            accepted INTEGER NOT NULL DEFAULT 0,
            rejected INTEGER NOT NULL DEFAULT 0,
            duplicates INTEGER NOT NULL DEFAULT 0,
            failed INTEGER NOT NULL DEFAULT 0,
            expired INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL
        )
        """
    )
    _ensure_columns(
        connection,
        "hourly_counters",
        {
            "image_segments": "INTEGER NOT NULL DEFAULT 0",
            "cdn_requests": "INTEGER NOT NULL DEFAULT 0",
            "cdn_400": "INTEGER NOT NULL DEFAULT 0",
            "expired": "INTEGER NOT NULL DEFAULT 0",
            "window_history_calls": "INTEGER NOT NULL DEFAULT 0",
        },
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS window_recovery_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id TEXT NOT NULL,
            not_before INTEGER NOT NULL,
            not_after INTEGER NOT NULL,
            anchor_mode TEXT NOT NULL DEFAULT 'legacy-forward',
            start_anchor_id TEXT NOT NULL,
            start_anchor_seq TEXT NOT NULL,
            start_anchor_time INTEGER NOT NULL,
            next_anchor_id TEXT NOT NULL,
            next_anchor_seq TEXT NOT NULL,
            next_anchor_time INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'probe',
            probe_ok INTEGER NOT NULL DEFAULT 0,
            pages INTEGER NOT NULL DEFAULT 0,
            history_calls INTEGER NOT NULL DEFAULT 0,
            messages_seen INTEGER NOT NULL DEFAULT 0,
            messages_in_window INTEGER NOT NULL DEFAULT 0,
            images_enqueued INTEGER NOT NULL DEFAULT 0,
            duplicates INTEGER NOT NULL DEFAULT 0,
            retry_count INTEGER NOT NULL DEFAULT 0,
            replay_count INTEGER NOT NULL DEFAULT 0,
            next_retry_at INTEGER NOT NULL DEFAULT 0,
            last_page_fingerprint TEXT,
            last_error TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            finished_at INTEGER,
            UNIQUE(group_id, not_before, not_after)
        )
        """
    )
    _ensure_columns(
        connection,
        "window_recovery_jobs",
        {"anchor_mode": "TEXT NOT NULL DEFAULT 'legacy-forward'"},
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS window_recovery_calls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id TEXT NOT NULL,
            not_before INTEGER NOT NULL,
            not_after INTEGER NOT NULL,
            called_at INTEGER NOT NULL,
            outcome TEXT NOT NULL,
            error TEXT
        )
        """
    )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_images_status_queue ON images(status, next_retry_at, discovered_at)")
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_images_claim_fifo
        ON images(discovered_at, queue_priority, sent_at, group_id, message_id,
                  image_index, next_retry_at)
        WHERE status IN ('queued','deferred')
        """
    )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_images_sha256 ON images(sha256)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_images_group_sent ON images(group_id, sent_at)")
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_images_group_seq ON images(group_id, message_seq, image_index)"
    )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_jobs_active ON jobs(status, created_at)")
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_window_recovery_ready
        ON window_recovery_jobs(status, next_retry_at, updated_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_window_recovery_calls_time
        ON window_recovery_calls(called_at)
        """
    )
    connection.execute("DROP VIEW IF EXISTS occurrences")
    connection.execute("CREATE VIEW occurrences AS SELECT * FROM images")

    # The event-driven collector never retries any non-terminal work item from
    # the legacy OneBot/QCE resolver.  This also catches queued/downloading rows
    # left behind by an abrupt cutover, not just the six known `failed` rows.
    connection.execute(
        """
        UPDATE images SET status='legacy_failed', next_retry_at=0, updated_at=?
        WHERE status NOT IN ('accepted','rejected_no_metadata','filtered_gif',
                             'failed_terminal','expired','legacy_failed')
          AND (coalesce(resolver, '') <> 'event-cdn' OR status='failed')
        """,
        (int(time.time()),),
    )
    now = int(time.time())
    connection.execute(
        """
        UPDATE jobs SET status='cancelled', cancel_requested=1, finished_at=?,
            updated_at=?, error=coalesce(error, 'cancelled during event-pipeline migration')
        WHERE kind<>'gap_recovery' AND status IN ('queued','running')
        """,
        (now, now),
    )
    for table in ("group_cursors", "deep_history_cursors", "qce_recent_cursors"):
        connection.execute(f"DROP TABLE IF EXISTS {table}")
    connection.commit()
    return connection


def _retry_locked_write(
    connection: sqlite3.Connection,
    operation: Callable[[], None],
    *,
    attempts: int = 4,
) -> None:
    for attempt in range(max(1, attempts)):
        try:
            operation()
            connection.commit()
            return
        except sqlite3.OperationalError as exc:
            connection.rollback()
            if "locked" not in str(exc).casefold() or attempt + 1 >= attempts:
                raise
            time.sleep(min(1.0, 0.1 * (2**attempt)))


def set_runtime_state(connection: sqlite3.Connection, key: str, value: Any) -> None:
    now = int(time.time())
    payload = json.dumps(value, ensure_ascii=False)

    def write() -> None:
        connection.execute(
            """
            INSERT INTO runtime_state(key, value_json, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at
            """,
            (key, payload, now),
        )

    _retry_locked_write(connection, write)


def get_runtime_state(connection: sqlite3.Connection, key: str, default: Any = None) -> Any:
    row = connection.execute("SELECT value_json FROM runtime_state WHERE key=?", (key,)).fetchone()
    if not row:
        return default
    try:
        return json.loads(str(row[0]))
    except (TypeError, ValueError):
        return default


def increment_counter(
    connection: sqlite3.Connection,
    column: str,
    amount: int = 1,
    *,
    timestamp: int | None = None,
) -> None:
    if column not in COUNTER_COLUMNS:
        raise ValueError(f"unknown counter: {column}")
    now = int(timestamp or time.time())
    bucket = now - now % 3600
    def write() -> None:
        connection.execute(
            "INSERT OR IGNORE INTO hourly_counters(bucket_start, updated_at) VALUES (?, ?)",
            (bucket, now),
        )
        connection.execute(
            f"UPDATE hourly_counters SET {column}={column}+?, updated_at=? WHERE bucket_start=?",
            (int(amount), now, bucket),
        )

    _retry_locked_write(connection, write)


def counter_sum(
    connection: sqlite3.Connection,
    column: str,
    since: int,
) -> int:
    if column not in COUNTER_COLUMNS:
        raise ValueError(f"unknown counter: {column}")
    row = connection.execute(
        f"SELECT coalesce(sum({column}), 0) FROM hourly_counters WHERE bucket_start>=?",
        (int(since) - int(since) % 3600,),
    ).fetchone()
    return int(row[0] or 0)


def local_day_start(timestamp: int | None = None) -> int:
    now = dt.datetime.fromtimestamp(timestamp or time.time(), FILENAME_TIMEZONE)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(start.timestamp())


def _priority(item: dict[str, Any]) -> int:
    original = item.get("original_flag")
    size = int(item.get("declared_size") or 0)
    resolver = item.get("resolver_data") or {}
    emoji_signal = bool(resolver.get("emoji_signal"))
    width = int(resolver.get("width") or 0)
    height = int(resolver.get("height") or 0)
    filename = str(item.get("file") or "").casefold()
    if original in (1, True):
        return 0
    # NapCat versions differ in whether they expose picElement.original.  When
    # it is absent, dimensions and extension keep substantial PNG/JPEG images
    # ahead of tiny emoji without treating the signal as a rejection rule.
    substantial = size >= 128 * 1024 or (width >= 512 and height >= 512)
    likely_still = not filename.endswith(".gif")
    if original is None and substantial and not emoji_signal and likely_still:
        return 1
    return 2


def enqueue_image(connection: sqlite3.Connection, item: dict[str, Any]) -> bool:
    now = int(time.time())
    priority = _priority(item)
    resolver_data = dict(item.get("resolver_data") or {})
    resolver_data["priority"] = priority
    url_expires_at = int(resolver_data.get("url_expires_at") or 0)
    url_material = "\0".join(
        str(resolver_data.get(key) or "") for key in ("url", "origin_url")
    )
    if url_material.strip("\0"):
        resolver_data["url_fingerprint"] = hashlib.sha256(
            url_material.encode("utf-8")
        ).hexdigest()
    group_id = str(item["group_id"])
    message_id = str(item["message_id"])
    message_seq = str(item.get("message_seq") or "")
    image_index = int(item.get("image_index") or 0)
    # History responses use NapCat's process-local short message ID, while
    # live debug events carry the durable NT msgId.  A stable real_seq/msgSeq
    # match keeps both paths in one row without ever replacing the live cursor.
    if message_seq:
        same_sequence = connection.execute(
            """
            SELECT message_id FROM images
            WHERE group_id=? AND message_seq=? AND image_index=?
            ORDER BY discovered_at LIMIT 1
            """,
            (group_id, message_seq, image_index),
        ).fetchone()
        if same_sequence:
            message_id = str(same_sequence[0])
    key = (group_id, message_id, image_index)
    existed = connection.execute(
        "SELECT status, resolver_json FROM images WHERE group_id=? AND message_id=? AND image_index=?",
        key,
    ).fetchone()
    if existed and str(existed[0]) == "expired" and resolver_data.get("url_fingerprint"):
        try:
            previous_resolver = json.loads(str(existed[1] or "{}"))
        except (TypeError, ValueError):
            previous_resolver = {}
        if previous_resolver.get("url_fingerprint") != resolver_data["url_fingerprint"]:
            connection.execute(
                """
                UPDATE images SET message_seq=?, sent_at=?, file_token=?, declared_size=?,
                    status='queued', sha256=NULL, local_path=NULL, metadata_source=NULL,
                    metadata_json=NULL, error=NULL, updated_at=?, attempts=0,
                    next_retry_at=0, resolver='event-cdn', resolver_json=?,
                    queue_priority=?, url_expires_at=?,
                    group_uin=coalesce(?, group_uin), group_name=coalesce(?, group_name),
                    sender_uin=coalesce(?, sender_uin), sender_uid=coalesce(?, sender_uid),
                    sender_member_name=coalesce(?, sender_member_name),
                    sender_nickname=coalesce(?, sender_nickname),
                    sender_remark_name=coalesce(?, sender_remark_name),
                    message_text=coalesce(?, message_text), is_imported=0, is_online=1,
                    original_flag=coalesce(?, original_flag), discovered_at=?, collected_at=NULL
                WHERE group_id=? AND message_id=? AND image_index=?
                """,
                (
                    message_seq,
                    int(item.get("sent_at") or 0),
                    str(item.get("file") or ""),
                    int(item.get("declared_size") or 0),
                    now,
                    json.dumps(resolver_data, ensure_ascii=False),
                    priority,
                    url_expires_at,
                    str(item.get("group_uin") or group_id),
                    item.get("group_name"),
                    item.get("sender_uin"),
                    item.get("sender_uid"),
                    item.get("sender_member_name"),
                    item.get("sender_nickname"),
                    item.get("sender_remark_name"),
                    item.get("message_text"),
                    item.get("original_flag"),
                    int(item.get("discovered_at") or now),
                    group_id,
                    message_id,
                    image_index,
                ),
            )
            connection.commit()
            increment_counter(connection, ("queued_high", "queued_medium", "queued_low")[priority])
            return True
    connection.execute(
        """
        INSERT INTO images (
            group_id, message_id, message_seq, image_index, sent_at, file_token,
            declared_size, status, updated_at, attempts, next_retry_at,
            resolver, resolver_json, group_uin, group_name, sender_uin,
            sender_uid, sender_member_name, sender_nickname, sender_remark_name,
            message_text, is_imported, is_online, original_flag,
            queue_priority, url_expires_at, discovered_at, collected_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, 0, 0, 'event-cdn', ?,
                  ?, ?, ?, ?, ?, ?, ?, ?, 0, 1, ?, ?, ?, ?, NULL)
        ON CONFLICT(group_id, message_id, image_index) DO UPDATE SET
            message_seq=excluded.message_seq,
            sent_at=coalesce(images.sent_at, excluded.sent_at),
            file_token=excluded.file_token,
            declared_size=excluded.declared_size,
            resolver_json=CASE
                WHEN images.status IN ('accepted','rejected_no_metadata','filtered_gif',
                    'failed_terminal','expired','legacy_failed')
                THEN images.resolver_json ELSE excluded.resolver_json END,
            status=CASE
                WHEN images.status IN ('accepted','rejected_no_metadata','filtered_gif',
                    'failed_terminal','expired','legacy_failed')
                THEN images.status ELSE 'queued' END,
            next_retry_at=CASE
                WHEN images.status IN ('accepted','rejected_no_metadata','filtered_gif',
                    'failed_terminal','expired','legacy_failed')
                THEN images.next_retry_at ELSE 0 END,
            group_uin=coalesce(images.group_uin, excluded.group_uin),
            group_name=coalesce(images.group_name, excluded.group_name),
            sender_uin=coalesce(images.sender_uin, excluded.sender_uin),
            sender_uid=coalesce(images.sender_uid, excluded.sender_uid),
            sender_member_name=coalesce(images.sender_member_name, excluded.sender_member_name),
            sender_nickname=coalesce(images.sender_nickname, excluded.sender_nickname),
            sender_remark_name=coalesce(images.sender_remark_name, excluded.sender_remark_name),
            message_text=coalesce(images.message_text, excluded.message_text),
            original_flag=coalesce(images.original_flag, excluded.original_flag),
            queue_priority=CASE
                WHEN images.status IN ('accepted','rejected_no_metadata','filtered_gif',
                    'failed_terminal','expired','legacy_failed')
                THEN images.queue_priority ELSE excluded.queue_priority END,
            url_expires_at=CASE
                WHEN images.status IN ('accepted','rejected_no_metadata','filtered_gif',
                    'failed_terminal','expired','legacy_failed')
                THEN images.url_expires_at ELSE excluded.url_expires_at END,
            updated_at=excluded.updated_at
        """,
        (
            group_id,
            message_id,
            message_seq,
            image_index,
            int(item.get("sent_at") or 0),
            str(item.get("file") or ""),
            int(item.get("declared_size") or 0),
            now,
            json.dumps(resolver_data, ensure_ascii=False),
            str(item.get("group_uin") or item["group_id"]),
            item.get("group_name"),
            item.get("sender_uin"),
            item.get("sender_uid"),
            item.get("sender_member_name"),
            item.get("sender_nickname"),
            item.get("sender_remark_name"),
            item.get("message_text"),
            item.get("original_flag"),
            priority,
            url_expires_at,
            int(item.get("discovered_at") or now),
        ),
    )
    connection.commit()
    inserted = existed is None
    if inserted:
        increment_counter(connection, ("queued_high", "queued_medium", "queued_low")[priority])
    return inserted


def recover_inflight(connection: sqlite3.Connection) -> int:
    cursor = connection.execute(
        "UPDATE images SET status='queued', updated_at=? WHERE status='downloading'",
        (int(time.time()),),
    )
    connection.commit()
    return int(cursor.rowcount or 0)


def claim_next_image(
    connection: sqlite3.Connection,
    *,
    expiry_urgent_seconds: int = 3600,
) -> sqlite3.Row | None:
    # Kept for API compatibility. FIFO prevents old event URLs from aging
    # behind newer work; URL refresh is handled immediately by the worker.
    del expiry_urgent_seconds
    connection.row_factory = sqlite3.Row
    now = int(time.time())
    connection.execute("BEGIN IMMEDIATE")
    try:
        row = connection.execute(
            """
            SELECT * FROM images INDEXED BY idx_images_claim_fifo
            WHERE status IN ('queued','deferred') AND next_retry_at<=?
            ORDER BY discovered_at, queue_priority, sent_at,
                     group_id, message_id, image_index
            LIMIT 1
            """,
            (now,),
        ).fetchone()
        if row:
            connection.execute(
                """
                UPDATE images SET status='downloading', attempts=attempts+1, updated_at=?
                WHERE group_id=? AND message_id=? AND image_index=?
                """,
                (now, row["group_id"], row["message_id"], row["image_index"]),
            )
        connection.commit()
        return row
    except Exception:
        connection.rollback()
        raise


def defer_image(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    delay_seconds: int,
    error: str,
) -> None:
    now = int(time.time())
    connection.execute(
        """
        UPDATE images SET status='deferred', next_retry_at=?, error=?, updated_at=?
        WHERE group_id=? AND message_id=? AND image_index=?
        """,
        (
            now + max(1, int(delay_seconds)),
            error[:2048],
            now,
            row["group_id"],
            row["message_id"],
            row["image_index"],
        ),
    )
    connection.commit()


def _redacted_resolver(value: str | None, **extra: Any) -> str:
    try:
        data = json.loads(value or "{}")
    except (TypeError, ValueError):
        data = {}
    url = str(data.pop("url", "") or "")
    origin_url = str(data.pop("origin_url", "") or "")
    data.pop("raw", None)
    chosen = url or origin_url
    if chosen:
        try:
            data["url_host"] = (urlsplit(chosen).hostname or "").lower()
        except ValueError:
            data["url_host"] = "invalid"
    data.update({key: value for key, value in extra.items() if value is not None})
    return json.dumps(data, ensure_ascii=False)


def finish_image(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    status: str,
    sha256: str | None = None,
    local_path: str | None = None,
    metadata_source: str | None = None,
    metadata_json: str | None = None,
    error: str | None = None,
    http_status: int | None = None,
) -> None:
    if status not in TERMINAL_IMAGE_STATUSES:
        raise ValueError(f"not a terminal image status: {status}")
    now = int(time.time())
    resolver_json = _redacted_resolver(
        row["resolver_json"],
        terminal_status=status,
        http_status=http_status,
    )
    connection.execute(
        """
        UPDATE images SET status=?, sha256=?, local_path=?, metadata_source=?,
            metadata_json=?, error=?, next_retry_at=0, resolver_json=?,
            collected_at=?, updated_at=?
        WHERE group_id=? AND message_id=? AND image_index=?
        """,
        (
            status,
            sha256,
            local_path,
            metadata_source,
            metadata_json,
            error[:2048] if error else None,
            resolver_json,
            now,
            now,
            row["group_id"],
            row["message_id"],
            row["image_index"],
        ),
    )
    connection.commit()


def queue_snapshot(connection: sqlite3.Connection) -> dict[str, Any]:
    now = int(time.time())
    row = connection.execute(
        """
        SELECT count(*), min(discovered_at),
               sum(CASE WHEN queue_priority=0 THEN 1 ELSE 0 END),
               sum(CASE WHEN queue_priority=1 THEN 1 ELSE 0 END),
               sum(CASE WHEN queue_priority=2 THEN 1 ELSE 0 END),
               sum(CASE WHEN url_expires_at>0 THEN 1 ELSE 0 END),
               sum(CASE WHEN url_expires_at BETWEEN 1 AND ? THEN 1 ELSE 0 END)
        FROM images WHERE status IN ('queued','deferred','downloading')
        """,
        (now + 3600,),
    ).fetchone()
    oldest = int(row[1] or 0)
    return {
        "depth": int(row[0] or 0),
        "oldest_at": oldest or None,
        "oldest_age_seconds": max(0, now - oldest) if oldest else 0,
        "high": int(row[2] or 0),
        "medium": int(row[3] or 0),
        "low": int(row[4] or 0),
        "expiring": int(row[5] or 0),
        "expiry_urgent": int(row[6] or 0),
    }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def accepted_path_for(
    storage_root: Path,
    digest: str,
    extension: str,
    source: str | None,
    sent_at: int,
    group_id: str | None,
    sender_uin: str | None,
    digest_length: int = 10,
) -> Path:
    try:
        timestamp = dt.datetime.fromtimestamp(int(sent_at), FILENAME_TIMEZONE).strftime(
            "%Y-%m-%d_%H-%M-%S"
        )
    except (OSError, OverflowError, TypeError, ValueError):
        timestamp = "unknown-time"
    group = str(group_id or "")
    sender = str(sender_uin or "")
    group = group if group.isdigit() else "unknown"
    sender = sender if sender.isdigit() else "unknown"
    name = f"{timestamp}_g{group}_u{sender}_{digest[:max(10, digest_length)]}{extension}"
    return storage_root / "final" / category_for_source(source) / name


def collision_safe_path(
    storage_root: Path,
    digest: str,
    extension: str,
    source: str | None,
    item: dict[str, Any],
) -> Path:
    for length in (10, 16, len(digest)):
        candidate = accepted_path_for(
            storage_root,
            digest,
            extension,
            source,
            int(item.get("sent_at") or 0),
            str(item.get("group_id") or ""),
            str(item.get("sender_uin") or ""),
            length,
        )
        if not candidate.exists() or sha256_file(candidate) == digest:
            return candidate
    raise FileExistsError("could not choose a collision-free accepted image name")


def store_asset(
    connection: sqlite3.Connection,
    temp_path: Path,
    *,
    digest: str,
    extension: str,
    source: str | None,
    metadata_json: str,
    width: int,
    height: int,
    image_format: str,
    item: dict[str, Any],
    storage_root: Path,
) -> tuple[Path, bool]:
    existing = connection.execute(
        "SELECT local_path FROM assets WHERE sha256=?",
        (digest,),
    ).fetchone()
    if existing:
        path = Path(str(existing[0]))
        if path.is_file():
            temp_path.unlink(missing_ok=True)
            return path, True

    destination = collision_safe_path(storage_root, digest, extension, source, item)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        temp_path.unlink(missing_ok=True)
        duplicate = True
    else:
        os.replace(temp_path, destination)
        duplicate = False
    now = int(time.time())
    connection.execute(
        """
        INSERT OR IGNORE INTO assets (
            sha256, local_path, category, file_extension, file_size, width, height,
            metadata_source, metadata_json, parser_version, canonical_group_id,
            canonical_sender_uin, canonical_message_id, canonical_image_index,
            canonical_sent_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            digest,
            str(destination),
            category_for_source(source),
            extension.lower(),
            destination.stat().st_size,
            int(width),
            int(height),
            source,
            metadata_json,
            ASSET_PARSER_VERSION,
            str(item.get("group_id") or "") or None,
            str(item.get("sender_uin") or "") or None,
            str(item.get("message_id") or "") or None,
            int(item.get("image_index") or 0),
            int(item.get("sent_at") or 0) or None,
            now,
            now,
        ),
    )
    connection.commit()
    return destination, duplicate


def ensure_final_directories(storage_root: Path) -> None:
    for category in FINAL_CATEGORIES:
        (storage_root / "final" / category).mkdir(parents=True, exist_ok=True)


def rows_to_dicts(rows: Iterable[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]
