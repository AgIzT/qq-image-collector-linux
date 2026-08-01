"""NapCat QQ image collector with metadata filtering.

QCE (QQ Chat Exporter) supplies restart-safe raw message IDs.  The collector
uses each ID with OneBot history to rebuild the temporary image token, then
calls ``get_image`` to trigger the original-quality download.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import random
import shutil
import sqlite3
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Iterable

from collector_control import (
    enabled_groups,
    get_setting,
    initialize_control_schema,
    next_active_job,
    seed_monitored_groups,
    update_group_runtime,
    update_job,
)
from metadata_reader import extension_for_format, inspect_image


FILENAME_TIMEZONE = dt.timezone(dt.timedelta(hours=8))
PROVENANCE_TEXT_LIMIT = 8192
ASSET_PARSER_VERSION = "3"
FINAL_CATEGORIES = (
    "NovelAI",
    "ComfyUI",
    "NAI含参但不可直接读取的",
    "其他模型生成",
)


class OneBotError(RuntimeError):
    pass


class OneBotClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        timeout: int = 180,
        path_mappings: list[tuple[str, Path]] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.path_mappings = path_mappings or []

    @classmethod
    def from_napcat_config(
        cls,
        config_dir: Path,
        server_name: str,
        *,
        timeout: int = 180,
        path_mappings: list[tuple[str, Path]] | None = None,
    ) -> "OneBotClient":
        candidates = sorted(
            config_dir.glob("onebot11*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for candidate in candidates:
            data = json.loads(candidate.read_text(encoding="utf-8"))
            servers = data.get("network", {}).get("httpServers", [])
            for server in servers:
                if server.get("enable") and server.get("name") == server_name:
                    host = server.get("host", "127.0.0.1")
                    if host in {"0.0.0.0", "::"}:
                        host = "127.0.0.1"
                    return cls(
                        f"http://{host}:{int(server['port'])}",
                        str(server["token"]),
                        timeout,
                        path_mappings,
                    )
        raise OneBotError(f"Enabled OneBot HTTP server {server_name!r} was not found in {config_dir}")

    @staticmethod
    def _path_mappings(settings: dict[str, Any]) -> list[tuple[str, Path]]:
        mappings: list[tuple[str, Path]] = []
        for row in settings.get("path_mappings") or []:
            if not isinstance(row, dict):
                continue
            source = str(row.get("source") or "").replace("\\", "/").rstrip("/")
            target = str(row.get("target") or "")
            if source and target:
                mappings.append((source, Path(target)))
        return mappings

    @classmethod
    def from_settings(cls, settings: dict[str, Any]) -> "OneBotClient":
        timeout = int(settings.get("timeout_seconds", 180))
        mappings = cls._path_mappings(settings)
        base_url = str(settings.get("base_url") or "").strip()
        if base_url:
            token = str(settings.get("token") or "")
            token_file = str(settings.get("token_file") or "").strip()
            if token_file:
                token = Path(token_file).read_text(encoding="utf-8").strip()
            return cls(base_url, token, timeout, mappings)
        return cls.from_napcat_config(
            Path(settings["config_dir"]),
            str(settings["server_name"]),
            timeout=timeout,
            path_mappings=mappings,
        )

    def resolve_local_path(self, value: Any) -> Path:
        raw = str(value or "")
        direct = Path(raw)
        if direct.is_file():
            return direct
        normalized = raw.replace("\\", "/")
        for source, target in self.path_mappings:
            if normalized != source and not normalized.startswith(source + "/"):
                continue
            relative = normalized[len(source) :].lstrip("/")
            mapped = target.joinpath(*[part for part in relative.split("/") if part])
            if mapped.is_file():
                return mapped
        return direct

    def call(self, action: str, params: dict[str, Any]) -> Any:
        payload = json.dumps(params, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            f"{self.base_url}/{action}",
            data=payload,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = json.load(response)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise OneBotError(f"{action} request failed: {exc}") from exc
        if body.get("status") != "ok" or int(body.get("retcode", -1)) != 0:
            raise OneBotError(f"{action} failed: {body.get('message') or body.get('wording') or body}")
        return body.get("data")


class LocalApiError(RuntimeError):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class JsonApiClient:
    def __init__(self, base_url: str, token: str, timeout: int = 180) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    def call(self, path: str, params: dict[str, Any] | None = None, method: str = "POST") -> Any:
        payload = None
        if params is not None:
            payload = json.dumps(params, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=payload,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = json.load(response)
        except urllib.error.HTTPError as exc:
            try:
                detail = exc.read().decode("utf-8", "replace")
            except Exception:
                detail = str(exc)
            raise LocalApiError(f"{path} request failed: HTTP {exc.code}: {detail}", exc.code) from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise LocalApiError(f"{path} request failed: {exc}") from exc
        if not body.get("success"):
            error = body.get("error") or body
            raise LocalApiError(f"{path} failed: {error}")
        return body.get("data")


class QCEClient(JsonApiClient):
    def __init__(
        self,
        base_url: str,
        security_config: Path | list[Path],
        timeout: int = 180,
    ) -> None:
        self.security_configs = (
            list(security_config) if isinstance(security_config, list) else [security_config]
        )
        self.security_config = self._existing_security_config()
        self.security_mtime_ns = 0
        super().__init__(base_url, "", timeout)
        self.reload_token()

    def _existing_security_config(self) -> Path:
        for candidate in self.security_configs:
            if candidate.is_file():
                return candidate
        candidates = ", ".join(str(path) for path in self.security_configs)
        raise FileNotFoundError(f"QCE security.json was not found in: {candidates}")

    def reload_token(self) -> None:
        self.security_config = self._existing_security_config()
        security = json.loads(self.security_config.read_text(encoding="utf-8"))
        self.token = str(security["accessToken"])
        self.security_mtime_ns = self.security_config.stat().st_mtime_ns

    def call(self, path: str, params: dict[str, Any] | None = None, method: str = "POST") -> Any:
        current = self._existing_security_config()
        if current != self.security_config:
            self.security_config = current
            self.security_mtime_ns = 0
        if self.security_config.stat().st_mtime_ns != self.security_mtime_ns:
            self.reload_token()
        try:
            return super().call(path, params, method)
        except LocalApiError as exc:
            if exc.status not in {401, 403}:
                raise
            self.reload_token()
            return super().call(path, params, method)

    @classmethod
    def from_settings(cls, settings: dict[str, Any]) -> "QCEClient":
        candidates = [
            Path(value)
            for value in (
                settings.get("security_configs")
                or [settings.get("security_config")]
            )
            if value
        ]
        return cls(
            str(settings.get("base_url", "http://127.0.0.1:40653")),
            candidates,
            int(settings.get("timeout_seconds", 180)),
        )

    def fetch_page(
        self,
        group_id: str,
        start_time: int,
        end_time: int,
        limit: int,
        page: int = 1,
    ) -> tuple[list[dict[str, Any]], bool]:
        data = self.call(
            "/api/messages/fetch",
            {
                "peer": {"chatType": 2, "peerUid": group_id},
                "filter": {
                    "startTime": max(0, start_time) * 1000,
                    "endTime": max(0, end_time) * 1000 + 999,
                },
                "batchSize": max(100, limit),
                "page": page,
                "limit": limit,
            },
        ) or {}
        messages = list(data.get("messages") or [])
        # QCE's in-memory cache can report hasNext=false even while later
        # cached pages still exist (ApiServer.ts returns only the fetcher's
        # hasMore flag on that branch).  A full page is therefore treated as
        # potentially followed by another page.  The extra request ends on a
        # short/empty page and prevents dense days from being truncated.
        has_next = bool(data.get("hasNext")) or len(messages) >= max(1, limit)
        return messages, has_next

    def fetch_before(
        self,
        group_id: str,
        message_id: str,
        count: int,
    ) -> tuple[list[dict[str, Any]], bool]:
        data = self.call(
            "/api/messages/fetch-before",
            {
                "peer": {"chatType": 2, "peerUid": group_id},
                "messageId": str(message_id),
                "count": max(2, min(int(count), 500)),
            },
        ) or {}
        return list(data.get("messages") or []), bool(data.get("hasMore"))


def load_settings(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def connect_database(
    path: Path,
    *,
    initialize: bool = True,
) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=60)
    connection.execute("PRAGMA busy_timeout=60000")
    if not initialize:
        return connection
    connection.execute("PRAGMA journal_mode=WAL")
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
            PRIMARY KEY (group_id, message_id, image_index)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS group_cursors (
            group_id TEXT PRIMARY KEY,
            oldest_seq TEXT,
            oldest_time INTEGER,
            completed INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS deep_history_cursors (
            group_id TEXT PRIMARY KEY,
            anchor_message_id TEXT NOT NULL,
            anchor_seq TEXT,
            anchor_time INTEGER NOT NULL,
            oldest_message_id TEXT NOT NULL,
            oldest_seq TEXT,
            oldest_time INTEGER NOT NULL,
            completed INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS qce_recent_cursors (
            group_id TEXT PRIMARY KEY,
            last_time INTEGER NOT NULL,
            scan_end_time INTEGER NOT NULL DEFAULT 0,
            target_time INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL
        )
        """
    )
    image_columns = {row[1] for row in connection.execute("PRAGMA table_info(images)")}
    if "attempts" not in image_columns:
        connection.execute("ALTER TABLE images ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0")
    if "next_retry_at" not in image_columns:
        connection.execute("ALTER TABLE images ADD COLUMN next_retry_at INTEGER NOT NULL DEFAULT 0")
    if "resolver" not in image_columns:
        connection.execute("ALTER TABLE images ADD COLUMN resolver TEXT NOT NULL DEFAULT 'onebot'")
    if "resolver_json" not in image_columns:
        connection.execute("ALTER TABLE images ADD COLUMN resolver_json TEXT")
    provenance_columns = {
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
        "discovered_at": "INTEGER",
        "collected_at": "INTEGER",
    }
    for column, declaration in provenance_columns.items():
        if column not in image_columns:
            connection.execute(f"ALTER TABLE images ADD COLUMN {column} {declaration}")
    connection.execute(
        "UPDATE images SET discovered_at=coalesce(discovered_at, updated_at) "
        "WHERE discovered_at IS NULL"
    )
    connection.execute(
        "UPDATE images SET collected_at=coalesce(collected_at, updated_at) "
        "WHERE collected_at IS NULL AND status IN ('accepted', 'rejected_no_metadata')"
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
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_images_sha256 ON images(sha256)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_images_sender_uin ON images(sender_uin)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_images_group_sent ON images(group_id, sent_at)"
    )
    connection.execute("DROP VIEW IF EXISTS occurrences")
    connection.execute("CREATE VIEW occurrences AS SELECT * FROM images")
    initialize_control_schema(connection)
    connection.commit()
    return connection


def _clean_text(value: Any) -> str | None:
    text = str(value or "").replace("\x00", "").strip()
    return text or None


def _optional_bool(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"", "null", "none"}:
            return None
        if normalized in {"0", "false", "no", "off"}:
            return 0
        if normalized in {"1", "true", "yes", "on"}:
            return 1
    return int(bool(value))


def _joined_message_text(parts: Iterable[Any]) -> str | None:
    text = "\n".join(str(value).strip() for value in parts if str(value or "").strip())
    return text[:PROVENANCE_TEXT_LIMIT] or None


def onebot_message_context(message: dict[str, Any]) -> dict[str, Any]:
    sender = message.get("sender") or {}
    message_text = _joined_message_text(
        (segment.get("data") or {}).get("text")
        for segment in (message.get("message") or [])
        if segment.get("type") == "text"
    )
    return {
        "group_uin": _clean_text(message.get("group_id") or message.get("peer_id")),
        "group_name": _clean_text(message.get("group_name")),
        "sender_uin": _clean_text(message.get("user_id") or sender.get("user_id")),
        "sender_uid": _clean_text(sender.get("user_uid") or sender.get("uid")),
        "sender_member_name": _clean_text(sender.get("card")),
        "sender_nickname": _clean_text(sender.get("nickname")),
        "sender_remark_name": _clean_text(sender.get("remark")),
        "message_text": message_text,
        "is_imported": None,
        "is_online": None,
    }


def qce_message_context(message: dict[str, Any], group_id: str) -> dict[str, Any]:
    message_text = _joined_message_text(
        (element.get("textElement") or {}).get("content")
        for element in (message.get("elements") or [])
        if element.get("textElement")
    )
    return {
        "group_uin": _clean_text(message.get("peerUin") or message.get("peerUid") or group_id),
        "group_name": _clean_text(message.get("peerName")),
        "sender_uin": _clean_text(message.get("senderUin")),
        "sender_uid": _clean_text(message.get("senderUid") or message.get("fromUid")),
        "sender_member_name": _clean_text(message.get("sendMemberName")),
        "sender_nickname": _clean_text(message.get("sendNickName")),
        "sender_remark_name": _clean_text(message.get("sendRemarkName")),
        "message_text": message_text,
        "is_imported": _optional_bool(message.get("isImportMsg")),
        "is_online": _optional_bool(message.get("isOnlineMsg")),
    }


def image_segments(messages: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    for message in messages:
        sent_at = int(message.get("time") or 0)
        real_seq = str(message.get("real_seq") or "")
        short_id = str(message.get("message_id") or message.get("real_id") or "")
        durable_key = f"ob:{sent_at}:{real_seq}:{short_id}"
        context = onebot_message_context(message)
        image_index = 0
        for segment in message.get("message") or []:
            if segment.get("type") != "image":
                continue
            data = segment.get("data") or {}
            yield {
                "group_id": str(message.get("group_id") or message.get("peer_id") or ""),
                "message_id": durable_key,
                "message_seq": real_seq or str(message.get("message_seq") or ""),
                "sent_at": sent_at,
                "image_index": image_index,
                "file": str(data.get("file") or ""),
                "declared_size": int(data.get("file_size") or 0),
                "resolver": "onebot",
                "resolver_data": {},
                "original_flag": _optional_bool(data.get("original")),
                **context,
            }
            image_index += 1


def qce_image_segments(messages: Iterable[dict[str, Any]], group_id: str) -> Iterable[dict[str, Any]]:
    for message in messages:
        context = qce_message_context(message, group_id)
        image_index = 0
        for element in message.get("elements") or []:
            picture = element.get("picElement") or {}
            if not picture:
                continue
            resolver_data = {
                "msgId": str(message.get("msgId") or ""),
                "chatType": int(message.get("chatType") or 2),
                "peerUid": str(message.get("peerUid") or group_id),
                "elementId": str(element.get("elementId") or ""),
                "sourcePath": str(picture.get("sourcePath") or ""),
                "original": bool(picture.get("original")),
                "declaredSize": int(picture.get("fileSize") or 0),
                "picWidth": int(picture.get("picWidth") or 0),
                "picHeight": int(picture.get("picHeight") or 0),
                "md5HexStr": str(picture.get("md5HexStr") or ""),
            }
            yield {
                "group_id": group_id,
                "message_id": resolver_data["msgId"],
                "message_seq": str(message.get("msgSeq") or ""),
                "sent_at": int(message.get("msgTime") or 0),
                "image_index": image_index,
                "file": str(picture.get("fileName") or picture.get("md5HexStr") or ""),
                "declared_size": resolver_data["declaredSize"],
                "resolver": "qce",
                "resolver_data": resolver_data,
                "original_flag": _optional_bool(picture.get("original")),
                **context,
            }
            image_index += 1


def record_status(connection: sqlite3.Connection, item: dict[str, Any], **values: Any) -> None:
    previous = connection.execute(
        """
        SELECT sha256, local_path, metadata_source, metadata_json, error,
               attempts, next_retry_at
        FROM images
        WHERE group_id=? AND message_id=? AND image_index=?
        """,
        (item["group_id"], item["message_id"], item["image_index"]),
    ).fetchone()
    previous = previous or (None, None, None, None, None, 0, 0)
    status = values.get("status", "discovered")
    attempts = int(previous[5] or 0)
    next_retry_at = int(previous[6] or 0)
    if status == "failed":
        attempts += 1
        # 5 min, 10 min, 20 min ... capped at 24 hours.
        next_retry_at = int(time.time()) + min(300 * (2 ** min(attempts - 1, 8)), 86400)
    elif status in {"accepted", "rejected_no_metadata"}:
        next_retry_at = 0
    if status in {"accepted", "rejected_no_metadata"}:
        error_value = values.get("error")
    else:
        error_value = values.get("error", previous[4])
    current = {
        "status": status,
        "sha256": values.get("sha256", previous[0]),
        "local_path": values.get("local_path", previous[1]),
        "metadata_source": values.get("metadata_source", previous[2]),
        "metadata_json": values.get("metadata_json", previous[3]),
        "error": error_value,
        "attempts": attempts,
        "next_retry_at": next_retry_at,
    }
    now = int(time.time())
    collected_at = now if status in {"accepted", "rejected_no_metadata"} else None
    connection.execute(
        """
        INSERT INTO images (
            group_id, message_id, message_seq, image_index, sent_at, file_token,
            declared_size, status, sha256, local_path, metadata_source,
            metadata_json, error, updated_at, attempts, next_retry_at,
            resolver, resolver_json, group_uin, group_name, sender_uin,
            sender_uid, sender_member_name, sender_nickname,
            sender_remark_name, message_text, is_imported, is_online,
            original_flag, discovered_at, collected_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(group_id, message_id, image_index) DO UPDATE SET
            message_seq=excluded.message_seq,
            file_token=excluded.file_token,
            declared_size=excluded.declared_size,
            resolver=excluded.resolver,
            resolver_json=excluded.resolver_json,
            status=excluded.status,
            sha256=excluded.sha256,
            local_path=excluded.local_path,
            metadata_source=excluded.metadata_source,
            metadata_json=excluded.metadata_json,
            error=excluded.error,
            updated_at=excluded.updated_at,
            attempts=excluded.attempts,
            next_retry_at=excluded.next_retry_at,
            group_uin=coalesce(nullif(images.group_uin, ''), excluded.group_uin),
            group_name=coalesce(nullif(images.group_name, ''), excluded.group_name),
            sender_uin=coalesce(nullif(images.sender_uin, ''), excluded.sender_uin),
            sender_uid=coalesce(nullif(images.sender_uid, ''), excluded.sender_uid),
            sender_member_name=coalesce(nullif(images.sender_member_name, ''), excluded.sender_member_name),
            sender_nickname=coalesce(nullif(images.sender_nickname, ''), excluded.sender_nickname),
            sender_remark_name=coalesce(nullif(images.sender_remark_name, ''), excluded.sender_remark_name),
            message_text=coalesce(nullif(images.message_text, ''), excluded.message_text),
            is_imported=coalesce(images.is_imported, excluded.is_imported),
            is_online=coalesce(images.is_online, excluded.is_online),
            original_flag=coalesce(images.original_flag, excluded.original_flag),
            discovered_at=coalesce(images.discovered_at, excluded.discovered_at),
            collected_at=coalesce(images.collected_at, excluded.collected_at)
        """,
        (
            item["group_id"], item["message_id"], item["message_seq"], item["image_index"],
            item["sent_at"], item["file"], item["declared_size"], current["status"],
            current["sha256"], current["local_path"], current["metadata_source"],
            current["metadata_json"], current["error"], now,
            current["attempts"], current["next_retry_at"],
            str(item.get("resolver") or "onebot"),
            json.dumps(item.get("resolver_data") or {}, ensure_ascii=False),
            item.get("group_uin"), item.get("group_name"), item.get("sender_uin"),
            item.get("sender_uid"), item.get("sender_member_name"),
            item.get("sender_nickname"), item.get("sender_remark_name"),
            item.get("message_text"), item.get("is_imported"),
            item.get("is_online"), item.get("original_flag"),
            int(item.get("discovered_at") or now), collected_at,
        ),
    )
    connection.commit()


def update_item_provenance(connection: sqlite3.Connection, item: dict[str, Any]) -> bool:
    cursor = connection.execute(
        """
        UPDATE images SET
            group_uin=coalesce(nullif(group_uin, ''), ?),
            group_name=coalesce(nullif(group_name, ''), ?),
            sender_uin=coalesce(nullif(sender_uin, ''), ?),
            sender_uid=coalesce(nullif(sender_uid, ''), ?),
            sender_member_name=coalesce(nullif(sender_member_name, ''), ?),
            sender_nickname=coalesce(nullif(sender_nickname, ''), ?),
            sender_remark_name=coalesce(nullif(sender_remark_name, ''), ?),
            message_text=coalesce(nullif(message_text, ''), ?),
            is_imported=coalesce(is_imported, ?),
            is_online=coalesce(is_online, ?),
            original_flag=coalesce(original_flag, ?),
            discovered_at=coalesce(discovered_at, updated_at)
        WHERE group_id=? AND message_id=? AND image_index=?
        """,
        (
            item.get("group_uin"), item.get("group_name"), item.get("sender_uin"),
            item.get("sender_uid"), item.get("sender_member_name"),
            item.get("sender_nickname"), item.get("sender_remark_name"),
            item.get("message_text"), item.get("is_imported"),
            item.get("is_online"), item.get("original_flag"),
            item["group_id"], item["message_id"], item["image_index"],
        ),
    )
    connection.commit()
    return bool(cursor.rowcount)


def update_qce_message_provenance(
    connection: sqlite3.Connection,
    group_id: str,
    message: dict[str, Any],
) -> int:
    context = qce_message_context(message, group_id)
    message_id = str(message.get("msgId") or "")
    if not message_id:
        return 0
    cursor = connection.execute(
        """
        UPDATE images SET
            group_uin=coalesce(nullif(group_uin, ''), ?),
            group_name=coalesce(nullif(group_name, ''), ?),
            sender_uin=coalesce(nullif(sender_uin, ''), ?),
            sender_uid=coalesce(nullif(sender_uid, ''), ?),
            sender_member_name=coalesce(nullif(sender_member_name, ''), ?),
            sender_nickname=coalesce(nullif(sender_nickname, ''), ?),
            sender_remark_name=coalesce(nullif(sender_remark_name, ''), ?),
            message_text=coalesce(nullif(message_text, ''), ?),
            is_imported=coalesce(is_imported, ?),
            is_online=coalesce(is_online, ?),
            discovered_at=coalesce(discovered_at, updated_at)
        WHERE group_id=? AND message_id=?
        """,
        (
            context["group_uin"], context["group_name"], context["sender_uin"],
            context["sender_uid"], context["sender_member_name"],
            context["sender_nickname"], context["sender_remark_name"],
            context["message_text"], context["is_imported"], context["is_online"],
            group_id, message_id,
        ),
    )
    image_index = 0
    for element in message.get("elements") or []:
        picture = element.get("picElement") or {}
        if not picture:
            continue
        connection.execute(
            """
            UPDATE images SET original_flag=coalesce(original_flag, ?)
            WHERE group_id=? AND message_id=? AND image_index=?
            """,
            (_optional_bool(picture.get("original")), group_id, message_id, image_index),
        )
        image_index += 1
    connection.commit()
    return int(cursor.rowcount or 0)


def backfill_accepted_provenance(
    qce: QCEClient,
    connection: sqlite3.Connection,
    groups: list[str],
    page_size: int = 500,
) -> dict[str, int]:
    group_set = set(groups)
    rows = connection.execute(
        """
        SELECT DISTINCT group_id, message_id, sent_at
        FROM images
        WHERE status='accepted' AND resolver='qce'
          AND coalesce(sender_uin, '')=''
          AND sent_at IS NOT NULL AND sent_at>0
        ORDER BY group_id, sent_at DESC
        """
    ).fetchall()
    targets_by_day: dict[tuple[str, int], set[str]] = {}
    for group_id, message_id, sent_at in rows:
        group_id = str(group_id)
        if group_set and group_id not in group_set:
            continue
        day = int(sent_at) // 86400
        targets_by_day.setdefault((group_id, day), set()).add(str(message_id))

    messages_matched = 0
    rows_updated = 0
    pages_fetched = 0
    for (group_id, day), target_ids in targets_by_day.items():
        remaining = set(target_ids)
        start_time = day * 86400
        end_time = start_time + 86399
        page = 1
        while remaining:
            messages, has_next = qce.fetch_page(
                group_id,
                start_time,
                end_time,
                max(100, min(page_size, 1000)),
                page,
            )
            pages_fetched += 1
            if not messages:
                break
            for message in messages:
                message_id = str(message.get("msgId") or "")
                if message_id not in remaining:
                    continue
                rows_updated += update_qce_message_provenance(
                    connection, group_id, message
                )
                messages_matched += 1
                remaining.discard(message_id)
            if not has_next:
                break
            page += 1
            if page > 200:
                raise LocalApiError(
                    f"provenance scan exceeded 200 pages for {group_id} day={day}"
                )
        print(
            f"{group_id}: provenance_day={day} targets={len(target_ids)} "
            f"matched={len(target_ids) - len(remaining)} pages={page}",
            flush=True,
        )

    onebot_rows = connection.execute(
        """
        SELECT group_id, message_id, image_index, sent_at, sha256
        FROM images
        WHERE status='accepted' AND resolver='onebot'
          AND coalesce(sender_uin, '')=''
        """
    ).fetchall()
    inferred_onebot = 0
    for group_id, message_id, image_index, sent_at, digest in onebot_rows:
        source = connection.execute(
            """
            SELECT group_uin, group_name, sender_uin, sender_uid,
                   sender_member_name, sender_nickname, sender_remark_name,
                   message_text, is_imported, is_online, original_flag
            FROM images
            WHERE status='accepted' AND resolver='qce' AND group_id=?
              AND sent_at=? AND sha256=? AND coalesce(sender_uin, '')<>''
            ORDER BY updated_at LIMIT 1
            """,
            (group_id, sent_at, digest),
        ).fetchone()
        if not source:
            continue
        item = {
            "group_id": group_id,
            "message_id": message_id,
            "image_index": image_index,
            "group_uin": source[0],
            "group_name": source[1],
            "sender_uin": source[2],
            "sender_uid": source[3],
            "sender_member_name": source[4],
            "sender_nickname": source[5],
            "sender_remark_name": source[6],
            "message_text": source[7],
            "is_imported": source[8],
            "is_online": source[9],
            "original_flag": source[10],
        }
        if update_item_provenance(connection, item):
            inferred_onebot += 1

    missing_qce = int(
        connection.execute(
            """
            SELECT count(*) FROM images
            WHERE status='accepted' AND resolver='qce'
              AND coalesce(sender_uin, '')=''
            """
        ).fetchone()[0]
    )
    assets_without_sender = int(
        connection.execute(
            """
            SELECT count(*) FROM (
                SELECT sha256 FROM images
                WHERE status='accepted' AND sha256 IS NOT NULL
                GROUP BY sha256
                HAVING sum(CASE WHEN coalesce(sender_uin, '')<>'' THEN 1 ELSE 0 END)=0
            )
            """
        ).fetchone()[0]
    )
    return {
        "target_messages": len(rows),
        "messages_matched": messages_matched,
        "rows_updated": rows_updated,
        "pages_fetched": pages_fetched,
        "onebot_inferred": inferred_onebot,
        "missing_qce_occurrences": missing_qce,
        "assets_without_sender": assets_without_sender,
    }


def should_skip(connection: sqlite3.Connection, item: dict[str, Any]) -> bool:
    row = connection.execute(
        "SELECT status, next_retry_at, local_path FROM images WHERE group_id=? AND message_id=? AND image_index=?",
        (item["group_id"], item["message_id"], item["image_index"]),
    ).fetchone()
    if not row:
        return False
    if row[0] == "accepted":
        return bool(row[2]) and Path(str(row[2])).is_file()
    if row[0] == "rejected_no_metadata":
        return True
    return row[0] == "failed" and int(row[1] or 0) > int(time.time())


def recover_stale_resolving(connection: sqlite3.Connection, stale_after_seconds: int = 300) -> int:
    cutoff = int(time.time()) - max(60, stale_after_seconds)
    cursor = connection.execute(
        """
        UPDATE images
        SET status='failed', error=coalesce(error, 'collector stopped while resolving'),
            next_retry_at=0, updated_at=?
        WHERE status='resolving' AND updated_at <= ?
        """,
        (int(time.time()), cutoff),
    )
    connection.commit()
    return int(cursor.rowcount or 0)


def supersede_onebot_failures(connection: sqlite3.Connection) -> int:
    """Retire session-local failures now covered by stable QCE discovery."""
    cursor = connection.execute(
        """
        UPDATE images
        SET status='superseded_by_qce', next_retry_at=0, updated_at=?
        WHERE resolver='onebot' AND status IN ('failed', 'resolving')
        """,
        (int(time.time()),),
    )
    connection.commit()
    return int(cursor.rowcount or 0)


def category_for_source(source: str | None) -> str:
    if source == "comfyui":
        return "ComfyUI"
    if source == "novelai":
        return "NovelAI"
    if source in {"novelai-unreadable", "novelai-stealth", "novelai-ztxt"}:
        return "NAI含参但不可直接读取的"
    return "其他模型生成"


def accepted_path_for(
    storage_root: Path,
    digest: str,
    extension: str,
    source: str | None,
    sent_at: int = 0,
    group_id: str | None = None,
    sender_uin: str | None = None,
    digest_length: int = 10,
) -> Path:
    category = category_for_source(source)
    try:
        normalized_sent_at = int(sent_at)
        timestamp = dt.datetime.fromtimestamp(normalized_sent_at, FILENAME_TIMEZONE).strftime(
            "%Y-%m-%d_%H-%M-%S"
        )
    except (OSError, OverflowError, TypeError, ValueError):
        normalized_sent_at = 0
        timestamp = "unknown-time"
    if normalized_sent_at <= 0:
        timestamp = "unknown-time"
    normalized_group = str(group_id or "").strip()
    normalized_sender = str(sender_uin or "").strip()
    group_component = normalized_group if normalized_group.isdigit() else "unknown"
    sender_component = normalized_sender if normalized_sender.isdigit() else "unknown"
    short_digest = digest[: max(10, digest_length)] or "unknown"
    return (
        storage_root
        / "final"
        / category
        / f"{timestamp}_g{group_component}_u{sender_component}_{short_digest}{extension}"
    )


def collision_safe_accepted_path(
    storage_root: Path,
    digest: str,
    extension: str,
    source: str | None,
    sent_at: int,
    group_id: str | None = None,
    sender_uin: str | None = None,
) -> Path:
    for digest_length in (10, 16, len(digest)):
        candidate = accepted_path_for(
            storage_root,
            digest,
            extension,
            source,
            sent_at,
            group_id,
            sender_uin,
            digest_length,
        )
        if not candidate.exists() or sha256_file(candidate) == digest:
            return candidate
    raise FileExistsError("could not choose a collision-free accepted image name")


def existing_accepted_path(
    connection: sqlite3.Connection,
    digest: str,
    source: str | None,
) -> Path | None:
    category = category_for_source(source)
    asset = connection.execute(
        "SELECT local_path, category FROM assets WHERE sha256=?", (digest,)
    ).fetchone()
    if asset and str(asset[1]) == category:
        asset_path = Path(str(asset[0]))
        if asset_path.is_file():
            return asset_path
    rows = connection.execute(
        """
        SELECT local_path, metadata_source
        FROM images
        WHERE status='accepted' AND sha256=? AND local_path IS NOT NULL
        ORDER BY updated_at ASC
        """,
        (digest,),
    ).fetchall()
    for local_path, existing_source in rows:
        if category_for_source(existing_source) != category:
            continue
        path = Path(str(local_path))
        if path.is_file():
            return path
    return None


def upsert_asset(
    connection: sqlite3.Connection,
    digest: str,
    local_path: Path,
    source: str | None,
    metadata_json: str | None,
    item: dict[str, Any],
    *,
    width: int | None = None,
    height: int | None = None,
    image_format: str | None = None,
) -> None:
    now = int(time.time())
    connection.execute(
        """
        INSERT INTO assets (
            sha256, local_path, category, file_extension, file_size,
            width, height, metadata_source, metadata_json, parser_version,
            canonical_group_id, canonical_sender_uin, canonical_message_id,
            canonical_image_index, canonical_sent_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(sha256) DO UPDATE SET
            local_path=excluded.local_path,
            category=excluded.category,
            file_extension=coalesce(excluded.file_extension, assets.file_extension),
            file_size=coalesce(excluded.file_size, assets.file_size),
            width=coalesce(excluded.width, assets.width),
            height=coalesce(excluded.height, assets.height),
            metadata_source=coalesce(assets.metadata_source, excluded.metadata_source),
            metadata_json=coalesce(assets.metadata_json, excluded.metadata_json),
            parser_version=coalesce(assets.parser_version, excluded.parser_version),
            canonical_group_id=coalesce(assets.canonical_group_id, excluded.canonical_group_id),
            canonical_sender_uin=coalesce(assets.canonical_sender_uin, excluded.canonical_sender_uin),
            canonical_message_id=coalesce(assets.canonical_message_id, excluded.canonical_message_id),
            canonical_image_index=coalesce(assets.canonical_image_index, excluded.canonical_image_index),
            canonical_sent_at=coalesce(assets.canonical_sent_at, excluded.canonical_sent_at),
            updated_at=excluded.updated_at
        """,
        (
            digest,
            str(local_path),
            category_for_source(source),
            local_path.suffix.lower() or None,
            local_path.stat().st_size if local_path.is_file() else None,
            width,
            height,
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


def cleanup_temp(storage_root: Path) -> int:
    temp_dir = storage_root / "temp"
    if not temp_dir.exists():
        return 0
    removed = 0
    for part in temp_dir.glob("*.part"):
        try:
            part.unlink()
            removed += 1
        except FileNotFoundError:
            pass
    return removed


def purge_excluded_accepted(
    connection: sqlite3.Connection,
    storage_root: Path,
) -> tuple[int, int]:
    rows = connection.execute(
        """
        SELECT group_id, message_id, image_index, local_path, sha256
        FROM images
        WHERE status='accepted' AND lower(local_path) LIKE '%.gif'
        """
    ).fetchall()
    final_root = (storage_root / "final").resolve()
    deleted_paths: set[Path] = set()
    now = int(time.time())

    purged_digests: set[str] = set()
    for group_id, message_id, image_index, local_path, digest in rows:
        path = Path(str(local_path))
        try:
            resolved_path = path.resolve()
            inside_final = resolved_path.is_relative_to(final_root)
        except (OSError, ValueError):
            inside_final = False
        if inside_final and path.suffix.casefold() == ".gif":
            path.unlink(missing_ok=True)
            path.with_suffix(path.suffix + ".json").unlink(missing_ok=True)
            deleted_paths.add(resolved_path)
        if digest:
            purged_digests.add(str(digest))
        connection.execute(
            """
            UPDATE images
            SET status='rejected_no_metadata', local_path=NULL,
                metadata_source=NULL, metadata_json=NULL,
                error='excluded image format: GIF', next_retry_at=0,
                updated_at=?
            WHERE group_id=? AND message_id=? AND image_index=?
            """,
            (now, group_id, message_id, image_index),
        )

    for digest in purged_digests:
        connection.execute("DELETE FROM assets WHERE sha256=?", (digest,))

    connection.commit()
    return len(rows), len(deleted_paths)


def find_legacy_file(local_path: str, legacy_roots: Iterable[Path]) -> Path:
    stored = Path(local_path)
    candidates = [stored]
    if not stored.is_absolute():
        candidates.append(Path.cwd() / stored)
        relative_parts = stored.parts[1:] if stored.parts and stored.parts[0].casefold() == "data" else stored.parts
        candidates.extend(root.joinpath(*relative_parts) for root in legacy_roots)
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return stored


def migrate_existing_accepted(
    connection: sqlite3.Connection,
    storage_root: Path,
    legacy_roots: Iterable[Path] = (),
) -> int:
    migrated = 0
    assets_without_sender = int(
        connection.execute(
            """
            SELECT count(*) FROM (
                SELECT sha256 FROM images
                WHERE status='accepted' AND sha256 IS NOT NULL
                GROUP BY sha256
                HAVING sum(CASE WHEN coalesce(sender_uin, '')<>'' THEN 1 ELSE 0 END)=0
            )
            """
        ).fetchone()[0]
    )
    if assets_without_sender:
        print(
            f"provenance_rename_deferred assets_without_sender={assets_without_sender}",
            flush=True,
        )
        return 0
    rows = connection.execute(
        """
        SELECT group_id, message_id, image_index, sent_at, sha256, local_path,
               metadata_source, metadata_json, sender_uin, updated_at
        FROM images
        WHERE status='accepted'
        """
    ).fetchall()
    grouped: dict[tuple[str, str], list[tuple[Any, ...]]] = {}
    for row in rows:
        digest = str(row[4] or "")
        local_path = str(row[5] or "")
        if not digest or not local_path:
            continue
        category = category_for_source(row[6])
        grouped.setdefault((digest, category), []).append(row)

    for (digest, category), records in grouped.items():
        sources = [record[6] for record in records if record[6]]
        source = "comfyui" if category == "ComfyUI" else (sources[0] if sources else None)

        asset = connection.execute(
            """
            SELECT canonical_group_id, canonical_sender_uin,
                   canonical_message_id, canonical_image_index,
                   canonical_sent_at
            FROM assets WHERE sha256=?
            """,
            (digest,),
        ).fetchone()
        provenance_records = [
            record for record in records if str(record[8] or "").isdigit()
        ]
        canonical_record = min(
            provenance_records or records,
            key=lambda record: (
                int(record[3] or 0) if int(record[3] or 0) > 0 else 2**63 - 1,
                int(record[9] or 0),
                str(record[0]),
                str(record[1]),
                int(record[2]),
            ),
        )
        canonical_item = {
            "group_id": str(asset[0] if asset and asset[0] else canonical_record[0]),
            "sender_uin": str(asset[1] if asset and asset[1] else canonical_record[8] or ""),
            "message_id": str(asset[2] if asset and asset[2] else canonical_record[1]),
            "image_index": int(asset[3] if asset and asset[3] is not None else canonical_record[2]),
            "sent_at": int(asset[4] if asset and asset[4] else canonical_record[3] or 0),
        }

        known_paths: list[Path] = []
        for record in records:
            path = find_legacy_file(str(record[5]), legacy_roots)
            if path not in known_paths:
                known_paths.append(path)
        existing_paths = [path for path in known_paths if path.is_file()]
        if not existing_paths:
            continue

        extension = existing_paths[0].suffix or ".img"
        expected_path = accepted_path_for(
            storage_root,
            digest,
            extension,
            source,
            canonical_item["sent_at"],
            canonical_item["group_id"],
            canonical_item["sender_uin"],
        )
        if expected_path in existing_paths:
            new_path = expected_path
        else:
            new_path = collision_safe_accepted_path(
                storage_root,
                digest,
                extension,
                source,
                canonical_item["sent_at"],
                canonical_item["group_id"],
                canonical_item["sender_uin"],
            )
        new_path.parent.mkdir(parents=True, exist_ok=True)

        if not new_path.exists():
            shutil.move(str(existing_paths[0]), str(new_path))

        for path in known_paths:
            sidecar = path.with_suffix(path.suffix + ".json")
            sidecar.unlink(missing_ok=True)
        new_path.with_suffix(new_path.suffix + ".json").unlink(missing_ok=True)

        for path in existing_paths:
            if path == new_path or not path.is_file():
                continue
            if sha256_file(path) == digest:
                path.unlink()

        if new_path.exists():
            for record in records:
                group_id, message_id, image_index = record[0], record[1], record[2]
                local_path = record[5]
                if str(local_path) == str(new_path):
                    continue
                connection.execute(
                    """
                    UPDATE images SET local_path=?, updated_at=?
                    WHERE group_id=? AND message_id=? AND image_index=?
                    """,
                    (
                        str(new_path),
                        int(time.time()),
                        group_id,
                        message_id,
                        image_index,
                    ),
                )
                migrated += 1

            width: int | None = None
            height: int | None = None
            try:
                from PIL import Image

                with Image.open(new_path) as image:
                    width, height = image.size
            except Exception:
                pass
            metadata_json = next(
                (str(record[7]) for record in records if record[7]), None
            )
            upsert_asset(
                connection,
                digest,
                new_path,
                source,
                metadata_json,
                canonical_item,
                width=width,
                height=height,
                image_format=extension.lstrip("."),
            )

    final_root = storage_root / "final"
    if final_root.exists():
        for sidecar in final_root.rglob("*.json"):
            sidecar.unlink(missing_ok=True)
        preserved_categories = {final_root / name for name in FINAL_CATEGORIES}
        directories = sorted(
            (
                path
                for path in final_root.rglob("*")
                if path.is_dir()
                and path not in preserved_categories
            ),
            key=lambda path: len(path.parts),
            reverse=True,
        )
        for directory in directories:
            try:
                directory.rmdir()
            except OSError:
                pass
    connection.commit()
    return migrated


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def copy_downloaded_file(source: Path, staging_dir: Path) -> Path:
    staging_dir.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix="qq-image-", suffix=".part", dir=staging_dir)
    os.close(fd)
    temp_path = Path(temp_name)
    try:
        shutil.copyfile(source, temp_path)
        if temp_path.stat().st_size <= 0:
            raise ValueError("downloaded image is empty")
        return temp_path
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def resolve_image_source(
    onebot: OneBotClient,
    item: dict[str, Any],
) -> Path:
    def local_path(value: Any) -> Path:
        resolver = getattr(onebot, "resolve_local_path", None)
        return resolver(value) if callable(resolver) else Path(str(value or ""))

    if item.get("resolver") != "qce":
        resolved = onebot.call("get_image", {"file": item["file"]}) or {}
        source = local_path(resolved.get("file"))
        if not source.is_file():
            raise FileNotFoundError("get_image did not return an existing local file")
        return source

    resolver_data = item.get("resolver_data") or {}
    cached_source = local_path(resolver_data.get("sourcePath"))
    declared_size = int(resolver_data.get("declaredSize") or item.get("declared_size") or 0)
    if resolver_data.get("original") and declared_size > 0 and cached_source.is_file():
        cached_size = cached_source.stat().st_size
        if cached_size >= declared_size:
            return cached_source

    raw_message_id = str(resolver_data.get("msgId") or item.get("message_id") or "")
    if not raw_message_id:
        raise ValueError("QCE image is missing its stable raw msgId")
    messages = fetch_history(onebot, item["group_id"], 1, raw_message_id, False)
    hydrated = list(image_segments(messages))
    image_index = int(item.get("image_index") or 0)
    if image_index >= len(hydrated):
        raise FileNotFoundError("OneBot did not rehydrate the requested QCE image")
    file_token = str(hydrated[image_index].get("file") or "")
    if not file_token:
        raise FileNotFoundError("rehydrated QCE image has no OneBot file token")
    resolved = onebot.call("get_image", {"file": file_token}) or {}
    source = local_path(resolved.get("file"))
    if not source.is_file():
        raise FileNotFoundError("get_image did not return an existing local file")
    return source


def collect_one(
    client: OneBotClient,
    connection: sqlite3.Connection,
    item: dict[str, Any],
    storage_root: Path,
    keep_rejected: bool,
) -> str:
    update_item_provenance(connection, item)
    if should_skip(connection, item):
        return "skipped"
    if item.get("resolver") != "qce" and not item["file"]:
        record_status(connection, item, status="failed", error="image segment has no file token")
        return "failed"

    record_status(connection, item, status="resolving")
    temp_path: Path | None = None
    try:
        source = resolve_image_source(client, item)
        temp_path = copy_downloaded_file(source, storage_root / "temp")
        result = inspect_image(temp_path)
        digest = sha256_file(temp_path)
        extension = extension_for_format(result.image_format)

        if not result.accepted:
            rejected_path: Path | None = None
            if keep_rejected:
                rejected_path = storage_root / "rejected" / digest[:2] / f"{digest}{extension}"
                rejected_path.parent.mkdir(parents=True, exist_ok=True)
                os.replace(temp_path, rejected_path)
                temp_path = None
            record_status(
                connection,
                item,
                status="rejected_no_metadata",
                sha256=digest,
                local_path=str(rejected_path) if rejected_path else None,
            )
            return "rejected"

        accepted_path = existing_accepted_path(connection, digest, result.source)
        if accepted_path is None:
            accepted_path = collision_safe_accepted_path(
                storage_root,
                digest,
                extension,
                result.source,
                int(item.get("sent_at") or 0),
                str(item.get("group_id") or ""),
                str(item.get("sender_uin") or ""),
            )
        accepted_path.parent.mkdir(parents=True, exist_ok=True)
        if accepted_path.exists():
            temp_path.unlink(missing_ok=True)
        else:
            os.replace(temp_path, accepted_path)
        temp_path = None

        metadata_json = json.dumps(result.fields, ensure_ascii=False)
        record_status(
            connection,
            item,
            status="accepted",
            sha256=digest,
            local_path=str(accepted_path),
            metadata_source=result.source,
            metadata_json=metadata_json,
        )
        upsert_asset(
            connection,
            digest,
            accepted_path,
            result.source,
            metadata_json,
            item,
            width=result.width,
            height=result.height,
            image_format=result.image_format,
        )
        return "accepted"
    except Exception as exc:
        record_status(connection, item, status="failed", error=f"{type(exc).__name__}: {exc}")
        return "failed"
    finally:
        if temp_path:
            temp_path.unlink(missing_ok=True)


def fetch_history(
    client: OneBotClient,
    group_id: str,
    count: int,
    cursor: str | int = 0,
    reverse_order: bool = False,
) -> list[dict[str, Any]]:
    data = client.call(
        "get_group_msg_history",
        {
            "group_id": group_id,
            "message_seq": cursor,
            "count": count,
            "reverse_order": reverse_order,
        },
    ) or {}
    return list(data.get("messages") or [])


def fetch_latest(client: OneBotClient, group_id: str, count: int) -> list[dict[str, Any]]:
    return fetch_history(client, group_id, count)


def process_items(
    client: OneBotClient,
    connection: sqlite3.Connection,
    items: Iterable[dict[str, Any]],
    storage_root: Path,
    keep_rejected: bool,
) -> dict[str, int]:
    totals = {"accepted": 0, "rejected": 0, "failed": 0, "skipped": 0}
    for item in items:
        outcome = collect_one(client, connection, item, storage_root, keep_rejected)
        totals[outcome] += 1
    return totals


def format_totals(totals: dict[str, int]) -> str:
    return " ".join(f"{key}={value}" for key, value in totals.items())


def command_probe(client: OneBotClient, groups: list[str], page_size: int) -> int:
    total = 0
    for group_id in groups:
        messages = fetch_latest(client, group_id, page_size)
        count = sum(1 for _ in image_segments(messages))
        total += count
        print(f"{group_id}: messages={len(messages)} images={count}")
    print(f"total_images={total}")
    return 0


def command_smoke(
    client: OneBotClient,
    connection: sqlite3.Connection,
    groups: list[str],
    storage_root: Path,
    per_group: int,
    page_size: int,
    keep_rejected: bool,
) -> int:
    totals = {"accepted": 0, "rejected": 0, "failed": 0, "skipped": 0}
    for group_id in groups:
        messages = fetch_latest(client, group_id, page_size)
        items = list(image_segments(messages))
        for item in items:
            item["group_id"] = group_id
        selected = items[-per_group:] if per_group > 0 else items
        group_totals = process_items(client, connection, selected, storage_root, keep_rejected)
        for key, value in group_totals.items():
            totals[key] += value
        print(
            f"{group_id}: selected={len(selected)} accepted={group_totals['accepted']} "
            f"rejected={group_totals['rejected']} failed={group_totals['failed']} "
            f"skipped={group_totals['skipped']}"
        )
    print("summary: " + format_totals(totals))
    return 1 if totals["failed"] else 0


def load_qce_cursor(connection: sqlite3.Connection, group_id: str) -> tuple[int, bool]:
    row = connection.execute(
        "SELECT oldest_seq, oldest_time, completed FROM group_cursors WHERE group_id=?",
        (group_id,),
    ).fetchone()
    if not row or str(row[0] or "") != "qce-time":
        # OneBot short IDs are process-local and cannot survive a QQ restart.
        # Restart QCE backfill from now; SHA-256 and message dedup make overlap safe.
        return int(time.time()), False
    return int(row[1] or 0), bool(row[2])


def save_group_cursor(
    connection: sqlite3.Connection,
    group_id: str,
    oldest_seq: str,
    oldest_time: int,
    completed: bool,
) -> None:
    connection.execute(
        """
        INSERT INTO group_cursors (group_id, oldest_seq, oldest_time, completed, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(group_id) DO UPDATE SET
            oldest_seq=excluded.oldest_seq,
            oldest_time=excluded.oldest_time,
            completed=excluded.completed,
            updated_at=excluded.updated_at
        """,
        (group_id, oldest_seq, oldest_time, int(completed), int(time.time())),
    )
    connection.commit()


def qce_page_with_boundary(
    qce: QCEClient,
    group_id: str,
    start_time: int,
    end_time: int,
    page_size: int,
) -> tuple[list[dict[str, Any]], bool]:
    messages, has_next = qce.fetch_page(group_id, start_time, end_time, page_size)
    if not messages:
        return [], has_next

    times = [int(message.get("msgTime") or 0) for message in messages]
    boundary_time = min((value for value in times if value > 0), default=0)
    if boundary_time <= 0:
        return messages, has_next

    # A time cursor alone can skip messages when a page boundary cuts through a
    # busy second. Fetch that whole second with pagination before moving older.
    boundary_limit = max(500, page_size * 10)
    boundary_messages: list[dict[str, Any]] = []
    boundary_page = 1
    while True:
        page, more = qce.fetch_page(
            group_id, boundary_time, boundary_time, boundary_limit, boundary_page
        )
        boundary_messages.extend(page)
        if not more or not page:
            break
        boundary_page += 1
        if boundary_page > 20:
            raise LocalApiError("QCE returned over 10,000 messages in one second")

    unique: dict[str, dict[str, Any]] = {}
    for message in [*messages, *boundary_messages]:
        key = str(message.get("msgId") or "")
        if key:
            unique[key] = message
    combined = sorted(
        unique.values(),
        key=lambda message: (int(message.get("msgTime") or 0), str(message.get("msgSeq") or "")),
        reverse=True,
    )
    return combined, has_next


def command_backfill(
    qce: QCEClient,
    client: OneBotClient,
    connection: sqlite3.Connection,
    groups: list[str],
    storage_root: Path,
    keep_rejected: bool,
    page_size: int,
    pages_per_group: int,
) -> int:
    total_failures = 0
    for group_id in groups:
        try:
            cursor_time, completed = load_qce_cursor(connection, group_id)
            if completed:
                update_group_runtime(
                    connection,
                    group_id,
                    backfill_status="complete",
                    backfill_cursor_time=cursor_time,
                    backfill_completed=1,
                    backfill_last_error=None,
                )
                print(f"{group_id}: history_complete")
                continue

            update_group_runtime(
                connection,
                group_id,
                backfill_status="running",
                backfill_cursor_time=cursor_time,
                backfill_completed=0,
                backfill_last_error=None,
            )

            for page_number in range(1, pages_per_group + 1):
                messages, has_next = qce_page_with_boundary(
                    qce, group_id, 0, cursor_time, page_size
                )
                if not messages:
                    save_group_cursor(connection, group_id, "qce-time", cursor_time, True)
                    update_group_runtime(
                        connection,
                        group_id,
                        backfill_status="complete",
                        backfill_cursor_time=cursor_time,
                        backfill_completed=1,
                        backfill_last_success=int(time.time()),
                        backfill_last_error=None,
                        backfill_messages=0,
                        backfill_images=0,
                        backfill_accepted=0,
                        backfill_rejected=0,
                        backfill_failed=0,
                        backfill_skipped=0,
                    )
                    print(f"{group_id}: page={page_number} history_complete")
                    break

                message_times = [int(message.get("msgTime") or 0) for message in messages]
                oldest_time = min((value for value in message_times if value > 0), default=0)
                next_cursor_time = max(0, oldest_time - 1)
                items = list(qce_image_segments(messages, group_id))
                totals = process_items(client, connection, items, storage_root, keep_rejected)
                total_failures += totals["failed"]

                reached_end = not has_next or next_cursor_time <= 0 or next_cursor_time >= cursor_time
                save_group_cursor(connection, group_id, "qce-time", next_cursor_time, reached_end)
                update_group_runtime(
                    connection,
                    group_id,
                    backfill_status="complete" if reached_end else "running",
                    backfill_cursor_time=next_cursor_time,
                    backfill_completed=int(reached_end),
                    backfill_last_success=int(time.time()),
                    backfill_last_error=None,
                    backfill_messages=len(messages),
                    backfill_images=len(items),
                    backfill_accepted=totals["accepted"],
                    backfill_rejected=totals["rejected"],
                    backfill_failed=totals["failed"],
                    backfill_skipped=totals["skipped"],
                )
                print(
                    f"{group_id}: page={page_number} messages={len(messages)} images={len(items)} "
                    f"{format_totals(totals)} cursor_time={next_cursor_time} "
                    f"cursor_advanced={not reached_end}"
                )
                cursor_time = next_cursor_time
                if reached_end:
                    break
        except Exception as exc:
            total_failures += 1
            update_group_runtime(
                connection,
                group_id,
                backfill_status="error",
                backfill_last_error=f"{type(exc).__name__}: {exc}",
            )
            print(
                f"{group_id}: backfill_error={type(exc).__name__}: {exc}",
                file=sys.stderr,
                flush=True,
            )
    return 1 if total_failures else 0


def _sequence_number(value: Any) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def load_deep_history_cursor(
    connection: sqlite3.Connection,
    group_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT anchor_message_id, anchor_seq, anchor_time,
               oldest_message_id, oldest_seq, oldest_time, completed
        FROM deep_history_cursors
        WHERE group_id=?
        """,
        (group_id,),
    ).fetchone()
    if not row:
        return None
    return {
        "anchor_message_id": str(row[0]),
        "anchor_seq": str(row[1] or ""),
        "anchor_time": int(row[2] or 0),
        "oldest_message_id": str(row[3]),
        "oldest_seq": str(row[4] or ""),
        "oldest_time": int(row[5] or 0),
        "completed": bool(row[6]),
    }


def initialize_deep_history_cursor(
    connection: sqlite3.Connection,
    group_id: str,
) -> dict[str, Any] | None:
    existing = load_deep_history_cursor(connection, group_id)
    if existing:
        return existing
    qce_cursor = connection.execute(
        "SELECT completed FROM group_cursors WHERE group_id=?",
        (group_id,),
    ).fetchone()
    if not qce_cursor or not qce_cursor[0]:
        return None
    anchor = connection.execute(
        """
        SELECT message_id, message_seq, sent_at
        FROM images
        WHERE group_id=? AND resolver='qce'
          AND sent_at IS NOT NULL AND length(message_id) >= 15
        ORDER BY sent_at, CAST(message_seq AS INTEGER), message_id
        LIMIT 1
        """,
        (group_id,),
    ).fetchone()
    if not anchor:
        return None
    now = int(time.time())
    connection.execute(
        """
        INSERT INTO deep_history_cursors (
            group_id, anchor_message_id, anchor_seq, anchor_time,
            oldest_message_id, oldest_seq, oldest_time, completed, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
        ON CONFLICT(group_id) DO NOTHING
        """,
        (
            group_id,
            str(anchor[0]),
            str(anchor[1] or ""),
            int(anchor[2] or 0),
            str(anchor[0]),
            str(anchor[1] or ""),
            int(anchor[2] or 0),
            now,
        ),
    )
    connection.commit()
    return load_deep_history_cursor(connection, group_id)


def save_deep_history_cursor(
    connection: sqlite3.Connection,
    group_id: str,
    state: dict[str, Any],
    oldest_message_id: str,
    oldest_seq: str,
    oldest_time: int,
    completed: bool,
) -> None:
    connection.execute(
        """
        INSERT INTO deep_history_cursors (
            group_id, anchor_message_id, anchor_seq, anchor_time,
            oldest_message_id, oldest_seq, oldest_time, completed, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(group_id) DO UPDATE SET
            oldest_message_id=excluded.oldest_message_id,
            oldest_seq=excluded.oldest_seq,
            oldest_time=excluded.oldest_time,
            completed=excluded.completed,
            updated_at=excluded.updated_at
        """,
        (
            group_id,
            state["anchor_message_id"],
            state["anchor_seq"],
            state["anchor_time"],
            oldest_message_id,
            oldest_seq,
            oldest_time,
            int(completed),
            int(time.time()),
        ),
    )
    connection.commit()


def effective_backfill_completed(
    connection: sqlite3.Connection,
    group_id: str,
) -> bool:
    deep = connection.execute(
        "SELECT completed FROM deep_history_cursors WHERE group_id=?",
        (group_id,),
    ).fetchone()
    if deep:
        return bool(deep[0])
    qce = connection.execute(
        "SELECT completed FROM group_cursors WHERE group_id=?",
        (group_id,),
    ).fetchone()
    return bool(qce and qce[0])


def _raw_message_order(message: dict[str, Any]) -> tuple[int, int, str]:
    sequence = _sequence_number(message.get("msgSeq"))
    return (
        sequence if sequence is not None else 2**63 - 1,
        int(message.get("msgTime") or 0),
        str(message.get("msgId") or ""),
    )


def command_deep_backfill(
    qce: QCEClient,
    client: OneBotClient,
    connection: sqlite3.Connection,
    groups: list[str],
    storage_root: Path,
    keep_rejected: bool,
    page_size: int,
    pages_per_group: int,
) -> int:
    """Resume roaming history older than QCE's time-range boundary.

    The QCE extension endpoint pages directly from a stable raw NTQQ msgId.
    Every returned page therefore remains resumable after QQ or the computer
    restarts, unlike OneBot's process-local short message IDs.
    """

    total_failures = 0
    page_size = max(20, min(int(page_size), 500))
    for group_id in groups:
        qce_cursor = connection.execute(
            "SELECT completed FROM group_cursors WHERE group_id=?",
            (group_id,),
        ).fetchone()
        if not qce_cursor or not qce_cursor[0]:
            continue
        state = initialize_deep_history_cursor(connection, group_id)
        if not state:
            total_failures += 1
            update_group_runtime(
                connection,
                group_id,
                backfill_status="error",
                backfill_completed=0,
                backfill_last_error="No stable QCE message anchor is available",
            )
            print(
                f"{group_id}: deep_backfill_error=no_stable_qce_anchor",
                file=sys.stderr,
                flush=True,
            )
            continue
        if state["completed"]:
            update_group_runtime(
                connection,
                group_id,
                backfill_status="complete",
                backfill_cursor_time=state["oldest_time"],
                backfill_completed=1,
                backfill_last_error=None,
            )
            print(f"{group_id}: deep_history_complete", flush=True)
            continue

        update_group_runtime(
            connection,
            group_id,
            backfill_status="running",
            backfill_cursor_time=state["oldest_time"],
            backfill_completed=0,
            backfill_last_error=None,
        )
        try:
            for page_number in range(1, pages_per_group + 1):
                messages, has_more = qce.fetch_before(
                    group_id,
                    state["oldest_message_id"],
                    page_size,
                )
                ordered = sorted(messages, key=_raw_message_order)
                current_seq = _sequence_number(state["oldest_seq"])
                current_time = int(state["oldest_time"])
                older_messages = []
                for message in ordered:
                    message_seq = _sequence_number(message.get("msgSeq"))
                    message_time = int(message.get("msgTime") or 0)
                    if current_seq is not None and message_seq is not None:
                        if message_seq < current_seq:
                            older_messages.append(message)
                    elif message_time and message_time < current_time:
                        older_messages.append(message)

                oldest = older_messages[0] if older_messages else None
                progressed = bool(
                    oldest
                    and str(oldest.get("msgId") or "")
                    and str(oldest.get("msgId")) != state["oldest_message_id"]
                )
                if not progressed:
                    save_deep_history_cursor(
                        connection,
                        group_id,
                        state,
                        state["oldest_message_id"],
                        state["oldest_seq"],
                        state["oldest_time"],
                        True,
                    )
                    update_group_runtime(
                        connection,
                        group_id,
                        backfill_status="complete",
                        backfill_cursor_time=state["oldest_time"],
                        backfill_completed=1,
                        backfill_last_success=int(time.time()),
                        backfill_last_error=None,
                        backfill_messages=0,
                        backfill_images=0,
                        backfill_accepted=0,
                        backfill_rejected=0,
                        backfill_failed=0,
                        backfill_skipped=0,
                    )
                    print(
                        f"{group_id}: deep_page={page_number} history_complete",
                        flush=True,
                    )
                    break

                items = list(qce_image_segments(older_messages, group_id))
                totals = process_items(
                    client,
                    connection,
                    items,
                    storage_root,
                    keep_rejected,
                )
                total_failures += totals["failed"]
                oldest_message_id = str(oldest.get("msgId") or "")
                oldest_seq = str(oldest.get("msgSeq") or "")
                oldest_time = int(oldest.get("msgTime") or 0)
                reached_end = not has_more
                save_deep_history_cursor(
                    connection,
                    group_id,
                    state,
                    oldest_message_id,
                    oldest_seq,
                    oldest_time,
                    reached_end,
                )
                update_group_runtime(
                    connection,
                    group_id,
                    backfill_status="complete" if reached_end else "running",
                    backfill_cursor_time=oldest_time,
                    backfill_completed=int(reached_end),
                    backfill_last_success=int(time.time()),
                    backfill_last_error=None,
                    backfill_messages=len(older_messages),
                    backfill_images=len(items),
                    backfill_accepted=totals["accepted"],
                    backfill_rejected=totals["rejected"],
                    backfill_failed=totals["failed"],
                    backfill_skipped=totals["skipped"],
                )
                print(
                    f"{group_id}: deep_page={page_number} "
                    f"messages={len(older_messages)} images={len(items)} "
                    f"{format_totals(totals)} cursor_time={oldest_time} "
                    f"cursor_seq={oldest_seq} cursor_advanced={not reached_end}",
                    flush=True,
                )
                state = {
                    **state,
                    "oldest_message_id": oldest_message_id,
                    "oldest_seq": oldest_seq,
                    "oldest_time": oldest_time,
                    "completed": reached_end,
                }
                if reached_end:
                    break
        except Exception as exc:
            total_failures += 1
            update_group_runtime(
                connection,
                group_id,
                backfill_status="error",
                backfill_completed=0,
                backfill_last_error=f"{type(exc).__name__}: {exc}",
            )
            print(
                f"{group_id}: deep_backfill_error={type(exc).__name__}: {exc}",
                file=sys.stderr,
                flush=True,
            )
    return 1 if total_failures else 0


def load_recent_cursor(
    connection: sqlite3.Connection,
    group_id: str,
    initial_lookback_seconds: int,
) -> tuple[int, int, int]:
    row = connection.execute(
        "SELECT last_time, scan_end_time, target_time FROM qce_recent_cursors WHERE group_id=?",
        (group_id,),
    ).fetchone()
    if row:
        return int(row[0]), int(row[1]), int(row[2])
    return max(0, int(time.time()) - initial_lookback_seconds), 0, 0


def save_recent_cursor(
    connection: sqlite3.Connection,
    group_id: str,
    last_time: int,
    scan_end_time: int,
    target_time: int,
) -> None:
    connection.execute(
        """
        INSERT INTO qce_recent_cursors (group_id, last_time, scan_end_time, target_time, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(group_id) DO UPDATE SET
            last_time=excluded.last_time,
            scan_end_time=excluded.scan_end_time,
            target_time=excluded.target_time,
            updated_at=excluded.updated_at
        """,
        (group_id, last_time, scan_end_time, target_time, int(time.time())),
    )
    connection.commit()


def command_qce_catchup(
    qce: QCEClient,
    client: OneBotClient,
    connection: sqlite3.Connection,
    groups: list[str],
    storage_root: Path,
    keep_rejected: bool,
    page_size: int,
    initial_lookback_seconds: int,
) -> int:
    total_failures = 0
    for group_id in groups:
        try:
            update_group_runtime(
                connection,
                group_id,
                recent_status="running",
                recent_last_error=None,
            )
            last_time, scan_end_time, target_time = load_recent_cursor(
                connection, group_id, initial_lookback_seconds
            )
            if scan_end_time <= last_time or target_time <= last_time:
                target_time = int(time.time())
                scan_end_time = target_time

            messages, has_next = qce_page_with_boundary(
                qce, group_id, last_time, scan_end_time, page_size
            )
            if not messages:
                save_recent_cursor(connection, group_id, target_time, 0, 0)
                update_group_runtime(
                    connection,
                    group_id,
                    recent_status="caught_up",
                    recent_last_success=int(time.time()),
                    recent_last_error=None,
                    recent_messages=0,
                    recent_images=0,
                    recent_accepted=0,
                    recent_rejected=0,
                    recent_failed=0,
                    recent_skipped=0,
                )
                print(f"{group_id}: catchup messages=0 caught_up=true", flush=True)
                continue

            items = list(qce_image_segments(messages, group_id))
            totals = process_items(client, connection, items, storage_root, keep_rejected)
            total_failures += totals["failed"]
            oldest_time = min(
                (int(message.get("msgTime") or 0) for message in messages if int(message.get("msgTime") or 0) > 0),
                default=0,
            )
            caught_up = not has_next or oldest_time <= last_time or oldest_time <= 0
            if caught_up:
                save_recent_cursor(connection, group_id, target_time, 0, 0)
            else:
                save_recent_cursor(connection, group_id, last_time, oldest_time - 1, target_time)
            update_group_runtime(
                connection,
                group_id,
                recent_status="caught_up" if caught_up else "catching_up",
                recent_last_success=int(time.time()),
                recent_last_error=None,
                recent_messages=len(messages),
                recent_images=len(items),
                recent_accepted=totals["accepted"],
                recent_rejected=totals["rejected"],
                recent_failed=totals["failed"],
                recent_skipped=totals["skipped"],
            )
            print(
                f"{group_id}: catchup messages={len(messages)} images={len(items)} "
                f"{format_totals(totals)} caught_up={str(caught_up).lower()}",
                flush=True,
            )
        except Exception as exc:
            total_failures += 1
            update_group_runtime(
                connection,
                group_id,
                recent_status="error",
                recent_last_error=f"{type(exc).__name__}: {exc}",
            )
            print(
                f"{group_id}: catchup_error={type(exc).__name__}: {exc}",
                file=sys.stderr,
                flush=True,
            )
    return 1 if total_failures else 0


def command_watch(
    client: OneBotClient,
    connection: sqlite3.Connection,
    groups: list[str],
    storage_root: Path,
    keep_rejected: bool,
    page_size: int,
    interval: int,
    once: bool,
) -> int:
    while True:
        total_failures = 0
        for group_id in groups:
            try:
                messages = fetch_latest(client, group_id, page_size)
                items = list(image_segments(messages))
                for item in items:
                    item["group_id"] = group_id
                totals = process_items(client, connection, items, storage_root, keep_rejected)
                total_failures += totals["failed"]
                print(f"{group_id}: poll images={len(items)} {format_totals(totals)}", flush=True)
            except Exception as exc:
                total_failures += 1
                print(
                    f"{group_id}: poll_error={type(exc).__name__}: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
        if once:
            return 1 if total_failures else 0
        time.sleep(interval)


def command_retry_failed(
    client: OneBotClient,
    connection: sqlite3.Connection,
    storage_root: Path,
    keep_rejected: bool,
    limit: int,
) -> int:
    rows = connection.execute(
        """
        SELECT group_id, message_id, message_seq, image_index, sent_at,
               file_token, declared_size, resolver, resolver_json,
               group_uin, group_name, sender_uin, sender_uid,
               sender_member_name, sender_nickname, sender_remark_name,
               message_text, is_imported, is_online, original_flag,
               discovered_at
        FROM images
        WHERE status='failed' AND next_retry_at <= ?
        ORDER BY updated_at ASC
        LIMIT ?
        """,
        (int(time.time()), limit),
    ).fetchall()
    totals = {"accepted": 0, "rejected": 0, "failed": 0, "skipped": 0}
    for row in rows:
        item = {
            "group_id": row[0],
            "message_id": row[1],
            "message_seq": row[2],
            "image_index": row[3],
            "sent_at": row[4],
            "file": row[5],
            "declared_size": row[6],
            "resolver": row[7] or "onebot",
            "resolver_data": json.loads(row[8] or "{}"),
            "group_uin": row[9],
            "group_name": row[10],
            "sender_uin": row[11],
            "sender_uid": row[12],
            "sender_member_name": row[13],
            "sender_nickname": row[14],
            "sender_remark_name": row[15],
            "message_text": row[16],
            "is_imported": row[17],
            "is_online": row[18],
            "original_flag": row[19],
            "discovered_at": row[20],
        }
        outcome = collect_one(client, connection, item, storage_root, keep_rejected)
        totals[outcome] += 1
    print(f"retried={len(rows)} " + " ".join(f"{key}={value}" for key, value in totals.items()))
    return 1 if totals["failed"] else 0


def command_status(connection: sqlite3.Connection, storage_root: Path) -> int:
    statuses = connection.execute(
        "SELECT status, count(*) FROM images GROUP BY status ORDER BY status"
    ).fetchall()
    sources = connection.execute(
        "SELECT metadata_source, count(*) FROM images WHERE status='accepted' GROUP BY metadata_source ORDER BY metadata_source"
    ).fetchall()
    cursors = connection.execute(
        "SELECT count(*), coalesce(sum(completed), 0) FROM group_cursors"
    ).fetchone()
    deep_cursors = connection.execute(
        "SELECT count(*), coalesce(sum(completed), 0) FROM deep_history_cursors"
    ).fetchone()
    recent_cursors = connection.execute("SELECT count(*) FROM qce_recent_cursors").fetchone()[0]
    final_files = [
        path
        for category in FINAL_CATEGORIES
        for path in (storage_root / "final" / category).rglob("*")
        if path.is_file() and not path.name.endswith(".json")
    ]
    print(f"statuses={statuses}")
    print(f"sources={sources}")
    print(f"group_cursors={cursors[0]} completed={cursors[1]}")
    print(f"deep_history_cursors={deep_cursors[0]} completed={deep_cursors[1]}")
    print(f"qce_recent_cursors={recent_cursors}")
    print(f"final_files={len(final_files)} final_bytes={sum(path.stat().st_size for path in final_files)}")
    print(f"temp_parts={len(list((storage_root / 'temp').glob('*.part'))) if (storage_root / 'temp').exists() else 0}")
    return 0


def pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        process_query_limited_information = 0x1000
        still_active = 259
        handle = ctypes.windll.kernel32.OpenProcess(
            process_query_limited_information, False, pid
        )
        if not handle:
            return False
        try:
            exit_code = ctypes.c_ulong()
            if not ctypes.windll.kernel32.GetExitCodeProcess(
                handle, ctypes.byref(exit_code)
            ):
                return False
            return int(exit_code.value) == still_active
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
        return True
    except PermissionError:
        return True
    except (OSError, SystemError):
        return False


class PidFile:
    def __init__(self, path: Path) -> None:
        self.path = path

    def __enter__(self) -> "PidFile":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        for _attempt in range(3):
            try:
                descriptor = os.open(
                    self.path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                )
            except FileExistsError:
                try:
                    raw_pid = self.path.read_text(encoding="ascii").strip()
                    if not raw_pid:
                        time.sleep(0.1)
                        raw_pid = self.path.read_text(encoding="ascii").strip()
                    existing_pid = int(raw_pid)
                except (ValueError, OSError):
                    existing_pid = 0
                if pid_is_alive(existing_pid):
                    raise RuntimeError(
                        f"Collector is already running with PID {existing_pid}"
                    )
                try:
                    self.path.unlink(missing_ok=True)
                except OSError:
                    time.sleep(0.05)
                continue
            try:
                os.write(descriptor, str(os.getpid()).encode("ascii"))
            finally:
                os.close(descriptor)
            return self
        raise RuntimeError(f"Could not acquire collector PID file: {self.path}")

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        try:
            if self.path.read_text(encoding="ascii").strip() == str(os.getpid()):
                self.path.unlink(missing_ok=True)
        except OSError:
            pass


def process_manual_backfill_job(
    qce: QCEClient,
    client: OneBotClient,
    connection: sqlite3.Connection,
    storage_root: Path,
    keep_rejected: bool,
    page_size: int,
    deep_backfill_enabled: bool = True,
) -> str | None:
    row = next_active_job(connection)
    if not row:
        return None

    job_id, kind, group_id, status, progress_pages, cancel_requested = row
    now = int(time.time())
    if cancel_requested:
        update_job(
            connection,
            int(job_id),
            status="cancelled",
            finished_at=now,
            error=None,
        )
        return str(group_id)

    if status == "queued":
        update_job(
            connection,
            int(job_id),
            status="running",
            started_at=now,
            error=None,
        )

    if kind == "rescan" and int(progress_pages or 0) == 0:
        save_group_cursor(
            connection,
            str(group_id),
            "qce-time",
            int(time.time()),
            False,
        )
        # A rescan is also used after importing older roaming history. Reset
        # the stable-id cursor only when the deployment has the optional QCE
        # fetch-before extension. Official Linux QCE images do not guarantee
        # that private extension, so a completed migrated cursor is preserved.
        if deep_backfill_enabled:
            connection.execute(
                "DELETE FROM deep_history_cursors WHERE group_id=?",
                (str(group_id),),
            )
        connection.commit()
        update_group_runtime(
            connection,
            str(group_id),
            backfill_status="running",
            backfill_cursor_time=int(time.time()),
            backfill_completed=0,
            backfill_last_error=None,
        )

    qce_result = command_backfill(
        qce,
        client,
        connection,
        [str(group_id)],
        storage_root,
        keep_rejected,
        page_size,
        1,
    )
    deep_result = 0
    if not qce_result and deep_backfill_enabled:
        deep_result = command_deep_backfill(
            qce,
            client,
            connection,
            [str(group_id)],
            storage_root,
            keep_rejected,
            max(100, page_size),
            1,
        )
    result = qce_result or deep_result
    next_progress = int(progress_pages or 0) + 1
    completed = effective_backfill_completed(connection, str(group_id))
    if result:
        error_row = connection.execute(
            "SELECT backfill_last_error FROM group_runtime WHERE group_id=?",
            (str(group_id),),
        ).fetchone()
        update_job(
            connection,
            int(job_id),
            status="failed",
            progress_pages=next_progress,
            finished_at=int(time.time()),
            error=str(error_row[0] if error_row and error_row[0] else "backfill failed"),
        )
    elif kind == "page" or completed:
        update_job(
            connection,
            int(job_id),
            status="completed",
            progress_pages=next_progress,
            finished_at=int(time.time()),
            error=None,
        )
    else:
        update_job(
            connection,
            int(job_id),
            status="running",
            progress_pages=next_progress,
            error=None,
        )
    return str(group_id)


def command_run(
    client: OneBotClient,
    qce: QCEClient,
    connection: sqlite3.Connection,
    groups: list[str],
    storage_root: Path,
    keep_rejected: bool,
    runtime: dict[str, Any],
) -> int:
    recovered = recover_stale_resolving(connection)
    if recovered:
        print(f"recovered_stale_resolving={recovered}", flush=True)
    superseded = supersede_onebot_failures(connection)
    if superseded:
        print(f"superseded_onebot_failures={superseded}", flush=True)

    while True:
        cycle_started = time.time()
        print(f"[{dt.datetime.now().isoformat(timespec='seconds')}] cycle_start", flush=True)
        interval = max(
            15,
            int(
                get_setting(
                    connection,
                    "poll_interval_seconds",
                    runtime.get("poll_interval_seconds", 60),
                )
            ),
        )
        jitter = max(
            0,
            min(
                300,
                int(
                    get_setting(
                        connection,
                        "poll_jitter_seconds",
                        runtime.get("poll_jitter_seconds", 0),
                    )
                ),
            ),
        )
        backfill_page_size = max(
            2,
            int(
                get_setting(
                    connection,
                    "backfill_page_size",
                    runtime.get("backfill_page_size", 20),
                )
            ),
        )
        deep_backfill_page_size = max(
            20,
            min(
                500,
                int(
                    get_setting(
                        connection,
                        "deep_backfill_page_size",
                        runtime.get("deep_backfill_page_size", max(100, backfill_page_size)),
                    )
                ),
            ),
        )
        backfill_pages = max(1, int(runtime.get("backfill_pages_per_cycle", 1)))
        catchup_page_size = max(
            20,
            int(
                get_setting(
                    connection,
                    "catchup_page_size",
                    runtime.get("catchup_page_size", 100),
                )
            ),
        )
        catchup_lookback = max(
            60, int(runtime.get("catchup_initial_lookback_seconds", 3600))
        )
        retry_limit = max(1, int(runtime.get("retry_limit_per_cycle", 5)))
        deep_backfill_enabled = bool(
            get_setting(
                connection,
                "deep_backfill_enabled",
                runtime.get("deep_backfill_enabled", True),
            )
        )
        cycle_groups = enabled_groups(connection, groups)

        if get_setting(
            connection,
            "collector_paused",
            runtime.get("collector_paused", False),
        ):
            removed = cleanup_temp(storage_root)
            print(f"cycle_paused temp_removed={removed}", flush=True)
            time.sleep(5)
            continue

        def run_phase(phase_name: str, phase: Any) -> None:
            try:
                phase()
            except Exception as exc:
                print(
                    f"{phase_name}_error={type(exc).__name__}: {exc}",
                    file=sys.stderr,
                    flush=True,
                )

        run_phase(
            "catchup",
            lambda: command_qce_catchup(
                qce,
                client,
                connection,
                cycle_groups,
                storage_root,
                keep_rejected,
                catchup_page_size,
                catchup_lookback,
            ),
        )
        run_phase(
            "retry",
            lambda: command_retry_failed(
                client, connection, storage_root, keep_rejected, retry_limit
            ),
        )

        manual_group: str | None = None
        try:
            manual_group = process_manual_backfill_job(
                qce,
                client,
                connection,
                storage_root,
                keep_rejected,
                backfill_page_size,
                deep_backfill_enabled,
            )
        except Exception as exc:
            print(
                f"manual_backfill_error={type(exc).__name__}: {exc}",
                file=sys.stderr,
                flush=True,
            )

        if not get_setting(
            connection,
            "backfill_paused",
            runtime.get("backfill_paused", False),
        ):
            automatic_groups = [
                group_id for group_id in cycle_groups if group_id != manual_group
            ]
            run_phase(
                "backfill",
                lambda: command_backfill(
                    qce,
                    client,
                    connection,
                    automatic_groups,
                    storage_root,
                    keep_rejected,
                    backfill_page_size,
                    backfill_pages,
                ),
            )
            if deep_backfill_enabled:
                run_phase(
                    "deep_backfill",
                    lambda: command_deep_backfill(
                        qce,
                        client,
                        connection,
                        automatic_groups,
                        storage_root,
                        keep_rejected,
                        deep_backfill_page_size,
                        backfill_pages,
                    ),
                )
        removed = cleanup_temp(storage_root)
        elapsed = int(time.time() - cycle_started)
        print(f"cycle_end elapsed={elapsed}s temp_removed={removed}", flush=True)
        time.sleep(max(5, interval - elapsed) + random.uniform(0, jitter))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect original QQ images that contain generation metadata")
    parser.add_argument("--config", type=Path, default=Path("collector_config.json"))
    subparsers = parser.add_subparsers(dest="command", required=True)

    probe = subparsers.add_parser("probe", help="Count recent image segments without downloading")
    probe.add_argument("--page-size", type=int, default=20)

    smoke = subparsers.add_parser("smoke", help="Download and inspect a small recent sample")
    smoke.add_argument("--per-group", type=int, default=4)
    smoke.add_argument("--page-size", type=int, default=100)
    smoke.add_argument("--keep-rejected", action="store_true")

    retry = subparsers.add_parser("retry-failed", help="Retry transient failures recorded in SQLite")
    retry.add_argument("--limit", type=int, default=20)
    retry.add_argument("--keep-rejected", action="store_true")

    backfill = subparsers.add_parser("backfill", help="Resume older history through QCE time cursors")
    backfill.add_argument("--page-size", type=int, default=20)
    backfill.add_argument("--pages-per-group", type=int, default=1)
    backfill.add_argument("--keep-rejected", action="store_true")

    deep_backfill = subparsers.add_parser(
        "deep-backfill",
        help="Resume QQ roaming history through stable raw message IDs",
    )
    deep_backfill.add_argument("--group", action="append", default=[])
    deep_backfill.add_argument("--page-size", type=int, default=100)
    deep_backfill.add_argument("--pages-per-group", type=int, default=1)
    deep_backfill.add_argument("--keep-rejected", action="store_true")

    watch = subparsers.add_parser("watch", help="Poll recent history with overlap and deduplication")
    watch.add_argument("--page-size", type=int, default=100)
    watch.add_argument("--interval", type=int, default=60)
    watch.add_argument("--once", action="store_true")
    watch.add_argument("--keep-rejected", action="store_true")

    provenance = subparsers.add_parser(
        "provenance-backfill",
        help="Backfill sender/group provenance for accepted images and rename assets",
    )
    provenance.add_argument("--page-size", type=int, default=500)

    subparsers.add_parser("run", help="Run QCE catch-up, retries and historical backfill")
    subparsers.add_parser("status", help="Show local collector state without contacting QQ")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings(args.config)
    groups = [str(group_id) for group_id in settings["groups"]]
    storage = settings["storage"]
    storage_root = Path(storage["root"])
    storage_root.mkdir(parents=True, exist_ok=True)
    for directory in (
        storage_root / "tools",
        storage_root / "config",
        storage_root / "temp",
        *(storage_root / "final" / name for name in FINAL_CATEGORIES),
        storage_root / "state",
        storage_root / "logs",
    ):
        directory.mkdir(parents=True, exist_ok=True)

    if args.command == "probe":
        onebot = settings["onebot"]
        client = OneBotClient.from_settings(onebot)
        return command_probe(client, groups, args.page_size)

    database_path = Path(storage["database"])
    if args.command == "status" and database_path.is_file():
        connection = connect_database(database_path, initialize=False)
        try:
            return command_status(connection, storage_root)
        finally:
            connection.close()

    connection = connect_database(database_path)
    try:
        seed_monitored_groups(connection, groups)
        cleanup_temp(storage_root)
        purge_excluded_accepted(connection, storage_root)
        legacy_roots = [Path(path) for path in storage.get("legacy_roots", [])]
        if (
            args.command != "provenance-backfill"
            and bool(storage.get("migrate_existing_accepted_on_start", True))
        ):
            migrate_existing_accepted(connection, storage_root, legacy_roots)
        onebot = settings["onebot"]
        client = OneBotClient.from_settings(onebot)
        qce = QCEClient.from_settings(settings["qce"]) if settings.get("qce") else None
        keep_rejected = bool(storage.get("keep_rejected")) or bool(getattr(args, "keep_rejected", False))
        if args.command == "provenance-backfill":
            if qce is None:
                raise LocalApiError("provenance-backfill requires qce settings")
            stats = backfill_accepted_provenance(
                qce,
                connection,
                groups,
                max(100, min(int(args.page_size), 1000)),
            )
            print("provenance=" + json.dumps(stats, ensure_ascii=False), flush=True)
            if stats["assets_without_sender"]:
                print(
                    "provenance migration stopped before rename because some assets "
                    "still have no sender",
                    file=sys.stderr,
                    flush=True,
                )
                return 1
            migrated = migrate_existing_accepted(
                connection, storage_root, legacy_roots
            )
            asset_count = int(connection.execute("SELECT count(*) FROM assets").fetchone()[0])
            print(
                f"provenance_renamed_records={migrated} assets={asset_count}",
                flush=True,
            )
            return 0
        if args.command == "run":
            if qce is None:
                raise LocalApiError("run requires qce settings")
            runtime = settings.get("runtime", {})
            pid_file = Path(runtime.get("pid_file", storage_root / "state" / "collector.pid"))
            with PidFile(pid_file):
                return command_run(
                    client, qce, connection, groups, storage_root, keep_rejected, runtime
                )
        if args.command == "smoke":
            return command_smoke(
                client,
                connection,
                groups,
                storage_root,
                args.per_group,
                args.page_size,
                keep_rejected,
            )
        if args.command == "retry-failed":
            return command_retry_failed(
                client,
                connection,
                storage_root,
                keep_rejected,
                args.limit,
            )
        if args.command == "backfill":
            if qce is None:
                raise LocalApiError("backfill requires qce settings")
            recover_stale_resolving(connection)
            return command_backfill(
                qce,
                client,
                connection,
                groups,
                storage_root,
                keep_rejected,
                args.page_size,
                args.pages_per_group,
            )
        if args.command == "deep-backfill":
            if qce is None:
                raise LocalApiError("deep-backfill requires qce settings")
            recover_stale_resolving(connection)
            selected_groups = [str(value) for value in args.group] or groups
            unknown_groups = sorted(set(selected_groups) - set(groups))
            if unknown_groups:
                raise LocalApiError(
                    "deep-backfill groups are not configured: " + ", ".join(unknown_groups)
                )
            return command_deep_backfill(
                qce,
                client,
                connection,
                selected_groups,
                storage_root,
                keep_rejected,
                args.page_size,
                args.pages_per_group,
            )
        return command_watch(
            client,
            connection,
            groups,
            storage_root,
            keep_rejected,
            args.page_size,
            args.interval,
            args.once,
        )
    finally:
        connection.close()


if __name__ == "__main__":
    sys.exit(main())
