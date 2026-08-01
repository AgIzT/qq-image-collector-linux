from __future__ import annotations

import asyncio
import json
import random
import sqlite3
import time
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

import websockets

from .database import increment_counter, set_runtime_state


def _text(value: Any) -> str | None:
    result = str(value or "").replace("\x00", "").strip()
    return result or None


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


def _integer(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _message_text(message: dict[str, Any]) -> str | None:
    parts = []
    for segment in message.get("message") or []:
        if not isinstance(segment, dict) or segment.get("type") != "text":
            continue
        text = _text((segment.get("data") or {}).get("text"))
        if text:
            parts.append(text)
    return "\n".join(parts)[:8192] or None


def _raw_picture_elements(raw: dict[str, Any]) -> list[dict[str, Any]]:
    pictures: list[dict[str, Any]] = []
    for element in raw.get("elements") or []:
        if not isinstance(element, dict):
            continue
        picture = element.get("picElement")
        if isinstance(picture, dict):
            pictures.append(picture)
    return pictures


def _emoji_signal(data: dict[str, Any], raw_picture: dict[str, Any]) -> bool:
    summary = str(data.get("summary") or raw_picture.get("summary") or "").casefold()
    filename = str(data.get("file") or raw_picture.get("fileName") or "").casefold()
    try:
        subtype = int(data.get("sub_type") or raw_picture.get("picSubType") or 0)
    except (TypeError, ValueError):
        subtype = 0
    return subtype != 0 or "动画表情" in summary or filename.endswith(".gif")


def parse_group_event(message: dict[str, Any]) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Return a durable group cursor and zero or more image queue items."""

    if message.get("post_type") != "message" or message.get("message_type") != "group":
        return None, []
    group_id = str(message.get("group_id") or "")
    if not group_id:
        return None, []
    raw = message.get("raw") if isinstance(message.get("raw"), dict) else {}
    sender = message.get("sender") if isinstance(message.get("sender"), dict) else {}
    raw_message_id = str(raw.get("msgId") or message.get("message_id") or "")
    sent_at = _integer(raw.get("msgTime") or message.get("time"))
    cursor = {
        "group_id": group_id,
        "message_id": raw_message_id,
        "message_seq": str(raw.get("msgSeq") or message.get("message_seq") or ""),
        "sent_at": sent_at,
        "event_at": int(time.time()),
    }
    raw_pictures = _raw_picture_elements(raw)
    image_items: list[dict[str, Any]] = []
    image_index = 0
    for segment in message.get("message") or []:
        if not isinstance(segment, dict) or segment.get("type") != "image":
            continue
        data = segment.get("data") if isinstance(segment.get("data"), dict) else {}
        picture = raw_pictures[image_index] if image_index < len(raw_pictures) else {}
        declared_size = _integer(data.get("file_size") or picture.get("fileSize"))
        original = _optional_bool(
            picture.get("original") if picture.get("original") is not None else data.get("original")
        )
        resolver_data = {
            "url": str(data.get("url") or ""),
            "origin_url": str(picture.get("originImageUrl") or ""),
            "raw_message_id": raw_message_id,
            "raw_message_seq": str(raw.get("msgSeq") or ""),
            "width": _integer(picture.get("picWidth") or data.get("width")),
            "height": _integer(picture.get("picHeight") or data.get("height")),
            "md5": str(picture.get("md5HexStr") or ""),
            "summary": str(data.get("summary") or picture.get("summary") or ""),
            "sub_type": data.get("sub_type", picture.get("picSubType")),
            "emoji_signal": _emoji_signal(data, picture),
            "url_refreshed": False,
        }
        item = {
            "group_id": group_id,
            "message_id": raw_message_id,
            "message_seq": cursor["message_seq"],
            "sent_at": sent_at,
            "image_index": image_index,
            "file": str(data.get("file") or picture.get("fileName") or ""),
            "declared_size": declared_size,
            "resolver_data": resolver_data,
            "original_flag": original,
            "group_uin": group_id,
            "group_name": _text(message.get("group_name")),
            "sender_uin": _text(raw.get("senderUin") or message.get("user_id") or sender.get("user_id")),
            "sender_uid": _text(raw.get("senderUid") or sender.get("user_uid") or sender.get("uid")),
            "sender_member_name": _text(raw.get("sendMemberName") or sender.get("card")),
            "sender_nickname": _text(raw.get("sendNickName") or sender.get("nickname")),
            "sender_remark_name": _text(raw.get("sendRemarkName") or sender.get("remark")),
            "message_text": _message_text(message),
            "discovered_at": int(time.time()),
        }
        image_items.append(item)
        image_index += 1
    return cursor, image_items


def enabled_group_ids(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT group_id FROM monitored_groups WHERE enabled=1"
        ).fetchall()
    }


def record_group_cursor(connection: sqlite3.Connection, cursor: dict[str, Any]) -> None:
    now = int(time.time())
    connection.execute(
        """
        INSERT INTO group_runtime (
            group_id, event_status, last_message_id, last_message_time,
            last_event_at, updated_at
        ) VALUES (?, 'receiving', ?, ?, ?, ?)
        ON CONFLICT(group_id) DO UPDATE SET
            event_status='receiving',
            last_message_id=excluded.last_message_id,
            last_message_time=excluded.last_message_time,
            last_event_at=excluded.last_event_at,
            updated_at=excluded.updated_at
        """,
        (
            cursor["group_id"],
            cursor.get("message_id"),
            int(cursor.get("sent_at") or 0),
            int(cursor.get("event_at") or now),
            now,
        ),
    )
    connection.commit()


def mark_group_image(connection: sqlite3.Connection, group_id: str, timestamp: int) -> None:
    connection.execute(
        "UPDATE group_runtime SET last_image_at=?, updated_at=? WHERE group_id=?",
        (int(timestamp), int(time.time()), str(group_id)),
    )
    connection.commit()


EventHandler = Callable[[dict[str, Any]], Awaitable[None]]
ReconnectHandler = Callable[[float], Awaitable[None]]


class EventListener:
    def __init__(
        self,
        connection: sqlite3.Connection,
        ws_url: str,
        token: str,
        handler: EventHandler,
        reconnect_handler: ReconnectHandler,
        *,
        ping_interval: int = 30,
    ) -> None:
        self.connection = connection
        self.ws_url = ws_url
        self.token = token
        self.handler = handler
        self.reconnect_handler = reconnect_handler
        self.ping_interval = ping_interval
        self._stopping = asyncio.Event()

    def _state(self, **updates: Any) -> dict[str, Any]:
        from .database import get_runtime_state

        state = get_runtime_state(self.connection, "event_stream", {}) or {}
        state.update(updates)
        set_runtime_state(self.connection, "event_stream", state)
        return state

    def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        delay = 1.0
        disconnected_at: float | None = None
        previous = self._state()
        previous_disconnect = (
            previous.get("disconnected_at")
            or previous.get("stopped_at")
            or (previous.get("last_event_at") if previous.get("connected") else None)
        )
        while not self._stopping.is_set():
            headers = {"Authorization": f"Bearer {self.token}"} if self.token else None
            try:
                async with websockets.connect(
                    self.ws_url,
                    additional_headers=headers,
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_interval,
                    close_timeout=5,
                    max_size=16 * 1024 * 1024,
                ) as websocket:
                    now = int(time.time())
                    self._state(
                        connected=True,
                        connected_at=now,
                        disconnected_at=None,
                        stopped_at=None,
                        last_error=None,
                    )
                    boundary = disconnected_at or previous_disconnect
                    if boundary is not None:
                        await self.reconnect_handler(max(0.0, time.time() - float(boundary)))
                    previous_disconnect = None
                    disconnected_at = None
                    delay = 1.0
                    async for payload in websocket:
                        if self._stopping.is_set():
                            break
                        try:
                            event = json.loads(payload)
                        except (TypeError, json.JSONDecodeError):
                            continue
                        if not isinstance(event, dict):
                            continue
                        increment_counter(self.connection, "events")
                        self._state(
                            connected=True,
                            connected_at=now,
                            last_event_at=int(time.time()),
                            last_error=None,
                        )
                        await self.handler(event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                disconnected_at = disconnected_at or time.time()
                self.connection.execute(
                    "UPDATE group_runtime SET event_status='disconnected', updated_at=? WHERE group_id IN (SELECT group_id FROM monitored_groups WHERE enabled=1)",
                    (int(time.time()),),
                )
                self.connection.commit()
                self._state(
                    connected=False,
                    disconnected_at=int(disconnected_at),
                    last_error=f"{type(exc).__name__}: {exc}",
                )
                wait = min(60.0, delay) + random.uniform(0, min(3.0, delay / 2))
                delay = min(60.0, delay * 2)
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=wait)
                except TimeoutError:
                    pass
        self._state(connected=False, stopped_at=int(time.time()), last_error=None)


def history_events(payload: Any) -> Iterable[dict[str, Any]]:
    if isinstance(payload, dict):
        rows = payload.get("messages") or []
    else:
        rows = payload or []
    for row in rows:
        if isinstance(row, dict):
            yield row
