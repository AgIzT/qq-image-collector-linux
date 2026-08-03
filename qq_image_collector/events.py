from __future__ import annotations

import asyncio
import json
import random
import re
import sqlite3
import time
from collections.abc import Awaitable, Callable, Iterable
from typing import Any
from urllib.parse import parse_qs, urlsplit

import websockets

from .database import increment_counter, set_runtime_state


MD5_TOKEN = re.compile(r"(?i)(?<![0-9a-f])([0-9a-f]{32})(?![0-9a-f])")
# A production header-only probe on 2026-08-03 observed 200 responses through
# 30 minutes and 400 responses around 60 minutes.  This is a scheduling window,
# not a claim about the server-side token's exact TTL.
RKEY_TTL_HINT_SECONDS = 30 * 60


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


def _md5_token(*values: Any) -> str | None:
    for value in values:
        match = MD5_TOKEN.search(str(value or ""))
        if match:
            return match.group(1).lower()
    return None


def _file_name(value: Any) -> str:
    text = str(value or "").replace("\\", "/").rsplit("/", 1)[-1]
    return text.casefold()


def _match_raw_picture(
    data: dict[str, Any],
    pictures: list[dict[str, Any]],
    used: set[int],
) -> tuple[dict[str, Any], str]:
    """Match a OneBot image segment to raw NT data without trusting list order blindly."""

    data_md5 = _md5_token(
        data.get("md5"),
        data.get("md5HexStr"),
        data.get("file"),
        data.get("file_id"),
        data.get("file_name"),
    )
    data_name = _file_name(data.get("file_name") or data.get("filename") or data.get("file"))
    if data_name:
        for index, picture in enumerate(pictures):
            if index in used:
                continue
            if _file_name(picture.get("fileName")) == data_name:
                used.add(index)
                return picture, "filename"
    for index, picture in enumerate(pictures):
        if index in used:
            continue
        raw_md5 = _md5_token(picture.get("md5HexStr"), picture.get("fileName"))
        if data_md5 and raw_md5 == data_md5:
            used.add(index)
            return picture, "md5"

    remaining = [(index, value) for index, value in enumerate(pictures) if index not in used]
    if not remaining:
        return {}, "missing"
    # A named standard segment that did not match a named raw picture may be a
    # market-face element interleaved with ordinary pictures.  Do not consume a
    # raw picture merely because it happens to be next in an array.
    if data_name or data_md5:
        return {}, "mismatch"
    if len(remaining) != 1:
        return {}, "ambiguous"
    index, picture = remaining[0]
    raw_md5 = _md5_token(picture.get("md5HexStr"), picture.get("fileName"))
    if data_md5 and raw_md5 and data_md5 != raw_md5:
        # A known mismatch is worse than missing raw metadata.  Falling back to
        # the ordinary OneBot segment avoids assigning another image's URL,
        # original flag, dimensions or MD5 to this queue item.
        return {}, "mismatch"
    used.add(index)
    return picture, "position-unverified"


def _url_traits(url: str) -> tuple[str, bool]:
    try:
        parsed = urlsplit(url)
        query = parse_qs(parsed.query, keep_blank_values=True)
    except ValueError:
        return "", False
    return (parsed.hostname or "").lower(), any(key.casefold() == "rkey" for key in query)


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
    raw_nt_message_id = str(raw.get("msgId") or "")
    raw_nt_message_seq = str(raw.get("msgSeq") or "")
    raw_message_id = str(
        raw_nt_message_id
        or message.get("real_id")
        or message.get("message_id")
        or message.get("real_seq")
        or message.get("message_seq")
        or ""
    )
    sent_at = _integer(raw.get("msgTime") or message.get("time"))
    message_seq = str(
        raw_nt_message_seq
        or message.get("real_seq")
        or message.get("message_seq")
        or ""
    )
    cursor = {
        "group_id": group_id,
        "message_id": raw_message_id,
        "message_seq": message_seq,
        "sent_at": sent_at,
        "event_at": int(time.time()),
        # Only debug raw msgId+msgSeq form a durable live anchor.  OneBot
        # message_id/message_seq values can be process-local short IDs.
        "durable_raw": bool(raw_nt_message_id and raw_nt_message_seq),
    }
    raw_pictures = _raw_picture_elements(raw)
    used_raw_pictures: set[int] = set()
    image_items: list[dict[str, Any]] = []
    image_index = 0

    def build_item(
        data: dict[str, Any],
        picture: dict[str, Any],
        raw_match: str,
        index: int,
    ) -> dict[str, Any]:
        declared_size = _integer(data.get("file_size") or picture.get("fileSize"))
        original = _optional_bool(
            picture.get("original")
            if picture.get("original") is not None
            else data.get("original")
        )
        data_url = str(data.get("url") or "")
        origin_url = str(picture.get("originImageUrl") or "")
        data_host, data_has_rkey = _url_traits(data_url)
        origin_host, origin_has_rkey = _url_traits(origin_url)
        selected_has_rkey = data_has_rkey or origin_has_rkey
        discovered_at = int(time.time())
        resolver_data = {
            "url": data_url,
            "origin_url": origin_url,
            "url_host": data_host,
            "origin_url_host": origin_host,
            "data_url_has_rkey": data_has_rkey,
            "origin_url_has_rkey": origin_has_rkey,
            "url_expires_at": discovered_at + RKEY_TTL_HINT_SECONDS
            if selected_has_rkey
            else None,
            "url_expiry_basis": "observed-any-rkey-30m-scheduling-window"
            if selected_has_rkey
            else None,
            "raw_message_id": raw_nt_message_id or None,
            "raw_message_seq": raw_nt_message_seq
            or str(message.get("real_seq") or "")
            or None,
            "raw_match": raw_match,
            "width": _integer(picture.get("picWidth") or data.get("width")),
            "height": _integer(picture.get("picHeight") or data.get("height")),
            "md5": str(picture.get("md5HexStr") or ""),
            "summary": str(data.get("summary") or picture.get("summary") or ""),
            "sub_type": data.get("sub_type", picture.get("picSubType")),
            "emoji_signal": _emoji_signal(data, picture),
            "url_refreshed": False,
        }
        return {
            "group_id": group_id,
            "message_id": raw_message_id,
            "message_seq": message_seq,
            "sent_at": sent_at,
            "image_index": index,
            "file": str(data.get("file") or picture.get("fileName") or ""),
            "declared_size": declared_size,
            "resolver_data": resolver_data,
            "original_flag": original,
            "group_uin": group_id,
            "group_name": _text(message.get("group_name")),
            "sender_uin": _text(
                raw.get("senderUin") or message.get("user_id") or sender.get("user_id")
            ),
            "sender_uid": _text(
                raw.get("senderUid") or sender.get("user_uid") or sender.get("uid")
            ),
            "sender_member_name": _text(raw.get("sendMemberName") or sender.get("card")),
            "sender_nickname": _text(raw.get("sendNickName") or sender.get("nickname")),
            "sender_remark_name": _text(raw.get("sendRemarkName") or sender.get("remark")),
            "message_text": _message_text(message),
            "discovered_at": discovered_at,
        }

    for segment in message.get("message") or []:
        if not isinstance(segment, dict) or segment.get("type") != "image":
            continue
        data = segment.get("data") if isinstance(segment.get("data"), dict) else {}
        picture, raw_match = _match_raw_picture(data, raw_pictures, used_raw_pictures)
        image_items.append(build_item(data, picture, raw_match, image_index))
        image_index += 1

    # Converter failures may omit a standard OneBot image segment even though
    # debug raw still contains picElement.originImageUrl.  Persist every such
    # raw picture instead of silently losing it; an absent URL becomes an
    # explicit `expired` alert in the downloader.
    for raw_index, picture in enumerate(raw_pictures):
        if raw_index in used_raw_pictures:
            continue
        image_items.append(build_item({}, picture, "raw-only", image_index))
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
    durable_raw = bool(cursor.get("durable_raw"))
    connection.execute(
        """
        INSERT INTO group_runtime (
            group_id, event_status, last_message_id, last_message_seq, last_message_time,
            last_event_at, updated_at
        ) VALUES (?, 'receiving', ?, ?, ?, ?, ?)
        ON CONFLICT(group_id) DO UPDATE SET
            event_status='receiving',
            last_message_id=CASE WHEN ? THEN excluded.last_message_id
                                 ELSE group_runtime.last_message_id END,
            last_message_seq=CASE WHEN ? THEN excluded.last_message_seq
                                  ELSE group_runtime.last_message_seq END,
            last_message_time=CASE WHEN ? THEN excluded.last_message_time
                                   ELSE group_runtime.last_message_time END,
            last_event_at=excluded.last_event_at,
            updated_at=excluded.updated_at
        """,
        (
            cursor["group_id"],
            cursor.get("message_id") if durable_raw else None,
            cursor.get("message_seq") if durable_raw else None,
            int(cursor.get("sent_at") or 0) if durable_raw else None,
            int(cursor.get("event_at") or now),
            now,
            int(durable_raw),
            int(durable_raw),
            int(durable_raw),
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
        state_heartbeat_interval: int = 10,
    ) -> None:
        self.connection = connection
        self.ws_url = ws_url
        self.token = token
        self.handler = handler
        self.reconnect_handler = reconnect_handler
        self.ping_interval = ping_interval
        self.state_heartbeat_interval = max(2, int(state_heartbeat_interval))
        self._state_write_interval = min(10, self.state_heartbeat_interval)
        self._stopping = asyncio.Event()
        self._runtime_state: dict[str, Any] | None = None
        self._last_state_write = 0.0

    def _state(self, *, force: bool = False, **updates: Any) -> dict[str, Any]:
        from .database import get_runtime_state

        if self._runtime_state is None:
            self._runtime_state = get_runtime_state(self.connection, "event_stream", {}) or {}
        state = self._runtime_state
        state.update(updates)
        now = time.monotonic()
        if not force and now - self._last_state_write < self._state_write_interval:
            return dict(state)
        try:
            set_runtime_state(self.connection, "event_stream", state)
            self._last_state_write = now
        except sqlite3.OperationalError as exc:
            self.connection.rollback()
            if "locked" not in str(exc).casefold():
                raise
        return dict(state)

    def stop(self) -> None:
        self._stopping.set()

    def _mark_groups_connected(self) -> None:
        try:
            self.connection.execute(
                """
                UPDATE group_runtime SET event_status='connected', updated_at=?
                WHERE group_id IN (
                    SELECT group_id FROM monitored_groups WHERE enabled=1
                )
                """,
                (int(time.time()),),
            )
            self.connection.commit()
        except sqlite3.OperationalError as exc:
            self.connection.rollback()
            if "locked" not in str(exc).casefold():
                raise

    async def _connection_heartbeat(self) -> None:
        while not self._stopping.is_set():
            self._state(
                connected=True,
                heartbeat_at=int(time.time()),
                last_error=None,
            )
            try:
                await asyncio.wait_for(
                    self._stopping.wait(), timeout=self.state_heartbeat_interval
                )
            except asyncio.TimeoutError:
                pass

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
                        force=True,
                        connected=True,
                        connected_at=now,
                        heartbeat_at=now,
                        disconnected_at=None,
                        stopped_at=None,
                        last_error=None,
                    )
                    self._mark_groups_connected()
                    heartbeat_task = asyncio.create_task(
                        self._connection_heartbeat(), name="event-stream-heartbeat"
                    )
                    try:
                        boundary = disconnected_at or previous_disconnect
                        if boundary is not None:
                            await self.reconnect_handler(
                                max(0.0, time.time() - float(boundary))
                            )
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
                            try:
                                increment_counter(self.connection, "events")
                            except sqlite3.OperationalError as exc:
                                self.connection.rollback()
                                if "locked" not in str(exc).casefold():
                                    raise
                            self._state(
                                connected=True,
                                connected_at=now,
                                heartbeat_at=int(time.time()),
                                last_event_at=int(time.time()),
                                last_error=None,
                            )
                            await self.handler(event)
                    finally:
                        heartbeat_task.cancel()
                        await asyncio.gather(heartbeat_task, return_exceptions=True)
                    if not self._stopping.is_set():
                        raise ConnectionError("OneBot WebSocket closed without a close exception")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                disconnected_at = disconnected_at or time.time()
                try:
                    self.connection.execute(
                        "UPDATE group_runtime SET event_status='disconnected', updated_at=? WHERE group_id IN (SELECT group_id FROM monitored_groups WHERE enabled=1)",
                        (int(time.time()),),
                    )
                    self.connection.commit()
                except sqlite3.OperationalError as database_error:
                    self.connection.rollback()
                    if "locked" not in str(database_error).casefold():
                        raise
                self._state(
                    force=True,
                    connected=False,
                    heartbeat_at=int(time.time()),
                    disconnected_at=int(disconnected_at),
                    last_error=f"{type(exc).__name__}: {exc}",
                )
                wait = min(60.0, delay) + random.uniform(0, min(3.0, delay / 2))
                delay = min(60.0, delay * 2)
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=wait)
                except asyncio.TimeoutError:
                    pass
        self._state(force=True, connected=False, stopped_at=int(time.time()), last_error=None)


def history_events(payload: Any) -> Iterable[dict[str, Any]]:
    if isinstance(payload, dict):
        rows = payload.get("messages") or []
    else:
        rows = payload or []
    for row in rows:
        if isinstance(row, dict):
            yield row
