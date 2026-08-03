from __future__ import annotations

import asyncio
import json
import os
import random
import signal
import sqlite3
import time
import traceback
from contextlib import suppress
from pathlib import Path
from typing import Any

import httpx

from .config import load_settings
from .database import (
    claim_next_image,
    connect_database,
    defer_image,
    ensure_final_directories,
    finish_image,
    get_runtime_state,
    increment_counter,
    queue_snapshot,
    recover_inflight,
    set_runtime_state,
)
from .downloader import (
    CdnDownloader,
    CdnHttpError,
    DownloadPolicyError,
    candidate_urls,
    resolver_data,
)
from .events import (
    EventListener,
    enabled_group_ids,
    history_events,
    mark_group_image,
    parse_group_event,
    record_group_cursor,
)
from .onebot import OneBotClient, OneBotError, websocket_settings


class JobCancelled(RuntimeError):
    pass


def _setting(connection: sqlite3.Connection, key: str, default: Any) -> Any:
    row = connection.execute("SELECT value_json FROM app_settings WHERE key=?", (key,)).fetchone()
    if not row:
        return default
    try:
        return json.loads(str(row[0]))
    except (TypeError, ValueError):
        return default


class CollectorWorker:
    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path
        self.settings = load_settings(config_path)
        self.storage_root = Path(self.settings["storage"]["root"])
        self.connection = connect_database(self.settings["storage"]["database"])
        self.connection.row_factory = sqlite3.Row
        now = int(time.time())
        for group_id in dict.fromkeys(str(value) for value in self.settings.get("groups", [])):
            if not group_id.isdigit():
                continue
            self.connection.execute(
                """
                INSERT INTO monitored_groups(group_id, enabled, created_at, updated_at)
                VALUES (?, 1, ?, ?) ON CONFLICT(group_id) DO NOTHING
                """,
                (group_id, now, now),
            )
            self.connection.execute(
                "INSERT INTO group_runtime(group_id, updated_at) VALUES (?, ?) ON CONFLICT(group_id) DO NOTHING",
                (group_id, now),
            )
        self.connection.commit()
        ensure_final_directories(self.storage_root)
        recover_inflight(self.connection)
        self.onebot = OneBotClient.from_settings(self.settings["onebot"])
        self.onebot.on_policy_violation = self._handle_policy_violation
        ws_url, ws_token = websocket_settings(self.settings["onebot"])
        runtime = self.settings["runtime"]
        self.listener = EventListener(
            self.connection,
            ws_url,
            ws_token,
            self.handle_event,
            self.handle_reconnect,
            ping_interval=int(runtime["ws_ping_interval_seconds"]),
            state_heartbeat_interval=int(runtime["event_state_heartbeat_seconds"]),
        )
        self.downloader = CdnDownloader(
            self.connection,
            self.storage_root,
            max_bytes=int(runtime["max_download_bytes"]),
            daily_limit=0,
            url_preference=str(runtime.get("url_preference") or "data"),
        )
        self.stop_event = asyncio.Event()
        self._gap_lock = asyncio.Lock()
        self._history_lock = asyncio.Lock()
        state = get_runtime_state(self.connection, "downloader", {}) or {}
        self._downloader_state = dict(state)
        self._downloader_state_written_at = 0.0
        self.accelerated = bool((state or {}).get("accelerated", False))

    def request_stop(self) -> None:
        self.listener.stop()
        self.stop_event.set()

    def runtime(self, key: str) -> Any:
        return _setting(self.connection, key, self.settings["runtime"].get(key))

    def _handle_policy_violation(self, action: str) -> None:
        if action != "get_image":
            return
        increment_counter(self.connection, "get_image_blocked")
        now = int(time.time())
        set_runtime_state(
            self.connection,
            "critical_alarm",
            {
                "active": True,
                "reason": "blocked production get_image call attempt",
                "action": action,
                "created_at": now,
            },
        )

    async def handle_event(self, event: dict[str, Any]) -> None:
        cursor, items = parse_group_event(event)
        if cursor is None:
            return
        for attempt in range(6):
            try:
                groups = enabled_group_ids(self.connection)
                if cursor["group_id"] not in groups:
                    return
                increment_counter(self.connection, "group_messages")
                record_group_cursor(self.connection, cursor)
                if not items:
                    return
                from .database import enqueue_image

                for item in items:
                    increment_counter(self.connection, "images_seen")
                    increment_counter(self.connection, "image_segments")
                    enqueue_image(self.connection, item)
                    mark_group_image(self.connection, item["group_id"], int(time.time()))
                return
            except sqlite3.OperationalError as exc:
                self.connection.rollback()
                if "locked" not in str(exc).casefold() or attempt == 5:
                    raise
                await asyncio.sleep(min(2.0, 0.1 * (2**attempt)))

    async def _history_call(self, params: dict[str, Any]) -> Any:
        async with self._history_lock:
            increment_counter(self.connection, "history_calls")
            return await self.onebot.call_async("get_group_msg_history", params)

    async def handle_reconnect(self, disconnected_seconds: float) -> None:
        if disconnected_seconds < float(self.runtime("ws_disconnect_gap_seconds")):
            return
        async with self._gap_lock:
            for group_id in sorted(enabled_group_ids(self.connection)):
                try:
                    await self.recover_gap(group_id, automatic=True)
                except Exception as exc:
                    self._set_gap(group_id, "error", f"{type(exc).__name__}: {exc}", finished=True)

    def _set_gap(
        self,
        group_id: str,
        status: str,
        error: str | None = None,
        *,
        started: bool = False,
        finished: bool = False,
    ) -> None:
        now = int(time.time())
        for attempt in range(4):
            try:
                self.connection.execute(
                    """
                    UPDATE group_runtime SET gap_status=?, gap_error=?,
                        gap_started_at=CASE WHEN ? THEN ? ELSE gap_started_at END,
                        gap_finished_at=CASE WHEN ? THEN ? ELSE gap_finished_at END,
                        updated_at=? WHERE group_id=?
                    """,
                    (status, error, int(started), now, int(finished), now, now, group_id),
                )
                self.connection.commit()
                return
            except sqlite3.OperationalError as exc:
                self.connection.rollback()
                if "locked" not in str(exc).casefold():
                    raise
                if attempt < 3:
                    time.sleep(min(1.0, 0.1 * (2**attempt)))

    async def recover_gap(
        self,
        group_id: str,
        *,
        automatic: bool = False,
        job_id: int | None = None,
    ) -> int:
        row = self.connection.execute(
            "SELECT last_message_id, last_message_seq, last_message_time FROM group_runtime WHERE group_id=?",
            (str(group_id),),
        ).fetchone()
        live_raw_anchor = str(row[0] or "") if row else ""
        anchor_seq = str(row[1] or "") if row else ""
        anchor_time = int(row[2] or 0) if row else 0
        if not live_raw_anchor or not anchor_seq or anchor_time <= 0:
            raise ValueError("group has no durable live raw message cursor yet")
        self._set_gap(group_id, "recovering", started=True)
        page_size = int(self.runtime("history_page_size"))
        del automatic
        discovered = 0
        current_anchor = live_raw_anchor
        current_seq = int(anchor_seq) if anchor_seq.isdecimal() else 0
        newest_time = anchor_time
        cutoff_time = int(time.time())
        page_index = 0
        from .database import enqueue_image

        while not self.stop_event.is_set():
            if job_id is not None:
                cancelled = self.connection.execute(
                    "SELECT cancel_requested FROM jobs WHERE id=?", (job_id,)
                ).fetchone()
                if cancelled and int(cancelled[0]):
                    raise JobCancelled("gap recovery cancelled at a page boundary")
            payload = await self._history_call(
                {
                    "group_id": str(group_id),
                    # NapCat maps this field to a short message ID or raw NT
                    # msgId. It is not the raw msgSeq despite the API name.
                    "message_seq": current_anchor,
                    "count": page_size,
                    "reverse_order": False,
                    "disable_get_url": False,
                    "parse_mult_msg": False,
                }
            )
            messages = list(history_events(payload))
            if not messages:
                break
            page_newest_anchor = current_anchor
            page_newest_seq = current_seq
            page_newest_time = newest_time
            for event in messages:
                event.setdefault("post_type", "message")
                event.setdefault("message_type", "group")
                event.setdefault("group_id", str(group_id))
                cursor, items = parse_group_event(event)
                if not cursor:
                    continue
                event_time = int(cursor.get("sent_at") or 0)
                event_seq_text = str(cursor.get("message_seq") or "")
                event_seq = int(event_seq_text) if event_seq_text.isdecimal() else 0
                event_anchor = str(cursor.get("message_id") or "")
                is_newer = event_time > page_newest_time or (
                    event_time == page_newest_time and event_seq > page_newest_seq
                )
                if is_newer and event_anchor:
                    page_newest_anchor = event_anchor
                    page_newest_seq = event_seq
                    page_newest_time = event_time
                in_gap = anchor_time < event_time <= cutoff_time or (
                    event_time == anchor_time and event_seq > current_seq
                )
                if not in_gap:
                    continue
                for item in items:
                    increment_counter(self.connection, "images_seen")
                    increment_counter(self.connection, "image_segments")
                    enqueue_image(self.connection, item)
                    discovered += 1
            if job_id is not None:
                self.connection.execute(
                    "UPDATE jobs SET progress_pages=?, updated_at=? WHERE id=?",
                    (page_index + 1, int(time.time()), job_id),
                )
                self.connection.commit()
            page_index += 1
            if len(messages) < page_size:
                break
            if page_newest_anchor == current_anchor or (
                page_newest_time < newest_time
                or (page_newest_time == newest_time and page_newest_seq <= current_seq)
            ):
                self._set_gap(
                    group_id,
                    "partial",
                    "history source made no forward progress from the durable cursor",
                    finished=True,
                )
                return discovered
            current_anchor = page_newest_anchor
            current_seq = page_newest_seq
            newest_time = page_newest_time
            await asyncio.sleep(max(0, int(self.runtime("history_page_interval_seconds"))))
        # History results intentionally never overwrite last_message_id or
        # last_message_seq.  Those are durable live-WS anchors; NapCat history
        # message_id values are process-local short IDs.
        self._set_gap(group_id, "complete", None, finished=True)
        return discovered

    async def refresh_url(self, row: sqlite3.Row) -> bool:
        data = resolver_data(row)
        if data.get("url_refresh_attempted"):
            return False
        raw_anchor = str(data.get("raw_message_id") or row["message_id"] or "")
        if not raw_anchor:
            return False
        async with self._history_lock:
            data["url_refresh_attempted"] = True
            self.connection.execute(
                """
                UPDATE images SET resolver_json=?, updated_at=?
                WHERE group_id=? AND message_id=? AND image_index=?
                """,
                (
                    json.dumps(data, ensure_ascii=False),
                    int(time.time()),
                    row["group_id"],
                    row["message_id"],
                    row["image_index"],
                ),
            )
            increment_counter(self.connection, "history_calls")
            payload = await self.onebot.call_async(
                "get_group_msg_history",
                {
                    "group_id": str(row["group_id"]),
                    "message_seq": raw_anchor,
                    "count": 1,
                    "reverse_order": False,
                    "disable_get_url": False,
                    "parse_mult_msg": False,
                },
            )
        messages = list(history_events(payload))
        if not messages:
            return False
        target = messages[0]
        for message in messages:
            raw = message.get("raw") if isinstance(message.get("raw"), dict) else {}
            identifiers = {
                str(raw.get("msgId") or ""),
                str(message.get("message_id") or ""),
                str(message.get("real_id") or ""),
            }
            if raw_anchor in identifiers:
                target = message
                break
        _cursor, items = parse_group_event(
            target
            | {
                "post_type": "message",
                "message_type": "group",
                "group_id": str(row["group_id"]),
            }
        )
        index = int(row["image_index"])
        if index >= len(items):
            return False
        refreshed = items[index].get("resolver_data") or {}
        url = str(refreshed.get("url") or "")
        if not url:
            return False
        previous_url = str(data.get("url") or "")
        previous_origin_url = str(data.get("origin_url") or "")
        data["url"] = url
        data["origin_url"] = str(refreshed.get("origin_url") or data.get("origin_url") or "")
        for key in (
            "url_host",
            "origin_url_host",
            "data_url_has_rkey",
            "origin_url_has_rkey",
            "url_expires_at",
            "url_expiry_basis",
            "raw_match",
        ):
            if key in refreshed:
                data[key] = refreshed[key]
        data["url_refresh_attempted"] = False
        data["url_refreshed"] = True
        data["url_refresh_count"] = int(data.get("url_refresh_count") or 0) + 1
        unchanged = (
            data["url"] == previous_url
            and data["origin_url"] == previous_origin_url
        )
        now = int(time.time())
        retry_delay = min(
            3600,
            60 * (2 ** min(6, max(0, int(data["url_refresh_count"]) - 1))),
        )
        self.connection.execute(
            """
            UPDATE images SET resolver_json=?, status=?, next_retry_at=?, error=?, updated_at=?
            WHERE group_id=? AND message_id=? AND image_index=?
            """,
            (
                json.dumps(data, ensure_ascii=False),
                "deferred" if unchanged else "queued",
                now + retry_delay if unchanged else 0,
                "history returned the same expired CDN URL; retry scheduled"
                if unchanged
                else None,
                now,
                row["group_id"],
                row["message_id"],
                row["image_index"],
            ),
        )
        self.connection.commit()
        return True

    def _persist_downloader_state(self, *, force: bool = False, **updates: Any) -> None:
        state = self._downloader_state
        changed = any(
            state.get(key) != value
            for key, value in updates.items()
            if key != "heartbeat_at"
        )
        state.update(updates)
        state["accelerated"] = self.accelerated
        state["unlimited"] = True
        state.pop("circuit_until", None)
        state.pop("recent_403", None)
        state.pop("reason", None)
        now = time.monotonic()
        if not force and not changed and now - self._downloader_state_written_at < 10:
            return
        try:
            set_runtime_state(self.connection, "downloader", state)
            self._downloader_state_written_at = now
        except sqlite3.OperationalError as exc:
            self.connection.rollback()
            if "locked" not in str(exc).casefold():
                raise

    def _defer_refresh_retry(
        self,
        row: sqlite3.Row,
        *,
        delay_seconds: int,
        error: str,
    ) -> None:
        current = self.connection.execute(
            """
            SELECT resolver_json FROM images
            WHERE group_id=? AND message_id=? AND image_index=?
            """,
            (row["group_id"], row["message_id"], row["image_index"]),
        ).fetchone()
        try:
            data = json.loads(str(current[0] or "{}")) if current else resolver_data(row)
        except (TypeError, ValueError):
            data = resolver_data(row)
        if not isinstance(data, dict):
            data = {}
        data["url_refresh_attempted"] = False
        failures = int(data.get("url_refresh_failures") or 0) + 1
        data["url_refresh_failures"] = failures
        now = int(time.time())
        retry_delay = max(
            max(1, int(delay_seconds)),
            min(3600, 60 * (2 ** min(6, max(0, failures - 1)))),
        )
        self.connection.execute(
            """
            UPDATE images SET status='deferred', next_retry_at=?, error=?,
                resolver_json=?, updated_at=?
            WHERE group_id=? AND message_id=? AND image_index=?
            """,
            (
                now + retry_delay,
                error[:2048],
                json.dumps(data, ensure_ascii=False),
                now,
                row["group_id"],
                row["message_id"],
                row["image_index"],
            ),
        )
        self.connection.commit()

    async def _process_claimed(self, row: sqlite3.Row) -> None:
        try:
            if not candidate_urls(row, self.downloader.url_preference):
                try:
                    refreshed = await self.refresh_url(row)
                except Exception as refresh_error:
                    self._defer_refresh_retry(
                        row,
                        delay_seconds=300,
                        error=f"missing URL refresh retry after {type(refresh_error).__name__}",
                    )
                    return
                if not refreshed:
                    self._defer_refresh_retry(
                        row,
                        delay_seconds=300,
                        error="image event has no usable CDN URL; refresh will retry",
                    )
                return
            result = await self.downloader.process(row)
            self._persist_downloader_state(
                status="running",
                last_success_at=int(time.time()),
                last_result=result,
                last_error=None,
            )
        except CdnHttpError as exc:
            if exc.status_code in {400, 403, 404, 410}:
                try:
                    refreshed = await self.refresh_url(row)
                except Exception as refresh_error:
                    self._defer_refresh_retry(
                        row,
                        delay_seconds=300,
                        error=f"URL refresh retry after {type(refresh_error).__name__}",
                    )
                    return
                if refreshed:
                    return
                self._defer_refresh_retry(
                    row,
                    delay_seconds=300,
                    error=f"CDN URL returned {exc.status_code}; refresh will retry",
                )
                return
            if exc.status_code == 429:
                pause = int(self.runtime("cdn_429_pause_seconds"))
                defer_image(self.connection, row, delay_seconds=pause, error=str(exc))
                return
            if exc.status_code in {408, 425} or 500 <= exc.status_code <= 599:
                attempts = int(row["attempts"] or 0) + 1
                defer_image(
                    self.connection,
                    row,
                    delay_seconds=min(3600, 60 * (2 ** min(6, max(0, attempts - 1)))),
                    error=f"transient CDN HTTP {exc.status_code}; retry {attempts}",
                )
                return
            defer_image(
                self.connection,
                row,
                delay_seconds=300,
                error=f"CDN HTTP {exc.status_code}; retry scheduled",
            )
        except DownloadPolicyError as exc:
            finish_image(self.connection, row, status="failed_terminal", error=str(exc))
            increment_counter(self.connection, "failed")
        except (httpx.HTTPError, OSError, asyncio.TimeoutError, TimeoutError) as exc:
            attempts = int(row["attempts"] or 0) + 1
            safe_error = type(exc).__name__
            defer_image(
                self.connection,
                row,
                delay_seconds=min(3600, 60 * (2 ** min(6, max(0, attempts - 1)))),
                error=f"{safe_error}; retry {attempts}",
            )
        except Exception as exc:
            defer_image(
                self.connection,
                row,
                delay_seconds=60,
                error=f"unexpected {type(exc).__name__}; worker restarting",
            )
            raise

    async def download_loop(self) -> None:
        runtime = self.settings["runtime"]
        while not self.stop_event.is_set():
            self._persist_downloader_state(heartbeat_at=int(time.time()))
            self.downloader.url_preference = str(self.runtime("url_preference") or "data")
            if bool(self.runtime("collector_paused")):
                self._persist_downloader_state(status="paused", last_error=None)
                await asyncio.sleep(2)
                continue
            row = claim_next_image(
                self.connection,
                expiry_urgent_seconds=int(self.runtime("url_expiry_urgent_seconds")),
            )
            if row is None:
                self.accelerated = False
                self._persist_downloader_state(status="idle", last_error=None)
                await asyncio.sleep(1)
                continue
            await self._process_claimed(row)
            snapshot = queue_snapshot(self.connection)
            oldest_age = int(snapshot["oldest_age_seconds"])
            if not self.accelerated and oldest_age >= int(runtime["accelerate_queue_age_seconds"]):
                self.accelerated = True
            elif self.accelerated and oldest_age <= int(runtime["resume_normal_queue_age_seconds"]):
                self.accelerated = False
            if self.accelerated:
                base = float(runtime["accelerated_interval_seconds"])
            else:
                base = float(self.runtime("download_interval_seconds"))
            jitter = float(self.runtime("download_jitter_seconds"))
            delay = max(1.0, base + random.uniform(-jitter, jitter))
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    async def job_loop(self) -> None:
        while not self.stop_event.is_set():
            row = self.connection.execute(
                """
                SELECT id, group_id FROM jobs
                WHERE kind='gap_recovery' AND status='queued'
                ORDER BY created_at, id LIMIT 1
                """
            ).fetchone()
            if not row:
                await asyncio.sleep(2)
                continue
            job_id, group_id = int(row[0]), str(row[1])
            now = int(time.time())
            self.connection.execute(
                "UPDATE jobs SET status='running', started_at=?, updated_at=? WHERE id=?",
                (now, now, job_id),
            )
            self.connection.commit()
            try:
                async with self._gap_lock:
                    await self.recover_gap(group_id, job_id=job_id)
                now = int(time.time())
                self.connection.execute(
                    """
                    UPDATE jobs SET status='completed', finished_at=?, updated_at=?
                    WHERE id=?
                    """,
                    (now, now, job_id),
                )
            except JobCancelled as exc:
                now = int(time.time())
                self.connection.execute(
                    """
                    UPDATE jobs SET status='cancelled', error=?, finished_at=?, updated_at=?
                    WHERE id=?
                    """,
                    (str(exc), now, now, job_id),
                )
            except Exception as exc:
                now = int(time.time())
                self.connection.execute(
                    """
                    UPDATE jobs SET status='failed', error=?, finished_at=?, updated_at=?
                    WHERE id=?
                    """,
                    (f"{type(exc).__name__}: {exc}"[:2048], now, now, job_id),
                )
            self.connection.commit()

    async def heartbeat_loop(self) -> None:
        interval = max(2, int(self.runtime("worker_heartbeat_seconds")))
        while not self.stop_event.is_set():
            state = get_runtime_state(self.connection, "worker", {}) or {}
            state.update(
                running=True,
                pid=os.getpid(),
                heartbeat_at=int(time.time()),
            )
            set_runtime_state(
                self.connection,
                "worker",
                state,
            )
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    async def run(self) -> None:
        started_at = int(time.time())
        set_runtime_state(
            self.connection,
            "worker",
            {
                "running": True,
                "pid": os.getpid(),
                "started_at": started_at,
                "heartbeat_at": started_at,
                "last_error": None,
            },
        )
        tasks = [
            asyncio.create_task(self.listener.run(), name="onebot-events"),
            asyncio.create_task(self.download_loop(), name="cdn-downloader"),
            asyncio.create_task(self.job_loop(), name="gap-jobs"),
            asyncio.create_task(self.heartbeat_loop(), name="worker-heartbeat"),
        ]
        stop_task = asyncio.create_task(self.stop_event.wait(), name="worker-stop")
        try:
            done, _pending = await asyncio.wait(
                [stop_task, *tasks],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_task not in done:
                failed = next(task for task in tasks if task in done)
                try:
                    exception = failed.exception()
                except asyncio.CancelledError as exc:
                    exception = exc
                if exception is None:
                    exception = RuntimeError(f"background task {failed.get_name()} stopped unexpectedly")
                try:
                    set_runtime_state(
                        self.connection,
                        "worker",
                        {
                            "running": False,
                            "pid": os.getpid(),
                            "heartbeat_at": int(time.time()),
                            "failed_task": failed.get_name(),
                            "last_error": f"{type(exception).__name__}: {exception}",
                        },
                    )
                except Exception:
                    self.connection.rollback()
                raise exception
        finally:
            self.listener.stop()
            stop_task.cancel()
            for task in tasks:
                task.cancel()
            await asyncio.gather(stop_task, *tasks, return_exceptions=True)
            await self.downloader.close()
            try:
                previous = get_runtime_state(self.connection, "worker", {}) or {}
                previous.update(
                    running=False,
                    pid=None,
                    stopped_at=int(time.time()),
                )
                set_runtime_state(self.connection, "worker", previous)
            except Exception:
                self.connection.rollback()
            self.connection.close()


async def run_worker(config_path: Path) -> None:
    loop = asyncio.get_running_loop()
    shutdown = asyncio.Event()
    active: CollectorWorker | None = None

    def request_stop() -> None:
        shutdown.set()
        if active is not None:
            active.request_stop()

    for signum in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(signum, request_stop)

    while not shutdown.is_set():
        try:
            active = CollectorWorker(config_path)
            await active.run()
            if not shutdown.is_set():
                raise RuntimeError("worker stopped without a process shutdown request")
        except asyncio.CancelledError:
            raise
        except Exception:
            traceback.print_exc()
            if shutdown.is_set():
                break
            delay = max(1, int(load_settings(config_path)["runtime"]["worker_restart_delay_seconds"]))
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
        finally:
            active = None
