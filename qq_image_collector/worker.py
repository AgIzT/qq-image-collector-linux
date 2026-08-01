from __future__ import annotations

import asyncio
import json
import os
import random
import signal
import sqlite3
import time
from contextlib import suppress
from pathlib import Path
from typing import Any

import httpx

from .config import load_settings
from .database import (
    claim_next_image,
    connect_database,
    counter_sum,
    defer_image,
    ensure_final_directories,
    finish_image,
    get_runtime_state,
    increment_counter,
    local_day_start,
    queue_snapshot,
    recover_inflight,
    set_runtime_state,
)
from .downloader import CdnDownloader, CdnHttpError, DownloadPolicyError, resolver_data
from .events import (
    EventListener,
    enabled_group_ids,
    history_events,
    mark_group_image,
    parse_group_event,
    record_group_cursor,
)
from .onebot import OneBotClient, OneBotError, websocket_settings


class HistoryBudgetExceeded(RuntimeError):
    pass


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
        )
        self.downloader = CdnDownloader(
            self.connection,
            self.storage_root,
            max_bytes=int(runtime["max_download_bytes"]),
            daily_limit=int(runtime["daily_download_limit"]),
            url_preference=str(runtime.get("url_preference") or "data"),
        )
        self.stop_event = asyncio.Event()
        self._gap_lock = asyncio.Lock()
        self._history_lock = asyncio.Lock()
        state = get_runtime_state(self.connection, "downloader", {})
        self.circuit_until = int((state or {}).get("circuit_until") or 0)
        self.recent_403 = [
            int(value)
            for value in ((state or {}).get("recent_403") or [])
            if int(value) > int(time.time()) - int(runtime["cdn_403_window_seconds"])
        ]
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
        self.connection.execute(
            """
            INSERT INTO app_settings(key, value_json, updated_at) VALUES ('collector_paused', 'true', ?)
            ON CONFLICT(key) DO UPDATE SET value_json='true', updated_at=excluded.updated_at
            """,
            (now,),
        )
        self.connection.commit()
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
        self.stop_event.set()

    async def handle_event(self, event: dict[str, Any]) -> None:
        cursor, items = parse_group_event(event)
        if cursor is None:
            return
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
            enqueue_image(self.connection, item)
            mark_group_image(self.connection, item["group_id"], int(time.time()))

    def _history_budget(self) -> None:
        now = int(time.time())
        hourly_limit = int(self.runtime("history_hourly_limit"))
        daily_limit = int(self.runtime("history_daily_limit"))
        if hourly_limit <= 0:
            raise HistoryBudgetExceeded("history calls are disabled")
        if counter_sum(self.connection, "history_calls", now - now % 3600) >= hourly_limit:
            raise HistoryBudgetExceeded("hourly history call limit reached")
        if daily_limit <= 0 or counter_sum(self.connection, "history_calls", local_day_start(now)) >= daily_limit:
            raise HistoryBudgetExceeded("daily history call limit reached")

    async def _history_call(self, params: dict[str, Any]) -> Any:
        async with self._history_lock:
            self._history_budget()
            increment_counter(self.connection, "history_calls")
            return await self.onebot.call_async("get_group_msg_history", params)

    async def handle_reconnect(self, disconnected_seconds: float) -> None:
        if disconnected_seconds < float(self.runtime("ws_disconnect_gap_seconds")):
            return
        async with self._gap_lock:
            for group_id in sorted(enabled_group_ids(self.connection)):
                try:
                    await self.recover_gap(group_id, automatic=True)
                except HistoryBudgetExceeded as exc:
                    self._set_gap(group_id, "deferred", str(exc), finished=True)
                    break
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

    async def recover_gap(
        self,
        group_id: str,
        *,
        automatic: bool = False,
        job_id: int | None = None,
    ) -> int:
        row = self.connection.execute(
            "SELECT last_message_id, last_message_time FROM group_runtime WHERE group_id=?",
            (str(group_id),),
        ).fetchone()
        anchor = str(row[0] or "") if row else ""
        if not anchor:
            raise ValueError("group has no durable event cursor")
        self._set_gap(group_id, "recovering", started=True)
        page_size = int(self.runtime("history_page_size"))
        max_pages = 1 if automatic else int(self.runtime("history_max_pages_per_gap"))
        discovered = 0
        current = anchor
        best_cursor: dict[str, Any] = {
            "group_id": str(group_id),
            "message_id": anchor,
            "sent_at": int(row[1] or 0),
            "event_at": int(time.time()),
        }
        from .database import enqueue_image

        for page_index in range(max_pages):
            if job_id is not None:
                cancelled = self.connection.execute(
                    "SELECT cancel_requested FROM jobs WHERE id=?", (job_id,)
                ).fetchone()
                if cancelled and int(cancelled[0]):
                    raise JobCancelled("gap recovery cancelled at a page boundary")
            payload = await self._history_call(
                {
                    "group_id": str(group_id),
                    "message_seq": current,
                    "count": page_size,
                    "reverse_order": True,
                    "disable_get_url": False,
                    "parse_mult_msg": False,
                }
            )
            messages = list(history_events(payload))
            if not messages:
                break
            page_best = dict(best_cursor)
            for event in messages:
                event.setdefault("post_type", "message")
                event.setdefault("message_type", "group")
                event.setdefault("group_id", str(group_id))
                cursor, items = parse_group_event(event)
                if (
                    cursor
                    and cursor.get("message_id")
                    and int(cursor.get("sent_at") or 0) >= int(page_best.get("sent_at") or 0)
                ):
                    page_best = cursor
                for item in items:
                    increment_counter(self.connection, "images_seen")
                    enqueue_image(self.connection, item)
                    discovered += 1
            if job_id is not None:
                self.connection.execute(
                    "UPDATE jobs SET progress_pages=?, updated_at=? WHERE id=?",
                    (page_index + 1, int(time.time()), job_id),
                )
                self.connection.commit()
            newest = str(page_best.get("message_id") or current)
            if int(page_best.get("sent_at") or 0) >= int(best_cursor.get("sent_at") or 0):
                best_cursor = page_best
            if len(messages) < page_size or newest == current:
                break
            current = newest
        else:
            if str(best_cursor.get("message_id") or "") != anchor:
                record_group_cursor(self.connection, best_cursor)
            self._set_gap(group_id, "partial", "bounded recovery reached its page limit", finished=True)
            return discovered
        if str(best_cursor.get("message_id") or "") != anchor:
            record_group_cursor(self.connection, best_cursor)
        self._set_gap(group_id, "complete", None, finished=True)
        return discovered

    async def refresh_url(self, row: sqlite3.Row) -> bool:
        data = resolver_data(row)
        if data.get("url_refresh_attempted") or data.get("url_refreshed"):
            return False
        raw_id = str(data.get("raw_message_id") or row["message_id"] or "")
        async with self._history_lock:
            self._history_budget()
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
                    "message_seq": raw_id,
                    "count": 1,
                    "reverse_order": False,
                    "disable_get_url": False,
                    "parse_mult_msg": False,
                },
            )
        messages = list(history_events(payload))
        if not messages:
            return False
        _cursor, items = parse_group_event(
            messages[0]
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
        data["url"] = url
        data["origin_url"] = str(refreshed.get("origin_url") or data.get("origin_url") or "")
        data["url_refreshed"] = True
        self.connection.execute(
            """
            UPDATE images SET resolver_json=?, status='queued', next_retry_at=0, updated_at=?
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
        self.connection.commit()
        return True

    def _persist_downloader_state(self, **updates: Any) -> None:
        state = get_runtime_state(self.connection, "downloader", {}) or {}
        state.update(updates)
        state["circuit_until"] = self.circuit_until
        state["recent_403"] = self.recent_403
        state["accelerated"] = self.accelerated
        set_runtime_state(self.connection, "downloader", state)

    def _trip_circuit(self, seconds: int, reason: str) -> None:
        self.circuit_until = max(self.circuit_until, int(time.time()) + int(seconds))
        self._persist_downloader_state(status="circuit_open", reason=reason, last_error=reason)

    def _note_403(self) -> None:
        now = int(time.time())
        window = int(self.runtime("cdn_403_window_seconds"))
        self.recent_403 = [value for value in self.recent_403 if value >= now - window]
        self.recent_403.append(now)
        increment_counter(self.connection, "cdn_403")
        if len(self.recent_403) >= int(self.runtime("cdn_403_trip_count")):
            self._trip_circuit(int(self.runtime("cdn_circuit_seconds")), "three CDN 403 responses within the safety window")
        else:
            self._persist_downloader_state(status="running", last_error="CDN 403")

    async def _process_claimed(self, row: sqlite3.Row) -> None:
        try:
            result = await self.downloader.process(row)
            self._persist_downloader_state(
                status="running",
                last_success_at=int(time.time()),
                last_result=result,
                last_error=None,
            )
        except CdnHttpError as exc:
            if exc.status_code == 403:
                self._note_403()
                try:
                    refreshed = await self.refresh_url(row)
                except HistoryBudgetExceeded as budget_error:
                    defer_image(
                        self.connection,
                        row,
                        delay_seconds=3600,
                        error=str(budget_error),
                    )
                    return
                if refreshed:
                    return
                finish_image(
                    self.connection,
                    row,
                    status="expired",
                    error="CDN URL expired and one bounded refresh failed",
                    http_status=403,
                )
                increment_counter(self.connection, "failed")
                return
            if exc.status_code == 429:
                increment_counter(self.connection, "cdn_429")
                pause = int(self.runtime("cdn_429_pause_seconds"))
                self._trip_circuit(pause, "QQ CDN returned HTTP 429")
                defer_image(self.connection, row, delay_seconds=pause, error=str(exc))
                return
            finish_image(
                self.connection,
                row,
                status="failed_terminal",
                error=str(exc),
                http_status=exc.status_code,
            )
            increment_counter(self.connection, "failed")
        except DownloadPolicyError as exc:
            if "daily CDN" in str(exc):
                defer_image(self.connection, row, delay_seconds=3600, error=str(exc))
            else:
                finish_image(self.connection, row, status="failed_terminal", error=str(exc))
                increment_counter(self.connection, "failed")
        except (httpx.HTTPError, OSError, TimeoutError) as exc:
            attempts = int(row["attempts"] or 0) + 1
            safe_error = type(exc).__name__
            if attempts >= 3:
                finish_image(
                    self.connection,
                    row,
                    status="failed_terminal",
                    error=safe_error,
                )
                increment_counter(self.connection, "failed")
            else:
                defer_image(
                    self.connection,
                    row,
                    delay_seconds=min(3600, 300 * (2 ** max(0, attempts - 1))),
                    error=safe_error,
                )
        except Exception as exc:
            finish_image(
                self.connection,
                row,
                status="failed_terminal",
                error=type(exc).__name__,
            )
            increment_counter(self.connection, "failed")

    async def download_loop(self) -> None:
        runtime = self.settings["runtime"]
        while not self.stop_event.is_set():
            self.downloader.daily_limit = int(self.runtime("daily_download_limit"))
            if bool(self.runtime("collector_paused")):
                self._persist_downloader_state(status="paused", last_error=None)
                await asyncio.sleep(2)
                continue
            now = int(time.time())
            if self.circuit_until > now:
                self._persist_downloader_state(status="circuit_open")
                await asyncio.sleep(min(30, self.circuit_until - now))
                continue
            if self.downloader.daily_remaining() <= 0:
                self._persist_downloader_state(status="daily_quota", last_error=None)
                await asyncio.sleep(60)
                continue
            row = claim_next_image(self.connection)
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

    async def run(self) -> None:
        set_runtime_state(
            self.connection,
            "worker",
            {"running": True, "pid": os.getpid(), "started_at": int(time.time())},
        )
        tasks = [
            asyncio.create_task(self.listener.run(), name="onebot-events"),
            asyncio.create_task(self.download_loop(), name="cdn-downloader"),
            asyncio.create_task(self.job_loop(), name="gap-jobs"),
        ]
        try:
            await self.stop_event.wait()
        finally:
            self.listener.stop()
            for task in tasks:
                task.cancel()
            for task in tasks:
                with suppress(asyncio.CancelledError):
                    await task
            await self.downloader.close()
            set_runtime_state(
                self.connection,
                "worker",
                {"running": False, "pid": None, "stopped_at": int(time.time())},
            )
            self.connection.close()


async def run_worker(config_path: Path) -> None:
    worker = CollectorWorker(config_path)
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(signum, worker.request_stop)
    await worker.run()
