"""Strict, one-shot recovery for a closed production outage window.

This runner is deliberately separate from the always-on worker.  The worker's
normal history budgets must remain zero while this process is active.  History
pagination starts at a current-session raw NT ``msgId`` at or after the fixed
upper bound, moves towards older messages, and never changes the live WS cursor.
If a quiet group has no current-session anchor, one latest-history page is used
only to bootstrap the same backwards traversal.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from .config import load_settings
from .database import (
    connect_database,
    enqueue_image,
    increment_counter,
    local_day_start,
    queue_snapshot,
    set_runtime_state,
)
from .events import history_events, parse_group_event
from .onebot import OneBotClient, OneBotError


NONTERMINAL_STATUSES = ("probe", "queued", "deferred")
CURRENT_UPPER = "current-upper"
LATEST_BOOTSTRAP = "latest-bootstrap"
TERMINAL_STATUSES = (
    "completed",
    "partial_source_exhausted",
    "partial_max_calls",
    "failed_anchor",
    "failed_direction",
    "failed_policy",
)


class WindowRecoveryError(RuntimeError):
    pass


class WindowPolicyError(WindowRecoveryError):
    pass


class WindowDirectionError(WindowRecoveryError):
    pass


class WindowOneBotNotReady(WindowRecoveryError):
    """The local OneBot endpoint is not ready for a history action yet."""

    pass


def _setting(connection: sqlite3.Connection, key: str, default: Any = None) -> Any:
    row = connection.execute(
        "SELECT value_json FROM app_settings WHERE key=?", (key,)
    ).fetchone()
    if not row:
        return default
    try:
        return json.loads(str(row[0]))
    except (TypeError, ValueError):
        return default


def _positive_decimal(value: Any, *, label: str) -> tuple[str, int]:
    text = str(value or "").strip()
    if not text.isdecimal() or int(text) <= 0:
        raise WindowDirectionError(f"history row has invalid {label}")
    return text, int(text)


def _normalized_history_page(
    payload: Any,
    *,
    expected_group_id: str,
) -> list[dict[str, Any]]:
    page: list[dict[str, Any]] = []
    seen_sequences: set[str] = set()
    for source in history_events(payload):
        event = dict(source)
        returned_group = str(event.get("group_id") or expected_group_id)
        if returned_group != expected_group_id:
            raise WindowDirectionError("history response crossed group boundary")
        sent_at_text, sent_at = _positive_decimal(event.get("time"), label="time")
        del sent_at_text
        real_seq, real_seq_number = _positive_decimal(
            event.get("real_seq"), label="real_seq"
        )
        message_id, _message_id_number = _positive_decimal(
            event.get("message_id"), label="message_id"
        )
        if real_seq in seen_sequences:
            continue
        seen_sequences.add(real_seq)
        # NapCat labels messages sent by the logged-in account as
        # `message_sent`; normalize it so the shared parser does not drop them.
        event["post_type"] = "message"
        event["message_type"] = "group"
        event["group_id"] = expected_group_id
        page.append(
            {
                "event": event,
                "sent_at": sent_at,
                "real_seq": real_seq,
                "real_seq_number": real_seq_number,
                "message_id": message_id,
            }
        )
    if not page:
        raise WindowDirectionError("history response contains no usable messages")
    return page


def _page_fingerprint(page: list[dict[str, Any]]) -> str:
    material = "\n".join(
        f"{row['sent_at']}:{row['real_seq']}:{row['message_id']}"
        for row in sorted(page, key=lambda value: value["real_seq_number"])
    )
    return hashlib.sha256(material.encode("ascii")).hexdigest()


class WindowRecoveryRunner:
    def __init__(
        self,
        config_path: Path,
        *,
        not_before: int,
        not_after: int,
        expected_groups: int = 6,
        page_size: int = 20,
        interval_seconds: int = 600,
        hourly_limit: int = 6,
        daily_limit: int = 20,
        max_calls_per_group: int = 200,
        queue_threshold: int = 0,
        poll_seconds: int = 30,
        onebot: OneBotClient | None = None,
    ) -> None:
        if not_before <= 0 or not_after <= not_before:
            raise WindowPolicyError("invalid recovery window")
        if expected_groups <= 0 or page_size <= 1:
            raise WindowPolicyError("invalid recovery dimensions")
        if interval_seconds < 60 or hourly_limit <= 0 or daily_limit <= 0:
            raise WindowPolicyError("unsafe recovery rate")
        if max_calls_per_group <= 0 or queue_threshold < 0:
            raise WindowPolicyError("invalid recovery limits")

        self.config_path = Path(config_path)
        self.settings = load_settings(self.config_path)
        self.connection = connect_database(self.settings["storage"]["database"])
        self.connection.row_factory = sqlite3.Row
        self.not_before = int(not_before)
        self.not_after = int(not_after)
        self.expected_groups = int(expected_groups)
        self.page_size = int(page_size)
        self.interval_seconds = int(interval_seconds)
        self.hourly_limit = int(hourly_limit)
        self.daily_limit = int(daily_limit)
        self.max_calls_per_group = int(max_calls_per_group)
        self.queue_threshold = int(queue_threshold)
        self.poll_seconds = max(1, int(poll_seconds))
        self.onebot = onebot or OneBotClient.from_settings(self.settings["onebot"])
        self.stop_event = threading.Event()
        self.report_path = (
            Path(self.settings["storage"]["root"])
            / "state"
            / "diagnostics"
            / "window-recovery-report.json"
        )
        self.lock_path = (
            Path(self.settings["storage"]["root"])
            / "state"
            / "window-recovery.lock"
        )

    def close(self) -> None:
        self.connection.close()

    def request_stop(self, *_args: Any) -> None:
        self.stop_event.set()

    def _enabled_groups(self) -> list[str]:
        return [
            str(row[0])
            for row in self.connection.execute(
                "SELECT group_id FROM monitored_groups WHERE enabled=1 ORDER BY group_id"
            ).fetchall()
        ]

    def _safety_check(self) -> list[str]:
        groups = self._enabled_groups()
        if len(groups) != self.expected_groups:
            raise WindowPolicyError("enabled production group count changed")
        frozen_groups = [
            str(row[0])
            for row in self.connection.execute(
                """
                SELECT group_id FROM window_recovery_jobs
                WHERE not_before=? AND not_after=? ORDER BY group_id
                """,
                (self.not_before, self.not_after),
            ).fetchall()
        ]
        if frozen_groups and frozen_groups != groups:
            raise WindowPolicyError("enabled group set differs from the frozen window jobs")
        if int(_setting(self.connection, "production_history_floor", 0)) != self.not_before:
            raise WindowPolicyError("recovery lower bound differs from production marker")
        if (
            int(_setting(self.connection, "production_live_only_started_at", 0))
            != self.not_after
        ):
            raise WindowPolicyError("recovery upper bound differs from production marker")
        if int(_setting(self.connection, "history_hourly_limit", -1)) != 0:
            raise WindowPolicyError("always-on hourly history budget is not disabled")
        if int(_setting(self.connection, "history_daily_limit", -1)) != 0:
            raise WindowPolicyError("always-on daily history budget is not disabled")
        if _setting(self.connection, "allow_403_history_refresh", False) is not False:
            raise WindowPolicyError("expired-URL history refresh is enabled")
        active_gap_jobs = int(
            self.connection.execute(
                """
                SELECT count(*) FROM jobs
                WHERE kind='gap_recovery' AND status IN ('queued','running')
                """
            ).fetchone()[0]
        )
        if active_gap_jobs:
            raise WindowPolicyError("an ordinary gap-recovery job is active")
        return groups

    def initialize(self) -> None:
        groups = self._safety_check()
        now = int(time.time())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            # 1.1.4 briefly created legacy-forward jobs before production
            # proved that QCE-era msgIds are not valid anchors in the current
            # QQ session.  Replace only those jobs; retain their call records
            # so rate limits and telemetry still account for every request.
            self.connection.execute(
                """
                DELETE FROM window_recovery_jobs
                WHERE not_before=? AND not_after=?
                  AND anchor_mode NOT IN (?, ?)
                """,
                (self.not_before, self.not_after, CURRENT_UPPER, LATEST_BOOTSTRAP),
            )
            for group_id in groups:
                anchor = self.connection.execute(
                    """
                    SELECT message_id, message_seq, sent_at
                    FROM images
                    WHERE group_id=? AND resolver='event-cdn' AND is_online=1
                      AND sent_at>=?
                      AND length(message_id)>=15
                      AND message_id<>''
                      AND message_id NOT GLOB '*[^0-9]*'
                      AND message_seq<>''
                      AND message_seq NOT GLOB '*[^0-9]*'
                    ORDER BY sent_at ASC, CAST(message_seq AS INTEGER) ASC
                    LIMIT 1
                    """,
                    (group_id, self.not_after),
                ).fetchone()
                if not anchor:
                    anchor = self.connection.execute(
                        """
                        SELECT last_message_id, last_message_seq, last_message_time
                        FROM group_runtime
                        WHERE group_id=? AND last_message_time>=?
                          AND length(last_message_id)>=15
                          AND last_message_id<>''
                          AND last_message_id NOT GLOB '*[^0-9]*'
                          AND last_message_seq<>''
                          AND last_message_seq NOT GLOB '*[^0-9]*'
                        """,
                        (group_id, self.not_after),
                    ).fetchone()
                if anchor:
                    anchor_id, anchor_id_number = _positive_decimal(
                        anchor[0], label="start msgId"
                    )
                    anchor_seq, _anchor_seq_number = _positive_decimal(
                        anchor[1], label="start msgSeq"
                    )
                    anchor_time = int(anchor[2] or 0)
                    if len(anchor_id) < 15 or anchor_id_number <= 0:
                        raise WindowPolicyError("upper anchor is not a raw NT msgId")
                    if anchor_time < self.not_after:
                        raise WindowPolicyError("upper anchor time precedes policy bound")
                    anchor_mode = CURRENT_UPPER
                else:
                    # Omitting message_seq asks NapCat for the latest page.  It
                    # is a positioning request only: the hard time filter below
                    # still prevents post-window images from entering the queue.
                    anchor_id = "0"
                    anchor_seq = "0"
                    anchor_time = 0
                    anchor_mode = LATEST_BOOTSTRAP
                prior_calls = int(
                    self.connection.execute(
                        """
                        SELECT count(*) FROM window_recovery_calls
                        WHERE group_id=? AND not_before=? AND not_after=?
                        """,
                        (group_id, self.not_before, self.not_after),
                    ).fetchone()[0]
                )
                self.connection.execute(
                    """
                    INSERT INTO window_recovery_jobs (
                        group_id, not_before, not_after, anchor_mode,
                        start_anchor_id, start_anchor_seq, start_anchor_time,
                        next_anchor_id, next_anchor_seq, next_anchor_time,
                        status, probe_ok, history_calls, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'probe', 0, ?, ?, ?)
                    ON CONFLICT(group_id, not_before, not_after) DO NOTHING
                    """,
                    (
                        group_id,
                        self.not_before,
                        self.not_after,
                        anchor_mode,
                        anchor_id,
                        anchor_seq,
                        anchor_time,
                        anchor_id,
                        anchor_seq,
                        anchor_time,
                        prior_calls,
                        now,
                        now,
                    ),
                )
            job_groups = int(
                self.connection.execute(
                    """
                    SELECT count(*) FROM window_recovery_jobs
                    WHERE not_before=? AND not_after=?
                    """,
                    (self.not_before, self.not_after),
                ).fetchone()[0]
            )
            if job_groups != self.expected_groups:
                raise WindowPolicyError("window job set does not match enabled groups")
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        self._publish_state("ready")

    def _budget_wait_seconds(self, now: int) -> int:
        rows = self.connection.execute(
            """
            SELECT called_at FROM window_recovery_calls
            WHERE not_before=? AND not_after=? AND called_at>?
            ORDER BY called_at
            """,
            (self.not_before, self.not_after, now - 86400),
        ).fetchall()
        calls = [int(row[0]) for row in rows]
        waits = [0]
        if calls:
            waits.append(max(0, calls[-1] + self.interval_seconds - now))
        hourly = [value for value in calls if value > now - 3600]
        if len(hourly) >= self.hourly_limit:
            waits.append(max(1, hourly[-self.hourly_limit] + 3601 - now))
        start_of_day = local_day_start(now)
        daily = [value for value in calls if value >= start_of_day]
        if len(daily) >= self.daily_limit:
            waits.append(max(1, local_day_start(now + 86400) - now))
        return max(waits)

    def _record_call(self, group_id: str) -> tuple[int, int]:
        now = int(time.time())
        cursor = self.connection.execute(
            """
            INSERT INTO window_recovery_calls (
                group_id, not_before, not_after, called_at, outcome
            ) VALUES (?, ?, ?, ?, 'started')
            """,
            (group_id, self.not_before, self.not_after, now),
        )
        self.connection.commit()
        increment_counter(self.connection, "history_calls")
        increment_counter(self.connection, "window_history_calls")
        return int(cursor.lastrowid), now

    def _finish_call(self, call_id: int, outcome: str, error: str | None = None) -> None:
        self.connection.execute(
            """
            UPDATE window_recovery_calls SET outcome=?, error=? WHERE id=?
            """,
            (outcome, (error or "")[:512] or None, int(call_id)),
        )
        self.connection.commit()

    def _next_job(self, now: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT * FROM window_recovery_jobs
            WHERE not_before=? AND not_after=?
              AND status IN ('probe','queued','deferred')
              AND next_retry_at<=?
            ORDER BY history_calls, pages, updated_at, id
            LIMIT 1
            """,
            (self.not_before, self.not_after, now),
        ).fetchone()

    def _job_call_count(self, group_id: str) -> int:
        return int(
            self.connection.execute(
                """
                SELECT count(*) FROM window_recovery_calls
                WHERE group_id=? AND not_before=? AND not_after=?
                """,
                (str(group_id), self.not_before, self.not_after),
            ).fetchone()[0]
        )

    def _ensure_onebot_ready(self) -> None:
        """Fail without consuming a history budget until QQ is actually logged in."""

        try:
            login = self.onebot.call("get_login_info", {})
        except Exception as exc:
            raise WindowOneBotNotReady(type(exc).__name__) from exc
        if not isinstance(login, dict):
            raise WindowOneBotNotReady("get_login_info returned a non-object payload")
        user_id = str(login.get("user_id") or "").strip()
        if not user_id.isdecimal() or int(user_id) <= 0:
            raise WindowOneBotNotReady("get_login_info did not return a logged-in account")

    def _call_page(self, job: sqlite3.Row) -> tuple[int, Any]:
        # This readiness action is deliberately outside `_record_call`: a
        # container/server restart may bring this process up before NapCat has
        # restored the QQ session.  Such waits must not consume a history
        # budget or mutate the immutable-anchor replay state.
        self._ensure_onebot_ready()
        call_id, _called_at = self._record_call(str(job["group_id"]))
        params: dict[str, Any] = {
            "group_id": str(job["group_id"]),
            "count": self.page_size,
            # true walks from a current-session upper anchor towards older
            # messages.  Every anchored page includes its anchor.
            "reverse_order": True,
            "disable_get_url": False,
            "parse_mult_msg": False,
        }
        if str(job["next_anchor_id"]) != "0":
            # NapCat interprets this field as a short message ID when mapped,
            # otherwise as a raw NT msgId.  It is not msgSeq.
            params["message_seq"] = str(job["next_anchor_id"])
        try:
            payload = self.onebot.call("get_group_msg_history", params)
        except Exception as exc:
            self._finish_call(call_id, "error", type(exc).__name__)
            raise
        self._finish_call(call_id, "ok")
        return call_id, payload

    def _fail_job(self, job: sqlite3.Row, status: str, reason: str) -> None:
        now = int(time.time())
        self.connection.execute(
            """
            UPDATE window_recovery_jobs
            SET status=?, last_error=?, updated_at=?, finished_at=?
            WHERE id=?
            """,
            (status, reason[:512], now, now, int(job["id"])),
        )
        self.connection.commit()

    def _handle_call_error(self, job: sqlite3.Row, exc: Exception) -> None:
        now = int(time.time())
        retries = int(job["retry_count"] or 0) + 1
        short_anchor = str(job["next_anchor_id"]) != str(job["start_anchor_id"])
        if short_anchor and int(job["replay_count"] or 0) < 3:
            # A NapCat restart invalidates the in-memory short-ID map.  Reset
            # to the immutable raw NT msgId and safely replay using real_seq
            # deduplication instead of substituting msgSeq as an API anchor.
            self.connection.execute(
                """
                UPDATE window_recovery_jobs
                SET status='probe', probe_ok=0,
                    next_anchor_id=start_anchor_id,
                    next_anchor_seq=start_anchor_seq,
                    next_anchor_time=start_anchor_time,
                    last_page_fingerprint=NULL,
                    retry_count=?, replay_count=replay_count+1,
                    history_calls=history_calls+1,
                    next_retry_at=?, last_error=?, updated_at=?
                WHERE id=?
                """,
                (
                    retries,
                    now + 1800,
                    f"short anchor reset after {type(exc).__name__}",
                    now,
                    int(job["id"]),
                ),
            )
        elif short_anchor:
            self.connection.execute(
                """
                UPDATE window_recovery_jobs
                SET status='failed_anchor', retry_count=?,
                    history_calls=history_calls+1,
                    last_error=?, updated_at=?, finished_at=?
                WHERE id=?
                """,
                (
                    retries,
                    "short anchor failed after three safe replays",
                    now,
                    now,
                    int(job["id"]),
                ),
            )
        elif str(job["anchor_mode"]) == CURRENT_UPPER:
            # A current raw msgId may still be unavailable to the history API
            # (for example after a QQ account/session change).  Falling back to
            # one latest-page bootstrap is bounded and safer than retrying a
            # known-invalid raw anchor.
            self.connection.execute(
                """
                UPDATE window_recovery_jobs
                SET anchor_mode=?, status='probe', probe_ok=0,
                    start_anchor_id='0', start_anchor_seq='0', start_anchor_time=0,
                    next_anchor_id='0', next_anchor_seq='0', next_anchor_time=0,
                    last_page_fingerprint=NULL,
                    retry_count=?, replay_count=0,
                    history_calls=history_calls+1,
                    next_retry_at=?, last_error=?, updated_at=?
                WHERE id=?
                """,
                (
                    LATEST_BOOTSTRAP,
                    retries,
                    now + self.interval_seconds,
                    f"upper anchor switched to latest bootstrap after {type(exc).__name__}",
                    now,
                    int(job["id"]),
                ),
            )
        elif retries < 3:
            self.connection.execute(
                """
                UPDATE window_recovery_jobs
                SET status='deferred', retry_count=?, history_calls=history_calls+1,
                    next_retry_at=?, last_error=?, updated_at=?
                WHERE id=?
                """,
                (
                    retries,
                    now + 1800,
                    f"latest bootstrap retry after {type(exc).__name__}",
                    now,
                    int(job["id"]),
                ),
            )
        else:
            self.connection.execute(
                """
                UPDATE window_recovery_jobs
                SET status='failed_anchor', retry_count=?,
                    history_calls=history_calls+1,
                    last_error=?, updated_at=?, finished_at=?
                WHERE id=?
                """,
                (
                    retries,
                    f"latest bootstrap failed: {type(exc).__name__}",
                    now,
                    now,
                    int(job["id"]),
                ),
            )
        self.connection.commit()

    def process_one_page(self) -> bool:
        self._safety_check()
        now = int(time.time())
        if queue_snapshot(self.connection)["depth"] > self.queue_threshold:
            self._publish_state("waiting_queue")
            return False
        wait = self._budget_wait_seconds(now)
        if wait > 0:
            self._publish_state("waiting_budget", next_call_at=now + wait)
            return False
        job = self._next_job(now)
        if not job:
            self._publish_state("finished")
            return False
        if self._job_call_count(str(job["group_id"])) >= self.max_calls_per_group:
            self._fail_job(job, "partial_max_calls", "per-group call limit reached")
            self._publish_state("running")
            return True

        try:
            _call_id, payload = self._call_page(job)
        except WindowOneBotNotReady:
            self._publish_state(
                "waiting_login", next_call_at=int(time.time()) + self.poll_seconds
            )
            return False
        except OneBotError as exc:
            self._handle_call_error(job, exc)
            self._publish_state("deferred")
            return True
        except Exception as exc:
            self._handle_call_error(job, exc)
            self._publish_state("deferred")
            return True

        try:
            page = _normalized_history_page(
                payload, expected_group_id=str(job["group_id"])
            )
            sequences = [int(row["real_seq_number"]) for row in page]
            current_sequence = int(str(job["next_anchor_seq"]))
            if current_sequence > 0 and max(sequences) > current_sequence:
                raise WindowDirectionError("history page moved after its backward anchor")
            if current_sequence > 0 and current_sequence not in sequences:
                raise WindowDirectionError("history page omitted its inclusive anchor")
            if current_sequence > 0 and min(sequences) == current_sequence:
                now = int(time.time())
                self.connection.execute(
                    """
                    UPDATE window_recovery_jobs
                    SET status='partial_source_exhausted',
                        probe_ok=1, history_calls=history_calls+1,
                        last_error='history source returned only its inclusive anchor',
                        updated_at=?, finished_at=?
                    WHERE id=?
                    """,
                    (now, now, int(job["id"])),
                )
                self.connection.commit()
                self._publish_state("running")
                return True
            ordered = sorted(page, key=lambda value: value["real_seq_number"])
            if any(
                int(later["sent_at"]) < int(earlier["sent_at"])
                for earlier, later in zip(ordered, ordered[1:])
            ):
                raise WindowDirectionError("history time moved backwards while sequence advanced")
            fingerprint = _page_fingerprint(page)
            if job["last_page_fingerprint"] and fingerprint == str(
                job["last_page_fingerprint"]
            ):
                raise WindowDirectionError("history page repeated without progress")
        except WindowDirectionError as exc:
            self.connection.execute(
                "UPDATE window_recovery_jobs SET history_calls=history_calls+1 WHERE id=?",
                (int(job["id"]),),
            )
            self.connection.commit()
            short_anchor = str(job["next_anchor_id"]) != str(job["start_anchor_id"])
            if short_anchor and int(job["replay_count"] or 0) < 3:
                now = int(time.time())
                self.connection.execute(
                    """
                    UPDATE window_recovery_jobs
                    SET status='probe', probe_ok=0,
                        next_anchor_id=start_anchor_id,
                        next_anchor_seq=start_anchor_seq,
                        next_anchor_time=start_anchor_time,
                        last_page_fingerprint=NULL,
                        replay_count=replay_count+1, retry_count=0,
                        next_retry_at=?, last_error=?, updated_at=?
                    WHERE id=?
                    """,
                    (
                        now + 1800,
                        f"short anchor reset after {type(exc).__name__}",
                        now,
                        int(job["id"]),
                    ),
                )
                self.connection.commit()
            else:
                self._fail_job(job, "failed_direction", str(exc))
            self._publish_state("running")
            return True

        oldest = min(page, key=lambda value: value["real_seq_number"])
        now = int(time.time())
        fingerprint = _page_fingerprint(page)
        messages_in_window = 0
        images_enqueued = 0
        duplicates = 0
        for row in page:
            sent_at = int(row["sent_at"])
            if not (self.not_before <= sent_at <= self.not_after):
                continue
            # First boundary check is above.  Parse only an in-window message;
            # then assert every emitted item carries the same bounded time.
            _cursor, items = parse_group_event(row["event"])
            messages_in_window += 1
            for item in items:
                item_time = int(item.get("sent_at") or 0)
                if not (self.not_before <= item_time <= self.not_after):
                    raise WindowPolicyError("parser emitted an out-of-window image")
                if enqueue_image(self.connection, item):
                    increment_counter(self.connection, "images_seen")
                    increment_counter(self.connection, "image_segments")
                    images_enqueued += 1
                else:
                    duplicates += 1

        page_times = [int(row["sent_at"]) for row in page]
        # Direction and non-decreasing message time were verified before any
        # row was enqueued.  Once a backwards page crosses the fixed lower
        # bound, every subsequent page is older than the outage window.
        completed = min(page_times) < self.not_before
        status = "completed" if completed else "queued"
        self.connection.execute(
            """
            UPDATE window_recovery_jobs
            SET status=?, probe_ok=1, pages=pages+1, history_calls=history_calls+1,
                messages_seen=messages_seen+?,
                messages_in_window=messages_in_window+?,
                images_enqueued=images_enqueued+?, duplicates=duplicates+?,
                next_anchor_id=?, next_anchor_seq=?, next_anchor_time=?,
                next_retry_at=?, last_page_fingerprint=?, retry_count=0,
                last_error=NULL, updated_at=?, finished_at=?
            WHERE id=?
            """,
            (
                status,
                len(page),
                messages_in_window,
                images_enqueued,
                duplicates,
                str(oldest["message_id"]),
                str(oldest["real_seq"]),
                int(oldest["sent_at"]),
                now + self.interval_seconds,
                fingerprint,
                now,
                now if completed else None,
                int(job["id"]),
            ),
        )
        self.connection.commit()
        self._publish_state("running")
        return True

    def _aggregate(self, phase: str, **extra: Any) -> dict[str, Any]:
        rows = self.connection.execute(
            """
            SELECT status, count(*) AS count,
                   coalesce(sum(pages),0) AS pages,
                   coalesce(sum(history_calls),0) AS history_calls,
                   coalesce(sum(messages_seen),0) AS messages_seen,
                   coalesce(sum(messages_in_window),0) AS messages_in_window,
                   coalesce(sum(images_enqueued),0) AS images_enqueued,
                   coalesce(sum(duplicates),0) AS duplicates
            FROM window_recovery_jobs
            WHERE not_before=? AND not_after=?
            GROUP BY status
            """,
            (self.not_before, self.not_after),
        ).fetchall()
        statuses = {str(row["status"]): int(row["count"]) for row in rows}
        totals = {
            key: sum(int(row[key] or 0) for row in rows)
            for key in (
                "pages",
                "history_calls",
                "messages_seen",
                "messages_in_window",
                "images_enqueued",
                "duplicates",
            )
        }
        terminal = sum(statuses.get(value, 0) for value in TERMINAL_STATUSES)
        result = {
            "active": terminal < self.expected_groups,
            "phase": phase,
            "not_before": self.not_before,
            "not_after": self.not_after,
            "groups_total": self.expected_groups,
            "groups_terminal": terminal,
            "statuses": statuses,
            **totals,
            "queue_depth": int(queue_snapshot(self.connection)["depth"]),
            "updated_at": int(time.time()),
        }
        result.update(extra)
        return result

    def _publish_state(self, phase: str, **extra: Any) -> dict[str, Any]:
        state = self._aggregate(phase, **extra)
        set_runtime_state(self.connection, "window_recovery", state)
        self.report_path.parent.mkdir(parents=True, exist_ok=True)
        self.report_path.write_text(
            json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        os.chmod(self.report_path, 0o600)
        print(json.dumps(state, ensure_ascii=False), flush=True)
        return state

    def run(self) -> None:
        self.initialize()
        while not self.stop_event.is_set():
            try:
                progressed = self.process_one_page()
            except WindowPolicyError as exc:
                self._publish_state("failed_policy", error=type(exc).__name__)
                return
            if progressed:
                continue
            state = self._aggregate("idle")
            delay = 3600 if not state["active"] else self.poll_seconds
            self.stop_event.wait(delay)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="bounded QQ history window recovery")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--not-before", type=int, required=True)
    parser.add_argument("--not-after", type=int, required=True)
    parser.add_argument("--expected-groups", type=int, default=6)
    parser.add_argument("--page-size", type=int, default=20)
    parser.add_argument("--interval-seconds", type=int, default=600)
    parser.add_argument("--hourly-limit", type=int, default=6)
    parser.add_argument("--daily-limit", type=int, default=20)
    parser.add_argument("--max-calls-per-group", type=int, default=200)
    parser.add_argument("--queue-threshold", type=int, default=0)
    parser.add_argument("--poll-seconds", type=int, default=30)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    runner = WindowRecoveryRunner(
        args.config,
        not_before=args.not_before,
        not_after=args.not_after,
        expected_groups=args.expected_groups,
        page_size=args.page_size,
        interval_seconds=args.interval_seconds,
        hourly_limit=args.hourly_limit,
        daily_limit=args.daily_limit,
        max_calls_per_group=args.max_calls_per_group,
        queue_threshold=args.queue_threshold,
        poll_seconds=args.poll_seconds,
    )
    signal.signal(signal.SIGTERM, runner.request_stop)
    signal.signal(signal.SIGINT, runner.request_stop)
    try:
        # The Compose service name prevents ordinary duplication; this shared
        # filesystem lock also fails closed if an operator starts a second
        # container or manual process against the same repository.
        import fcntl

        runner.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with runner.lock_path.open("a+", encoding="ascii") as lock_file:
            os.chmod(runner.lock_path, 0o600)
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise WindowPolicyError("another window recovery runner is active") from exc
            runner.run()
    finally:
        runner.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
