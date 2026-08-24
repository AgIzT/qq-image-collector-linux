from __future__ import annotations

import io
import json
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from qq_image_collector.database import increment_counter
from qq_image_collector.onebot import OneBotError
from qq_image_collector.window_recovery import (
    WindowPolicyError,
    WindowRecoveryRunner,
    main as window_recovery_main,
)


GROUP = "100000001"
SENDER = "200000002"
NOT_BEFORE = 1_785_412_800
NOT_AFTER = NOT_BEFORE + 3 * 86400
RAW_ANCHOR = "9000000000000000100"
ANCHOR_SEQUENCE = 104


class FakeOneBot:
    def __init__(
        self,
        *responses: object,
        login_responses: list[object] | None = None,
    ) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.login_responses = list(login_responses or [])
        self.login_calls: list[dict[str, object]] = []

    def call(self, action: str, params: dict[str, object]) -> object:
        if action == "get_login_info":
            self.login_calls.append(dict(params))
            response = (
                self.login_responses.pop(0)
                if self.login_responses
                else {"user_id": SENDER, "nickname": "synthetic"}
            )
            if isinstance(response, Exception):
                raise response
            return response
        self.calls.append((action, dict(params)))
        if not self.responses:
            raise AssertionError("unexpected OneBot call")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def history_message(
    sequence: int,
    sent_at: int,
    *,
    message_id: int | None = None,
    image: bool = False,
    post_type: str = "message",
) -> dict[str, object]:
    message: list[dict[str, object]] = []
    if image:
        message.append(
            {
                "type": "image",
                "data": {
                    "file": f"synthetic-{sequence}.png",
                    "url": f"https://gchat.qpic.cn/synthetic/{sequence}?rkey=test",
                    "file_size": "204800",
                    "summary": "[图片]",
                    "sub_type": 0,
                },
            }
        )
    return {
        "post_type": post_type,
        "message_type": "group",
        "group_id": int(GROUP),
        "user_id": int(SENDER),
        "message_id": message_id if message_id is not None else 800000 + sequence,
        "real_seq": sequence,
        "time": sent_at,
        "sender": {"user_id": int(SENDER), "nickname": "synthetic"},
        "message": message,
    }


def page(*messages: dict[str, object]) -> dict[str, object]:
    return {"messages": list(messages)}


class WindowRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.config_path = self.root / "config.json"
        self.database_path = self.root / "state" / "collector.sqlite3"
        self.config_path.write_text(
            json.dumps(
                {
                    "onebot": {},
                    "groups": [],
                    "storage": {
                        "root": str(self.root / "storage"),
                        "database": str(self.database_path),
                    },
                }
            ),
            encoding="utf-8",
        )
        self.runners: list[WindowRecoveryRunner] = []

    def tearDown(self) -> None:
        for runner in self.runners:
            runner.close()
        self.temporary.cleanup()

    def make_runner(
        self,
        onebot: FakeOneBot,
        **overrides: object,
    ) -> WindowRecoveryRunner:
        options: dict[str, object] = {
            "not_before": NOT_BEFORE,
            "not_after": NOT_AFTER,
            "expected_groups": 1,
            "page_size": 20,
            "interval_seconds": 600,
            "hourly_limit": 6,
            "daily_limit": 20,
            "max_calls_per_group": 200,
            "queue_threshold": 0,
            "onebot": onebot,
        }
        options.update(overrides)
        runner = WindowRecoveryRunner(self.config_path, **options)
        runner._publish_state = lambda _phase, **_extra: {}  # type: ignore[method-assign]
        self.runners.append(runner)
        self.seed_policy(runner.connection)
        self.insert_anchor(runner.connection)
        return runner

    def seed_policy(self, connection: sqlite3.Connection) -> None:
        now = NOT_AFTER + 1
        connection.execute(
            "INSERT INTO monitored_groups(group_id, enabled, created_at, updated_at) "
            "VALUES (?, 1, ?, ?)",
            (GROUP, now, now),
        )
        settings = {
            "production_history_floor": NOT_BEFORE,
            "production_live_only_started_at": NOT_AFTER,
            "history_hourly_limit": 0,
            "history_daily_limit": 0,
            "allow_403_history_refresh": False,
        }
        connection.executemany(
            "INSERT INTO app_settings(key, value_json, updated_at) VALUES (?, ?, ?)",
            [(key, json.dumps(value), now) for key, value in settings.items()],
        )
        connection.commit()

    def test_cli_refuses_an_uncapped_budget_without_an_explicit_override(self) -> None:
        base = [
            "window_recovery",
            "--config",
            str(self.config_path),
            "--not-before",
            str(NOT_BEFORE),
            "--not-after",
            str(NOT_AFTER),
            "--hourly-limit",
            "0",
        ]
        with mock.patch.object(sys, "argv", base):
            with mock.patch("sys.stderr", io.StringIO()) as captured:
                self.assertEqual(window_recovery_main(), 2)
        self.assertIn("--hourly-limit", captured.getvalue())

    def test_worker_history_spend_holds_recovery_at_the_shared_ceiling(self) -> None:
        runner = self.make_runner(FakeOneBot(), daily_limit=5, interval_seconds=0)
        now = int(time.time())
        self.assertEqual(runner._budget_wait_seconds(now), 0)
        # Spent by the worker, not by this runner: one account, one ceiling.
        for _ in range(5):
            increment_counter(runner.connection, "history_calls")
        self.assertGreater(runner._budget_wait_seconds(int(time.time())), 0)

    def test_traversal_that_never_reaches_the_window_is_abandoned(self) -> None:
        runner = self.make_runner(FakeOneBot(), no_yield_pages=50)
        runner.initialize()
        job_id = int(
            runner.connection.execute(
                "SELECT id FROM window_recovery_jobs LIMIT 1"
            ).fetchone()[0]
        )
        runner.connection.execute(
            "UPDATE window_recovery_jobs SET pages=49, messages_in_window=0, "
            "images_enqueued=0 WHERE id=?",
            (job_id,),
        )
        runner.connection.commit()
        runner._stop_if_no_yield(job_id)
        self.assertNotEqual(self._job_status(runner, job_id), "partial_no_yield")

        runner.connection.execute(
            "UPDATE window_recovery_jobs SET pages=50 WHERE id=?", (job_id,)
        )
        runner.connection.commit()
        runner._stop_if_no_yield(job_id)
        self.assertEqual(self._job_status(runner, job_id), "partial_no_yield")

    def test_a_traversal_inside_the_window_is_never_abandoned(self) -> None:
        runner = self.make_runner(FakeOneBot(), no_yield_pages=10)
        runner.initialize()
        job_id = int(
            runner.connection.execute(
                "SELECT id FROM window_recovery_jobs LIMIT 1"
            ).fetchone()[0]
        )
        # Pages well past the ceiling, but the anchor did descend into the
        # window, so this is a slow recovery rather than a runaway one.
        runner.connection.execute(
            "UPDATE window_recovery_jobs SET pages=500, messages_in_window=3, "
            "images_enqueued=0 WHERE id=?",
            (job_id,),
        )
        runner.connection.commit()
        runner._stop_if_no_yield(job_id)
        self.assertNotEqual(self._job_status(runner, job_id), "partial_no_yield")

    @staticmethod
    def _job_status(runner: WindowRecoveryRunner, job_id: int) -> str:
        return str(
            runner.connection.execute(
                "SELECT status FROM window_recovery_jobs WHERE id=?", (job_id,)
            ).fetchone()[0]
        )

    def test_running_state_publication_is_throttled_without_delaying_pages(self) -> None:
        runner = WindowRecoveryRunner(
            self.config_path,
            not_before=NOT_BEFORE,
            not_after=NOT_AFTER,
            expected_groups=1,
            interval_seconds=0,
            onebot=FakeOneBot(),
        )
        self.runners.append(runner)
        aggregate = mock.Mock(return_value={"phase": "running", "updated_at": 1})
        runner._aggregate = aggregate  # type: ignore[method-assign]

        first = runner._publish_state("running")
        second = runner._publish_state("running")
        runner._publish_state("deferred")

        self.assertEqual(first, second)
        self.assertEqual(aggregate.call_count, 2)
        self.assertEqual(runner.interval_seconds, 0)

    def insert_anchor(
        self,
        connection: sqlite3.Connection,
        *,
        message_id: str = RAW_ANCHOR,
        sequence: str = str(ANCHOR_SEQUENCE),
        sent_at: int = NOT_AFTER + 1,
        resolver: str = "event-cdn",
        is_online: int = 1,
    ) -> None:
        connection.execute(
            """
            INSERT INTO images (
                group_id, message_id, message_seq, image_index, sent_at,
                status, updated_at, resolver, is_online
            ) VALUES (?, ?, ?, 0, ?, 'accepted', ?, ?, ?)
            """,
            (GROUP, message_id, sequence, sent_at, sent_at, resolver, is_online),
        )
        connection.commit()

    @staticmethod
    def mark_probe_complete(runner: WindowRecoveryRunner) -> None:
        runner.connection.execute(
            """
            UPDATE window_recovery_jobs
            SET status='queued', probe_ok=1, next_retry_at=0
            """
        )
        runner.connection.commit()
        runner._budget_wait_seconds = lambda _now: 0  # type: ignore[method-assign]

    def test_initialize_uses_earliest_current_session_upper_anchor(self) -> None:
        runner = self.make_runner(FakeOneBot())
        runner.connection.execute(
            "UPDATE images SET sent_at=? WHERE group_id=? AND message_id=?",
            (NOT_AFTER + 30, GROUP, RAW_ANCHOR),
        )
        runner.connection.commit()
        self.insert_anchor(
            runner.connection,
            message_id="9000000000000000101",
            sequence="101",
            sent_at=NOT_AFTER + 10,
        )
        # A process-local short ID and legacy/offline rows cannot become a
        # current-session upper anchor.
        self.insert_anchor(
            runner.connection,
            message_id="12345",
            sequence="102",
            sent_at=NOT_AFTER + 2,
        )
        self.insert_anchor(
            runner.connection,
            message_id="9000000000000000999",
            sequence="999",
            sent_at=NOT_AFTER + 1,
            resolver="qce",
            is_online=0,
        )

        runner.initialize()

        job = runner.connection.execute(
            "SELECT * FROM window_recovery_jobs WHERE group_id=?", (GROUP,)
        ).fetchone()
        self.assertEqual(job["start_anchor_id"], "9000000000000000101")
        self.assertEqual(job["start_anchor_seq"], "101")
        self.assertEqual(job["anchor_mode"], "current-upper")
        self.assertEqual(job["status"], "probe")
        self.assertEqual(job["probe_ok"], 0)

    def test_quiet_group_bootstraps_from_latest_page_without_an_anchor(self) -> None:
        fake = FakeOneBot(
            page(
                history_message(103, NOT_AFTER - 1, image=True),
                history_message(104, NOT_AFTER + 1),
            )
        )
        runner = self.make_runner(fake)
        runner.connection.execute("DELETE FROM images")
        runner.connection.commit()
        runner.initialize()
        runner._budget_wait_seconds = lambda _now: 0  # type: ignore[method-assign]

        job = runner.connection.execute("SELECT * FROM window_recovery_jobs").fetchone()
        self.assertEqual(job["anchor_mode"], "latest-bootstrap")
        self.assertEqual(job["start_anchor_id"], "0")
        self.assertEqual(job["start_anchor_seq"], "0")

        self.assertTrue(runner.process_one_page())

        self.assertEqual(len(fake.calls), 1)
        action, params = fake.calls[0]
        self.assertEqual(action, "get_group_msg_history")
        self.assertNotIn("message_seq", params)
        self.assertIs(params["reverse_order"], True)
        job = runner.connection.execute("SELECT * FROM window_recovery_jobs").fetchone()
        self.assertEqual(job["next_anchor_seq"], "103")
        self.assertEqual(job["images_enqueued"], 1)

    def test_invalid_current_upper_anchor_switches_once_to_latest_bootstrap(self) -> None:
        fake = FakeOneBot(
            OneBotError("current raw anchor is unavailable"),
            page(
                history_message(103, NOT_AFTER - 1, image=True),
                history_message(104, NOT_AFTER + 1),
            ),
        )
        runner = self.make_runner(fake)
        runner.initialize()
        runner._budget_wait_seconds = lambda _now: 0  # type: ignore[method-assign]

        self.assertTrue(runner.process_one_page())

        job = runner.connection.execute("SELECT * FROM window_recovery_jobs").fetchone()
        self.assertEqual(job["anchor_mode"], "latest-bootstrap")
        self.assertEqual(job["status"], "probe")
        self.assertEqual(job["start_anchor_id"], "0")
        self.assertEqual(job["next_anchor_id"], "0")
        self.assertEqual(job["history_calls"], 1)

        runner.connection.execute(
            "UPDATE window_recovery_jobs SET next_retry_at=0"
        )
        runner.connection.commit()
        self.assertTrue(runner.process_one_page())

        self.assertIn("message_seq", fake.calls[0][1])
        self.assertNotIn("message_seq", fake.calls[1][1])
        job = runner.connection.execute("SELECT * FROM window_recovery_jobs").fetchone()
        self.assertEqual(job["images_enqueued"], 1)

    def test_legacy_forward_jobs_are_replaced_but_calls_remain_accounted(self) -> None:
        runner = self.make_runner(FakeOneBot())
        runner.initialize()
        runner.connection.execute(
            """
            UPDATE window_recovery_jobs
            SET anchor_mode='legacy-forward', start_anchor_id='999',
                start_anchor_seq='999', next_anchor_id='999', next_anchor_seq='999'
            """
        )
        runner.connection.execute(
            """
            INSERT INTO window_recovery_calls(
                group_id, not_before, not_after, called_at, outcome
            ) VALUES (?, ?, ?, ?, 'error')
            """,
            (GROUP, NOT_BEFORE, NOT_AFTER, NOT_AFTER + 5),
        )
        runner.connection.commit()

        runner.initialize()

        job = runner.connection.execute("SELECT * FROM window_recovery_jobs").fetchone()
        self.assertEqual(job["anchor_mode"], "current-upper")
        self.assertEqual(job["start_anchor_id"], RAW_ANCHOR)
        self.assertEqual(job["start_anchor_seq"], str(ANCHOR_SEQUENCE))
        self.assertEqual(job["history_calls"], 1)
        self.assertEqual(
            runner.connection.execute(
                "SELECT count(*) FROM window_recovery_calls"
            ).fetchone()[0],
            1,
        )

    def test_first_response_is_validated_enqueued_and_not_refetched(self) -> None:
        fake = FakeOneBot(
            page(
                history_message(103, NOT_AFTER - 1, image=True),
                history_message(104, NOT_AFTER + 1),
            )
        )
        runner = self.make_runner(fake)
        runner.initialize()
        runner._budget_wait_seconds = lambda _now: 0  # type: ignore[method-assign]

        self.assertTrue(runner.process_one_page())

        self.assertEqual(
            runner.connection.execute(
                "SELECT count(*) FROM images WHERE resolver='event-cdn' AND sent_at BETWEEN ? AND ?",
                (NOT_BEFORE, NOT_AFTER),
            ).fetchone()[0],
            1,
        )
        job = runner.connection.execute("SELECT * FROM window_recovery_jobs").fetchone()
        self.assertEqual(job["status"], "queued")
        self.assertEqual(job["probe_ok"], 1)
        self.assertEqual(job["next_anchor_id"], "800103")
        self.assertEqual(job["next_anchor_seq"], "103")
        self.assertEqual(job["pages"], 1)
        self.assertEqual(job["images_enqueued"], 1)
        self.assertEqual(len(fake.calls), 1)
        action, params = fake.calls[0]
        self.assertEqual(action, "get_group_msg_history")
        self.assertEqual(params["message_seq"], RAW_ANCHOR)
        self.assertIs(params["reverse_order"], True)

    def test_hard_window_is_inclusive_and_message_sent_is_collected(self) -> None:
        fake = FakeOneBot(
            page(
                history_message(100, NOT_BEFORE - 1, image=True),
                history_message(101, NOT_BEFORE, image=True),
                history_message(
                    102, NOT_BEFORE + 100, image=True, post_type="message_sent"
                ),
                history_message(103, NOT_AFTER, image=True),
                history_message(104, NOT_AFTER + 1, image=True),
            )
        )
        runner = self.make_runner(fake, queue_threshold=10)
        runner.initialize()
        self.mark_probe_complete(runner)

        self.assertTrue(runner.process_one_page())

        rows = runner.connection.execute(
            "SELECT message_seq, sent_at FROM images WHERE resolver='event-cdn' AND sent_at BETWEEN ? AND ? ORDER BY sent_at",
            (NOT_BEFORE, NOT_AFTER),
        ).fetchall()
        self.assertEqual(
            [(row["message_seq"], row["sent_at"]) for row in rows],
            [("101", NOT_BEFORE), ("102", NOT_BEFORE + 100), ("103", NOT_AFTER)],
        )
        job = runner.connection.execute("SELECT * FROM window_recovery_jobs").fetchone()
        self.assertEqual(job["messages_in_window"], 3)
        self.assertEqual(job["images_enqueued"], 3)
        self.assertEqual(job["status"], "completed")

    def test_drain_pages_persists_and_blocks_next_history_page(self) -> None:
        fake = FakeOneBot(
            page(
                history_message(103, NOT_AFTER - 1, image=True),
                history_message(104, NOT_AFTER + 1),
            ),
            page(
                history_message(100, NOT_BEFORE - 1),
                history_message(103, NOT_AFTER - 1),
            ),
        )
        runner = self.make_runner(
            fake,
            drain_pages=True,
            pending_timeout_seconds=1200,
            queue_threshold=-1,
            interval_seconds=0,
        )
        runner.initialize()
        self.mark_probe_complete(runner)

        self.assertTrue(runner.process_one_page())
        job = runner.connection.execute(
            "SELECT pending_page_json FROM window_recovery_jobs"
        ).fetchone()
        pending = json.loads(job[0])
        self.assertEqual(pending["items"], [["103", 0]])
        resolver = json.loads(
            runner.connection.execute(
                "SELECT resolver_json FROM images WHERE message_seq='103'"
            ).fetchone()[0]
        )
        self.assertEqual(resolver["history_source"], "window-recovery")
        self.assertEqual(resolver["history_message_id"], "800103")

        self.assertFalse(runner.process_one_page())
        self.assertEqual(len(fake.calls), 1)
        runner.connection.execute(
            "UPDATE images SET status='accepted' WHERE message_seq='103'"
        )
        runner.connection.commit()
        self.assertTrue(runner.process_one_page())
        self.assertEqual(len(fake.calls), 1)
        self.assertTrue(runner.process_one_page())
        self.assertEqual(len(fake.calls), 2)
        completed = runner.connection.execute(
            "SELECT status, pending_page_json FROM window_recovery_jobs"
        ).fetchone()
        self.assertEqual(tuple(completed), ("completed", None))

    def test_overlapping_pages_deduplicate_by_real_sequence(self) -> None:
        fake = FakeOneBot(
            page(
                history_message(103, NOT_AFTER - 1, message_id=8103, image=True),
                history_message(104, NOT_AFTER + 1),
            ),
            page(
                history_message(102, NOT_AFTER - 2, message_id=9102, image=True),
                history_message(103, NOT_AFTER - 1, message_id=9103, image=True),
                # Duplicate real_seq in one response is ignored as well.
                history_message(102, NOT_AFTER - 2, message_id=9992, image=True),
            ),
        )
        runner = self.make_runner(fake, queue_threshold=10)
        runner.initialize()
        self.mark_probe_complete(runner)

        self.assertTrue(runner.process_one_page())
        runner.connection.execute(
            "UPDATE window_recovery_jobs SET next_retry_at=0"
        )
        runner.connection.commit()
        self.assertTrue(runner.process_one_page())

        rows = runner.connection.execute(
            "SELECT message_id, message_seq FROM images WHERE resolver='event-cdn' AND sent_at BETWEEN ? AND ? ORDER BY message_seq",
            (NOT_BEFORE, NOT_AFTER),
        ).fetchall()
        self.assertEqual([(row["message_id"], row["message_seq"]) for row in rows], [("9102", "102"), ("8103", "103")])
        job = runner.connection.execute("SELECT * FROM window_recovery_jobs").fetchone()
        self.assertEqual(job["images_enqueued"], 2)
        self.assertEqual(job["duplicates"], 1)
        self.assertEqual(fake.calls[1][1]["message_seq"], "8103")

    def test_wrong_direction_fails_before_any_enqueue(self) -> None:
        fake = FakeOneBot(
            page(
                history_message(103, NOT_AFTER - 1, image=True),
                history_message(104, NOT_AFTER + 1),
                history_message(105, NOT_AFTER + 2, image=True),
            )
        )
        runner = self.make_runner(fake)
        runner.initialize()
        runner._budget_wait_seconds = lambda _now: 0  # type: ignore[method-assign]

        self.assertTrue(runner.process_one_page())

        self.assertEqual(
            runner.connection.execute(
                "SELECT count(*) FROM images WHERE resolver='event-cdn' AND sent_at BETWEEN ? AND ?",
                (NOT_BEFORE, NOT_AFTER),
            ).fetchone()[0],
            0,
        )
        job = runner.connection.execute("SELECT * FROM window_recovery_jobs").fetchone()
        self.assertEqual(job["status"], "failed_direction")
        self.assertEqual(job["images_enqueued"], 0)

    def test_time_must_not_move_backwards_as_sequence_advances(self) -> None:
        fake = FakeOneBot(
            page(
                history_message(102, NOT_AFTER - 10, image=True),
                history_message(103, NOT_AFTER - 20, image=True),
                history_message(104, NOT_AFTER + 1),
            )
        )
        runner = self.make_runner(fake)
        runner.initialize()
        runner._budget_wait_seconds = lambda _now: 0  # type: ignore[method-assign]

        self.assertTrue(runner.process_one_page())

        self.assertEqual(
            runner.connection.execute(
                "SELECT count(*) FROM images WHERE resolver='event-cdn' AND sent_at BETWEEN ? AND ?",
                (NOT_BEFORE, NOT_AFTER),
            ).fetchone()[0],
            0,
        )
        job = runner.connection.execute("SELECT * FROM window_recovery_jobs").fetchone()
        self.assertEqual(job["status"], "failed_direction")
        self.assertIn("time moved backwards", job["last_error"])

    def test_inclusive_anchor_only_marks_source_exhausted_without_replay(self) -> None:
        fake = FakeOneBot(
            page(history_message(104, NOT_AFTER + 1, image=True))
        )
        runner = self.make_runner(fake)
        runner.initialize()
        runner._budget_wait_seconds = lambda _now: 0  # type: ignore[method-assign]

        self.assertTrue(runner.process_one_page())

        job = runner.connection.execute("SELECT * FROM window_recovery_jobs").fetchone()
        self.assertEqual(job["status"], "partial_source_exhausted")
        self.assertEqual(job["probe_ok"], 1)
        self.assertEqual(job["history_calls"], 1)
        self.assertEqual(job["replay_count"], 0)
        self.assertEqual(job["next_anchor_id"], RAW_ANCHOR)
        self.assertIsNotNone(job["finished_at"])
        self.assertEqual(
            runner.connection.execute(
                "SELECT count(*) FROM images WHERE resolver='event-cdn' AND sent_at BETWEEN ? AND ?",
                (NOT_BEFORE, NOT_AFTER),
            ).fetchone()[0],
            0,
        )
        self.assertEqual(len(fake.calls), 1)
        self.assertFalse(runner._aggregate("test")["active"])

    def test_not_logged_in_waits_without_consuming_or_replaying_history(self) -> None:
        fake = FakeOneBot(
            page(
                history_message(103, NOT_AFTER - 1, image=True),
                history_message(104, NOT_AFTER + 1),
            ),
            login_responses=[
                RuntimeError("OneBot is still starting"),
                {"user_id": SENDER, "nickname": "synthetic"},
            ],
        )
        runner = self.make_runner(fake)
        runner.initialize()
        runner._budget_wait_seconds = lambda _now: 0  # type: ignore[method-assign]
        phases: list[str] = []
        runner._publish_state = (  # type: ignore[method-assign]
            lambda phase, **_extra: phases.append(phase) or {}
        )

        self.assertFalse(runner.process_one_page())

        self.assertEqual(phases[-1], "waiting_login")
        self.assertEqual(fake.calls, [])
        self.assertEqual(len(fake.login_calls), 1)
        self.assertEqual(
            runner.connection.execute(
                "SELECT count(*) FROM window_recovery_calls"
            ).fetchone()[0],
            0,
        )
        counters = runner.connection.execute(
            """
            SELECT coalesce(sum(history_calls), 0),
                   coalesce(sum(window_history_calls), 0)
            FROM hourly_counters
            """
        ).fetchone()
        self.assertEqual(tuple(counters), (0, 0))
        job = runner.connection.execute(
            "SELECT * FROM window_recovery_jobs"
        ).fetchone()
        self.assertEqual(job["status"], "probe")
        self.assertEqual(job["history_calls"], 0)
        self.assertEqual(job["retry_count"], 0)
        self.assertEqual(job["replay_count"], 0)
        self.assertEqual(job["next_anchor_id"], RAW_ANCHOR)

        # The next short poll sees a logged-in account and proceeds normally.
        self.assertTrue(runner.process_one_page())
        self.assertEqual(len(fake.login_calls), 2)
        self.assertEqual(len(fake.calls), 1)
        self.assertEqual(fake.calls[0][0], "get_group_msg_history")

    def test_login_probe_without_an_account_is_not_history_ready(self) -> None:
        fake = FakeOneBot(
            page(history_message(104, NOT_AFTER + 1)),
            login_responses=[{"user_id": "0", "nickname": ""}],
        )
        runner = self.make_runner(fake)
        runner.initialize()
        runner._budget_wait_seconds = lambda _now: 0  # type: ignore[method-assign]
        phases: list[str] = []
        runner._publish_state = (  # type: ignore[method-assign]
            lambda phase, **_extra: phases.append(phase) or {}
        )

        self.assertFalse(runner.process_one_page())

        self.assertEqual(phases[-1], "waiting_login")
        self.assertEqual(fake.calls, [])
        self.assertEqual(
            runner.connection.execute(
                "SELECT count(*) FROM window_recovery_calls"
            ).fetchone()[0],
            0,
        )
        job = runner.connection.execute(
            "SELECT history_calls, retry_count, replay_count FROM window_recovery_jobs"
        ).fetchone()
        self.assertEqual(tuple(job), (0, 0, 0))

    def test_nonempty_download_queue_blocks_history_call(self) -> None:
        fake = FakeOneBot(page(history_message(104, NOT_AFTER + 1)))
        runner = self.make_runner(fake)
        runner.initialize()
        runner.connection.execute(
            """
            INSERT INTO images (
                group_id, message_id, message_seq, image_index, sent_at,
                status, updated_at, resolver, resolver_json, discovered_at
            ) VALUES (?, '7001', '7001', 0, ?, 'queued', ?, 'event-cdn', '{}', ?)
            """,
            (GROUP, NOT_AFTER + 1, NOT_AFTER + 1, NOT_AFTER + 1),
        )
        runner.connection.commit()
        phases: list[str] = []
        runner._publish_state = lambda phase, **_extra: phases.append(phase) or {}  # type: ignore[method-assign]

        self.assertFalse(runner.process_one_page())
        self.assertEqual(fake.calls, [])
        self.assertEqual(phases[-1], "waiting_queue")

    def test_zero_limits_ignore_prior_calls_queue_depth_and_group_count(self) -> None:
        fake = FakeOneBot(page(history_message(104, NOT_AFTER + 1)))
        runner = self.make_runner(
            fake,
            interval_seconds=0,
            hourly_limit=0,
            daily_limit=0,
            max_calls_per_group=0,
            queue_threshold=-1,
        )
        runner.initialize()
        runner.connection.executemany(
            """
            INSERT INTO window_recovery_calls(
                group_id, not_before, not_after, called_at, outcome
            ) VALUES (?, ?, ?, ?, 'ok')
            """,
            [
                (GROUP, NOT_BEFORE, NOT_AFTER, NOT_AFTER + index)
                for index in range(500)
            ],
        )
        runner.connection.execute(
            """
            INSERT INTO images(
                group_id, message_id, message_seq, image_index, sent_at,
                status, updated_at, resolver, resolver_json, discovered_at
            ) VALUES (?, 'queued-test', 'queued-test', 0, ?, 'queued', ?,
                      'event-cdn', '{}', ?)
            """,
            (GROUP, NOT_AFTER, NOT_AFTER, NOT_AFTER),
        )
        runner.connection.commit()
        self.assertEqual(runner._budget_wait_seconds(NOT_AFTER + 1000), 0)
        self.assertTrue(runner.process_one_page())
        self.assertEqual(len(fake.calls), 1)

    def test_page_crossing_lower_bound_completes_without_an_extra_call(self) -> None:
        fake = FakeOneBot(
            page(
                history_message(101, NOT_BEFORE - 1, image=True),
                history_message(102, NOT_BEFORE + 2, image=True),
                history_message(104, NOT_AFTER + 1),
            )
        )
        runner = self.make_runner(fake)
        runner.initialize()
        self.mark_probe_complete(runner)
        runner.connection.execute(
            """
            UPDATE window_recovery_jobs
            SET next_anchor_id=?, next_anchor_seq='104',
                next_anchor_time=?, next_retry_at=0
            """,
            (RAW_ANCHOR, NOT_AFTER + 1),
        )
        runner.connection.commit()

        self.assertTrue(runner.process_one_page())

        job = runner.connection.execute("SELECT * FROM window_recovery_jobs").fetchone()
        self.assertEqual(job["status"], "completed")
        self.assertIsNotNone(job["finished_at"])
        self.assertEqual(job["images_enqueued"], 1)
        self.assertEqual(len(fake.calls), 1)
        self.assertFalse(runner._aggregate("test")["active"])

    def test_enabled_group_set_is_frozen_after_initialization(self) -> None:
        fake = FakeOneBot(page(history_message(104, NOT_AFTER + 1)))
        runner = self.make_runner(fake)
        runner.initialize()
        now = NOT_AFTER + 10
        runner.connection.execute(
            "UPDATE monitored_groups SET enabled=0, updated_at=? WHERE group_id=?",
            (now, GROUP),
        )
        runner.connection.execute(
            """
            INSERT INTO monitored_groups(group_id, enabled, created_at, updated_at)
            VALUES ('100000002', 1, ?, ?)
            """,
            (now, now),
        )
        runner.connection.commit()

        with self.assertRaisesRegex(WindowPolicyError, "frozen window jobs"):
            runner.process_one_page()
        self.assertEqual(fake.calls, [])

    def test_short_anchor_failures_keep_replaying_from_durable_anchor(self) -> None:
        fake = FakeOneBot(OneBotError("short id expired"), OneBotError("expired again"))
        runner = self.make_runner(fake, queue_threshold=10)
        runner.initialize()
        runner._budget_wait_seconds = lambda _now: 0  # type: ignore[method-assign]
        runner.connection.execute(
            """
            UPDATE window_recovery_jobs
            SET status='queued', probe_ok=1, next_anchor_id='8103',
                next_anchor_seq='103', next_anchor_time=?, replay_count=2,
                next_retry_at=0
            """,
            (NOT_AFTER - 1,),
        )
        runner.connection.commit()

        self.assertTrue(runner.process_one_page())
        job = runner.connection.execute("SELECT * FROM window_recovery_jobs").fetchone()
        self.assertEqual(job["status"], "probe")
        self.assertEqual(job["replay_count"], 3)
        self.assertEqual(job["next_anchor_id"], RAW_ANCHOR)

        # A later short-ID failure starts another safe replay instead of
        # permanently abandoning the bounded recovery window.
        runner.connection.execute(
            """
            UPDATE window_recovery_jobs
            SET status='queued', probe_ok=1, next_anchor_id='8102',
                next_anchor_seq='102', next_anchor_time=?, next_retry_at=0
            """,
            (NOT_AFTER - 2,),
        )
        runner.connection.commit()
        self.assertTrue(runner.process_one_page())

        job = runner.connection.execute("SELECT * FROM window_recovery_jobs").fetchone()
        self.assertEqual(job["status"], "probe")
        self.assertEqual(job["replay_count"], 4)
        self.assertEqual(job["next_anchor_id"], RAW_ANCHOR)
        self.assertEqual(len(fake.calls), 2)


if __name__ == "__main__":
    unittest.main()
