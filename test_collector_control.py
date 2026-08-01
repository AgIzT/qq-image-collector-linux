from __future__ import annotations

import tempfile
import unittest
import os
import time
from unittest.mock import patch
from pathlib import Path

from collector import PidFile, connect_database, pid_is_alive, process_manual_backfill_job
from collector_control import (
    enabled_groups,
    enqueue_job,
    get_setting,
    next_active_job,
    request_job_cancel,
    seed_monitored_groups,
    set_group_enabled,
    set_setting,
    update_job,
)


class CollectorControlTests(unittest.TestCase):
    def test_noninitializing_reader_does_not_reapply_schema_during_write(self) -> None:
        self.connection.execute("BEGIN IMMEDIATE")
        self.connection.execute(
            "UPDATE app_settings SET value_json=value_json WHERE key=?",
            ("backfill_paused",),
        )
        reader = connect_database(self.database, initialize=False)
        try:
            reader.execute("SELECT count(*) FROM monitored_groups").fetchone()
        finally:
            reader.close()
            self.connection.rollback()

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "state.sqlite3"
        self.connection = connect_database(self.database)
        self.connection.row_factory = __import__("sqlite3").Row

    def tearDown(self) -> None:
        self.connection.close()
        self.temporary.cleanup()

    def test_disabled_all_groups_does_not_fall_back_to_old_config(self) -> None:
        self.assertEqual(enabled_groups(self.connection, ["10001"]), ["10001"])
        seed_monitored_groups(self.connection, ["10001", "10002"])
        set_group_enabled(self.connection, "10001", False)
        set_group_enabled(self.connection, "10002", False)
        self.assertEqual(enabled_groups(self.connection, ["10001", "10002"]), [])

    def test_group_disable_preserves_runtime_and_can_be_reenabled(self) -> None:
        seed_monitored_groups(self.connection, ["123456"])
        set_group_enabled(self.connection, "123456", False, "测试群")
        row = self.connection.execute(
            "SELECT display_name, enabled FROM monitored_groups WHERE group_id='123456'"
        ).fetchone()
        self.assertEqual((row["display_name"], row["enabled"]), ("测试群", 0))
        self.assertIsNotNone(
            self.connection.execute(
                "SELECT 1 FROM group_runtime WHERE group_id='123456'"
            ).fetchone()
        )
        set_group_enabled(self.connection, "123456", True)
        self.assertEqual(enabled_groups(self.connection), ["123456"])

    def test_jobs_are_serial_per_group_and_cancel_at_boundary(self) -> None:
        first = enqueue_job(self.connection, "123456", "continuous")
        with self.assertRaises(ValueError):
            enqueue_job(self.connection, "123456", "page")
        second = enqueue_job(self.connection, "654321", "page")
        active = next_active_job(self.connection)
        self.assertEqual(active["id"], first)
        self.assertTrue(request_job_cancel(self.connection, first))
        active = next_active_job(self.connection)
        self.assertEqual(active["cancel_requested"], 1)
        update_job(self.connection, first, status="cancelled")
        self.assertEqual(next_active_job(self.connection)["id"], second)

    def test_dynamic_settings_are_json_typed(self) -> None:
        self.assertEqual(get_setting(self.connection, "poll", 60), 60)
        set_setting(self.connection, "poll", 45)
        set_setting(self.connection, "paused", True)
        self.assertEqual(get_setting(self.connection, "poll"), 45)
        self.assertIs(get_setting(self.connection, "paused"), True)

    def test_windows_safe_pid_probe(self) -> None:
        self.assertTrue(pid_is_alive(os.getpid()))
        self.assertFalse(pid_is_alive(2_000_000_000))

    def test_rescan_job_resets_history_cursor_before_first_page(self) -> None:
        job_id = enqueue_job(self.connection, "123456", "rescan")

        def fake_backfill(*args, **kwargs):
            row = self.connection.execute(
                "SELECT oldest_time, completed FROM group_cursors WHERE group_id='123456'"
            ).fetchone()
            self.assertLessEqual(abs(int(time.time()) - int(row["oldest_time"])), 2)
            self.assertEqual(row["completed"], 0)
            return 0

        with patch("collector.command_backfill", side_effect=fake_backfill):
            processed = process_manual_backfill_job(
                None, None, self.connection, Path(self.temporary.name), False, 20
            )
        self.assertEqual(processed, "123456")
        job = self.connection.execute(
            "SELECT status, progress_pages FROM jobs WHERE id=?", (job_id,)
        ).fetchone()
        self.assertEqual((job["status"], job["progress_pages"]), ("running", 1))

    def test_cancelled_job_stops_before_processing_next_page(self) -> None:
        job_id = enqueue_job(self.connection, "123456", "continuous")
        request_job_cancel(self.connection, job_id)
        processed = process_manual_backfill_job(
            None, None, self.connection, Path(self.temporary.name), False, 20
        )
        self.assertEqual(processed, "123456")
        status = self.connection.execute(
            "SELECT status FROM jobs WHERE id=?", (job_id,)
        ).fetchone()[0]
        self.assertEqual(status, "cancelled")

    def test_pid_file_rejects_a_second_worker(self) -> None:
        path = Path(self.temporary.name) / "worker.pid"
        with PidFile(path):
            with self.assertRaises(RuntimeError):
                with PidFile(path):
                    pass
        self.assertFalse(path.exists())


if __name__ == "__main__":
    unittest.main()
