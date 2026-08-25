from __future__ import annotations

import os
import sys
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from qq_image_collector.pidfile import pid_start_time
from qq_image_console.main import _live_other_manager, main


class ConsoleMainTests(unittest.TestCase):
    def test_reused_container_pid_file_is_treated_as_stale(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            pid_file = Path(temporary) / "manager.pid"
            with patch("qq_image_console.main.os.getpid", return_value=7):
                pid_file.write_text("7", encoding="ascii")
                self.assertIsNone(_live_other_manager(pid_file))
        self.assertFalse(pid_file.exists())

    def test_reused_pid_from_a_previous_container_is_not_a_live_manager(self) -> None:
        # A container numbers processes from 1 again, so a leftover file names a
        # PID that exists and belongs to something else.  The recorded start
        # time is what tells the two apart.
        with tempfile.TemporaryDirectory() as temporary:
            pid_file = Path(temporary) / "manager.pid"
            live_pid = os.getpid()
            actual = pid_start_time(live_pid)
            pid_file.write_text(f"{live_pid + 1} 999999999", encoding="ascii")
            with patch("qq_image_collector.pidfile.pid_is_alive", return_value=True):
                with patch(
                    "qq_image_collector.pidfile.pid_start_time",
                    return_value=(actual if actual is not None else 12345),
                ):
                    self.assertIsNone(_live_other_manager(pid_file))
            self.assertFalse(pid_file.exists())

    def test_a_genuinely_live_manager_is_still_reported(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            pid_file = Path(temporary) / "manager.pid"
            other = os.getpid() + 1
            pid_file.write_text(f"{other} 4242", encoding="ascii")
            with patch("qq_image_collector.pidfile.pid_is_alive", return_value=True):
                with patch(
                    "qq_image_collector.pidfile.pid_start_time", return_value=4242
                ):
                    self.assertEqual(_live_other_manager(pid_file), other)
            self.assertTrue(pid_file.exists())

    def test_loopback_server_does_not_rewrite_client_from_proxy_headers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            config = SimpleNamespace(
                manager_log_file=base / "manager.log",
                manager_pid_file=base / "manager.pid",
                token_file=base / "manager.token",
                open_browser=False,
                port=17890,
            )
            with (
                patch.object(sys, "argv", ["console", "--no-browser"]),
                patch("qq_image_console.main.load_console_config", return_value=config),
                patch("qq_image_console.main._load_or_create_token", return_value="x" * 40),
                patch("qq_image_console.main.create_app", return_value=object()),
                patch("qq_image_console.main.PidFile", return_value=nullcontext()),
                patch("uvicorn.run") as run,
            ):
                self.assertEqual(main(), 0)

        self.assertEqual(run.call_count, 1)
        self.assertIs(run.call_args.kwargs["proxy_headers"], False)
        self.assertEqual(run.call_args.kwargs["host"], "127.0.0.1")


if __name__ == "__main__":
    unittest.main()
