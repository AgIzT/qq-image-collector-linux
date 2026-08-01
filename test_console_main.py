from __future__ import annotations

import sys
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from qq_image_console.main import _live_other_manager, main


class ConsoleMainTests(unittest.TestCase):
    def test_reused_container_pid_file_is_treated_as_stale(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            pid_file = Path(temporary) / "manager.pid"
            with patch("qq_image_console.main.os.getpid", return_value=7):
                pid_file.write_text("7", encoding="ascii")
                self.assertIsNone(_live_other_manager(pid_file))
        self.assertFalse(pid_file.exists())

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
