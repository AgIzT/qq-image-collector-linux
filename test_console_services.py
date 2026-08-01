from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock

from qq_image_console.services import ProcessSupervisor, WORKER_START_TIMEOUT_SECONDS
from test_console_repository import make_console_config


def snapshot(qq: bool, onebot: bool, ready: bool):
    return {
        "services": {
            "qq": {"healthy": qq, "detail": ""},
            "onebot": {"healthy": onebot, "detail": ""},
            "webui": {"healthy": ready, "detail": ""},
            "qce": {"healthy": ready, "detail": ""},
        }
    }


class SequencedHealth:
    def __init__(self, values):
        self.values = list(values)
        self.calls = 0

    def snapshot(self, force: bool = False):
        value = self.values[min(self.calls, len(self.values) - 1)]
        self.calls += 1
        return value

    def ready_for_collection(self, current=None):
        current = current or self.snapshot()
        services = current["services"]
        return all(services[name]["healthy"] for name in ("webui", "onebot", "qce"))

    def qq_processes(self):
        return []


class ProcessSupervisorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.config = make_console_config(Path(self.temporary.name), [])

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def wait_action(self, supervisor: ProcessSupervisor) -> dict:
        deadline = time.time() + 3
        while time.time() < deadline:
            action = supervisor.action()
            if action["status"] != "running":
                return action
            time.sleep(0.02)
        self.fail("system action did not finish")

    def test_worker_start_deadline_allows_large_repository_scan(self) -> None:
        self.assertGreaterEqual(WORKER_START_TIMEOUT_SECONDS, 300)

    def test_ready_services_start_worker_immediately(self) -> None:
        health = SequencedHealth([snapshot(True, True, True)])
        supervisor = ProcessSupervisor(self.config, health)  # type: ignore[arg-type]
        supervisor.start_worker = Mock(return_value={"healthy": True})  # type: ignore[method-assign]
        result = supervisor.request_start()
        self.assertFalse(result["confirmation_required"])
        action = self.wait_action(supervisor)
        self.assertEqual(action["status"], "completed")
        supervisor.start_worker.assert_called_once()

    def test_pending_containers_are_awaited_before_the_worker(self) -> None:
        health = SequencedHealth(
            [
                snapshot(False, False, False),
                snapshot(False, False, False),
                snapshot(True, True, True),
                snapshot(True, True, True),
            ]
        )
        supervisor = ProcessSupervisor(self.config, health)  # type: ignore[arg-type]
        supervisor.start_worker = Mock(return_value={"healthy": True})  # type: ignore[method-assign]
        result = supervisor.request_start()
        self.assertFalse(result["confirmation_required"])
        action = self.wait_action(supervisor)
        self.assertEqual(action["status"], "completed")
        self.assertEqual(action["stage"], "done")
        supervisor.start_worker.assert_called_once()

    def test_start_refuses_to_run_two_actions_at_once(self) -> None:
        health = SequencedHealth([snapshot(True, True, True)])
        supervisor = ProcessSupervisor(self.config, health)  # type: ignore[arg-type]
        supervisor._action["status"] = "running"
        with self.assertRaises(RuntimeError):
            supervisor.request_start()


if __name__ == "__main__":
    unittest.main()
