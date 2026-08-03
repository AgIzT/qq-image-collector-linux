from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from qq_image_console.app import create_app
from qq_image_console.config import ConsoleConfig


TOKEN = "local-test-token-" + "x" * 32
GROUP = "100000001"


class FakeHealth:
    def snapshot(self, force: bool = False):
        del force
        return {
            "services": {
                "manager": {"healthy": True, "detail": "ok"},
                "qq": {"healthy": True, "detail": "ok"},
                "napcat": {"healthy": True, "detail": "ok"},
                "webui": {"healthy": True, "detail": "ok"},
                "onebot": {"healthy": True, "detail": "ok"},
                "event_socket": {"healthy": True, "detail": "ok"},
            },
            "account": {"user_id": "300000003", "nickname": "synthetic"},
        }

    def available_groups(self):
        return [
            {
                "group_id": GROUP,
                "group_name": "synthetic-group",
                "member_count": 2,
                "max_member_count": 200,
            }
        ]


class FakeSupervisor:
    def worker_status(self):
        return {"healthy": True, "detail": "event worker", "pid": 42}

    def action(self):
        return {"name": None, "status": "idle", "stage": None, "message": None, "error": None}

    def action_running(self):
        return False

    def request_start(self, _confirm=False):
        return {"confirmation_required": False, "action": self.action()}

    def request_stop(self):
        return {"action": self.action()}

    def request_restart(self, _confirm=False):
        return {"confirmation_required": False, "action": self.action()}


def make_config(root: Path) -> ConsoleConfig:
    repository = root / "repository"
    (repository / "state").mkdir(parents=True)
    manager = root / "manager"
    manager.mkdir()
    collector_config = repository / "config.json"
    collector_config.write_text(
        json.dumps(
            {
                "onebot": {
                    "base_url": "http://napcat:3000",
                    "ws_url": "ws://napcat:3001",
                    "webui_url": "http://napcat:6099",
                    "token": "synthetic",
                },
                "groups": [],
                "storage": {
                    "root": str(repository),
                    "database": str(repository / "state" / "collector.sqlite3"),
                },
                "runtime": {
                    "download_interval_seconds": 15,
                    "download_jitter_seconds": 3,
                    "daily_download_limit": 600,
                    "url_preference": "data",
                    "history_hourly_limit": 6,
                    "history_daily_limit": 20,
                    "collector_paused": False,
                },
            }
        ),
        encoding="utf-8",
    )
    return ConsoleConfig(
        data_dir=str(manager),
        collector_config=str(collector_config),
        qq_path="/opt/QQ/qq",
        napcat_root="/app/napcat",
        deployment_mode="linux-docker",
        launcher_kind="external",
        host="0.0.0.0",
        port=17890,
    )


class ConsoleEventApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        config = make_config(Path(self.temporary.name))
        self.config = config
        app = create_app(
            config,
            TOKEN,
            testing=True,
            health=FakeHealth(),
            supervisor=FakeSupervisor(),
        )
        self.client_context = TestClient(app)
        self.client = self.client_context.__enter__()
        response = self.client.get("/api/v1/session", params={"session_token": TOKEN})
        self.assertEqual(response.status_code, 200)

    def tearDown(self) -> None:
        self.client_context.__exit__(None, None, None)
        self.temporary.cleanup()

    def test_status_has_event_queue_downloader_and_recovery(self) -> None:
        response = self.client.get("/api/v1/status")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["account"]["user_id"], "300000003")
        for key in ("event_stream", "queue", "downloader", "recovery"):
            self.assertIn(key, payload["services"])
        self.assertNotIn("circuit", payload["services"])
        self.assertEqual(payload["statistics"]["today"]["get_image_blocked"], 0)

    def test_group_gap_api_and_retired_backfill(self) -> None:
        self.assertEqual(
            self.client.post(
                "/api/v1/groups", json={"group_id": GROUP, "display_name": "synthetic"}
            ).status_code,
            201,
        )
        retired = self.client.post(f"/api/v1/groups/{GROUP}/backfill", json={"mode": "continuous"})
        self.assertEqual(retired.status_code, 410)
        self.assertEqual(
            self.client.post(f"/api/v1/groups/{GROUP}/recover-gap", json={}).status_code,
            409,
        )
        import sqlite3

        with sqlite3.connect(self.config.database_path()) as connection:
            connection.execute(
                """
                UPDATE group_runtime SET last_message_id='9000000000000000001',
                    last_message_seq='12345', last_message_time=1785670000
                WHERE group_id=?
                """,
                (GROUP,),
            )
            connection.commit()
        recovery = self.client.post(f"/api/v1/groups/{GROUP}/recover-gap", json={})
        self.assertEqual(recovery.status_code, 202)
        jobs = self.client.get("/api/v1/jobs").json()
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["kind"], "gap_recovery")

    def test_safe_runtime_settings(self) -> None:
        current = self.client.get("/api/v1/settings").json()
        self.assertTrue(current["unlimited_collection"])
        self.assertNotIn("daily_download_limit", current)
        self.assertNotIn("history_hourly_limit", current)
        updated = self.client.patch(
            "/api/v1/settings",
            json={
                "download_interval_seconds": 20,
                "download_jitter_seconds": 2,
                "url_preference": "raw",
                "collector_paused": True,
            },
        )
        self.assertEqual(updated.status_code, 200)
        self.assertEqual(updated.json()["download_interval_seconds"], 20)
        self.assertEqual(updated.json()["url_preference"], "raw")
        self.assertTrue(updated.json()["collector_paused"])

        obsolete = self.client.patch(
            "/api/v1/settings", json={"daily_download_limit": 1}
        )
        self.assertEqual(obsolete.status_code, 422)

        rejected = self.client.patch(
            "/api/v1/settings", json={"url_preference": "unverified"}
        )
        self.assertEqual(rejected.status_code, 422)

    def test_static_assets_are_immutable_but_html_is_not_cached(self) -> None:
        asset = self.client.get("/assets/not-found.js")
        self.assertEqual(
            asset.headers.get("cache-control"),
            "public, max-age=31536000, immutable",
        )
        root = self.client.get("/")
        self.assertEqual(root.headers.get("cache-control"), "no-store")


if __name__ == "__main__":
    unittest.main()
