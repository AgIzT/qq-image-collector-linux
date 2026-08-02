from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from collector_control import set_setting
from linux.bootstrap import prepare
from linux.cache_cleanup import cleanup_once
from linux.event_probe import receive_sample
from linux.telemetry_report import report as telemetry_report
from qq_image_collector.database import connect_database


class LinuxEventAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.runtime = self.root / "runtime"
        (self.root / ".env.example").write_text("QQAI_RUNTIME_ROOT=./runtime\n", encoding="utf-8")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_bootstrap_reconciles_http_ws_and_preserves_manager_access(self) -> None:
        results = prepare(self.root, ["100000001"], runtime_root=self.runtime)
        self.assertTrue(results["onebot_token"])
        token = (self.runtime / "napcat-config" / "collector.onebot.token").read_text(encoding="utf-8").strip()
        config = json.loads((self.runtime / "napcat-config" / "onebot11.json").read_text(encoding="utf-8"))
        http = config["network"]["httpServers"][0]
        ws = config["network"]["websocketServers"][0]
        self.assertEqual((http["host"], http["port"], http["debug"]), ("0.0.0.0", 3000, True))
        self.assertEqual((ws["host"], ws["port"], ws["debug"]), ("0.0.0.0", 3001, True))
        self.assertEqual(http["token"], token)
        self.assertEqual(ws["token"], token)

        collector_path = self.runtime / "repository" / "config" / "collector_config.json"
        collector = json.loads(collector_path.read_text(encoding="utf-8"))
        self.assertNotIn("qce", collector)
        self.assertEqual(collector["groups"], ["100000001"])
        self.assertEqual(collector["runtime"]["daily_download_limit"], 3000)

        # 600 was the old production default.  A second prepare must upgrade
        # that exact value without requiring a fresh runtime directory.
        collector["runtime"]["daily_download_limit"] = 600
        collector_path.write_text(json.dumps(collector), encoding="utf-8")

        manager_path = self.runtime / "manager" / "manager_config.json"
        manager = json.loads(manager_path.read_text(encoding="utf-8"))
        manager["direct_public_enabled"] = True
        manager["direct_public_hosts"] = ["status.example.invalid"]
        manager_path.write_text(json.dumps(manager), encoding="utf-8")
        account_config = self.runtime / "napcat-config" / "onebot11_300000003.json"
        account_config.write_text("{}", encoding="utf-8")
        (self.runtime / "napcat-config" / "plugins.json").write_text(
            json.dumps({"napcat-plugin-qce": True, "other": True}), encoding="utf-8"
        )
        prepare(self.root, [], runtime_root=self.runtime)
        collector_after = json.loads(collector_path.read_text(encoding="utf-8"))
        self.assertEqual(collector_after["runtime"]["daily_download_limit"], 3000)
        manager_after = json.loads(manager_path.read_text(encoding="utf-8"))
        self.assertTrue(manager_after["direct_public_enabled"])
        self.assertEqual(manager_after["direct_public_hosts"], ["status.example.invalid"])
        self.assertEqual(json.loads(account_config.read_text(encoding="utf-8")), config)
        plugins = json.loads((self.runtime / "napcat-config" / "plugins.json").read_text(encoding="utf-8"))
        self.assertNotIn("napcat-plugin-qce", plugins)
        self.assertTrue(plugins["other"])

    def test_compose_is_private_and_pinned(self) -> None:
        compose = (Path(__file__).parent / "linux" / "docker-compose.yml").read_text(encoding="utf-8")
        self.assertIn("sha256:e66a6e52", compose)
        self.assertNotIn(":3000\"", compose)
        self.assertNotIn(":3001\"", compose)
        self.assertNotIn("40653", compose)
        self.assertIn("ss.xingzhige.com:127.0.0.1", compose)
        self.assertIn("secret-service.bietiaop.com:127.0.0.1", compose)
        self.assertIn("restart: unless-stopped", compose)

    def test_cleanup_never_touches_nt_db_or_final(self) -> None:
        account = self.runtime / "qq-session" / "300000003"
        stale_pic = account / "nt_data" / "Pic" / "stale.png"
        database = account / "nt_db" / "messages.db"
        final = self.runtime / "repository" / "final" / "NovelAI" / "keep.png"
        part = self.runtime / "repository" / "temp" / "old.part"
        for path in (stale_pic, database, final, part):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"data")
        old = time.time() - 10 * 3600
        os.utime(stale_pic, (old, old))
        os.utime(database, (old, old))
        os.utime(final, (old, old))
        os.utime(part, (old, old))
        args = argparse.Namespace(
            session_root=self.runtime / "qq-session",
            account=None,
            napcat_log_root=None,
            collector_temp_root=self.runtime / "repository" / "temp",
            collector_state_root=None,
            legacy_qce_root=None,
            short_keep_hours=2,
            media_keep_hours=24,
            log_keep_hours=48,
            legacy_keep_hours=168,
            loop_hours=0,
            apply=True,
        )
        cleanup_once(args)
        self.assertFalse(stale_pic.exists())
        self.assertFalse(part.exists())
        self.assertTrue(database.exists())
        self.assertTrue(final.exists())

        global_cache = self.runtime / "qq-session" / "nt_qq" / "global" / "nt_data" / "Pic" / "old.png"
        global_cache.parent.mkdir(parents=True, exist_ok=True)
        global_cache.write_bytes(b"old")
        os.utime(global_cache, (old, old))
        cleanup_once(args)
        self.assertFalse(global_cache.exists())

    def test_production_source_has_no_callable_get_image_or_qce_client(self) -> None:
        root = Path(__file__).parent
        production = "\n".join(
            path.read_text(encoding="utf-8")
            for path in [root / "collector.py", *(root / "qq_image_collector").glob("*.py")]
        )
        self.assertNotIn('call("get_image"', production)
        self.assertNotIn("QCEClient", production)

    def test_manage_container_python_commands_have_application_import_path(self) -> None:
        manage = (Path(__file__).parent / "linux" / "manage.sh").read_text(
            encoding="utf-8"
        )
        marker = "docker compose exec -T -e PYTHONPATH=/app"
        expected_commands = (
            "collector-console \\\n      python /app/linux/event_probe.py",
            "collector-console \\\n      python /app/linux/diagnostic_compare.py",
            "collector-console \\\n      python /app/linux/url_lifecycle_probe.py",
            "collector-console \\\n      python /app/linux/telemetry_report.py",
            "cache-cleaner python /app/linux/cache_cleanup.py",
        )
        for command in expected_commands:
            with self.subTest(command=command):
                self.assertIn(f"{marker} {command}", manage)

        # Both diagnostic modes and both lifecycle modes share their scripts;
        # the count locks every manage.sh branch, not just each distinct file.
        self.assertEqual(manage.count(f"{marker} collector-console"), 6)
        self.assertEqual(manage.count(f"{marker} cache-cleaner"), 1)
        self.assertNotIn(
            "docker compose exec -T collector-console python /app/linux/", manage
        )
        self.assertIn("./audit_rkey_network.sh qqai-napcat", manage)

    def test_event_probe_counts_unmatched_standard_and_raw_as_union(self) -> None:
        config = self.root / "collector.json"
        output = self.root / "probe.json"
        config.write_text(
            json.dumps(
                {
                    "onebot": {"ws_url": "ws://synthetic.invalid", "token": "test"},
                    "storage": {
                        "root": str(self.root / "repository"),
                        "database": str(self.root / "repository" / "state.sqlite3"),
                    },
                }
            ),
            encoding="utf-8",
        )
        event = {
            "post_type": "message",
            "message_type": "group",
            "group_id": 100000001,
            "message": [
                {
                    "type": "image",
                    "data": {
                        "file": "standard-only.png",
                        "url": "https://gchat.qpic.cn/standard?rkey=x",
                    },
                }
            ],
            "raw": {
                "elements": [
                    {
                        "picElement": {
                            "fileName": "raw-only.png",
                            "originImageUrl": "https://gchat.qpic.cn/raw?rkey=y",
                        }
                    }
                ]
            },
        }

        class FakeWebSocket:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            async def recv(self):
                return json.dumps(event)

        with patch("linux.event_probe.websockets.connect", return_value=FakeWebSocket()):
            code = asyncio.run(receive_sample(config, None, 2, 5, output))

        payload = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(code, 0)
        self.assertEqual(payload["independently_matched_pairs"], 0)
        self.assertEqual(payload["captured_estimated_image_slots"], 2)
        self.assertEqual(payload["standard_without_raw_match"], 1)
        self.assertEqual(payload["raw_without_standard_match"], 1)

    def test_telemetry_cannot_pass_before_full_observation_window(self) -> None:
        database = self.root / "telemetry.sqlite3"
        config = self.root / "telemetry.json"
        config.write_text(
            json.dumps(
                {
                    "storage": {
                        "root": str(self.root / "repository"),
                        "database": str(database),
                    }
                }
            ),
            encoding="utf-8",
        )
        now = 2_000_000_000
        with connect_database(database) as connection:
            set_setting(connection, "rollout_started_at", now - 3600)

        with patch("linux.telemetry_report.time.time", return_value=now):
            payload, gate = telemetry_report(config, 72)

        self.assertFalse(gate)
        self.assertEqual(payload["steady_state_gate"], "fail")
        self.assertFalse(payload["duration_requirement_met"])
        self.assertEqual(payload["observation_seconds"], 3600)
        self.assertEqual(payload["required_observation_seconds"], 72 * 3600)

    def test_get_image_diagnostic_consumes_sentinel_before_request(self) -> None:
        source = (Path(__file__).parent / "linux" / "diagnostic_compare.py").read_text(
            encoding="utf-8"
        )
        guarded = source.index("if args.allow_get_image_diagnostic:")
        exists_check = source.index("if sentinel.exists():", guarded)
        sentinel_write = source.index("atomic_private_json(", exists_check)
        outbound_call = source.index("await raw_onebot_get_image(", sentinel_write)
        self.assertLess(exists_check, sentinel_write)
        self.assertLess(sentinel_write, outbound_call)
        self.assertIn("one-time get_image diagnostic has already been consumed", source)


if __name__ == "__main__":
    unittest.main()
