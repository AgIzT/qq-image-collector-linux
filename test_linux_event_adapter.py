from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
import unittest
from pathlib import Path

from linux.bootstrap import prepare
from linux.cache_cleanup import cleanup_once


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
        token = (self.runtime / "napcat-config" / "collector.onebot.token").read_text().strip()
        config = json.loads((self.runtime / "napcat-config" / "onebot11.json").read_text())
        http = config["network"]["httpServers"][0]
        ws = config["network"]["websocketServers"][0]
        self.assertEqual((http["host"], http["port"], http["debug"]), ("0.0.0.0", 3000, True))
        self.assertEqual((ws["host"], ws["port"], ws["debug"]), ("0.0.0.0", 3001, True))
        self.assertEqual(http["token"], token)
        self.assertEqual(ws["token"], token)

        collector_path = self.runtime / "repository" / "config" / "collector_config.json"
        collector = json.loads(collector_path.read_text())
        self.assertNotIn("qce", collector)
        self.assertEqual(collector["groups"], ["100000001"])
        self.assertEqual(collector["runtime"]["daily_download_limit"], 600)

        manager_path = self.runtime / "manager" / "manager_config.json"
        manager = json.loads(manager_path.read_text())
        manager["direct_public_enabled"] = True
        manager["direct_public_hosts"] = ["status.example.invalid"]
        manager_path.write_text(json.dumps(manager), encoding="utf-8")
        account_config = self.runtime / "napcat-config" / "onebot11_300000003.json"
        account_config.write_text("{}", encoding="utf-8")
        (self.runtime / "napcat-config" / "plugins.json").write_text(
            json.dumps({"napcat-plugin-qce": True, "other": True}), encoding="utf-8"
        )
        prepare(self.root, [], runtime_root=self.runtime)
        manager_after = json.loads(manager_path.read_text())
        self.assertTrue(manager_after["direct_public_enabled"])
        self.assertEqual(manager_after["direct_public_hosts"], ["status.example.invalid"])
        self.assertEqual(json.loads(account_config.read_text()), config)
        plugins = json.loads((self.runtime / "napcat-config" / "plugins.json").read_text())
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


if __name__ == "__main__":
    unittest.main()
