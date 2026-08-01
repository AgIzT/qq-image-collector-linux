from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from collector import OneBotClient, QCEClient
from linux.activate_account import activate, normalized_onebot
from linux.bootstrap import DEFAULT_GROUPS, configured_runtime_root, prepare
from linux.cache_cleanup import candidate_files
from linux.migrate_windows_snapshot import (
    rewrite_database,
    rewrite_path,
    verify_database,
)
from linux.rotate_napcat_token import rotate_webui_token


class LinuxAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_onebot_direct_settings_and_cross_namespace_path_mapping(self) -> None:
        token_file = self.root / "onebot.token"
        token_file.write_text("secret-value", encoding="utf-8")
        mapped_root = self.root / "qq-session"
        image = mapped_root / "123" / "nt_qq" / "nt_data" / "Pic" / "a.png"
        image.parent.mkdir(parents=True)
        image.write_bytes(b"png")
        client = OneBotClient.from_settings(
            {
                "base_url": "http://napcat:3000",
                "token_file": str(token_file),
                "path_mappings": [
                    {
                        "source": "/app/.config/QQ",
                        "target": str(mapped_root),
                    }
                ],
            }
        )
        self.assertEqual(client.base_url, "http://napcat:3000")
        self.assertEqual(client.token, "secret-value")
        self.assertEqual(
            client.resolve_local_path(
                "/app/.config/QQ/123/nt_qq/nt_data/Pic/a.png"
            ),
            image,
        )

    def test_qce_uses_first_existing_security_candidate(self) -> None:
        missing = self.root / "app" / "security.json"
        available = self.root / "root" / "security.json"
        available.parent.mkdir(parents=True)
        available.write_text(
            json.dumps({"accessToken": "qce-local-token"}),
            encoding="utf-8",
        )
        client = QCEClient(
            "http://127.0.0.1:40653",
            [missing, available],
        )
        self.assertEqual(client.security_config, available)
        self.assertEqual(client.token, "qce-local-token")

    def test_bootstrap_is_idempotent_and_disables_deep_history(self) -> None:
        first = prepare(self.root, DEFAULT_GROUPS)
        onebot_path = self.root / "runtime" / "napcat-config" / "onebot11.json"
        first_token = json.loads(onebot_path.read_text(encoding="utf-8"))["network"][
            "httpServers"
        ][0]["token"]
        second = prepare(self.root, DEFAULT_GROUPS)
        second_token = json.loads(onebot_path.read_text(encoding="utf-8"))["network"][
            "httpServers"
        ][0]["token"]
        collector = json.loads(
            (
                self.root
                / "runtime"
                / "repository"
                / "config"
                / "collector_config.json"
            ).read_text(encoding="utf-8")
        )
        manager = json.loads(
            (
                self.root / "runtime" / "manager" / "manager_config.json"
            ).read_text(encoding="utf-8")
        )
        self.assertTrue(first["onebot"])
        self.assertFalse(second["onebot"])
        self.assertEqual(first_token, second_token)
        self.assertFalse(collector["runtime"]["deep_backfill_enabled"])
        self.assertFalse(collector["runtime"]["backfill_paused"])
        self.assertEqual(
            collector["runtime"]["catchup_initial_lookback_seconds"],
            3600,
        )
        self.assertEqual(manager["deployment_mode"], "linux-docker")
        self.assertEqual(manager["launcher_kind"], "external")
        self.assertEqual(manager["local_forward_ports"], [17891])
        self.assertFalse(manager["direct_public_enabled"])
        self.assertEqual(manager["direct_public_hosts"], [])
        self.assertEqual(manager["direct_public_port"], 17891)
        onebot = json.loads(onebot_path.read_text(encoding="utf-8"))
        self.assertIsInstance(onebot["timeout"], dict)
        self.assertEqual(onebot["timeout"]["maxTimeout"], 180000)

    def test_activate_account_upgrades_legacy_timeout_and_targets_account(self) -> None:
        prepare(self.root, DEFAULT_GROUPS)
        config_dir = self.root / "runtime" / "napcat-config"
        template = config_dir / "onebot11.json"
        payload = json.loads(template.read_text(encoding="utf-8"))
        payload["timeout"] = 180000
        template.write_text(json.dumps(payload), encoding="utf-8")
        (config_dir / "napcat_1057233163.json").write_text(
            "{}\n",
            encoding="utf-8",
        )
        account, changed = activate(
            self.root / "runtime",
            "1057233163",
        )
        activated = json.loads(
            (config_dir / "onebot11_1057233163.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(account, "1057233163")
        self.assertTrue(changed)
        self.assertIsInstance(activated["timeout"], dict)
        self.assertEqual(activated["timeout"]["maxTimeout"], 180000)
        self.assertEqual(
            normalized_onebot(activated)["network"]["httpServers"][0]["port"],
            3000,
        )

    def test_bootstrap_uses_external_runtime_root_from_env_file(self) -> None:
        runtime = self.root / "mounted-disk" / "qqai"
        (self.root / ".env").write_text(
            f"QQAI_RUNTIME_ROOT={runtime}\n",
            encoding="utf-8",
        )
        self.assertEqual(configured_runtime_root(self.root), runtime)
        prepare(self.root, DEFAULT_GROUPS)
        self.assertTrue(
            (
                runtime
                / "repository"
                / "config"
                / "collector_config.json"
            ).is_file()
        )
        self.assertFalse((self.root / "runtime").exists())
        self.assertTrue(
            (runtime / "repository" / "final" / "NAI含参但不可直接读取的").is_dir()
        )
        self.assertTrue(
            (runtime / "repository" / "final" / "其他模型生成").is_dir()
        )
        config = json.loads(
            (
                runtime
                / "repository"
                / "config"
                / "collector_config.json"
            ).read_text(encoding="utf-8")
        )
        self.assertFalse(config["storage"]["migrate_existing_accepted_on_start"])

    def test_windows_paths_are_rewritten_to_container_repository(self) -> None:
        value = r"D:\qq-image-collector\final\NovelAI\2026-01-01.png"
        rewritten = rewrite_path(
            value,
            r"D:\qq-image-collector",
            Path("/data/qq-image-collector"),
        )
        self.assertEqual(
            rewritten,
            "/data/qq-image-collector/final/NovelAI/2026-01-01.png",
        )

    def test_in_place_database_migration_verifies_container_paths(self) -> None:
        image = self.root / "final" / "NovelAI" / "accepted.png"
        image.parent.mkdir(parents=True)
        image.write_bytes(b"accepted image")
        digest = hashlib.sha256(b"accepted image").hexdigest()
        database = self.root / "state" / "collector_state.sqlite3"
        database.parent.mkdir(parents=True)
        with sqlite3.connect(database) as connection:
            connection.execute(
                "CREATE TABLE images (status TEXT, sha256 TEXT, local_path TEXT)"
            )
            connection.execute(
                "INSERT INTO images VALUES ('accepted', ?, ?)",
                (
                    digest,
                    r"D:\qq-image-collector\final\NovelAI\accepted.png",
                ),
            )
            connection.commit()
        rewritten = rewrite_database(
            database,
            r"D:\qq-image-collector",
            Path("/data/qq-image-collector"),
        )
        verified = verify_database(
            database,
            Path("/data/qq-image-collector"),
            self.root,
        )
        self.assertEqual(rewritten["images"], 1)
        self.assertEqual(verified["verified"], 1)

    def test_cache_cleanup_never_walks_nt_db(self) -> None:
        account = self.root / "1197039114" / "nt_qq"
        pic = account / "nt_data" / "Pic"
        database = account / "nt_db"
        pic.mkdir(parents=True)
        database.mkdir(parents=True)
        old_original = pic / "original"
        recent_thumbnail = pic / "preview_198"
        protected_database = database / "messages.db"
        for path in (old_original, recent_thumbnail, protected_database):
            path.write_bytes(b"x")
        now = dt.datetime.now()
        old = now.timestamp() - 30 * 86400
        os.utime(old_original, (old, old))
        os.utime(recent_thumbnail, (old, old))
        os.utime(protected_database, (old, old))
        candidates = candidate_files(
            account,
            keep_days=7,
            thumbnail_keep_days=90,
            now=now,
        )
        self.assertIn(old_original, candidates)
        self.assertNotIn(recent_thumbnail, candidates)
        self.assertNotIn(protected_database, candidates)

    def test_compose_passes_optional_quick_login_fallbacks(self) -> None:
        compose = (
            Path(__file__).parent / "linux" / "docker-compose.yml"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "NAPCAT_QUICK_PASSWORD: ${NAPCAT_QUICK_PASSWORD:-}",
            compose,
        )
        self.assertIn(
            "NAPCAT_QUICK_PASSWORD_MD5: ${NAPCAT_QUICK_PASSWORD_MD5:-}",
            compose,
        )

    def test_napcat_public_webui_is_opt_in_and_keeps_loopback_rescue_port(
        self,
    ) -> None:
        project = Path(__file__).parent
        compose = (project / "linux" / "docker-compose.yml").read_text(
            encoding="utf-8"
        )
        example = (project / "linux" / ".env.example").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            '"127.0.0.1:${NAPCAT_WEBUI_PORT:-16099}:6099"',
            compose,
        )
        self.assertIn(
            '"${NAPCAT_PUBLIC_BIND:-127.0.0.1}:'
            '${NAPCAT_PUBLIC_WEBUI_PORT:-10058}:6099"',
            compose,
        )
        self.assertIn("NAPCAT_PUBLIC_BIND=127.0.0.1", example)
        self.assertIn("NAPCAT_PUBLIC_WEBUI_PORT=10058", example)

    def test_napcat_webui_token_rotation_is_atomic_and_private(self) -> None:
        config = self.root / "webui.json"
        config.write_text(
            json.dumps({"host": "::", "port": 6099, "token": "old"}),
            encoding="utf-8",
        )
        replacement = "a" * 64
        self.assertEqual(
            rotate_webui_token(config, replacement),
            replacement,
        )
        payload = json.loads(config.read_text(encoding="utf-8"))
        self.assertEqual(payload["token"], replacement)
        self.assertEqual(payload["host"], "::")
        self.assertEqual(payload["port"], 6099)
        if os.name != "nt":
            self.assertEqual(config.stat().st_mode & 0o777, 0o600)
        self.assertEqual(list(self.root.glob(".webui.json.*.tmp")), [])


if __name__ == "__main__":
    unittest.main()
