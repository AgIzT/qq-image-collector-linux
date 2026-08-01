from __future__ import annotations

import hashlib
import json
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path

from collector import connect_database
from qq_image_console.config import ConsoleConfig
from qq_image_console.repository import Repository


def make_console_config(base: Path, groups: list[str] | None = None) -> ConsoleConfig:
    storage = base / "repository"
    collector_config = storage / "config" / "collector_config.json"
    collector_config.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "onebot": {"config_dir": str(base / "napcat" / "config"), "server_name": "local"},
        "qce": {
            "base_url": "http://127.0.0.1:40653",
            "security_config": str(base / "security.json"),
        },
        "groups": groups or [],
        "storage": {
            "root": str(storage),
            "database": str(storage / "state" / "collector_state.sqlite3"),
            "legacy_roots": [],
            "keep_rejected": False,
        },
        "runtime": {
            "pid_file": str(storage / "state" / "collector.pid"),
            "poll_interval_seconds": 60,
            "catchup_page_size": 50,
            "backfill_page_size": 50,
        },
    }
    collector_config.write_text(json.dumps(payload), encoding="utf-8")
    config = ConsoleConfig(
        data_dir=str(base / "manager"),
        collector_config=str(collector_config),
        qq_path=str(base / "QQ.exe"),
        napcat_root=str(base / "napcat"),
    )
    config.save()
    return config


class ConsoleRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.config = make_console_config(self.base, ["123456"])
        self.repository = Repository(self.config)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_group_changes_sync_config_without_deleting_state(self) -> None:
        self.repository.upsert_group("654321", "新增群")
        job_id = self.repository.create_job("123456", "continuous")
        payload = self.config.collector_settings()
        self.assertEqual(payload["groups"], ["123456", "654321"])
        self.repository.disable_group("123456")
        payload = self.config.collector_settings()
        self.assertEqual(payload["groups"], ["654321"])
        with closing(self.repository.connect()) as connection:
            row = connection.execute(
                "SELECT enabled FROM monitored_groups WHERE group_id='123456'"
            ).fetchone()
        self.assertEqual(row[0], 0)
        with closing(self.repository.connect()) as connection:
            job = connection.execute(
                "SELECT status, cancel_requested FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
        self.assertEqual((job["status"], job["cancel_requested"]), ("cancelled", 1))

    def test_statistics_count_unique_sha_not_duplicate_records(self) -> None:
        image_a = self.config.storage_root() / "final" / "NovelAI" / "a.png"
        image_b = self.config.storage_root() / "final" / "NovelAI" / "b.png"
        image_a.parent.mkdir(parents=True, exist_ok=True)
        image_a.write_bytes(b"same image")
        image_b.write_bytes(b"same image")
        digest = hashlib.sha256(b"same image").hexdigest()
        with connect_database(self.config.database_path()) as connection:
            for index, path in enumerate((image_a, image_b)):
                connection.execute(
                    """
                    INSERT INTO images (
                        group_id, message_id, image_index, sent_at, status, sha256,
                        local_path, metadata_source, updated_at
                    ) VALUES (?, ?, ?, ?, 'accepted', ?, ?, 'novelai', ?)
                    """,
                    ("123456", f"m{index}", 0, int(time.time()), digest, str(path), int(time.time())),
                )
            connection.commit()
        stats = self.repository.stats(force=True)
        self.assertEqual(stats["accepted_records"], 2)
        self.assertEqual(stats["unique_images"], 1)
        self.assertEqual(stats["novelai"], 1)

    def test_statistics_expose_all_four_categories(self) -> None:
        categories = (
            ("NovelAI", "novelai", "novelai"),
            ("ComfyUI", "comfyui", "comfyui"),
            (
                "NAI含参但不可直接读取的",
                "novelai-unreadable",
                "novelai_unreadable",
            ),
            ("其他模型生成", "a1111-compatible", "other_models"),
        )
        with connect_database(self.config.database_path()) as connection:
            for index, (directory, source, _stat_key) in enumerate(categories):
                content = f"image-{index}".encode()
                digest = hashlib.sha256(content).hexdigest()
                path = self.config.storage_root() / "final" / directory / f"{index}.png"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
                connection.execute(
                    """
                    INSERT INTO images (
                        group_id, message_id, image_index, sent_at, status, sha256,
                        local_path, metadata_source, updated_at
                    ) VALUES (?, ?, 0, ?, 'accepted', ?, ?, ?, ?)
                    """,
                    (
                        "123456",
                        f"category-{index}",
                        int(time.time()),
                        digest,
                        str(path),
                        source,
                        int(time.time()),
                    ),
                )
            connection.commit()

        stats = self.repository.stats(force=True)

        self.assertEqual(stats["unique_images"], 4)
        for _directory, _source, stat_key in categories:
            self.assertEqual(stats[stat_key], 1)

    def test_settings_are_persisted_for_live_worker(self) -> None:
        updated = self.repository.patch_app_settings(
            {"poll_interval_seconds": 30, "collector_paused": True}
        )
        self.assertEqual(updated["poll_interval_seconds"], 30)
        self.assertTrue(updated["collector_paused"])
        reloaded = Repository(self.config).get_app_settings()
        self.assertEqual(reloaded["poll_interval_seconds"], 30)
        self.assertTrue(reloaded["collector_paused"])

    def test_group_status_prefers_deep_roaming_cursor(self) -> None:
        with closing(self.repository.connect()) as connection:
            now = int(time.time())
            connection.execute(
                """
                INSERT INTO group_cursors (
                    group_id, oldest_seq, oldest_time, completed, updated_at
                ) VALUES ('123456', '100', 1000, 1, ?)
                """,
                (now,),
            )
            connection.execute(
                """
                INSERT INTO deep_history_cursors (
                    group_id, anchor_message_id, anchor_seq, anchor_time,
                    oldest_message_id, oldest_seq, oldest_time, completed, updated_at
                ) VALUES ('123456', '7657847017725580171', '100', 1000,
                          '7657847017725579999', '90', 900, 0, ?)
                """,
                (now,),
            )
            connection.execute(
                """
                UPDATE group_runtime
                SET backfill_status='complete', backfill_cursor_time=1000,
                    backfill_completed=1, updated_at=?
                WHERE group_id='123456'
                """,
                (now,),
            )
            connection.commit()

        group = self.repository.list_groups()[0]
        self.assertEqual(group["backfill_status"], "running")
        self.assertEqual(group["backfill_cursor_time"], 900)
        self.assertEqual(group["backfill_completed"], 0)

    def test_first_run_checklist_can_be_completed_after_dependencies_exist(self) -> None:
        new_base = self.base / "fresh"
        config = make_console_config(new_base, [])
        repository = Repository(config)
        self.assertFalse(repository.setup_status()["completed"])
        Path(config.qq_path).parent.mkdir(parents=True, exist_ok=True)
        Path(config.qq_path).write_bytes(b"")
        napcat = Path(config.napcat_root)
        napcat.mkdir(parents=True, exist_ok=True)
        for name in ("napimain.exe", "napiloader.dll", "nativeLoader.cjs"):
            (napcat / name).write_bytes(b"")
        onebot_dir = Path(config.collector_settings()["onebot"]["config_dir"])
        onebot_dir.mkdir(parents=True, exist_ok=True)
        (onebot_dir / "onebot11_test.json").write_text("{}", encoding="utf-8")
        security = Path(config.collector_settings()["qce"]["security_config"])
        security.parent.mkdir(parents=True, exist_ok=True)
        security.write_text("{}", encoding="utf-8")
        repository.upsert_group("123456")
        completed = repository.complete_setup()
        self.assertTrue(completed["completed"])
        self.assertTrue(completed["ready"])


if __name__ == "__main__":
    unittest.main()
