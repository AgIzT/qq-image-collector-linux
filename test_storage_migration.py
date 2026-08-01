from __future__ import annotations

import hashlib
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from collector import connect_database, upsert_asset
from qq_image_console.storage import migrate_storage
from test_console_repository import make_console_config


class StorageMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.config = make_console_config(self.base, ["123456"])
        self.source = self.config.storage_root()
        self.image = self.source / "final" / "NovelAI" / "sample.png"
        self.image.parent.mkdir(parents=True, exist_ok=True)
        self.image.write_bytes(b"verified image payload")
        digest = hashlib.sha256(self.image.read_bytes()).hexdigest()
        with connect_database(self.config.database_path()) as connection:
            connection.execute(
                """
                INSERT INTO images (
                    group_id, message_id, image_index, sent_at, status, sha256,
                    local_path, metadata_source, updated_at
                ) VALUES ('123456', 'message', 0, ?, 'accepted', ?, ?, 'novelai', ?)
                """,
                (int(time.time()), digest, str(self.image), int(time.time())),
            )
            connection.execute(
                "INSERT INTO group_cursors (group_id, oldest_time, completed, updated_at) "
                "VALUES ('123456', 100, 0, ?)",
                (int(time.time()),),
            )
            connection.commit()
            upsert_asset(
                connection,
                digest,
                self.image,
                "novelai",
                "{}",
                {
                    "group_id": "123456",
                    "sender_uin": "654321",
                    "message_id": "message",
                    "image_index": 0,
                    "sent_at": int(time.time()),
                },
            )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_migration_keeps_old_repository_and_rewrites_paths(self) -> None:
        destination = self.base / "migrated"
        result = migrate_storage(self.config, destination)
        self.assertTrue(self.image.is_file(), "old repository must remain for rollback")
        migrated_image = destination / "final" / "NovelAI" / "sample.png"
        self.assertEqual(migrated_image.read_bytes(), self.image.read_bytes())
        self.assertEqual(Path(self.config.collector_config), destination / "config" / "collector_config.json")
        self.assertEqual(self.config.storage_root(), destination)
        with connect_database(self.config.database_path()) as connection:
            local_path = connection.execute(
                "SELECT local_path FROM images WHERE message_id='message'"
            ).fetchone()[0]
            cursor = connection.execute(
                "SELECT oldest_time FROM group_cursors WHERE group_id='123456'"
            ).fetchone()[0]
            asset_path = connection.execute(
                "SELECT local_path FROM assets WHERE sha256=?",
                (hashlib.sha256(self.image.read_bytes()).hexdigest(),),
            ).fetchone()[0]
        self.assertEqual(Path(local_path), migrated_image)
        self.assertEqual(Path(asset_path), migrated_image)
        self.assertEqual(cursor, 100)
        self.assertEqual(result["accepted_images_verified"], 1)
        self.assertTrue(Path(result["database_backup"]).is_file())

    def test_copy_failure_keeps_original_config_and_cleans_staging(self) -> None:
        destination = self.base / "failed-migration"
        original_config = self.config.collector_config
        with patch("qq_image_console.storage.shutil.copy2", side_effect=OSError("disk failure")):
            with self.assertRaises(OSError):
                migrate_storage(self.config, destination)
        self.assertEqual(self.config.collector_config, original_config)
        self.assertEqual(self.config.storage_root(), self.source)
        self.assertTrue(self.image.is_file())
        self.assertFalse(destination.exists())
        self.assertFalse(any(self.base.glob(".*.migration-*.tmp")))

    def test_nested_destination_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            migrate_storage(self.config, self.source / "nested")


if __name__ == "__main__":
    unittest.main()
