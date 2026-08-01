import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from PIL import Image
from PIL.PngImagePlugin import PngInfo

from collector import connect_database, record_status, upsert_asset
from linux.reclassify_repository import apply_plan, create_plan


class RepositoryReclassificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.final = self.root / "final"
        self.quarantine = self.root / "quarantine"
        self.database = self.root / "state.sqlite3"
        self.connection = connect_database(self.database)

    def tearDown(self) -> None:
        self.connection.close()
        self.temporary.cleanup()

    def _novelai(self, path: Path, prompt: str) -> str:
        path.parent.mkdir(parents=True, exist_ok=True)
        metadata = PngInfo()
        metadata.add_text("Source", "NovelAI Diffusion V4")
        metadata.add_text(
            "Comment",
            json.dumps({"prompt": prompt, "steps": 28, "signed_hash": "x"}),
        )
        Image.new("RGB", (8, 8), "white").save(path, pnginfo=metadata)
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def _plain(self, path: Path) -> str:
        path.parent.mkdir(parents=True, exist_ok=True)
        metadata = PngInfo()
        metadata.add_text("Comment", "creator note")
        Image.new("RGB", (8, 8), "white").save(path, pnginfo=metadata)
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def _record(self, digest: str, path: Path, message: str, status: str) -> None:
        item = {
            "group_id": "1",
            "message_id": message,
            "message_seq": message,
            "image_index": 0,
            "sent_at": 1704067200,
            "file": path.name,
            "declared_size": path.stat().st_size,
            "sender_uin": "2",
        }
        source = "novelai-unreadable" if status == "accepted" else None
        record_status(
            self.connection,
            item,
            status=status,
            sha256=digest,
            local_path=str(path),
            metadata_source=source,
            metadata_json="{}" if source else None,
            error=None if status == "accepted" else "old rejection",
        )
        if status == "accepted":
            upsert_asset(self.connection, digest, path, source, "{}", item)

    def test_dry_run_and_apply_restore_and_quarantine_without_losing_rows(self) -> None:
        misplaced = self.final / "NAI含参但不可直接读取的" / "misplaced.png"
        restored = self.quarantine / "NovelAI" / "restored.png"
        rejected = self.final / "NovelAI" / "plain.png"
        misplaced_sha = self._novelai(misplaced, "cat")
        restored_sha = self._novelai(restored, "dog")
        rejected_sha = self._plain(rejected)
        self._record(misplaced_sha, misplaced, "one", "accepted")
        self._record(restored_sha, restored, "two", "rejected_no_metadata")
        self._record(rejected_sha, rejected, "three", "accepted")

        entries, report = create_plan(self.final, self.quarantine)

        self.assertEqual(report["total_files"], 3)
        self.assertEqual(report["categories"], {"NovelAI": 2})
        self.assertEqual(report["rejected"], 1)
        self.assertTrue(misplaced.is_file())
        self.assertTrue(restored.is_file())
        self.assertTrue(rejected.is_file())

        backup = self.root / "backup.sqlite3"
        result = apply_plan(entries, self.database, backup)

        self.assertTrue(backup.is_file())
        self.assertEqual(result["accepted_assets"], 2)
        self.assertTrue((self.final / "NovelAI" / misplaced.name).is_file())
        self.assertTrue((self.final / "NovelAI" / restored.name).is_file())
        self.assertTrue((self.quarantine / "NovelAI" / rejected.name).is_file())
        rows = self.connection.execute(
            "SELECT sha256, status, metadata_source FROM images ORDER BY message_id"
        ).fetchall()
        self.assertEqual(
            rows,
            [
                (misplaced_sha, "accepted", "novelai"),
                (rejected_sha, "rejected_no_metadata", None),
                (restored_sha, "accepted", "novelai"),
            ],
        )
        assets = self.connection.execute(
            "SELECT sha256, category, parser_version FROM assets ORDER BY sha256"
        ).fetchall()
        self.assertEqual(
            assets,
            sorted(
                [
                    (misplaced_sha, "NovelAI", "3"),
                    (restored_sha, "NovelAI", "3"),
                ]
            ),
        )


if __name__ == "__main__":
    unittest.main()
