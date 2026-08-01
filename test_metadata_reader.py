import json
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from PIL import Image
from PIL.PngImagePlugin import PngInfo

from collector import (
    accepted_path_for,
    category_for_source,
    connect_database,
    migrate_existing_accepted,
    purge_excluded_accepted,
    record_status,
    should_skip,
)
from metadata_reader import _extract_stealth_png, inspect_image


class MetadataReaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_png_parameters(self) -> None:
        path = self.root / "a1111.png"
        metadata = PngInfo()
        metadata.add_text(
            "parameters",
            "cat\nNegative prompt: blur\nSteps: 20, Sampler: Euler, Seed: 1",
        )
        Image.new("RGB", (8, 8), "white").save(path, pnginfo=metadata)

        result = inspect_image(path)
        self.assertTrue(result.accepted)
        self.assertEqual(result.source, "a1111-compatible")
        self.assertIn("parameters", result.fields)

    def test_plain_comment_is_not_generation_metadata(self) -> None:
        path = self.root / "comment-note.png"
        metadata = PngInfo()
        metadata.add_text("Comment", "artist note and repost information")
        metadata.add_text("Description", "https://example.invalid/creator")
        Image.new("RGBA", (8, 8), (255, 255, 255, 255)).save(path, pnginfo=metadata)

        result = inspect_image(path)

        self.assertFalse(result.accepted)
        self.assertIsNone(result.source)
        self.assertEqual(result.fields, {})

    def test_parameters_without_structure_are_rejected(self) -> None:
        path = self.root / "parameters-note.png"
        metadata = PngInfo()
        metadata.add_text("parameters", "just a note")
        Image.new("RGB", (8, 8), "white").save(path, pnginfo=metadata)

        result = inspect_image(path)

        self.assertFalse(result.accepted)
        self.assertIsNone(result.source)

    def test_novelai_stealth_is_official_fallback(self) -> None:
        path = self.root / "novelai-stealth.png"
        Image.new("RGBA", (8, 8), (255, 255, 255, 255)).save(path)

        with patch(
            "metadata_reader._extract_stealth_png",
            return_value={
                "Comment": json.dumps({"prompt": "cat", "signed_hash": "x"}),
                "Source": "NovelAI Diffusion V4",
            },
        ):
            result = inspect_image(path)

        self.assertTrue(result.accepted)
        self.assertEqual(result.source, "novelai")

    def test_webp_decoder_fields_do_not_block_alpha_fallback(self) -> None:
        path = self.root / "novelai.webp"
        Image.new("RGBA", (8, 8), (255, 255, 255, 255)).save(path, lossless=True)

        with patch(
            "metadata_reader._extract_stealth_png",
            return_value={"prompt": "cat", "steps": 28, "signed_hash": "x"},
        ):
            result = inspect_image(path)

        self.assertTrue(result.accepted)
        self.assertEqual(result.source, "novelai")

    def test_jpeg_comment_is_recovery_only_for_novelai(self) -> None:
        class FakeJpeg:
            size = (8, 8)
            format = "JPEG"
            info = {
                "Comment": json.dumps({"prompt": "cat", "signed_hash": "x"}),
                "Source": "NovelAI Diffusion V4",
            }

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def load(self) -> None:
                pass

            def getexif(self) -> dict:
                return {}

            def getbands(self) -> tuple[str, str, str]:
                return ("R", "G", "B")

        with patch("metadata_reader.Image.open", return_value=FakeJpeg()):
            result = inspect_image(self.root / "novelai-comment.jpg")

        self.assertTrue(result.accepted)
        self.assertEqual(result.source, "novelai-unreadable")

    def test_native_novelai_comment_wins_over_alpha_duplicate(self) -> None:
        path = self.root / "novelai-text-and-stealth.png"
        metadata = PngInfo()
        metadata.add_text("Source", "NovelAI Diffusion V4")
        metadata.add_text("Comment", json.dumps({"prompt": "cat", "signed_hash": "x"}))
        Image.new("RGBA", (8, 8), (255, 255, 255, 255)).save(path, pnginfo=metadata)

        with patch(
            "metadata_reader._extract_stealth_png",
            return_value={"prompt": "cat", "steps": 28, "signed_hash": "x"},
        ):
            result = inspect_image(path)

        self.assertTrue(result.accepted)
        self.assertEqual(result.source, "novelai")

    def test_partial_text_blocks_alpha_official_fallback(self) -> None:
        path = self.root / "novelai-partial-text-and-stealth.png"
        metadata = PngInfo()
        metadata.add_text("Source", "NovelAI Diffusion V4")
        metadata.add_text("Title", "NovelAI generated")
        Image.new("RGBA", (8, 8), (255, 255, 255, 255)).save(path, pnginfo=metadata)

        with patch(
            "metadata_reader._extract_stealth_png",
            return_value={"prompt": "cat", "steps": 28, "signed_hash": "x"},
        ):
            result = inspect_image(path)

        self.assertTrue(result.accepted)
        self.assertEqual(result.source, "novelai-unreadable")

    def test_non_novelai_stealth_notes_are_rejected(self) -> None:
        path = self.root / "creator-stealth.png"
        Image.new("RGBA", (8, 8), (255, 255, 255, 255)).save(path)

        with patch(
            "metadata_reader._extract_stealth_png",
            return_value={
                "Comment": "creator note",
                "Source": "artist archive",
                "Description": "https://example.invalid/creator",
            },
        ):
            result = inspect_image(path)

        self.assertFalse(result.accepted)
        self.assertIsNone(result.source)

    def test_comfyui_version_in_parameters(self) -> None:
        path = self.root / "comfyui-parameters.png"
        metadata = PngInfo()
        metadata.add_text(
            "parameters",
            "cat\nSteps: 20, Seed: 1, Version: ComfyUI",
        )
        Image.new("RGB", (8, 8), "white").save(path, pnginfo=metadata)

        result = inspect_image(path)

        self.assertTrue(result.accepted)
        self.assertEqual(result.source, "comfyui")

    def test_png_ztxt_parameters_without_alpha(self) -> None:
        path = self.root / "ztxt-novelai.png"
        metadata = PngInfo()
        metadata.add_text(
            "Comment",
            json.dumps(
                {
                    "prompt": "cat",
                    "steps": 28,
                    "signed_hash": "x",
                }
            ),
            zip=True,
        )
        metadata.add_text("Source", "NovelAI Diffusion V4", zip=True)
        Image.new("RGB", (8, 8), "white").save(path, pnginfo=metadata)

        result = inspect_image(path)

        self.assertTrue(result.accepted)
        self.assertEqual(result.source, "novelai-unreadable")
        self.assertIn("signed_hash", result.fields["Comment"])

    def test_ztxt_duplicate_does_not_downgrade_native_comment(self) -> None:
        path = self.root / "text-and-ztxt-novelai.png"
        metadata = PngInfo()
        metadata.add_text("Source", "NovelAI Diffusion V4")
        metadata.add_text("Comment", json.dumps({"prompt": "cat", "signed_hash": "x"}))
        metadata.add_text("Description", "cat", zip=True)
        Image.new("RGBA", (8, 8), (255, 255, 255, 255)).save(path, pnginfo=metadata)

        result = inspect_image(path)

        self.assertTrue(result.accepted)
        self.assertEqual(result.source, "novelai")

    def test_ztxt_with_partial_text_and_alpha_is_unreadable(self) -> None:
        path = self.root / "blocked-ztxt-novelai.png"
        metadata = PngInfo()
        metadata.add_text("Source", "NovelAI Diffusion V4")
        metadata.add_text("Title", "NovelAI generated")
        metadata.add_text(
            "Comment",
            json.dumps({"prompt": "cat", "steps": 28, "signed_hash": "x"}),
            zip=True,
        )
        Image.new("RGBA", (8, 8), (255, 255, 255, 255)).save(path, pnginfo=metadata)

        with patch(
            "metadata_reader._extract_stealth_png",
            return_value={"prompt": "cat", "steps": 28, "signed_hash": "x"},
        ):
            result = inspect_image(path)

        self.assertTrue(result.accepted)
        self.assertEqual(result.source, "novelai-unreadable")

    def test_novelai_comment_marker(self) -> None:
        path = self.root / "novelai-comment.png"
        metadata = PngInfo()
        metadata.add_text("Software", "NovelAI")
        metadata.add_text("Comment", json.dumps({"prompt": "cat", "signed_hash": "x"}))
        Image.new("RGB", (8, 8), "white").save(path, pnginfo=metadata)

        result = inspect_image(path)

        self.assertTrue(result.accepted)
        self.assertEqual(result.source, "novelai")

    def test_novelai_compressed_itxt_is_official_text(self) -> None:
        path = self.root / "novelai-itxt.png"
        metadata = PngInfo()
        metadata.add_itxt("Source", "NovelAI Diffusion V4", zip=True)
        metadata.add_itxt(
            "Comment",
            json.dumps({"prompt": "cat", "steps": 28, "signed_hash": "x"}),
            zip=True,
        )
        Image.new("RGB", (8, 8), "white").save(path, pnginfo=metadata)

        result = inspect_image(path)

        self.assertTrue(result.accepted)
        self.assertEqual(result.source, "novelai")

    def test_png_without_metadata(self) -> None:
        path = self.root / "plain.png"
        Image.new("RGB", (8, 8), "white").save(path)

        result = inspect_image(path)
        self.assertFalse(result.accepted)
        self.assertEqual(result.fields, {})

    def test_gif_comment_is_always_rejected(self) -> None:
        path = self.root / "sticker.gif"
        Image.new("RGB", (8, 8), "white").save(
            path,
            format="GIF",
            comment=b'{"prompt":"cat","comment":"gif.ski"}',
        )

        result = inspect_image(path)

        self.assertFalse(result.accepted)
        self.assertIsNone(result.source)
        self.assertEqual(result.fields, {})
        self.assertEqual(result.image_format, "GIF")

    def test_truncated_stealth_payload_is_ignored(self) -> None:
        header = b"stealth_pngcomp" + (8).to_bytes(4, "big", signed=True) + b"x"
        bits = [
            (byte >> shift) & 1
            for byte in header
            for shift in range(7, -1, -1)
        ]
        image = Image.new("RGBA", (1, len(bits)), (0, 0, 0, 254))
        alpha = Image.new("L", image.size)
        alpha.putdata([254 | bit for bit in bits])
        image.putalpha(alpha)

        with patch(
            "metadata_reader.gzip.decompress",
            side_effect=EOFError("compressed stream ended early"),
        ):
            self.assertIsNone(_extract_stealth_png(image))

    def test_jpeg_user_comment(self) -> None:
        path = self.root / "a1111.jpg"
        image = Image.new("RGB", (8, 8), "white")
        exif = image.getexif()
        exif[37510] = b"ASCII\x00\x00\x00cat\nSteps: 20, Seed: 2"
        image.save(path, exif=exif)

        result = inspect_image(path)
        self.assertTrue(result.accepted)
        self.assertEqual(result.source, "a1111-compatible")
        self.assertIn("Steps: 20", result.fields["UserComment"])

    def test_nested_bytes_metadata_is_json_serializable(self) -> None:
        class FakeImage:
            size = (8, 8)
            format = "PNG"
            info = {
                "custom_metadata": {
                    "nodes": [
                        {
                            "class_type": "SaveImage",
                            "inputs": {
                                "text": b"cat",
                                "binary": b"\xff\xfe",
                            },
                        }
                    ],
                    "tuple_value": (b"nested",),
                    "bytearray_value": bytearray(b"also nested"),
                }
            }

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def load(self) -> None:
                pass

            def getexif(self) -> dict:
                return {}

            def getbands(self) -> tuple[str, str, str]:
                return ("R", "G", "B")

        with patch("metadata_reader.Image.open", return_value=FakeImage()):
            result = inspect_image(self.root / "nested.png")

        self.assertTrue(result.accepted)
        self.assertEqual(result.source, "comfyui")
        self.assertEqual(
            result.fields["custom_metadata"]["nodes"][0]["inputs"]["text"],
            "cat",
        )
        self.assertIsInstance(
            result.fields["custom_metadata"]["nodes"][0]["inputs"]["binary"],
            str,
        )
        self.assertEqual(
            result.fields["custom_metadata"]["bytearray_value"],
            "also nested",
        )
        json.dumps(result.fields, ensure_ascii=False)

    def test_four_category_layout(self) -> None:
        self.assertEqual(category_for_source("comfyui"), "ComfyUI")
        self.assertEqual(category_for_source("novelai"), "NovelAI")
        self.assertEqual(category_for_source("novelai-unreadable"), "NAI含参但不可直接读取的")
        self.assertEqual(category_for_source("novelai-stealth"), "NAI含参但不可直接读取的")
        self.assertEqual(category_for_source("a1111-compatible"), "其他模型生成")
        self.assertEqual(category_for_source("unknown-generator"), "其他模型生成")
        path = accepted_path_for(
            self.root,
            "ab" * 32,
            ".png",
            "comfyui",
            1704067200,
            "10000001",
            "123456789",
        )
        self.assertEqual(path.parent, self.root / "final" / "ComfyUI")
        self.assertEqual(
            path.name,
            "2024-01-01_08-00-00_g10000001_u123456789_ababababab.png",
        )

    def test_migration_flattens_images_and_removes_sidecars(self) -> None:
        content = b"same image content"
        digest = hashlib.sha256(content).hexdigest()
        old_path = self.root / "final" / "NovelAI" / digest[:2] / f"{digest}.png"
        old_path.parent.mkdir(parents=True)
        old_path.write_bytes(content)
        old_path.with_suffix(".png.json").write_text("{}", encoding="utf-8")

        connection = connect_database(self.root / "state.sqlite3")
        for message_id, sent_at in (("newer", 1704067200), ("older", 1704063600)):
            item = {
                "group_id": "1",
                "message_id": message_id,
                "message_seq": message_id,
                "image_index": 0,
                "sent_at": sent_at,
                "file": "token",
                "declared_size": len(content),
                "group_uin": "1",
                "sender_uin": "987654321",
            }
            record_status(
                connection,
                item,
                status="accepted",
                sha256=digest,
                local_path=str(old_path),
                metadata_source="novelai",
                metadata_json="{}",
            )

        migrated = migrate_existing_accepted(connection, self.root)
        expected_path = accepted_path_for(
            self.root,
            digest,
            ".png",
            "novelai",
            1704063600,
            "1",
            "987654321",
        )

        self.assertEqual(migrated, 2)
        self.assertTrue(expected_path.is_file())
        self.assertEqual(expected_path.read_bytes(), content)
        self.assertFalse(old_path.exists())
        self.assertFalse(expected_path.with_suffix(".png.json").exists())
        stored_paths = connection.execute(
            "SELECT DISTINCT local_path FROM images WHERE status='accepted'"
        ).fetchall()
        self.assertEqual(stored_paths, [(str(expected_path),)])
        asset = connection.execute(
            "SELECT local_path, canonical_group_id, canonical_sender_uin FROM assets"
        ).fetchone()
        self.assertEqual(asset, (str(expected_path), "1", "987654321"))
        connection.close()

    def test_migration_preserves_unreadable_novelai_category(self) -> None:
        content = b"novelai metadata stored outside the official comment"
        digest = hashlib.sha256(content).hexdigest()
        old_path = self.root / "final" / "NovelAI" / f"{digest}.png"
        old_path.parent.mkdir(parents=True)
        old_path.write_bytes(content)

        connection = connect_database(self.root / "state.sqlite3")
        item = {
            "group_id": "2",
            "message_id": "message",
            "message_seq": "message",
            "image_index": 0,
            "sent_at": 1704067200,
            "file": "token",
            "declared_size": len(content),
            "group_uin": "2",
            "sender_uin": "123456789",
        }
        record_status(
            connection,
            item,
            status="accepted",
            sha256=digest,
            local_path=str(old_path),
            metadata_source="novelai-unreadable",
            metadata_json='{"signed_hash":"x"}',
        )

        migrated = migrate_existing_accepted(connection, self.root)
        expected_path = accepted_path_for(
            self.root,
            digest,
            ".png",
            "novelai-unreadable",
            1704067200,
            "2",
            "123456789",
        )

        self.assertEqual(migrated, 1)
        self.assertTrue(expected_path.is_file())
        self.assertFalse(old_path.exists())
        stored = connection.execute(
            "SELECT local_path, metadata_source FROM images WHERE status='accepted'"
        ).fetchone()
        self.assertEqual(stored, (str(expected_path), "novelai-unreadable"))
        asset = connection.execute(
            "SELECT local_path, category, metadata_source FROM assets"
        ).fetchone()
        self.assertEqual(
            asset,
            (str(expected_path), "NAI含参但不可直接读取的", "novelai-unreadable"),
        )
        connection.close()

    def test_purge_excluded_gif_updates_all_duplicate_records(self) -> None:
        gif_path = self.root / "final" / "NovelAI" / "2026-07-12_00-00-00_abcdef.gif"
        gif_path.parent.mkdir(parents=True)
        gif_path.write_bytes(b"GIF89a")
        connection = connect_database(self.root / "state.sqlite3")
        for message_id in ("first", "duplicate"):
            item = {
                "group_id": "1",
                "message_id": message_id,
                "message_seq": message_id,
                "image_index": 0,
                "sent_at": 1704067200,
                "file": "token",
                "declared_size": 6,
            }
            record_status(
                connection,
                item,
                status="accepted",
                sha256="ab" * 32,
                local_path=str(gif_path),
                metadata_source="novelai",
                metadata_json='{"comment":"gif.ski"}',
            )

        changed_rows, deleted_files = purge_excluded_accepted(connection, self.root)

        self.assertEqual((changed_rows, deleted_files), (2, 1))
        self.assertFalse(gif_path.exists())
        rows = connection.execute(
            "SELECT status, local_path, metadata_source, metadata_json, error FROM images"
        ).fetchall()
        self.assertEqual(
            rows,
            [
                ("rejected_no_metadata", None, None, None, "excluded image format: GIF"),
                ("rejected_no_metadata", None, None, None, "excluded image format: GIF"),
            ],
        )
        connection.close()

    def test_failed_download_uses_retry_backoff(self) -> None:
        connection = connect_database(self.root / "state.sqlite3")
        item = {
            "group_id": "1",
            "message_id": "2",
            "message_seq": "3",
            "image_index": 0,
            "sent_at": 0,
            "file": "token",
            "declared_size": 1,
        }
        record_status(connection, item, status="failed", error="timeout")
        self.assertTrue(should_skip(connection, item))
        attempts, next_retry_at = connection.execute(
            "SELECT attempts, next_retry_at FROM images"
        ).fetchone()
        self.assertEqual(attempts, 1)
        self.assertGreater(next_retry_at, 0)
        connection.close()


if __name__ == "__main__":
    unittest.main()
