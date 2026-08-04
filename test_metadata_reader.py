import gzip
import json
import tempfile
import unittest
import zlib
from pathlib import Path
from unittest.mock import patch

from PIL import Image
from PIL.PngImagePlugin import PngInfo

from linux.diagnostic_compare import metadata_summary
from qq_image_collector.database import accepted_path_for, category_for_source
from metadata_reader import (
    PNG_TEXT_CHUNK_LIMIT,
    _decompress_png_text,
    _extract_stealth_png,
    inspect_image,
)


class MetadataReaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def assert_png_bytes_are_extension_invariant(self, png_path: Path) -> None:
        expected = inspect_image(png_path)
        expected_summary = metadata_summary(png_path)
        payload = png_path.read_bytes()
        for suffix in (".part", ".bin"):
            candidate = self.root / f"{png_path.stem}{suffix}"
            candidate.write_bytes(payload)
            actual = inspect_image(candidate)
            self.assertEqual(actual.accepted, expected.accepted)
            self.assertEqual(actual.source, expected.source)
            self.assertEqual(actual.fields, expected.fields)
            self.assertEqual(actual.image_format, expected.image_format)
            self.assertEqual(metadata_summary(candidate), expected_summary)

    def test_png_metadata_channels_do_not_depend_on_filename_extension(self) -> None:
        novelai_text = self.root / "novelai-text.png"
        metadata = PngInfo()
        metadata.add_text("Source", "NovelAI Diffusion V4")
        metadata.add_text(
            "Comment",
            json.dumps({"prompt": "cat", "steps": 28, "signed_hash": "x"}),
        )
        Image.new("RGB", (64, 64), "white").save(novelai_text, pnginfo=metadata)
        self.assertEqual(inspect_image(novelai_text).source, "novelai")
        self.assert_png_bytes_are_extension_invariant(novelai_text)

        novelai_ztxt = self.root / "novelai-ztxt.png"
        metadata = PngInfo()
        metadata.add_text("Source", "NovelAI Diffusion V4", zip=True)
        metadata.add_text(
            "Comment",
            json.dumps({"prompt": "cat", "steps": 28, "signed_hash": "x"}),
            zip=True,
        )
        Image.new("RGB", (64, 64), "white").save(novelai_ztxt, pnginfo=metadata)
        self.assertEqual(inspect_image(novelai_ztxt).source, "novelai-unreadable")
        self.assert_png_bytes_are_extension_invariant(novelai_ztxt)

        comfyui = self.root / "comfyui-workflow.png"
        metadata = PngInfo()
        metadata.add_text(
            "workflow",
            json.dumps({"nodes": [{"id": 1, "class_type": "SaveImage"}]}),
        )
        Image.new("RGB", (64, 64), "white").save(comfyui, pnginfo=metadata)
        self.assertEqual(inspect_image(comfyui).source, "comfyui")
        self.assert_png_bytes_are_extension_invariant(comfyui)

        stealth = self.root / "novelai-alpha.png"
        stealth_payload = gzip.compress(
            json.dumps(
                {"prompt": "cat", "steps": 28, "signed_hash": "x"}
            ).encode("utf-8")
        )
        packed = (
            b"stealth_pngcomp"
            + (len(stealth_payload) * 8).to_bytes(4, "big", signed=True)
            + stealth_payload
        )
        bits = [
            (byte >> shift) & 1
            for byte in packed
            for shift in range(7, -1, -1)
        ]
        width = 64
        height = (len(bits) + width - 1) // width
        if height % 8 == 0:
            height += 1
        image = Image.new("RGBA", (width, height), (255, 255, 255, 254))
        alpha = [254] * (width * height)
        for index, bit in enumerate(bits):
            x, y = divmod(index, height)
            alpha[y * width + x] |= bit
        channel = Image.new("L", image.size)
        channel.putdata(alpha)
        image.putalpha(channel)
        image.save(stealth)
        self.assertEqual(inspect_image(stealth).source, "novelai")
        self.assert_png_bytes_are_extension_invariant(stealth)

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

    def test_compressed_png_text_larger_than_pillow_default_is_supported(self) -> None:
        path = self.root / "large-workflow.png"
        metadata = PngInfo()
        parameters = (
            "cat\nNegative prompt: blur\n"
            + ("workflow-node," * 90_000)
            + "\nSteps: 20, Sampler: Euler, Seed: 1"
        )
        self.assertGreater(len(parameters.encode("utf-8")), 1024 * 1024)
        self.assertLess(len(parameters.encode("utf-8")), PNG_TEXT_CHUNK_LIMIT)
        metadata.add_text("parameters", parameters, zip=True)
        Image.new("RGB", (8, 8), "white").save(path, pnginfo=metadata)

        result = inspect_image(path)

        self.assertTrue(result.accepted)
        self.assertEqual(result.source, "a1111-compatible")
        self.assertEqual(result.fields["parameters"], parameters)

    def test_png_text_decompression_is_bounded(self) -> None:
        within_limit = b"x" * PNG_TEXT_CHUNK_LIMIT
        over_limit = within_limit + b"x"

        self.assertEqual(
            _decompress_png_text(zlib.compress(within_limit)),
            within_limit,
        )
        self.assertIsNone(_decompress_png_text(zlib.compress(over_limit)))
        self.assertIsNone(_decompress_png_text(b"not-a-zlib-stream"))
        self.assertIsNone(_decompress_png_text(zlib.compress(b"truncated")[:-1]))

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

if __name__ == "__main__":
    unittest.main()
