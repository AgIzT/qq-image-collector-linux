from __future__ import annotations

import asyncio
import io
import json
import os
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

import httpx
from PIL import Image
from PIL.PngImagePlugin import PngInfo

from qq_image_collector.database import (
    claim_next_image,
    connect_database,
    enqueue_image,
    finish_image,
    queue_snapshot,
)
from qq_image_collector.downloader import CdnDownloader, validate_cdn_url
from qq_image_collector.events import parse_group_event
from qq_image_collector.onebot import OneBotClient, OneBotPolicyError


GROUP = "100000001"
SENDER = "200000002"
MESSAGE = "9000000000000000001"


def image_event(*, url: str, original: object = True, two: bool = False) -> dict:
    segments = [
        {
            "type": "image",
            "data": {
                "file": "sample.png",
                "url": url,
                "file_size": "204800",
                "summary": "[图片]",
                "sub_type": 0,
            },
        }
    ]
    raw_pictures = [
        {
            "picElement": {
                "original": original,
                "picWidth": 1024,
                "picHeight": 768,
                "fileSize": 204800,
                "md5HexStr": "00" * 16,
                "originImageUrl": url,
            }
        }
    ]
    if two:
        segments.append(
            {
                "type": "image",
                "data": {
                    "file": "second.gif",
                    "url": url + "&second=1",
                    "file_size": 1024,
                    "summary": "[动画表情]",
                    "sub_type": 1,
                },
            }
        )
        raw_pictures.append({"picElement": {"original": None, "fileSize": 1024}})
    return {
        "post_type": "message",
        "message_type": "group",
        "group_id": int(GROUP),
        "user_id": int(SENDER),
        "message_id": 12345,
        "time": 1_704_067_200,
        "sender": {"user_id": int(SENDER), "nickname": "synthetic-sender"},
        "message": segments,
        "raw": {
            "msgId": MESSAGE,
            "msgSeq": "12345",
            "msgTime": 1_704_067_200,
            "senderUin": SENDER,
            "elements": raw_pictures,
        },
    }


def a1111_png() -> bytes:
    metadata = PngInfo()
    metadata.add_text(
        "parameters",
        "cat\nNegative prompt: blur\nSteps: 20, Sampler: Euler a, CFG scale: 7, Seed: 1, Size: 8x8",
    )
    output = io.BytesIO()
    Image.new("RGB", (8, 8), "white").save(output, format="PNG", pnginfo=metadata)
    return output.getvalue()


class TrackedGifStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.read_after_header = False

    async def __aiter__(self):
        yield b"GIF89a"
        self.read_after_header = True
        yield b"x" * (1024 * 1024)


class EventPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.connection = connect_database(self.root / "state.sqlite3")
        self.connection.row_factory = sqlite3.Row

    def tearDown(self) -> None:
        self.connection.close()
        self.temporary.cleanup()

    def test_raw_event_and_multi_image_fields(self) -> None:
        cursor, items = parse_group_event(
            image_event(url="https://gchat.qpic.cn/path?rkey=secret", two=True)
        )
        self.assertEqual(cursor["message_id"], MESSAGE)
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["original_flag"], 1)
        self.assertEqual(items[0]["sender_uin"], SENDER)
        self.assertEqual(items[0]["resolver_data"]["width"], 1024)
        self.assertTrue(items[1]["resolver_data"]["emoji_signal"])
        self.assertIsNone(items[1]["original_flag"])

    def test_standard_original_fallback_and_priority(self) -> None:
        event = image_event(url="https://gchat.qpic.cn/path?rkey=secret", original=None)
        event["raw"]["elements"][0]["picElement"].pop("original")
        event["message"][0]["data"]["original"] = False
        _cursor, items = parse_group_event(event)
        self.assertEqual(items[0]["original_flag"], 0)
        self.assertTrue(enqueue_image(self.connection, items[0]))
        self.assertFalse(enqueue_image(self.connection, items[0]))
        stored = json.loads(self.connection.execute("SELECT resolver_json FROM images").fetchone()[0])
        self.assertEqual(stored["priority"], 2)
        self.assertEqual(queue_snapshot(self.connection)["depth"], 1)

    def test_terminal_result_removes_rkey_and_raw_url(self) -> None:
        _cursor, items = parse_group_event(
            image_event(url="https://gchat.qpic.cn/path?rkey=super-secret")
        )
        enqueue_image(self.connection, items[0])
        row = claim_next_image(self.connection)
        self.assertIsNotNone(row)
        finish_image(self.connection, row, status="rejected_no_metadata")
        resolver = self.connection.execute("SELECT resolver_json FROM images").fetchone()[0]
        self.assertNotIn("super-secret", resolver)
        self.assertNotIn("origin_url", resolver)
        self.assertNotIn('"url"', resolver)
        self.assertEqual(json.loads(resolver)["url_host"], "gchat.qpic.cn")

    def test_cdn_allowlist_is_fail_closed(self) -> None:
        self.assertEqual(
            validate_cdn_url("https://multimedia.nt.qq.com.cn/path?rkey=x"),
            "https://multimedia.nt.qq.com.cn/path?rkey=x",
        )
        for value in (
            "http://gchat.qpic.cn/path",
            "https://example.invalid/path",
            "https://user:pass@gchat.qpic.cn/path",
            "https://gchat.qpic.cn:444/path",
        ):
            with self.assertRaises(Exception):
                validate_cdn_url(value)

    def test_onebot_get_image_is_blocked_before_network(self) -> None:
        blocked: list[str] = []
        client = OneBotClient("http://127.0.0.1:9", "token", on_policy_violation=blocked.append)
        with self.assertRaises(OneBotPolicyError):
            client.call("get_image", {"file": "never-sent"})
        self.assertEqual(blocked, ["get_image"])

    def test_database_migration_marks_legacy_failures_and_drops_old_cursors(self) -> None:
        self.connection.close()
        database = self.root / "legacy.sqlite3"
        legacy = sqlite3.connect(database)
        legacy.execute(
            """
            CREATE TABLE images (
                group_id TEXT, message_id TEXT, image_index INTEGER, status TEXT,
                resolver TEXT, updated_at INTEGER,
                PRIMARY KEY(group_id, message_id, image_index)
            )
            """
        )
        legacy.execute(
            "INSERT INTO images VALUES (?, ?, 0, 'failed', 'onebot', ?)",
            (GROUP, MESSAGE, int(time.time())),
        )
        legacy.execute("CREATE TABLE group_cursors(group_id TEXT)")
        legacy.execute("CREATE TABLE deep_history_cursors(group_id TEXT)")
        legacy.execute("CREATE TABLE qce_recent_cursors(group_id TEXT)")
        legacy.commit()
        legacy.close()
        migrated = connect_database(database)
        self.assertEqual(migrated.execute("SELECT status FROM images").fetchone()[0], "legacy_failed")
        tables = {row[0] for row in migrated.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertNotIn("group_cursors", tables)
        self.assertNotIn("deep_history_cursors", tables)
        self.assertNotIn("qce_recent_cursors", tables)
        migrated.close()
        self.connection = connect_database(self.root / "state.sqlite3")

    def test_download_accepts_png_and_stops_gif_after_magic(self) -> None:
        async def scenario() -> None:
            png = a1111_png()

            async def png_handler(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(200, content=png)

            _cursor, items = parse_group_event(
                image_event(url="https://gchat.qpic.cn/image?rkey=secret")
            )
            enqueue_image(self.connection, items[0])
            row = claim_next_image(self.connection)
            downloader = CdnDownloader(self.connection, self.root, max_bytes=1024 * 1024, daily_limit=10)
            await downloader.client.aclose()
            downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(png_handler))
            self.assertEqual(await downloader.process(row), "accepted")
            await downloader.close()
            stored = self.connection.execute("SELECT status, metadata_source, resolver_json FROM images").fetchone()
            self.assertEqual(stored[0:2], ("accepted", "a1111-compatible"))
            self.assertNotIn("secret", stored[2])
            self.assertEqual(len(list((self.root / "final" / "其他模型生成").glob("*.png"))), 1)

            event = image_event(url="https://gchat.qpic.cn/gif?rkey=hidden")
            event["raw"]["msgId"] = str(int(MESSAGE) + 1)
            _cursor, gif_items = parse_group_event(event)
            enqueue_image(self.connection, gif_items[0])
            gif_row = claim_next_image(self.connection)
            stream = TrackedGifStream()

            async def gif_handler(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(200, stream=stream)

            downloader = CdnDownloader(self.connection, self.root, max_bytes=2 * 1024 * 1024, daily_limit=10)
            await downloader.client.aclose()
            downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(gif_handler))
            self.assertEqual(await downloader.process(gif_row), "filtered_gif")
            await downloader.close()
            self.assertFalse(stream.read_after_header)
            status = self.connection.execute(
                "SELECT status FROM images WHERE message_id=?", (str(int(MESSAGE) + 1),)
            ).fetchone()[0]
            self.assertEqual(status, "filtered_gif")
            self.assertFalse(any((self.root / "temp").glob("*.part")))

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
