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
from unittest import mock

import httpx
import websockets
from PIL import Image
from PIL.PngImagePlugin import PngInfo

from qq_image_collector.database import (
    claim_next_image,
    connect_database,
    enqueue_image,
    finish_image,
    get_runtime_state,
    queue_snapshot,
)
from qq_image_collector.downloader import CdnDownloader, validate_cdn_url
from qq_image_collector.events import EventListener, parse_group_event, record_group_cursor
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
                "fileName": "sample.png",
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
        raw_pictures.append(
            {
                "picElement": {
                    "original": None,
                    "fileSize": 1024,
                    "fileName": "second.gif",
                    "originImageUrl": url + "&second=1",
                }
            }
        )
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

    def test_process_local_short_ids_never_replace_durable_raw_cursor(self) -> None:
        durable, _items = parse_group_event(
            image_event(url="https://gchat.qpic.cn/durable")
        )
        self.assertTrue(durable["durable_raw"])
        record_group_cursor(self.connection, durable)

        short_event = image_event(url="https://gchat.qpic.cn/short")
        short_event.pop("raw")
        short_event["message_id"] = 77
        short_event["message_seq"] = 88
        short_event["real_seq"] = 99
        short, _items = parse_group_event(short_event)
        self.assertFalse(short["durable_raw"])
        short["event_at"] = int(durable["event_at"]) + 10
        record_group_cursor(self.connection, short)

        row = self.connection.execute(
            "SELECT last_message_id, last_message_seq, last_message_time, last_event_at "
            "FROM group_runtime WHERE group_id=?",
            (GROUP,),
        ).fetchone()
        self.assertEqual(row[0], MESSAGE)
        self.assertEqual(row[1], "12345")
        self.assertEqual(row[2], durable["sent_at"])
        self.assertEqual(row[3], short["event_at"])

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

    def test_raw_pictures_match_by_filename_and_unmatched_raw_is_not_lost(self) -> None:
        event = image_event(url="https://gchat.qpic.cn/first", two=True)
        event["raw"]["elements"].reverse()
        _cursor, items = parse_group_event(event)
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["resolver_data"]["raw_match"], "filename")
        self.assertEqual(items[0]["resolver_data"]["summary"], "[图片]")
        self.assertEqual(items[1]["resolver_data"]["raw_match"], "filename")

        event = image_event(url="https://gchat.qpic.cn/standard")
        event["message"][0]["data"]["file"] = "market-face-token"
        _cursor, items = parse_group_event(event)
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["resolver_data"]["raw_match"], "mismatch")
        self.assertEqual(items[0]["resolver_data"]["origin_url"], "")
        self.assertEqual(items[1]["resolver_data"]["raw_match"], "raw-only")
        self.assertTrue(items[1]["resolver_data"]["origin_url"])

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
        redacted = json.loads(resolver)
        self.assertNotIn("origin_url", redacted)
        self.assertNotIn("url", redacted)
        self.assertEqual(redacted["url_host"], "gchat.qpic.cn")

    def test_expired_row_revives_only_when_event_supplies_a_new_url(self) -> None:
        _cursor, items = parse_group_event(
            image_event(url="https://gchat.qpic.cn/path?rkey=old")
        )
        self.assertTrue(enqueue_image(self.connection, items[0]))
        row = claim_next_image(self.connection)
        finish_image(self.connection, row, status="expired", http_status=403)
        self.assertFalse(enqueue_image(self.connection, items[0]))
        self.assertEqual(
            self.connection.execute("SELECT status FROM images").fetchone()[0], "expired"
        )

        _cursor, refreshed = parse_group_event(
            image_event(url="https://gchat.qpic.cn/path?rkey=fresh")
        )
        self.assertTrue(enqueue_image(self.connection, refreshed[0]))
        status, resolver = self.connection.execute(
            "SELECT status, resolver_json FROM images"
        ).fetchone()
        self.assertEqual(status, "queued")
        self.assertIn("fresh", resolver)

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
        legacy.execute(
            "INSERT INTO images VALUES (?, ?, 0, 'queued', 'qce', ?)",
            (GROUP, str(int(MESSAGE) + 1), int(time.time())),
        )
        legacy.execute(
            "INSERT INTO images VALUES (?, ?, 0, 'downloading', 'onebot', ?)",
            (GROUP, str(int(MESSAGE) + 2), int(time.time())),
        )
        legacy.execute(
            "INSERT INTO images VALUES (?, ?, 0, 'queued', 'event-cdn', ?)",
            (GROUP, str(int(MESSAGE) + 3), int(time.time())),
        )
        legacy.execute(
            "INSERT INTO images VALUES (?, ?, 0, 'accepted', 'onebot', ?)",
            (GROUP, str(int(MESSAGE) + 4), int(time.time())),
        )
        legacy.execute("CREATE TABLE group_cursors(group_id TEXT)")
        legacy.execute("CREATE TABLE deep_history_cursors(group_id TEXT)")
        legacy.execute("CREATE TABLE qce_recent_cursors(group_id TEXT)")
        legacy.execute(
            """
            CREATE TABLE jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL,
                group_id TEXT NOT NULL, status TEXT NOT NULL,
                progress_pages INTEGER NOT NULL DEFAULT 0,
                cancel_requested INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL, started_at INTEGER,
                updated_at INTEGER NOT NULL, finished_at INTEGER, error TEXT
            )
            """
        )
        legacy.execute(
            """
            INSERT INTO jobs(kind, group_id, status, created_at, updated_at)
            VALUES ('continuous', ?, 'running', ?, ?)
            """,
            (GROUP, int(time.time()), int(time.time())),
        )
        legacy.execute(
            """
            INSERT INTO jobs(kind, group_id, status, created_at, updated_at)
            VALUES ('single_page', ?, 'queued', ?, ?)
            """,
            (GROUP, int(time.time()), int(time.time())),
        )
        legacy.execute(
            """
            INSERT INTO jobs(kind, group_id, status, created_at, updated_at)
            VALUES ('gap_recovery', ?, 'running', ?, ?)
            """,
            (GROUP, int(time.time()), int(time.time())),
        )
        legacy.commit()
        legacy.close()
        migrated = connect_database(database)
        statuses = dict(
            migrated.execute("SELECT message_id, status FROM images ORDER BY message_id")
        )
        self.assertEqual(statuses[MESSAGE], "legacy_failed")
        self.assertEqual(statuses[str(int(MESSAGE) + 1)], "legacy_failed")
        self.assertEqual(statuses[str(int(MESSAGE) + 2)], "legacy_failed")
        self.assertEqual(statuses[str(int(MESSAGE) + 3)], "queued")
        self.assertEqual(statuses[str(int(MESSAGE) + 4)], "accepted")
        tables = {row[0] for row in migrated.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertNotIn("group_cursors", tables)
        self.assertNotIn("deep_history_cursors", tables)
        self.assertNotIn("qce_recent_cursors", tables)
        jobs = dict(migrated.execute("SELECT kind, status FROM jobs ORDER BY id"))
        self.assertEqual(jobs["continuous"], "cancelled")
        self.assertEqual(jobs["single_page"], "cancelled")
        self.assertEqual(jobs["gap_recovery"], "running")
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
            counters = self.connection.execute(
                "SELECT sum(cdn_requests), sum(cdn_downloads) FROM hourly_counters"
            ).fetchone()
            self.assertEqual(tuple(counters), (1, 1))

            event = image_event(url="https://gchat.qpic.cn/gif?rkey=hidden")
            event["raw"]["msgId"] = str(int(MESSAGE) + 1)
            event["raw"]["msgSeq"] = "12346"
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

    def test_cdn_falls_back_to_second_allowed_event_url(self) -> None:
        async def scenario() -> None:
            event = image_event(url="https://gchat.qpic.cn/expired?rkey=old")
            event["raw"]["elements"][0]["picElement"]["originImageUrl"] = (
                "https://multimedia.nt.qq.com.cn/fresh?rkey=new"
            )
            _cursor, items = parse_group_event(event)
            enqueue_image(self.connection, items[0])
            row = claim_next_image(self.connection)

            async def handler(request: httpx.Request) -> httpx.Response:
                if request.url.host == "gchat.qpic.cn":
                    return httpx.Response(403)
                return httpx.Response(200, content=a1111_png())

            downloader = CdnDownloader(
                self.connection, self.root, max_bytes=1024 * 1024, daily_limit=10
            )
            await downloader.client.aclose()
            downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            self.assertEqual(await downloader.process(row), "accepted")
            counters = self.connection.execute(
                "SELECT sum(cdn_requests), sum(cdn_downloads), sum(cdn_403) FROM hourly_counters"
            ).fetchone()
            self.assertEqual(tuple(counters), (2, 1, 1))
            await downloader.close()

        asyncio.run(scenario())

    def test_cdn_400_falls_back_to_second_allowed_event_url(self) -> None:
        async def scenario() -> None:
            event = image_event(
                url="https://multimedia.nt.qq.com.cn/stale?rkey=old"
            )
            event["raw"]["elements"][0]["picElement"]["originImageUrl"] = (
                "https://gchat.qpic.cn/fresh"
            )
            _cursor, items = parse_group_event(event)
            enqueue_image(self.connection, items[0])
            requested_hosts: list[str] = []

            async def handler(request: httpx.Request) -> httpx.Response:
                requested_hosts.append(str(request.url.host))
                if request.url.host == "multimedia.nt.qq.com.cn":
                    return httpx.Response(400)
                return httpx.Response(200, content=a1111_png())

            downloader = CdnDownloader(
                self.connection, self.root, max_bytes=1024 * 1024, daily_limit=10
            )
            await downloader.client.aclose()
            downloader.client = httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            )
            self.assertEqual(
                await downloader.process(claim_next_image(self.connection)),
                "accepted",
            )
            self.assertEqual(
                requested_hosts,
                ["multimedia.nt.qq.com.cn", "gchat.qpic.cn"],
            )
            counters = self.connection.execute(
                """
                SELECT sum(cdn_requests), sum(cdn_downloads),
                       sum(cdn_400), sum(cdn_403), sum(cdn_429)
                FROM hourly_counters
                """
            ).fetchone()
            self.assertEqual(tuple(counters), (2, 1, 1, 0, 0))
            await downloader.close()

        asyncio.run(scenario())

    def test_cdn_skips_invalid_preferred_url_and_uses_allowed_fallback(self) -> None:
        async def scenario() -> None:
            event = image_event(url="https://example.invalid/not-qq?rkey=bad")
            event["raw"]["elements"][0]["picElement"]["originImageUrl"] = (
                "https://multimedia.nt.qq.com.cn/valid?rkey=good"
            )
            _cursor, items = parse_group_event(event)
            enqueue_image(self.connection, items[0])
            row = claim_next_image(self.connection)
            requested_hosts: list[str] = []

            async def handler(request: httpx.Request) -> httpx.Response:
                requested_hosts.append(str(request.url.host))
                return httpx.Response(200, content=a1111_png())

            downloader = CdnDownloader(
                self.connection, self.root, max_bytes=1024 * 1024, daily_limit=10
            )
            await downloader.client.aclose()
            downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            self.assertEqual(await downloader.process(row), "accepted")
            self.assertEqual(requested_hosts, ["multimedia.nt.qq.com.cn"])
            await downloader.close()

        asyncio.run(scenario())

    def test_normal_websocket_close_is_recorded_as_disconnect(self) -> None:
        async def scenario() -> None:
            async def handler(_connection) -> None:
                return

            async with websockets.serve(handler, "127.0.0.1", 0) as server:
                port = server.sockets[0].getsockname()[1]

                async def event_handler(_event: dict) -> None:
                    return

                async def reconnect_handler(_seconds: float) -> None:
                    return

                listener = EventListener(
                    self.connection,
                    f"ws://127.0.0.1:{port}",
                    "",
                    event_handler,
                    reconnect_handler,
                )
                task = asyncio.create_task(listener.run())
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    state = get_runtime_state(self.connection, "event_stream", {})
                    if str(state.get("last_error") or "").startswith("ConnectionError"):
                        break
                    await asyncio.sleep(0.05)
                listener.stop()
                await asyncio.wait_for(task, timeout=3)
                self.assertTrue(
                    str(state.get("last_error") or "").startswith("ConnectionError")
                )

        asyncio.run(scenario())

    def test_quiet_websocket_updates_connection_heartbeat(self) -> None:
        async def scenario() -> None:
            async def handler(_connection) -> None:
                await asyncio.sleep(4)

            async with websockets.serve(handler, "127.0.0.1", 0) as server:
                port = server.sockets[0].getsockname()[1]

                async def no_event(_event: dict) -> None:
                    return

                async def no_gap(_seconds: float) -> None:
                    return

                listener = EventListener(
                    self.connection,
                    f"ws://127.0.0.1:{port}",
                    "",
                    no_event,
                    no_gap,
                    state_heartbeat_interval=2,
                )
                task = asyncio.create_task(listener.run())
                deadline = time.monotonic() + 2
                first = 0
                while time.monotonic() < deadline:
                    state = get_runtime_state(self.connection, "event_stream", {})
                    first = int(state.get("heartbeat_at") or 0)
                    if first:
                        break
                    await asyncio.sleep(0.05)
                await asyncio.sleep(2.2)
                state = get_runtime_state(self.connection, "event_stream", {})
                self.assertTrue(state.get("connected"))
                self.assertGreater(int(state.get("heartbeat_at") or 0), first)
                self.assertFalse(state.get("last_event_at"))
                listener.stop()
                await asyncio.wait_for(task, timeout=3)

        asyncio.run(scenario())

    def test_runtime_lock_is_nonfatal_to_event_state(self) -> None:
        async def no_event(_event: dict) -> None:
            return

        async def no_gap(_seconds: float) -> None:
            return

        listener = EventListener(
            self.connection,
            "ws://127.0.0.1:9",
            "",
            no_event,
            no_gap,
        )
        with mock.patch(
            "qq_image_collector.events.set_runtime_state",
            side_effect=sqlite3.OperationalError("database is locked"),
        ):
            state = listener._state(force=True, connected=True)
        self.assertTrue(state["connected"])


if __name__ == "__main__":
    unittest.main()
