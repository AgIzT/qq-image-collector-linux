from __future__ import annotations

import asyncio
import hashlib
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
    increment_counter,
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
        self.assertEqual(
            items[0]["resolver_data"]["url_expires_at"]
            - items[0]["discovered_at"],
            30 * 60,
        )
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

    def test_queue_claims_oldest_event_before_newer_priority_work(self) -> None:
        older_event = image_event(
            url="https://gchat.qpic.cn/older?rkey=old",
            original=False,
        )
        older_event["raw"]["msgId"] = str(int(MESSAGE) + 1)
        older_event["raw"]["msgSeq"] = "12346"
        _cursor, older_items = parse_group_event(older_event)
        older_items[0]["discovered_at"] = int(time.time()) - 120

        newer_event = image_event(
            url="https://gchat.qpic.cn/newer?rkey=new",
            original=True,
        )
        newer_event["raw"]["msgId"] = str(int(MESSAGE) + 2)
        newer_event["raw"]["msgSeq"] = "12347"
        _cursor, newer_items = parse_group_event(newer_event)
        newer_items[0]["discovered_at"] = int(time.time())

        enqueue_image(self.connection, older_items[0])
        enqueue_image(self.connection, newer_items[0])
        priorities = dict(
            self.connection.execute(
                "SELECT message_id, queue_priority FROM images ORDER BY message_id"
            )
        )
        self.assertEqual(priorities[older_items[0]["message_id"]], 2)
        self.assertEqual(priorities[newer_items[0]["message_id"]], 0)

        statements: list[str] = []
        self.connection.set_trace_callback(statements.append)
        claimed = claim_next_image(self.connection)
        snapshot = queue_snapshot(self.connection)
        self.connection.set_trace_callback(None)

        self.assertEqual(claimed["message_id"], older_items[0]["message_id"])
        self.assertEqual(snapshot["depth"], 2)
        self.assertNotIn("json_extract", "\n".join(statements).casefold())

    def test_fresh_deadline_precedes_stale_fifo_backlog(self) -> None:
        now = int(time.time())
        stale_event = image_event(url="https://gchat.qpic.cn/stale?rkey=old")
        stale_event["raw"]["msgId"] = str(int(MESSAGE) + 10)
        stale_event["raw"]["msgSeq"] = "12355"
        _cursor, stale_items = parse_group_event(stale_event)
        stale_items[0]["discovered_at"] = now - 3600
        stale_items[0]["resolver_data"]["url_expires_at"] = now - 1

        fresh_event = image_event(url="https://gchat.qpic.cn/fresh?rkey=new")
        fresh_event["raw"]["msgId"] = str(int(MESSAGE) + 11)
        fresh_event["raw"]["msgSeq"] = "12356"
        _cursor, fresh_items = parse_group_event(fresh_event)
        fresh_items[0]["discovered_at"] = now
        fresh_items[0]["resolver_data"]["url_expires_at"] = now + 3600

        enqueue_image(self.connection, stale_items[0])
        enqueue_image(self.connection, fresh_items[0])
        claimed = claim_next_image(self.connection)
        self.assertEqual(claimed["message_id"], fresh_items[0]["message_id"])

    def test_batched_enqueue_and_counters_share_one_transaction(self) -> None:
        _cursor, items = parse_group_event(
            image_event(url="https://gchat.qpic.cn/batched?rkey=secret")
        )
        self.assertTrue(enqueue_image(self.connection, items[0], commit=False))
        increment_counter(self.connection, "images_seen", commit=False)
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM images").fetchone()[0],
            1,
        )
        self.connection.rollback()
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM images").fetchone()[0],
            0,
        )
        counters = self.connection.execute(
            "SELECT coalesce(sum(images_seen), 0), coalesce(sum(queued_high), 0) "
            "FROM hourly_counters"
        ).fetchone()
        self.assertEqual(tuple(counters), (0, 0))

    def test_queue_columns_migrate_once_from_resolver_json(self) -> None:
        self.connection.close()
        database = self.root / "queue-migration.sqlite3"
        legacy = sqlite3.connect(database)
        legacy.execute(
            """
            CREATE TABLE images (
                group_id TEXT, message_id TEXT, image_index INTEGER,
                status TEXT, resolver TEXT, resolver_json TEXT,
                updated_at INTEGER, discovered_at INTEGER,
                PRIMARY KEY(group_id, message_id, image_index)
            )
            """
        )
        legacy.execute(
            """
            INSERT INTO images VALUES (?, ?, 0, 'queued', 'event-cdn', ?, ?, NULL)
            """,
            (
                GROUP,
                MESSAGE,
                json.dumps(
                    {
                        "priority": 0,
                        "url_expires_at": 111 + 21601,
                        "url_expiry_basis": "conservative-any-rkey-6h-hint",
                    }
                ),
                111,
            ),
        )
        legacy.commit()
        legacy.close()

        migrated = connect_database(database)
        try:
            row = tuple(
                migrated.execute(
                    "SELECT queue_priority, url_expires_at, discovered_at, resolver_json FROM images"
                ).fetchone()
            )
            index_sql = " ".join(
                str(item[0])
                for item in migrated.execute(
                    """
                    SELECT sql FROM sqlite_master
                    WHERE name IN ('idx_images_claim_fifo','idx_images_claim_fresh')
                    ORDER BY name
                    """
                )
            )
            fifo_plan = " ".join(
                str(item[3])
                for item in migrated.execute(
                    """
                    EXPLAIN QUERY PLAN SELECT * FROM images
                    INDEXED BY idx_images_claim_fifo
                    WHERE status IN ('queued','deferred') AND next_retry_at<=?
                    ORDER BY discovered_at, queue_priority, sent_at,
                             group_id, message_id, image_index LIMIT 1
                    """,
                    (int(time.time()),),
                )
            )
            fresh_plan = " ".join(
                str(item[3])
                for item in migrated.execute(
                    """
                    EXPLAIN QUERY PLAN SELECT * FROM images
                    INDEXED BY idx_images_claim_fresh
                    WHERE status IN ('queued','deferred') AND next_retry_at<=?
                      AND url_expires_at>0 AND url_expires_at>?
                    ORDER BY url_expires_at, discovered_at, queue_priority, sent_at,
                             group_id, message_id, image_index LIMIT 1
                    """,
                    (int(time.time()), int(time.time())),
                )
            )
        finally:
            migrated.close()
        resolver = json.loads(row[3])
        self.assertEqual(row[0:3], (0, 111 + 1801, 111))
        self.assertEqual(resolver["url_expires_at"], 111 + 1801)
        self.assertEqual(
            resolver["url_expiry_basis"],
            "observed-any-rkey-30m-scheduling-window",
        )
        self.assertNotIn("json_extract", index_sql.casefold())
        self.assertIn("idx_images_claim_fifo", fifo_plan)
        self.assertNotIn("TEMP B-TREE", fifo_plan)
        self.assertIn("idx_images_claim_fresh", fresh_plan)
        self.assertNotIn("TEMP B-TREE", fresh_plan)
        self.connection = connect_database(self.root / "state.sqlite3")
        self.connection.row_factory = sqlite3.Row

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
        self.assertEqual(
            validate_cdn_url(
                "https://gxh.vip.qq.com/club/item/parcel/item/00/hash/raw300.gif"
            ),
            "https://gxh.vip.qq.com/club/item/parcel/item/00/hash/raw300.gif",
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

    def test_metadata_decode_error_rejects_only_the_bad_image(self) -> None:
        async def scenario() -> None:
            payload = a1111_png()

            async def handler(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(200, content=payload)

            _cursor, items = parse_group_event(
                image_event(url="https://gchat.qpic.cn/bad-metadata?rkey=secret")
            )
            enqueue_image(self.connection, items[0])
            downloader = CdnDownloader(
                self.connection,
                self.root,
                max_bytes=1024 * 1024,
                daily_limit=10,
            )
            await downloader.client.aclose()
            downloader.client = httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            )

            with mock.patch(
                "qq_image_collector.downloader.inspect_image",
                side_effect=ValueError(
                    "Decompressed data too large for PngImagePlugin.MAX_TEXT_CHUNK"
                ),
            ):
                self.assertEqual(
                    await downloader.process(claim_next_image(self.connection)),
                    "rejected",
                )

            bad = self.connection.execute(
                "SELECT status, sha256, error, resolver_json FROM images"
            ).fetchone()
            self.assertEqual(bad[0], "rejected_no_metadata")
            self.assertEqual(bad[1], hashlib.sha256(payload).hexdigest())
            self.assertEqual(bad[2], "metadata_decode_error:ValueError")
            self.assertNotIn("secret", bad[3])
            self.assertFalse(any((self.root / "temp").glob("*.part")))

            next_event = image_event(url="https://gchat.qpic.cn/good-metadata")
            next_event["raw"]["msgId"] = str(int(MESSAGE) + 1)
            next_event["raw"]["msgSeq"] = "12346"
            _cursor, next_items = parse_group_event(next_event)
            enqueue_image(self.connection, next_items[0])
            self.assertEqual(
                await downloader.process(claim_next_image(self.connection)),
                "accepted",
            )
            await downloader.close()

            statuses = list(
                self.connection.execute(
                    "SELECT status FROM images ORDER BY sent_at, message_id"
                )
            )
            self.assertEqual(
                [row[0] for row in statuses],
                ["rejected_no_metadata", "accepted"],
            )

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

    def test_expired_rkey_is_skipped_for_stable_fallback(self) -> None:
        async def scenario() -> None:
            event = image_event(
                url="https://multimedia.nt.qq.com.cn/stale?rkey=old"
            )
            event["raw"]["elements"][0]["picElement"]["originImageUrl"] = (
                "https://gchat.qpic.cn/stable"
            )
            _cursor, items = parse_group_event(event)
            items[0]["resolver_data"]["url_expires_at"] = int(time.time()) - 1
            enqueue_image(self.connection, items[0])
            requested: list[str] = []

            async def handler(request: httpx.Request) -> httpx.Response:
                requested.append(str(request.url))
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
            self.assertEqual(requested, ["https://gchat.qpic.cn/stable"])
            counters = self.connection.execute(
                "SELECT sum(cdn_requests), sum(cdn_downloads), sum(cdn_400) "
                "FROM hourly_counters"
            ).fetchone()
            self.assertEqual(tuple(counters), (1, 1, 0))
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
