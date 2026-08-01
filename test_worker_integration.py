from __future__ import annotations

import asyncio
import io
import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

import httpx
import websockets
from PIL import Image
from PIL.PngImagePlugin import PngInfo

from qq_image_collector.database import claim_next_image, connect_database, enqueue_image
from qq_image_collector.events import parse_group_event
from qq_image_collector.worker import CollectorWorker
from test_event_pipeline import GROUP, MESSAGE, a1111_png, image_event


def write_config(root: Path, ws_url: str) -> Path:
    path = root / "collector.json"
    path.write_text(
        json.dumps(
            {
                "onebot": {
                    "base_url": "http://127.0.0.1:9",
                    "ws_url": ws_url,
                    "token": "synthetic-token",
                    "timeout_seconds": 1,
                },
                "groups": [GROUP],
                "storage": {
                    "root": str(root / "repository"),
                    "database": str(root / "repository" / "state" / "collector.sqlite3"),
                },
                "runtime": {
                    "pid_file": str(root / "collector.pid"),
                    "download_interval_seconds": 1,
                    "download_jitter_seconds": 0,
                    "accelerated_interval_seconds": 1,
                    "accelerate_queue_age_seconds": 1800,
                    "resume_normal_queue_age_seconds": 900,
                    "daily_download_limit": 600,
                    "max_download_bytes": 1024 * 1024,
                    "ws_ping_interval_seconds": 30,
                    "ws_disconnect_gap_seconds": 60,
                    "history_page_size": 20,
                    "history_max_pages_per_gap": 5,
                    "history_hourly_limit": 6,
                    "history_daily_limit": 20,
                    "cdn_403_window_seconds": 600,
                    "cdn_403_trip_count": 3,
                    "cdn_circuit_seconds": 3600,
                    "cdn_429_pause_seconds": 3600,
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


class WorkerIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    async def asyncTearDown(self) -> None:
        self.temporary.cleanup()

    async def _run_once(self, event: dict, response: bytes, wait_for_accept: bool) -> int:
        async def ws_handler(connection) -> None:
            await connection.send(json.dumps(event))
            await asyncio.sleep(5)

        calls = 0

        async def cdn_handler(_request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            return httpx.Response(200, content=response)

        async with websockets.serve(ws_handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            config = write_config(self.root, f"ws://127.0.0.1:{port}")
            worker = CollectorWorker(config)
            await worker.downloader.client.aclose()
            worker.downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(cdn_handler))
            task = asyncio.create_task(worker.run())
            deadline = time.monotonic() + 5
            while wait_for_accept and time.monotonic() < deadline:
                row = worker.connection.execute(
                    "SELECT status FROM images WHERE group_id=? AND message_id=? AND image_index=0",
                    (GROUP, MESSAGE),
                ).fetchone()
                if row and row[0] == "accepted":
                    break
                await asyncio.sleep(0.05)
            if not wait_for_accept:
                await asyncio.sleep(0.4)
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5)
        return calls

    async def test_fake_ws_to_cdn_persists_and_restart_deduplicates(self) -> None:
        event = image_event(url="https://gchat.qpic.cn/direct?rkey=ephemeral")
        self.assertEqual(await self._run_once(event, a1111_png(), True), 1)
        database = self.root / "repository" / "state" / "collector.sqlite3"
        with connect_database(database) as connection:
            status, local_path, resolver = connection.execute(
                "SELECT status, local_path, resolver_json FROM images"
            ).fetchone()
            self.assertEqual(status, "accepted")
            self.assertTrue(Path(local_path).is_file())
            self.assertNotIn("ephemeral", resolver)
            self.assertEqual(connection.execute("SELECT count(*) FROM assets").fetchone()[0], 1)
        self.assertEqual(await self._run_once(event, a1111_png(), False), 0)
        with connect_database(database) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM images").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT count(*) FROM assets").fetchone()[0], 1)

    async def test_403_refreshes_url_once_then_expires(self) -> None:
        config = write_config(self.root, "ws://127.0.0.1:9")
        worker = CollectorWorker(config)

        async def forbidden(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(403)

        await worker.downloader.client.aclose()
        worker.downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(forbidden))
        event = image_event(url="https://gchat.qpic.cn/expired?rkey=old")
        _cursor, items = parse_group_event(event)
        enqueue_image(worker.connection, items[0])
        history_calls = 0

        async def history(_action: str, _params: dict) -> dict:
            nonlocal history_calls
            history_calls += 1
            refreshed = image_event(url="https://gchat.qpic.cn/refreshed?rkey=new")
            return {"messages": [refreshed]}

        worker.onebot.call_async = history  # type: ignore[method-assign]
        first = claim_next_image(worker.connection)
        await worker._process_claimed(first)
        queued = worker.connection.execute("SELECT status, resolver_json FROM images").fetchone()
        self.assertEqual(queued[0], "queued")
        self.assertTrue(json.loads(queued[1])["url_refresh_attempted"])
        self.assertEqual(history_calls, 1)
        second = claim_next_image(worker.connection)
        await worker._process_claimed(second)
        self.assertEqual(worker.connection.execute("SELECT status FROM images").fetchone()[0], "expired")
        self.assertEqual(history_calls, 1)
        self.assertEqual(
            worker.connection.execute("SELECT sum(history_calls) FROM hourly_counters").fetchone()[0],
            1,
        )
        await worker.downloader.close()
        worker.connection.close()

    async def test_get_image_policy_violation_stops_and_pauses_worker(self) -> None:
        worker = CollectorWorker(write_config(self.root, "ws://127.0.0.1:9"))
        with self.assertRaises(Exception):
            await worker.onebot.call_async("get_image", {"file": "blocked"})
        self.assertTrue(worker.stop_event.is_set())
        paused = worker.connection.execute(
            "SELECT value_json FROM app_settings WHERE key='collector_paused'"
        ).fetchone()[0]
        self.assertEqual(paused, "true")
        blocked = worker.connection.execute(
            "SELECT sum(get_image_blocked) FROM hourly_counters"
        ).fetchone()[0]
        self.assertEqual(blocked, 1)
        await worker.downloader.close()
        worker.connection.close()

    async def test_429_defers_item_and_opens_one_hour_circuit(self) -> None:
        worker = CollectorWorker(write_config(self.root, "ws://127.0.0.1:9"))

        async def limited(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(429)

        await worker.downloader.client.aclose()
        worker.downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(limited))
        _cursor, items = parse_group_event(
            image_event(url="https://gchat.qpic.cn/limited?rkey=temporary")
        )
        enqueue_image(worker.connection, items[0])
        row = claim_next_image(worker.connection)
        before = int(time.time())
        await worker._process_claimed(row)
        status, next_retry, resolver = worker.connection.execute(
            "SELECT status, next_retry_at, resolver_json FROM images"
        ).fetchone()
        self.assertEqual(status, "deferred")
        self.assertGreaterEqual(next_retry, before + 3590)
        self.assertGreaterEqual(worker.circuit_until, before + 3590)
        self.assertIn("temporary", resolver)
        counters = worker.connection.execute(
            "SELECT sum(cdn_429), sum(history_calls) FROM hourly_counters"
        ).fetchone()
        self.assertEqual(tuple(counters), (1, 0))
        await worker.downloader.close()
        worker.connection.close()

    async def test_gap_recovery_is_bounded_to_five_pages(self) -> None:
        worker = CollectorWorker(write_config(self.root, "ws://127.0.0.1:9"))
        worker.connection.execute(
            "UPDATE group_runtime SET last_message_id=?, last_message_time=? WHERE group_id=?",
            (MESSAGE, 1_704_067_200, GROUP),
        )
        worker.connection.commit()
        calls = 0

        async def history(_action: str, _params: dict) -> dict:
            nonlocal calls
            calls += 1
            messages = []
            for offset in range(1, 21):
                event = image_event(
                    url=f"https://gchat.qpic.cn/gap-{calls}-{offset}?rkey=short-lived"
                )
                event["raw"]["msgId"] = str(int(MESSAGE) + calls * 100 + offset)
                event["raw"]["msgTime"] = 1_704_067_200 + calls * 100 + offset
                messages.append(event)
            return {"messages": messages}

        worker.onebot.call_async = history  # type: ignore[method-assign]
        discovered = await worker.recover_gap(GROUP)
        self.assertEqual(discovered, 100)
        self.assertEqual(calls, 5)
        self.assertEqual(
            worker.connection.execute("SELECT gap_status FROM group_runtime WHERE group_id=?", (GROUP,)).fetchone()[0],
            "partial",
        )
        self.assertEqual(
            worker.connection.execute("SELECT sum(history_calls) FROM hourly_counters").fetchone()[0],
            5,
        )
        self.assertEqual(
            worker.connection.execute("SELECT count(*) FROM images WHERE status='queued'").fetchone()[0],
            100,
        )
        await worker.downloader.close()
        worker.connection.close()


if __name__ == "__main__":
    unittest.main()
