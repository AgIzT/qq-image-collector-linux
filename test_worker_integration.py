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
                    "max_download_bytes": 1024 * 1024,
                    "ws_ping_interval_seconds": 30,
                    "event_state_heartbeat_seconds": 2,
                    "ws_disconnect_gap_seconds": 60,
                    "history_page_size": 20,
                    "history_page_interval_seconds": 0,
                    "cdn_429_pause_seconds": 5,
                    "worker_restart_delay_seconds": 1,
                    "worker_heartbeat_seconds": 2,
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

    async def test_403_refreshes_repeatedly_without_terminal_drop(self) -> None:
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
        deferred = worker.connection.execute(
            "SELECT status, next_retry_at, resolver_json FROM images"
        ).fetchone()
        self.assertEqual(deferred[0], "deferred")
        self.assertGreater(deferred[1], int(time.time()))
        self.assertFalse(json.loads(deferred[2])["url_refresh_attempted"])
        self.assertEqual(json.loads(deferred[2])["url_refresh_count"], 1)
        self.assertEqual(history_calls, 1)
        worker.connection.execute("UPDATE images SET next_retry_at=0")
        worker.connection.commit()
        second = claim_next_image(worker.connection)
        await worker._process_claimed(second)
        status, retry_at = worker.connection.execute(
            "SELECT status, next_retry_at FROM images"
        ).fetchone()
        self.assertEqual(status, "deferred")
        self.assertGreater(retry_at, int(time.time()))
        self.assertEqual(history_calls, 2)
        self.assertEqual(
            worker.connection.execute("SELECT sum(history_calls) FROM hourly_counters").fetchone()[0],
            2,
        )
        await worker.downloader.close()
        worker.connection.close()

    async def test_403_always_attempts_history_and_defers_if_unavailable(self) -> None:
        worker = CollectorWorker(write_config(self.root, "ws://127.0.0.1:9"))

        async def forbidden(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(403)

        await worker.downloader.client.aclose()
        worker.downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(forbidden))
        _cursor, items = parse_group_event(
            image_event(url="https://gchat.qpic.cn/expired-default?rkey=old")
        )
        enqueue_image(worker.connection, items[0])
        history_calls = 0

        async def history(_action: str, _params: dict) -> dict:
            nonlocal history_calls
            history_calls += 1
            return {"messages": []}

        worker.onebot.call_async = history  # type: ignore[method-assign]
        await worker._process_claimed(claim_next_image(worker.connection))
        self.assertEqual(history_calls, 1)
        status, expired = worker.connection.execute(
            "SELECT status, (SELECT coalesce(sum(expired),0) FROM hourly_counters) FROM images"
        ).fetchone()
        self.assertEqual(status, "deferred")
        self.assertEqual(expired, 0)
        await worker.downloader.close()
        worker.connection.close()

    async def test_400_attempts_history_and_never_expires_silently(self) -> None:
        worker = CollectorWorker(write_config(self.root, "ws://127.0.0.1:9"))

        async def stale(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(400)

        await worker.downloader.client.aclose()
        worker.downloader.client = httpx.AsyncClient(
            transport=httpx.MockTransport(stale)
        )
        _cursor, items = parse_group_event(
            image_event(
                url="https://multimedia.nt.qq.com.cn/stale-default?rkey=old"
            )
        )
        enqueue_image(worker.connection, items[0])
        history_calls = 0

        async def history(_action: str, _params: dict) -> dict:
            nonlocal history_calls
            history_calls += 1
            return {"messages": []}

        worker.onebot.call_async = history  # type: ignore[method-assign]
        await worker._process_claimed(claim_next_image(worker.connection))
        status, resolver = worker.connection.execute(
            "SELECT status, resolver_json FROM images"
        ).fetchone()
        self.assertEqual(status, "deferred")
        self.assertNotIn("http_status", json.loads(resolver))
        self.assertEqual(history_calls, 1)
        counters = worker.connection.execute(
            """
            SELECT sum(cdn_400), sum(expired), sum(failed), sum(history_calls)
            FROM hourly_counters
            """
        ).fetchone()
        self.assertEqual(tuple(counters), (1, 0, 0, 1))
        await worker.downloader.close()
        worker.connection.close()

    async def test_400_with_rkey_refreshes_once_then_new_url_succeeds(self) -> None:
        worker = CollectorWorker(write_config(self.root, "ws://127.0.0.1:9"))
        async def cdn(request: httpx.Request) -> httpx.Response:
            if "stale" in request.url.path:
                return httpx.Response(400)
            return httpx.Response(200, content=a1111_png())

        await worker.downloader.client.aclose()
        worker.downloader.client = httpx.AsyncClient(
            transport=httpx.MockTransport(cdn)
        )
        _cursor, items = parse_group_event(
            image_event(url="https://multimedia.nt.qq.com.cn/stale?rkey=old")
        )
        enqueue_image(worker.connection, items[0])
        history_calls = 0

        async def history(action: str, _params: dict) -> dict:
            nonlocal history_calls
            history_calls += 1
            self.assertEqual(action, "get_group_msg_history")
            return {
                "messages": [
                    image_event(
                        url="https://multimedia.nt.qq.com.cn/fresh?rkey=new"
                    )
                ]
            }

        worker.onebot.call_async = history  # type: ignore[method-assign]
        await worker._process_claimed(claim_next_image(worker.connection))
        completed = worker.connection.execute(
            "SELECT status, resolver_json FROM images"
        ).fetchone()
        flags = json.loads(completed[1])
        self.assertEqual(completed[0], "accepted")
        self.assertFalse(flags["url_refresh_attempted"])
        self.assertTrue(flags["url_refreshed"])
        self.assertEqual(history_calls, 1)
        counters = worker.connection.execute(
            """
            SELECT sum(history_calls), sum(cdn_requests), sum(cdn_downloads),
                   sum(cdn_400), sum(expired)
            FROM hourly_counters
            """
        ).fetchone()
        self.assertEqual(tuple(counters), (1, 2, 1, 1, 0))
        await worker.downloader.close()
        worker.connection.close()

    async def test_400_without_expiry_hint_still_refreshes(self) -> None:
        worker = CollectorWorker(write_config(self.root, "ws://127.0.0.1:9"))

        async def bad_request(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(400)

        await worker.downloader.client.aclose()
        worker.downloader.client = httpx.AsyncClient(
            transport=httpx.MockTransport(bad_request)
        )
        _cursor, items = parse_group_event(
            image_event(url="https://multimedia.nt.qq.com.cn/no-expiry-evidence")
        )
        enqueue_image(worker.connection, items[0])
        history_calls = 0

        async def history(_action: str, _params: dict) -> dict:
            nonlocal history_calls
            history_calls += 1
            return {"messages": []}

        worker.onebot.call_async = history  # type: ignore[method-assign]
        await worker._process_claimed(claim_next_image(worker.connection))
        self.assertEqual(history_calls, 1)
        status, resolver = worker.connection.execute(
            "SELECT status, resolver_json FROM images"
        ).fetchone()
        self.assertEqual(status, "deferred")
        self.assertNotIn("http_status", json.loads(resolver))
        self.assertEqual(
            worker.connection.execute(
                "SELECT sum(history_calls) FROM hourly_counters"
            ).fetchone()[0],
            1,
        )
        await worker.downloader.close()
        worker.connection.close()

    async def test_obsolete_zero_budget_does_not_block_refresh(self) -> None:
        worker = CollectorWorker(write_config(self.root, "ws://127.0.0.1:9"))
        now = int(time.time())
        for key, value in (("history_hourly_limit", "0"), ("history_daily_limit", "0")):
            worker.connection.execute(
                "INSERT INTO app_settings(key, value_json, updated_at) VALUES (?, ?, ?)",
                (key, value, now),
            )
        worker.connection.commit()

        async def stale(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(400)

        await worker.downloader.client.aclose()
        worker.downloader.client = httpx.AsyncClient(
            transport=httpx.MockTransport(stale)
        )
        _cursor, items = parse_group_event(
            image_event(
                url="https://multimedia.nt.qq.com.cn/stale-budget?rkey=old"
            )
        )
        enqueue_image(worker.connection, items[0])
        history_calls = 0

        async def forbidden_history(_action: str, _params: dict) -> dict:
            nonlocal history_calls
            history_calls += 1
            return {"messages": []}

        worker.onebot.call_async = forbidden_history  # type: ignore[method-assign]
        before = int(time.time())
        await worker._process_claimed(claim_next_image(worker.connection))
        status, next_retry = worker.connection.execute(
            "SELECT status, next_retry_at FROM images"
        ).fetchone()
        self.assertEqual(status, "deferred")
        self.assertGreaterEqual(next_retry, before + 299)
        self.assertEqual(history_calls, 1)
        self.assertEqual(
            worker.connection.execute(
                "SELECT sum(history_calls) FROM hourly_counters"
            ).fetchone()[0],
            1,
        )
        await worker.downloader.close()
        worker.connection.close()

    async def test_404_and_410_refresh_then_defer(self) -> None:
        for status_code in (404, 410):
            with self.subTest(status_code=status_code):
                case_root = self.root / str(status_code)
                case_root.mkdir()
                worker = CollectorWorker(
                    write_config(case_root, "ws://127.0.0.1:9")
                )
                async def missing(
                    _request: httpx.Request, code: int = status_code
                ) -> httpx.Response:
                    return httpx.Response(code)

                await worker.downloader.client.aclose()
                worker.downloader.client = httpx.AsyncClient(
                    transport=httpx.MockTransport(missing)
                )
                _cursor, items = parse_group_event(
                    image_event(
                        url=(
                            "https://multimedia.nt.qq.com.cn/"
                            f"missing-{status_code}?rkey=old"
                        )
                    )
                )
                enqueue_image(worker.connection, items[0])
                history_calls = 0

                async def history(_action: str, _params: dict) -> dict:
                    nonlocal history_calls
                    history_calls += 1
                    return {"messages": []}

                worker.onebot.call_async = history  # type: ignore[method-assign]
                await worker._process_claimed(claim_next_image(worker.connection))
                self.assertEqual(history_calls, 1)
                status, resolver = worker.connection.execute(
                    "SELECT status, resolver_json FROM images"
                ).fetchone()
                self.assertEqual(status, "deferred")
                self.assertNotIn("http_status", json.loads(resolver))
                self.assertEqual(
                    worker.connection.execute(
                        "SELECT sum(history_calls) FROM hourly_counters"
                    ).fetchone()[0],
                    1,
                )
                await worker.downloader.close()
                worker.connection.close()

    async def test_get_image_policy_violation_is_blocked_without_stopping_worker(self) -> None:
        worker = CollectorWorker(write_config(self.root, "ws://127.0.0.1:9"))
        with self.assertRaises(Exception):
            await worker.onebot.call_async("get_image", {"file": "blocked"})
        self.assertFalse(worker.stop_event.is_set())
        paused = worker.connection.execute(
            "SELECT value_json FROM app_settings WHERE key='collector_paused'"
        ).fetchone()
        self.assertIsNone(paused)
        blocked = worker.connection.execute(
            "SELECT sum(get_image_blocked) FROM hourly_counters"
        ).fetchone()[0]
        self.assertEqual(blocked, 1)
        alarm = json.loads(
            worker.connection.execute(
                "SELECT value_json FROM runtime_state WHERE key='critical_alarm'"
            ).fetchone()[0]
        )
        self.assertTrue(alarm["active"])
        await worker.downloader.close()
        worker.connection.close()

    async def test_429_defers_only_the_current_item(self) -> None:
        worker = CollectorWorker(write_config(self.root, "ws://127.0.0.1:9"))
        requested_hosts: list[str] = []

        async def limited(request: httpx.Request) -> httpx.Response:
            requested_hosts.append(str(request.url.host))
            return httpx.Response(429)

        await worker.downloader.client.aclose()
        worker.downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(limited))
        event = image_event(url="https://gchat.qpic.cn/limited?rkey=temporary")
        event["raw"]["elements"][0]["picElement"]["originImageUrl"] = (
            "https://multimedia.nt.qq.com.cn/unused?rkey=temporary"
        )
        _cursor, items = parse_group_event(event)
        enqueue_image(worker.connection, items[0])
        row = claim_next_image(worker.connection)
        before = int(time.time())
        await worker._process_claimed(row)
        status, next_retry, resolver = worker.connection.execute(
            "SELECT status, next_retry_at, resolver_json FROM images"
        ).fetchone()
        self.assertEqual(status, "deferred")
        self.assertGreaterEqual(next_retry, before + 4)
        self.assertIn("temporary", resolver)
        counters = worker.connection.execute(
            "SELECT sum(cdn_429), sum(history_calls) FROM hourly_counters"
        ).fetchone()
        self.assertEqual(tuple(counters), (1, 0))
        self.assertEqual(requested_hosts, ["gchat.qpic.cn"])
        state = worker._downloader_state
        self.assertNotIn("circuit_until", state)
        await worker.downloader.close()
        worker.connection.close()

    async def test_fallback_403_and_400_defer_without_global_circuit(self) -> None:
        worker = CollectorWorker(write_config(self.root, "ws://127.0.0.1:9"))
        requested_hosts: list[str] = []

        async def expired_candidates(request: httpx.Request) -> httpx.Response:
            requested_hosts.append(str(request.url.host))
            if request.url.host == "gchat.qpic.cn":
                return httpx.Response(403)
            return httpx.Response(400)

        await worker.downloader.client.aclose()
        worker.downloader.client = httpx.AsyncClient(
            transport=httpx.MockTransport(expired_candidates)
        )
        for index in range(3):
            event = image_event(
                url=f"https://gchat.qpic.cn/forbidden-{index}?rkey=old"
            )
            event["raw"]["msgId"] = str(int(MESSAGE) + index + 1)
            event["raw"]["msgSeq"] = str(12346 + index)
            event["raw"]["elements"][0]["picElement"]["originImageUrl"] = (
                "https://multimedia.nt.qq.com.cn/"
                f"stale-{index}?rkey=old"
            )
            _cursor, items = parse_group_event(event)
            self.assertTrue(enqueue_image(worker.connection, items[0]))
            await worker._process_claimed(claim_next_image(worker.connection))

        self.assertEqual(
            requested_hosts,
            ["gchat.qpic.cn", "multimedia.nt.qq.com.cn"] * 3,
        )
        self.assertEqual(
            worker.connection.execute(
                "SELECT count(*) FROM images WHERE status='deferred'"
            ).fetchone()[0],
            3,
        )
        counters = worker.connection.execute(
            """
            SELECT sum(cdn_requests), sum(cdn_400), sum(cdn_403),
                   sum(expired), sum(history_calls)
            FROM hourly_counters
            """
        ).fetchone()
        self.assertEqual(tuple(counters), (6, 3, 3, 0, 3))
        self.assertNotIn("circuit_until", worker._downloader_state)
        await worker.downloader.close()
        worker.connection.close()

    async def test_transient_cdn_status_keeps_retrying(self) -> None:
        for status_code in (408, 503):
            with self.subTest(status_code=status_code):
                case_root = self.root / str(status_code)
                case_root.mkdir()
                worker = CollectorWorker(write_config(case_root, "ws://127.0.0.1:9"))
                calls = 0

                async def transient(_request: httpx.Request) -> httpx.Response:
                    nonlocal calls
                    calls += 1
                    return httpx.Response(status_code)

                await worker.downloader.client.aclose()
                worker.downloader.client = httpx.AsyncClient(
                    transport=httpx.MockTransport(transient)
                )
                event = image_event(
                    url=f"https://gchat.qpic.cn/transient-{status_code}?rkey=temporary"
                )
                _cursor, items = parse_group_event(event)
                enqueue_image(worker.connection, items[0])

                for expected_attempt in (1, 2, 3):
                    row = claim_next_image(worker.connection)
                    self.assertIsNotNone(row)
                    await worker._process_claimed(row)
                    status, attempts = worker.connection.execute(
                        "SELECT status, attempts FROM images"
                    ).fetchone()
                    self.assertEqual(attempts, expected_attempt)
                    self.assertEqual(status, "deferred")
                    worker.connection.execute(
                        "UPDATE images SET next_retry_at=0 WHERE status='deferred'"
                    )
                    worker.connection.commit()

                self.assertEqual(calls, 3)
                worker.connection.execute(
                    "UPDATE images SET next_retry_at=0 WHERE status='deferred'"
                )
                worker.connection.commit()
                self.assertIsNotNone(claim_next_image(worker.connection))
                failed = worker.connection.execute(
                    "SELECT coalesce(sum(failed),0) FROM hourly_counters"
                ).fetchone()[0]
                self.assertEqual(failed, 0)
                await worker.downloader.close()
                worker.connection.close()

    async def test_gap_recovery_continues_past_five_pages_until_source_end(self) -> None:
        worker = CollectorWorker(write_config(self.root, "ws://127.0.0.1:9"))
        worker.connection.execute(
            """
            UPDATE group_runtime SET last_message_id=?, last_message_seq=?,
                last_message_time=? WHERE group_id=?
            """,
            (MESSAGE, "12345", 1_704_067_200, GROUP),
        )
        worker.connection.commit()
        calls = 0

        async def history(_action: str, _params: dict) -> dict:
            nonlocal calls
            calls += 1
            messages = []
            page_length = 1 if calls == 7 else 20
            for offset in range(1, page_length + 1):
                event = image_event(
                    url=f"https://gchat.qpic.cn/gap-{calls}-{offset}?rkey=short-lived"
                )
                event["raw"]["msgId"] = str(int(MESSAGE) + calls * 100 + offset)
                event["raw"]["msgSeq"] = str(12345 + calls * 100 + offset)
                event["raw"]["msgTime"] = 1_704_067_200 + calls * 100 + offset
                messages.append(event)
            return {"messages": messages}

        worker.onebot.call_async = history  # type: ignore[method-assign]
        discovered = await worker.recover_gap(GROUP)
        self.assertEqual(discovered, 121)
        self.assertEqual(calls, 7)
        self.assertEqual(
            worker.connection.execute("SELECT gap_status FROM group_runtime WHERE group_id=?", (GROUP,)).fetchone()[0],
            "complete",
        )
        self.assertEqual(
            worker.connection.execute("SELECT sum(history_calls) FROM hourly_counters").fetchone()[0],
            7,
        )
        self.assertEqual(
            worker.connection.execute("SELECT count(*) FROM images WHERE status='queued'").fetchone()[0],
            121,
        )
        durable = worker.connection.execute(
            "SELECT last_message_id, last_message_seq FROM group_runtime WHERE group_id=?",
            (GROUP,),
        ).fetchone()
        self.assertEqual(tuple(durable), (MESSAGE, "12345"))
        await worker.downloader.close()
        worker.connection.close()

    async def test_legacy_live_only_marker_does_not_block_gap_recovery(self) -> None:
        worker = CollectorWorker(write_config(self.root, "ws://127.0.0.1:9"))
        now = int(time.time())
        worker.connection.execute(
            """
            INSERT INTO app_settings(key, value_json, updated_at)
            VALUES ('production_live_only_started_at', '1785670554', ?)
            """,
            (now,),
        )
        worker.connection.execute(
            """
            UPDATE group_runtime SET last_message_id=?, last_message_seq=?,
                last_message_time=? WHERE group_id=?
            """,
            (MESSAGE, "12345", 1_704_067_200, GROUP),
        )
        worker.connection.commit()
        calls = 0

        async def history(_action: str, _params: dict) -> dict:
            nonlocal calls
            calls += 1
            return {"messages": []}

        worker.onebot.call_async = history  # type: ignore[method-assign]
        self.assertEqual(await worker.recover_gap(GROUP), 0)
        self.assertEqual(calls, 1)
        self.assertEqual(
            worker.connection.execute(
                "SELECT coalesce(sum(history_calls),0) FROM hourly_counters"
            ).fetchone()[0],
            1,
        )
        await worker.downloader.close()
        worker.connection.close()

    async def test_background_task_exit_fails_fast_instead_of_fake_alive(self) -> None:
        worker = CollectorWorker(write_config(self.root, "ws://127.0.0.1:9"))

        async def listener_failure() -> None:
            raise RuntimeError("synthetic listener failure")

        worker.listener.run = listener_failure  # type: ignore[method-assign]
        with self.assertRaisesRegex(RuntimeError, "synthetic listener failure"):
            await asyncio.wait_for(worker.run(), timeout=3)


if __name__ == "__main__":
    unittest.main()
