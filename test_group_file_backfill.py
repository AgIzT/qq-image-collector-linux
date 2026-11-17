from __future__ import annotations

import asyncio
import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

import httpx

from qq_image_collector.database import connect_database, enqueue_image
from qq_image_collector.downloader import CdnDownloader
from qq_image_collector.events import parse_group_event
from qq_image_collector.group_file_backfill import (
    KEY_PREFIX,
    FileListClient,
    GroupFileBackfill,
    file_key,
)
from qq_image_collector.worker import CollectorWorker
from test_event_pipeline import GROUP, a1111_png, file_event
from test_worker_integration import write_config


def listed(name: str, *, size: int = 2_000_000, modified: int = 1_780_000_000, token: str | None = None) -> dict:
    return {
        "group_id": int(GROUP),
        "file_id": token or name.encode().hex()[:32],
        "file_name": name,
        "busid": 102,
        "size": size,
        "file_size": size,
        "upload_time": 0,
        "dead_time": 0,
        "modify_time": modified,
        "uploader": 300000003,
        "uploader_name": "uploader",
    }


class FakeNapCat(FileListClient):
    """Serves one group's file list and a link per file token."""

    def __init__(self, root: list[dict], folders: dict[str, list[dict]] | None = None) -> None:
        self.root = root
        self.folders = folders or {}
        self.calls: list[tuple[str, dict]] = []
        self.links: dict[str, list[str]] = {}

    async def close(self) -> None:
        return None

    async def call(self, action: str, **params):
        self.calls.append((action, params))
        if action == "get_group_root_files":
            return {
                "files": self.root,
                "folders": [{"folder_id": key, "folder_name": key} for key in self.folders],
            }
        if action == "get_group_files_by_folder":
            return {"files": self.folders[params["folder_id"]], "folders": []}
        if action == "get_group_file_url":
            queue = self.links.get(params["file_id"]) or []
            url = queue.pop(0) if len(queue) > 1 else (queue[0] if queue else "")
            return {"url": url or f"https://njc-download.ftn.qq.com/ftn_handler/{params['file_id']}/?fname="}
        raise AssertionError(action)

    def url_calls(self) -> int:
        return sum(1 for action, _ in self.calls if action == "get_group_file_url")


class GroupFileBackfillTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.connection = connect_database(self.root / "state" / "state.sqlite3")
        self.connection.row_factory = sqlite3.Row
        self.served: list[str] = []
        self.status_for: dict[str, int] = {}

    def tearDown(self) -> None:
        self.connection.close()
        self.temporary.cleanup()

    def backfill(self, napcat: FakeNapCat, *, free: int = 10**12, **options) -> GroupFileBackfill:
        async def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            self.served.append(url)
            for marker, status in self.status_for.items():
                if marker in url:
                    return httpx.Response(status)
            return httpx.Response(200, content=a1111_png())

        downloader = CdnDownloader(self.connection, self.root, max_bytes=50 * 1024 * 1024, daily_limit=0)
        asyncio.run(downloader.client.aclose())
        downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        self.downloader = downloader

        async def no_sleep(_seconds: float) -> None:
            return None

        return GroupFileBackfill(
            self.connection,
            napcat,
            downloader,
            self.root,
            max_bytes=50 * 1024 * 1024,
            refresh_max_failures=5,
            min_free_bytes=options.pop("min_free_bytes", 0),
            max_stored_bytes=options.pop("max_stored_bytes", 0),
            pause=0,
            state_path=self.root / "state" / "group_file_backfill.json",
            sleep=no_sleep,
            free_bytes=lambda _path: free,
            log=lambda _message: None,
            **options,
        )

    def run_backfill(self, backfill: GroupFileBackfill) -> dict:
        async def scenario() -> dict:
            try:
                return await backfill.run([(GROUP, "synthetic group")])
            finally:
                await self.downloader.close()

        return asyncio.run(scenario())

    def rows(self) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM images WHERE message_id LIKE ? ORDER BY sent_at", (KEY_PREFIX + "%",)
        ).fetchall()

    def test_image_files_are_fetched_once_and_recorded_like_live_ones(self) -> None:
        napcat = FakeNapCat(
            [
                listed("old.png", modified=1_700_000_000),
                listed("model.safetensors", size=200_000_000),
                listed("huge.png", size=60 * 1024 * 1024),
                listed("live.png"),
            ],
            folders={"/f1": [listed("in-folder.JPG", modified=1_750_000_000)]},
        )
        # live.png already arrived through a live file message.
        event = file_event(name="live.png")
        event["message"][0]["data"]["file_size"] = "2000000"
        _cursor, items = parse_group_event(event)
        enqueue_image(self.connection, items[0])

        summary = self.run_backfill(self.backfill(napcat))
        self.assertEqual(summary["stop_reason"], "done")
        self.assertEqual(summary["stats"]["listed_images"], 4)
        self.assertEqual(summary["stats"]["skipped_too_large"], 1)
        self.assertEqual(summary["stats"]["skipped_known"], 1)
        self.assertEqual(summary["stats"]["processed"], 2)
        self.assertEqual(napcat.url_calls(), 2)

        rows = self.rows()
        self.assertEqual([row["file_token"] for row in rows], ["old.png", "in-folder.JPG"])
        first = rows[0]
        self.assertEqual(first["status"], "accepted")
        self.assertEqual(first["message_id"], file_key(GROUP, listed("old.png", modified=1_700_000_000)))
        self.assertEqual(first["sent_at"], 1_700_000_000)
        self.assertEqual(first["sender_uin"], "300000003")
        self.assertEqual(first["group_name"], "synthetic group")
        resolver = json.loads(first["resolver_json"])
        self.assertNotIn("url", resolver)
        self.assertEqual(resolver["source_kind"], "group_file")
        self.assertEqual(resolver["history_source"], "group-file-backfill")
        self.assertTrue(Path(first["local_path"]).is_file())
        # Both files hash the same, so the second is the same asset.
        self.assertEqual(rows[1]["status"], "accepted")
        self.assertEqual(summary["stats"]["duplicate"], 1)

        state = json.loads((self.root / "state" / "group_file_backfill.json").read_text("utf-8"))
        self.assertIn("2023-11-15", state["touched_days"])
        self.assertFalse(state["running"])

        # A second run lists again but fetches nothing it already finished.
        again = self.run_backfill(self.backfill(napcat))
        self.assertEqual(again["stats"]["processed"], 0)
        self.assertEqual(napcat.url_calls(), 2)

    def test_dead_link_is_fetched_again_then_given_up_and_retried_later(self) -> None:
        napcat = FakeNapCat([listed("a.png", token="aaaa"), listed("b.png", token="bbbb")])
        napcat.links["aaaa"] = [
            "https://njc-download.ftn.qq.com/ftn_handler/stale-a/?fname=",
            "https://njc-download.ftn.qq.com/ftn_handler/fresh-a/?fname=",
        ]
        napcat.links["bbbb"] = ["https://njc-download.ftn.qq.com/ftn_handler/stale-b/?fname="]
        self.status_for = {"stale": 403}

        summary = self.run_backfill(self.backfill(napcat))
        statuses = {row["file_token"]: row["status"] for row in self.rows()}
        self.assertEqual(statuses, {"a.png": "accepted", "b.png": "expired"})
        self.assertEqual(summary["stats"]["expired"], 1)
        self.assertEqual(napcat.url_calls(), 4)

        self.status_for = {}
        retry = self.run_backfill(self.backfill(napcat))
        self.assertEqual(retry["stats"]["processed"], 1)
        statuses = {row["file_token"]: row["status"] for row in self.rows()}
        self.assertEqual(statuses, {"a.png": "accepted", "b.png": "accepted"})

    def test_another_groups_list_answer_is_never_used(self) -> None:
        foreign = listed("foreign.png") | {"group_id": 999}
        napcat = FakeNapCat([foreign, listed("own.png")])
        summary = self.run_backfill(self.backfill(napcat))
        self.assertEqual([row["file_token"] for row in self.rows()], ["own.png"])

        # An answer made only of another group's files means the request and
        # its answer got crossed: skip the group this run, fetch nothing.
        napcat = FakeNapCat([foreign])
        summary = self.run_backfill(self.backfill(napcat))
        self.assertEqual(summary["remaining"], 1)
        self.assertEqual(napcat.url_calls(), 0)

    def test_run_stops_before_the_disk_reserve(self) -> None:
        napcat = FakeNapCat([listed("a.png"), listed("b.png", modified=1_780_000_100)])
        summary = self.run_backfill(
            self.backfill(napcat, free=3_000_000_000, min_free_bytes=2_999_000_000)
        )
        self.assertEqual(summary["stop_reason"], "min_free_bytes")
        self.assertEqual(summary["remaining"], 2)
        self.assertEqual(napcat.url_calls(), 0)
        self.assertEqual(self.rows(), [])

    def test_interrupted_rows_are_closed_and_retried(self) -> None:
        napcat = FakeNapCat([listed("a.png")])
        backfill = self.backfill(napcat)
        item = backfill._item(GROUP, None, listed("a.png"), "https://njc-download.ftn.qq.com/ftn_handler/x/?fname=")
        backfill._claim_row(item)
        self.assertEqual(self.rows()[0]["status"], "downloading")

        summary = self.run_backfill(self.backfill(napcat))
        self.assertEqual(summary["swept_interrupted"], 1)
        self.assertEqual(self.rows()[0]["status"], "accepted")


class LeftoverRowTests(unittest.IsolatedAsyncioTestCase):
    async def test_worker_gives_up_a_leftover_row_without_a_history_call(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        worker = CollectorWorker(write_config(Path(temporary.name), "ws://127.0.0.1:9"))

        async def dead(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(403)

        await worker.downloader.client.aclose()
        worker.downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(dead))
        backfill = GroupFileBackfill(
            worker.connection, FakeNapCat([]), worker.downloader, Path(temporary.name),
            max_bytes=1024 * 1024, refresh_max_failures=5, min_free_bytes=0, max_stored_bytes=0,
        )
        item = backfill._item(GROUP, None, listed("a.png"), "https://njc-download.ftn.qq.com/ftn_handler/x/?fname=")
        enqueue_image(worker.connection, item)
        history_calls = 0

        async def history(_action: str, _params: dict) -> dict:
            nonlocal history_calls
            history_calls += 1
            return {"messages": []}

        worker.onebot.call_async = history  # type: ignore[method-assign]
        from qq_image_collector.database import claim_next_image

        await worker._process_claimed(claim_next_image(worker.connection))
        status = worker.connection.execute("SELECT status FROM images").fetchone()[0]
        self.assertEqual(status, "failed_terminal")
        self.assertEqual(history_calls, 0)
        await worker.downloader.close()
        worker.connection.close()


if __name__ == "__main__":
    unittest.main()
