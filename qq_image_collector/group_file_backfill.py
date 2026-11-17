"""One-time backfill of image files already sitting in monitored groups' file lists.

Live collection only sees group files posted after 2026-11-18 (see
EVENT_PIPELINE.md).  Image files are uploaded as permanent group files
(busid 102, no dead_time), so the older ones can still be fetched.  This walks
each enabled group's file list, asks NapCat for a download link per image
file, and runs it through the same downloader the worker uses.

It is an operator tool, not part of the worker:

* It uses three NapCat actions the worker never calls -- the two list actions
  and ``get_group_file_url`` -- through its own allowlisted client.  NapCat
  already calls the same GetGroupFileUrl for every live file message.
* A row is inserted already ``downloading`` and finished in the same run, so
  the worker never claims one.  Rows have no message anchor, so they are
  marked to fail fast instead of costing a history call if one is ever left
  behind.
* Disk is the limit: the image volume keeps 14 days locally and runs close
  to full.  A run stops before free space falls under ``--min-free-bytes`` or
  after ``--max-stored-bytes`` of new files, and records which days it wrote
  to, so a driver can let the archive move those days to R2 before the next
  run.  Rerunning skips everything already finished and retries what expired.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Callable

import httpx

from .config import load_settings, runtime_value
from .database import (
    connect_database,
    enqueue_image,
    finish_image,
    get_runtime_state,
)
from .downloader import CdnDownloader, CdnHttpError, DownloadPolicyError, validate_cdn_url
from .events import is_image_file_name
from .onebot import OneBotError, OneBotPolicyError, _read_token


ALLOWED_ACTIONS = frozenset(
    {"get_group_root_files", "get_group_files_by_folder", "get_group_file_url"}
)
KEY_PREFIX = "groupfile:"
# A large group's list can take NapCat longer than its 5 s page timeout when
# asked for everything at once; a smaller page count gets through.
LIST_COUNTS = (4000, 1500)
DAY_TIMEZONE = dt.timezone(dt.timedelta(hours=8))
GIVE_UP_AFTER_RUNS = 2
STATE_FILE = "group_file_backfill.json"


class FileListClient:
    """NapCat HTTP client limited to the group-file actions this tool needs."""

    def __init__(self, base_url: str, token: str, *, timeout: float = 120) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.client = httpx.AsyncClient(timeout=timeout)

    async def close(self) -> None:
        await self.client.aclose()

    async def call(self, action: str, **params: Any) -> Any:
        if action not in ALLOWED_ACTIONS:
            raise OneBotPolicyError(f"group file backfill may not call {action}")
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        try:
            response = await self.client.post(
                f"{self.base_url}/{action}", json=params, headers=headers
            )
            response.raise_for_status()
            body = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise OneBotError(f"{action} request failed: {exc}") from exc
        if not isinstance(body, dict) or body.get("status") != "ok":
            detail = body.get("message") or body.get("wording") if isinstance(body, dict) else body
            raise OneBotError(f"{action} failed: {detail}")
        return body.get("data")


def file_key(group_id: str, entry: dict[str, Any]) -> str:
    """A stable message key for a listed file.

    NapCat's ``file_id`` is a token from an in-memory cache that changes on
    every listing, so identity comes from what the list reports stably.
    """

    material = "\0".join(
        (
            str(group_id),
            str(entry.get("file_name") or ""),
            str(int(entry.get("size") or 0)),
            str(int(entry.get("modify_time") or 0)),
        )
    )
    return KEY_PREFIX + hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]


def sent_day(timestamp: int) -> str:
    return dt.datetime.fromtimestamp(int(timestamp), DAY_TIMEZONE).strftime("%Y-%m-%d")


async def list_group_files(
    client: FileListClient,
    group_id: str,
    *,
    pause: float = 2.0,
    sleep: Callable[[float], Any] = asyncio.sleep,
) -> list[dict[str, Any]]:
    root: dict[str, Any] | None = None
    last_error: Exception | None = None
    for count in LIST_COUNTS:
        try:
            root = await client.call("get_group_root_files", group_id=str(group_id), file_count=count)
            break
        except OneBotError as exc:
            last_error = exc
            await sleep(10)
    if root is None:
        raise OneBotError(f"group {group_id} file list failed: {last_error}")
    files = _own_entries(group_id, (root or {}).get("files"))
    for folder in _own_entries(group_id, (root or {}).get("folders")):
        await sleep(pause)
        folder_id = str(folder.get("folder_id") or folder.get("folder") or "")
        if not folder_id:
            continue
        listed = None
        for count in LIST_COUNTS:
            try:
                listed = await client.call(
                    "get_group_files_by_folder",
                    group_id=str(group_id),
                    folder_id=folder_id,
                    file_count=count,
                )
                break
            except OneBotError as exc:
                last_error = exc
                await sleep(10)
        if listed is None:
            raise OneBotError(f"group {group_id} folder list failed: {last_error}")
        files.extend(_own_entries(group_id, listed.get("files")))
    return files


def _own_entries(group_id: str, entries: Any) -> list[dict[str, Any]]:
    """Keep only entries NapCat reports for the group that was asked about.

    NapCat 4.18 waits for the kernel's next onGroupFileInfoUpdate without
    checking which group it is for, so after a timeout the late answer to
    one group's request is handed to the next request.  Each entry still
    carries its real group, which is what this checks.
    """

    entries = [entry for entry in entries or [] if isinstance(entry, dict)]
    own = [entry for entry in entries if str(entry.get("group_id") or group_id) == str(group_id)]
    if entries and not own:
        raise OneBotError(f"group {group_id} list answer belonged to another group")
    return own


class GroupFileBackfill:
    def __init__(
        self,
        connection: sqlite3.Connection,
        client: FileListClient,
        downloader: CdnDownloader,
        storage_root: Path,
        *,
        max_bytes: int,
        refresh_max_failures: int,
        min_free_bytes: int,
        max_stored_bytes: int,
        max_files: int = 0,
        pause: float = 2.0,
        state_path: Path | None = None,
        sleep: Callable[[float], Any] = asyncio.sleep,
        free_bytes: Callable[[Path], int] | None = None,
        log: Callable[[str], None] | None = None,
    ) -> None:
        self.connection = connection
        self.client = client
        self.downloader = downloader
        self.storage_root = storage_root
        self.max_bytes = int(max_bytes)
        self.refresh_max_failures = int(refresh_max_failures)
        self.min_free_bytes = int(min_free_bytes)
        self.max_stored_bytes = int(max_stored_bytes)
        self.max_files = int(max_files)
        self.pause = float(pause)
        self.state_path = state_path
        self.sleep = sleep
        self.free_bytes = free_bytes or (lambda path: shutil.disk_usage(path).free)
        self.log = log or (lambda message: print(message, flush=True))
        self.stats: dict[str, int] = {
            "listed_images": 0,
            "skipped_known": 0,
            "skipped_too_large": 0,
            "processed": 0,
            "accepted": 0,
            "duplicate": 0,
            "rejected": 0,
            "filtered_gif": 0,
            "expired": 0,
            "failed": 0,
            "url_calls": 0,
            "stored_bytes": 0,
        }
        self.touched_days: dict[str, int] = {}
        self.stop_reason: str | None = None
        self._live = self._live_group_files()
        self._finished = self._backfill_status()

    # -- selection -----------------------------------------------------------

    def _live_group_files(self) -> set[tuple[str, str, int]]:
        return {
            (str(group_id), str(name or ""), int(size or 0))
            for group_id, name, size in self.connection.execute(
                """
                SELECT group_id, file_token, declared_size FROM images
                WHERE message_id NOT LIKE ? AND resolver_json LIKE '%group_file%'
                """,
                (KEY_PREFIX + "%",),
            )
        }

    def _backfill_status(self) -> dict[tuple[str, str], str]:
        status_by_key = {}
        for group_id, message_id, status, attempts in self.connection.execute(
            "SELECT group_id, message_id, status, attempts FROM images WHERE message_id LIKE ?",
            (KEY_PREFIX + "%",),
        ):
            # Each run claims a file at most twice (the link, then one fresh
            # link), so an expired row past this has failed two runs.
            if status == "expired" and int(attempts or 0) >= 2 * GIVE_UP_AFTER_RUNS:
                status = "given_up"
            status_by_key[(str(group_id), str(message_id))] = str(status)
        return status_by_key

    def wanted(self, group_id: str, entry: dict[str, Any]) -> bool:
        name = str(entry.get("file_name") or "")
        if not is_image_file_name(name):
            return False
        self.stats["listed_images"] += 1
        size = int(entry.get("size") or 0)
        if size <= 0 or size > self.max_bytes:
            self.stats["skipped_too_large"] += 1
            return False
        status = self._finished.get((str(group_id), file_key(group_id, entry)))
        if status is not None and status != "expired":
            self.stats["skipped_known"] += 1
            return False
        if (str(group_id), name, size) in self._live:
            self.stats["skipped_known"] += 1
            return False
        return True

    # -- one file ------------------------------------------------------------

    def _item(
        self,
        group_id: str,
        group_name: str | None,
        entry: dict[str, Any],
        url: str,
    ) -> dict[str, Any]:
        try:
            host = (httpx.URL(url).host or "").lower()
        except (httpx.InvalidURL, TypeError):
            host = ""
        sent_at = int(entry.get("modify_time") or entry.get("upload_time") or 0)
        resolver_data = {
            "source_kind": "group_file",
            "history_source": "group-file-backfill",
            "url": url,
            "origin_url": "",
            "url_host": host,
            "origin_url_host": "",
            "data_url_has_rkey": False,
            "origin_url_has_rkey": False,
            "url_expires_at": None,
            "url_expiry_basis": None,
            "raw_message_id": None,
            "raw_message_seq": None,
            "raw_match": "file-list",
            "file_busid": entry.get("busid"),
            "file_dead_time": int(entry.get("dead_time") or 0),
            "width": 0,
            "height": 0,
            "md5": "",
            "summary": "",
            "sub_type": None,
            "emoji_signal": False,
            "url_refreshed": False,
            # There is no message to refresh this link from.  If the worker
            # ever handles a row left behind, these make its first refresh
            # attempt give up instead of sending get_group_msg_history a
            # made-up anchor.
            "url_refresh_attempted": True,
            "url_refresh_failures": max(0, self.refresh_max_failures - 1),
        }
        return {
            "group_id": str(group_id),
            "message_id": file_key(group_id, entry),
            "message_seq": "",
            "sent_at": sent_at,
            "image_index": 0,
            "file": str(entry.get("file_name") or ""),
            "declared_size": int(entry.get("size") or 0),
            "resolver_data": resolver_data,
            "original_flag": 1,
            "group_uin": str(group_id),
            "group_name": group_name,
            "sender_uin": str(entry.get("uploader") or "") or None,
            "sender_uid": None,
            "sender_member_name": None,
            "sender_nickname": str(entry.get("uploader_name") or "") or None,
            "sender_remark_name": None,
            "message_text": None,
            "discovered_at": int(time.time()),
        }

    def _claim_row(self, item: dict[str, Any]) -> sqlite3.Row:
        key = (item["group_id"], item["message_id"], 0)
        now = int(time.time())
        existing = self.connection.execute(
            "SELECT status FROM images WHERE group_id=? AND message_id=? AND image_index=?",
            key,
        ).fetchone()
        if existing is None:
            enqueue_image(self.connection, item, commit=False)
        else:
            # A retry of an expired row: its stored resolver lost the URL when
            # it was finished, so put the fresh one back explicitly.
            resolver = dict(item["resolver_data"])
            self.connection.execute(
                """
                UPDATE images SET resolver_json=?, url_expires_at=0, error=NULL,
                    sha256=NULL, local_path=NULL, metadata_source=NULL,
                    next_retry_at=0, collected_at=NULL
                WHERE group_id=? AND message_id=? AND image_index=?
                """,
                (json.dumps(resolver, ensure_ascii=False), *key),
            )
        # Same transaction as the insert: the worker only claims queued or
        # deferred rows, so it never sees this one.
        self.connection.execute(
            """
            UPDATE images SET status='downloading', attempts=attempts+1, updated_at=?
            WHERE group_id=? AND message_id=? AND image_index=?
            """,
            (now, *key),
        )
        self.connection.commit()
        row = self.connection.execute(
            "SELECT * FROM images WHERE group_id=? AND message_id=? AND image_index=?",
            key,
        ).fetchone()
        if row is None:
            raise RuntimeError("backfill row vanished after insert")
        return row

    async def _file_url(self, group_id: str, entry: dict[str, Any]) -> str:
        self.stats["url_calls"] += 1
        data = await self.client.call(
            "get_group_file_url", group_id=str(group_id), file_id=str(entry.get("file_id") or "")
        )
        url = str((data or {}).get("url") or "")
        return validate_cdn_url(url)

    def _finish_failed(self, row: sqlite3.Row, status: str, error: str, http_status: int | None = None) -> None:
        finish_image(self.connection, row, status=status, error=error, http_status=http_status)
        self.stats["expired" if status == "expired" else "failed"] += 1

    async def process_file(self, group_id: str, group_name: str | None, entry: dict[str, Any]) -> str:
        try:
            url = await self._file_url(group_id, entry)
        except DownloadPolicyError as exc:
            row = self._claim_row(self._item(group_id, group_name, entry, ""))
            self._finish_failed(row, "failed_terminal", f"group file backfill: {exc}")
            return "failed"
        row = self._claim_row(self._item(group_id, group_name, entry, url))
        refetched = False
        network_failures = 0
        while True:
            try:
                result = await self.downloader.process(row)
                break
            except CdnHttpError as exc:
                if refetched:
                    self._finish_failed(
                        row, "expired", f"group file backfill: file server HTTP {exc.status_code}",
                        exc.status_code,
                    )
                    return "expired"
                refetched = True
                try:
                    url = await self._file_url(group_id, entry)
                except (OneBotError, DownloadPolicyError) as refetch_error:
                    self._finish_failed(
                        row, "expired",
                        f"group file backfill: link refetch failed: {type(refetch_error).__name__}",
                        exc.status_code,
                    )
                    return "expired"
                row = self._claim_row(self._item(group_id, group_name, entry, url))
            except DownloadPolicyError as exc:
                self._finish_failed(row, "failed_terminal", f"group file backfill: {exc}")
                return "failed"
            except (httpx.HTTPError, asyncio.TimeoutError, TimeoutError) as exc:
                network_failures += 1
                if network_failures >= 3:
                    self._finish_failed(
                        row, "expired", f"group file backfill: {type(exc).__name__} x{network_failures}"
                    )
                    return "expired"
                await self.sleep(10 * network_failures)
            except BaseException:
                # Never leave a downloading row behind for the worker.
                self._finish_failed(row, "expired", "group file backfill interrupted; rerun retries")
                raise
        if result in self.stats:
            self.stats[result] += 1
        if result == "accepted":
            # store_asset also rewrites a known image whose local copy was
            # already archived away, so "accepted" means bytes hit the disk.
            # The archive files an asset under its canonical date, while the
            # file sits in the directory of this row's date; both may need
            # the archive's attention before the space comes back.
            stored = self.connection.execute(
                """
                SELECT i.local_path, a.canonical_sent_at FROM images i
                LEFT JOIN assets a ON a.sha256 = i.sha256
                WHERE i.group_id=? AND i.message_id=? AND i.image_index=0
                """,
                (row["group_id"], row["message_id"]),
            ).fetchone()
            now = int(time.time())
            if stored and stored[0]:
                path = Path(str(stored[0]))
                try:
                    self.stats["stored_bytes"] += path.stat().st_size
                except OSError:
                    pass
                self.touched_days[path.parent.name] = now
            canonical = int((stored[1] if stored else 0) or row["sent_at"] or 0)
            if canonical:
                self.touched_days[sent_day(canonical)] = now
        return result

    # -- the run -------------------------------------------------------------

    def _budget_stop(self, upcoming_size: int) -> str | None:
        if self.max_files and self.stats["processed"] >= self.max_files:
            return "max_files"
        if self.max_stored_bytes and self.stats["stored_bytes"] >= self.max_stored_bytes:
            return "max_stored_bytes"
        free = int(self.free_bytes(self.storage_root))
        if free - upcoming_size < self.min_free_bytes:
            return "min_free_bytes"
        return None

    def sweep_interrupted(self) -> int:
        """Finish rows a killed run left mid-download, so a rerun retries them."""

        rows = self.connection.execute(
            """
            SELECT * FROM images
            WHERE message_id LIKE ? AND status IN ('queued','deferred','downloading')
            """,
            (KEY_PREFIX + "%",),
        ).fetchall()
        for row in rows:
            finish_image(
                self.connection,
                row,
                status="expired",
                error="group file backfill interrupted; rerun retries",
            )
            self._finished[(str(row["group_id"]), str(row["message_id"]))] = "expired"
        return len(rows)

    async def run(self, groups: list[tuple[str, str | None]], *, dry_run: bool = False) -> dict[str, Any]:
        swept = 0 if dry_run else self.sweep_interrupted()
        remaining = 0
        for group_id, group_name in groups:
            if self.stop_reason:
                break
            try:
                listing = await list_group_files(self.client, group_id, pause=self.pause, sleep=self.sleep)
            except OneBotError as exc:
                self.log(f"{group_id}: listing failed, skipped this run: {exc}")
                remaining += 1
                continue
            todo = [entry for entry in listing if self.wanted(group_id, entry)]
            todo.sort(key=lambda entry: int(entry.get("modify_time") or 0))
            self.log(f"{group_id}: {len(listing)} files, {len(todo)} image files to fetch")
            if dry_run:
                remaining += len(todo)
                continue
            for index, entry in enumerate(todo):
                reason = self._budget_stop(int(entry.get("size") or 0))
                if reason:
                    self.stop_reason = reason
                    remaining += len(todo) - index
                    break
                try:
                    result = await self.process_file(group_id, group_name, entry)
                except OneBotError as exc:
                    # Usually the listing token fell out of NapCat's cache;
                    # the next run lists again.
                    self.log(f"{group_id}: no link for a file ({exc}); left for the next run")
                    remaining += 1
                    continue
                self.stats["processed"] += 1
                self._finished[(str(group_id), file_key(group_id, entry))] = result
                if self.stats["processed"] % 25 == 0:
                    self.log(f"progress {json.dumps(self.stats, ensure_ascii=False)}")
                    self.save_state(partial=True)
                await self.sleep(self.pause)
        summary = {
            "finished_at": int(time.time()),
            "dry_run": dry_run,
            "stop_reason": self.stop_reason or ("listed" if dry_run else "done"),
            "remaining": remaining,
            "swept_interrupted": swept,
            "stats": dict(self.stats),
            "touched_days": dict(sorted(self.touched_days.items())),
        }
        if not dry_run:
            self.save_state(partial=False, summary=summary)
        return summary

    def save_state(self, *, partial: bool, summary: dict[str, Any] | None = None) -> None:
        if self.state_path is None:
            return
        try:
            state = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            state = {}
        days = dict(state.get("touched_days") or {})
        for day, at in self.touched_days.items():
            days[day] = max(int(days.get(day) or 0), int(at))
        state["touched_days"] = dict(sorted(days.items()))
        state["updated_at"] = int(time.time())
        state["running"] = partial
        if summary is not None:
            state["last_run"] = summary
        temporary = self.state_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")
        os.replace(temporary, self.state_path)


def enabled_groups(connection: sqlite3.Connection, only: list[str] | None) -> list[tuple[str, str | None]]:
    rows = connection.execute(
        "SELECT group_id, display_name FROM monitored_groups WHERE enabled=1 ORDER BY group_id"
    ).fetchall()
    groups = [(str(group_id), name) for group_id, name in rows]
    if only:
        wanted = {str(value) for value in only}
        groups = [group for group in groups if group[0] in wanted]
    return groups


async def main_async(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--config", default=os.environ.get("QQ_IMAGE_COLLECTOR_CONFIG",
                        "/data/qq-image-collector/config/collector_config.json"))
    parser.add_argument("--group", action="append", help="limit to this group (repeatable)")
    parser.add_argument("--dry-run", action="store_true", help="list and count only")
    parser.add_argument("--min-free-bytes", type=float, default=3 * 1024**3)
    parser.add_argument("--max-stored-bytes", type=float, default=0, help="0 = no limit")
    parser.add_argument("--max-files", type=int, default=0)
    parser.add_argument("--pause", type=float, default=2.0, help="seconds between files")
    args = parser.parse_args(argv)

    settings = load_settings(args.config)
    storage_root = Path(settings["storage"]["root"])
    connection = connect_database(settings["storage"]["database"])
    connection.row_factory = sqlite3.Row
    onebot = settings["onebot"]
    client = FileListClient(str(onebot.get("base_url") or ""), _read_token(onebot))
    max_bytes = int(runtime_value(settings, "max_download_bytes"))
    downloader = CdnDownloader(connection, storage_root, max_bytes=max_bytes, daily_limit=0)
    backfill = GroupFileBackfill(
        connection,
        client,
        downloader,
        storage_root,
        max_bytes=max_bytes,
        refresh_max_failures=int(runtime_value(settings, "url_refresh_max_failures")),
        min_free_bytes=int(args.min_free_bytes),
        max_stored_bytes=int(args.max_stored_bytes),
        max_files=args.max_files,
        pause=args.pause,
        state_path=Path(settings["storage"]["database"]).parent / STATE_FILE,
    )
    try:
        summary = await backfill.run(enabled_groups(connection, args.group), dry_run=args.dry_run)
    finally:
        await downloader.close()
        await client.close()
        connection.close()
    # The last line is the machine-readable result a driver parses.
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(main_async(argv))


if __name__ == "__main__":
    sys.exit(main())
