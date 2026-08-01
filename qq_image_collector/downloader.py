from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from metadata_reader import extension_for_format, inspect_image

from .database import (
    counter_sum,
    finish_image,
    increment_counter,
    local_day_start,
    sha256_file,
    store_asset,
)


ALLOWED_CDN_HOSTS = frozenset({"gchat.qpic.cn", "multimedia.nt.qq.com.cn"})
GIF_SIGNATURES = (b"GIF87a", b"GIF89a")


class DownloadError(RuntimeError):
    pass


class DownloadPolicyError(DownloadError):
    pass


class CdnHttpError(DownloadError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = int(status_code)


class GifDetected(DownloadError):
    pass


def resolver_data(row: sqlite3.Row) -> dict[str, Any]:
    try:
        value = json.loads(str(row["resolver_json"] or "{}"))
    except (TypeError, ValueError):
        value = {}
    return value if isinstance(value, dict) else {}


def candidate_urls(row: sqlite3.Row, preference: str = "data") -> list[str]:
    data = resolver_data(row)
    if preference == "raw":
        candidates = (data.get("origin_url"), data.get("url"))
    else:
        candidates = (data.get("url"), data.get("origin_url"))
    result: list[str] = []
    for value in candidates:
        url = str(value or "").strip()
        if not url or url in result:
            continue
        try:
            result.append(validate_cdn_url(url))
        except DownloadPolicyError:
            continue
    return result


def selected_url(row: sqlite3.Row, preference: str = "data") -> str:
    candidates = candidate_urls(row, preference)
    return candidates[0] if candidates else ""


def validate_cdn_url(url: str) -> str:
    try:
        parsed = urlsplit(url)
    except ValueError as exc:
        raise DownloadPolicyError(f"invalid CDN URL: {exc}") from exc
    hostname = (parsed.hostname or "").lower()
    if parsed.scheme != "https" or hostname not in ALLOWED_CDN_HOSTS:
        raise DownloadPolicyError(f"CDN URL is not allowed: {parsed.scheme}://{hostname}")
    if parsed.username or parsed.password or parsed.port not in {None, 443}:
        raise DownloadPolicyError("CDN URL contains forbidden authority fields")
    return url


class CdnDownloader:
    def __init__(
        self,
        connection: sqlite3.Connection,
        storage_root: Path,
        *,
        max_bytes: int,
        daily_limit: int,
        url_preference: str = "data",
        timeout_seconds: int = 120,
    ) -> None:
        self.connection = connection
        self.storage_root = storage_root
        self.max_bytes = int(max_bytes)
        self.daily_limit = int(daily_limit)
        self.url_preference = url_preference
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds, connect=20),
            follow_redirects=False,
            headers={"User-Agent": "Mozilla/5.0"},
        )

    async def close(self) -> None:
        await self.client.aclose()

    def daily_remaining(self) -> int:
        used = counter_sum(self.connection, "cdn_requests", local_day_start())
        return max(0, self.daily_limit - used)

    async def _stream_to_temp(self, url: str) -> tuple[Path, int, bytes]:
        url = validate_cdn_url(url)
        temp_root = self.storage_root / "temp"
        temp_root.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix="qq-cdn-", suffix=".part", dir=temp_root)
        os.close(fd)
        path = Path(name)
        size = 0
        prefix = bytearray()
        # Count every outbound CDN request, including 403/429/timeouts.  The
        # daily guard is an operational runaway limiter, while cdn_downloads
        # below counts only complete non-empty 200 responses.
        increment_counter(self.connection, "cdn_requests")
        try:
            async with self.client.stream("GET", url) as response:
                if response.status_code != 200:
                    if response.status_code == 403:
                        increment_counter(self.connection, "cdn_403")
                    elif response.status_code == 429:
                        increment_counter(self.connection, "cdn_429")
                    raise CdnHttpError(response.status_code, f"QQ CDN returned HTTP {response.status_code}")
                length = int(response.headers.get("content-length") or 0)
                if length > self.max_bytes:
                    raise DownloadPolicyError(f"image exceeds {self.max_bytes} bytes")

                def write_chunk(handle: Any, chunk: bytes) -> None:
                    nonlocal size
                    if not chunk:
                        return
                    if len(prefix) < 16:
                        prefix.extend(chunk[: 16 - len(prefix)])
                        if len(prefix) >= 6 and bytes(prefix).startswith(GIF_SIGNATURES):
                            raise GifDetected("excluded image format: GIF")
                    size += len(chunk)
                    if size > self.max_bytes:
                        raise DownloadPolicyError(f"image exceeds {self.max_bytes} bytes")
                    handle.write(chunk)

                with path.open("wb") as handle:
                    if response.is_stream_consumed:
                        # Mock/in-process transports may hand back an already buffered
                        # response.  Production HTTP responses take the raw streaming
                        # branch below so Content-Encoding is never decoded silently.
                        write_chunk(handle, response.content)
                    else:
                        async for chunk in response.aiter_raw():
                            write_chunk(handle, chunk)
            if size <= 0:
                raise DownloadError("QQ CDN returned an empty body")
            increment_counter(self.connection, "cdn_downloads")
            increment_counter(self.connection, "cdn_bytes", size)
            return path, size, bytes(prefix)
        except BaseException:
            path.unlink(missing_ok=True)
            raise

    async def process(self, row: sqlite3.Row) -> str:
        if self.daily_remaining() <= 0:
            raise DownloadPolicyError("daily CDN download limit reached")
        urls = candidate_urls(row, self.url_preference)
        if not urls:
            finish_image(
                self.connection,
                row,
                status="expired",
                error="image event contains no allowed CDN URL",
            )
            increment_counter(self.connection, "expired")
            return "expired"

        temp_path: Path | None = None
        try:
            try:
                for index, url in enumerate(urls):
                    try:
                        temp_path, _size, _prefix = await self._stream_to_temp(url)
                        break
                    except CdnHttpError as exc:
                        if exc.status_code in {403, 404, 410} and index + 1 < len(urls):
                            continue
                        raise
            except GifDetected:
                finish_image(
                    self.connection,
                    row,
                    status="filtered_gif",
                    error="excluded image format: GIF",
                )
                increment_counter(self.connection, "filtered_gif")
                return "filtered_gif"

            result = await asyncio.to_thread(inspect_image, temp_path)
            digest = await asyncio.to_thread(sha256_file, temp_path)
            if not result.accepted:
                finish_image(
                    self.connection,
                    row,
                    status="rejected_no_metadata",
                    sha256=digest,
                    error=None,
                )
                increment_counter(self.connection, "rejected")
                return "rejected"

            item = dict(row)
            metadata_json = json.dumps(result.fields, ensure_ascii=False)
            destination, duplicate = store_asset(
                self.connection,
                temp_path,
                digest=digest,
                extension=extension_for_format(result.image_format),
                source=result.source,
                metadata_json=metadata_json,
                width=result.width,
                height=result.height,
                image_format=result.image_format,
                item=item,
                storage_root=self.storage_root,
            )
            temp_path = None
            finish_image(
                self.connection,
                row,
                status="accepted",
                sha256=digest,
                local_path=str(destination),
                metadata_source=result.source,
                metadata_json=metadata_json,
            )
            increment_counter(self.connection, "accepted")
            if duplicate:
                increment_counter(self.connection, "duplicates")
            return "duplicate" if duplicate else "accepted"
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)
