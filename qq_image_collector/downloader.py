from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import tempfile
import time
import zlib
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import httpx
from PIL import Image, UnidentifiedImageError

from metadata_reader import extension_for_format, inspect_image

from .database import (
    finish_image,
    increment_counter,
    sha256_file,
    store_asset,
)


ALLOWED_CDN_HOSTS = frozenset(
    {"gchat.qpic.cn", "multimedia.nt.qq.com.cn", "gxh.vip.qq.com"}
)
GIF_SIGNATURES = (b"GIF87a", b"GIF89a")
QQ_PARCEL_EXPRESSION_HOSTS = frozenset({"gxh.vip.qq.com", "p.qpic.cn"})
METADATA_DECODE_ERRORS = (
    UnidentifiedImageError,
    Image.DecompressionBombError,
    OSError,
    ValueError,
    SyntaxError,
    EOFError,
    zlib.error,
)


class DownloadError(RuntimeError):
    pass


class DownloadPolicyError(DownloadError):
    pass


class CdnHttpError(DownloadError):
    def __init__(
        self,
        status_code: int,
        message: str,
        *,
        attempted_statuses: tuple[int, ...] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = int(status_code)
        self.attempted_statuses = tuple(
            int(value) for value in (attempted_statuses or (self.status_code,))
        )


class GifDetected(DownloadError):
    pass


def resolver_data(row: sqlite3.Row) -> dict[str, Any]:
    try:
        value = json.loads(str(row["resolver_json"] or "{}"))
    except (TypeError, ValueError):
        value = {}
    return value if isinstance(value, dict) else {}


def _row_value(row: sqlite3.Row, key: str) -> Any:
    try:
        return row[key]
    except (IndexError, KeyError, TypeError):
        return None


def _url_has_rkey(url: str) -> bool:
    try:
        return any(key.casefold() == "rkey" for key, _value in parse_qsl(urlsplit(url).query))
    except ValueError:
        return False


def candidate_urls(
    row: sqlite3.Row,
    preference: str = "data",
    *,
    now: int | None = None,
) -> list[str]:
    data = resolver_data(row)
    expiry = int(_row_value(row, "url_expires_at") or data.get("url_expires_at") or 0)
    expired = expiry > 0 and expiry <= int(now or time.time())
    if preference == "raw":
        candidates = (
            (data.get("origin_url"), data.get("origin_url_has_rkey")),
            (data.get("url"), data.get("data_url_has_rkey")),
        )
    else:
        candidates = (
            (data.get("url"), data.get("data_url_has_rkey")),
            (data.get("origin_url"), data.get("origin_url_has_rkey")),
        )
    result: list[str] = []
    for value, stored_has_rkey in candidates:
        url = str(value or "").strip()
        if not url or url in result:
            continue
        # The deadline is deliberately conservative. Once it passes, a URL
        # carrying rkey must be refreshed instead of issuing a request that is
        # already known to return 400. A stable no-rkey fallback remains usable.
        if expired and (bool(stored_has_rkey) or _url_has_rkey(url)):
            continue
        try:
            result.append(validate_cdn_url(url))
        except DownloadPolicyError:
            continue
    return result


def selected_url(row: sqlite3.Row, preference: str = "data") -> str:
    candidates = candidate_urls(row, preference)
    return candidates[0] if candidates else ""


def is_known_qq_parcel_expression(row: sqlite3.Row) -> bool:
    data = resolver_data(row)
    if not bool(data.get("emoji_signal")):
        return False
    for key in ("url", "origin_url"):
        url = str(data.get(key) or "").strip()
        if not url:
            continue
        try:
            parsed = urlsplit(url)
        except ValueError:
            continue
        path = parsed.path.casefold()
        if (
            parsed.scheme == "https"
            and (parsed.hostname or "").lower() in QQ_PARCEL_EXPRESSION_HOSTS
            and path.startswith("/club/item/parcel/item/")
            and path.endswith("/raw300.gif")
        ):
            return True
    return False


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
        self.last_attempted_statuses: tuple[int, ...] = ()
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds, connect=20),
            follow_redirects=False,
            headers={"User-Agent": "Mozilla/5.0"},
        )

    async def close(self) -> None:
        await self.client.aclose()

    def daily_remaining(self) -> int:
        # Kept for API compatibility with older status/tests. Collection no
        # longer has a request quota, regardless of a stale configured value.
        return 2**63 - 1

    async def _stream_to_temp(self, url: str) -> tuple[Path, int, bytes]:
        url = validate_cdn_url(url)
        temp_root = self.storage_root / "temp"
        temp_root.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix="qq-cdn-", suffix=".part", dir=temp_root)
        os.close(fd)
        path = Path(name)
        size = 0
        prefix = bytearray()
        # Count every outbound CDN request, including 403/429/timeouts.
        increment_counter(self.connection, "cdn_requests")
        try:
            async with self.client.stream("GET", url) as response:
                if response.status_code != 200:
                    if response.status_code == 400:
                        increment_counter(self.connection, "cdn_400")
                    elif response.status_code == 403:
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
        self.last_attempted_statuses = ()
        if is_known_qq_parcel_expression(row):
            # Some deleted parcel expressions redirect to a dead asset, so
            # byte-level GIF confirmation is no longer possible. Require the
            # event signal, fixed Tencent host and fixed path together.
            finish_image(
                self.connection,
                row,
                status="filtered_gif",
                error="excluded QQ parcel GIF expression",
            )
            increment_counter(self.connection, "filtered_gif")
            return "filtered_gif"
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
        attempted_statuses: list[int] = []
        try:
            try:
                for index, url in enumerate(urls):
                    try:
                        temp_path, _size, _prefix = await self._stream_to_temp(url)
                        break
                    except CdnHttpError as exc:
                        attempted_statuses.extend(exc.attempted_statuses)
                        self.last_attempted_statuses = tuple(attempted_statuses)
                        if exc.status_code in {400, 403, 404, 410} and index + 1 < len(urls):
                            continue
                        raise CdnHttpError(
                            exc.status_code,
                            str(exc),
                            attempted_statuses=tuple(attempted_statuses),
                        ) from exc
            except GifDetected:
                finish_image(
                    self.connection,
                    row,
                    status="filtered_gif",
                    error="excluded image format: GIF",
                )
                increment_counter(self.connection, "filtered_gif")
                return "filtered_gif"

            digest = await asyncio.to_thread(sha256_file, temp_path)
            try:
                result = await asyncio.to_thread(inspect_image, temp_path)
            except METADATA_DECODE_ERRORS as exc:
                # A malformed or intentionally oversized metadata block belongs
                # to this image. It must not tear down the shared Worker or keep
                # a page-draining recovery job blocked forever.
                finish_image(
                    self.connection,
                    row,
                    status="rejected_no_metadata",
                    sha256=digest,
                    error=f"metadata_decode_error:{type(exc).__name__}",
                )
                increment_counter(self.connection, "rejected")
                return "rejected"
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
