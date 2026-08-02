from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
import websockets

from qq_image_collector.config import load_settings
from qq_image_collector.downloader import validate_cdn_url
from qq_image_collector.events import parse_group_event
from qq_image_collector.onebot import websocket_settings


CONTENT_RANGE = re.compile(r"bytes\s+(?:\d+-\d+|\*)/(?:\d+|\*)", re.IGNORECASE)
MEDIA_TYPE = re.compile(r"[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+", re.IGNORECASE)


def _atomic_private_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)
    path.chmod(0o600)


def _url_record(url: str, kind: str) -> dict[str, Any]:
    parsed = urlsplit(validate_cdn_url(url))
    query = parse_qs(parsed.query, keep_blank_values=True)
    return {
        "id": hashlib.sha256(url.encode("utf-8")).hexdigest()[:16],
        "kind": kind,
        "host": (parsed.hostname or "").lower(),
        "has_rkey": any(key.casefold() == "rkey" for key in query),
        "captured_at": int(time.time()),
        "url": url,
    }


def _public_record(record: dict[str, Any]) -> dict[str, Any]:
    return {key: record.get(key) for key in ("id", "kind", "host", "has_rkey", "captured_at")}


def _safe_response_headers(response: httpx.Response) -> dict[str, Any]:
    headers = response.headers
    length_text = headers.get("content-length", "").strip()
    content_length = int(length_text) if length_text.isdigit() else None
    range_text = headers.get("content-range", "").strip()
    content_range = range_text if CONTENT_RANGE.fullmatch(range_text) else None
    media_type = headers.get("content-type", "").split(";", 1)[0].strip().lower()
    content_type = media_type if MEDIA_TYPE.fullmatch(media_type) else None
    accept_ranges = {
        value.strip().casefold()
        for value in headers.get("accept-ranges", "").split(",")
        if value.strip()
    }
    transfer_encoding = {
        value.strip().casefold()
        for value in headers.get("transfer-encoding", "").split(",")
        if value.strip()
    }
    etag = headers.get("etag", "")
    location = headers.get("location", "").strip()
    location_summary: dict[str, Any] = {"present": bool(location)}
    if location:
        parsed = urlsplit(location)
        query = parse_qs(parsed.query, keep_blank_values=True)
        location_summary.update(
            {
                "host": (parsed.hostname or "").lower(),
                "relative": not bool(parsed.scheme or parsed.netloc),
                "has_rkey": any(key.casefold() == "rkey" for key in query),
            }
        )
    return {
        "content_length": content_length,
        "content_range": content_range,
        "accept_ranges_bytes": "bytes" in accept_ranges,
        "content_type": content_type,
        "transfer_encoding_chunked": "chunked" in transfer_encoding,
        "etag_sha256": hashlib.sha256(etag.encode("utf-8")).hexdigest() if etag else None,
        "location": location_summary,
    }


async def _probe_response_headers(
    client: httpx.AsyncClient,
    url: str,
    mode: str,
) -> dict[str, Any]:
    if mode not in {"range_get", "plain_get"}:
        raise ValueError(f"unsupported lifecycle probe mode: {mode}")
    response: httpx.Response | None = None
    result: dict[str, Any] = {
        "method": "GET",
        "mode": mode,
        "checked_at": int(time.time()),
        "http_status": None,
        "error": None,
        "body_consumed": False,
        "response_headers": {},
    }
    try:
        request_headers = {"Range": "bytes=0-0"} if mode == "range_get" else {}
        request = client.build_request(
            "GET",
            validate_cdn_url(url),
            headers=request_headers,
        )
        response = await client.send(request, stream=True)
        result["http_status"] = response.status_code
        result["response_headers"] = _safe_response_headers(response)
    except (httpx.HTTPError, ValueError) as exc:
        result["error"] = type(exc).__name__
    finally:
        if response is not None:
            try:
                await response.aclose()
            except httpx.HTTPError as exc:
                result["close_error"] = type(exc).__name__
    return result


async def _paired_probe(client: httpx.AsyncClient, url: str) -> dict[str, Any]:
    started_at = int(time.time())
    range_get = await _probe_response_headers(client, url, "range_get")
    plain_get = await _probe_response_headers(client, url, "plain_get")
    return {
        "pair_started_at": started_at,
        "pair_finished_at": int(time.time()),
        "range_get": range_get,
        "plain_get": plain_get,
    }


def _load_report(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"schema": 2, "checks": [], "legacy_schema1_checks": 0}
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict) or not isinstance(loaded.get("checks"), list):
        raise RuntimeError("URL lifecycle report has an invalid structure")
    schema = int(loaded.get("schema") or 1)
    if schema == 1:
        checks = []
        for value in loaded["checks"]:
            if not isinstance(value, dict):
                continue
            legacy = dict(value)
            legacy.update(
                {
                    "legacy": True,
                    "legacy_schema": 1,
                    "probe_modes": ["range_get"],
                    "plain_get_recorded": False,
                }
            )
            checks.append(legacy)
        return {
            "schema": 2,
            "checks": checks,
            "legacy_schema1_checks": len(checks),
            "migration_note": "schema 1 checks are Range-only; no plain GET result was fabricated",
        }
    if schema != 2:
        raise RuntimeError(f"unsupported URL lifecycle report schema: {schema}")
    loaded.setdefault("legacy_schema1_checks", 0)
    return loaded


async def check(
    secret_path: Path,
    report_path: Path,
    label: str,
    *,
    finalize: bool = False,
) -> int:
    records = json.loads(secret_path.read_text(encoding="utf-8"))
    if not isinstance(records, list) or not records:
        raise RuntimeError("URL lifecycle secret contains no records")
    results = []
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(30, connect=10),
        follow_redirects=False,
        headers={"User-Agent": "Mozilla/5.0"},
    ) as client:
        for record in records:
            probes = await _paired_probe(client, str(record.get("url") or ""))
            results.append(_public_record(record) | probes)
    report = _load_report(report_path)
    report["checks"].append(
        {
            "label": label,
            "checked_at": int(time.time()),
            "legacy": False,
            "probe_modes": ["range_get", "plain_get"],
            "body_policy": "streamed response headers only; response body was not consumed",
            "results": results,
        }
    )
    _atomic_private_json(report_path, report)
    print(json.dumps(report["checks"][-1], ensure_ascii=False, indent=2))
    if finalize:
        secret_path.unlink(missing_ok=True)
        print(json.dumps({"finalized": True, "secret_urls_deleted": True}))
    return 0


async def capture(
    config: Path,
    secret_path: Path,
    report_path: Path,
    group_id: str | None,
    target_urls: int,
    timeout: int,
) -> int:
    if secret_path.exists():
        raise RuntimeError("lifecycle secret already exists; finalize or remove it before recapturing")
    settings = load_settings(config)
    ws_url, token = websocket_settings(settings["onebot"])
    headers = {"Authorization": f"Bearer {token}"} if token else None
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(5, timeout)
    async with websockets.connect(
        ws_url,
        additional_headers=headers,
        ping_interval=30,
        ping_timeout=30,
        max_size=16 * 1024 * 1024,
    ) as websocket:
        while len(records) < target_urls:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise RuntimeError("timed out before enough lifecycle URLs were captured")
            event = json.loads(await asyncio.wait_for(websocket.recv(), timeout=remaining))
            cursor, items = parse_group_event(event)
            if cursor is None or (group_id and cursor["group_id"] != group_id):
                continue
            for item in items:
                resolver = item.get("resolver_data") or {}
                for field, kind in (("url", "data_url"), ("origin_url", "origin_url")):
                    url = str(resolver.get(field) or "")
                    if not url or url in seen:
                        continue
                    try:
                        record = _url_record(url, kind)
                    except (ValueError, RuntimeError):
                        continue
                    seen.add(url)
                    records.append(record)
                    if len(records) >= target_urls:
                        break
                if len(records) >= target_urls:
                    break
    _atomic_private_json(secret_path, records)
    print(
        json.dumps(
            {
                "captured": len(records),
                "records": [_public_record(record) for record in records],
                "secret_path": str(secret_path),
                "privacy": "full URLs are stored chmod 0600 and never printed",
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return await check(secret_path, report_path, "T+0", finalize=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Capture and recheck 10 CDN URLs without printing rkeys.")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("/data/qq-image-collector/config/collector_config.json"),
    )
    parser.add_argument(
        "--secret-path",
        type=Path,
        default=Path("/data/qq-image-collector/state/url_lifecycle.secret.json"),
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        default=Path("/data/qq-image-collector/state/url_lifecycle.report.json"),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    capture_parser = subparsers.add_parser("capture")
    capture_parser.add_argument("--group")
    capture_parser.add_argument("--urls", type=int, default=10)
    capture_parser.add_argument("--timeout", type=int, default=3600)
    check_parser = subparsers.add_parser("check")
    check_parser.add_argument("--label", required=True)
    check_parser.add_argument("--finalize", action="store_true")
    args = parser.parse_args()
    if args.command == "capture":
        return asyncio.run(
            capture(
                args.config,
                args.secret_path,
                args.report_path,
                args.group,
                max(1, args.urls),
                max(5, args.timeout),
            )
        )
    return asyncio.run(
        check(args.secret_path, args.report_path, args.label, finalize=args.finalize)
    )


if __name__ == "__main__":
    raise SystemExit(main())
