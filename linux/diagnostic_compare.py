from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
import websockets

from metadata_reader import inspect_image
from qq_image_collector.config import load_settings
from qq_image_collector.downloader import validate_cdn_url
from qq_image_collector.events import parse_group_event
from qq_image_collector.onebot import OneBotClient, websocket_settings


MD5_TOKEN = re.compile(r"(?i)(?<![0-9a-f])([0-9a-f]{32})(?![0-9a-f])")
MAX_BYTES = 128 * 1024 * 1024


def atomic_private_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)
    path.chmod(0o600)


def hash_path(path: Path, algorithm: str = "sha256") -> str:
    digest = hashlib.new(algorithm)
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def metadata_summary(path: Path) -> dict[str, Any]:
    result = inspect_image(path)
    serialized = json.dumps(
        result.fields,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=lambda value: value.hex() if isinstance(value, bytes) else str(value),
    ).encode("utf-8")
    return {
        "accepted": result.accepted,
        "source": result.source,
        "field_names": sorted(result.fields),
        "fields_sha256": hashlib.sha256(serialized).hexdigest(),
    }


def url_shape(url: str) -> dict[str, Any]:
    try:
        parsed = urlsplit(url)
        query = parse_qs(parsed.query, keep_blank_values=True)
    except ValueError:
        return {"host": "<invalid>", "has_rkey": False}
    return {
        "host": (parsed.hostname or "").lower(),
        "has_rkey": any(key.casefold() == "rkey" for key in query),
    }


def event_md5(item: dict[str, Any]) -> str | None:
    resolver = item.get("resolver_data") or {}
    for value in (resolver.get("md5"), item.get("file")):
        match = MD5_TOKEN.search(str(value or ""))
        if match:
            return match.group(1).lower()
    return None


async def download_url(
    client: httpx.AsyncClient,
    url: str,
    destination: Path,
) -> dict[str, Any]:
    if not url:
        return {"available": False, "status": None, "error": "missing URL"}
    shape = url_shape(url)
    try:
        async with client.stream("GET", validate_cdn_url(url)) as response:
            result: dict[str, Any] = {"available": True, "status": response.status_code} | shape
            if response.status_code != 200:
                return result
            size = 0
            with destination.open("wb") as stream:
                async for block in response.aiter_raw():
                    size += len(block)
                    if size > MAX_BYTES:
                        raise RuntimeError("diagnostic image exceeds 128 MiB")
                    stream.write(block)
            result.update(
                {
                    "size": size,
                    "sha256": await asyncio.to_thread(hash_path, destination, "sha256"),
                    "metadata": await asyncio.to_thread(metadata_summary, destination),
                }
            )
            return result
    except (httpx.HTTPError, OSError, RuntimeError) as exc:
        return {"available": True, "status": None, "error": type(exc).__name__} | shape


async def raw_onebot_get_image(
    client: httpx.AsyncClient,
    onebot: OneBotClient,
    file_token: str,
) -> dict[str, Any]:
    response = await client.post(
        f"{onebot.base_url}/get_image",
        headers=onebot.headers,
        json={"file": file_token},
    )
    response.raise_for_status()
    body = response.json()
    if body.get("status") != "ok" or int(body.get("retcode", -1)) != 0:
        raise RuntimeError("one-time get_image diagnostic failed")
    data = body.get("data")
    return data if isinstance(data, dict) else {}


def _preferred_result(
    preference: str,
    data_result: dict[str, Any],
    origin_result: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    if preference == "raw" and origin_result.get("available"):
        return "origin_url", origin_result
    if data_result.get("available"):
        return "data_url", data_result
    return "origin_url", origin_result


def _recommend(data_result: dict[str, Any], origin_result: dict[str, Any], source_sha: str) -> str:
    matches = []
    for name, result in (("data", data_result), ("raw", origin_result)):
        if result.get("sha256") == source_sha:
            stable = result.get("host") == "gchat.qpic.cn" and not result.get("has_rkey")
            matches.append((not stable, name))
    return sorted(matches)[0][1] if matches else "none"


async def run(args: argparse.Namespace) -> int:
    source_root = Path("/diagnostics").resolve()
    source = args.source.resolve()
    if source_root not in source.parents or not source.is_file():
        raise RuntimeError("source must be an existing file below /diagnostics")
    settings = load_settings(args.config)
    onebot = OneBotClient.from_settings(settings["onebot"])
    source_md5 = await asyncio.to_thread(hash_path, source, "md5")
    source_sha = await asyncio.to_thread(hash_path, source, "sha256")
    source_metadata = await asyncio.to_thread(metadata_summary, source)
    ws_url, token = websocket_settings(settings["onebot"])
    headers = {"Authorization": f"Bearer {token}"} if token else None
    event: dict[str, Any] | None = None
    item: dict[str, Any] | None = None
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(5, args.timeout)
    async with websockets.connect(
        ws_url,
        additional_headers=headers,
        ping_interval=30,
        ping_timeout=30,
        max_size=16 * 1024 * 1024,
    ) as websocket:
        while item is None:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise RuntimeError("timed out waiting for an event whose MD5 matches the source file")
            candidate = json.loads(await asyncio.wait_for(websocket.recv(), timeout=remaining))
            cursor, items = parse_group_event(candidate)
            if cursor is None or (args.group and cursor["group_id"] != args.group):
                continue
            for candidate_item in items:
                if args.sender and str(candidate_item.get("sender_uin") or "") != args.sender:
                    continue
                if event_md5(candidate_item) == source_md5:
                    event, item = candidate, candidate_item
                    break
    assert event is not None and item is not None
    resolver = item["resolver_data"]
    direct_url = str(resolver.get("url") or "")
    raw_url = str(resolver.get("origin_url") or "")

    with tempfile.TemporaryDirectory(prefix="qq-image-diagnostic-") as temporary:
        root = Path(temporary)
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(120, connect=20),
            follow_redirects=False,
            headers={"User-Agent": "Mozilla/5.0"},
        ) as client:
            data_result = await download_url(client, direct_url, root / "data-url.bin")
            if raw_url and raw_url == direct_url:
                origin_result = dict(data_result) | {"same_as_data_url": True}
            else:
                origin_result = await download_url(client, raw_url, root / "origin-url.bin")
            get_image_result: dict[str, Any] = {"skipped": True, "reason": "metadata-only mode"}
            if args.allow_get_image_diagnostic:
                sentinel = args.get_image_sentinel or (
                    Path(settings["storage"]["root"]) / "state" / "get_image_diagnostic.used.json"
                )
                if sentinel.exists():
                    raise RuntimeError("the one-time get_image diagnostic has already been consumed")
                # Write the intent before the account-session request so a crash
                # cannot accidentally permit a second diagnostic call.
                atomic_private_json(
                    sentinel,
                    {
                        "used_at": int(time.time()),
                        "source_md5": source_md5,
                        "reason": "one-time Test B byte comparison",
                    },
                )
                try:
                    get_image = await raw_onebot_get_image(
                        client, onebot, str(item.get("file") or "")
                    )
                    returned_path = Path(str(get_image.get("file") or ""))
                    if returned_path.is_file():
                        get_image_result = {
                            "available": True,
                            "status": 200,
                            "sha256": await asyncio.to_thread(
                                hash_path, returned_path, "sha256"
                            ),
                            "size": returned_path.stat().st_size,
                            "metadata": await asyncio.to_thread(
                                metadata_summary, returned_path
                            ),
                        }
                    else:
                        get_image_result = await download_url(
                            client,
                            str(get_image.get("url") or ""),
                            root / "get-image.bin",
                        )
                except (httpx.HTTPError, OSError, RuntimeError, ValueError) as exc:
                    get_image_result = {
                        "available": False,
                        "status": None,
                        "error": type(exc).__name__,
                    }

    for result in (data_result, origin_result):
        result["matches_source"] = result.get("sha256") == source_sha
        metadata = result.get("metadata") or {}
        result["metadata_matches_source"] = (
            metadata.get("source") == source_metadata.get("source")
            and metadata.get("fields_sha256") == source_metadata.get("fields_sha256")
        )

    if not get_image_result.get("skipped"):
        get_image_result["matches_source"] = get_image_result.get("sha256") == source_sha
        metadata = get_image_result.get("metadata") or {}
        get_image_result["metadata_matches_source"] = (
            metadata.get("source") == source_metadata.get("source")
            and metadata.get("fields_sha256") == source_metadata.get("fields_sha256")
        )

    preference = str(settings.get("runtime", {}).get("url_preference") or "data")
    production_name, production_result = _preferred_result(preference, data_result, origin_result)
    production_pass = bool(production_result.get("matches_source"))
    metadata_pass = bool(production_result.get("metadata_matches_source"))
    get_image_pass = bool(get_image_result.get("matches_source"))
    gate = production_pass and metadata_pass and (
        get_image_pass if args.allow_get_image_diagnostic else True
    )
    result = {
        "schema": 2,
        "matched_event_md5": source_md5,
        "raw_debug_present": bool(event.get("raw")),
        "raw_picture_match": resolver.get("raw_match"),
        "original_flag": item.get("original_flag"),
        "source": {
            "sha256": source_sha,
            "size": source.stat().st_size,
            "metadata": source_metadata,
        },
        "data_url": data_result,
        "origin_url": origin_result,
        "get_image_one_time_diagnostic": get_image_result,
        "configured_url_preference": preference,
        "production_candidate": production_name,
        "recommended_url_preference": _recommend(data_result, origin_result, source_sha),
        "diagnostic_mode": "test-b-one-time-get-image"
        if args.allow_get_image_diagnostic
        else "test-c-cdn-metadata-only",
        "production_gate": "pass" if gate else "fail-keep-worker-paused",
        "privacy": "account/group IDs, filenames, URLs, prompts and URL query strings are not emitted",
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if gate else 2


def main() -> int:
    parser = argparse.ArgumentParser(
        description="One-time isolated byte/metadata comparison; never imported by the production Worker."
    )
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--group")
    parser.add_argument("--sender")
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--allow-get-image-diagnostic", action="store_true")
    parser.add_argument("--get-image-sentinel", type=Path)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("/data/qq-image-collector/config/collector_config.json"),
    )
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
