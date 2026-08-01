from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any

import httpx
import websockets

from qq_image_collector.config import load_settings
from qq_image_collector.downloader import validate_cdn_url
from qq_image_collector.events import parse_group_event
from qq_image_collector.onebot import OneBotClient, websocket_settings


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


async def sha256_url(client: httpx.AsyncClient, url: str) -> str:
    digest = hashlib.sha256()
    size = 0
    async with client.stream("GET", validate_cdn_url(url)) as response:
        response.raise_for_status()
        async for block in response.aiter_raw():
            size += len(block)
            if size > 128 * 1024 * 1024:
                raise RuntimeError("diagnostic image exceeds 128 MiB")
            digest.update(block)
    return digest.hexdigest()


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


async def run(args: argparse.Namespace) -> int:
    if not args.allow_get_image_diagnostic:
        raise RuntimeError("explicit --allow-get-image-diagnostic is required")
    source_root = Path("/diagnostics").resolve()
    source = args.source.resolve()
    if source_root not in source.parents or not source.is_file():
        raise RuntimeError("source must be an existing file below /diagnostics")
    settings = load_settings(args.config)
    onebot = OneBotClient.from_settings(settings["onebot"])
    ws_url, token = websocket_settings(settings["onebot"])
    headers = {"Authorization": f"Bearer {token}"} if token else None
    event: dict[str, Any] | None = None
    item: dict[str, Any] | None = None
    async with websockets.connect(ws_url, additional_headers=headers, ping_interval=30) as websocket:
        while True:
            candidate = json.loads(await asyncio.wait_for(websocket.recv(), timeout=args.timeout))
            cursor, items = parse_group_event(candidate)
            if cursor is None or (args.group and cursor["group_id"] != args.group) or not items:
                continue
            event, item = candidate, items[0]
            break
    assert event is not None and item is not None
    resolver = item["resolver_data"]
    direct_url = str(resolver.get("url") or "")
    raw_url = str(resolver.get("origin_url") or "")
    async with httpx.AsyncClient(timeout=120, follow_redirects=False) as client:
        direct_sha = await sha256_url(client, direct_url)
        raw_sha = await sha256_url(client, raw_url) if raw_url and raw_url != direct_url else direct_sha
        get_image = await raw_onebot_get_image(client, onebot, str(item.get("file") or ""))
        returned_path = Path(str(get_image.get("file") or ""))
        if returned_path.is_file():
            get_image_sha = await asyncio.to_thread(sha256_path, returned_path)
        else:
            get_image_sha = await sha256_url(client, str(get_image.get("url") or ""))
    source_sha = await asyncio.to_thread(sha256_path, source)
    result = {
        "group_id": item["group_id"],
        "raw_debug_present": bool(event.get("raw")),
        "original_flag": item.get("original_flag"),
        "source_sha256": source_sha,
        "data_url_sha256": direct_sha,
        "raw_url_sha256": raw_sha,
        "get_image_sha256": get_image_sha,
        "data_url_matches_source": direct_sha == source_sha,
        "raw_url_matches_source": raw_sha == source_sha,
        "get_image_matches_source": get_image_sha == source_sha,
        "production_gate": "pass" if direct_sha == source_sha else "fail-keep-worker-paused",
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if direct_sha == source_sha else 2


def main() -> int:
    parser = argparse.ArgumentParser(
        description="One-time isolated byte comparison. This is not imported by the production Worker."
    )
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--group")
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--allow-get-image-diagnostic", action="store_true")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("/data/qq-image-collector/config/collector_config.json"),
    )
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
