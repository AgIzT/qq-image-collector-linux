from __future__ import annotations

import argparse
import asyncio
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import websockets

from qq_image_collector.config import load_settings
from qq_image_collector.onebot import websocket_settings


LONG_NUMBER = re.compile(r"\d{5,}")
MD5_TOKEN = re.compile(r"(?i)(?<![0-9a-f])([0-9a-f]{32})(?![0-9a-f])")


def _url_details(value: Any) -> tuple[str, bool, str] | None:
    url = str(value or "")
    if not url:
        return None
    try:
        parsed = urlsplit(url)
        query = parse_qs(parsed.query, keep_blank_values=True)
    except ValueError:
        return "<invalid>", False, ""
    host = (parsed.hostname or "<empty>").lower()
    has_rkey = any(key.casefold() == "rkey" for key in query)
    return host, has_rkey, parsed.path


def _redact_gchat_path(path: str) -> str:
    parts = path.split("/")
    if len(parts) >= 5 and parts[1] == "gchatpic_new":
        object_parts = parts[3].split("-")
        if len(object_parts) >= 3:
            parts[2] = "<uin>"
            parts[3] = "<group>-<file>-" + "-".join(object_parts[2:])
            return "/".join(parts)
    return LONG_NUMBER.sub("<id>", path)


def _counter(counter: Counter[str]) -> dict[str, int]:
    return {key: int(counter[key]) for key in sorted(counter)}


def _identity(*values: Any) -> tuple[str | None, str | None]:
    filename = str(values[0] or "").replace("\\", "/").rsplit("/", 1)[-1].casefold()
    md5 = None
    for value in values:
        match = MD5_TOKEN.search(str(value or ""))
        if match:
            md5 = match.group(1).lower()
            break
    return filename or None, md5


def _independent_match_count(
    standard: list[dict[str, Any]], raw_pictures: list[dict[str, Any]]
) -> int:
    used: set[int] = set()
    matched = 0
    for data in standard:
        data_name, data_md5 = _identity(
            data.get("file_name") or data.get("filename") or data.get("file"),
            data.get("md5"),
            data.get("file_id"),
        )
        for index, picture in enumerate(raw_pictures):
            if index in used:
                continue
            raw_name, raw_md5 = _identity(
                picture.get("fileName"), picture.get("md5HexStr")
            )
            if (data_name and raw_name == data_name) or (data_md5 and raw_md5 == data_md5):
                used.add(index)
                matched += 1
                break
    return matched


async def receive_sample(
    config: Path,
    group_id: str | None,
    target_segments: int,
    timeout: int,
    output: Path | None,
) -> int:
    settings = load_settings(config)
    ws_url, token = websocket_settings(settings["onebot"])
    headers = {"Authorization": f"Bearer {token}"} if token else None
    data_hosts: Counter[str] = Counter()
    origin_hosts: Counter[str] = Counter()
    data_rkey: Counter[str] = Counter()
    origin_rkey: Counter[str] = Counter()
    original: Counter[str] = Counter()
    samples: list[str] = []
    events_seen = 0
    group_messages = 0
    standard_segments = 0
    raw_pic_elements = 0
    matched_pairs = 0
    estimated_slots = 0
    timed_out = False
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(5, timeout)

    try:
        async with websockets.connect(
            ws_url,
            additional_headers=headers,
            ping_interval=30,
            ping_timeout=30,
            max_size=16 * 1024 * 1024,
        ) as websocket:
            while estimated_slots < target_segments:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise asyncio.TimeoutError
                payload = await asyncio.wait_for(websocket.recv(), timeout=remaining)
                events_seen += 1
                try:
                    event = json.loads(payload)
                except (TypeError, json.JSONDecodeError):
                    continue
                if (
                    not isinstance(event, dict)
                    or event.get("post_type") != "message"
                    or event.get("message_type") != "group"
                ):
                    continue
                if group_id and str(event.get("group_id") or "") != group_id:
                    continue
                group_messages += 1
                standard = [
                    segment.get("data") if isinstance(segment.get("data"), dict) else {}
                    for segment in (event.get("message") or [])
                    if isinstance(segment, dict) and segment.get("type") == "image"
                ]
                raw = event.get("raw") if isinstance(event.get("raw"), dict) else {}
                raw_pictures = [
                    element["picElement"]
                    for element in (raw.get("elements") or [])
                    if isinstance(element, dict) and isinstance(element.get("picElement"), dict)
                ]
                standard_segments += len(standard)
                raw_pic_elements += len(raw_pictures)
                matched_this_event = _independent_match_count(standard, raw_pictures)
                matched_pairs += matched_this_event
                # Count the union, not merely the longer list: an unmatched
                # standard segment and an unmatched raw picElement are two
                # independently observed image candidates.
                estimated_slots += (
                    len(standard) + len(raw_pictures) - matched_this_event
                )

                for data in standard:
                    details = _url_details(data.get("url"))
                    if details is None:
                        data_hosts["<empty>"] += 1
                        continue
                    host, has_rkey, path = details
                    data_hosts[host] += 1
                    data_rkey["with_rkey" if has_rkey else "without_rkey"] += 1
                    if host == "gchat.qpic.cn" and path and len(samples) < 3:
                        sample = _redact_gchat_path(path)
                        if sample not in samples:
                            samples.append(sample)
                for picture in raw_pictures:
                    details = _url_details(picture.get("originImageUrl"))
                    if details is None:
                        origin_hosts["<empty>"] += 1
                    else:
                        host, has_rkey, path = details
                        origin_hosts[host] += 1
                        origin_rkey["with_rkey" if has_rkey else "without_rkey"] += 1
                        if host == "gchat.qpic.cn" and path and len(samples) < 3:
                            sample = _redact_gchat_path(path)
                            if sample not in samples:
                                samples.append(sample)
                    value = picture.get("original")
                    original[
                        "null" if value is None else "true" if bool(value) else "false"
                    ] += 1
    except asyncio.TimeoutError:
        timed_out = True

    all_url_count = sum(data_rkey.values()) + sum(origin_rkey.values())
    all_rkey_count = data_rkey["with_rkey"] + origin_rkey["with_rkey"]
    result = {
        "schema": 2,
        "target_estimated_image_slots": target_segments,
        "captured_estimated_image_slots": estimated_slots,
        "complete": estimated_slots >= target_segments,
        "timed_out": timed_out,
        "events_seen": events_seen,
        "group_messages_seen": group_messages,
        "standard_image_segments": standard_segments,
        "raw_pic_elements": raw_pic_elements,
        "independently_matched_pairs": matched_pairs,
        "standard_without_raw_match": max(0, standard_segments - matched_pairs),
        "raw_without_standard_match": max(0, raw_pic_elements - matched_pairs),
        "data_url_host_distribution": _counter(data_hosts),
        "origin_url_host_distribution": _counter(origin_hosts),
        "data_url_rkey": _counter(data_rkey),
        "origin_url_rkey": _counter(origin_rkey),
        "combined_rkey_ratio_percent": round(100 * all_rkey_count / all_url_count, 3)
        if all_url_count
        else 0.0,
        "gchat_path_samples_redacted": samples,
        "raw_original_flag_distribution": _counter(original),
        "privacy": "group/account identifiers and URL query strings are not emitted",
    }
    rendered = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    destination = output or Path(settings["storage"]["root"]) / "state" / "url_probe.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(rendered, encoding="utf-8")
    destination.chmod(0o600)
    print(rendered, end="")
    return 0 if result["complete"] else 2


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Collect independent, privacy-redacted standard/raw URL-shape statistics."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("/data/qq-image-collector/config/collector_config.json"),
    )
    parser.add_argument("--group")
    parser.add_argument("--image-segments", type=int, default=200)
    parser.add_argument("--timeout", type=int, default=6 * 3600)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    return asyncio.run(
        receive_sample(
            args.config,
            args.group,
            max(1, args.image_segments),
            max(5, args.timeout),
            args.output,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
