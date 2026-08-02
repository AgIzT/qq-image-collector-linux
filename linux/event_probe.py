from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sqlite3
import time
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import websockets
from websockets.exceptions import WebSocketException

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


def _empty_state() -> dict[str, Any]:
    return {
        "events_seen": 0,
        "group_messages": 0,
        "standard_segments": 0,
        "raw_pic_elements": 0,
        "matched_pairs": 0,
        "estimated_slots": 0,
        "data_hosts": {},
        "origin_hosts": {},
        "data_rkey": {},
        "origin_rkey": {},
        "original": {},
        "samples": [],
        "connection_attempts": 0,
        "disconnects": 0,
        "reconnects": 0,
        "connection_state": "not_started",
        "last_disconnect_error": None,
    }


def _scope_hash(group_id: str) -> str:
    return hashlib.sha256(group_id.encode("utf-8")).hexdigest()


def _event_hash(event: dict[str, Any]) -> str:
    serialized = json.dumps(
        event, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()


def _open_checkpoint(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    os.close(descriptor)
    path.chmod(0o600)
    connection = sqlite3.connect(path, timeout=30)
    connection.execute("PRAGMA journal_mode=DELETE")
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS probe_state (
            scope_hash TEXT PRIMARY KEY,
            state_json TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS probe_events (
            scope_hash TEXT NOT NULL,
            event_hash TEXT NOT NULL,
            seen_at INTEGER NOT NULL,
            PRIMARY KEY(scope_hash, event_hash)
        )
        """
    )
    connection.commit()
    path.chmod(0o600)
    return connection


def _load_state(connection: sqlite3.Connection, scope: str) -> tuple[dict[str, Any], bool]:
    row = connection.execute(
        "SELECT state_json FROM probe_state WHERE scope_hash=?", (scope,)
    ).fetchone()
    if not row:
        return _empty_state(), False
    try:
        loaded = json.loads(str(row[0]))
    except (TypeError, ValueError):
        loaded = {}
    state = _empty_state()
    if isinstance(loaded, dict):
        state.update(loaded)
    return state, True


def _save_state(connection: sqlite3.Connection, scope: str, state: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT INTO probe_state(scope_hash, state_json, updated_at) VALUES (?, ?, ?)
        ON CONFLICT(scope_hash) DO UPDATE SET
            state_json=excluded.state_json, updated_at=excluded.updated_at
        """,
        (scope, json.dumps(state, ensure_ascii=False, separators=(",", ":")), int(time.time())),
    )


def _reset_scope(connection: sqlite3.Connection, scope: str) -> None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute("DELETE FROM probe_events WHERE scope_hash=?", (scope,))
        connection.execute("DELETE FROM probe_state WHERE scope_hash=?", (scope,))
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def _apply_event(state: dict[str, Any], event: dict[str, Any]) -> None:
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
    matched = _independent_match_count(standard, raw_pictures)
    state["events_seen"] = int(state["events_seen"]) + 1
    state["group_messages"] = int(state["group_messages"]) + 1
    state["standard_segments"] = int(state["standard_segments"]) + len(standard)
    state["raw_pic_elements"] = int(state["raw_pic_elements"]) + len(raw_pictures)
    state["matched_pairs"] = int(state["matched_pairs"]) + matched
    state["estimated_slots"] = int(state["estimated_slots"]) + len(standard) + len(raw_pictures) - matched

    data_hosts = Counter(state.get("data_hosts") or {})
    origin_hosts = Counter(state.get("origin_hosts") or {})
    data_rkey = Counter(state.get("data_rkey") or {})
    origin_rkey = Counter(state.get("origin_rkey") or {})
    original = Counter(state.get("original") or {})
    samples = list(state.get("samples") or [])[:3]
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
        original["null" if value is None else "true" if bool(value) else "false"] += 1
    state.update(
        data_hosts=_counter(data_hosts),
        origin_hosts=_counter(origin_hosts),
        data_rkey=_counter(data_rkey),
        origin_rkey=_counter(origin_rkey),
        original=_counter(original),
        samples=samples,
    )


def _checkpoint_event(
    connection: sqlite3.Connection,
    scope: str,
    state: dict[str, Any],
    event: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    event_hash = _event_hash(event)
    connection.execute("BEGIN IMMEDIATE")
    try:
        inserted = connection.execute(
            "INSERT OR IGNORE INTO probe_events(scope_hash,event_hash,seen_at) VALUES (?, ?, ?)",
            (scope, event_hash, int(time.time())),
        ).rowcount
        if not inserted:
            connection.commit()
            return state, False
        next_state = json.loads(json.dumps(state, ensure_ascii=False))
        _apply_event(next_state, event)
        _save_state(connection, scope, next_state)
        connection.commit()
        return next_state, True
    except Exception:
        connection.rollback()
        raise


def _checkpoint_state(
    connection: sqlite3.Connection, scope: str, state: dict[str, Any]
) -> None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        _save_state(connection, scope, state)
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def _result(
    state: dict[str, Any],
    target_segments: int,
    *,
    timed_out: bool,
    resumed: bool,
) -> dict[str, Any]:
    data_rkey = Counter(state.get("data_rkey") or {})
    origin_rkey = Counter(state.get("origin_rkey") or {})
    all_url_count = sum(data_rkey.values()) + sum(origin_rkey.values())
    all_rkey_count = data_rkey["with_rkey"] + origin_rkey["with_rkey"]
    standard = int(state.get("standard_segments") or 0)
    raw = int(state.get("raw_pic_elements") or 0)
    matched = int(state.get("matched_pairs") or 0)
    estimated = int(state.get("estimated_slots") or 0)
    return {
        "schema": 3,
        "target_estimated_image_slots": target_segments,
        "captured_estimated_image_slots": estimated,
        "complete": estimated >= target_segments,
        "timed_out": timed_out,
        "events_seen": int(state.get("events_seen") or 0),
        "group_messages_seen": int(state.get("group_messages") or 0),
        "standard_image_segments": standard,
        "raw_pic_elements": raw,
        "independently_matched_pairs": matched,
        "standard_without_raw_match": max(0, standard - matched),
        "raw_without_standard_match": max(0, raw - matched),
        "data_url_host_distribution": dict(state.get("data_hosts") or {}),
        "origin_url_host_distribution": dict(state.get("origin_hosts") or {}),
        "data_url_rkey": dict(state.get("data_rkey") or {}),
        "origin_url_rkey": dict(state.get("origin_rkey") or {}),
        "combined_rkey_ratio_percent": round(100 * all_rkey_count / all_url_count, 3)
        if all_url_count
        else 0.0,
        "gchat_path_samples_redacted": list(state.get("samples") or [])[:3],
        "raw_original_flag_distribution": dict(state.get("original") or {}),
        "connection": {
            "state": str(state.get("connection_state") or "unknown"),
            "attempts": int(state.get("connection_attempts") or 0),
            "disconnects": int(state.get("disconnects") or 0),
            "reconnects": int(state.get("reconnects") or 0),
            "resumed_from_checkpoint": bool(resumed),
            "last_disconnect_error": state.get("last_disconnect_error"),
        },
        "privacy": "only a group scope hash and event hashes are persisted; account/group IDs, prompts, full URLs and rkeys are never stored",
    }


def _atomic_output(path: Path, result: dict[str, Any]) -> str:
    rendered = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(rendered)
    os.replace(temporary, path)
    path.chmod(0o600)
    return rendered


async def receive_sample(
    config: Path,
    group_id: str,
    target_segments: int,
    timeout: int | float,
    output: Path | None,
    database: Path | None = None,
    *,
    reset: bool = False,
) -> int:
    if not str(group_id or "").strip():
        raise ValueError("an explicit test group is required")
    settings = load_settings(config)
    ws_url, token = websocket_settings(settings["onebot"])
    headers = {"Authorization": f"Bearer {token}"} if token else None
    state_root = Path(settings["storage"]["root"]) / "state"
    destination = output or state_root / "url_probe.json"
    checkpoint_path = database or state_root / "url_probe.sqlite3"
    scope = _scope_hash(str(group_id))
    connection = _open_checkpoint(checkpoint_path)
    if reset:
        _reset_scope(connection, scope)
    state, resumed = _load_state(connection, scope)
    target_segments = max(1, int(target_segments))
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0.01, float(timeout))
    delay = 1.0
    timed_out = False

    def save_output() -> None:
        _atomic_output(
            destination,
            _result(state, target_segments, timed_out=timed_out, resumed=resumed),
        )

    try:
        save_output()
        while int(state.get("estimated_slots") or 0) < target_segments:
            remaining = deadline - loop.time()
            if remaining <= 0:
                timed_out = True
                break
            state["connection_attempts"] = int(state.get("connection_attempts") or 0) + 1
            try:
                async with websockets.connect(
                    ws_url,
                    additional_headers=headers,
                    ping_interval=30,
                    ping_timeout=30,
                    open_timeout=min(10.0, remaining),
                    max_size=16 * 1024 * 1024,
                ) as websocket:
                    if int(state.get("disconnects") or 0) > 0:
                        state["reconnects"] = int(state.get("reconnects") or 0) + 1
                    state["connection_state"] = "connected"
                    state["last_disconnect_error"] = None
                    _checkpoint_state(connection, scope, state)
                    save_output()
                    while int(state.get("estimated_slots") or 0) < target_segments:
                        remaining = deadline - loop.time()
                        if remaining <= 0:
                            timed_out = True
                            break
                        try:
                            payload = await asyncio.wait_for(websocket.recv(), timeout=remaining)
                        except asyncio.TimeoutError:
                            timed_out = True
                            break
                        # A received frame proves the connection recovered;
                        # immediate connect/close loops keep the exponential
                        # backoff instead of resetting it prematurely.
                        delay = 1.0
                        try:
                            event = json.loads(payload)
                        except (TypeError, json.JSONDecodeError):
                            continue
                        if (
                            not isinstance(event, dict)
                            or event.get("post_type") != "message"
                            or event.get("message_type") != "group"
                            or str(event.get("group_id") or "") != str(group_id)
                        ):
                            continue
                        state, _inserted = _checkpoint_event(
                            connection, scope, state, event
                        )
                        save_output()
                    if timed_out:
                        break
            except (OSError, TimeoutError, WebSocketException) as exc:
                state["disconnects"] = int(state.get("disconnects") or 0) + 1
                state["connection_state"] = "disconnected"
                state["last_disconnect_error"] = type(exc).__name__
                _checkpoint_state(connection, scope, state)
                save_output()
                remaining = deadline - loop.time()
                if remaining <= 0:
                    timed_out = True
                    break
                await asyncio.sleep(min(delay, remaining))
                delay = min(60.0, delay * 2)

        if int(state.get("estimated_slots") or 0) >= target_segments:
            state["connection_state"] = "complete"
        elif timed_out:
            state["connection_state"] = "partial_timeout"
        _checkpoint_state(connection, scope, state)
        rendered = _atomic_output(
            destination,
            _result(state, target_segments, timed_out=timed_out, resumed=resumed),
        )
        print(rendered, end="")
        return 0 if int(state.get("estimated_slots") or 0) >= target_segments else 2
    finally:
        connection.close()
        checkpoint_path.chmod(0o600)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Collect resumable, privacy-redacted standard/raw URL-shape statistics."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("/data/qq-image-collector/config/collector_config.json"),
    )
    parser.add_argument("--group", required=True, help="isolated test group ID (never persisted)")
    parser.add_argument("--image-segments", type=int, default=200)
    parser.add_argument("--timeout", type=int, default=6 * 3600)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()
    return asyncio.run(
        receive_sample(
            args.config,
            args.group,
            max(1, args.image_segments),
            max(1, args.timeout),
            args.output,
            args.database,
            reset=args.reset,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
