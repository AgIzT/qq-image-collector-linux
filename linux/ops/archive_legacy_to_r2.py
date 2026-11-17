#!/usr/bin/env python3
"""Upload the pre-server local collection to the same R2 archive.

These 5,090 images were collected on Windows before the server existed, so
unlike everything else in the archive they have no row in ``assets`` or
``images``: none of the queries in IMAGE_LIBRARY_SPEC.md can see them, and the
metadata has to be re-read from the files rather than looked up. That is the
whole reason this is a separate script - the upload, the key layout and the
record shape are deliberately identical to ``archive_to_r2.py``, which it
imports rather than reimplements.

Runs on the machine that holds the files (Windows), not on the server.

    python linux/ops/archive_legacy_to_r2.py --root "D:/program/群聊图片获取/final" \
        --config r2_config.json --state legacy_archive.sqlite3

The day indexes it writes carry ``origin: "legacy"`` and live under
``data/legacy/days/``. They are kept separate from the server's days because
the two have different database状态, not because the images differ - the
``originals/`` space is shared, so an image that exists in both batches lands
on one key and is stored once.

Nothing is deleted. Removing the local copies is a separate decision.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import importlib.util
import json
import re
import sqlite3
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO))


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


archive = _load("archive_to_r2", HERE / "archive_to_r2.py")
model_index = _load("build_model_index", HERE / "build_model_index.py")

from metadata_reader import extension_for_format, inspect_image  # noqa: E402
from qq_image_collector.database import category_for_source  # noqa: E402
from qq_image_collector.downloader import METADATA_DECODE_ERRORS  # noqa: E402

log = archive.log
TZ = archive.TZ

# YYYY-MM-DD_HH-MM-SS_g<group>_u<sender>_<sha prefix><ext>
FILENAME = re.compile(r"^(\d{4}-\d{2}-\d{2})_(\d{2})-(\d{2})-(\d{2})_g(\d+)_u(\d+)_[0-9a-f]+\.")


def open_state(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS scanned (
            path        TEXT PRIMARY KEY,
            sha256      TEXT NOT NULL,
            record      TEXT NOT NULL,
            private     TEXT NOT NULL,
            metadata    TEXT,
            scanned_at  INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS scanned_sha ON scanned(sha256);
        CREATE TABLE IF NOT EXISTS uploaded (
            sha256      TEXT PRIMARY KEY,
            uploaded_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS meta_uploaded (
            sha256      TEXT PRIMARY KEY,
            uploaded_at INTEGER NOT NULL
        );
        """
    )
    conn.commit()
    return conn


def parse_name(name: str) -> dict:
    matched = FILENAME.match(name)
    if not matched:
        return {}
    day, hour, minute, second, group, sender = matched.groups()
    sent = dt.datetime.strptime(f"{day} {hour}:{minute}:{second}", "%Y-%m-%d %H:%M:%S")
    return {
        "day": day,
        "sentAt": int(sent.replace(tzinfo=TZ).timestamp()),
        "groupId": group,
        "senderUin": sender,
    }


def build_record(*, sha, day, category, source, fields, model, family,
                 ext, width, height, size, sent_at) -> dict:
    """Shape one listing record, exactly as archive_to_r2 shapes the server's."""
    shaped = archive.SHAPERS.get(source, lambda _f: {})(fields)
    record = {
        "id": sha,
        "title": archive.title_for(shaped.get("tags"), shaped.get("model") or model, day),
        "path": [category, day],
        "ext": ext,
        "width": width,
        "height": height,
        "size": size,
        "sentAt": sent_at,
        "category": category,
        "metadataSource": source,
        "modelFamily": family,
        "origin": "legacy",
    }
    if model:
        record["model"] = model
    for key in ("tags", "negative", "params", "model", "hasWorkflow", "promptSource"):
        value = shaped.get(key)
        if value:
            record.setdefault(key, value)
    return record


def reshape(state) -> int:
    """Rebuild the records from the metadata already stored, without touching the files.

    The prompt shapers get refined - which node holds a ComfyUI prompt is a
    convention that keeps turning out to have another variant. Re-reading 11 GB
    off disk and re-uploading it to answer that would be absurd when the parsed
    metadata is sitting in this database, so re-derive from there. The server
    side has the same escape hatch in ``archive_to_r2.py --reindex``.
    """
    rows = state.execute("SELECT path, sha256, record, metadata FROM scanned").fetchall()
    changed = 0
    for path, sha, record_json, metadata_json in rows:
        old = json.loads(record_json)
        stored = json.loads(metadata_json) if metadata_json else {}
        fields = stored.get("metadata") or {}
        category = old["category"]
        model, family = model_index.extract(
            json.dumps(fields, ensure_ascii=False), category
        )
        new = build_record(
            sha=sha, day=old["path"][1], category=category,
            source=old["metadataSource"], fields=fields, model=model, family=family,
            ext=old["ext"], width=old["width"], height=old["height"],
            size=old["size"], sent_at=old.get("sentAt"),
        )
        if new != old:
            state.execute("UPDATE scanned SET record = ? WHERE path = ?",
                          (json.dumps(new, ensure_ascii=False), path))
            changed += 1
    state.commit()
    log(f"reshape: {changed} of {len(rows)} records changed")
    return changed


def scan(state, root: Path, workers: int) -> int:
    """Hash and re-parse every file. The slow half; resumable and idempotent."""
    files = sorted(p for p in root.rglob("*") if p.is_file())
    done = {row[0] for row in state.execute("SELECT path FROM scanned")}
    todo = [p for p in files if str(p) not in done]
    log(f"scan: {len(files)} files, {len(todo)} to read")
    if not todo:
        return 0

    lock = threading.Lock()
    checkpoint = archive.Checkpoint(every=200)
    processed = skipped = 0

    def read(path: Path):
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        try:
            result = inspect_image(path)
        except METADATA_DECODE_ERRORS as exc:
            return path, digest.hexdigest(), None, type(exc).__name__
        return path, digest.hexdigest(), result, None

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for future in concurrent.futures.as_completed([pool.submit(read, p) for p in todo]):
            try:
                path, sha, result, error = future.result()
            except Exception as exc:  # noqa: BLE001
                log(f"  read failed: {exc}")
                continue
            if result is None or not result.accepted:
                with lock:
                    skipped += 1
                continue

            named = parse_name(path.name)
            day = named.get("day") or path.parent.name
            category = category_for_source(result.source) or path.parent.parent.name
            fields = result.fields or {}
            blob = json.dumps(fields, ensure_ascii=False)
            model, family = model_index.extract(blob, category)

            record = build_record(
                sha=sha, day=day, category=category, source=result.source, fields=fields,
                model=model, family=family, ext=extension_for_format(result.image_format),
                width=result.width, height=result.height, size=path.stat().st_size,
                sent_at=named.get("sentAt"),
            )

            private = {
                "sha256": sha, "file": path.name,
                "groupId": named.get("groupId"), "senderUin": named.get("senderUin"),
                "sentAt": named.get("sentAt"), "category": category,
            }
            with lock:
                state.execute(
                    "INSERT OR REPLACE INTO scanned VALUES (?,?,?,?,?,?)",
                    (str(path), sha, json.dumps(record, ensure_ascii=False),
                     json.dumps(private, ensure_ascii=False),
                     json.dumps({"sha256": sha, "metadataSource": result.source,
                                 "parserVersion": "5", "metadata": fields}, ensure_ascii=False),
                     int(time.time())),
                )
                processed += 1
                if checkpoint.due(processed):
                    state.commit()
                    log(f"  scanned {processed}/{len(todo)}")
    state.commit()
    log(f"scan: {processed} usable, {skipped} rejected by the current parser")
    return processed


def upload(state, client, workers: int) -> int:
    """Send the bytes and the raw metadata. Keys are the same as the server's."""
    rows = state.execute(
        "SELECT s.path, s.sha256, s.record, s.metadata FROM scanned s "
        "LEFT JOIN uploaded u ON u.sha256 = s.sha256 WHERE u.sha256 IS NULL "
        "GROUP BY s.sha256"
    ).fetchall()
    meta_done = {r[0] for r in state.execute("SELECT sha256 FROM meta_uploaded")}
    log(f"upload: {len(rows)} objects")
    if not rows:
        return 0

    lock = threading.Lock()
    checkpoint = archive.Checkpoint()
    sent = 0
    total_bytes = 0

    def send(row):
        path, sha, record_json, metadata_json = row
        record = json.loads(record_json)
        body = Path(path).read_bytes()
        actual = hashlib.sha256(body).hexdigest()
        if actual != sha:
            raise RuntimeError(f"{path} changed since the scan")
        client.put_bytes(
            archive.object_key(sha, record["ext"]), body,
            archive.content_type_for(record["ext"]), sha=actual,
            cache_control="public, max-age=31536000, immutable",
        )
        if sha not in meta_done and metadata_json:
            client.put_json(archive.meta_key(sha), json.loads(metadata_json),
                            cache_control="public, max-age=31536000, immutable")
        return sha, len(body), sha not in meta_done

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for future in concurrent.futures.as_completed([pool.submit(send, r) for r in rows]):
            try:
                sha, size, wrote_meta = future.result()
            except Exception as exc:  # noqa: BLE001
                log(f"  FAILED: {exc}")
                continue
            with lock:
                state.execute("INSERT OR REPLACE INTO uploaded VALUES (?,?)", (sha, int(time.time())))
                if wrote_meta:
                    state.execute("INSERT OR REPLACE INTO meta_uploaded VALUES (?,?)",
                                  (sha, int(time.time())))
                sent += 1
                total_bytes += size
                if checkpoint.due(sent):
                    state.commit()
                    log(f"  {sent}/{len(rows)} ({total_bytes / 1048576:.0f} MB)")
    state.commit()
    log(f"upload: {sent} objects, {total_bytes / 1048576:.0f} MB")
    return sent


def write_indexes(state, client) -> list[dict]:
    """One index per day, same shape as the server's, marked origin=legacy."""
    days: dict[str, list] = {}
    for record_json, private_json, sha in state.execute(
        "SELECT s.record, s.private, s.sha256 FROM scanned s "
        "JOIN uploaded u ON u.sha256 = s.sha256 GROUP BY s.sha256"
    ):
        record = json.loads(record_json)
        days.setdefault(record["path"][1], []).append((record, json.loads(private_json)))

    summary = []
    for day, pairs in sorted(days.items()):
        entries = [record for record, _ in pairs]
        categories, families = {}, {}
        for entry in entries:
            categories[entry["category"]] = categories.get(entry["category"], 0) + 1
            family = entry.get("modelFamily")
            if family:
                families[family] = families.get(family, 0) + 1
        total_bytes = sum(e["size"] for e in entries)
        index = {
            "id": day, "type": "day", "origin": "legacy",
            "generatedAt": dt.datetime.now(TZ).isoformat(timespec="seconds"),
            "entryCount": len(entries), "bytes": total_bytes,
            "categories": categories, "modelFamilies": families,
            "entries": sorted(entries, key=lambda e: e.get("sentAt") or 0),
        }
        client.put_json(f"data/legacy/days/{day}.json", index)
        client.put_json(
            f"private/legacy/days/{day}.json",
            {"day": day, "origin": "legacy", "generatedAt": index["generatedAt"],
             "records": [private for _, private in pairs]},
            cache_control="private, no-store",
        )
        summary.append({"day": day, "origin": "legacy", "asset_count": len(entries),
                        "bytes": total_bytes, "categories": categories, "families": families})
        log(f"  {day}: {len(entries)} entries, {total_bytes / 1048576:.0f} MB")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", type=Path,
                        help="the final/ directory of the pre-server collection "
                             "(not needed with --reindex)")
    parser.add_argument("--config", type=Path, required=True, help="R2 credentials, JSON")
    parser.add_argument("--bucket", help="override the bucket in --config; pass this when "
                                         "reusing a credentials file written for another bucket")
    parser.add_argument("--state", type=Path, default=Path("legacy_archive.sqlite3"))
    parser.add_argument("--out", type=Path, default=Path("legacy_days.json"),
                        help="day summary to merge into the server's archive state")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--scan-only", action="store_true")
    parser.add_argument("--reindex", action="store_true",
                        help="re-derive the records from the metadata already scanned and "
                             "rewrite the day indexes; does not read files or re-upload images")
    args = parser.parse_args()

    if not args.reindex and not (args.root and args.root.is_dir()):
        raise SystemExit(f"--root must be a directory (got {args.root})")
    cfg = json.loads(args.config.read_text(encoding="utf-8"))
    if args.bucket:
        cfg["bucket"] = args.bucket
    cfg.setdefault("bucket", archive.DEFAULT_BUCKET)
    log(f"bucket: {cfg['bucket']}")
    state = open_state(args.state)

    if args.reindex:
        reshape(state)
        client = archive.R2Client(cfg)
        summary = write_indexes(state, client)
        args.out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"wrote {args.out}: {len(summary)} days")
        return 0

    scan(state, args.root, args.workers)
    if args.scan_only:
        return 0

    client = archive.R2Client(cfg)
    upload(state, client, args.workers)
    summary = write_indexes(state, client)
    args.out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"wrote {args.out}: {len(summary)} days")
    log("now merge it on the server, so data/index.json lists these days too:")
    log(f"  scp {args.out} <server>:/tmp/{args.out.name}")
    log(f"  ssh <server> 'python3 /opt/qq-ai-image-collector-linux-event-a1cfe1a/linux/ops/"
        f"archive_to_r2.py --merge-days /tmp/{args.out.name}'")
    return 0


if __name__ == "__main__":
    sys.exit(main())
