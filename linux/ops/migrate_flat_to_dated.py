#!/usr/bin/env python3
"""Move images stored before the date-directory layout into their send-date folder.

Images collected before 2026-08-22 sit directly under ``final/<category>/`` while
everything since lands in ``final/<category>/<YYYY-MM-DD>/``.  Two layouts means
every later tool - above all the archiver, which deletes - needs two code paths,
and a special case in a delete path is where data goes missing.  One pass now
removes it permanently.

Source and destination share a filesystem, so each move is a rename: no bytes
travel and no extra space is needed.

The run is idempotent and resumable.  If it is interrupted after a file moved
but before its rows were updated, the next run sees the source gone and the
destination present and repairs the rows alone.

    migrate_flat_to_dated.py --config <collector_config.json> [--dry-run]
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
import time
from pathlib import Path

from qq_image_collector.config import load_settings
from qq_image_collector.database import (
    FILENAME_TIMEZONE,
    connect_database,
    sha256_file,
)


DATED_DIRECTORY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
NAME_PREFIX = re.compile(r"^(\d{4}-\d{2}-\d{2})_")


def day_from_sent_at(sent_at: int | None) -> str | None:
    try:
        value = int(sent_at or 0)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    try:
        return dt.datetime.fromtimestamp(value, FILENAME_TIMEZONE).strftime("%Y-%m-%d")
    except (OSError, OverflowError, ValueError):
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--batch", type=int, default=200)
    parser.add_argument("--sleep", type=float, default=0.05)
    parser.add_argument("--limit", type=int, default=0, help="0 = 全部")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--trust-filename",
        action="store_true",
        help="日期不一致时以文件名为准（reclassify 改过 canonical_sent_at 会造成这种不一致）",
    )
    args = parser.parse_args()

    settings = load_settings(args.config)
    storage_root = Path(settings["storage"]["root"])
    connection = connect_database(settings["storage"]["database"], initialize=False)

    rows = connection.execute(
        "SELECT sha256, local_path, canonical_sent_at FROM assets WHERE local_path IS NOT NULL"
    ).fetchall()

    pending: list[tuple[str, Path, Path]] = []
    already = skipped = 0
    for digest, raw_path, sent_at in rows:
        path = Path(str(raw_path))
        if DATED_DIRECTORY.match(path.parent.name):
            already += 1
            continue
        day = day_from_sent_at(sent_at)
        named = NAME_PREFIX.match(path.name)
        # The filename was generated from sent_at by accepted_path_for, so the
        # two agree by construction.  Disagreement means an assumption this
        # migration rests on is wrong, and the safe response is to leave the
        # file alone and say so rather than to guess a destination.
        if day is None or named is None or named.group(1) != day:
            # reclassify_repository can rewrite canonical_sent_at after the file
            # was named, so the two legitimately disagree for a handful of rows.
            # The filename is what a person reads, so trusting it keeps name and
            # location consistent - but only when asked for explicitly.
            if args.trust_filename and named is not None:
                pending.append((str(digest), path, path.parent / named.group(1) / path.name))
                continue
            print(f"  skip (date mismatch): {path.name} sent_at_day={day}", file=sys.stderr)
            skipped += 1
            continue
        pending.append((str(digest), path, path.parent / day / path.name))
        if args.limit and len(pending) >= args.limit:
            break

    print(f"已在日期目录: {already}  待迁移: {len(pending)}  跳过: {skipped}", flush=True)
    if args.dry_run:
        for digest, source, target in pending[:5]:
            print(f"  would move {source.name} -> {target.parent.name}/")
        print("dry-run，未做任何改动")
        connection.close()
        return 0

    moved = repaired = collided = missing = 0
    for index, (digest, source, target) in enumerate(pending, start=1):
        target.parent.mkdir(parents=True, exist_ok=True)
        if not source.exists():
            if target.is_file():
                repaired += 1  # interrupted earlier run; rows still point at the old path
            else:
                missing += 1
                print(f"  missing on disk: {source}", file=sys.stderr)
                continue
        elif target.exists():
            # Never overwrite. Identical content means the move already happened
            # under a different row; anything else needs a human.
            if sha256_file(target) == digest:
                source.unlink(missing_ok=True)
                repaired += 1
            else:
                collided += 1
                print(f"  collision, left in place: {source}", file=sys.stderr)
                continue
        else:
            source.replace(target)
            moved += 1

        connection.execute(
            "UPDATE assets SET local_path=?, updated_at=? WHERE sha256=?",
            (str(target), int(time.time()), digest),
        )
        connection.execute(
            "UPDATE images SET local_path=?, updated_at=? WHERE local_path=?",
            (str(target), int(time.time()), str(source)),
        )
        if index % args.batch == 0:
            connection.commit()
            print(f"  {index}/{len(pending)} moved={moved} repaired={repaired}", flush=True)
            time.sleep(args.sleep)
    connection.commit()

    print(
        f"完成: moved={moved} repaired={repaired} collided={collided} missing={missing}",
        flush=True,
    )
    remaining = sum(
        1
        for (_d, raw, _s) in connection.execute(
            "SELECT sha256, local_path, canonical_sent_at FROM assets WHERE local_path IS NOT NULL"
        )
        if not DATED_DIRECTORY.match(Path(str(raw)).parent.name)
    )
    print(f"仍未进日期目录的 assets 行: {remaining}", flush=True)
    connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
