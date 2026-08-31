#!/usr/bin/env python3
"""Classify a pre-server local collection with the current parser, without moving anything.

The Windows-era collection was sorted by an older parser under folder names that
have since been renamed, and the acceptance rules themselves have moved on - a
sample showed images in the old catch-all folder that today's parser rejects
outright for having no usable generation metadata. Deciding the new layout from
the old folder names would carry both mistakes forward, so re-read every file
and write a manifest of what the current rules say.

Read-only. Produces JSON for a separate move step to act on.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from metadata_reader import extension_for_format, inspect_image  # noqa: E402
from qq_image_collector.database import category_for_source  # noqa: E402
from qq_image_collector.downloader import METADATA_DECODE_ERRORS  # noqa: E402


NAME = re.compile(r"^(\d{4}-\d{2}-\d{2})_\d{2}-\d{2}-\d{2}_g(\d+)_u(\d+)_([0-9a-f]+)\.")


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    files = sorted(p for p in args.root.rglob("*") if p.is_file())
    print(f"scanning {len(files)} files", flush=True)

    records = []
    for index, path in enumerate(files, start=1):
        record: dict[str, object] = {
            "path": str(path),
            "old_category": path.parent.name,
            "size": path.stat().st_size,
        }
        matched = NAME.match(path.name)
        record["day"] = matched.group(1) if matched else None
        record["group_id"] = matched.group(2) if matched else None
        record["sender_uin"] = matched.group(3) if matched else None
        try:
            result = inspect_image(path)
        except METADATA_DECODE_ERRORS as exc:
            record["accepted"] = False
            record["source"] = None
            record["error"] = type(exc).__name__
        else:
            record["accepted"] = bool(result.accepted)
            record["source"] = result.source
            record["width"] = result.width
            record["height"] = result.height
            record["format"] = result.image_format
            record["extension"] = extension_for_format(result.image_format)
        record["new_category"] = (
            category_for_source(record.get("source")) if record.get("accepted") else None
        )
        record["sha256"] = sha256_of(path)
        records.append(record)
        if index % 250 == 0:
            print(f"  {index}/{len(files)}", flush=True)

    args.out.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {args.out} ({len(records)} records)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
