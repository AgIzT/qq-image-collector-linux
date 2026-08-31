#!/usr/bin/env python3
"""Rebuild a pre-server local collection into the layout the server uses.

Acts on the manifest produced by survey_legacy_local.py, so the classification
is the current parser's rather than the one the folders were named by. Three
destinations:

    final/<category>/<YYYY-MM-DD>/   images today's rules accept and the server
                                    does not already hold - the set to upload
    _rejected/                      no usable generation metadata; the library's
                                    contract excludes these, so they must not go
                                    into final/ where an archiver would treat
                                    them as library content
    _already_on_server/             same sha256 already in the server's assets
                                    table, so uploading them again would put two
                                    copies of one image in the archive

Nothing is deleted. Moves are renames within one volume, so no bytes travel.
Re-running is safe: a file already at its destination is left alone.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--server-hashes", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    records = json.loads(args.manifest.read_text(encoding="utf-8"))
    server = {
        line.strip()
        for line in args.server_hashes.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }

    plan: list[tuple[Path, Path, str]] = []
    for record in records:
        source = Path(str(record["path"]))
        name = source.name
        if not record["accepted"]:
            target = args.root / "_rejected" / str(record["old_category"]) / name
            bucket = "rejected"
        elif record["sha256"] in server:
            target = args.root / "_already_on_server" / name
            bucket = "on_server"
        else:
            day = str(record["day"] or "unknown-date")
            target = args.root / "final" / str(record["new_category"]) / day / name
            bucket = "upload"
        plan.append((source, target, bucket))

    counts: dict[str, int] = {}
    for _s, _t, bucket in plan:
        counts[bucket] = counts.get(bucket, 0) + 1
    print("plan:", counts, flush=True)

    # Extensions come from the decoded format on the server, so a file named
    # .png that actually decodes as WebP would be named differently there.
    mismatched = [
        r for r in records
        if r.get("accepted")
        and r.get("extension")
        and not str(r["path"]).lower().endswith(str(r["extension"]).lower())
    ]
    print(f"extension mismatch (reported, not renamed): {len(mismatched)}", flush=True)
    for record in mismatched[:5]:
        print(f"  {Path(str(record['path'])).name} decodes as {record['format']}")

    if args.dry_run:
        print("dry-run, nothing moved")
        return 0

    moved = skipped = missing = collided = 0
    for source, target, _bucket in plan:
        if not source.exists():
            if target.exists():
                skipped += 1
            else:
                missing += 1
                print(f"  missing: {source}")
            continue
        if target.exists():
            collided += 1
            print(f"  target exists, left in place: {target}")
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        source.replace(target)
        moved += 1
        if moved % 500 == 0:
            print(f"  {moved}/{len(plan)}", flush=True)

    print(f"moved={moved} skipped={skipped} missing={missing} collided={collided}", flush=True)

    for directory in sorted(args.root.rglob("*"), key=lambda p: -len(p.parts)):
        if directory.is_dir() and not any(directory.iterdir()):
            directory.rmdir()
            print(f"  removed empty: {directory.relative_to(args.root)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
