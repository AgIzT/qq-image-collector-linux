from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import sqlite3
from pathlib import Path


FILENAME_TIMEZONE = dt.timezone(dt.timedelta(hours=8))
NAME_PATTERN = re.compile(
    r"^(?P<time>\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})_"
    r"g(?P<group>\d+)_u(?P<sender>\d+)_"
    r"(?P<digest>[0-9a-f]{10}|[0-9a-f]{16}|[0-9a-f]{64})"
    r"(?P<extension>\.[^.]+)$"
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify(config_path: Path) -> dict[str, int]:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    storage_root = Path(config["storage"]["root"]).resolve()
    final_root = (storage_root / "final").resolve()
    database_path = Path(config["storage"]["database"])
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    errors: list[str] = []
    try:
        assets = connection.execute(
            """
            SELECT sha256, local_path, category, file_extension,
                   canonical_group_id, canonical_sender_uin,
                   canonical_sent_at
            FROM assets ORDER BY local_path
            """
        ).fetchall()
        unique_accepted = int(
            connection.execute(
                "SELECT count(DISTINCT sha256) FROM images "
                "WHERE status='accepted' AND sha256 IS NOT NULL"
            ).fetchone()[0]
            or 0
        )
        missing_provenance = int(
            connection.execute(
                "SELECT count(*) FROM images WHERE status='accepted' "
                "AND (coalesce(group_uin, '')='' OR coalesce(sender_uin, '')='')"
            ).fetchone()[0]
            or 0
        )
        missing_assets = int(
            connection.execute(
                """
                SELECT count(*) FROM images AS occurrence
                LEFT JOIN assets AS asset ON asset.sha256=occurrence.sha256
                WHERE occurrence.status='accepted' AND asset.sha256 IS NULL
                """
            ).fetchone()[0]
            or 0
        )
        path_mismatches = int(
            connection.execute(
                """
                SELECT count(*) FROM images AS occurrence
                JOIN assets AS asset ON asset.sha256=occurrence.sha256
                WHERE occurrence.status='accepted'
                  AND occurrence.local_path<>asset.local_path
                """
            ).fetchone()[0]
            or 0
        )
    finally:
        connection.close()

    if len(assets) != unique_accepted:
        errors.append(
            f"asset count {len(assets)} != unique accepted count {unique_accepted}"
        )
    if missing_provenance:
        errors.append(f"accepted occurrences missing group/sender: {missing_provenance}")
    if missing_assets:
        errors.append(f"accepted occurrences missing asset row: {missing_assets}")
    if path_mismatches:
        errors.append(f"accepted occurrence paths differ from asset path: {path_mismatches}")

    database_paths: set[Path] = set()
    total_bytes = 0
    for index, asset in enumerate(assets, 1):
        digest = str(asset["sha256"])
        path = Path(str(asset["local_path"])).resolve()
        database_paths.add(path)
        if not path.is_file():
            errors.append(f"missing file: {path}")
            continue
        if not path.is_relative_to(final_root):
            errors.append(f"asset outside final repository: {path}")
            continue
        total_bytes += path.stat().st_size
        match = NAME_PATTERN.fullmatch(path.name)
        if not match:
            errors.append(f"invalid provenance filename: {path.name}")
            continue
        expected_time = dt.datetime.fromtimestamp(
            int(asset["canonical_sent_at"] or 0), FILENAME_TIMEZONE
        ).strftime("%Y-%m-%d_%H-%M-%S")
        if match.group("time") != expected_time:
            errors.append(f"timestamp mismatch: {path.name}")
        if match.group("group") != str(asset["canonical_group_id"] or ""):
            errors.append(f"group mismatch: {path.name}")
        if match.group("sender") != str(asset["canonical_sender_uin"] or ""):
            errors.append(f"sender mismatch: {path.name}")
        if not digest.startswith(match.group("digest")):
            errors.append(f"digest prefix mismatch: {path.name}")
        if path.parent.name != str(asset["category"]):
            errors.append(f"category mismatch: {path}")
        if path.suffix.casefold() != str(asset["file_extension"] or "").casefold():
            errors.append(f"extension mismatch: {path.name}")
        if sha256(path) != digest:
            errors.append(f"SHA-256 mismatch: {path}")
        if index % 100 == 0 or index == len(assets):
            print(f"verified_assets={index}/{len(assets)}", flush=True)

    physical_files = {
        path.resolve() for path in final_root.rglob("*") if path.is_file()
    }
    extras = physical_files - database_paths
    if extras:
        errors.append(f"untracked final files: {len(extras)}")
    missing_physical = database_paths - physical_files
    if missing_physical:
        errors.append(f"database paths missing from final folders: {len(missing_physical)}")
    gifs = [path for path in physical_files if path.suffix.casefold() == ".gif"]
    if gifs:
        errors.append(f"GIF files remain: {len(gifs)}")
    temp_files = [path for path in (storage_root / "temp").rglob("*") if path.is_file()]
    if temp_files:
        errors.append(f"temporary files remain: {len(temp_files)}")

    if errors:
        for error in errors[:20]:
            print(f"ERROR: {error}")
        if len(errors) > 20:
            print(f"ERROR: and {len(errors) - 20} more")
        raise RuntimeError(f"provenance verification failed with {len(errors)} error(s)")
    return {
        "assets": len(assets),
        "unique_accepted": unique_accepted,
        "accepted_occurrences_missing_provenance": missing_provenance,
        "physical_files": len(physical_files),
        "total_bytes": total_bytes,
        "gif_files": len(gifs),
        "temp_files": len(temp_files),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.config), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
