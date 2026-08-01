from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
from pathlib import Path
from typing import Any


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def copy_images(source: Path, destination: Path) -> tuple[int, int]:
    count = 0
    size = 0
    in_place = source.resolve() == destination.resolve()
    final_root = source / "final"
    if not final_root.is_dir():
        return count, size
    for path in final_root.rglob("*"):
        if not path.is_file():
            continue
        target = destination / path.relative_to(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        if in_place:
            pass
        elif target.is_file() and sha256(target) == sha256(path):
            pass
        else:
            temporary = target.with_suffix(target.suffix + ".migrating")
            temporary.unlink(missing_ok=True)
            shutil.copy2(path, temporary)
            os.replace(temporary, target)
        count += 1
        size += path.stat().st_size
    return count, size


def rewrite_path(value: str, source_prefix: str, destination: Path) -> str:
    normalized = value.replace("\\", "/")
    prefix = source_prefix.replace("\\", "/").rstrip("/")
    if normalized.casefold() == prefix.casefold():
        return destination.as_posix()
    if normalized.casefold().startswith(prefix.casefold() + "/"):
        suffix = normalized[len(prefix) :].lstrip("/")
        return (destination / Path(*suffix.split("/"))).as_posix()
    marker = "/final/"
    index = normalized.casefold().find(marker)
    if index >= 0:
        suffix = normalized[index + 1 :]
        return (destination / Path(*suffix.split("/"))).as_posix()
    return value


def column_names(connection: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row[1])
        for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
    }


def rewrite_database(
    database: Path,
    source_prefix: str,
    destination: Path,
) -> dict[str, int]:
    stats: dict[str, int] = {}
    with sqlite3.connect(database) as connection:
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        for table in ("images", "assets"):
            if table not in tables or "local_path" not in column_names(connection, table):
                continue
            rows = connection.execute(
                f"SELECT DISTINCT local_path FROM {table} WHERE local_path IS NOT NULL"
            ).fetchall()
            changed = 0
            for (old_value,) in rows:
                old = str(old_value)
                new = rewrite_path(old, source_prefix, destination)
                if new == old:
                    continue
                connection.execute(
                    f"UPDATE {table} SET local_path=? WHERE local_path=?",
                    (new, old),
                )
                changed += 1
            stats[table] = changed
        connection.commit()
    return stats


def verify_database(
    database: Path,
    logical_root: Path,
    physical_root: Path,
) -> dict[str, int]:
    checked = 0
    missing = 0
    mismatched = 0
    with sqlite3.connect(database) as connection:
        rows = connection.execute(
            "SELECT sha256, min(local_path) FROM images "
            "WHERE status='accepted' AND sha256 IS NOT NULL AND local_path IS NOT NULL "
            "GROUP BY sha256"
        ).fetchall()
    for expected, value in rows:
        stored = Path(str(value))
        try:
            relative = stored.relative_to(logical_root)
            path = physical_root / relative
        except ValueError:
            path = stored
        if not path.is_file():
            missing += 1
            continue
        checked += 1
        if sha256(path).casefold() != str(expected).casefold():
            mismatched += 1
    if missing or mismatched:
        raise RuntimeError(
            f"Migration verification failed: missing={missing}, sha256_mismatch={mismatched}"
        )
    return {"verified": checked, "missing": missing, "sha256_mismatch": mismatched}


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Install a Windows snapshot into the Linux persistent repository. "
            "The snapshot may be uploaded directly into the destination to avoid "
            "using twice the image disk space."
        )
    )
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument(
        "--destination",
        type=Path,
        default=Path(__file__).resolve().parent / "runtime" / "repository",
    )
    parser.add_argument(
        "--source-prefix",
        required=True,
        help=(
            "Windows repository root recorded in the snapshot database, "
            r"for example D:\qq-image-collector"
        ),
    )
    parser.add_argument("--replace-database", action="store_true")
    args = parser.parse_args()

    snapshot = args.snapshot.resolve()
    destination = args.destination.resolve()
    source_database = snapshot / "state" / "collector_state.sqlite3"
    if not source_database.is_file():
        parser.error(f"Snapshot database does not exist: {source_database}")
    database = destination / "state" / "collector_state.sqlite3"
    if database.exists() and not args.replace_database:
        parser.error(
            "Destination database already exists; stop the stack and pass "
            "--replace-database only for an intentional cutover"
        )

    destination.mkdir(parents=True, exist_ok=True)
    copied_files, copied_bytes = copy_images(snapshot, destination)
    database.parent.mkdir(parents=True, exist_ok=True)
    temporary = database.with_suffix(".sqlite3.migrating")
    temporary.unlink(missing_ok=True)
    with sqlite3.connect(source_database) as source_connection:
        with sqlite3.connect(temporary) as destination_connection:
            source_connection.backup(destination_connection)
    rewritten = rewrite_database(
        temporary,
        args.source_prefix,
        Path("/data/qq-image-collector"),
    )
    verification = verify_database(
        temporary,
        Path("/data/qq-image-collector"),
        destination,
    )
    os.replace(temporary, database)

    report: dict[str, Any] = {
        "copied_files": copied_files,
        "copied_bytes": copied_bytes,
        "rewritten_paths": rewritten,
        **verification,
        "database": str(database),
    }
    report_path = destination / "state" / "linux_migration_report.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
