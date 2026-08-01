#!/usr/bin/env python3
"""Safely reclassify final and quarantined images with the current parser."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from collector import ASSET_PARSER_VERSION, category_for_source
from metadata_reader import inspect_image


REJECT_REASON = "current parser found no valid generation metadata"


@dataclass(frozen=True)
class PlannedFile:
    source_path: Path
    sha256: str
    accepted: bool
    metadata_source: str | None
    category: str | None
    target_path: Path
    metadata_json: str | None
    width: int | None
    height: int | None
    image_format: str | None
    error: str | None


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def category_from_path(path: Path, final_root: Path, quarantine_root: Path) -> str:
    try:
        return path.relative_to(final_root).parts[0]
    except ValueError:
        relative = path.relative_to(quarantine_root)
        return f"quarantine/{relative.parts[0] if len(relative.parts) > 1 else 'unclassified'}"


def discover_files(final_root: Path, quarantine_root: Path) -> list[Path]:
    paths = {
        path.resolve()
        for root in (final_root, quarantine_root)
        if root.exists()
        for path in root.rglob("*")
        if path.is_file()
    }
    return sorted(paths)


def create_plan(final_root: Path, quarantine_root: Path) -> tuple[list[PlannedFile], dict[str, Any]]:
    entries: list[PlannedFile] = []
    categories: Counter[str] = Counter()
    transitions: Counter[str] = Counter()
    errors: Counter[str] = Counter()

    for path in discover_files(final_root, quarantine_root):
        digest = sha256_file(path)
        current = category_from_path(path, final_root, quarantine_root)
        try:
            result = inspect_image(path)
        except Exception as exc:  # keep the original file and report the parser failure
            error = f"{type(exc).__name__}: {exc}"
            errors[type(exc).__name__] += 1
            entries.append(
                PlannedFile(path, digest, False, None, None, path, None, None, None, None, error)
            )
            transitions[f"{current} -> parser-error"] += 1
            continue

        if result.accepted and result.source:
            category = category_for_source(result.source)
            target = final_root / category / path.name
            categories[category] += 1
            transitions[f"{current} -> {category}"] += 1
            entries.append(
                PlannedFile(
                    path,
                    digest,
                    True,
                    result.source,
                    category,
                    target,
                    json.dumps(result.fields, ensure_ascii=False, separators=(",", ":")),
                    result.width,
                    result.height,
                    result.image_format,
                    None,
                )
            )
        else:
            relative_category = current.removeprefix("quarantine/")
            target = quarantine_root / relative_category / path.name
            transitions[f"{current} -> quarantine"] += 1
            entries.append(
                PlannedFile(
                    path,
                    digest,
                    False,
                    None,
                    None,
                    target,
                    None,
                    result.width,
                    result.height,
                    result.image_format,
                    REJECT_REASON,
                )
            )

    report = {
        "parser_version": ASSET_PARSER_VERSION,
        "total_files": len(entries),
        "accepted": sum(entry.accepted for entry in entries),
        "rejected": sum(entry.error == REJECT_REASON for entry in entries),
        "parser_errors": sum(errors.values()),
        "categories": dict(sorted(categories.items())),
        "transitions": dict(sorted(transitions.items())),
    }
    return entries, report


def validate_plan(entries: list[PlannedFile]) -> None:
    targets: dict[Path, str] = {}
    digests: dict[str, Path] = {}
    for entry in entries:
        if not entry.accepted and entry.error != REJECT_REASON:
            raise RuntimeError(
                f"parser error must be resolved before apply: {entry.source_path}: {entry.error}"
            )
        target = entry.target_path.resolve()
        previous_path = digests.setdefault(entry.sha256, entry.source_path.resolve())
        if previous_path != entry.source_path.resolve():
            raise RuntimeError(
                f"duplicate physical content requires manual review: {previous_path} and {entry.source_path}"
            )
        previous = targets.setdefault(target, entry.sha256)
        if previous != entry.sha256:
            raise RuntimeError(f"target collision with different content: {target}")
        if target.exists() and target != entry.source_path.resolve():
            if sha256_file(target) != entry.sha256:
                raise RuntimeError(f"existing target has different content: {target}")
            raise RuntimeError(f"duplicate physical file requires manual review: {target}")


def backup_database(database: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise FileExistsError(f"backup already exists: {destination}")
    source = sqlite3.connect(database)
    backup = sqlite3.connect(destination)
    try:
        source.backup(backup)
    finally:
        backup.close()
        source.close()


def canonical_occurrence(connection: sqlite3.Connection, digest: str) -> tuple[Any, ...]:
    row = connection.execute(
        """
        SELECT group_id, sender_uin, message_id, image_index, sent_at
        FROM images WHERE sha256=?
        ORDER BY coalesce(sent_at, 9223372036854775807), group_id, message_id, image_index
        LIMIT 1
        """,
        (digest,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"no message occurrence for sha256 {digest}")
    return row


def apply_plan(
    entries: list[PlannedFile],
    database: Path,
    backup: Path,
) -> dict[str, int]:
    validate_plan(entries)
    backup_database(database, backup)
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA foreign_keys=ON")
    moved: list[tuple[Path, Path]] = []
    updated_images = 0
    restored_assets = 0
    rejected_assets = 0
    now = int(time.time())
    try:
        connection.execute("BEGIN IMMEDIATE")
        for entry in entries:
            source = entry.source_path
            target = entry.target_path
            if source.resolve() != target.resolve():
                target.parent.mkdir(parents=True, exist_ok=True)
                os.replace(source, target)
                moved.append((target, source))

            if entry.accepted:
                updated_images += connection.execute(
                    """
                    UPDATE images
                    SET status='accepted', local_path=?, metadata_source=?, metadata_json=?,
                        error=NULL, updated_at=?
                    WHERE sha256=?
                    """,
                    (
                        str(target),
                        entry.metadata_source,
                        entry.metadata_json,
                        now,
                        entry.sha256,
                    ),
                ).rowcount
                group_id, sender_uin, message_id, image_index, sent_at = canonical_occurrence(
                    connection, entry.sha256
                )
                connection.execute(
                    """
                    INSERT INTO assets (
                        sha256, local_path, category, file_extension, file_size,
                        width, height, metadata_source, metadata_json, parser_version,
                        canonical_group_id, canonical_sender_uin, canonical_message_id,
                        canonical_image_index, canonical_sent_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(sha256) DO UPDATE SET
                        local_path=excluded.local_path,
                        category=excluded.category,
                        file_extension=excluded.file_extension,
                        file_size=excluded.file_size,
                        width=excluded.width,
                        height=excluded.height,
                        metadata_source=excluded.metadata_source,
                        metadata_json=excluded.metadata_json,
                        parser_version=excluded.parser_version,
                        updated_at=excluded.updated_at
                    """,
                    (
                        entry.sha256,
                        str(target),
                        entry.category,
                        target.suffix.casefold(),
                        target.stat().st_size,
                        entry.width,
                        entry.height,
                        entry.metadata_source,
                        entry.metadata_json,
                        ASSET_PARSER_VERSION,
                        group_id,
                        sender_uin,
                        message_id,
                        image_index,
                        sent_at,
                        now,
                        now,
                    ),
                )
                restored_assets += 1
            else:
                updated_images += connection.execute(
                    """
                    UPDATE images
                    SET status='rejected_no_metadata', local_path=?, metadata_source=NULL,
                        metadata_json=NULL, error=?, updated_at=?
                    WHERE sha256=?
                    """,
                    (str(target), entry.error, now, entry.sha256),
                ).rowcount
                rejected_assets += connection.execute(
                    "DELETE FROM assets WHERE sha256=?", (entry.sha256,)
                ).rowcount
        connection.commit()
    except Exception:
        connection.rollback()
        for current, original in reversed(moved):
            original.parent.mkdir(parents=True, exist_ok=True)
            if current.exists():
                os.replace(current, original)
        raise
    finally:
        connection.close()

    for directory in sorted(
        {path.parent for entry in entries for path in (entry.source_path, entry.target_path)},
        key=lambda path: len(path.parts),
        reverse=True,
    ):
        try:
            directory.rmdir()
        except OSError:
            pass
    return {
        "moved_files": len(moved),
        "updated_image_rows": updated_images,
        "accepted_assets": restored_assets,
        "deleted_rejected_assets": rejected_assets,
    }


def write_report(path: Path | None, report: dict[str, Any]) -> None:
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")
    print(text)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--final-root", type=Path, required=True)
    parser.add_argument("--quarantine-root", type=Path, required=True)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup-database", type=Path)
    args = parser.parse_args()

    entries, report = create_plan(args.final_root, args.quarantine_root)
    validate_plan(entries)
    if args.apply:
        if not args.database or not args.backup_database:
            parser.error("--apply requires --database and --backup-database")
        report["apply"] = apply_plan(entries, args.database, args.backup_database)
        report["database_backup"] = str(args.backup_database)
    write_report(args.report, report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
