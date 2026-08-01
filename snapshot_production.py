from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import time
from contextlib import closing
from pathlib import Path
from typing import Any


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def snapshot(config_path: Path, output_dir: Path, backup: bool) -> dict[str, Any]:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    storage_root = Path(config["storage"]["root"])
    database_path = Path(config["storage"]["database"])
    output_dir.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    try:
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        image_columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(images)")
        }
        if backup:
            backup_path = output_dir / "collector_state.sqlite3"
            with closing(sqlite3.connect(backup_path)) as backup_connection:
                connection.backup(backup_connection)
            shutil.copy2(config_path, output_dir / "collector_config.json")
        statuses = {
            str(row["status"]): int(row["count"])
            for row in connection.execute(
                "SELECT status, count(*) AS count FROM images GROUP BY status"
            )
        }
        group_cursors = {
            str(row["group_id"]): {
                "oldest_time": int(row["oldest_time"] or 0),
                "completed": int(row["completed"] or 0),
            }
            for row in connection.execute(
                "SELECT group_id, oldest_time, completed FROM group_cursors"
            )
        }
        recent_cursors = {
            str(row["group_id"]): int(row["last_time"] or 0)
            for row in connection.execute(
                "SELECT group_id, last_time FROM qce_recent_cursors"
            )
        }
        monitored = [
            str(row["group_id"])
            for row in connection.execute(
                "SELECT group_id FROM monitored_groups WHERE enabled=1 ORDER BY group_id"
            )
        ] if connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='monitored_groups'"
        ).fetchone() else [str(value) for value in config.get("groups", [])]
        unique_images = int(
            connection.execute(
                "SELECT count(DISTINCT sha256) FROM images "
                "WHERE status='accepted' AND sha256 IS NOT NULL"
            ).fetchone()[0]
            or 0
        )
        assets = (
            int(connection.execute("SELECT count(*) FROM assets").fetchone()[0] or 0)
            if "assets" in tables
            else 0
        )
        accepted_provenance_missing = (
            int(
                connection.execute(
                    "SELECT count(*) FROM images "
                    "WHERE status='accepted' AND coalesce(sender_uin, '')=''"
                ).fetchone()[0]
                or 0
            )
            if "sender_uin" in image_columns
            else unique_images
        )
        assets_without_sender = (
            int(
                connection.execute(
                    "SELECT count(*) FROM ("
                    "SELECT sha256 FROM images "
                    "WHERE status='accepted' AND sha256 IS NOT NULL "
                    "GROUP BY sha256 HAVING sum(CASE WHEN "
                    "coalesce(sender_uin, '')<>'' THEN 1 ELSE 0 END)=0)"
                ).fetchone()[0]
                or 0
            )
            if "sender_uin" in image_columns
            else unique_images
        )
    finally:
        connection.close()

    final_root = storage_root / "final"
    files = [path for path in final_root.rglob("*") if path.is_file()]
    result: dict[str, Any] = {
        "captured_at": int(time.time()),
        "config": str(config_path),
        "database": str(database_path),
        "database_sha256": sha256(output_dir / "collector_state.sqlite3") if backup else None,
        "groups": monitored,
        "statuses": statuses,
        "unique_images": unique_images,
        "assets": assets,
        "accepted_provenance_missing": accepted_provenance_missing,
        "assets_without_sender": assets_without_sender,
        "final_files": len(files),
        "final_bytes": sum(path.stat().st_size for path in files),
        "group_cursors": group_cursors,
        "recent_cursors": recent_cursors,
    }
    target = output_dir / ("before.json" if backup else "after.json")
    target.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--backup", action="store_true")
    args = parser.parse_args()
    result = snapshot(args.config, args.output, args.backup)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
