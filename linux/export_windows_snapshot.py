from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import time
from pathlib import Path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def copy_final(source: Path, destination: Path) -> tuple[int, int]:
    count = 0
    size = 0
    final_root = source / "final"
    if not final_root.is_dir():
        return count, size
    for path in final_root.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(source)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.is_file() or target.stat().st_size != path.stat().st_size:
            shutil.copy2(path, target)
        count += 1
        size += path.stat().st_size
    return count, size


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Create a consistent Windows migration snapshot without stopping the "
            "running collector. Repeat once more after the final cutover stop."
        )
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    config_path = args.config.resolve()
    output = args.output.resolve()
    settings = json.loads(config_path.read_text(encoding="utf-8"))
    storage = Path(settings["storage"]["root"]).resolve()
    database = Path(settings["storage"]["database"]).resolve()
    if output == storage or storage in output.parents:
        parser.error("Snapshot output must be outside the live repository")

    output.mkdir(parents=True, exist_ok=True)
    copied_count, copied_bytes = copy_final(storage, output)
    database_target = output / "state" / "collector_state.sqlite3"
    database_target.parent.mkdir(parents=True, exist_ok=True)
    temporary = database_target.with_suffix(".sqlite3.tmp")
    temporary.unlink(missing_ok=True)
    with sqlite3.connect(database) as source_connection:
        with sqlite3.connect(temporary) as destination_connection:
            source_connection.backup(destination_connection)
    os.replace(temporary, database_target)
    shutil.copy2(config_path, output / "collector_config.windows.json")

    with sqlite3.connect(database_target) as connection:
        statuses = dict(
            connection.execute(
                "SELECT status, count(*) FROM images GROUP BY status"
            ).fetchall()
        )
        unique_images = int(
            connection.execute(
                "SELECT count(DISTINCT sha256) FROM images "
                "WHERE status='accepted' AND sha256 IS NOT NULL"
            ).fetchone()[0]
            or 0
        )
    manifest = {
        "created_at": int(time.time()),
        "source_root": str(storage),
        "database_sha256": sha256(database_target),
        "final_files": copied_count,
        "final_bytes": copied_bytes,
        "unique_images": unique_images,
        "statuses": statuses,
    }
    (output / "migration_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(manifest, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
