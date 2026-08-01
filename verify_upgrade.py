from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("before", type=Path)
    parser.add_argument("after", type=Path)
    args = parser.parse_args()
    before = json.loads(args.before.read_text(encoding="utf-8"))
    after = json.loads(args.after.read_text(encoding="utf-8"))
    errors: list[str] = []
    if set(before["groups"]) != set(after["groups"]):
        errors.append("enabled group IDs changed")
    if int(after["unique_images"]) < int(before["unique_images"]):
        errors.append("unique image count decreased")
    if int(after["final_files"]) < int(before["final_files"]):
        errors.append("final image file count decreased")
    if int(after["final_bytes"]) < int(before["final_bytes"]):
        errors.append("final image bytes decreased")
    if "assets" in after and int(after["assets"]) != int(after["unique_images"]):
        errors.append("asset catalog count does not match unique accepted images")
    if int(after.get("assets_without_sender", 0)):
        errors.append("one or more unique assets still have no sender provenance")
    for group_id, cursor in before["group_cursors"].items():
        current = after["group_cursors"].get(group_id)
        if current is None:
            errors.append(f"missing history cursor for {group_id}")
            continue
        old_time = int(cursor.get("oldest_time") or 0)
        new_time = int(current.get("oldest_time") or 0)
        if old_time and not new_time:
            errors.append(f"history cursor was cleared for {group_id}")
        elif old_time and new_time > old_time:
            errors.append(f"history cursor moved forward for {group_id}")
    for group_id, old_time in before["recent_cursors"].items():
        new_time = int(after["recent_cursors"].get(group_id) or 0)
        if new_time < int(old_time or 0):
            errors.append(f"recent cursor moved backward for {group_id}")
    if errors:
        print("upgrade verification failed: " + "; ".join(errors))
        return 1
    print(
        "upgrade verification passed: "
        f"groups={len(after['groups'])} unique_images={after['unique_images']} "
        f"final_files={after['final_files']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
