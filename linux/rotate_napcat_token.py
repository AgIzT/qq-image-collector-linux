from __future__ import annotations

import argparse
import json
import os
import secrets
import tempfile
from pathlib import Path


def rotate_webui_token(path: Path, token: str | None = None) -> str:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("NapCat webui.json must contain a JSON object")

    replacement = token or secrets.token_hex(32)
    if len(replacement) < 32:
        raise ValueError("NapCat WebUI token must contain at least 32 characters")

    payload["token"] = replacement
    serialized = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(serialized)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return replacement


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Atomically rotate the NapCat WebUI token."
    )
    parser.add_argument(
        "config",
        type=Path,
        help="Path to NapCat webui.json",
    )
    args = parser.parse_args()
    print(rotate_webui_token(args.config))


if __name__ == "__main__":
    main()
