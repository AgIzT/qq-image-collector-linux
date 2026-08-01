#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PUBLIC_PORT="${1:-10058}"
if [[ ! "$PUBLIC_PORT" =~ ^[0-9]+$ ]] ||
   (( PUBLIC_PORT < 1024 || PUBLIC_PORT > 65535 )); then
  echo "Public port must be an integer between 1024 and 65535." >&2
  exit 2
fi

python3 - "$SCRIPT_DIR/.env" "$PUBLIC_PORT" <<'PY'
import os
import sys
import tempfile
from pathlib import Path

path = Path(sys.argv[1])
port = sys.argv[2]
replacements = {
    "NAPCAT_PUBLIC_BIND": "0.0.0.0",
    "NAPCAT_PUBLIC_WEBUI_PORT": port,
}
lines = path.read_text(encoding="utf-8").splitlines()
seen = set()
updated = []
for line in lines:
    key = line.split("=", 1)[0] if "=" in line else ""
    if key in replacements:
        updated.append(f"{key}={replacements[key]}")
        seen.add(key)
    else:
        updated.append(line)
for key, value in replacements.items():
    if key not in seen:
        updated.append(f"{key}={value}")

fd, temporary_name = tempfile.mkstemp(
    prefix=".env.",
    suffix=".tmp",
    dir=path.parent,
)
temporary = Path(temporary_name)
try:
    with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
        stream.write("\n".join(updated) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)
finally:
    temporary.unlink(missing_ok=True)
PY

docker compose config >/dev/null
WEBUI_CONFIG="$(
  python3 - <<'PY'
import os
from pathlib import Path

env_path = Path(".env")
runtime_root = "./runtime"
for line in env_path.read_text(encoding="utf-8").splitlines():
    if line.startswith("QQAI_RUNTIME_ROOT="):
        runtime_root = line.split("=", 1)[1].strip()
        break
print(Path(os.path.expandvars(runtime_root)).expanduser() / "napcat-config" / "webui.json")
PY
)"

TOKEN="$(python3 rotate_napcat_token.py "$WEBUI_CONFIG")"
printf 'NAPCAT_WEBUI_TOKEN=%s\n' "$TOKEN"

if command -v ufw >/dev/null 2>&1; then
  ufw allow "$PUBLIC_PORT/tcp" comment "napcat-token-webui" >/dev/null
fi

docker compose up -d --force-recreate napcat-qce collector-console
docker compose ps
ss -lnt | grep -E ":(${PUBLIC_PORT}|16099|40653|17890)\\b" || true
