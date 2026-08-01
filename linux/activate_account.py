from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any

try:
    from .bootstrap import configured_runtime_root
except ImportError:
    from bootstrap import configured_runtime_root


ACCOUNT_CONFIG = re.compile(r"^napcat_(\d+)\.json$")


def discover_accounts(config_dir: Path) -> list[str]:
    accounts: list[str] = []
    for path in config_dir.glob("napcat_*.json"):
        match = ACCOUNT_CONFIG.fullmatch(path.name)
        if match:
            accounts.append(match.group(1))
    return sorted(set(accounts))


def normalized_onebot(payload: dict[str, Any]) -> dict[str, Any]:
    timeout = payload.get("timeout")
    if isinstance(timeout, (int, float)):
        payload["timeout"] = {
            "baseTimeout": 10000,
            "uploadSpeedKBps": 256,
            "downloadSpeedKBps": 256,
            "maxTimeout": int(timeout),
        }
    if not isinstance(payload.get("timeout"), dict):
        raise ValueError("OneBot timeout must be an object")
    network = payload.get("network")
    if not isinstance(network, dict) or not isinstance(
        network.get("httpServers"), list
    ):
        raise ValueError("OneBot network.httpServers is missing")
    if not any(
        item.get("enable") and int(item.get("port") or 0) == 3000
        for item in network["httpServers"]
        if isinstance(item, dict)
    ):
        raise ValueError("No enabled OneBot HTTP server on port 3000")
    return payload


def atomic_write(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)
    path.chmod(0o600)


def activate(runtime_root: Path, account: str | None = None) -> tuple[str, bool]:
    config_dir = runtime_root / "napcat-config"
    template = config_dir / "onebot11.json"
    if not template.is_file():
        raise FileNotFoundError(f"OneBot template is missing: {template}")
    accounts = discover_accounts(config_dir)
    if account is None:
        if len(accounts) != 1:
            raise RuntimeError(
                "Unable to select one QQ account; pass --account. "
                f"Discovered: {', '.join(accounts) or 'none'}"
            )
        account = accounts[0]
    if not account.isdigit():
        raise ValueError("QQ account must be numeric")
    if accounts and account not in accounts:
        raise RuntimeError(
            f"QQ account {account} has not logged in; discovered: "
            + ", ".join(accounts)
        )
    payload = normalized_onebot(
        json.loads(template.read_text(encoding="utf-8"))
    )
    target = config_dir / f"onebot11_{account}.json"
    before = target.read_bytes() if target.is_file() else None
    atomic_write(template, payload)
    atomic_write(target, payload)
    return account, before != target.read_bytes()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Activate the generated OneBot template for the QQ account created "
            "by the first NapCat login. A restart may be required if OneBot "
            "failed during initial adapter setup."
        )
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parent,
    )
    parser.add_argument("--runtime-root", type=Path)
    parser.add_argument("--account")
    args = parser.parse_args()
    root = args.root.resolve()
    runtime = configured_runtime_root(root, args.runtime_root)
    account, changed = activate(runtime, args.account)
    print(
        f"account={account} onebot_config="
        f"{'updated' if changed else 'already_current'}"
    )
    print(
        "No token value was printed. If OneBot was not initialized during "
        "login, restart napcat once after the QQ session is persisted."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
