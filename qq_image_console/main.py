from __future__ import annotations

import argparse
import os
import secrets
import sys
import threading
import time
import urllib.parse
import webbrowser
from pathlib import Path

from collector import PidFile, main as collector_main, pid_is_alive

from .app import create_app
from .config import load_console_config


_STREAM_HANDLE = None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="QQ AI 原图采集控制台")
    parser.add_argument("--config", type=Path, help="manager_config.json 路径")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--no-browser", action="store_true")
    parser.add_argument("--autostart", action="store_true", help=argparse.SUPPRESS)
    return parser


def _load_or_create_token(path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_file():
        token = path.read_text(encoding="ascii").strip()
        if len(token) >= 32:
            return token
    token = secrets.token_urlsafe(32)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(token, encoding="ascii")
    os.replace(temporary, path)
    return token


def _ensure_streams(log_path: Path) -> None:
    """Windowed PyInstaller apps have no stdout/stderr; give libraries a safe stream."""
    global _STREAM_HANDLE
    if sys.stdout is not None and sys.stderr is not None:
        return
    log_path.parent.mkdir(parents=True, exist_ok=True)
    _STREAM_HANDLE = log_path.open("a", encoding="utf-8", buffering=1)
    if sys.stdout is None:
        sys.stdout = _STREAM_HANDLE
    if sys.stderr is None:
        sys.stderr = _STREAM_HANDLE


def _live_other_manager(path: Path) -> int | None:
    if not path.is_file():
        return None
    try:
        existing = int(path.read_text(encoding="ascii").strip())
    except (OSError, ValueError):
        return None
    # Container PIDs are routinely reused after a restart. A PID file that
    # already contains our not-yet-locked process ID can only be stale.
    if existing == os.getpid():
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return None
    return existing if pid_is_alive(existing) else None


def _run_worker(config_path: Path | None) -> int:
    config = load_console_config(config_path)
    _ensure_streams(config.storage_root() / "logs" / "collector-console.log")
    sys.argv = [
        "collector",
        "--config",
        str(config.collector_config_path),
        "run",
    ]
    return collector_main()


def _open_console(config: object, token: str) -> None:
    url = (
        f"http://127.0.0.1:{config.port}/?session_token="
        f"{urllib.parse.quote(token, safe='')}"
    )
    webbrowser.open(url)


def main() -> int:
    args = build_parser().parse_args()
    if args.worker:
        return _run_worker(args.config)

    config = load_console_config(args.config)
    _ensure_streams(config.manager_log_file)
    token = _load_or_create_token(config.token_file)
    existing = _live_other_manager(config.manager_pid_file)
    if existing:
        if not args.no_browser:
            _open_console(config, token)
        return 0

    app = create_app(config, token)
    if not args.no_browser and config.open_browser:
        threading.Timer(1.0, _open_console, args=(config, token)).start()
    if args.autostart:
        def start_stack() -> None:
            time.sleep(2)
            try:
                app.state.context.supervisor.request_start(False)
            except Exception:
                pass

        threading.Thread(target=start_stack, name="autostart-stack", daemon=True).start()

    import uvicorn

    with PidFile(config.manager_pid_file):
        uvicorn.run(
            app,
            host=getattr(config, "host", "127.0.0.1"),
            port=config.port,
            # Proxy header rewriting stays disabled. The security middleware
            # validates the direct socket peer against explicit loopback or
            # Docker-internal CIDRs and authenticates public Cloudflare hosts.
            proxy_headers=False,
            access_log=False,
            log_level="info",
            workers=1,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
