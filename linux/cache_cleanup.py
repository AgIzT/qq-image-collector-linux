from __future__ import annotations

import argparse
import datetime as dt
import time
from pathlib import Path
from typing import Iterable


SHORT_CACHE_DIRS = (Path("nt_data/Pic"), Path("nt_data/Emoji"), Path("nt_temp"))
MEDIA_CACHE_DIRS = (Path("nt_data/Video"), Path("nt_data/File"), Path("nt_data/Ptt"))
LOG_DIRS = (
    Path("log"),
    Path("logs"),
    Path("nt_qq/log"),
    Path("nt_data/Log"),
    Path("nt_data/log"),
    Path("nt_data/log-cache"),
    Path("NapCat/logs"),
)


def discover_account_roots(session_root: Path, account: str | None = None) -> list[Path]:
    if not session_root.is_dir():
        return []
    roots = [session_root / account] if account else list(session_root.iterdir())
    candidates: list[Path] = []
    for root in roots:
        if not root.is_dir():
            continue
        if (root / "nt_data").is_dir():
            candidates.append(root)
        if (root / "global" / "nt_data").is_dir():
            candidates.append(root / "global")
        if (root / "nt_qq" / "nt_data").is_dir():
            candidates.append(root / "nt_qq")
    return sorted(set(candidates))


def _older_files(root: Path, cutoff: float) -> Iterable[Path]:
    if not root.is_dir():
        return ()
    return (
        path
        for path in root.rglob("*")
        if path.is_file() and not path.is_symlink() and path.stat().st_mtime < cutoff
    )


def candidate_files(
    account_root: Path,
    *,
    short_keep_hours: int,
    media_keep_hours: int,
    log_keep_hours: int,
    now: dt.datetime,
) -> list[Path]:
    current = now.timestamp()
    result: list[Path] = []
    for relative in SHORT_CACHE_DIRS:
        result.extend(_older_files(account_root / relative, current - short_keep_hours * 3600))
    for relative in MEDIA_CACHE_DIRS:
        result.extend(_older_files(account_root / relative, current - media_keep_hours * 3600))
    for relative in LOG_DIRS:
        result.extend(_older_files(account_root / relative, current - log_keep_hours * 3600))
    return result


def _assert_within(path: Path, root: Path) -> Path:
    resolved = path.resolve()
    base = root.resolve()
    if resolved != base and base not in resolved.parents:
        raise RuntimeError(f"Refusing to touch a path outside {base}: {resolved}")
    return resolved


def _remove_empty_directories(roots: Iterable[Path]) -> None:
    for root in roots:
        if not root.is_dir():
            continue
        directories = sorted(
            (path for path in root.rglob("*") if path.is_dir() and not path.is_symlink()),
            key=lambda value: len(value.parts),
            reverse=True,
        )
        for directory in directories:
            try:
                directory.rmdir()
            except OSError:
                pass


def cleanup_once(args: argparse.Namespace) -> tuple[int, int]:
    session_root = args.session_root.resolve()
    accounts = discover_account_roots(session_root, args.account)
    files: list[tuple[Path, Path]] = []
    now = dt.datetime.now()
    for account in accounts:
        _assert_within(account, session_root)
        for path in candidate_files(
            account,
            short_keep_hours=max(1, args.short_keep_hours),
            media_keep_hours=max(1, args.media_keep_hours),
            log_keep_hours=max(1, args.log_keep_hours),
            now=now,
        ):
            files.append((path, session_root))

    for relative in (Path("NapCat/temp"), Path("NapCat/logs")):
        target = session_root / relative
        keep_hours = args.short_keep_hours if relative.name == "temp" else args.log_keep_hours
        files.extend(
            (path, session_root)
            for path in _older_files(target, now.timestamp() - keep_hours * 3600)
        )

    extra_roots: list[Path] = []
    if args.napcat_log_root:
        root = args.napcat_log_root.resolve()
        extra_roots.append(root)
        files.extend((path, root) for path in _older_files(root, now.timestamp() - args.log_keep_hours * 3600))
    if args.collector_temp_root:
        root = args.collector_temp_root.resolve()
        extra_roots.append(root)
        files.extend(
            (path, root)
            for path in _older_files(root, now.timestamp() - args.short_keep_hours * 3600)
            if path.suffix == ".part"
        )
    if args.legacy_qce_root:
        root = args.legacy_qce_root.resolve()
        extra_roots.append(root)
        root.mkdir(parents=True, exist_ok=True)
        marker = root / ".retired-at"
        if not marker.exists():
            if args.apply:
                marker.touch()
        elif marker.stat().st_mtime < now.timestamp() - args.legacy_keep_hours * 3600:
            files.extend(
                (path, root)
                for path in root.rglob("*")
                if path.is_file() and not path.is_symlink() and path != marker
            )
    if args.collector_state_root:
        root = args.collector_state_root.resolve()
        extra_roots.append(root)
        cutoff = now.timestamp() - args.legacy_keep_hours * 3600
        files.extend(
            (path, root)
            for path in _older_files(root, cutoff)
            if ".pre-event-v1." in path.name
        )

    unique: dict[Path, Path] = {}
    for path, root in files:
        unique[path] = root
    total = sum(path.stat().st_size for path in unique if path.exists())
    if args.apply:
        for path, root in unique.items():
            _assert_within(path, root)
            path.unlink(missing_ok=True)
        cleanup_roots = [
            account / relative
            for account in accounts
            for relative in (*SHORT_CACHE_DIRS, *MEDIA_CACHE_DIRS, *LOG_DIRS)
        ]
        _remove_empty_directories([*cleanup_roots, *extra_roots])
    print(
        f"time={int(time.time())} mode={'deleted' if args.apply else 'dry-run'} "
        f"files={len(unique)} bytes={total}",
        flush=True,
    )
    return len(unique), total


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Delete only bounded QQ media caches, stale logs, collector .part files, "
            "and seven-day legacy rollback data. nt_db and final images are never traversed."
        )
    )
    parser.add_argument("--session-root", type=Path, required=True)
    parser.add_argument("--account")
    parser.add_argument("--napcat-log-root", type=Path)
    parser.add_argument("--collector-temp-root", type=Path)
    parser.add_argument("--collector-state-root", type=Path)
    parser.add_argument("--legacy-qce-root", type=Path)
    parser.add_argument("--short-keep-hours", type=int, default=2)
    parser.add_argument("--media-keep-hours", type=int, default=24)
    parser.add_argument("--log-keep-hours", type=int, default=48)
    parser.add_argument("--legacy-keep-hours", type=int, default=168)
    parser.add_argument("--loop-hours", type=float, default=0)
    parser.add_argument("--apply", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not args.session_root.is_dir():
        args.session_root.mkdir(parents=True, exist_ok=True)
    while True:
        cleanup_once(args)
        if args.loop_hours <= 0:
            return 0
        time.sleep(max(60.0, args.loop_hours * 3600))


if __name__ == "__main__":
    raise SystemExit(main())
