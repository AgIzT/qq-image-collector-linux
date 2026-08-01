from __future__ import annotations

import argparse
import datetime as dt
import re
from pathlib import Path


THUMBNAIL = re.compile(r"_(0|198|720)$")


def discover_account_roots(session_root: Path, account: str | None = None) -> list[Path]:
    candidates: list[Path] = []
    roots = [session_root / account] if account else list(session_root.iterdir())
    for root in roots:
        if not root.is_dir():
            continue
        if (root / "nt_data").is_dir():
            candidates.append(root)
        legacy = root / "nt_qq"
        if (legacy / "nt_data").is_dir():
            candidates.append(legacy)
    return sorted(set(candidates))


def candidate_files(
    account_root: Path,
    *,
    keep_days: int,
    thumbnail_keep_days: int,
    now: dt.datetime,
) -> list[Path]:
    main_cutoff = now.timestamp() - keep_days * 86400
    thumbnail_cutoff = now.timestamp() - thumbnail_keep_days * 86400
    result: list[Path] = []
    pic = account_root / "nt_data" / "Pic"
    if pic.is_dir():
        for path in pic.rglob("*"):
            if not path.is_file() or path.is_symlink():
                continue
            cutoff = thumbnail_cutoff if THUMBNAIL.search(path.stem) else main_cutoff
            if path.stat().st_mtime < cutoff:
                result.append(path)
    for relative in (Path("nt_data") / "Emoji", Path("nt_temp")):
        target = account_root / relative
        if not target.is_dir():
            continue
        result.extend(
            path
            for path in target.rglob("*")
            if path.is_file()
            and not path.is_symlink()
            and path.stat().st_mtime < main_cutoff
        )
    return result


def remove_empty_directories(account_root: Path) -> None:
    for relative in (
        Path("nt_data") / "Pic",
        Path("nt_data") / "Emoji",
        Path("nt_temp"),
    ):
        target = account_root / relative
        if not target.is_dir():
            continue
        for directory in sorted(
            (path for path in target.rglob("*") if path.is_dir()),
            key=lambda path: len(path.parts),
            reverse=True,
        ):
            try:
                directory.rmdir()
            except OSError:
                pass


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Safely purge only QQ Pic/Emoji/nt_temp caches. nt_db and the final "
            "four-category image repository are never traversed."
        )
    )
    parser.add_argument(
        "--session-root",
        type=Path,
        default=Path(__file__).resolve().parent / "runtime" / "qq-session",
    )
    parser.add_argument("--account")
    parser.add_argument("--keep-days", type=int, default=7)
    parser.add_argument("--thumbnail-keep-days", type=int, default=90)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    root = args.session_root.resolve()
    if not root.is_dir():
        parser.error(f"Session root does not exist: {root}")
    accounts = discover_account_roots(root, args.account)
    files: list[Path] = []
    for account in accounts:
        resolved = account.resolve()
        if root not in resolved.parents:
            raise RuntimeError(f"Account path escaped session root: {resolved}")
        files.extend(
            candidate_files(
                resolved,
                keep_days=max(1, args.keep_days),
                thumbnail_keep_days=max(1, args.thumbnail_keep_days),
                now=dt.datetime.now(),
            )
        )
    total = sum(path.stat().st_size for path in files)
    if args.apply:
        for path in files:
            resolved = path.resolve()
            if root not in resolved.parents:
                raise RuntimeError(f"Refusing to delete outside session root: {resolved}")
            path.unlink(missing_ok=True)
        for account in accounts:
            remove_empty_directories(account)
    mode = "deleted" if args.apply else "dry-run"
    print(f"mode={mode} files={len(files)} bytes={total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
