#!/usr/bin/env python3
"""Independently audit files the parser rejected, at the byte level.

"Rejected" covers three very different situations, and only a raw read tells
them apart:

  A. nothing carrying text is present at all
  B. text is present but is not generation parameters - a creator name, a
     watermark, a repost URL - which the contract says to reject
  C. generation parameters ARE present and the parser missed them

C is a bug. This deliberately does not call inspect_image or reuse its
acceptance rules: it walks PNG chunks out of the file itself, reads EXIF, tries
the NovelAI alpha channel, and looks at JPEG comment segments, then applies its
own crude keyword test. Agreeing with the parser is only meaningful if the two
arrive independently.
"""

from __future__ import annotations

import argparse
import json
import struct
import sys
import zlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from PIL import Image  # noqa: E402

from metadata_reader import _extract_stealth_png, _decode_user_comment  # noqa: E402


# Deliberately broad and independent of the parser's rules.
MARKERS = (
    "steps:", "sampler", "seed:", "cfg scale", "negative prompt", "denoising",
    "novelai", "stable diffusion", "stable-diffusion", "sd_model", "model hash",
    "workflow", "class_type", "nodes", "prompt", "parameters", "diffusionmodel",
    "scale:", "uc:", "strength", "checkpoint", "lora", "automatic1111", "comfy",
)


def png_chunks(path: Path) -> list[dict[str, object]]:
    """Walk the PNG container directly; PIL merges and filters, this does not."""
    found: list[dict[str, object]] = []
    with path.open("rb") as handle:
        if handle.read(8) != b"\x89PNG\r\n\x1a\n":
            return found
        while True:
            header = handle.read(8)
            if len(header) < 8:
                break
            length, kind = struct.unpack(">I4s", header)
            payload = handle.read(length)
            handle.read(4)  # crc
            name = kind.decode("ascii", "replace")
            if name == "IEND":
                break
            if name not in {"tEXt", "zTXt", "iTXt", "eXIf"}:
                continue
            entry: dict[str, object] = {"chunk": name, "raw_len": length}
            try:
                if name == "tEXt":
                    key, _, value = payload.partition(b"\x00")
                    entry["key"] = key.decode("latin-1", "replace")
                    entry["text"] = value.decode("utf-8", "replace")
                elif name == "zTXt":
                    key, _, rest = payload.partition(b"\x00")
                    entry["key"] = key.decode("latin-1", "replace")
                    entry["text"] = zlib.decompress(rest[1:]).decode("utf-8", "replace")
                elif name == "iTXt":
                    key, _, rest = payload.partition(b"\x00")
                    entry["key"] = key.decode("latin-1", "replace")
                    compressed = rest[0:1] == b"\x01"
                    body = rest[2:].split(b"\x00", 2)[-1]
                    entry["text"] = (
                        zlib.decompress(body) if compressed else body
                    ).decode("utf-8", "replace")
                else:
                    entry["text"] = ""
            except Exception as exc:  # a chunk we cannot read is itself a finding
                entry["error"] = f"{type(exc).__name__}: {exc}"
                entry["text"] = ""
            found.append(entry)
    return found


def jpeg_comments(path: Path) -> list[str]:
    out: list[str] = []
    data = path.read_bytes()
    if not data.startswith(b"\xff\xd8"):
        return out
    index = 2
    while index < len(data) - 3:
        if data[index] != 0xFF:
            index += 1
            continue
        marker = data[index + 1]
        if marker in (0xD8, 0xD9) or 0xD0 <= marker <= 0xD7:
            index += 2
            continue
        if marker == 0xDA:
            break
        size = int.from_bytes(data[index + 2 : index + 4], "big")
        if marker == 0xFE:  # COM
            out.append(data[index + 4 : index + 2 + size].decode("utf-8", "replace"))
        index += 2 + size
    return out


def probe(path: Path) -> dict[str, object]:
    report: dict[str, object] = {"path": str(path), "channels": {}}
    channels: dict[str, object] = report["channels"]  # type: ignore[assignment]

    chunks = png_chunks(path)
    if chunks:
        channels["png_chunks"] = [
            {"chunk": c["chunk"], "key": c.get("key"), "len": len(str(c.get("text") or "")),
             "preview": str(c.get("text") or "")[:200], "error": c.get("error")}
            for c in chunks
        ]

    comments = jpeg_comments(path)
    if comments:
        channels["jpeg_com"] = [{"len": len(c), "preview": c[:200]} for c in comments]

    try:
        with Image.open(path) as image:
            image.load()
            exif = image.getexif()
            if exif:
                items = {}
                for tag, value in exif.items():
                    decoded = _decode_user_comment(value) if isinstance(value, bytes) else value
                    text = str(decoded)
                    if text.strip():
                        items[str(tag)] = text[:200]
                if items:
                    channels["exif"] = items
            # PIL's merged view can surface things the container walk missed.
            info = {
                k: str(v)[:200]
                for k, v in image.info.items()
                if k not in {"icc_profile", "exif", "dpi", "transparency"}
                and isinstance(v, (str, bytes)) and str(v).strip()
            }
            if info:
                channels["pil_info"] = info
            try:
                stealth = _extract_stealth_png(image)
            except Exception as exc:
                stealth = {"probe_error": f"{type(exc).__name__}: {exc}"}
            if stealth:
                channels["alpha_stealth"] = str(stealth)[:400]
            report["mode"] = image.mode
            report["format"] = image.format
            report["has_alpha"] = image.mode in ("RGBA", "LA", "PA")
    except Exception as exc:
        report["open_error"] = f"{type(exc).__name__}: {exc}"

    blob = json.dumps(channels, ensure_ascii=False).casefold()
    hits = sorted({m for m in MARKERS if m in blob})
    report["marker_hits"] = hits
    report["has_any_text"] = bool(channels)
    report["verdict"] = "C_suspect" if hits else ("B_text_no_params" if channels else "A_empty")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    files = sorted(p for p in args.root.rglob("*") if p.is_file())
    print(f"probing {len(files)} files", flush=True)
    reports = []
    for index, path in enumerate(files, start=1):
        reports.append(probe(path))
        if index % 50 == 0:
            print(f"  {index}/{len(files)}", flush=True)
    args.out.write_text(json.dumps(reports, ensure_ascii=False, indent=1), encoding="utf-8")

    tally: dict[str, int] = {}
    for report in reports:
        verdict = str(report["verdict"])
        tally[verdict] = tally.get(verdict, 0) + 1
    print(f"verdicts: {tally}", flush=True)
    print(f"wrote {args.out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
