"""Stable Diffusion image metadata extraction.

The supported metadata families mirror the behavior of
Akegarasu/stable-diffusion-inspector without copying its frontend code:

* PNG tEXt/zTXt/iTXt fields (A1111, NovelAI, ComfyUI and related tools)
* JPEG/WebP/AVIF EXIF UserComment
* NovelAI ``stealth_pngcomp`` data stored in alpha-channel low bits

Reference project: https://github.com/Akegarasu/stable-diffusion-inspector
"""

from __future__ import annotations

import gzip
import json
import zlib
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from PIL import Image, PngImagePlugin


PNG_TEXT_CHUNK_LIMIT = 16 * 1024 * 1024
PNG_TEXT_TOTAL_LIMIT = 64 * 1024 * 1024

# Pillow's default compressed PNG text limit is 1 MiB, which is smaller than
# real ComfyUI workflows. Keep decompression bounded, but permit practical
# workflow payloads without mutating the limit around individual threads.
PngImagePlugin.MAX_TEXT_CHUNK = PNG_TEXT_CHUNK_LIMIT
PngImagePlugin.MAX_TEXT_MEMORY = PNG_TEXT_TOTAL_LIMIT


GENERATION_KEYS = {
    "prompt",
    "prompts",
    "negative_prompt",
    "v4_prompt",
    "v4_negative_prompt",
    "steps",
    "seed",
    "sampler",
    "scale",
    "cfg_scale",
    "width",
    "height",
    "n_samples",
    "noise_schedule",
    "workflow",
    "nodes",
    "class_type",
}

NAI_MARKER_KEYS = {"signed_hash", "v4_prompt", "request_type"}

A1111_TEXT_MARKERS = (
    "negative prompt:",
    "steps:",
    "sampler:",
    "seed:",
    "size:",
    "cfg scale:",
    "model hash:",
)


@dataclass(frozen=True)
class MetadataResult:
    accepted: bool
    source: str | None
    fields: dict[str, Any]
    width: int
    height: int
    image_format: str


def _decode_user_comment(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value.replace("\x00", "").strip()
    if not isinstance(value, (bytes, bytearray)):
        return str(value).strip() or None

    raw = bytes(value)
    encodings: list[str]
    if raw.startswith(b"ASCII\x00\x00\x00"):
        raw, encodings = raw[8:], ["utf-8", "latin-1"]
    elif raw.startswith(b"UNICODE\x00"):
        raw, encodings = raw[8:], ["utf-16", "utf-16-be", "utf-8"]
    elif raw.startswith(b"JIS\x00\x00\x00\x00\x00"):
        raw, encodings = raw[8:], ["shift_jis", "utf-8"]
    else:
        encodings = ["utf-8", "utf-16", "latin-1"]

    for encoding in encodings:
        try:
            return raw.decode(encoding).replace("\x00", "").strip() or None
        except (UnicodeDecodeError, UnicodeError):
            continue
    return None


def _read_byte(bits: Iterator[int]) -> int:
    value = 0
    for _ in range(8):
        try:
            bit = next(bits)
        except StopIteration as exc:
            raise ValueError("Stealth metadata bitstream ended early") from exc
        value = (value << 1) | bit
    return value


def _extract_stealth_png(image: Image.Image) -> dict[str, Any] | None:
    if "A" not in image.getbands():
        return None

    rgba = image.convert("RGBA")
    width, height = rgba.size

    try:
        header_bytes = len("stealth_pngcomp") + 4
        if height % 8 == 0:
            # The reference format walks x first, then y. Transposing turns that
            # into Pillow's native row-major order; mode 1 then packs the LSBs
            # MSB-first, exactly matching one metadata byte per eight pixels.
            alpha = rgba.getchannel("A").transpose(Image.Transpose.TRANSPOSE)
            lsb = alpha.point([255 if value & 1 else 0 for value in range(256)])
            packed = lsb.convert("1", dither=Image.Dither.NONE).tobytes()
            if packed[: len("stealth_pngcomp")] != b"stealth_pngcomp":
                return None
            bit_length = int.from_bytes(
                packed[len("stealth_pngcomp") : header_bytes], "big", signed=True
            )
            if bit_length <= 0 or bit_length % 8:
                return None
            payload_end = header_bytes + bit_length // 8
            if payload_end > len(packed):
                return None
            compressed = packed[header_bytes:payload_end]
        else:
            # Unusual dimensions need a padding-free fallback because mode 1
            # pads each transposed scanline to a whole byte.
            alpha = rgba.getchannel("A").tobytes()
            bits = iter(
                alpha[y * width + x] & 1
                for x in range(width)
                for y in range(height)
            )
            magic = bytes(_read_byte(bits) for _ in range(len("stealth_pngcomp")))
            if magic != b"stealth_pngcomp":
                return None
            bit_length = int.from_bytes(
                bytes(_read_byte(bits) for _ in range(4)), "big", signed=True
            )
            consumed_bits = header_bytes * 8
            if bit_length <= 0 or bit_length % 8 or bit_length > len(alpha) - consumed_bits:
                return None
            compressed = bytes(_read_byte(bits) for _ in range(bit_length // 8))

        decoded = gzip.decompress(compressed).decode("utf-8")
        data = json.loads(decoded)
        return data if isinstance(data, dict) else {"stealth": data}
    except (EOFError, ValueError, OSError, zlib.error, UnicodeDecodeError, json.JSONDecodeError):
        return None


def _normalise_key(key: Any) -> str:
    return str(key).casefold().replace(" ", "_").replace("-", "_")


def _iter_metadata_items(value: Any):
    if isinstance(value, dict):
        for key, item in value.items():
            yield str(key), item
            yield from _iter_metadata_items(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _iter_metadata_items(item)
    elif isinstance(value, str):
        text = value.strip()
        if text[:1] in {"{", "["}:
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                return
            yield from _iter_metadata_items(parsed)


def _contains_generation_metadata(value: Any) -> bool:
    if isinstance(value, str):
        text = value.strip()
        if text[:1] in {"{", "["}:
            try:
                return _contains_generation_metadata(json.loads(text))
            except json.JSONDecodeError:
                pass
        folded = text.casefold()
        marker_count = sum(marker in folded for marker in A1111_TEXT_MARKERS)
        return marker_count >= 2 or (
            "negative prompt:" in folded and "steps:" in folded
        )

    keys = {_normalise_key(key) for key, _ in _iter_metadata_items(value)}
    if {"nodes", "class_type"}.issubset(keys):
        return True
    if "prompts" in keys and {"schema_version", "mode"}.issubset(keys):
        return True
    if keys & NAI_MARKER_KEYS and {"prompt", "v4_prompt"} & keys:
        return True
    if "prompt" in keys or "prompts" in keys:
        return bool(keys & {"steps", "seed", "sampler", "width", "height", "scale"})
    return bool(keys & GENERATION_KEYS - {"prompt", "prompts", "workflow"})


def _looks_like_generation_metadata(key: str, value: Any) -> bool:
    # A field name such as Comment or Description is not evidence by itself.
    # Validate the value so creator notes and watermarks are rejected.
    return _contains_generation_metadata(value)


def _metadata_text(fields: dict[str, Any]) -> str:
    return json.dumps(fields, ensure_ascii=False, sort_keys=True).casefold()


def _field_text(fields: dict[str, Any], key: str) -> str:
    for field, value in fields.items():
        if field.casefold() == key.casefold():
            return str(value).strip().casefold()
    return ""


def _has_explicit_novelai_marker(fields: dict[str, Any]) -> bool:
    for key, value in _iter_metadata_items(fields):
        normalized = _normalise_key(key)
        text = str(value).strip().casefold()
        if normalized in NAI_MARKER_KEYS:
            return True
        if normalized == "software" and text == "novelai":
            return True
        if normalized == "source" and text.startswith("novelai"):
            return True
    return False


def _has_explicit_comfyui_marker(fields: dict[str, Any]) -> bool:
    folded_keys = {_normalise_key(key) for key, _ in _iter_metadata_items(fields)}
    if "workflow" in folded_keys:
        return True
    if "class_type" in folded_keys and "nodes" in folded_keys:
        return True
    version = _field_text(fields, "Version")
    if version == "comfyui":
        return True
    parameters = _field_text(fields, "parameters")
    return "version: comfyui" in parameters


def _has_native_novelai_file_metadata(fields: dict[str, Any]) -> bool:
    if not _has_explicit_novelai_marker(fields):
        return False
    for key, value in fields.items():
        if _normalise_key(key) in {"comment", "usercomment"} and _contains_generation_metadata(value):
            return True
    return False


def _json_safe(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray)):
        decoded = _decode_user_comment(value)
        if decoded:
            return decoded
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _decode_png_text(value: bytes) -> str | None:
    for encoding in ("utf-8", "latin-1"):
        try:
            return value.decode(encoding).replace("\x00", "").strip() or None
        except UnicodeDecodeError:
            continue
    return None


def _decompress_png_text(value: bytes) -> bytes | None:
    try:
        decompressor = zlib.decompressobj()
        decoded = decompressor.decompress(value, PNG_TEXT_CHUNK_LIMIT + 1)
        if len(decoded) > PNG_TEXT_CHUNK_LIMIT or decompressor.unconsumed_tail:
            return None
        tail = decompressor.flush()
        if (
            not decompressor.eof
            or decompressor.unused_data
            or len(decoded) + len(tail) > PNG_TEXT_CHUNK_LIMIT
        ):
            return None
        return decoded + tail
    except zlib.error:
        return None


def _read_png_text_channels(path: Path) -> tuple[dict[str, str], dict[str, str]]:
    """Return official PNG text fields and recovery-only zTXt fields.

    NovelAI's inspector reads ordinary tEXt/iTXt fields before trying EXIF and
    Alpha stealth metadata.  A zTXt payload is useful to this collector, but it
    must not be treated as an ordinary field when simulating that fallback
    order.  CDN downloads are inspected while they still have a temporary
    ``.part``/``.bin`` name, so PNG detection must be based on the signature
    and chunk structure rather than the filename suffix.
    """
    try:
        raw = path.read_bytes()
    except OSError:
        return {}, {}
    if not raw.startswith(b"\x89PNG\r\n\x1a\n"):
        return {}, {}

    regular_fields: dict[str, str] = {}
    ztxt_fields: dict[str, str] = {}
    offset = 8
    while offset + 12 <= len(raw):
        length = int.from_bytes(raw[offset : offset + 4], "big")
        chunk_end = offset + 12 + length
        if chunk_end > len(raw):
            break
        chunk_type = raw[offset + 4 : offset + 8]
        chunk_data = raw[offset + 8 : offset + 8 + length]
        if chunk_type == b"tEXt":
            separator = chunk_data.find(b"\x00")
            if separator > 0:
                keyword = _decode_png_text(chunk_data[:separator])
                value = _decode_png_text(chunk_data[separator + 1 :])
                if keyword and value is not None:
                    regular_fields[keyword] = value
        elif chunk_type == b"zTXt":
            separator = chunk_data.find(b"\x00")
            if separator > 0 and separator + 2 < len(chunk_data):
                keyword = _decode_png_text(chunk_data[:separator])
                # PNG zTXt currently defines compression method 0 (DEFLATE).
                if keyword and chunk_data[separator + 1] == 0:
                    decoded = _decompress_png_text(chunk_data[separator + 2 :])
                    text = _decode_png_text(decoded or b"")
                    if text:
                        ztxt_fields[keyword] = text
        elif chunk_type == b"iTXt":
            separator = chunk_data.find(b"\x00")
            if separator > 0 and separator + 3 < len(chunk_data):
                keyword = _decode_png_text(chunk_data[:separator])
                remainder = chunk_data[separator + 1 :]
                compressed = remainder[0] == 1
                compression_method = remainder[1]
                remainder = remainder[2:]
                language_end = remainder.find(b"\x00")
                if language_end >= 0:
                    remainder = remainder[language_end + 1 :]
                    translated_end = remainder.find(b"\x00")
                else:
                    translated_end = -1
                if keyword and translated_end >= 0:
                    text_bytes = remainder[translated_end + 1 :]
                    if compressed and compression_method == 0:
                        text_bytes = _decompress_png_text(text_bytes) or b""
                    elif compressed:
                        text_bytes = b""
                    text = _decode_png_text(text_bytes)
                    if text is not None:
                        regular_fields[keyword] = text
        offset = chunk_end
        if chunk_type == b"IEND":
            break
    return regular_fields, ztxt_fields


def _read_png_ztxt(path: Path) -> dict[str, str]:
    """Read compressed PNG zTXt fields as a recovery channel."""
    return _read_png_text_channels(path)[1]


def _classify_fields(fields: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
    if not fields:
        return {}, None
    explicit_novelai = _has_explicit_novelai_marker(fields)
    explicit_comfyui = _has_explicit_comfyui_marker(fields)
    accepted_fields = {
        key: value
        for key, value in fields.items()
        if _looks_like_generation_metadata(key, value)
    }
    folded = {key.casefold() for key in accepted_fields}
    if explicit_comfyui:
        accepted_fields = dict(fields)
        source = "comfyui"
    elif explicit_novelai:
        # Keep the marker fields even when the generation payload is partial;
        # this is the evidence used for the unreadable-NovelAI category.
        accepted_fields = dict(fields)
        source = "novelai"
    elif "parameters" in folded or "usercomment" in folded:
        source = "a1111-compatible"
    elif accepted_fields:
        source = "unknown-generator"
    else:
        source = None
    return accepted_fields, source


def inspect_image(path: str | Path) -> MetadataResult:
    image_path = Path(path)
    with Image.open(image_path) as image:
        width, height = image.size
        image_format = (image.format or image_path.suffix.lstrip(".")).upper()
        if image_format == "GIF":
            return MetadataResult(
                accepted=False,
                source=None,
                fields={},
                width=width,
                height=height,
                image_format=image_format,
            )

        image.load()
        info_fields: dict[str, Any] = {}

        for key, value in image.info.items():
            if key in {"exif", "icc_profile", "dpi", "transparency", "duration", "loop"}:
                continue
            if isinstance(value, bytes):
                decoded = _decode_user_comment(value)
                if decoded:
                    value = decoded
                else:
                    continue
            if isinstance(value, (str, int, float, bool, dict, list)):
                info_fields[str(key)] = _json_safe(value)

        try:
            user_comment = image.getexif().get(37510)
        except (AttributeError, ValueError, TypeError):
            user_comment = None
        decoded_comment = _decode_user_comment(user_comment)
        exif_fields = {"UserComment": decoded_comment} if decoded_comment else {}
        if decoded_comment:
            info_fields.setdefault("UserComment", decoded_comment)

        regular_png_fields, ztxt_raw = _read_png_text_channels(image_path)
        # The official first stage is PNG tEXt/iTXt, not every decoder-specific
        # value Pillow exposes (for example WebP background/timestamp or JPEG
        # comments).  Other formats proceed through EXIF and then Alpha.
        regular_fields: dict[str, Any] = regular_png_fields

        file_fields, file_source = _classify_fields(info_fields)
        regular_accepted, regular_source = _classify_fields(regular_fields)
        exif_accepted, exif_source = _classify_fields(exif_fields)
        ztxt_fields, ztxt_source = _classify_fields(ztxt_raw)
        stealth = _extract_stealth_png(image)

        stealth_source: str | None = None
        if stealth:
            if _has_explicit_comfyui_marker(stealth):
                stealth_source = "comfyui"
            elif _has_explicit_novelai_marker(stealth):
                stealth_source = "novelai"
            elif _contains_generation_metadata(stealth):
                stealth_source = "unknown-generator"

        channel_sources = {
            file_source,
            regular_source,
            exif_source,
            ztxt_source,
            stealth_source,
        }
        nai_recovery_found = "novelai" in channel_sources

        if "comfyui" in channel_sources:
            source = "comfyui"
            fields = next(
                value
                for value, candidate in (
                    (file_fields, file_source),
                    (regular_accepted, regular_source),
                    (exif_accepted, exif_source),
                    (ztxt_fields, ztxt_source),
                    (stealth or {}, stealth_source),
                )
                if candidate == "comfyui"
            )
        elif regular_fields and _has_native_novelai_file_metadata(regular_fields):
            # A complete ordinary tEXt/iTXt Comment is the primary official
            # path.  Alpha or zTXt duplicates must not downgrade it.
            source = "novelai"
            fields = regular_accepted
        elif regular_fields and nai_recovery_found:
            # Ordinary text exists but is incomplete.  The official inspector
            # stops here and does not fall back to Alpha; our zTXt/Alpha readers
            # can still recover the parameters.
            source = "novelai-unreadable"
            fields = {
                **regular_accepted,
                **ztxt_fields,
                **exif_accepted,
                **(stealth or {}),
            }
        elif exif_fields and _has_native_novelai_file_metadata(exif_fields):
            source = "novelai"
            fields = exif_accepted
        elif exif_fields and nai_recovery_found:
            source = "novelai-unreadable"
            fields = {**exif_accepted, **ztxt_fields, **(stealth or {})}
        elif stealth_source == "novelai":
            # With no ordinary text or EXIF result, Alpha stealth is the
            # official fallback and is directly readable.
            source = "novelai"
            fields = stealth or {}
        elif ztxt_source == "novelai" or file_source == "novelai":
            source = "novelai-unreadable"
            fields = {**file_fields, **ztxt_fields}
        elif regular_source:
            source = regular_source
            fields = regular_accepted
        elif exif_source:
            source = exif_source
            fields = exif_accepted
        elif ztxt_source:
            source = ztxt_source
            fields = ztxt_fields
        elif file_source:
            source = file_source
            fields = file_fields
        else:
            source = stealth_source
            fields = stealth or {}

        return MetadataResult(
            accepted=bool(fields) and source is not None,
            source=source,
            fields=fields,
            width=width,
            height=height,
            image_format=image_format,
        )


def extension_for_format(image_format: str) -> str:
    return {
        "JPEG": ".jpg",
        "JPG": ".jpg",
        "PNG": ".png",
        "WEBP": ".webp",
        "AVIF": ".avif",
        "GIF": ".gif",
    }.get(image_format.upper(), ".img")
