# Metadata Classification

This is the final metadata and storage contract for the collector.

## Acceptance

An image is retained only when the value contains usable generation metadata:

- A1111/Forge text has at least two structural markers such as `Steps`, `Sampler`,
  `Seed`, `Size`, `CFG scale`, or `Negative prompt`.
- JSON has a group of generation fields such as prompt, steps, seed, sampler,
  width, height, or scale.
- ComfyUI has a `workflow`, or a workflow object containing both `nodes` and
  `class_type`.
- NovelAI has explicit NAI markers and NAI metadata, even when the official
  inspector cannot read it.

The field name alone is never enough. A plain `Comment`, `Description`,
`UserComment`, or `parameters` value is rejected. Creator names, repost URLs,
watermarks, empty postprocessing fields, and GIF files are rejected.

## Four Categories

| Directory | Internal source | Rule |
| --- | --- | --- |
| `NovelAI` | `novelai` | Explicit NAI marker plus complete ordinary PNG `tEXt`/`iTXt` metadata, compatible EXIF metadata, or NAI Alpha data reached by the official fallback order. |
| `ComfyUI` | `comfyui` | Explicit workflow, `nodes` + `class_type`, or `Version: ComfyUI`. |
| `NAI含参但不可直接读取的` | `novelai-unreadable` | NAI parameters are recoverable, but an incomplete earlier official channel blocks the fallback, or the payload exists only in an unsupported recovery channel such as zTXt/JPEG Comment. |
| `其他模型生成` | `a1111-compatible` or `unknown-generator` | Valid generation metadata without NAI or ComfyUI evidence. |

`a1111-compatible` is an internal source value and always maps to `其他模型生成`.
A model name, model hash, sampler, or A1111/Forge version does not prove NAI or
ComfyUI. A checkpoint is a model file, not a workflow engine.

## Channel Order

The parser checks every available channel for recovery, then separately simulates
the official readability order:

1. ComfyUI workflow evidence.
2. Complete ordinary PNG `tEXt`/`iTXt` NAI metadata -> `NovelAI`.
3. With no ordinary PNG text, complete EXIF NAI metadata -> `NovelAI`.
4. With neither earlier result, valid Alpha `stealth_pngcomp` NAI metadata ->
   `NovelAI`.
5. If an earlier ordinary field is present but incomplete while zTXt/Alpha can
   recover NAI parameters -> `NAI含参但不可直接读取的`.
6. zTXt/JPEG Comment recovery without an official-readable path ->
   `NAI含参但不可直接读取的`.
7. Valid A1111/Forge or other generation metadata -> `其他模型生成`; plain text
   or creator metadata -> reject.

Alpha is a normal NovelAI fallback channel, not evidence of unreadability. zTXt
is also not allowed to downgrade a complete ordinary `tEXt`/`iTXt` Comment.

Supported channels are PNG `tEXt`, `iTXt`, and `zTXt`, EXIF `UserComment`, and
NovelAI `stealth_pngcomp` Alpha data. The parser keeps ordinary PNG text separate
from zTXt so Pillow's merged `Image.info` view cannot change the official fallback
decision. SHA-256 deduplicates stored image entities;
message occurrences remain in SQLite.

## Source Compatibility

The parser keeps legacy `novelai-stealth` and `novelai-ztxt` values mapped to
`NAI含参但不可直接读取的`. Parser version `3` records the corrected official
fallback simulation in the asset table.

Normal Worker startup may set `storage.migrate_existing_accepted_on_start=false`
to leave existing files in their historical directories. This does not affect
classification of newly collected files. Explicit provenance migration remains
available when an intentional historical re-layout is required.
