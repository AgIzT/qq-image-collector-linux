#!/usr/bin/env python3
"""Archive the image repository to Cloudflare R2 and reclaim local disk.

The image volume fills faster than it can be emptied by hand, and running out of
space does not fail loudly - the collector retries, backs off to an hour, and
looks healthy while it stores nothing. So this runs from cron: upload whole days
oldest-first, then delete the days that are provably safe to delete.

Three rules shape the design.

Object keys are content-addressed (``originals/<sha[:2]>/<sha>.<ext>``) rather
than mirroring ``final/<category>/<day>/<filename>``. The repository filenames
embed the QQ group number and the sender's QQ number, and an object key is the
one part of the archive that leaks into every URL, log and listing. The category
and day still exist - as fields in the day index, where they belong.

The reader's metadata is split by size. A day index carries what a listing page
needs (prompt, negative, sampler, seed, model family) and stays a few hundred KB
gzipped; the raw ``metadata_json`` - ComfyUI workflows average 165 KB and reach
2.4 MB - becomes one ``meta/`` object per image, fetched only when something
opens that image. Provenance that must not be published (group, sender, message
id, original filename) goes to ``private/``, which no public route serves.

Nothing is deleted that has not been confirmed present in R2 by a HEAD in the
same run, and no day is deleted while it can still gain files: backfill writes
by ``sent_at``, so a day that ended a week ago can still grow today.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import gzip
import hashlib
import hmac
import json
import os
import re
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

CONFIG_PATH = Path("/etc/qqai-archive.json")
STATE_PATH = Path("/var/lib/qqai-state/archive_state.sqlite3")
LOCK_PATH = Path("/var/run/qqai-archive.lock")
COLLECTOR_DB = "/var/lib/qqai-state/collector_state.sqlite3"

# The database stores the path the collector sees inside its container.
CONTAINER_ROOT = "/data/qq-image-collector"
HOST_ROOT = "/mnt/disk-1/qq-ai-image-collector/repository"

DEFAULT_BUCKET = "qqai-image-archive"
DEFAULT_KEEP_DAYS = 14
DEFAULT_SEAL_HOURS = 6
DEFAULT_WORKERS = 8

# Reading one file at a time gets 3 MB/s off the image volume and eight at a
# time gets 10; past that it drops again.
MAX_WORKERS = 16

RETRYABLE_STATUSES = {408, 409, 425, 429, 500, 502, 503, 504}
RETRYABLE_EXCEPTIONS = (TimeoutError, urllib.error.URLError, ConnectionError, OSError)

FILENAME = re.compile(r"^(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})_g(\d+)_u(\d+)_([0-9a-f]+)\.")

# Eight hours ahead of UTC: the day directories are named in Beijing time.
TZ = dt.timezone(dt.timedelta(hours=8))


def log(message: str) -> None:
    stamp = dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{stamp}] {message}", flush=True)


# --------------------------------------------------------------------------
# R2
# --------------------------------------------------------------------------


class R2Client:
    """The subset of the S3 API this needs, over urllib, signed with SigV4."""

    def __init__(self, cfg: dict):
        self.access_key = cfg["access_key_id"]
        self.secret_key = cfg["secret_access_key"]
        self.bucket = cfg.get("bucket", DEFAULT_BUCKET)
        self.region = cfg.get("region") or "auto"
        self.host = f"{cfg['account_id']}.r2.cloudflarestorage.com"
        self.timeout = float(cfg.get("request_timeout", 180.0))
        self.retries = int(cfg.get("request_retries", 4))

    def _signing_key(self, datestamp: str) -> bytes:
        def sign(key: bytes, msg: str) -> bytes:
            return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

        k_date = sign(("AWS4" + self.secret_key).encode("utf-8"), datestamp)
        k_region = sign(k_date, self.region)
        k_service = sign(k_region, "s3")
        return sign(k_service, "aws4_request")

    def _once(self, method, key, body, headers, payload_hash, query):
        headers = dict(headers or {})
        now = dt.datetime.now(dt.timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        datestamp = now.strftime("%Y%m%d")
        path = "/" + self.bucket + ("/" + key if key else "")
        canonical_uri = urllib.parse.quote(path, safe="/~")
        canonical_query = "&".join(
            f"{urllib.parse.quote(k, safe='-_.~')}={urllib.parse.quote(str(v), safe='-_.~')}"
            for k, v in sorted((query or {}).items())
        )

        headers.update({
            "host": self.host,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        })
        ordered = sorted(headers, key=str.lower)
        canonical_headers = "".join(f"{k.lower()}:{str(headers[k]).strip()}\n" for k in ordered)
        signed_headers = ";".join(k.lower() for k in ordered)
        canonical_request = "\n".join([
            method, canonical_uri, canonical_query,
            canonical_headers, signed_headers, payload_hash,
        ])
        scope = f"{datestamp}/{self.region}/s3/aws4_request"
        string_to_sign = "\n".join([
            "AWS4-HMAC-SHA256", amz_date, scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ])
        signature = hmac.new(
            self._signing_key(datestamp), string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        headers["Authorization"] = (
            f"AWS4-HMAC-SHA256 Credential={self.access_key}/{scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )

        url = f"https://{self.host}{canonical_uri}"
        if canonical_query:
            url += "?" + canonical_query
        request = urllib.request.Request(
            url, data=body if method not in ("GET", "HEAD") else None,
            headers=headers, method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return response.status, {k.lower(): v for k, v in response.headers.items()}, response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, {k.lower(): v for k, v in exc.headers.items()}, exc.read()

    def request(self, method, key="", body=b"", headers=None, payload_hash=None, query=None):
        if payload_hash is None:
            payload_hash = hashlib.sha256(body).hexdigest()
        attempts = max(1, self.retries + 1)
        for attempt in range(1, attempts + 1):
            try:
                status, response_headers, response_body = self._once(
                    method, key, body, headers, payload_hash, query
                )
            except RETRYABLE_EXCEPTIONS as exc:
                if attempt >= attempts:
                    raise
                wait = 2 ** (attempt - 1)
                log(f"  retry {attempt}/{attempts - 1} {method} {key}: {exc}; wait {wait}s")
                time.sleep(wait)
                continue
            if status in RETRYABLE_STATUSES and attempt < attempts:
                wait = 2 ** (attempt - 1)
                log(f"  retry {attempt}/{attempts - 1} {method} {key}: status {status}; wait {wait}s")
                time.sleep(wait)
                continue
            return status, response_headers, response_body
        raise AssertionError("unreachable")

    def put_bytes(self, key, body, content_type, sha=None, cache_control=None, encoding=None):
        headers = {"content-type": content_type, "content-length": str(len(body))}
        if cache_control:
            headers["cache-control"] = cache_control
        if encoding:
            headers["content-encoding"] = encoding
        status, _, response = self.request("PUT", key, body=body, headers=headers, payload_hash=sha)
        if status != 200:
            raise RuntimeError(f"PUT {key} -> {status}: {response[:400].decode('utf-8', 'replace')}")

    def head(self, key):
        status, headers, _ = self.request("HEAD", key)
        return status, headers

    def put_json(self, key, payload, cache_control="public, max-age=300"):
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        body = gzip.compress(raw, 6)
        self.put_bytes(
            key, body, "application/json; charset=utf-8",
            cache_control=cache_control, encoding="gzip",
        )
        return hashlib.sha256(raw).hexdigest()[:16], len(raw), len(body)


# --------------------------------------------------------------------------
# State
# --------------------------------------------------------------------------


def open_state(path: Path = STATE_PATH) -> sqlite3.Connection:
    """Archive bookkeeping, deliberately in its own database.

    The collector is the only writer of ``assets`` and ``images``; adding a
    column there to track upload progress would put a second writer on the
    tables the collector holds locks on.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS uploaded (
            sha256      TEXT PRIMARY KEY,
            key         TEXT NOT NULL,
            size        INTEGER NOT NULL,
            day         TEXT,
            origin      TEXT NOT NULL DEFAULT 'server',
            uploaded_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS uploaded_day ON uploaded(day);

        CREATE TABLE IF NOT EXISTS days (
            day         TEXT NOT NULL,
            origin      TEXT NOT NULL DEFAULT 'server',
            asset_count INTEGER,
            bytes       INTEGER,
            indexed_at  INTEGER,
            purged_at   INTEGER,
            purged_bytes INTEGER,
            categories  TEXT,
            families    TEXT,
            PRIMARY KEY (day, origin)
        );

        CREATE TABLE IF NOT EXISTS meta_uploaded (
            sha256      TEXT PRIMARY KEY,
            uploaded_at INTEGER NOT NULL
        );
        """
    )
    have = {row[1] for row in conn.execute("PRAGMA table_info(days)")}
    for column in ("categories", "families"):
        if column not in have:
            conn.execute(f"ALTER TABLE days ADD COLUMN {column} TEXT")
    conn.commit()
    return conn


# --------------------------------------------------------------------------
# Metadata shaping
# --------------------------------------------------------------------------


def _loads(value):
    if not value:
        return None
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        return None


def _clean(value, limit=8000):
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    return text[:limit]


def _novelai_fields(fields: dict) -> dict:
    """NovelAI writes the prompt as plain text and everything else as JSON in a string."""
    out = {"tags": _clean(fields.get("Description")), "model": _clean(fields.get("Source"), 200)}
    comment = _loads(fields.get("Comment"))
    if not isinstance(comment, dict):
        # A minority of records come through the EXIF channel instead.
        comment = _loads(fields.get("UserComment"))
    if isinstance(comment, dict):
        if not out["tags"]:
            out["tags"] = _clean(comment.get("prompt"))
        out["negative"] = _clean(comment.get("uc"))
        params = {
            "steps": comment.get("steps"),
            "sampler": comment.get("sampler"),
            "seed": comment.get("seed"),
            "scale": comment.get("scale"),
            "noiseSchedule": comment.get("noise_schedule"),
            "cfgRescale": comment.get("cfg_rescale"),
        }
        out["params"] = {k: v for k, v in params.items() if v is not None}
    return out


A1111_TAIL = re.compile(r"^(?:Steps|Sampler|CFG scale|Seed|Size|Model):", re.MULTILINE)


def _a1111_fields(fields: dict) -> dict:
    """A1111 writes one text blob: prompt, then negative, then a key/value tail."""
    text = fields.get("parameters")
    if not isinstance(text, str):
        return {}
    negative = None
    body = text
    if "Negative prompt:" in text:
        body, _, rest = text.partition("Negative prompt:")
        negative = rest
    tail_match = A1111_TAIL.search(negative if negative is not None else body)
    tail = ""
    if tail_match:
        if negative is not None:
            tail = negative[tail_match.start():]
            negative = negative[: tail_match.start()]
        else:
            tail = body[tail_match.start():]
            body = body[: tail_match.start()]
    params = {}
    for chunk in tail.replace("\n", ", ").split(","):
        key, _, value = chunk.partition(":")
        key, value = key.strip(), value.strip()
        if key and value:
            params[key] = value
    out = {"tags": _clean(body), "negative": _clean(negative)}
    if params:
        out["params"] = {
            "steps": params.get("Steps"),
            "sampler": params.get("Sampler"),
            "seed": params.get("Seed"),
            "scale": params.get("CFG scale"),
        }
        out["params"] = {k: v for k, v in out["params"].items() if v is not None}
        out["model"] = _clean(params.get("Model"), 200)
    return out


TEXT_KEYS = ("text", "string", "value", "prompt", "positive", "string_1", "text_positive")
NEG_KEY = re.compile(r"negative|\buc\b", re.I)
WIDGET_NOISE = re.compile(r"^(true|false|enable|disable|none|\d+(\.\d+)?)$", re.I)
STRING_INPUT = re.compile(r"^(string|text)_?\d*$", re.I)

# Where a graph does not label which encoder is which, the text says so itself:
# these terms belong to negative prompts and turn up in positives essentially
# never. Two distinct ones is not a coincidence.
NEGATIVE_MARKERS = re.compile(
    r"worst quality|low quality|normal quality|lowres|bad anatomy|bad hands|"
    r"missing fingers|extra digits?|fewer digits|jpeg artifacts|watermark|username|"
    r"signature|artist name|poorly drawn|bad proportions|extra limbs|mutated|"
    r"deformed|disfigured|artistic error|scan artifacts|very displeasing|"
    r"score_[1-4]|blank page|negative space|multiple views",
    re.I,
)


def _looks_negative(text: str) -> bool:
    return len({m.group(0).lower() for m in NEGATIVE_MARKERS.finditer(text)}) >= 2


def _widget_text(value, nodes, depth=0) -> str | None:
    """A literal widget value, or a ``[node_id, slot]`` link that has to be followed.

    ComfyUI inputs are either the value or a wire to whatever produces it, so a
    prompt typed into a ``CR Text`` node and wired into the encoder reads as a
    two-element list here, not a string. Following the wire is most of the
    difference between a prompt and a blank.
    """
    if isinstance(value, str):
        text = value.strip()
        return text if text and not WIDGET_NOISE.match(text) else None
    if depth < 4 and isinstance(value, list) and value and isinstance(value[0], (str, int)):
        node = nodes.get(str(value[0]))
        if isinstance(node, dict):
            inputs = node.get("inputs") or {}
            for key in TEXT_KEYS:
                found = _widget_text(inputs.get(key), nodes, depth + 1)
                if found:
                    return found
            # JoinStringMulti and friends: several numbered string inputs.
            parts = [_widget_text(v, nodes, depth + 1)
                     for k, v in inputs.items() if STRING_INPUT.match(str(k))]
            parts = [p for p in parts if p]
            if parts:
                return ", ".join(parts)
    return None


def _sort_prompt(text: str, labelled_negative: bool, positive: list, negative: list) -> None:
    if labelled_negative or _looks_negative(text):
        negative.append(text)
    else:
        positive.append(text)


def _from_node_graph(graph, workflow=None) -> dict | None:
    """Recover the prompt from a node graph. Every step of this is a guess.

    Which node holds the prompt is a convention rather than a rule, and the
    conventions differ per UI pack, so this tries three of them in order of how
    much it can trust the answer: the standard encoders, then any node carrying
    a long string under a prompt-ish key, then the saved UI workflow for images
    that have no API graph at all. Whatever it returns is labelled a guess, and
    the untouched graph is in the ``meta/`` object regardless.
    """
    if isinstance(graph, str):
        graph = _loads(graph)
    positive, negative = [], []

    if isinstance(graph, dict) and graph:
        for node in graph.values():
            if not isinstance(node, dict):
                continue
            if "CLIPTextEncode" not in str(node.get("class_type", "")):
                continue
            text = _widget_text((node.get("inputs") or {}).get("text"), graph)
            if text:
                title = str((node.get("_meta") or {}).get("title", ""))
                _sort_prompt(text, bool(NEG_KEY.search(title)), positive, negative)

        if not (positive or negative):
            # No standard encoder, or none resolved: some packs keep the prompt
            # on a node of their own (WeiLinPromptUI, StringConstantMultiline).
            for node in graph.values():
                if not isinstance(node, dict):
                    continue
                for key, value in (node.get("inputs") or {}).items():
                    if not isinstance(value, str):
                        continue
                    text = value.strip()
                    if len(text) < 12 or WIDGET_NOISE.match(text):
                        continue
                    if NEG_KEY.search(str(key)) or _looks_negative(text):
                        negative.append(text)
                    elif str(key).lower() in TEXT_KEYS:
                        positive.append(text)

    if not (positive or negative):
        # Only the UI workflow was saved. Its nodes keep widget values in a
        # positional list, so there are no key names to go by - only the node
        # type, its title, and what the text looks like.
        if isinstance(workflow, str):
            workflow = _loads(workflow)
        if isinstance(workflow, dict):
            for node in workflow.get("nodes") or []:
                if not isinstance(node, dict):
                    continue
                kind = str(node.get("type", ""))
                if not any(k in kind for k in ("CLIPTextEncode", "Text", "Prompt")):
                    continue
                title = str(node.get("title") or "")
                labelled = bool(NEG_KEY.search(title) or NEG_KEY.search(kind))
                for value in node.get("widgets_values") or []:
                    if not isinstance(value, str):
                        continue
                    text = value.strip()
                    if len(text) < 12 or WIDGET_NOISE.match(text):
                        continue
                    _sort_prompt(text, labelled, positive, negative)

    if not (positive or negative):
        return None

    def join(parts):
        seen, ordered = set(), []
        for part in parts:
            if part not in seen:
                seen.add(part)
                ordered.append(part)
        return "\n".join(ordered) or None

    return {
        "tags": _clean(join(positive)),
        "negative": _clean(join(negative)),
        "promptSource": "node-graph-heuristic",
    }


def _comfyui_fields(fields: dict) -> dict:
    out = {"hasWorkflow": True}
    out.update(_from_node_graph(fields.get("prompt"), fields.get("workflow")) or {})
    return out


def _unknown_fields(fields: dict) -> dict:
    """Anything the parser could not attribute. Often a ComfyUI fork.

    The ``prompt`` here is frequently a serialised node graph rather than text;
    putting that straight into ``tags`` would give every such image a 5 KB
    "prompt" that is really JSON, and a title made of ``{"10002": {...``.
    """
    prompt = fields.get("prompt")
    from_graph = _from_node_graph(prompt, fields.get("workflow"))
    if from_graph:
        return {**from_graph, "hasWorkflow": True}
    if isinstance(prompt, dict):
        prompt = prompt.get("prompt") or prompt.get("text")
    if isinstance(prompt, str) and prompt.lstrip()[:1] in ("{", "["):
        # Structured but not a shape we recognise: keep it out of the prompt field.
        return {"hasWorkflow": True}
    return {"tags": _clean(prompt)}


SHAPERS = {
    "novelai": _novelai_fields,
    "novelai-unreadable": _novelai_fields,
    "novelai-stealth": _novelai_fields,
    "novelai-ztxt": _novelai_fields,
    "a1111-compatible": _a1111_fields,
    "comfyui": _comfyui_fields,
    "unknown-generator": _unknown_fields,
}


def title_for(tags: str | None, model: str | None, day: str) -> str:
    """A listing needs a label; the prompt's opening is the most useful one."""
    if tags:
        # Tag-style prompts open with something substantial (an artist tag, a
        # character name); natural-language ones open with "A cinematic," and
        # need more of the line before they say anything.
        first = re.split(r"[\n,]", tags.strip(), maxsplit=1)[0].strip()
        if len(first) >= 16:
            return first[:80]
        return " ".join(tags.split())[:80]
    if model:
        return f"{model[:40]} · {day}"
    return day


def light_record(row: sqlite3.Row) -> dict:
    """The per-image record a listing page loads. No provenance, no raw workflow."""
    source = row["metadata_source"]
    fields = _loads(row["metadata_json"]) or {}
    shaped = SHAPERS.get(source, lambda _f: {})(fields) if isinstance(fields, dict) else {}
    day = dt.datetime.fromtimestamp(row["canonical_sent_at"], TZ).strftime("%Y-%m-%d")
    record = {
        "id": row["sha256"],
        "title": title_for(shaped.get("tags"), shaped.get("model") or row["model"], day),
        "path": [row["category"], day],
        "ext": row["file_extension"],
        "width": row["width"],
        "height": row["height"],
        "size": row["file_size"],
        "sentAt": row["canonical_sent_at"],
        "category": row["category"],
        "metadataSource": source,
        "origin": "server",
    }
    if row["model_family"]:
        record["modelFamily"] = row["model_family"]
    if row["model"]:
        record["model"] = row["model"]
    for key in ("tags", "negative", "params", "model", "hasWorkflow", "promptSource"):
        value = shaped.get(key)
        if value:
            record.setdefault(key, value)
    return record


def private_record(row: sqlite3.Row) -> dict:
    """Everything the public index deliberately leaves out."""
    return {
        "sha256": row["sha256"],
        "file": row["local_path"].rsplit("/", 1)[-1],
        "groupId": row["canonical_group_id"],
        "senderUin": row["canonical_sender_uin"],
        "messageId": row["canonical_message_id"],
        "imageIndex": row["canonical_image_index"],
        "sentAt": row["canonical_sent_at"],
        "category": row["category"],
    }


# --------------------------------------------------------------------------
# Reading the collector database
# --------------------------------------------------------------------------


def collector_conn(path: str = COLLECTOR_DB) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def day_bounds(day: str) -> tuple[int, int]:
    start = dt.datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=TZ)
    return int(start.timestamp()), int((start + dt.timedelta(days=1)).timestamp())


def all_days(conn: sqlite3.Connection) -> list[str]:
    return [
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT date(canonical_sent_at, 'unixepoch', '+8 hours') AS d "
            "FROM assets ORDER BY d"
        )
    ]


def assets_for_day(conn: sqlite3.Connection, day: str) -> list[sqlite3.Row]:
    start, end = day_bounds(day)
    return conn.execute(
        """
        SELECT a.*, m.model, m.model_family
        FROM assets a LEFT JOIN asset_model m ON m.sha256 = a.sha256
        WHERE a.canonical_sent_at >= ? AND a.canonical_sent_at < ?
        ORDER BY a.canonical_sent_at
        """,
        (start, end),
    ).fetchall()


def pending_for_day(conn: sqlite3.Connection, day: str) -> int:
    """Backfill lands by ``sent_at``, so a finished day can still gain files."""
    start, end = day_bounds(day)
    return conn.execute(
        "SELECT count(*) FROM images WHERE status IN ('queued','deferred','downloading') "
        "AND sent_at >= ? AND sent_at < ?",
        (start, end),
    ).fetchone()[0]


def host_path(local_path: str) -> Path:
    return Path(local_path.replace(CONTAINER_ROOT, HOST_ROOT, 1))


def object_key(sha: str, ext: str) -> str:
    return f"originals/{sha[:2]}/{sha}{ext}"


def meta_key(sha: str) -> str:
    return f"meta/{sha[:2]}/{sha}.json"


# --------------------------------------------------------------------------
# Upload
# --------------------------------------------------------------------------


class Checkpoint:
    """Decide when to commit progress and say so.

    Counting objects alone is the wrong unit when the two ends of this run are
    so far apart: the server pushes 100 objects in a minute, while the machine
    holding the pre-server collection needs twelve. Every-100 there means a
    quarter hour of silence and a quarter hour of re-uploads after any
    interruption. Whichever comes first.
    """

    def __init__(self, every=100, seconds=60.0):
        self.every = every
        self.seconds = seconds
        self.last = time.time()

    def due(self, count: int) -> bool:
        if count % self.every == 0 or time.time() - self.last >= self.seconds:
            self.last = time.time()
            return True
        return False


class Budget:
    """Lets a cron run stop cleanly part-way and pick up where it left off."""

    def __init__(self, max_seconds=None, max_bytes=None):
        self.deadline = time.time() + max_seconds if max_seconds else None
        self.remaining = max_bytes
        self.lock = threading.Lock()

    def spend(self, size: int) -> bool:
        with self.lock:
            if self.deadline and time.time() > self.deadline:
                return False
            if self.remaining is not None:
                if self.remaining <= 0:
                    return False
                self.remaining -= size
            return True

    def exhausted(self) -> bool:
        with self.lock:
            if self.deadline and time.time() > self.deadline:
                return True
            return self.remaining is not None and self.remaining <= 0


def upload_day(client, state, conn, day, args, budget) -> dict:
    rows = assets_for_day(conn, day)
    done = {
        row[0]
        for row in state.execute("SELECT sha256 FROM uploaded WHERE day = ?", (day,))
    }
    todo = [r for r in rows if r["sha256"] not in done]
    stats = {"day": day, "total": len(rows), "uploaded": 0, "skipped": len(done),
             "bytes": 0, "missing": 0, "failed": 0, "stopped": False}
    if not todo:
        log(f"{day}: {len(rows)} assets, all already uploaded")
        return stats

    log(f"{day}: {len(rows)} assets, {len(todo)} to upload")
    lock = threading.Lock()

    def send(row):
        if budget.exhausted():
            return ("stop", row, 0)
        path = host_path(row["local_path"])
        try:
            body = path.read_bytes()
        except FileNotFoundError:
            return ("missing", row, 0)
        actual = hashlib.sha256(body).hexdigest()
        if actual != row["sha256"]:
            # Not fatal: the row is stale or the file was replaced. Store it
            # under the hash it actually has so the bytes are never lost.
            log(f"  sha mismatch {path.name}: db={row['sha256'][:10]} disk={actual[:10]}")
            return ("mismatch", row, 0)
        if not budget.spend(len(body)):
            return ("stop", row, 0)
        client.put_bytes(
            object_key(row["sha256"], row["file_extension"]), body,
            content_type_for(row["file_extension"]), sha=actual,
            cache_control="public, max-age=31536000, immutable",
        )
        return ("ok", row, len(body))

    stopped = False
    checkpoint = Checkpoint()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(send, row): row for row in todo}
        for future in concurrent.futures.as_completed(futures):
            row = futures[future]
            try:
                outcome, row, size = future.result()
            except Exception as exc:  # noqa: BLE001 - one bad file must not stop the run
                log(f"  FAILED {row['sha256'][:10]}: {exc}")
                stats["failed"] += 1
                continue
            if outcome == "stop":
                stopped = True
                continue
            if outcome == "missing":
                stats["missing"] += 1
                continue
            if outcome == "mismatch":
                stats["failed"] += 1
                continue
            with lock:
                state.execute(
                    "INSERT OR REPLACE INTO uploaded (sha256, key, size, day, origin, uploaded_at) "
                    "VALUES (?,?,?,?,'server',?)",
                    (row["sha256"], object_key(row["sha256"], row["file_extension"]),
                     size, day, int(time.time())),
                )
                stats["uploaded"] += 1
                stats["bytes"] += size
                if checkpoint.due(stats["uploaded"]):
                    state.commit()
                    log(f"  {stats['uploaded']}/{len(todo)} ({stats['bytes'] / 1048576:.0f} MB)")
    state.commit()
    stats["stopped"] = stopped
    log(f"{day}: uploaded {stats['uploaded']} ({stats['bytes'] / 1048576:.0f} MB)"
        + (f", missing {stats['missing']}" if stats["missing"] else "")
        + (f", failed {stats['failed']}" if stats["failed"] else "")
        + (", stopped on budget" if stopped else ""))
    return stats


CONTENT_TYPES = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                 ".webp": "image/webp", ".gif": "image/gif", ".avif": "image/avif"}


def content_type_for(ext: str) -> str:
    return CONTENT_TYPES.get(ext.lower(), "application/octet-stream")


def write_day_indexes(client, state, conn, day, args) -> dict | None:
    """Publish the day's listing, its private provenance, and any missing meta blobs."""
    rows = assets_for_day(conn, day)
    if not rows:
        return None
    have = {r[0] for r in state.execute("SELECT sha256 FROM uploaded WHERE day = ?", (day,))}
    rows = [r for r in rows if r["sha256"] in have]
    if not rows:
        return None

    meta_done = {r[0] for r in state.execute("SELECT sha256 FROM meta_uploaded")}
    pending_meta = [r for r in rows if r["sha256"] not in meta_done and r["metadata_json"]]
    if pending_meta:
        log(f"{day}: uploading {len(pending_meta)} metadata blobs")
        lock = threading.Lock()

        def send_meta(row):
            payload = {
                "sha256": row["sha256"],
                "metadataSource": row["metadata_source"],
                "parserVersion": row["parser_version"],
                "metadata": _loads(row["metadata_json"]),
            }
            client.put_json(meta_key(row["sha256"]), payload,
                            cache_control="public, max-age=31536000, immutable")
            return row["sha256"]

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
            for future in concurrent.futures.as_completed(
                [pool.submit(send_meta, r) for r in pending_meta]
            ):
                try:
                    sha = future.result()
                except Exception as exc:  # noqa: BLE001
                    log(f"  meta FAILED: {exc}")
                    continue
                with lock:
                    state.execute(
                        "INSERT OR REPLACE INTO meta_uploaded VALUES (?,?)",
                        (sha, int(time.time())),
                    )
        state.commit()

    entries = [light_record(r) for r in rows]
    categories, families = {}, {}
    for entry in entries:
        categories[entry["category"]] = categories.get(entry["category"], 0) + 1
        family = entry.get("modelFamily")
        if family:
            families[family] = families.get(family, 0) + 1
    total_bytes = sum(r["file_size"] for r in rows)
    index = {
        "id": day,
        "type": "day",
        "origin": "server",
        "generatedAt": dt.datetime.now(TZ).isoformat(timespec="seconds"),
        "entryCount": len(entries),
        "bytes": total_bytes,
        "categories": categories,
        "modelFamilies": families,
        "entries": entries,
    }
    rev, raw_size, gz_size = client.put_json(f"data/days/{day}.json", index)
    client.put_json(
        f"private/days/{day}.json",
        {"day": day, "generatedAt": index["generatedAt"],
         "records": [private_record(r) for r in rows]},
        cache_control="private, no-store",
    )
    state.execute(
        "INSERT OR REPLACE INTO days (day, origin, asset_count, bytes, indexed_at, "
        "categories, families, purged_at, purged_bytes) VALUES (?, 'server', ?, ?, ?, ?, ?, "
        "(SELECT purged_at FROM days WHERE day=? AND origin='server'), "
        "(SELECT purged_bytes FROM days WHERE day=? AND origin='server'))",
        (day, len(entries), total_bytes, int(time.time()),
         json.dumps(categories, ensure_ascii=False), json.dumps(families, ensure_ascii=False),
         day, day),
    )
    state.commit()
    log(f"{day}: index {len(entries)} entries, {raw_size / 1024:.0f} KB -> {gz_size / 1024:.0f} KB gz")
    return {"day": day, "origin": "server", "entryCount": len(entries),
            "bytes": total_bytes, "rev": rev,
            "categories": categories, "modelFamilies": families}


# --------------------------------------------------------------------------
# Purge
# --------------------------------------------------------------------------


def purge_day(client, state, conn, day, args) -> int:
    """Delete a day's local files, but only what R2 confirms it holds right now."""
    today = dt.datetime.now(TZ).date()
    age = (today - dt.datetime.strptime(day, "%Y-%m-%d").date()).days
    if age < args.keep_days:
        return 0

    pending = pending_for_day(conn, day)
    if pending:
        log(f"{day}: SKIP purge, {pending} images still queued for that date")
        return 0

    rows = assets_for_day(conn, day)
    if not rows:
        return 0

    paths = [host_path(r["local_path"]) for r in rows]
    existing = [p for p in paths if p.exists()]
    if not existing:
        return 0

    newest = max(p.stat().st_mtime for p in existing)
    quiet_hours = (time.time() - newest) / 3600
    if quiet_hours < args.seal_hours:
        log(f"{day}: SKIP purge, a file changed {quiet_hours:.1f}h ago "
            f"(needs {args.seal_hours}h quiet)")
        return 0

    # Confirm in this run, not from bookkeeping: the whole point of the check is
    # to be independent of the table that says the upload happened.
    log(f"{day}: verifying {len(rows)} objects in R2 before deleting")
    failures = []
    lock = threading.Lock()

    def check(row):
        key = object_key(row["sha256"], row["file_extension"])
        status, headers = client.head(key)
        return row, key, status, int(headers.get("content-length") or 0)

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        for future in concurrent.futures.as_completed([pool.submit(check, r) for r in rows]):
            try:
                row, key, status, length = future.result()
            except Exception as exc:  # noqa: BLE001
                with lock:
                    failures.append(f"HEAD error: {exc}")
                continue
            with lock:
                if status != 200:
                    failures.append(f"{key} -> HTTP {status}")
                elif length != row["file_size"]:
                    failures.append(f"{key} -> {length} bytes, expected {row['file_size']}")

    if failures:
        log(f"{day}: REFUSING to purge, {len(failures)} objects unverified")
        for line in failures[:5]:
            log(f"    {line}")
        return 0

    freed = 0
    removed = 0
    for row, path in zip(rows, paths):
        if not path.exists():
            continue
        if args.dry_run:
            freed += row["file_size"]
            removed += 1
            continue
        try:
            size = path.stat().st_size
            path.unlink()
            freed += size
            removed += 1
        except OSError as exc:
            log(f"  could not delete {path}: {exc}")

    # Directories are only removed once genuinely empty; a file that reappeared
    # between the HEAD sweep and now keeps its directory alive.
    if not args.dry_run:
        for directory in {p.parent for p in paths}:
            try:
                if directory.exists() and not any(directory.iterdir()):
                    directory.rmdir()
            except OSError:
                pass
        state.execute(
            "UPDATE days SET purged_at = ?, purged_bytes = ? WHERE day = ? AND origin = 'server'",
            (int(time.time()), freed, day),
        )
        state.commit()

    log(f"{day}: {'would free' if args.dry_run else 'freed'} "
        f"{freed / 1048576:.0f} MB ({removed} files)")
    return freed


# --------------------------------------------------------------------------
# Top-level index
# --------------------------------------------------------------------------


def merge_days(state, path: Path) -> int:
    """Record days that were uploaded from somewhere else.

    The pre-server Windows collection has no rows in ``assets``, so this host
    cannot derive those days itself; ``archive_legacy_to_r2.py`` uploads them
    and hands over a summary. Merging it here is what makes them appear in
    ``data/index.json`` alongside the server's own days.
    """
    rows = json.loads(path.read_text(encoding="utf-8"))
    for row in rows:
        state.execute(
            "INSERT OR REPLACE INTO days (day, origin, asset_count, bytes, indexed_at, "
            "categories, families, purged_at, purged_bytes) VALUES (?,?,?,?,?,?,?,"
            "(SELECT purged_at FROM days WHERE day=? AND origin=?),"
            "(SELECT purged_bytes FROM days WHERE day=? AND origin=?))",
            (row["day"], row.get("origin", "legacy"), row["asset_count"], row["bytes"],
             int(time.time()),
             json.dumps(row.get("categories", {}), ensure_ascii=False),
             json.dumps(row.get("families", {}), ensure_ascii=False),
             row["day"], row.get("origin", "legacy"), row["day"], row.get("origin", "legacy")),
        )
    state.commit()
    log(f"merged {len(rows)} days from {path}")
    return len(rows)


def write_root_index(client, state, conn) -> None:
    """Roll the day indexes up. Counts describe the archive, not the library.

    Taking the totals from ``assets`` instead would read as though everything
    were already archived from the first day of a backfill that takes hours.
    """
    days = []
    categories, families = {}, {}
    total_count = total_bytes = 0
    for row in state.execute(
        "SELECT day, origin, asset_count, bytes, categories, families FROM days ORDER BY day"
    ):
        day, origin, count, size, day_categories, day_families = row
        days.append({"day": day, "origin": origin, "entryCount": count, "bytes": size,
                     "path": f"data/days/{day}.json" if origin == "server"
                             else f"data/legacy/days/{day}.json"})
        total_count += count or 0
        total_bytes += size or 0
        for target, blob in ((categories, day_categories), (families, day_families)):
            for name, value in (json.loads(blob) if blob else {}).items():
                target[name] = target.get(name, 0) + value
    archived_here = state.execute(
        "SELECT coalesce(sum(asset_count), 0) FROM days WHERE origin = 'server'"
    ).fetchone()[0]
    remaining = conn.execute("SELECT count(*) FROM assets").fetchone()[0] - archived_here

    index = {
        "id": "qqai-archive",
        "title": "QQ 群 AI 原图归档",
        "generatedAt": dt.datetime.now(TZ).isoformat(timespec="seconds"),
        "originalKey": "originals/<sha256[0:2]>/<sha256><ext>",
        "metaKey": "meta/<sha256[0:2]>/<sha256>.json",
        "dayKey": "data/days/<YYYY-MM-DD>.json",
        "entryCount": total_count,
        "bytes": total_bytes,
        "dayCount": len(days),
        "pendingCount": max(0, remaining),
        "categories": categories,
        "modelFamilies": families,
        "days": days,
    }
    client.put_json("data/index.json", index, cache_control="public, max-age=60")
    log(f"index: {len(days)} days, {total_count} entries, {total_bytes / 1073741824:.1f} GB"
        + (f", {remaining} not archived yet" if remaining > 0 else ""))


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------


def load_config(path: Path) -> dict:
    if not path.exists():
        raise SystemExit(
            f"missing {path}\n"
            "Create it with the R2 credentials, then rerun:\n"
            '  qqai-set-r2 <account_id> <access_key_id> <secret_access_key>'
        )
    return json.loads(path.read_text(encoding="utf-8"))


def acquire_lock() -> object:
    """cron can fire again while the previous run is still uploading."""
    import fcntl  # POSIX only; the legacy uploader imports this module on Windows.

    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    handle = LOCK_PATH.open("w")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        log("another archive run holds the lock; exiting")
        raise SystemExit(0)
    handle.write(str(os.getpid()))
    handle.flush()
    return handle


def needs_index(state, day: str, asset_count: int) -> bool:
    """Rewrite a day index when it is missing or the day has since gained files."""
    row = state.execute(
        "SELECT asset_count FROM days WHERE day = ? AND origin = 'server'", (day,)
    ).fetchone()
    return row is None or row[0] != asset_count


def print_status(state, conn) -> None:
    total, uploaded_bytes = state.execute(
        "SELECT count(*), coalesce(sum(size), 0) FROM uploaded"
    ).fetchone()
    assets, asset_bytes = conn.execute(
        "SELECT count(*), coalesce(sum(file_size), 0) FROM assets"
    ).fetchone()
    print(f"assets in library : {assets:>7}  ({asset_bytes / 1073741824:.1f} GB)")
    print(f"objects in R2     : {total:>7}  ({uploaded_bytes / 1073741824:.1f} GB)")
    print(f"remaining         : {assets - total:>7}")
    print()
    print(f"{'day':<12}{'assets':>8}{'in R2':>8}{'GB':>7}  state")
    for day in all_days(conn):
        rows = conn.execute(
            "SELECT count(*), coalesce(sum(file_size), 0) FROM assets "
            "WHERE canonical_sent_at >= ? AND canonical_sent_at < ?",
            day_bounds(day),
        ).fetchone()
        done = state.execute("SELECT count(*) FROM uploaded WHERE day = ?", (day,)).fetchone()[0]
        purged = state.execute(
            "SELECT purged_at FROM days WHERE day = ? AND origin = 'server'", (day,)
        ).fetchone()
        mark = "purged" if purged and purged[0] else ("complete" if done >= rows[0] else "")
        print(f"{day:<12}{rows[0]:>8}{done:>8}{rows[1] / 1073741824:>7.1f}  {mark}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--upload", action="store_true", help="upload days that are not in R2 yet")
    parser.add_argument("--purge", action="store_true",
                        help="delete local files for verified days older than --keep-days")
    parser.add_argument("--index", action="store_true", help="rewrite data/index.json")
    parser.add_argument("--merge-days", type=Path,
                        help="merge a day summary produced by archive_legacy_to_r2.py, so days "
                             "uploaded from another machine appear in data/index.json")
    parser.add_argument("--reindex", action="store_true",
                        help="rewrite the day indexes from the database without re-uploading "
                             "images (use after changing how records are shaped)")
    parser.add_argument("--status", action="store_true", help="print progress and exit")
    parser.add_argument("--day", action="append", help="restrict to this day (repeatable)")
    parser.add_argument("--keep-days", type=int, default=None,
                        help=f"days of images to keep on disk (default {DEFAULT_KEEP_DAYS})")
    parser.add_argument("--seal-hours", type=float, default=None,
                        help="hours a day must go untouched before it may be purged")
    parser.add_argument("--workers", type=int, default=None,
                        help=f"parallel transfers (default {DEFAULT_WORKERS}, max {MAX_WORKERS})")
    parser.add_argument("--max-seconds", type=float, help="stop uploading after this long")
    parser.add_argument("--max-bytes", type=float, help="stop uploading after this many bytes")
    parser.add_argument("--dry-run", action="store_true", help="do not delete anything")
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    parser.add_argument("--state", type=Path, default=STATE_PATH)
    args = parser.parse_args()

    cfg = load_config(args.config)
    args.keep_days = args.keep_days if args.keep_days is not None else cfg.get("keep_days", DEFAULT_KEEP_DAYS)
    args.seal_hours = args.seal_hours if args.seal_hours is not None else cfg.get("seal_hours", DEFAULT_SEAL_HOURS)
    args.workers = min(MAX_WORKERS, args.workers or cfg.get("workers", DEFAULT_WORKERS))

    state = open_state(args.state)
    conn = collector_conn(cfg.get("collector_db", COLLECTOR_DB))

    if args.status:
        print_status(state, conn)
        return 0

    if not (args.upload or args.purge or args.index or args.reindex or args.merge_days):
        parser.error("nothing to do: pass --upload, --purge, --reindex, --index, "
                     "--merge-days or --status")

    lock = acquire_lock()  # noqa: F841 - held for the lifetime of the process
    client = R2Client(cfg)
    if args.merge_days:
        merge_days(state, args.merge_days)
    budget = Budget(args.max_seconds, args.max_bytes)
    days = args.day or all_days(conn)

    if args.upload:
        for day in days:
            if budget.exhausted():
                log("budget spent; the next run continues from here")
                break
            stats = upload_day(client, state, conn, day, args, budget)
            if stats["uploaded"] or needs_index(state, day, stats["total"]):
                write_day_indexes(client, state, conn, day, args)

    if args.reindex:
        for day in days:
            write_day_indexes(client, state, conn, day, args)

    if args.purge:
        freed = 0
        for day in days:
            freed += purge_day(client, state, conn, day, args)
        log(f"purge: {'would free' if args.dry_run else 'freed'} {freed / 1073741824:.1f} GB")

    if args.index or args.upload or args.reindex or args.merge_days:
        write_root_index(client, state, conn)

    return 0


if __name__ == "__main__":
    sys.exit(main())
