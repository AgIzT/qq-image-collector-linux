"""Capture collection changes and keep a narrow date index off the collector's DB.

Only the append-only feed is written by the collector. The archive database
owns its cursor, projection and pending dates, committed in a single transaction.
"""
from datetime import datetime, timedelta, timezone
import json

SCHEMA = """
CREATE TABLE IF NOT EXISTS archive_feed_identity (epoch TEXT NOT NULL);
INSERT INTO archive_feed_identity SELECT lower(hex(randomblob(16)))
  WHERE NOT EXISTS (SELECT 1 FROM archive_feed_identity);
CREATE TABLE IF NOT EXISTS archive_asset_changes (seq INTEGER PRIMARY KEY AUTOINCREMENT, sha TEXT NOT NULL);
CREATE TRIGGER IF NOT EXISTS archive_asset_added AFTER INSERT ON assets
BEGIN INSERT INTO archive_asset_changes(sha) VALUES (NEW.sha256); END;
CREATE TRIGGER IF NOT EXISTS archive_asset_changed AFTER UPDATE OF
  local_path, category, file_extension, file_size, width, height, metadata_source,
  metadata_json, parser_version, canonical_group_id, canonical_sender_uin,
  canonical_message_id, canonical_image_index, canonical_sent_at ON assets
BEGIN INSERT INTO archive_asset_changes(sha) VALUES (NEW.sha256); END;
CREATE TRIGGER IF NOT EXISTS archive_asset_removed AFTER DELETE ON assets
BEGIN INSERT INTO archive_asset_changes(sha) VALUES (OLD.sha256); END;
CREATE TRIGGER IF NOT EXISTS archive_model_added AFTER INSERT ON asset_model
BEGIN INSERT INTO archive_asset_changes(sha) VALUES (NEW.sha256); END;
CREATE TRIGGER IF NOT EXISTS archive_model_changed AFTER UPDATE OF model, model_family ON asset_model
WHEN OLD.model IS NOT NEW.model OR OLD.model_family IS NOT NEW.model_family
BEGIN INSERT INTO archive_asset_changes(sha) VALUES (NEW.sha256); END;
CREATE TRIGGER IF NOT EXISTS archive_model_removed AFTER DELETE ON asset_model
BEGIN INSERT INTO archive_asset_changes(sha) VALUES (OLD.sha256); END;
"""

STATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS archive_feed_cursor (
    id INTEGER PRIMARY KEY CHECK (id=1), epoch TEXT NOT NULL, seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS archive_asset_dates (sha TEXT PRIMARY KEY, day TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS archive_asset_dates_day ON archive_asset_dates(day,sha);
CREATE TABLE IF NOT EXISTS archive_pending_days (day TEXT PRIMARY KEY, retry_at INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS archive_day_content (day TEXT PRIMARY KEY, fingerprint TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS archive_root_pending (id INTEGER PRIMARY KEY CHECK (id=1));
"""
LOCAL_TZ = timezone(timedelta(hours=8))


def install(db):
    # No index build or scan of the collector's large assets table under a write lock.
    db.executescript("BEGIN IMMEDIATE;\n" + SCHEMA + "\nCOMMIT;")


def shanghai_day(timestamp):
    return datetime.fromtimestamp(timestamp, LOCAL_TZ).date().isoformat() if timestamp is not None else None


def stage(collector, archive):
    archive.executescript(STATE_SCHEMA)
    collector.execute("BEGIN")
    try:
        epoch = collector.execute("SELECT epoch FROM archive_feed_identity").fetchone()[0]
        high = collector.execute("SELECT coalesce(max(seq),0) FROM archive_asset_changes").fetchone()[0]
        cursor = archive.execute("SELECT epoch,seq FROM archive_feed_cursor WHERE id=1").fetchone()
        with archive:
            if cursor is None or cursor[0] != epoch or cursor[1] > high:
                archive.execute("DELETE FROM archive_asset_dates")
                rows = collector.execute("SELECT sha256,canonical_sent_at FROM assets")
                archive.executemany(
                    "INSERT INTO archive_asset_dates VALUES(?,?)",
                    ((sha, shanghai_day(ts)) for sha, ts in rows if ts is not None),
                )
                # Seed incomplete dates using the existing ledger, plus recent days
                # that may have been updated before installing the feed.
                archive.execute("""
                    INSERT OR REPLACE INTO archive_pending_days(day)
                    SELECT d.day FROM archive_asset_dates d
                    LEFT JOIN uploaded u ON u.sha256=d.sha
                    LEFT JOIN days recorded ON recorded.day=d.day AND recorded.origin='server'
                    GROUP BY d.day
                    HAVING count(*) != count(CASE WHEN u.day=d.day THEN u.sha256 END) OR count(*) != coalesce(max(recorded.asset_count),0)
                       OR d.day >= date('now','+8 hours','-1 day')
                """)
            else:
                events = collector.execute(
                    "SELECT seq,sha FROM archive_asset_changes WHERE seq>? ORDER BY seq LIMIT 10000", (cursor[1],)
                ).fetchall()
                if not events:
                    return
                high = events[-1][0]
                hashes = json.dumps(sorted({row[1] for row in events}))
                changed_days = set()
                if events:
                    changed_days.update(row[0] for row in archive.execute(
                        "SELECT DISTINCT day FROM archive_asset_dates WHERE sha IN (SELECT value FROM json_each(?))", (hashes,)
                    ))
                    archive.execute("DELETE FROM archive_asset_dates WHERE sha IN (SELECT value FROM json_each(?))", (hashes,))
                    rows = collector.execute(
                        "SELECT sha256,canonical_sent_at FROM assets WHERE sha256 IN (SELECT value FROM json_each(?))",
                        (hashes,),
                    )
                    for sha, ts in rows:
                        day = shanghai_day(ts)
                        if day is not None:
                            changed_days.add(day)
                            archive.execute("INSERT INTO archive_asset_dates VALUES(?,?)", (sha, day))
                archive.executemany(
                    "INSERT INTO archive_pending_days(day) VALUES(?) ON CONFLICT(day) DO UPDATE SET retry_at=0",
                    ((day,) for day in sorted(changed_days)),
                )
            archive.execute("INSERT OR REPLACE INTO archive_feed_cursor VALUES(1,?,?)", (epoch, high))
    finally:
        collector.rollback()


def select_days(state, now, limit):
    rows = state.execute(
        "SELECT day FROM archive_pending_days WHERE retry_at<=? ORDER BY retry_at, day DESC LIMIT ?",
        (now, limit),
    )
    return {day: json.dumps([r[0] for r in state.execute(
        "SELECT sha FROM archive_asset_dates WHERE day=? ORDER BY sha", (day,)
    )]) for (day,) in rows}


def finish_day(state, day, retry_at=None):
    with state:
        if retry_at is None:
            state.execute("DELETE FROM archive_pending_days WHERE day=?", (day,))
        else:
            state.execute("UPDATE archive_pending_days SET retry_at=? WHERE day=?", (retry_at, day))
