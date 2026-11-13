"""SQLite schema. The events table is the append-only interaction ledger."""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (          -- one row per ingested file; makes ingest idempotent
    file_id     INTEGER PRIMARY KEY,
    drop_name   TEXT NOT NULL,
    kind        TEXT NOT NULL CHECK (kind IN ('leads', 'events')),
    relpath     TEXT NOT NULL,
    sha256      TEXT NOT NULL,
    workflow_id TEXT,                       -- leads files only
    batch_id    TEXT,                       -- leads files only (file stem)
    stats_json  TEXT NOT NULL,              -- per-file data-quality counters
    UNIQUE (drop_name, kind, relpath)
);

CREATE TABLE IF NOT EXISTS leads (
    lead_id      INTEGER PRIMARY KEY,
    workflow_id  TEXT NOT NULL,
    phone_hash   TEXT NOT NULL,             -- HMAC of canonical phone (no plaintext)
    phone_last4  TEXT NOT NULL,
    external_ref TEXT NOT NULL,
    batch_id     TEXT NOT NULL,             -- batch that first created the lead; never changes
    attrs_json   TEXT NOT NULL,             -- non-PII schema fields
    pii_enc      BLOB NOT NULL,             -- Fernet-encrypted PII fields
    UNIQUE (workflow_id, phone_hash)
);

CREATE TABLE IF NOT EXISTS rejections (     -- no PII: only position and reason
    file_id     INTEGER NOT NULL REFERENCES files(file_id),
    workflow_id TEXT NOT NULL,
    batch_id    TEXT NOT NULL,
    row_number  INTEGER NOT NULL,
    reason      TEXT NOT NULL,
    PRIMARY KEY (file_id, row_number)
);

CREATE TABLE IF NOT EXISTS events (         -- append-only ledger
    event_id         TEXT PRIMARY KEY,
    workflow_id      TEXT NOT NULL,
    event_type       TEXT NOT NULL,
    call_id          TEXT NOT NULL,
    occurred_at_us   INTEGER NOT NULL,      -- UTC epoch microseconds
    has_phone        INTEGER NOT NULL,      -- 0: no phone in event, 1: a phone was given
    phone_hash       TEXT,                  -- NULL if absent or not a valid number
    disposition_code TEXT,
    payload_json     TEXT NOT NULL,
    file_id          INTEGER NOT NULL REFERENCES files(file_id),
    line_no          INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS events_phone ON events (workflow_id, phone_hash);

CREATE TRIGGER IF NOT EXISTS events_no_update BEFORE UPDATE ON events
BEGIN SELECT RAISE(ABORT, 'ledger is append-only'); END;
CREATE TRIGGER IF NOT EXISTS events_no_delete BEFORE DELETE ON events
BEGIN SELECT RAISE(ABORT, 'ledger is append-only'); END;
"""


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)  # explicit BEGIN/COMMIT
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    return conn
