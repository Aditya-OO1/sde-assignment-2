"""Lead and event ingestion. Each file is ingested in one transaction and recorded in `files`,
so re-running a drop is a no-op and a crash never leaves a half-ingested file."""
from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from collections import Counter
from pathlib import Path

from .normalize import normalize_header, normalize_phone, parse_date, parse_decimal, parse_timestamp_us
from .pii import Vault
from .schema import Field, Workflow

EVENT_BATCH = 5000


class IngestError(Exception):
    pass


# ---------------------------------------------------------------- lead values

def parse_value(field: Field, raw: str) -> str | None:
    """Parsed value for a trimmed non-empty cell, or None if invalid."""
    if field.type == "string":
        return raw
    if field.type == "phone":
        return normalize_phone(raw)
    if field.type == "date":
        d = parse_date(raw)
        return d.isoformat() if d else None
    if field.type == "decimal":
        return parse_decimal(raw)
    if field.type == "enum":
        v = raw.lower()
        return v if v in {x.lower() for x in field.values} else None
    raise ValueError(f"unknown field type {field.type}")


def validate_row(wf: Workflow, cells: dict[str, str | None]) -> tuple[dict, int] | str:
    """Walk fields in schema order. Returns (parsed values, warning count) or a rejection reason."""
    parsed: dict[str, str | None] = {}
    warnings = 0
    for f in wf.fields:
        raw = cells.get(f.name)
        if raw is None:
            if f.required:
                return f"missing_required:{f.name}"
            parsed[f.name] = None
            continue
        value = parse_value(f, raw)
        if value is None:
            if f.required:
                return "invalid_phone" if f.type == "phone" else f"invalid_value:{f.name}"
            warnings += 1
        parsed[f.name] = value
    return parsed, warnings


def _upsert_lead(conn, vault: Vault, wf: Workflow, batch_id: str, parsed: dict) -> bool:
    """Create or merge a lead. Returns True if it merged into an existing lead."""
    phone = parsed[wf.phone_field.name]
    phone_hash = vault.hash_phone(phone)
    row = conn.execute(
        "SELECT lead_id, external_ref, attrs_json, pii_enc FROM leads "
        "WHERE workflow_id=? AND phone_hash=?", (wf.workflow_id, phone_hash)).fetchone()
    if row:
        lead_id, ref, attrs, pii = row[0], row[1], json.loads(row[2]), vault.decrypt(row[3])
    else:
        lead_id, ref, attrs, pii = None, None, {}, {}
    for f in wf.fields:
        v = parsed[f.name]
        if f is wf.phone_field or v is None:
            continue
        if f.reference:
            ref = v
        elif f.pii:
            pii[f.name] = v
        else:
            attrs[f.name] = v
    for f in wf.fields:  # defaults apply to whatever is still null after merging
        if f.default is not None and not f.pii and attrs.get(f.name) is None:
            attrs[f.name] = f.default
    attrs_json = json.dumps(attrs, sort_keys=True)
    if lead_id is None:
        conn.execute(
            "INSERT INTO leads (workflow_id, phone_hash, phone_last4, external_ref, batch_id,"
            " attrs_json, pii_enc) VALUES (?,?,?,?,?,?,?)",
            (wf.workflow_id, phone_hash, phone[-4:], ref, batch_id, attrs_json, vault.encrypt(pii)))
        return False
    conn.execute("UPDATE leads SET external_ref=?, attrs_json=?, pii_enc=? WHERE lead_id=?",
                 (ref, attrs_json, vault.encrypt(pii), lead_id))
    return True


def _ingest_lead_rows(conn, vault, wf: Workflow, batch_id: str, path: Path, file_id: int) -> dict:
    stats = {"rows": 0, "rejected": 0, "reasons": Counter(), "merged": 0, "warnings": 0}
    lookup = wf.header_lookup
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        header = next(reader, [])
        columns: dict[str, int] = {}
        for idx, h in enumerate(header):
            field = lookup.get(normalize_header(h))
            if field and field.name not in columns:
                columns[field.name] = idx
        for row in reader:
            if all(not c.strip() for c in row):
                continue
            stats["rows"] += 1
            cells = {}
            for name, idx in columns.items():
                value = row[idx].strip() if idx < len(row) else ""
                cells[name] = value or None
            result = validate_row(wf, cells)
            if isinstance(result, str):
                stats["rejected"] += 1
                stats["reasons"][result] += 1
                conn.execute("INSERT INTO rejections VALUES (?,?,?,?,?)",
                             (file_id, wf.workflow_id, batch_id, stats["rows"], result))
                continue
            parsed, warnings = result
            stats["warnings"] += warnings
            if _upsert_lead(conn, vault, wf, batch_id, parsed):
                stats["merged"] += 1
    stats["reasons"] = dict(stats["reasons"])
    return stats


# --------------------------------------------------------------------- events

def _parse_event(line: str, vault: Vault):
    """Returns the ledger row values (without file_id/line_no) or None if malformed."""
    try:
        e = json.loads(line)
    except ValueError:
        return None
    if not isinstance(e, dict) or e.get("event_id") in (None, ""):
        return None
    ts = parse_timestamp_us(e.get("occurred_at"))
    if ts is None:
        return None
    payload = e.get("payload") if isinstance(e.get("payload"), dict) else {}
    etype = str(e.get("event_type") or "")
    code = payload.get("disposition_code") if etype.startswith("disposition.") else None
    raw_phone = e.get("phone")
    has_phone = raw_phone not in (None, "")
    canonical = normalize_phone(str(raw_phone)) if has_phone else None
    return (str(e["event_id"]), str(e.get("workflow_id") or ""), etype,
            str(e.get("call_id") or ""), ts, int(has_phone),
            vault.hash_phone(canonical) if canonical else None,
            code if isinstance(code, str) else None,
            json.dumps(payload, sort_keys=True, ensure_ascii=False))


def _ingest_event_lines(conn, vault, path: Path, file_id: int) -> dict:
    stats = {"total": 0, "malformed": 0, "duplicates": 0}
    batch: list[tuple] = []

    def flush():
        before = conn.total_changes
        conn.executemany("INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?)", batch)
        stats["duplicates"] += len(batch) - (conn.total_changes - before)
        batch.clear()

    with open(path, encoding="utf-8", errors="replace") as fh:
        for line_no, line in enumerate(fh, 1):
            if not line.strip():
                continue
            stats["total"] += 1
            parsed = _parse_event(line, vault)
            if parsed is None:
                stats["malformed"] += 1
                continue
            batch.append((*parsed, file_id, line_no))
            if len(batch) >= EVENT_BATCH:
                flush()
    flush()
    return stats


# ---------------------------------------------------------------------- drops

def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _ingest_file(conn: sqlite3.Connection, drop: str, kind: str, relpath: str, path: Path,
                 workflow_id: str | None, batch_id: str | None, run) -> str:
    sha = _sha256(path)
    existing = conn.execute("SELECT sha256 FROM files WHERE drop_name=? AND kind=? AND relpath=?",
                            (drop, kind, relpath)).fetchone()
    if existing:
        if existing[0] != sha:
            raise IngestError(f"{drop}/{relpath} was ingested before with different content")
        return "skipped"
    conn.execute("BEGIN")
    try:
        file_id = conn.execute(
            "INSERT INTO files (drop_name, kind, relpath, sha256, workflow_id, batch_id, stats_json)"
            " VALUES (?,?,?,?,?,?,'{}')", (drop, kind, relpath, sha, workflow_id, batch_id)).lastrowid
        stats = run(file_id)
        conn.execute("UPDATE files SET stats_json=? WHERE file_id=?", (json.dumps(stats), file_id))
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return "ingested"


def ingest_drop(conn, vault: Vault, workflows: dict[str, Workflow], drop_dir: Path,
                log=print) -> None:
    drop_dir = drop_dir.resolve()
    if not drop_dir.is_dir():
        raise IngestError(f"{drop_dir} is not a directory")
    drop = drop_dir.name
    for wf_dir in sorted(p for p in (drop_dir / "leads").glob("*") if p.is_dir()):
        wf = workflows.get(wf_dir.name)
        if wf is None:
            raise IngestError(f"no schema for workflow '{wf_dir.name}' ({wf_dir})")
        for path in sorted(wf_dir.glob("*.csv")):
            outcome = _ingest_file(
                conn, drop, "leads", f"{wf_dir.name}/{path.name}", path, wf.workflow_id, path.stem,
                lambda fid, wf=wf, path=path: _ingest_lead_rows(conn, vault, wf, path.stem, path, fid))
            log(f"{outcome:9} leads  {drop}/{wf_dir.name}/{path.name}")
    for path in sorted((drop_dir / "events").glob("*.jsonl")):
        outcome = _ingest_file(conn, drop, "events", path.name, path, None, None,
                               lambda fid, path=path: _ingest_event_lines(conn, vault, path, fid))
        log(f"{outcome:9} events {drop}/{path.name}")
