"""Derives the five reports from the ledger. Linking, orphan status and every derived fact are
computed here from everything ingested so far, so late-arriving leads fix earlier orphans."""
from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from .normalize import ist_datetime
from .schema import Workflow

BUCKETS = ("yet_to_reach_out", "in_progress_telephony_failed", "in_progress_dialed", "completed")


@dataclass
class Call:
    lead_id: int
    initiated_us: int | None = None
    connected: bool = False
    telephony_failed: bool = False
    sets: list = field(default_factory=list)       # (occurred_at_us, event_id, code)
    overrides: list = field(default_factory=list)

    @property
    def disposition(self) -> str | None:
        """Latest override, else latest set. Ties on time break by event_id."""
        pool = self.overrides or self.sets
        return max(pool)[2] if pool else None


def lead_bucket(calls: list[Call], terminal: set[str]) -> str:
    if not calls:
        return "yet_to_reach_out"
    if any(c.disposition in terminal for c in calls):
        return "completed"
    if all(c.telephony_failed for c in calls):
        return "in_progress_telephony_failed"
    return "in_progress_dialed"


def _write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def _bool(v: bool) -> str:
    return "true" if v else "false"


def build_reports(conn: sqlite3.Connection, workflows: dict[str, Workflow], out: Path) -> dict:
    out.mkdir(parents=True, exist_ok=True)

    leads = {}   # lead_id -> (workflow_id, external_ref, batch_id, last4)
    by_phone = {}
    for lid, wid, ph, last4, ref, batch in conn.execute(
            "SELECT lead_id, workflow_id, phone_hash, phone_last4, external_ref, batch_id FROM leads"):
        leads[lid] = (wid, ref, batch, last4)
        by_phone[(wid, ph)] = lid

    rows = conn.execute("SELECT event_id, workflow_id, event_type, call_id, occurred_at_us,"
                        " has_phone, phone_hash, disposition_code FROM events").fetchall()

    # Pass 1: which lead owns each call, from events that carry a phone.
    call_lead: dict[tuple[str, str], int] = {}
    for _, wid, _, call_id, _, _, ph, _ in rows:
        lid = by_phone.get((wid, ph)) if ph else None
        if lid is not None and call_id:
            key = (wid, call_id)
            if key not in call_lead or lid < call_lead[key]:
                call_lead[key] = lid

    # Pass 2: link every event (phone first; phone-less events fall back to their call).
    orphans = unknown = 0
    calls: dict[tuple[str, str], Call] = {}
    for eid, wid, etype, call_id, ts, has_phone, ph, code in rows:
        if has_phone:
            lid = by_phone.get((wid, ph)) if ph else None
        else:
            lid = call_lead.get((wid, call_id)) if call_id else None
        if lid is None:
            orphans += 1
            continue
        if etype.startswith("disposition.") and code not in workflows[wid].dispositions:
            unknown += 1
            continue
        if not call_id:
            continue
        call = calls.setdefault((wid, call_id), Call(lid))
        if etype == "call.initiated":
            call.initiated_us = ts if call.initiated_us is None else min(call.initiated_us, ts)
        elif etype == "call.connected":
            call.connected = True
        elif etype == "call.telephony_failed":
            call.telephony_failed = True
        elif etype == "disposition.set":
            call.sets.append((ts, eid, code))
        elif etype == "disposition.override":
            call.overrides.append((ts, eid, code))

    calls_by_lead: dict[int, list[tuple[str, Call]]] = defaultdict(list)
    for (wid, call_id), call in calls.items():
        if call.initiated_us is not None:  # a call exists only if its call.initiated is present
            calls_by_lead[call.lead_id].append((call_id, call))

    def flags(wid: str, call: Call) -> tuple[bool, bool]:
        d = workflows[wid].dispositions.get(call.disposition)
        return (d[0], d[1]) if d else (False, False)

    batch_rows: dict[tuple[str, str], Counter] = defaultdict(Counter)
    daily: dict[tuple[str, str], dict] = defaultdict(
        lambda: {"total": 0, "connected": 0, "success": set()})
    lead_rows = []
    for lid, (wid, ref, batch, last4) in leads.items():
        wf = workflows[wid]
        terminal = {c for c, (t, _) in wf.dispositions.items() if t}
        lead_calls = sorted(calls_by_lead.get(lid, ()), key=lambda x: (x[1].initiated_us, x[0]))
        plain = [c for _, c in lead_calls]
        bucket = lead_bucket(plain, terminal)
        counts = batch_rows[(wid, batch)]
        counts["total"] += 1
        counts[bucket] += 1
        for c in plain:
            d = daily[(wid, ist_datetime(c.initiated_us).date().isoformat())]
            d["total"] += 1
            d["connected"] += c.connected
            if flags(wid, c)[1]:
                d["success"].add(lid)
        with_disp = [c for c in plain if c.disposition is not None]
        lead_rows.append([
            wid, ref, batch, "+91******" + last4, bucket, len(plain),
            sum(c.connected for c in plain),
            ist_datetime(plain[-1].initiated_us).strftime("%Y-%m-%dT%H:%M:%S+05:30") if plain else "",
            with_disp[-1].disposition if with_disp else "",
            _bool(any(flags(wid, c)[1] for c in plain)), lid])

    _write_csv(out / "batch_summary.csv",
               ["workflow_id", "batch_id", "total_leads", *BUCKETS],
               [[w, b, c["total"], *(c[k] for k in BUCKETS)]
                for (w, b), c in sorted(batch_rows.items())])
    _write_csv(out / "daily_overview.csv",
               ["workflow_id", "date", "total_calls", "connected_calls", "connectivity_rate",
                "success_leads"],
               [[w, d, v["total"], v["connected"], f"{v['connected'] / v['total']:.4f}",
                 len(v["success"])] for (w, d), v in sorted(daily.items())])
    lead_rows.sort(key=lambda r: (r[0], r[1], r[-1]))
    _write_csv(out / "lead_report.csv",
               ["workflow_id", "external_ref", "batch_id", "phone_masked", "bucket", "total_calls",
                "connected_calls", "last_call_at", "current_disposition", "ever_success"],
               [r[:-1] for r in lead_rows])

    rejections = conn.execute(
        "SELECT workflow_id, batch_id, row_number, reason FROM rejections"
        " ORDER BY workflow_id, batch_id, row_number").fetchall()
    _write_csv(out / "rejections.csv", ["workflow_id", "batch_id", "row_number", "reason"],
               [list(r) for r in rejections])

    totals: Counter = Counter()
    reasons: Counter = Counter()
    for kind, stats_json in conn.execute("SELECT kind, stats_json FROM files"):
        s = json.loads(stats_json)
        if kind == "leads":
            totals.update(rows=s["rows"], rejected=s["rejected"], merged=s["merged"],
                          warnings=s["warnings"])
            reasons.update(s["reasons"])
        else:
            totals.update(total=s["total"], malformed=s["malformed"], duplicates=s["duplicates"])
    quality = {
        "lead_rows_total": totals["rows"],
        "lead_rows_rejected": totals["rejected"],
        "rejections_by_reason": dict(sorted(reasons.items())),
        "duplicate_lead_rows_merged": totals["merged"],
        "field_warnings": totals["warnings"],
        "events_total": totals["total"],
        "malformed_events": totals["malformed"],
        "duplicate_events_dropped": totals["duplicates"],
        "orphan_events": orphans,
        "unknown_disposition_events": unknown,
    }
    (out / "data_quality.json").write_text(json.dumps(quality, indent=2) + "\n", encoding="utf-8")
    return quality
