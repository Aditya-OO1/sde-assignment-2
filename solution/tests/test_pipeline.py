import csv
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from solution import db, ingest, report
from solution.pii import Vault
from solution.report import Call, lead_bucket
from solution.schema import load_workflows

ROOT = Path(__file__).resolve().parents[2]
WF = load_workflows()
IST_Z = "2026-10-05T10:00:00Z"


# ---------------------------------------------------------------- unit: derived facts

def call(**kw):
    return Call(lead_id=1, initiated_us=1, **kw)


def test_override_beats_later_set():
    c = call(sets=[(10, "e2", "renewed")], overrides=[(5, "e1", "not_interested")])
    assert c.disposition == "not_interested"


def test_latest_override_wins_and_set_is_fallback():
    c = call(sets=[(1, "e1", "callback_requested"), (9, "e2", "renewed")],
             overrides=[(3, "e3", "dnd"), (4, "e4", "wrong_number")])
    assert c.disposition == "wrong_number"
    assert call(sets=[(1, "a", "x"), (9, "b", "y")]).disposition == "y"
    assert call().disposition is None


TERMINAL = {"renewed", "dnd"}


def test_buckets():
    assert lead_bucket([], TERMINAL) == "yet_to_reach_out"
    done = call(sets=[(1, "e", "renewed")], telephony_failed=True)
    assert lead_bucket([done, call()], TERMINAL) == "completed"
    assert lead_bucket([call(telephony_failed=True), call(telephony_failed=True)],
                       TERMINAL) == "in_progress_telephony_failed"
    assert lead_bucket([call(telephony_failed=True), call()], TERMINAL) == "in_progress_dialed"
    assert lead_bucket([call(sets=[(1, "e", "callback_requested")])], TERMINAL) == "in_progress_dialed"


# ------------------------------------------------------------ integration: hand-built drops

HEADER = "customer_name,phone,policy_number,policy_expiry,premium_amount,preferred_language\n"


def write_drop(root: Path, name: str, leads: dict[str, str] | None = None, events=()):
    d = root / name
    if leads:
        for batch, body in leads.items():
            p = d / "leads" / "motor_renewal" / f"{batch}.csv"
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(body)
    if events:
        p = d / "events" / "events_0001.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("\n".join(e if isinstance(e, str) else json.dumps(e) for e in events) + "\n")
    return d


def ev(eid, etype, call_id="c1", phone="9876543210", ts=IST_Z, **payload):
    e = {"event_id": eid, "event_type": etype, "workflow_id": "motor_renewal", "call_id": call_id,
         "occurred_at": ts, "payload": payload}
    if phone:
        e["phone"] = phone
    return e


@pytest.fixture
def env(tmp_path):
    conn = db.connect(tmp_path / "t.db")
    vault = Vault(Fernet.generate_key())
    return conn, vault, tmp_path


def run(env, *drops):
    conn, vault, tmp = env
    for d in drops:
        ingest.ingest_drop(conn, vault, WF, d, log=lambda *_: None)
    out = tmp / "out"
    quality = report.build_reports(conn, WF, out)
    return quality, out


def read(out, name):
    with open(out / name, newline="") as f:
        return list(csv.DictReader(f))


def test_rejection_order_merge_and_defaults(env):
    _, _, tmp = env
    leads = (HEADER
             + "Asha,98765 43210,P1,01/12/2026,\"₹1,000\",HI\n"
             + ",,,,,\n"                                  # blank: not counted
             + ",9000000000,P2,,,\n"                      # name missing (first in schema order)
             + "Bob,12345,P3,,,\n"                        # invalid phone
             + "Cara,9111111111,P4,31/02/2026,N/A,Klingon\n"   # 3 warnings
             + "Asha B,+919876543210,P1x,,,\n")           # duplicate: overrides name/ref only
    quality, out = run(env, write_drop(tmp, "drop_1", {"batch_001": leads}))
    assert quality["lead_rows_total"] == 5
    assert quality["rejections_by_reason"] == {"invalid_phone": 1, "missing_required:customer_name": 1}
    assert quality["duplicate_lead_rows_merged"] == 1
    assert quality["field_warnings"] == 3
    assert [r["row_number"] for r in read(out, "rejections.csv")] == ["2", "3"]
    refs = {r["external_ref"]: r for r in read(out, "lead_report.csv")}
    assert set(refs) == {"P1x", "P4"}
    conn = env[0]
    attrs = json.loads(conn.execute("SELECT attrs_json FROM leads WHERE external_ref='P1x'").fetchone()[0])
    assert attrs == {"policy_expiry": "2026-12-01", "premium_amount": "1000.00", "preferred_language": "hi"}


def test_renamed_columns_in_later_batch(env):
    _, _, tmp = env
    b1 = HEADER + "Asha,9876543210,P1,,,\n"
    b3 = "Language,Insured Name,Policy No,Mobile No,agent_notes\nen,Zed,P9,9222222222,VIP\n"
    _, out = run(env, write_drop(tmp, "drop_1", {"batch_001": b1}),
                 write_drop(tmp, "drop_2", {"batch_003": b3}))
    assert {r["external_ref"]: r["batch_id"] for r in read(out, "lead_report.csv")} == {
        "P1": "batch_001", "P9": "batch_003"}


def test_orphan_becomes_linked_when_lead_arrives_later(env):
    _, _, tmp = env
    events = [ev("e1", "call.initiated"), ev("e2", "call.connected"),
              ev("e3", "disposition.set", disposition_code="renewed"),
              ev("e4", "disposition.override", phone=None, disposition_code="not_interested")]
    d1 = write_drop(tmp, "drop_1", events=events)
    q1, out1 = run(env, d1)
    assert q1["orphan_events"] == 4          # lead unknown: even the phone-less override
    d2 = write_drop(tmp, "drop_2", {"batch_003": HEADER + "Asha,9876543210,P1,,,\n"})
    q2, out2 = run(env, d2)
    assert q2["orphan_events"] == 0
    row = read(out2, "lead_report.csv")[0]
    assert (row["bucket"], row["total_calls"], row["connected_calls"]) == ("completed", "1", "1")
    assert row["current_disposition"] == "not_interested" and row["ever_success"] == "false"


def test_event_pipeline_counters(env):
    _, _, tmp = env
    leads = write_drop(tmp, "drop_1", {"batch_001": HEADER + "Asha,9876543210,P1,,,\n"},
                       [ev("e1", "call.initiated"),
                        ev("e1", "call.initiated"),                       # duplicate
                        "{not json", json.dumps({"call_id": "c"}),        # malformed, no event_id
                        ev("e2", "disposition.set", disposition_code="RENEWD"),  # unknown
                        ev("e3", "call.initiated", call_id="c9", phone="9000000001"),  # orphan
                        ev("e4", "disposition.set", call_id="c9", phone="9000000001",
                           disposition_code="nonsense")])  # orphan wins over unknown
    q, out = run(env, leads)
    assert (q["events_total"], q["malformed_events"], q["duplicate_events_dropped"],
            q["orphan_events"], q["unknown_disposition_events"]) == (7, 2, 1, 2, 1)
    row = read(out, "lead_report.csv")[0]
    assert row["total_calls"] == "1" and row["current_disposition"] == ""


def test_call_without_initiated_is_not_counted(env):
    _, _, tmp = env
    d = write_drop(tmp, "drop_1", {"batch_001": HEADER + "Asha,9876543210,P1,,,\n"},
                   [ev("e1", "call.connected")])
    _, out = run(env, d)
    assert read(out, "lead_report.csv")[0]["bucket"] == "yet_to_reach_out"
    assert read(out, "daily_overview.csv") == []


def test_daily_uses_ist_date_and_success_distinct_leads(env):
    _, _, tmp = env
    late_utc = "2026-10-05T20:00:00Z"   # 01:30 IST on the 6th
    events = [ev("e1", "call.initiated", "c1", ts=late_utc), ev("e2", "call.connected", "c1"),
              ev("e3", "disposition.set", "c1", disposition_code="renewed"),
              ev("e4", "call.initiated", "c2", ts=late_utc),
              ev("e5", "disposition.set", "c2", disposition_code="renewed")]
    _, out = run(env, write_drop(tmp, "drop_1", {"batch_001": HEADER + "A,9876543210,P1,,,\n"}, events))
    assert read(out, "daily_overview.csv") == [{
        "workflow_id": "motor_renewal", "date": "2026-10-06", "total_calls": "2",
        "connected_calls": "1", "connectivity_rate": "0.5000", "success_leads": "1"}]


def test_reingest_is_noop_and_ledger_is_append_only(env):
    _, _, tmp = env
    d = write_drop(tmp, "drop_1", {"batch_001": HEADER + "Asha,9876543210,P1,,,\n"},
                   [ev("e1", "call.initiated")])
    q1, out1 = run(env, d)
    snap = {n: (out1 / n).read_text() for n in ("lead_report.csv", "data_quality.json")}
    q2, out2 = run(env, d)
    assert q1 == q2 and snap == {n: (out2 / n).read_text() for n in snap}
    with pytest.raises(sqlite3.DatabaseError):
        env[0].execute("UPDATE events SET call_id='x'")
    with pytest.raises(sqlite3.DatabaseError):
        env[0].execute("DELETE FROM events")


def test_changed_file_under_same_name_is_refused(env):
    _, _, tmp = env
    d = write_drop(tmp, "drop_1", events=[ev("e1", "call.initiated")])
    run(env, d)
    write_drop(tmp, "drop_1", events=[ev("e2", "call.initiated")])
    with pytest.raises(ingest.IngestError):
        run(env, d)


def test_no_plaintext_pii_in_database(env):
    conn, _, tmp = env
    d = write_drop(tmp, "drop_1", {"batch_001": HEADER + "Zebediah Quux,9876543210,P1,,,\n"},
                   [ev("e1", "call.initiated", phone="98765 43210")])
    run(env, d)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    raw = (tmp / "t.db").read_bytes()
    for needle in (b"Zebediah", b"Quux", b"9876543210", b"98765 43210"):
        assert needle not in raw


# ---------------------------------------------------- end to end on generated data

@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    data = tmp_path_factory.mktemp("gen")
    subprocess.run([sys.executable, str(ROOT / "assignment-a-data/tools/generate_data.py"),
                    "--seed", "7", "--scale", "small", "--out", str(data)],
                   check=True, capture_output=True)
    return data


def cli(tmp, *args):
    subprocess.run([sys.executable, "-m", "solution", "--db", str(tmp / "s.db"), *args],
                   check=True, cwd=ROOT, capture_output=True)


def reports_of(out):
    return {p.name: p.read_bytes() for p in sorted(out.iterdir())}


def test_incremental_equals_all_at_once_and_rerun_is_noop(generated, tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    a.mkdir(), b.mkdir()
    cli(a, "ingest", str(generated / "drop_1"))
    cli(a, "ingest", str(generated / "drop_2"))
    cli(a, "ingest", str(generated / "drop_1"))      # re-run an old drop
    cli(a, "report", "--out", str(a / "out"))
    cli(b, "ingest", str(generated / "drop_1"), str(generated / "drop_2"))
    cli(b, "report", "--out", str(b / "out"))
    assert reports_of(a / "out") == reports_of(b / "out")
    assert len(reports_of(a / "out")) == 5
