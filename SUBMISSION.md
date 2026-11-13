# Submission

## Candidate

- **Name:** Aditya Aggrawal
- **Assignment chosen:** A (Data)

## How to run

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data

python -m solution ingest data/drop_1          # drops are processed in the order given
python -m solution ingest data/drop_2
python -m solution report --out reports/
pytest                                          # 48 tests, ~1s
```

State lives in `solution/.state/` (SQLite DB + PII key file, git-ignored). Override with
`--db PATH` / `LEDGER_DB`; supply the PII key via `LEDGER_PII_KEY` instead of the key file.

## Design

SQLite, five tables (`solution/db.py`):

- `files`: one row per ingested file with its SHA-256 and per-file data-quality counters. A file
is ingested in **one transaction** together with its `files` row, so re-ingesting a drop skips
every file already there (idempotent), a crash leaves nothing half-done, and a file re-sent
under the same name with different content is refused loudly.
- `leads`: unique on `(workflow_id, phone_hash)`. `batch_id` is set once at creation.
- `rejections`: position and reason only, no PII.
- `events`: the **append-only ledger**, `event_id` primary key, `UPDATE`/`DELETE` blocked by
triggers. Overrides are just more rows; nothing is edited.

**Link, orphan and every derived fact are computed at report time**, not at ingest. The ledger
holds the facts (event + hashed phone); `report` joins them to whatever leads exist now. That is
why an event that was an orphan after `drop_1` is counted once its lead arrives in `drop_2`, why
orphan counts reflect the final state, and why incremental == all-at-once without any
reprocessing or fix-up logic. Cost: report re-reads the whole ledger (2s for ~600k events).

**PII:** phone is stored as `HMAC-SHA256(key, +91XXXXXXXXXX)`, which supports equality matching
of events to leads but cannot be reversed or brute-forced without the key (a plain hash of a
10-digit number is trivially brute-forced). Last 4 digits are kept in the clear because the
report only ever shows a masked number. Other `pii: true` fields (names) are Fernet-encrypted in
one blob per lead. Raw event phones are never stored. A test greps the raw DB file for plaintext.

Layout: `normalize.py` (pure parsers), `schema.py`, `pii.py`, `ingest.py`, `report.py`,
`__main__.py`.

## Decisions & assumptions


| Decision                                                                                                                                           | Why                                                                                              | Alternative considered                            |
| -------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ | ------------------------------------------------- |
| Linking/orphans/unknown-disposition evaluated at report time                                                                                       | Gives "final state" semantics for free                                                           | Re-link on every ingest (writes, order-sensitive) |
| Orphan check runs before unknown-disposition check                                                                                                 | Spec's processing order (3 before 4)                                                             | Count both                                        |
| Events with **no phone** link via `call_id` to a lead found from other events of that call; an event with a phone present-but-invalid is an orphan | Spec describes this only for overrides; I generalise to "no phone", not to "bad phone"           | Override-only fallback                            |
| Call identity is `(workflow_id, call_id)`                                                                                                          | call ids could collide across workflows                                                          | `call_id` alone                                   |
| Event with unparseable `occurred_at` counts as malformed                                                                                           | Cannot be ordered or dated, spec silent                                                          | Keep with a null time                             |
| Event with no `call_id` can link to a lead but never forms a call                                                                                  | No way to group it                                                                               | Orphan                                            |
| Required non-phone field present but invalid → `invalid_value:<field>`                                                                             | Spec only names two reasons; no required date/enum exists in current schemas                     | Treat as missing                                  |
| Defaults applied at upsert time (after merging that row)                                                                                           | Equivalent to "after merging": a later non-null value always overwrites                          | Apply at report time                              |
| Batches with only rejected rows get no `batch_summary` row                                                                                         | They contain no leads                                                                            | Emit zero rows                                    |
| Ties: `lead_report` equal `external_ref` → insertion order; equal disposition timestamps → larger `event_id`                                       | Deterministic output                                                                             | n/a, spec silent                                  |
| `duplicate_events_dropped` is order-dependent only in *which* copy is kept, not the count                                                          | Count = lines − unique ids                                                                       | n/a                                               |
| Drops are processed in the order given on the command line                                                                                         | Spec: "in the order ingested". Ingesting drop_2 before drop_1 can change which batch owns a lead | Sort by directory name                            |
| `rejections_by_reason` is `{}` when there are none and only lists reasons seen                                                                     | Contract example shows illustrative keys                                                         | Fixed key set                                     |




## Failure modes considered

- **Dirty leads:** blank rows, renamed/reordered/extra columns, short rows, BOM, bad phones,
impossible dates, bad currency, bad enums (null + warning, default applied).
- **Dirty events:** non-JSON, non-object JSON, missing `event_id`, bad timestamps, duplicates in
the same file / across files / across drops, unknown event types (stored, ignored).
- **Re-runs / crashes:** per-file transaction + file hash; `kill -9` mid-file rolls back that file.
- **Not handled:** concurrent ingests (SQLite single-writer would serialize, but there is no
locking story across processes); a lead file corrected and re-sent under a new name is treated
as new rows (merged as duplicates); PII key rotation; the PII key file is plaintext on disk
(fine for the exercise, use a KMS in production); non-IST/odd timestamp edge cases like leap
seconds.



## Testing

48 pytest tests: parsers (phone formats, day-first dates, currency), effective-disposition and
bucket logic, rejection ordering, merge/defaults, renamed columns, orphan resolved by a later
drop, event counters, IST date bucketing, append-only triggers, no plaintext PII in the DB file,
re-ingest no-op, and an end-to-end generated-data test that (drop_1 → drop_2 → drop_1 again)
yields byte-identical reports to ingesting both in one command. Not tested: the large dataset in
CI (run manually below) and cross-process concurrency.

## Results

Seed 42, `--scale large` (143,990 lead rows, ~600k events), laptop, wall-clock:

- ingest drop_1 + drop_2: **18 s**
- report: **2 s**
- re-ingest of both drops: **1 s** (all files skipped)

Small seed 42 `data_quality.json`: 2057 lead rows, 108 rejected, 57 merged duplicates, 122 field
warnings, 8745 events, 8 malformed, 237 duplicates, 462 orphans, 18 unknown dispositions.

## If I had another day

1. Do the report aggregation in SQL (window functions) so memory doesn't scale with the ledger.
2. A production note for 100k leads/day: scheduled per-drop ingest, `files` table as the
  checkpoint, alerts on rejection/orphan-rate spikes, backfills by re-running `report`.
3. Move to Postgres, KMS-held key with rotation (key id stored per blob).
4. Cross-process locking and a `kill -9` fault-injection test.

