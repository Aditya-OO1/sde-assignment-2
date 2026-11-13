"""CLI:  python -m solution ingest <drop_dir>...   |   python -m solution report --out <dir>"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from . import db, ingest, report
from .pii import Vault
from .schema import DEFAULT_SCHEMAS_DIR, load_workflows

DEFAULT_STATE = Path(__file__).resolve().parent / ".state"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m solution")
    ap.add_argument("--db", type=Path, default=Path(os.environ.get("LEDGER_DB", DEFAULT_STATE / "ledger.db")))
    ap.add_argument("--schemas", type=Path, default=DEFAULT_SCHEMAS_DIR)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("ingest", help="ingest one or more drop directories, in the order given")
    p.add_argument("drops", nargs="+", type=Path)
    p = sub.add_parser("report", help="write the five report files")
    p.add_argument("--out", type=Path, required=True)
    args = ap.parse_args(argv)

    workflows = load_workflows(args.schemas)
    conn = db.connect(args.db)
    try:
        if args.cmd == "ingest":
            vault = Vault.load(args.db.with_name("pii.key"))
            for drop in args.drops:
                ingest.ingest_drop(conn, vault, workflows, drop)
        else:
            report.build_reports(conn, workflows, args.out)
            print(f"reports written to {args.out}")
    except ingest.IngestError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
