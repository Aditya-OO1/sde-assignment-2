"""Loads lead schemas and the disposition catalogue."""
from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path

from .normalize import normalize_header

DEFAULT_SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "assignment-a-data" / "schemas"


@dataclass(frozen=True)
class Field:
    name: str
    type: str
    required: bool = False
    pii: bool = False
    reference: bool = False
    values: tuple[str, ...] = ()
    default: str | None = None
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True)
class Workflow:
    workflow_id: str
    fields: tuple[Field, ...]
    dispositions: dict[str, tuple[bool, bool]]  # code -> (is_terminal, is_success)

    @property
    def header_lookup(self) -> dict[str, Field]:
        lookup: dict[str, Field] = {}
        for f in self.fields:
            for label in (f.name, *f.aliases):
                lookup.setdefault(normalize_header(label), f)
        return lookup

    @property
    def phone_field(self) -> Field:
        return next(f for f in self.fields if f.type == "phone")

    @property
    def reference_field(self) -> Field:
        return next(f for f in self.fields if f.reference)


def load_workflows(schemas_dir: Path = DEFAULT_SCHEMAS_DIR) -> dict[str, Workflow]:
    catalogue: dict[str, dict[str, tuple[bool, bool]]] = {}
    with open(schemas_dir / "dispositions.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            catalogue.setdefault(row["workflow_id"], {})[row["code"]] = (
                row["is_terminal"].strip().lower() == "true",
                row["is_success"].strip().lower() == "true",
            )
    workflows = {}
    for path in sorted(schemas_dir.glob("*.json")):
        spec = json.loads(path.read_text(encoding="utf-8"))
        fields = tuple(
            Field(
                name=d["name"], type=d["type"], required=d.get("required", False),
                pii=d.get("pii", False), reference=d.get("reference", False),
                values=tuple(d.get("values", ())), default=d.get("default"),
                aliases=tuple(d.get("aliases", ())),
            )
            for d in spec["fields"]
        )
        wid = spec["workflow_id"]
        workflows[wid] = Workflow(wid, fields, catalogue.get(wid, {}))
    return workflows
