"""Basic type inference and missing-value profiling."""

from __future__ import annotations

import csv
from pathlib import Path

MISSING = {"", "null", "none", "na", "n/a"}


def read_csv(path: str | Path) -> list[dict[str, str]]:
    with Path(path).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _kind(values: list[str]) -> str:
    present = [value for value in values if value.strip().lower() not in MISSING]
    if not present:
        return "unknown"
    try:
        [float(value) for value in present]
        return "number"
    except ValueError:
        return "string"


def profile_rows(rows: list[dict[str, str]]) -> dict:
    columns = sorted({key for row in rows for key in row})
    fields = {}
    for column in columns:
        values = [str(row.get(column, "")) for row in rows]
        missing = sum(value.strip().lower() in MISSING for value in values)
        fields[column] = {
            "type": _kind(values),
            "missing": missing,
            "missing_rate": missing / len(values) if values else 0.0,
        }
    return {"row_count": len(rows), "fields": fields}
