"""Type inference, missing-value profiling, and reusable distribution snapshots."""

from __future__ import annotations

import csv
import hashlib
import random
from pathlib import Path

MISSING = {"", "null", "none", "na", "n/a"}
SAMPLE_LIMIT = 2048
CATEGORY_LIMIT = 50
OTHER_CATEGORY = "__other__"


def read_csv(path: str | Path) -> list[dict[str, str]]:
    with Path(path).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def input_identity(path: str | Path) -> str:
    """Stable identity of a profiling input: sha256 over the raw bytes.

    Retrying the same batch (same file content) yields the same identity, which
    the baseline store uses to deduplicate repeated submissions.
    """
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(65536), b""):
            digest.update(block)
    return f"sha256:{digest.hexdigest()}"


def _kind(values: list[str]) -> str:
    present = [value for value in values if value.strip().lower() not in MISSING]
    if not present:
        return "unknown"
    try:
        [float(value) for value in present]
        return "number"
    except ValueError:
        return "string"


def _numeric_sample(values: list[float], limit: int, seed: int) -> list[float]:
    """Deterministic bounded sample; sorted so profiles are order-stable."""
    if len(values) <= limit:
        return sorted(values)
    return sorted(random.Random(seed).sample(values, limit))


def _categories(values: list[str], limit: int) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    if len(ranked) <= limit:
        return dict(ranked)
    kept = dict(ranked[:limit])
    kept[OTHER_CATEGORY] = sum(count for _, count in ranked[limit:])
    return kept


def profile_rows(
    rows: list[dict[str, str]],
    sample_limit: int = SAMPLE_LIMIT,
    category_limit: int = CATEGORY_LIMIT,
    seed: int = 7,
) -> dict:
    """Build a profile snapshot.

    Besides row/missing counts, each field carries a bounded numeric sample or
    categorical counts so later versions can be compared reproducibly (PSI bin
    edges are derived from the baseline snapshot, never from raw data).
    """
    columns = sorted({key for row in rows for key in row})
    fields = {}
    for column in columns:
        raw = [str(row.get(column, "")) for row in rows]
        present = [value for value in raw if value.strip().lower() not in MISSING]
        missing = len(raw) - len(present)
        kind = _kind(raw)
        field = {
            "type": kind,
            "count": len(raw),
            "missing": missing,
            "missing_rate": missing / len(raw) if raw else 0.0,
        }
        if kind == "number":
            field["values"] = _numeric_sample([float(v) for v in present], sample_limit, seed)
        elif kind == "string":
            field["categories"] = _categories([v.strip() for v in present], category_limit)
        fields[column] = field
    return {"row_count": len(rows), "fields": fields}
