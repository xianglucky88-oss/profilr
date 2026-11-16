"""Type-aware field profiling.

Each column is profiled in one streaming pass into raw counts, a
type-specific summary and an explicit status, so missing semantics and
numeric/string summaries always line up in a single result.

Decisions (see README for the full version):

* Missing: "", "null", "none", "na", "n/a", "nan" (case/space
  insensitive) and ``None`` (short rows) count as missing and do not
  participate in type inference.
* Type inference degrades deterministically: all present values finite
  numbers -> "number"; none -> "string"; mixed -> majority class wins
  (ties degrade to "string", the lossless choice) with
  ``status="degraded"`` and the minority counted in ``invalid``.
  Non-finite numbers ("1e309", "inf", ...) are invalid numbers: counted
  in ``invalid`` and excluded from numeric summaries.
* Numeric quantiles/histograms come from a deterministic reservoir
  sample once a field exceeds ``max_numeric_values``; min/max/mean/count
  stay exact and the summary is flagged ``approximate``.
* String summaries track at most ``max_distinct`` values; beyond the cap
  ``distinct`` is a lower bound and ``distinct_exact`` is False.
* With a ``reference`` profile, numeric histograms reuse the reference
  edges (out-of-range values land in underflow/overflow) so batches are
  bin-by-bin comparable, and column-set changes surface as field status
  "dropped" plus ``result["schema"]`` added/dropped/type_changed lists.
"""

from __future__ import annotations

import csv
import math
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .distribution import (
    DEFAULT_BINS,
    DEFAULT_QUANTILES,
    Reservoir,
    histogram,
    histogram_edges,
    quantile,
    quantile_label,
)

MISSING = {"", "null", "none", "na", "n/a", "nan"}

# Field statuses.
OK = "ok"                      # values conform to the inferred type
DEGRADED = "degraded"          # mixed/invalid values; summary covers conforming values only
ALL_MISSING = "all_missing"    # rows seen, but every value is missing
EMPTY = "empty"                # column known (header) but the file has no rows
DROPPED = "dropped"            # present in the reference profile, absent from this batch


@dataclass(frozen=True)
class ProfileConfig:
    bins: int = DEFAULT_BINS
    quantiles: tuple[float, ...] = DEFAULT_QUANTILES
    top_k: int = 10
    max_distinct: int = 1024
    max_numeric_values: int = 100_000
    seed: int = 7


DEFAULT_CONFIG = ProfileConfig()


def read_csv(path: str | Path) -> list[dict[str, str]]:
    with Path(path).open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def profile_csv(path: str | Path, *, reference: dict | None = None,
                config: ProfileConfig | None = None) -> dict:
    """Profile a CSV file, keeping the header even when the file is empty."""
    with Path(path).open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        columns = [name for name in (reader.fieldnames or []) if name is not None]
    return profile_rows(rows, columns=columns, reference=reference, config=config)


class FieldProfiler:
    """Streaming, memory-bounded accumulator for one column."""

    def __init__(self, name: str, config: ProfileConfig):
        self.name = name
        self.config = config
        self.total = 0
        self.missing = 0
        self.numeric = 0        # present values that parse as finite floats
        self.other = 0          # present values that do not (incl. inf/nan overflow)
        self._sum = 0.0
        self._min: float | None = None
        self._max: float | None = None
        seed = config.seed + zlib.crc32(name.encode("utf-8"))  # stable across processes
        self._reservoir = Reservoir(config.max_numeric_values, seed=seed)
        self._strings: dict[str, int] = {}
        self._strings_capped = False

    def add(self, raw: object) -> None:
        self.total += 1
        if raw is None:
            self.missing += 1
            return
        text = str(raw).strip()
        if text.lower() in MISSING:
            self.missing += 1
            return
        tracked = self._strings.get(text)
        if tracked is not None:
            self._strings[text] = tracked + 1
        elif len(self._strings) < self.config.max_distinct:
            self._strings[text] = 1
        else:
            self._strings_capped = True
        try:
            value = float(text)
        except ValueError:
            self.other += 1
            return
        if not math.isfinite(value):
            self.other += 1  # 非法数字: "1e309"、"inf" 等溢出/非有限值
            return
        self.numeric += 1
        self._sum += value
        self._min = value if self._min is None else min(self._min, value)
        self._max = value if self._max is None else max(self._max, value)
        self._reservoir.add(value)

    @property
    def present(self) -> int:
        return self.numeric + self.other

    def _infer_type(self) -> tuple[str, str]:
        if self.present == 0:
            return "unknown", EMPTY if self.total == 0 else ALL_MISSING
        if self.other == 0:
            return "number", OK
        if self.numeric == 0:
            return "string", OK
        # Mixed column: degrade to the majority class; ties go to string.
        if self.numeric > self.other:
            return "number", DEGRADED
        return "string", DEGRADED

    def finish(self, reference_field: dict | None = None) -> dict:
        ftype, status = self._infer_type()
        invalid = self.other if ftype == "number" else 0
        summary = None
        if ftype == "number":
            summary = self._numeric_summary(reference_field)
        elif ftype == "string":
            summary = self._string_summary()
        return {
            "type": ftype,
            "status": status,
            "total": self.total,
            "missing": self.missing,
            "present": self.present,
            "invalid": invalid,
            "missing_rate": self.missing / self.total if self.total else None,
            "summary": summary,
        }

    def _numeric_summary(self, reference_field: dict | None) -> dict:
        kept = self._reservoir.values
        ref_summary = (reference_field or {}).get("summary") or {}
        ref_histogram = ref_summary.get("histogram") or {}
        if ref_summary.get("kind") == "numeric" and ref_histogram.get("edges"):
            edges = list(ref_histogram["edges"])
            edges_source = "baseline"
        else:
            edges = histogram_edges(self._min, self._max, self.config.bins)
            edges_source = "data"
        hist = histogram(kept, edges)
        hist["edges_source"] = edges_source
        return {
            "kind": "numeric",
            "count": self.numeric,
            "min": self._min,
            "max": self._max,
            "mean": self._sum / self.numeric,
            "quantiles": {quantile_label(q): quantile(kept, q) for q in self.config.quantiles},
            "histogram": hist,
            "approximate": self._reservoir.approximate,
            "sample_count": len(kept),
        }

    def _string_summary(self) -> dict:
        ranked = sorted(self._strings.items(), key=lambda item: (-item[1], item[0]))
        top = [{"value": value, "count": count} for value, count in ranked[: self.config.top_k]]
        return {
            "kind": "string",
            "count": self.present,
            "distinct": len(self._strings),
            "distinct_exact": not self._strings_capped,
            "numeric_like": self.numeric,
            "top": top,
        }


def _dropped_field(reference_field: dict) -> dict:
    return {
        "type": reference_field.get("type", "unknown"),
        "status": DROPPED,
        "total": 0,
        "missing": 0,
        "present": 0,
        "invalid": 0,
        "missing_rate": None,
        "summary": None,
    }


def profile_rows(rows: Iterable[dict], *, columns: list[str] | None = None,
                 reference: dict | None = None,
                 config: ProfileConfig | None = None) -> dict:
    """Profile ``rows`` into raw counts + type-aware summaries + statuses.

    ``columns`` pins the expected column set (e.g. a CSV header, so empty
    files still report their fields); ``reference`` is a previous profile
    used to align histogram edges and to flag column-set changes.
    """
    config = config or DEFAULT_CONFIG
    rows = list(rows)
    seen = {key for row in rows for key in row if key is not None}
    current_columns = sorted(set(columns or ()) | seen)

    profilers = {name: FieldProfiler(name, config) for name in current_columns}
    for row in rows:
        for name, profiler in profilers.items():
            profiler.add(row.get(name))

    reference_fields = (reference or {}).get("fields", {})
    fields = {name: profilers[name].finish(reference_fields.get(name)) for name in current_columns}

    schema = None
    if reference is not None:
        dropped = sorted(set(reference_fields) - set(current_columns))
        added = sorted(set(current_columns) - set(reference_fields))
        for name in dropped:
            fields[name] = _dropped_field(reference_fields[name])
        type_changed = []
        for name in sorted(set(current_columns) & set(reference_fields)):
            before = reference_fields[name].get("type")
            after = fields[name]["type"]
            if before and after and before != "unknown" and after != "unknown" and before != after:
                type_changed.append({"field": name, "from": before, "to": after})
        schema = {"added": added, "dropped": dropped, "type_changed": type_changed}

    return {
        "row_count": len(rows),
        "status": EMPTY if not rows else OK,
        "columns": current_columns,
        "schema": schema,
        "fields": fields,
    }
