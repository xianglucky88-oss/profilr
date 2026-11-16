"""Cross-batch drift calculations.

Two kinds of findings are produced and kept strictly separate:

* schema events — fields added/removed or changing type between versions;
* data drift — per-field metric shifts (missing-rate delta, numeric PSI,
  categorical PSI) evaluated against a comparison spec (比较口径).

`compute_metrics` is pure and deterministic given two profile snapshots and a
spec, so a stored comparison can be re-evaluated later with new thresholds
without re-profiling the inputs.
"""

from __future__ import annotations

import math

from .distribution import quantile

#: Default comparison spec (比较口径). Every comparison records the spec it
#: used, so alerts can be re-interpreted when thresholds change.
DEFAULT_SPEC = {
    "psi_bins": 10,
    "psi_threshold": 0.25,
    "categorical_psi_threshold": 0.25,
    "missing_rate_threshold": 0.1,
    "alert_on_schema_events": True,
}

SCHEMA_EVENT_FIELDS = ("field_added", "field_removed", "type_changed")


def normalize_spec(spec: dict | None = None) -> dict:
    merged = {**DEFAULT_SPEC, **(spec or {})}
    merged["alert_on_schema_events"] = bool(merged["alert_on_schema_events"])
    return merged


def absolute_drift(baseline: dict, current: dict) -> list[dict]:
    """Initial implementation: compare missing rates by absolute difference."""
    alerts = []
    for field, before in baseline.get("fields", {}).items():
        after = current.get("fields", {}).get(field)
        if not after:
            continue
        delta = abs(after["missing_rate"] - before["missing_rate"])
        alerts.append({"field": field, "metric": "missing_rate", "delta": delta})
    return alerts


def psi(expected: list[float], actual: list[float], bins: int = 10) -> float:
    if not expected or not actual:
        return 0.0
    low, high = min(expected + actual), max(expected + actual)
    if low == high:
        return 0.0
    width = (high - low) / bins

    def proportions(values):
        counts = [0] * bins
        for value in values:
            counts[min(int((value - low) / width), bins - 1)] += 1
        total = len(values)
        return [(count + 0.5) / (total + 0.5 * bins) for count in counts]

    return sum((a - e) * math.log(a / e) for e, a in zip(proportions(expected), proportions(actual)))


def bin_edges(baseline_values: list[float], bins: int = 10) -> list[float]:
    """Interior cut points derived from the *baseline* distribution.

    Edges come only from the baseline snapshot, which keeps comparisons
    reproducible: the same pair of versions always bins identically.
    A constant baseline still yields edges so a move away from the constant
    is detected instead of collapsing into a single bin.
    """
    unique = sorted(set(baseline_values))
    if not unique:
        return []
    if len(unique) == 1:
        center = unique[0]
        return [center - 0.5, center + 0.5]
    edges = {quantile(baseline_values, i / bins) for i in range(1, bins)}
    return sorted(edges)


def _proportions(values: list[float], edges: list[float]) -> list[float]:
    counts = [0] * (len(edges) + 1)
    for value in values:
        index = 0
        while index < len(edges) and value >= edges[index]:
            index += 1
        counts[index] += 1
    total = len(values)
    buckets = len(counts)
    return [(count + 0.5) / (total + 0.5 * buckets) for count in counts]


def psi_with_edges(expected: list[float], actual: list[float], edges: list[float]) -> float | None:
    """PSI of two samples binned by fixed edges; None if not computable."""
    if not expected or not actual:
        return None
    expected_props, actual_props = _proportions(expected, edges), _proportions(actual, edges)
    return sum((a - e) * math.log(a / e) for e, a in zip(expected_props, actual_props))


def categorical_psi(baseline_counts: dict, current_counts: dict) -> float | None:
    """PSI over the union of categories; None if either side has no data."""
    keys = sorted(set(baseline_counts) | set(current_counts))
    base_total = sum(baseline_counts.values())
    current_total = sum(current_counts.values())
    if not keys or base_total == 0 or current_total == 0:
        return None
    buckets = len(keys)
    total = 0.0
    for key in keys:
        e = (baseline_counts.get(key, 0) + 0.5) / (base_total + 0.5 * buckets)
        a = (current_counts.get(key, 0) + 0.5) / (current_total + 0.5 * buckets)
        total += (a - e) * math.log(a / e)
    return total


def compute_metrics(baseline: dict, current: dict, spec: dict | None = None) -> dict:
    """Schema events + per-field metrics for two profile snapshots.

    Fields missing on either side become schema events instead of being
    silently skipped; metrics are only computed where they are meaningful.
    """
    spec = normalize_spec(spec)
    base_fields = baseline.get("fields", {})
    current_fields = current.get("fields", {})
    schema_events = []
    field_metrics = []
    for name in sorted(set(base_fields) | set(current_fields)):
        before = base_fields.get(name)
        after = current_fields.get(name)
        if before is None:
            schema_events.append({
                "field": name,
                "event": "field_added",
                "baseline_type": None,
                "current_type": after.get("type"),
                "detail": f"字段 {name!r} 在基线版本中不存在，当前版本新增（类型 {after.get('type')}）",
            })
            continue
        if after is None:
            schema_events.append({
                "field": name,
                "event": "field_removed",
                "baseline_type": before.get("type"),
                "current_type": None,
                "detail": f"字段 {name!r} 在基线版本中存在（类型 {before.get('type')}），当前版本已缺失",
            })
            continue
        entry = {
            "field": name,
            "baseline_type": before.get("type"),
            "current_type": after.get("type"),
            "metrics": {},
        }
        base_rate = before.get("missing_rate", 0.0)
        current_rate = after.get("missing_rate", 0.0)
        entry["metrics"]["missing_rate"] = {
            "baseline": base_rate,
            "current": current_rate,
            "delta": current_rate - base_rate,
            "abs_delta": abs(current_rate - base_rate),
        }
        if before.get("type") != after.get("type"):
            schema_events.append({
                "field": name,
                "event": "type_changed",
                "baseline_type": before.get("type"),
                "current_type": after.get("type"),
                "detail": f"字段 {name!r} 类型由 {before.get('type')} 变为 {after.get('type')}，分布指标不再可比",
            })
        elif before.get("type") == "number":
            edges = bin_edges(before.get("values", []), spec["psi_bins"])
            value = psi_with_edges(before.get("values", []), after.get("values", []), edges)
            entry["metrics"]["psi"] = {
                "value": value,
                "computable": value is not None,
                "bins": len(edges) + 1,
                "edges": edges,
                "note": None if value is not None else "某一侧没有可用数值，PSI 不可计算",
            }
        elif before.get("type") == "string":
            value = categorical_psi(before.get("categories", {}), after.get("categories", {}))
            entry["metrics"]["categorical_psi"] = {
                "value": value,
                "computable": value is not None,
                "note": None if value is not None else "某一侧没有可用类别，PSI 不可计算",
            }
        field_metrics.append(entry)
    return {"schema_events": schema_events, "field_metrics": field_metrics}


def evaluate_alerts(metrics: dict, spec: dict | None = None) -> list[dict]:
    """Turn stored metrics into threshold breaches, each with a human reason."""
    spec = normalize_spec(spec)
    alerts = []
    if spec["alert_on_schema_events"]:
        for event in metrics.get("schema_events", []):
            alerts.append({
                "field": event["field"],
                "category": "schema_event",
                "event": event["event"],
                "reason": event["detail"],
            })
    for entry in metrics.get("field_metrics", []):
        field = entry["field"]
        field_metrics = entry.get("metrics", {})
        missing = field_metrics.get("missing_rate")
        threshold = spec["missing_rate_threshold"]
        if missing and missing["abs_delta"] >= threshold:
            alerts.append({
                "field": field,
                "category": "data_drift",
                "metric": "missing_rate",
                "baseline_value": missing["baseline"],
                "current_value": missing["current"],
                "delta": missing["delta"],
                "threshold": threshold,
                "reason": (
                    f"missing_rate 由 {missing['baseline']:.4f} 变为 {missing['current']:.4f}，"
                    f"|Δ|={missing['abs_delta']:.4f} ≥ 阈值 {threshold}"
                ),
            })
        numeric = field_metrics.get("psi")
        threshold = spec["psi_threshold"]
        if numeric and numeric["computable"] and numeric["value"] >= threshold:
            alerts.append({
                "field": field,
                "category": "data_drift",
                "metric": "psi",
                "value": numeric["value"],
                "threshold": threshold,
                "reason": (
                    f"PSI={numeric['value']:.4f} ≥ 阈值 {threshold}"
                    f"（{numeric['bins']} 个分箱，边界取自基线版本分位数）"
                ),
            })
        categorical = field_metrics.get("categorical_psi")
        threshold = spec["categorical_psi_threshold"]
        if categorical and categorical["computable"] and categorical["value"] >= threshold:
            alerts.append({
                "field": field,
                "category": "data_drift",
                "metric": "categorical_psi",
                "value": categorical["value"],
                "threshold": threshold,
                "reason": f"类别分布 PSI={categorical['value']:.4f} ≥ 阈值 {threshold}",
            })
    return alerts


def compare_profiles(baseline: dict, current: dict, spec: dict | None = None) -> dict:
    """Full deterministic comparison of two profile snapshots."""
    spec = normalize_spec(spec)
    metrics = compute_metrics(baseline, current, spec)
    return {
        "spec": spec,
        "schema_events": metrics["schema_events"],
        "field_metrics": metrics["field_metrics"],
        "alerts": evaluate_alerts(metrics, spec),
    }
