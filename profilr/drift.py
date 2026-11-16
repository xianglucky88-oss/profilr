"""Cross-batch drift calculations."""

from __future__ import annotations

import math


def absolute_drift(baseline: dict, current: dict) -> list[dict]:
    """Compare missing rates; surface column-set changes as schema alerts."""
    alerts = []
    before_fields = baseline.get("fields", {})
    after_fields = current.get("fields", {})
    for field, before in before_fields.items():
        after = after_fields.get(field)
        if after is None or after.get("status") == "dropped":
            alerts.append({"field": field, "metric": "schema", "delta": 1.0})
            continue
        before_rate = before.get("missing_rate")
        after_rate = after.get("missing_rate")
        if before_rate is None or after_rate is None:
            continue
        alerts.append({"field": field, "metric": "missing_rate", "delta": abs(after_rate - before_rate)})
    for field in after_fields:
        if field not in before_fields:
            alerts.append({"field": field, "metric": "schema", "delta": 1.0})
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
