"""Cross-batch drift calculations."""

from __future__ import annotations

import math


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
