"""Distribution summaries for numeric columns."""

from __future__ import annotations


def quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def histogram(values: list[float], bins: int = 5) -> list[int]:
    if not values or bins <= 0:
        return [0] * max(bins, 0)
    low, high = min(values), max(values)
    if low == high:
        return [len(values)] + [0] * (bins - 1)
    counts = [0] * bins
    width = (high - low) / bins
    for value in values:
        index = min(int((value - low) / width), bins - 1)
        counts[index] += 1
    return counts


def summarize(values: list[float]) -> dict:
    return {"count": len(values), "min": min(values, default=0.0),
            "p50": quantile(values, 0.5), "p95": quantile(values, 0.95),
            "max": max(values, default=0.0), "histogram": histogram(values)}
