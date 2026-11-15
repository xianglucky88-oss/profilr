"""Alert grouping and suppression-window primitives."""

from __future__ import annotations


def merge_alerts(alerts: list[dict], threshold: float = 0.1) -> list[dict]:
    grouped = {}
    for alert in alerts:
        if alert.get("delta", 0.0) < threshold:
            continue
        key = (alert.get("field"), alert.get("metric"))
        grouped[key] = max(grouped.get(key, 0.0), alert["delta"])
    return [{"field": field, "metric": metric, "delta": delta} for (field, metric), delta in grouped.items()]
