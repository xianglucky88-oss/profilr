"""Alert grouping, suppression-window primitives and lifecycle pipeline glue.

``merge_alerts`` is retained for backward compatibility: it performs the
original single-batch field/metric deduplication.  New code should feed the
full drift observations (including below-threshold pairs) into
:class:`profilr.alerting.AlertStore.ingest_observations`, which keeps one
lifecycle record per stable alert identity across batches.
"""

from __future__ import annotations

from typing import Any

from .alerting import AlertStore, alert_id_for, utcnow


def merge_alerts(alerts: list[dict], threshold: float = 0.1) -> list[dict]:
    """Legacy single-batch dedupe: keep the max delta per (field, metric)."""
    grouped = {}
    for alert in alerts:
        if alert.get("delta", 0.0) < threshold:
            continue
        key = (alert.get("field"), alert.get("metric"))
        grouped[key] = max(grouped.get(key, 0.0), alert["delta"])
    return [
        {"field": field, "metric": metric, "delta": delta}
        for (field, metric), delta in grouped.items()
    ]


def run_alert_pipeline(
    store: AlertStore,
    table_name: str,
    observations: list[dict],
    *,
    baseline_version: int | None,
    observed_version: int | None,
    threshold: float = 0.1,
    batch_id: str | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Fold a batch of raw drift observations into the lifecycle store.

    Wraps :meth:`AlertStore.ingest_observations` and additionally returns the
    legacy-style merged view (``merged``) so existing callers keep working,
    alongside the lifecycle summary and the affected alert rows.
    """
    now = now or utcnow()
    summary = store.ingest_observations(
        table_name,
        observations,
        baseline_version=baseline_version,
        observed_version=observed_version,
        threshold=threshold,
        batch_id=batch_id,
        now=now,
    )
    affected = store.query_alerts(
        table_name=table_name,
        since=None,
        limit=200,
        now=now,
    )["items"]
    return {
        "batch_id": summary["batch_id"],
        "duplicate": summary["duplicate"],
        "summary": summary,
        # Legacy-compatible view of this batch's over-threshold pairs.
        "merged": merge_alerts(observations, threshold),
        # Current lifecycle view for the table after folding this batch.
        "alerts": affected,
        "identity_of": lambda field, metric: alert_id_for(table_name, field, metric),
    }
