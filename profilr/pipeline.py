"""Versioned profiling → baseline → drift pipeline orchestration.

Every comparison links four things: the two version identities, the profile
snapshots behind them, the comparison spec (比较口径), and the resulting
alerts. Repeating a comparison with the same inputs returns the stored record
unchanged (reproducible); `reevaluate` re-interprets stored metrics with new
thresholds without re-profiling.
"""

from __future__ import annotations

import json

from .baseline import (
    BaselineError,
    BaselineNotFoundError,
    BaselineStore,
    ComparisonNotFoundError,
    VersionNotFoundError,
)
from .drift import compare_profiles, evaluate_alerts, normalize_spec


def save_baseline(store: BaselineStore, table: str, profile: dict, input_identity: str | None = None) -> dict:
    return store.save_profile(table, profile, input_identity)


def list_versions(store: BaselineStore, table: str) -> dict:
    return {"table": table, "head": store.head(table), "versions": store.versions(table)}


def rollback_baseline(store: BaselineStore, table: str, to_version: int) -> dict:
    result = store.rollback(table, to_version)
    result["head_profile"] = store.load(table)
    return result


def _resolve_versions(store: BaselineStore, table: str, from_version: int | None, to_version: int | None) -> tuple[int, int]:
    head = store.head(table)
    if head is None:
        raise BaselineNotFoundError(f"表 {table!r} 还没有任何基线版本，请先保存基线再比较")
    available = [entry["version"] for entry in store.versions(table)]
    to_v = to_version if to_version is not None else head["version"]
    if to_v not in available:
        raise VersionNotFoundError(f"表 {table!r} 不存在版本 {to_v}（现有版本: {available}）")
    if from_version is None:
        earlier = [version for version in available if version < to_v]
        if not earlier:
            raise BaselineError(f"表 {table!r} 在版本 {to_v} 之前没有可比较的历史版本")
        from_v = max(earlier)
    else:
        from_v = from_version
        if from_v not in available:
            raise VersionNotFoundError(f"表 {table!r} 不存在版本 {from_v}（现有版本: {available}）")
    if from_v == to_v:
        raise BaselineError(f"基线版本与当前版本相同（{from_v}），无法比较")
    return from_v, to_v


def _envelope(store: BaselineStore, row: dict, reused: bool) -> dict:
    metrics = json.loads(row["metrics_json"]) if row.get("metrics_json") else {"schema_events": [], "field_metrics": []}
    alerts = json.loads(row["alerts_json"]) if row.get("alerts_json") else []
    return {
        "comparison_id": row["comparison_id"],
        "table": row["table_name"],
        "compared_versions": {"baseline": row["baseline_version"], "current": row["current_version"]},
        "spec_id": row["spec_id"],
        "spec": store.get_spec(row["spec_id"]),
        "status": row["status"],
        "reused": reused,
        "parent_id": row.get("parent_id"),
        "error": row.get("error"),
        "schema_events": metrics["schema_events"],
        "field_metrics": metrics["field_metrics"],
        "alerts": alerts,
        "summary": {
            "alert_count": len(alerts),
            "schema_event_count": len(metrics["schema_events"]),
            "drifted_fields": sorted({a["field"] for a in alerts if a.get("category") == "data_drift"}),
        },
        "created_at": row.get("created_at"),
    }


def compare_versions(
    store: BaselineStore,
    table: str,
    from_version: int | None = None,
    to_version: int | None = None,
    spec: dict | None = None,
) -> dict:
    """Compare two versions reproducibly.

    Defaults to the latest head against its immediate predecessor. If the same
    (table, versions, spec) was already computed, the stored record is
    returned with ``reused=True``; interrupted half-finished rows are
    recomputed from scratch.
    """
    spec = normalize_spec(spec)
    from_v, to_v = _resolve_versions(store, table, from_version, to_version)
    spec_id = store.ensure_spec(spec)
    comparison_id = BaselineStore.comparison_id(table, from_v, to_v, spec_id)
    existing = store.get_comparison(comparison_id)
    if existing and existing["status"] == "complete":
        return _envelope(store, existing, reused=True)
    store.start_comparison(comparison_id, table, from_v, to_v, spec_id)
    try:
        baseline = store.load(table, from_v)
        current = store.load(table, to_v)
        result = compare_profiles(baseline, current, spec)
        store.finish_comparison(
            comparison_id,
            {"schema_events": result["schema_events"], "field_metrics": result["field_metrics"]},
            result["alerts"],
        )
    except Exception as exc:
        store.fail_comparison(comparison_id, str(exc))
        raise
    return _envelope(store, store.get_comparison(comparison_id), reused=False)


def reevaluate(store: BaselineStore, table: str, comparison_id: str, spec: dict | None = None) -> dict:
    """Re-interpret a completed comparison's stored metrics with a new spec.

    Used when alert thresholds change: historical comparisons keep their raw
    metrics, so alerts can be re-derived without re-profiling the inputs. The
    result is stored as a new comparison linked to its parent.
    """
    original = store.get_comparison(comparison_id)
    if original is None or original["table_name"] != table:
        raise ComparisonNotFoundError(f"表 {table!r} 不存在比较记录 {comparison_id}")
    if original["status"] != "complete":
        raise BaselineError(f"比较 {comparison_id} 状态为 {original['status']}，只有已完成的比较可以重新解释")
    spec = normalize_spec(spec)
    metrics = json.loads(original["metrics_json"])
    alerts = evaluate_alerts(metrics, spec)
    spec_id = store.ensure_spec(spec)
    new_id = BaselineStore.comparison_id(table, original["baseline_version"], original["current_version"], spec_id)
    existing = store.get_comparison(new_id)
    if existing and existing["status"] == "complete":
        return _envelope(store, existing, reused=True)
    store.start_comparison(
        new_id, table, original["baseline_version"], original["current_version"], spec_id,
        parent_id=comparison_id,
    )
    store.finish_comparison(new_id, metrics, alerts)
    return _envelope(store, store.get_comparison(new_id), reused=False)


def get_comparison_result(store: BaselineStore, comparison_id: str) -> dict:
    row = store.get_comparison(comparison_id)
    if row is None:
        raise ComparisonNotFoundError(f"不存在比较记录 {comparison_id}")
    return _envelope(store, row, reused=True)
