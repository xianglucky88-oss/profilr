from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .alerting import AlertStore, utcnow
from .baseline import BaselineStore
from .drift import absolute_drift
from .notify import merge_alerts, run_alert_pipeline
from .profile import profile_rows, read_csv


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = "/tmp/profilr/profilr.db"


def demo() -> dict:
    # Clean run so the demo script stays deterministic.
    Path(DB_PATH).unlink(missing_ok=True)

    store = BaselineStore(DB_PATH)
    alerts = AlertStore(DB_PATH)

    baseline = profile_rows(read_csv(ROOT / "data" / "batch_a.csv"))
    current = profile_rows(read_csv(ROOT / "data" / "batch_b.csv"))
    version = store.save("orders", baseline)

    # 1. First noisy batch: identity appears for the first time -> status new.
    drift = absolute_drift(baseline, current)
    first = run_alert_pipeline(
        alerts, "orders", drift,
        baseline_version=version, observed_version=2, threshold=0.1,
        batch_id="batch-b",
    )

    # 2. Replaying the same batch is idempotent: no duplicate active rows.
    replay = alerts.ingest_observations(
        "orders", drift,
        baseline_version=version, observed_version=2,
        threshold=0.1, batch_id="batch-b",
    )

    # 3. The same drift observed again becomes "active" (persistent drift).
    persistent = alerts.ingest_observations(
        "orders", drift,
        baseline_version=version, observed_version=3,
        threshold=0.1, batch_id="batch-c",
    )

    # 4. A quiet batch (profile equal to the baseline) recovers the alert.
    quiet = absolute_drift(baseline, baseline)
    recovered = alerts.ingest_observations(
        "orders", quiet,
        baseline_version=version, observed_version=4,
        threshold=0.1, batch_id="batch-d",
    )

    # 5. The same identity drifting again reopens the same alert row.
    reopened = alerts.ingest_observations(
        "orders", drift,
        baseline_version=version, observed_version=5,
        threshold=0.1, batch_id="batch-e",
    )

    # 6. Operator suppresses the alert for a fixed window.  Threshold changes
    #    never reopen it; only the window expiring while drift persists can.
    target = alerts.query_alerts(table_name="orders", limit=1)["items"][0]
    window_end = (
        datetime.now(timezone.utc) + timedelta(hours=1)
    ).isoformat(timespec="seconds")
    suppressed = alerts.suppress(target["alert_id"], window_end)
    suppressed_hit = alerts.ingest_observations(
        "orders", drift,
        baseline_version=version, observed_version=6,
        threshold=0.1, batch_id="batch-f",
    )

    view = alerts.query_alerts(table_name="orders", limit=50)
    events = alerts.list_events(target["alert_id"])

    # Legacy-compatible fields are kept verbatim; the lifecycle view is added.
    return {
        "baseline_version": version,
        "baseline": baseline,
        "current": current,
        "drift": drift,
        "alerts": merge_alerts(drift, threshold=0.1),
        "lifecycle": {
            "first_batch": first["summary"],
            "replay": replay,
            "persistent_batch": persistent,
            "recovery_batch": recovered,
            "reopen_batch": reopened,
            "suppressed_batch": suppressed_hit,
        },
        "alert_view": view,
        "alert_events": events,
        "suppression": {
            "alert_id": target["alert_id"],
            "suppress_until": suppressed["suppress_until"],
            "now": utcnow(),
        },
    }


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] != "demo":
        raise SystemExit("usage: python3 -m profilr.cli demo")
    print(json.dumps(demo(), indent=2, ensure_ascii=False))
