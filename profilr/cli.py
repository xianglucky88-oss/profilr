from __future__ import annotations

import json
import sys
from pathlib import Path

from .baseline import BaselineStore
from .drift import absolute_drift
from .notify import merge_alerts
from .profile import profile_rows, read_csv


ROOT = Path(__file__).resolve().parents[1]


def demo() -> dict:
    db_path = "/tmp/profilr/profilr.db"
    store = BaselineStore(db_path)
    baseline = profile_rows(read_csv(ROOT / "data" / "batch_a.csv"))
    current = profile_rows(read_csv(ROOT / "data" / "batch_b.csv"))
    version = store.save("orders", baseline)
    drift = absolute_drift(baseline, current)
    alerts = merge_alerts(drift, threshold=0.1)
    return {"baseline_version": version, "baseline": baseline, "current": current, "drift": drift, "alerts": alerts}


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] != "demo":
        raise SystemExit("usage: python3 -m profilr.cli demo")
    print(json.dumps(demo(), indent=2, ensure_ascii=False))
