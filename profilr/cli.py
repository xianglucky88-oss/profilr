from __future__ import annotations

import json
import sys
from pathlib import Path

from .baseline import BaselineStore
from .drift import absolute_drift
from .notify import merge_alerts
from .profile import profile_csv
from .validate import check_profile


ROOT = Path(__file__).resolve().parents[1]

USAGE = "usage: python3 -m profilr.cli demo | python3 -m profilr.cli profile CSV [--reference CSV]"


def demo() -> dict:
    db_path = "/tmp/profilr/profilr.db"
    store = BaselineStore(db_path)
    baseline = profile_csv(ROOT / "data" / "batch_a.csv")
    # Pass the baseline as reference so the current batch reuses its
    # histogram edges and column-set changes surface explicitly.
    current = profile_csv(ROOT / "data" / "batch_b.csv", reference=baseline)
    version = store.save("orders", baseline)
    drift = absolute_drift(baseline, current)
    alerts = merge_alerts(drift, threshold=0.1)
    return {
        "baseline_version": version,
        "baseline": baseline,
        "current": current,
        "drift": drift,
        "alerts": alerts,
        "consistency": {
            "baseline": check_profile(baseline),
            "current": check_profile(current),
        },
    }


def profile_command(csv_path: str, reference_path: str | None = None) -> dict:
    reference = profile_csv(reference_path) if reference_path else None
    profile = profile_csv(csv_path, reference=reference)
    violations = check_profile(profile)
    return {"profile": profile, "consistency": {"ok": not violations, "violations": violations}}


if __name__ == "__main__":
    argv = sys.argv[1:]
    if argv == ["demo"]:
        print(json.dumps(demo(), indent=2, ensure_ascii=False))
    elif len(argv) >= 2 and argv[0] == "profile":
        reference = None
        rest = argv[2:]
        if rest:
            if len(rest) == 2 and rest[0] == "--reference":
                reference = rest[1]
            else:
                raise SystemExit(USAGE)
        print(json.dumps(profile_command(argv[1], reference), indent=2, ensure_ascii=False))
    else:
        raise SystemExit(USAGE)
