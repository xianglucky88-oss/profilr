#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
rm -rf /tmp/profilr
first=$(PYTHONPYCACHEPREFIX=/tmp/profilr-pycache python3 -m profilr.cli demo)
second=$(PYTHONPYCACHEPREFIX=/tmp/profilr-pycache python3 -m profilr.cli demo)
printf '%s\n' "$first"
PYTHONPYCACHEPREFIX=/tmp/profilr-pycache python3 - "$first" "$second" <<'PY'
import json
import sys

r = json.loads(sys.argv[1])
r2 = json.loads(sys.argv[2])
amount_alerts = [a for a in r["alerts"] if a["field"] == "amount"]
amount_metrics = next(m["metrics"] for m in r["metrics"] if m["field"] == "amount")
checks = [
    ("基线版本", r["baseline_version"] == 1),
    ("当前版本", r["current_version"] == 2),
    ("行数剖析", r["baseline"]["row_count"] == 4 and r["current"]["row_count"] == 5),
    ("比较版本标识", r["compared_versions"] == {"baseline": 1, "current": 2}),
    ("空值漂移告警", any(a["metric"] == "missing_rate" for a in amount_alerts)),
    ("混合指标(PSI+缺失率)", "psi" in amount_metrics and "missing_rate" in amount_metrics
     and amount_metrics["psi"]["computable"]),
    ("告警含触发原因", all(a.get("reason") for a in r["alerts"])),
    ("schema事件与漂移分离", r["schema_events"] == [] and r["summary"]["schema_event_count"] == 0),
    ("重复提交幂等", r2["baseline_created"] is False and r2["current_created"] is False),
    ("比较可重复", r2["comparison_id"] == r["comparison_id"] and r2["comparison_reused"] is True),
]
failed = 0
print("== 断言 ==")
for name, ok in checks:
    if not ok:
        failed += 1
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
print(f"\n结果: {len(checks) - failed}/{len(checks)} 通过")
raise SystemExit(1 if failed else 0)
PY
