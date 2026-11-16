#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
rm -rf /tmp/profilr
output=$(PYTHONPYCACHEPREFIX=/tmp/profilr-pycache python3 -m profilr.cli demo)
printf '%s\n' "$output"
PYTHONPYCACHEPREFIX=/tmp/profilr-pycache python3 - "$output" <<'PY'
import json
import sys

r = json.loads(sys.argv[1])
view = r["alert_view"]["items"]
checks = [
    ("基线版本", r["baseline_version"] == 1),
    ("行数剖析", r["baseline"]["row_count"] == 4 and r["current"]["row_count"] == 5),
    ("空值漂移", any(a["field"] == "amount" for a in r["alerts"])),
    ("通知合并(兼容)", len(r["alerts"]) == 1),
    ("生命周期单条记录", len(view) == 1),
    ("最终抑制状态", view[0]["status"] == "suppressed" and view[0]["suppression_active"] is True),
    ("首次出现时间", view[0]["first_seen_at"] is not None),
    ("重开次数", view[0]["reopen_count"] == 1),
    ("重复批次幂等", r["lifecycle"]["replay"]["duplicate"] is True),
    ("恢复批次计数", r["lifecycle"]["recovery_batch"]["recovered"] == 1),
    ("抑制窗口命中", r["lifecycle"]["suppressed_batch"]["suppressed_hits"] == 1),
    ("事件审计链", [e["event_type"] for e in r["alert_events"]]
        == ["opened", "repeated", "recovered", "reopened", "suppressed", "repeated"]),
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
