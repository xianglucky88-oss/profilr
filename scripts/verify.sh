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
checks = [
    ("基线版本", r["baseline_version"] == 1),
    ("行数剖析", r["baseline"]["row_count"] == 4 and r["current"]["row_count"] == 5),
    ("空值漂移", any(a["field"] == "amount" for a in r["alerts"])),
    ("通知合并", len(r["alerts"]) == 1),
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
