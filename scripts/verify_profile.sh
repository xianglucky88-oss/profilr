#!/bin/sh
# 验收: 对包含缺失、混合类型和极端值的 CSV 做类型感知剖析,
# 断言原始计数、摘要和字段状态彼此一致。
set -eu
cd "$(dirname "$0")/.."
export PYTHONPYCACHEPREFIX=/tmp/profilr-pycache

mixed=$(python3 -m profilr.cli profile data/mixed.csv)
empty=$(python3 -m profilr.cli profile data/empty.csv)
schema=$(python3 -m profilr.cli profile data/batch_c.csv --reference data/batch_a.csv)

python3 - "$mixed" "$empty" "$schema" <<'PY'
import csv
import json
import sys

mixed, empty, schema = (json.loads(arg) for arg in sys.argv[1:4])
profile = mixed["profile"]
fields = profile["fields"]
amount = fields["amount"]

checks = [
    ("混合CSV一致性", mixed["consistency"]["ok"]),
    ("空CSV一致性", empty["consistency"]["ok"]),
    ("列变化一致性", schema["consistency"]["ok"]),
    ("非法数字降级", amount["status"] == "degraded" and amount["invalid"] == 1),
    ("极端值排除", amount["summary"]["max"] == 1000000.5 and amount["summary"]["min"] == -7.25),
    ("分位数有序", amount["summary"]["quantiles"]["p25"] <= amount["summary"]["quantiles"]["p50"] <= amount["summary"]["quantiles"]["p95"]),
    ("全空列状态", fields["always_null"]["type"] == "unknown" and fields["always_null"]["status"] == "all_missing" and fields["always_null"]["summary"] is None),
    ("字符串基数", fields["country"]["summary"]["distinct"] == 2 and fields["country"]["summary"]["top"][0] == {"value": "US", "count": 8}),
    ("混合字符串", fields["note"]["status"] == "degraded" and fields["note"]["summary"]["numeric_like"] == 2),
    ("空文件状态", empty["profile"]["status"] == "empty" and empty["profile"]["row_count"] == 0 and empty["profile"]["fields"]["amount"]["status"] == "empty"),
    ("列集合变化", schema["profile"]["schema"]["dropped"] == ["country"] and schema["profile"]["schema"]["added"] == ["channel"]),
    ("删除列状态", schema["profile"]["fields"]["country"]["status"] == "dropped" and schema["profile"]["fields"]["country"]["missing_rate"] is None),
]

# 独立重算: 不经过剖析代码, 直接从 CSV 数 amount 缺失, 与结果对齐
with open("data/mixed.csv", newline="", encoding="utf-8") as handle:
    rows = list(csv.DictReader(handle))
missing_tokens = {"", "null", "none", "na", "n/a", "nan"}
recount = sum(str(row.get("amount", "")).strip().lower() in missing_tokens for row in rows)
checks.append(("原始计数对齐", recount == amount["missing"] == 3 and amount["total"] == len(rows)))

# 直方图合计必须与数值摘要的样本数对齐
hist = amount["summary"]["histogram"]
checks.append(("直方图对齐", sum(hist["counts"]) + hist["underflow"] + hist["overflow"] == amount["summary"]["sample_count"] == amount["summary"]["count"]))

failed = 0
print("== 类型感知剖析断言 ==")
for name, ok in checks:
    if not ok:
        failed += 1
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
print(f"\n结果: {len(checks) - failed}/{len(checks)} 通过")
raise SystemExit(1 if failed else 0)
PY
