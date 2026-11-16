"""Consistency checks: raw counts, summaries and field statuses must agree.

``check_profile`` returns a list of human-readable violations; an empty
list means the profile is internally consistent. It is used by the unit
tests and by ``scripts/verify_profile.sh`` as the acceptance gate.
"""

from __future__ import annotations


def check_profile(profile: dict) -> list[str]:
    problems: list[str] = []
    row_count = profile.get("row_count")
    fields = profile.get("fields", {})
    schema = profile.get("schema")

    expected_status = "empty" if row_count == 0 else "ok"
    if profile.get("status") != expected_status:
        problems.append(f"row_count={row_count} but status is {profile.get('status')!r}")

    dropped = set(schema["dropped"]) if schema else set()
    if set(fields) - dropped != set(profile.get("columns", [])):
        problems.append("columns list does not match profiled fields")

    for name, field in fields.items():
        problems.extend(_check_field(name, field, row_count))

    if schema is not None:
        for name in schema["dropped"]:
            if fields.get(name, {}).get("status") != "dropped":
                problems.append(f"schema lists {name!r} as dropped but field status disagrees")
        for name in schema["added"]:
            if name not in fields:
                problems.append(f"schema lists {name!r} as added but the field is missing")
    return problems


def _check_field(name: str, field: dict, row_count: int) -> list[str]:
    problems = []
    prefix = f"field {name!r}"
    total, missing, present = field["total"], field["missing"], field["present"]
    invalid, status, ftype = field["invalid"], field["status"], field["type"]
    summary = field["summary"]

    if missing + present != total:
        problems.append(f"{prefix}: missing({missing}) + present({present}) != total({total})")
    expected_rate = missing / total if total else None
    if field["missing_rate"] != expected_rate:
        problems.append(f"{prefix}: missing_rate does not equal missing/total")

    if status in ("empty", "dropped"):
        if total != 0:
            problems.append(f"{prefix}: status {status!r} but total={total}")
        if summary is not None:
            problems.append(f"{prefix}: status {status!r} but a summary is present")
        return problems

    if total != row_count:
        problems.append(f"{prefix}: total({total}) != row_count({row_count})")
    if present == 0:
        if status != "all_missing":
            problems.append(f"{prefix}: no present values but status is {status!r}")
        if ftype != "unknown" or summary is not None:
            problems.append(f"{prefix}: no present values but type={ftype!r} with a summary")
        return problems
    if summary is None:
        problems.append(f"{prefix}: {present} present values but no summary")
        return problems

    if status == "degraded":
        if ftype == "number" and invalid == 0:
            problems.append(f"{prefix}: degraded number but invalid == 0")
        if ftype == "string" and summary.get("numeric_like", 0) == 0:
            problems.append(f"{prefix}: degraded string but numeric_like == 0")
    elif invalid != 0 or (ftype == "string" and summary.get("numeric_like", 0) != 0):
        problems.append(f"{prefix}: status 'ok' but non-conforming values exist")

    if ftype == "number":
        problems.extend(_check_numeric(prefix, field, summary))
    elif ftype == "string":
        problems.extend(_check_string(prefix, field, summary))
    else:
        problems.append(f"{prefix}: present values but type is {ftype!r}")
    return problems


def _check_numeric(prefix: str, field: dict, summary: dict) -> list[str]:
    problems = []
    if summary.get("kind") != "numeric":
        return [f"{prefix}: number field with non-numeric summary"]
    if summary["count"] + field["invalid"] != field["present"]:
        problems.append(f"{prefix}: numeric count + invalid != present")
    if summary["count"] <= 0:
        problems.append(f"{prefix}: numeric summary without values")
        return problems

    low, high = summary["min"], summary["max"]
    if low is None or high is None or low > high:
        problems.append(f"{prefix}: min/max inconsistent")
    quantiles = sorted(
        (float(label[1:]), value)
        for label, value in summary.get("quantiles", {}).items()
        if value is not None
    )
    values = [value for _, value in quantiles]
    if values != sorted(values):
        problems.append(f"{prefix}: quantiles are not monotonic in q")
    if values and low is not None and high is not None and (values[0] < low or values[-1] > high):
        problems.append(f"{prefix}: quantiles fall outside [min, max]")

    hist = summary.get("histogram", {})
    edges, counts = hist.get("edges", []), hist.get("counts", [])
    if len(edges) != len(counts) + 1:
        problems.append(f"{prefix}: histogram has {len(counts)} bins but {len(edges)} edges")
    if any(right <= left for left, right in zip(edges, edges[1:])):
        problems.append(f"{prefix}: histogram edges are not strictly increasing")
    if sum(counts) + hist.get("underflow", 0) + hist.get("overflow", 0) != summary.get("sample_count"):
        problems.append(f"{prefix}: histogram total + under/overflow != sample_count")
    if hist.get("edges_source") == "data" and (hist.get("underflow") or hist.get("overflow")):
        problems.append(f"{prefix}: data-derived edges but under/overflow is non-zero")
    if not summary.get("approximate") and summary.get("sample_count") != summary.get("count"):
        problems.append(f"{prefix}: exact summary but sample_count != count")
    return problems


def _check_string(prefix: str, field: dict, summary: dict) -> list[str]:
    problems = []
    if summary.get("kind") != "string":
        return [f"{prefix}: string field with non-string summary"]
    if field["invalid"] != 0:
        problems.append(f"{prefix}: string field excludes values (invalid != 0)")
    if summary["count"] != field["present"]:
        problems.append(f"{prefix}: string count != present")
    top = summary.get("top", [])
    counts = [item["count"] for item in top]
    if counts != sorted(counts, reverse=True):
        problems.append(f"{prefix}: top values are not sorted by count")
    if any(count <= 0 for count in counts):
        problems.append(f"{prefix}: top values with non-positive counts")
    if sum(counts) > summary["count"]:
        problems.append(f"{prefix}: top counts exceed the string count")
    if len(top) > summary["distinct"]:
        problems.append(f"{prefix}: more top values than distinct values")
    if not isinstance(summary.get("distinct_exact"), bool):
        problems.append(f"{prefix}: distinct_exact flag missing")
    return problems
