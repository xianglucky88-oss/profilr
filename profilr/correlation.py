"""Numeric and simple categorical correlation helpers."""

from __future__ import annotations

import math


def pearson(a: list[float], b: list[float]) -> float:
    if len(a) != len(b) or len(a) < 2:
        return 0.0
    ma, mb = sum(a) / len(a), sum(b) / len(b)
    numerator = sum((x - ma) * (y - mb) for x, y in zip(a, b))
    denominator = math.sqrt(sum((x - ma) ** 2 for x in a) * sum((y - mb) ** 2 for y in b))
    return numerator / denominator if denominator else 0.0


def numeric_matrix(rows: list[dict[str, str]], columns: list[str]) -> dict[str, dict[str, float]]:
    values = {column: [float(row[column]) for row in rows if row.get(column, "") != ""] for column in columns}
    return {left: {right: pearson(values[left], values[right]) for right in columns} for left in columns}
