"""Sampling and chunking helpers for large inputs."""

from __future__ import annotations

import random


def sample_rows(rows: list[dict], limit: int, seed: int = 7) -> list[dict]:
    if len(rows) <= limit:
        return list(rows)
    return random.Random(seed).sample(rows, limit)


def chunks(rows: list[dict], size: int):
    for start in range(0, len(rows), size):
        yield rows[start:start + size]
