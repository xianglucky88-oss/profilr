"""Distribution summaries for numeric columns.

Conventions
-----------
* Quantiles use linear interpolation over ``(n - 1) * q`` (R type-7 /
  numpy "linear"): ``q=0`` is the minimum, ``q=1`` the maximum, and an
  empty input yields ``None`` instead of an invented number.
* Histograms carry explicit edges (``bins + 1`` strictly increasing
  floats). Bin ``i`` covers ``[edges[i], edges[i+1])``; the last bin is
  closed on both ends. Values outside ``[edges[0], edges[-1]]`` are
  counted in ``underflow`` / ``overflow`` instead of being silently
  dropped, so a batch profiled with baseline edges stays bin-by-bin
  comparable with that baseline.
* Constant columns are widened around the value so edges stay strictly
  increasing and bin indices remain well defined.
"""

from __future__ import annotations

import bisect
import random

DEFAULT_BINS = 10
DEFAULT_QUANTILES = (0.25, 0.5, 0.75, 0.95)


def quantile(values: list[float], q: float) -> float | None:
    """Linear-interpolated quantile; ``None`` for empty input."""
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def quantile_label(q: float) -> str:
    """Stable label for a quantile level, e.g. 0.95 -> ``p95``."""
    return f"p{q * 100:g}"


def histogram_edges(low: float, high: float, bins: int = DEFAULT_BINS) -> list[float]:
    """Equal-width edges from ``low`` to ``high`` (constant columns widened)."""
    if bins <= 0:
        raise ValueError("bins must be positive")
    if low == high:
        pad = max(0.5, abs(low) * 1e-6)
        low, high = low - pad, high + pad
    edges = [low + (high - low) * i / bins for i in range(bins)]
    edges.append(high)
    return edges


def histogram(values: list[float], edges: list[float]) -> dict:
    """Bin ``values`` against explicit ``edges``; out-of-range values counted."""
    if len(edges) < 2:
        raise ValueError("histogram needs at least two edges")
    counts = [0] * (len(edges) - 1)
    underflow = overflow = 0
    low, high = edges[0], edges[-1]
    for value in values:
        if value < low:
            underflow += 1
        elif value > high:
            overflow += 1
        else:
            index = min(bisect.bisect_right(edges, value) - 1, len(counts) - 1)
            counts[index] += 1
    return {"edges": list(edges), "counts": counts, "underflow": underflow, "overflow": overflow}


class Reservoir:
    """Deterministic bounded sample (algorithm R).

    Keeps at most ``capacity`` values no matter how many are added, and
    is seeded so repeated runs over the same data yield the same sample.
    """

    def __init__(self, capacity: int, seed: int = 0):
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self.seen = 0
        self._rng = random.Random(seed)
        self._values: list[float] = []

    def add(self, value: float) -> None:
        self.seen += 1
        if len(self._values) < self.capacity:
            self._values.append(value)
            return
        index = self._rng.randrange(self.seen)
        if index < self.capacity:
            self._values[index] = value

    @property
    def values(self) -> list[float]:
        return list(self._values)

    @property
    def approximate(self) -> bool:
        return self.seen > self.capacity
