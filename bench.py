"""
bench.py — statistically-rigorous timing for the MapReduce cluster.

Why not just `time.time()` / a bare chronometer?
-------------------------------------------------
A single timestamp delta is noisy and misleading. This module gives the same
guarantees a tool like Google Benchmark provides:

  * Monotonic, nanosecond-resolution clock (`time.perf_counter_ns`) — immune to
    wall-clock adjustments (NTP, DST) that corrupt `time.time()` deltas.
  * Every measured span is stored as a *sample*, so a phase that runs many times
    (e.g. one TCP query per node) yields a full distribution, not one number.
  * Aggregates report count / total / mean / median / p90 / p95 / stddev and the
    coefficient of variation (CV) so you can tell a stable measurement from a
    noisy one.
  * `benchmark()` runs a callable repeatedly with warm-up + auto-scaling until a
    minimum wall budget is reached — the Google-Benchmark methodology for
    micro-benchmarks.
  * `report()` ranks phases by total time, groups them by category
    (io / network / compute / sync / deploy / cleanup) and prints the dominant
    **bottleneck**.
  * `save_json()` exports raw samples for later speedup / Amdahl plotting.

Typical use
-----------
    import bench

    with bench.phase("read:machines.txt", bench.Cat.IO):
        data = open("machines.txt").read()

    for host in hosts:
        with bench.phase("network:query", bench.Cat.NETWORK):
            query(host)

    bench.report()
    bench.save_json("timings.json")
"""

from __future__ import annotations

import json
import math
import statistics
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import wraps
from typing import Callable, Iterable

# Monotonic, nanosecond-resolution clock. Never goes backwards.
now_ns: Callable[[], int] = time.perf_counter_ns


class Cat:
    """Phase categories used to group totals and locate bottlenecks."""
    DEPLOY = "deploy"
    CLEANUP = "cleanup"
    IO = "io"
    SYNC = "sync"
    NETWORK = "network"
    COMPUTE = "compute"
    OTHER = "other"


# ── Statistics ────────────────────────────────────────────────────────────────

def _percentile(sorted_samples: list[int], p: float) -> float:
    """Linear-interpolation percentile (p in [0, 1]) over a sorted list."""
    n = len(sorted_samples)
    if n == 1:
        return float(sorted_samples[0])
    k = (n - 1) * p
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return float(sorted_samples[int(k)])
    return sorted_samples[lo] * (hi - k) + sorted_samples[hi] * (k - lo)


@dataclass
class Stats:
    label: str
    category: str
    count: int
    total: int          # ns
    mean: float         # ns
    median: float       # ns
    stdev: float        # ns
    cv: float           # stdev / mean (dimensionless)
    minimum: int        # ns
    maximum: int        # ns
    p90: float          # ns
    p95: float          # ns

    def as_dict(self) -> dict:
        return {
            "label": self.label,
            "category": self.category,
            "count": self.count,
            "total_ns": self.total,
            "mean_ns": self.mean,
            "median_ns": self.median,
            "stdev_ns": self.stdev,
            "cv": self.cv,
            "min_ns": self.minimum,
            "max_ns": self.maximum,
            "p90_ns": self.p90,
            "p95_ns": self.p95,
        }


@dataclass
class _Series:
    category: str
    samples: list[int] = field(default_factory=list)


# ── Registry ────────────────────────────────────────────────────────────────

class Registry:
    """Collects timed samples keyed by label, preserving insertion order."""

    def __init__(self) -> None:
        self._series: dict[str, _Series] = {}
        self._wall0 = now_ns()

    def reset(self) -> None:
        self._series.clear()
        self._wall0 = now_ns()

    def record(self, label: str, duration_ns: int, category: str = Cat.OTHER) -> None:
        s = self._series.get(label)
        if s is None:
            s = _Series(category=category)
            self._series[label] = s
        s.samples.append(int(duration_ns))

    @contextmanager
    def phase(self, label: str, category: str = Cat.OTHER):
        """Time the enclosed block and record one sample under `label`."""
        t0 = now_ns()
        try:
            yield
        finally:
            self.record(label, now_ns() - t0, category)

    def timed(self, label: str | None = None, category: str = Cat.OTHER):
        """Decorator: record one sample per call of the wrapped function."""
        def deco(fn: Callable):
            name = label or f"{fn.__module__}.{fn.__qualname__}"

            @wraps(fn)
            def wrapper(*args, **kwargs):
                t0 = now_ns()
                try:
                    return fn(*args, **kwargs)
                finally:
                    self.record(name, now_ns() - t0, category)
            return wrapper
        return deco

    def benchmark(
        self,
        label: str,
        fn: Callable[[], object],
        *,
        category: str = Cat.COMPUTE,
        min_time: float = 0.5,
        warmup: int = 3,
        max_iterations: int = 1_000_000,
    ) -> Stats:
        """
        Google-Benchmark-style micro-benchmark: warm up, then iterate the
        callable until `min_time` seconds of work has accumulated (or
        `max_iterations` reached), recording every iteration as a sample.
        """
        for _ in range(max(0, warmup)):
            fn()

        budget_ns = int(min_time * 1e9)
        accumulated = 0
        iterations = 0
        while accumulated < budget_ns and iterations < max_iterations:
            t0 = now_ns()
            fn()
            dt = now_ns() - t0
            self.record(label, dt, category)
            accumulated += dt
            iterations += 1
        return self.stats(label)

    def stats(self, label: str) -> Stats | None:
        s = self._series.get(label)
        if not s or not s.samples:
            return None
        samples = s.samples
        srt = sorted(samples)
        n = len(srt)
        total = sum(srt)
        mean = total / n
        stdev = statistics.pstdev(srt) if n > 1 else 0.0
        return Stats(
            label=label,
            category=s.category,
            count=n,
            total=total,
            mean=mean,
            median=statistics.median(srt),
            stdev=stdev,
            cv=(stdev / mean) if mean else 0.0,
            minimum=srt[0],
            maximum=srt[-1],
            p90=_percentile(srt, 0.90),
            p95=_percentile(srt, 0.95),
        )

    def all_stats(self) -> list[Stats]:
        out = [self.stats(label) for label in self._series]
        return [s for s in out if s is not None]

    # ── Reporting ──────────────────────────────────────────────────────────

    def report(self, file=sys.stdout, title: str = "TIMING REPORT") -> None:
        rows = self.all_stats()
        if not rows:
            print("  (no timing samples collected)", file=file)
            return

        rows.sort(key=lambda s: s.total, reverse=True)
        grand_total = sum(s.total for s in rows) or 1

        width = 72
        print("", file=file)
        print("=" * width, file=file)
        print(f"  {title}", file=file)
        print("=" * width, file=file)
        header = f"  {'phase':<26}{'n':>4}{'total':>12}{'mean':>11}{'p95':>11}{'%':>7}"
        print(header, file=file)
        print("  " + "-" * (width - 4), file=file)
        for s in rows:
            pct = 100.0 * s.total / grand_total
            print(
                f"  {s.label:<26}{s.count:>4}{_fmt(s.total):>12}"
                f"{_fmt(s.mean):>11}{_fmt(s.p95):>11}{pct:>6.1f}%",
                file=file,
            )

        # Category roll-up
        cats: dict[str, int] = {}
        for s in rows:
            cats[s.category] = cats.get(s.category, 0) + s.total
        print("  " + "-" * (width - 4), file=file)
        print("  by category:", file=file)
        for cat, tot in sorted(cats.items(), key=lambda kv: kv[1], reverse=True):
            pct = 100.0 * tot / grand_total
            print(f"    {cat:<22}{_fmt(tot):>12}{pct:>6.1f}%", file=file)

        # Bottleneck
        top = rows[0]
        top_cat = max(cats.items(), key=lambda kv: kv[1])
        print("  " + "-" * (width - 4), file=file)
        print(
            f"  BOTTLENECK phase    : {top.label} "
            f"({_fmt(top.total)}, {100.0 * top.total / grand_total:.1f}%)",
            file=file,
        )
        print(
            f"  BOTTLENECK category : {top_cat[0]} "
            f"({_fmt(top_cat[1])}, {100.0 * top_cat[1] / grand_total:.1f}%)",
            file=file,
        )
        # Flag noisy measurements (high relative variance, enough samples).
        noisy = [s for s in rows if s.count >= 5 and s.cv > 0.5]
        if noisy:
            names = ", ".join(f"{s.label} (CV={s.cv:.2f})" for s in noisy)
            print(f"  high variance       : {names}", file=file)
        print("=" * width, file=file)

    def save_json(self, path: str) -> None:
        payload = {
            "clock": "perf_counter_ns",
            "unit": "ns",
            "series": {
                label: {"category": s.category, "samples": s.samples}
                for label, s in self._series.items()
            },
            "stats": [s.as_dict() for s in self.all_stats()],
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)


def _fmt(ns: float) -> str:
    """Human-readable duration from nanoseconds."""
    if ns >= 1e9:
        return f"{ns / 1e9:.3f} s"
    if ns >= 1e6:
        return f"{ns / 1e6:.3f} ms"
    if ns >= 1e3:
        return f"{ns / 1e3:.3f} \u00b5s"
    return f"{int(ns)} ns"


# ── Module-level default registry + convenience wrappers ─────────────────────

default = Registry()


def reset() -> None:
    default.reset()


def record(label: str, duration_ns: int, category: str = Cat.OTHER) -> None:
    default.record(label, duration_ns, category)


def phase(label: str, category: str = Cat.OTHER):
    return default.phase(label, category)


def timed(label: str | None = None, category: str = Cat.OTHER):
    return default.timed(label, category)


def benchmark(label: str, fn: Callable[[], object], **kwargs) -> Stats:
    return default.benchmark(label, fn, **kwargs)


def stats(label: str) -> Stats | None:
    return default.stats(label)


def report(file=sys.stdout, title: str = "TIMING REPORT") -> None:
    default.report(file=file, title=title)


def save_json(path: str) -> None:
    default.save_json(path)


if __name__ == "__main__":
    # Self-test / demo of the measurement methodology.
    def busy():
        s = 0
        for i in range(10_000):
            s += i * i
        return s

    benchmark("demo:busy-loop", busy, min_time=0.2)
    with phase("demo:sleep", Cat.SYNC):
        time.sleep(0.05)
    report(title="bench.py self-test")
