#!/usr/bin/env python3
"""
plot_amdahl.py — Plot empirical Amdahl's law speedup from amdahl_results.json.

Usage (run locally after copying amdahl_results.json from the lab machine):
    python3 plot_amdahl.py [--input amdahl_results.json] [--output amdahl_speedup.png]

Requires: matplotlib, numpy, scipy
    pip install matplotlib numpy scipy
"""

import argparse
import json
import sys

import numpy as np

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec
except ImportError:
    print("ERROR: matplotlib not installed. Run: pip install matplotlib numpy scipy")
    sys.exit(1)

try:
    from scipy.optimize import curve_fit
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    print("WARN: scipy not installed — Amdahl fit will be skipped. Run: pip install scipy")


def amdahl(N, f):
    """Amdahl's law: S(N) = 1 / (f + (1 - f) / N)."""
    return 1.0 / (f + (1.0 - f) / N)


def load_results(path):
    with open(path) as fp:
        data = json.load(fp)
    # Back-compat: an older format stored {"times": {"1": t1, "2": t2, ...}}.
    # Normalise it to the list-of-records schema the rest of the script expects.
    if isinstance(data, dict) and "times" in data:
        data = [
            {"n_workers": int(n), "t_total": t, "t_map": 0.0, "t_reduce": 0.0}
            for n, t in data["times"].items()
        ]
    # Filter out failed runs
    valid = [r for r in data if r.get("t_total") is not None]
    if not valid:
        print("ERROR: no valid timing results in file.")
        sys.exit(1)
    # Ensure phase fields always exist so the breakdown plot never crashes.
    for r in valid:
        r.setdefault("t_map", 0.0)
        r.setdefault("t_reduce", 0.0)
    return sorted(valid, key=lambda r: r["n_workers"])


def compute_speedup(results):
    t1 = next((r["t_total"] for r in results if r["n_workers"] == 1), None)
    if t1 is None:
        t1 = results[0]["t_total"]
        print(f"WARN: no N=1 run found; using N={results[0]['n_workers']} as baseline.")
    ns      = np.array([r["n_workers"] for r in results], dtype=float)
    totals  = np.array([r["t_total"]   for r in results], dtype=float)
    t_maps  = np.array([r["t_map"]     for r in results], dtype=float)
    t_reds  = np.array([r["t_reduce"]  for r in results], dtype=float)
    speedup = t1 / totals
    # Adjusted speedup: exclude download time from wall clock for fair comparison
    has_adj = all(r.get("t_map_adj") is not None for r in results)
    if has_adj:
        t_maps_adj = np.array([r["t_map_adj"] for r in results], dtype=float)
        t_dl_max   = np.array([r.get("t_download_max", 0.0) for r in results], dtype=float)
        totals_adj = totals - t_dl_max
        t1_adj     = totals_adj[np.argmax(ns == ns.min())]  # baseline = smallest N
        speedup_adj = t1_adj / totals_adj
    else:
        speedup_adj = t_maps_adj = None
    return ns, speedup, speedup_adj, t_maps, t_reds, t1


def fit_amdahl(ns, speedup):
    if not SCIPY_AVAILABLE:
        return None, None
    try:
        popt, _ = curve_fit(amdahl, ns, speedup, p0=[0.1], bounds=(0, 1))
        f = popt[0]
        return f, amdahl
    except Exception as e:
        print(f"WARN: curve_fit failed: {e}")
        return None, None


def plot(ns, speedup, speedup_adj, t_maps, t_reds, f_serial, output_path):
    fig = plt.figure(figsize=(13, 5))
    gs  = gridspec.GridSpec(1, 2, figure=fig, wspace=0.38)

    # ── Left: Speedup curve ────────────────────────────────────────────
    ax1 = fig.add_subplot(gs[0])

    ax1.plot(ns, speedup, "o-", color="#2196F3", linewidth=2, markersize=7,
             label="Empirical speedup", zorder=3)

    if speedup_adj is not None:
        ax1.plot(ns, speedup_adj, "s--", color="#4CAF50", linewidth=2, markersize=7,
                 label="Speedup (excl. download)", zorder=3)

    # Cap ideal-linear line at the empirical max + 20% so it doesn't dwarf the data
    s_emp_max = speedup.max()
    y_top = s_emp_max * 1.35
    n_dense = np.linspace(ns.min(), ns.max(), 300)
    ideal = np.minimum(n_dense, y_top)
    ax1.plot(n_dense, ideal, "--", color="#9E9E9E", linewidth=1.2, label="Ideal linear")

    if f_serial is not None:
        s_max = 1.0 / f_serial if f_serial > 1e-4 else None
        ax1.plot(
            n_dense,
            amdahl(n_dense, f_serial),
            "-",
            color="#FF5722",
            linewidth=2,
            label=f"Amdahl fit  (f = {f_serial:.3f})",
        )
        # Only annotate if theoretical max is within a sensible range
        if s_max is not None and s_max < y_top * 10:
            ax1.axhline(s_max, color="#FF5722", linestyle=":", linewidth=1, alpha=0.6)
            ax1.text(
                ns[len(ns) // 2], min(s_max * 1.04, y_top * 0.95),
                f"max ≈ {s_max:.1f}×",
                color="#FF5722", fontsize=9,
            )
        elif s_max is None:
            ax1.text(
                ns[0], y_top * 0.92,
                "f ≈ 0 (near-perfect scaling)",
                color="#FF5722", fontsize=9,
            )

    # Annotate each empirical point with its speedup value
    for n, s in zip(ns, speedup):
        ax1.annotate(
            f"{s:.2f}×",
            xy=(n, s),
            xytext=(4, 8),
            textcoords="offset points",
            fontsize=8.5,
            color="#1565C0",
        )

    ax1.set_xscale("log", base=2)
    ax1.set_xticks(ns.astype(int))
    ax1.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax1.set_ylim(0, y_top)
    ax1.set_xlabel("Number of workers (N)  [log₂ scale]", fontsize=11)
    ax1.set_ylabel("Speedup  S(N) = T(1) / T(N)", fontsize=11)
    ax1.set_title("Amdahl's Law — MapReduce Speedup", fontsize=12, fontweight="bold")
    ax1.legend(fontsize=9)
    ax1.grid(True, alpha=0.3, which="both")

    # ── Right: Phase breakdown (stacked bar) ────────────────────────────────
    ax2 = fig.add_subplot(gs[1])

    bar_w = 0.55
    x_pos = np.arange(len(ns))
    bars_map = ax2.bar(x_pos, t_maps, bar_w, label="MAP", color="#42A5F5")
    bars_red = ax2.bar(x_pos, t_reds, bar_w, bottom=t_maps, label="REDUCE", color="#EF5350")

    # Label each bar segment with its duration
    for i, (tm, tr) in enumerate(zip(t_maps, t_reds)):
        if tm > 2:
            ax2.text(i, tm / 2,       f"{tm:.0f}s", ha="center", va="center", fontsize=8, color="white", fontweight="bold")
        if tr > 2:
            ax2.text(i, tm + tr / 2,  f"{tr:.0f}s", ha="center", va="center", fontsize=8, color="white", fontweight="bold")

    ax2.set_xticks(x_pos)
    ax2.set_xticklabels([f"N={int(n)}" for n in ns], fontsize=9)
    ax2.set_ylabel("Wall-clock time (seconds)", fontsize=11)
    ax2.set_title("Phase Breakdown per Worker Count", fontsize=12, fontweight="bold")
    ax2.legend(fontsize=9)
    ax2.grid(True, axis="y", alpha=0.3)

    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Saved: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Plot Amdahl's law from MapReduce timing results")
    parser.add_argument("--input",  default="amdahl_results.json")
    parser.add_argument("--output", default="amdahl_speedup.png")
    args = parser.parse_args()

    results  = load_results(args.input)
    ns, speedup, speedup_adj, t_maps, t_reds, t1 = compute_speedup(results)

    print(f"Baseline T(1) = {t1:.1f}s")
    print(f"{'N':>5}  {'T(N)':>8}  {'S(N)':>7}  {'S(N)adj':>9}  {'T_map':>8}  {'T_red':>8}")
    for r, s, sa in zip(results, speedup, speedup_adj if speedup_adj is not None else speedup):
        sadj_str = f"{sa:>9.3f}" if speedup_adj is not None else "        -"
        print(f"{r['n_workers']:>5}  {r['t_total']:>8.1f}  {s:>7.3f}  {sadj_str}  {r['t_map']:>8.1f}  {r['t_reduce']:>8.1f}")

    f_serial, _ = fit_amdahl(ns, speedup)
    if f_serial is not None:
        print(f"\nAmdahl serial fraction f = {f_serial:.4f}")
        print(f"Theoretical max speedup   = {1/f_serial:.1f}\u00d7")

    plot(ns, speedup, speedup_adj, t_maps, t_reds, f_serial, args.output)


if __name__ == "__main__":
    main()
