#!/usr/bin/env python3
"""
ssh_parallelism_sweep.py — Find optimal MAX_PARALLEL_SSH for the cluster.

Runs the MapReduce pipeline with a fixed worker count (N) but varies the
--max-ssh cap from 1 up to N, measuring t_shuffle at each level.

Run ON a lab machine:
    python3 ssh_parallelism_sweep.py [--workers 8] [--splits 8] [--reducers 4]
                                     [--machines machines.txt] [--port 59100]
                                     [--ssh-levels 1,2,4,8,16,32]
                                     [--output ssh_sweep.json]
"""

import argparse
import json
import os
import socket
import subprocess
import sys
import time

# Reuse helpers from amdahl_bench
sys.path.insert(0, os.path.dirname(__file__))
from amdahl_bench import (
    SSH_OPTS, INPUT_DIR, LOCAL_MAP_DIR,
    load_machines, get_master_host,
    run_master, wait_for_master, collect_master,
    collect_worker_timings, kill_workers,
    start_workers, log,
)

OUTPUT_DIR = os.path.expanduser("~/slr207-group1-commoncrawl/output-sweep")
DEFAULT_PORT = 59100


def sweep_run(n_workers, machines, port, n_splits, n_reducers, max_ssh):
    selected = machines[:n_workers]
    master_host = get_master_host()

    log(f"  max_ssh={max_ssh} | N={n_workers} | machines={selected}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for f in os.listdir(OUTPUT_DIR):
        try:
            os.remove(os.path.join(OUTPUT_DIR, f))
        except OSError:
            pass

    master_proc = run_master(port, n_splits, n_reducers)
    if not wait_for_master("localhost", port):
        log("  ERROR: master port never opened")
        master_proc.kill()
        return None

    start_workers(selected, port, INPUT_DIR, OUTPUT_DIR, LOCAL_MAP_DIR,
                  master_host, max_ssh=max_ssh)
    timing = collect_master(master_proc, port)
    worker_timing = collect_worker_timings(selected, port)
    kill_workers(selected, port)

    if not timing:
        return None

    return {
        "max_ssh":    max_ssh,
        "n_workers":  n_workers,
        "n_splits":   n_splits,
        "n_reducers": n_reducers,
        "t_total":    timing["t_total"],
        "t_map":      timing["t_map"],
        "t_reduce":   timing["t_reduce"],
        "t_shuffle":  worker_timing["t_shuffle"],
        "t_compute":  worker_timing["t_compute"],
        "t_io_write": worker_timing["t_io_write"],
    }


def main():
    parser = argparse.ArgumentParser(description="Sweep MAX_PARALLEL_SSH to find optimal level")
    parser.add_argument("--workers",    type=int, default=8,           help="Fixed worker count (default: 8)")
    parser.add_argument("--splits",     type=int, default=8,           help="Number of input splits (default: 8)")
    parser.add_argument("--reducers",   type=int, default=4,           help="Number of reducers (default: 4)")
    parser.add_argument("--machines",   default="machines.txt")
    parser.add_argument("--port",       type=int, default=DEFAULT_PORT)
    parser.add_argument("--ssh-levels", default="1,2,4,8,16,32",      help="Comma-separated max_ssh values to test")
    parser.add_argument("--output",     default="ssh_sweep.json")
    args = parser.parse_args()

    ssh_levels = [int(x) for x in args.ssh_levels.split(",")]
    all_machines = load_machines(args.machines)

    if len(all_machines) < args.workers:
        print(f"ERROR: need {args.workers} machines, only {len(all_machines)} available")
        sys.exit(1)

    log(f"SSH parallelism sweep: N={args.workers}, splits={args.splits}, reducers={args.reducers}")
    log(f"Testing max_ssh levels: {ssh_levels}")
    log("")

    results = []
    for level in ssh_levels:
        # Cap level at n_workers (no point having more threads than workers)
        effective = min(level, args.workers)
        r = sweep_run(args.workers, all_machines, args.port,
                      args.splits, args.reducers, effective)
        if r:
            results.append(r)
            log(f"  max_ssh={effective:>2} → t_total={r['t_total']:.1f}s  "
                f"t_shuffle={r['t_shuffle']:.2f}s  t_reduce={r['t_reduce']:.1f}s")
        else:
            log(f"  max_ssh={effective:>2} → FAILED")

        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        log("")

    # Print summary table
    if results:
        log("=== SUMMARY ===")
        log(f"{'max_ssh':>8}  {'t_total':>8}  {'t_shuffle':>10}  {'t_reduce':>9}")
        for r in results:
            log(f"{r['max_ssh']:>8}  {r['t_total']:>8.1f}  {r['t_shuffle']:>10.3f}  {r['t_reduce']:>9.1f}")
        best = min(results, key=lambda r: r["t_total"])
        log(f"\nOptimal max_ssh = {best['max_ssh']} (t_total={best['t_total']:.1f}s)")

    log(f"\nResults written to {args.output}")


if __name__ == "__main__":
    main()
