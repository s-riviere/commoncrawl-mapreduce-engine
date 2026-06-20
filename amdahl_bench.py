#!/usr/bin/env python3
"""
amdahl_bench.py — Empirical Amdahl's law benchmark for the MapReduce cluster.

Run this script ON a lab machine (e.g. tp-1a201-02.enst.fr) so that:
  - The master binds to a real cluster IP that workers can reach.
  - Workers can SSH back to each other for the REDUCE shuffle phase.

Usage (from ~/proj):
    python3 amdahl_bench.py [--machines machines.txt] [--splits 32] \
                            [--reducers 8] [--port 59000] [--output amdahl_results.json]

After completion, copy amdahl_results.json to your local machine and run plot_amdahl.py.
"""

import argparse
import json
import os
import re
import socket
import subprocess
import sys
import time


# ── Defaults ────────────────────────────────────────────────────────────────
DEFAULT_MACHINES_FILE = "machines.txt"
DEFAULT_SPLITS        = 32
DEFAULT_REDUCERS      = 8
DEFAULT_PORT          = 59000
DEFAULT_OUTPUT        = "amdahl_results.json"
WORKER_COUNTS         = [1, 2, 4, 8, 16, 32]

# Directories that must already exist on NFS (pre-populated by deploy_commoncrawl.sh)
NFS_DIR       = os.path.expanduser("~/slr207-group1-commoncrawl")
INPUT_DIR     = f"{NFS_DIR}/input"
OUTPUT_DIR    = f"{NFS_DIR}/output"
LOCAL_MAP_DIR = f"/tmp/slr207-group1-commoncrawl-{os.getenv('USER', 'cdaou-25')}/map-outputs"

SSH_OPTS = (
    "-o StrictHostKeyChecking=no "
    "-o BatchMode=yes "
    "-o LogLevel=ERROR "
    "-o ConnectTimeout=5"
)

TIMING_RE = re.compile(r"TIMING:\s*(\{.*\})")
WORKER_TIMING_RE = re.compile(r"WORKER_TIMING:\s*(\{.*\})")


def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] [BENCH] {msg}", flush=True)


def load_machines(path):
    with open(path) as f:
        machines = [l.strip() for l in f if l.strip() and not l.startswith("#")]
    # Exclude localhost — workers need real hostnames for SSH shuffle
    machines = [m for m in machines if m != "localhost"]
    return machines


def get_master_host():
    """Return this machine's hostname as seen on the cluster network."""
    return socket.gethostname()


def start_workers(machines, port, input_dir, output_dir, local_map_dir, master_host, max_ssh=None):
    """SSH-launch one worker process per machine; return list of Popen handles."""
    max_ssh_arg = f"--max-ssh {max_ssh}" if max_ssh is not None else ""
    procs = []
    for host in machines:
        cmd = (
            f"ssh {SSH_OPTS} {host} "
            f"'cd ~/proj && nohup python3 src/map_reduce/worker.py "
            f"-h {master_host} -p {port} "
            f"-i {input_dir} -o {output_dir} -l {local_map_dir} "
            f"{max_ssh_arg} "
            f">/tmp/worker_{port}.log 2>&1 &'"
        )
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append((host, proc))
        log(f"  SSH launch → {host}")
    # Wait for all SSH launches to complete
    for host, proc in procs:
        proc.wait()
        log(f"  SSH done   → {host}")
    # Give workers a moment to connect to master
    time.sleep(2)
    return procs


def kill_workers(machines, port):
    """Kill all worker processes on the given machines."""
    for host in machines:
        subprocess.Popen(
            f"ssh {SSH_OPTS} {host} 'pkill -f \"worker.py.*-p {port}\"' 2>/dev/null",
            shell=True,
        )
    time.sleep(1)


def wait_for_master(host, port, timeout=15):
    """Poll until master TCP port is accepting connections."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.3)
    return False


def run_master(port, n_splits, n_reducers, timeout=600):
    """
    Start master as a subprocess, stream its output live, wait for it to finish.
    Returns the parsed TIMING dict, or None on failure.
    """
    cmd = [
        sys.executable, "-u",
        "src/map_reduce/master.py",
        "-p", str(port),
        "-s", str(n_splits),
        "-r", str(n_reducers),
    ]
    log(f"  Starting master: {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )
    return proc


def collect_master(proc, port, timeout=600):
    """Stream master output, parse TIMING line, return timing dict or None."""
    timing = None
    deadline = time.time() + timeout

    for line in proc.stdout:
        line = line.rstrip()
        print(f"  [MASTER] {line}", flush=True)
        m = TIMING_RE.search(line)
        if m:
            try:
                timing = json.loads(m.group(1))
            except json.JSONDecodeError:
                pass
        if time.time() > deadline:
            log("  WARN: master timed out — killing")
            proc.kill()
            break

    proc.wait()

    if timing is None:
        log("  ERROR: no TIMING line found in master output.")
    return timing


def collect_worker_timings(machines, port):
    """SSH-read worker log files and aggregate WORKER_TIMING entries."""
    aggregated = {"t_clean": 0.0, "t_io_read": 0.0, "t_compute": 0.0,
                  "t_io_write": 0.0, "t_shuffle": 0.0}
    for host in machines:
        cmd = f"ssh {SSH_OPTS} {host} 'cat /tmp/worker_{port}.log 2>/dev/null'"
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
            for line in result.stdout.splitlines():
                m = WORKER_TIMING_RE.search(line)
                if m:
                    d = json.loads(m.group(1))
                    for key in aggregated:
                        aggregated[key] += d.get(key, 0.0)
        except Exception as e:
            log(f"  WARN: could not read worker log from {host}: {e}")
    return aggregated


def bench_run(n_workers, machines, port, n_splits, n_reducers, output_dir, max_ssh=8):
    """Single benchmark run with n_workers workers."""
    selected = machines[:n_workers]
    master_host = get_master_host()

    log(f"Run N={n_workers} | machines={selected} | master={master_host}")

    # Clean output dir so results don't accumulate across runs
    os.makedirs(output_dir, exist_ok=True)
    for f in os.listdir(output_dir):
        try:
            os.remove(os.path.join(output_dir, f))
        except OSError:
            pass

    # Start master first, wait until its port is open, then launch workers
    master_proc = run_master(port, n_splits, n_reducers)
    if not wait_for_master("localhost", port):
        log("  ERROR: master port never opened — killing")
        master_proc.kill()
        return None
    log(f"  Master ready on port {port} — launching {n_workers} worker(s)")
    start_workers(selected, port, INPUT_DIR, output_dir, LOCAL_MAP_DIR, master_host, max_ssh=max_ssh)
    timing = collect_master(master_proc, port)
    worker_timing = collect_worker_timings(selected, port)
    kill_workers(selected, port)

    if timing:
        log(f"  N={n_workers} → total={timing['t_total']:.1f}s  map={timing['t_map']:.1f}s  reduce={timing['t_reduce']:.1f}s")
        log(f"           worker breakdown → "
            f"clean={worker_timing['t_clean']:.1f}s  "
            f"io_read={worker_timing['t_io_read']:.1f}s  "
            f"compute={worker_timing['t_compute']:.1f}s  "
            f"shuffle={worker_timing['t_shuffle']:.1f}s  "
            f"io_write={worker_timing['t_io_write']:.1f}s")
    else:
        log(f"  N={n_workers} → FAILED")

    return timing, worker_timing

def main():
    parser = argparse.ArgumentParser(description="Amdahl's law benchmark for MapReduce cluster")
    parser.add_argument("--machines", default=DEFAULT_MACHINES_FILE)
    parser.add_argument("--splits",   type=int, default=DEFAULT_SPLITS)
    parser.add_argument("--reducers", type=int, default=DEFAULT_REDUCERS)
    parser.add_argument("--port",     type=int, default=DEFAULT_PORT)
    parser.add_argument("--output",   default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--counts",
        default=",".join(map(str, WORKER_COUNTS)),
        help="Comma-separated list of worker counts (default: 1,2,4,8,16,32)",
    )
    parser.add_argument(
        "--max-ssh",
        type=int,
        default=None,
        help="Max parallel SSH connections per reducer during shuffle (default: auto = min(n_workers, 8))",
    )
    args = parser.parse_args()

    worker_counts = [int(x) for x in args.counts.split(",")]

    all_machines = load_machines(args.machines)
    max_needed = max(worker_counts)
    if len(all_machines) < max_needed:
        log(f"WARN: only {len(all_machines)} machines available; max N capped at {len(all_machines)}")
        worker_counts = [n for n in worker_counts if n <= len(all_machines)]

    log(f"Config: splits={args.splits}, reducers={args.reducers}, port={args.port}")
    log(f"Worker counts to test: {worker_counts}")
    log(f"Machines file: {args.machines} ({len(all_machines)} available)")
    log("")

    results = []
    for n in worker_counts:
        timing, worker_timing = bench_run(n, all_machines, args.port, args.splits, args.reducers, OUTPUT_DIR,
                                          max_ssh=args.max_ssh)
        record = {
            "n_workers":  n,
            "n_splits":   args.splits,
            "n_reducers": args.reducers,
            "t_total":    timing["t_total"]   if timing else None,
            "t_map":      timing["t_map"]     if timing else None,
            "t_reduce":   timing["t_reduce"]  if timing else None,
            "t_clean":    worker_timing["t_clean"],
            "t_io_read":  worker_timing["t_io_read"],
            "t_compute":  worker_timing["t_compute"],
            "t_shuffle":  worker_timing["t_shuffle"],
            "t_io_write": worker_timing["t_io_write"],
        }
        results.append(record)
        # Save incrementally so partial results survive a crash
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        log(f"  Saved to {args.output}")
        log("")

    log("=== DONE ===")
    log(f"Results written to {args.output}")


if __name__ == "__main__":
    main()
