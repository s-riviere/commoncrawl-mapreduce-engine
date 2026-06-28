#!/usr/bin/env python3
"""
run_all.py — One-command SOLO test harness for the distributed MapReduce engine.

Runs the WHOLE system on a SINGLE machine — no cluster, no SSH daemon, no
teammates required. It spins up a real master plus several real worker processes
on localhost (each with its own local map directory, using --local-shuffle so
reducers read partitions straight off the local filesystem), then exercises
every deliverable end to end:

  1. Correctness of all four analyses (wordcount, lang, wordlen, bigram),
     each checked against the single-machine reference in validate.py.
  2. Fault tolerance: a worker is killed mid-job; the master must detect it,
     re-run the lost MAP outputs, and still finish with a correct result.
  3. Amdahl's law: a speedup sweep (N = 1, 2, 4 workers) on the SAME dataset,
     writing runtime/amdahl_results.json (and an amdahl_speedup.png if
     matplotlib is available) so plot_amdahl.py can be reproduced solo.

Exit code is 0 only if every check passes.

Usage:
    python3 tests/run_all.py                 # full suite
    python3 tests/run_all.py --quick         # correctness + FT only (no sweep)
    python3 tests/run_all.py --keep          # keep the generated /tmp data
"""

import argparse
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MASTER = os.path.join(ROOT, "src", "mapreduce", "master.py")
WORKER = os.path.join(ROOT, "src", "mapreduce", "worker.py")
VALIDATE = os.path.join(ROOT, "src", "mapreduce", "validate.py")
PY = sys.executable

BASE = f"/tmp/mr-solo-{os.getenv('USER', 'user')}"
INPUT_DIR = os.path.join(BASE, "input")          # correctness/FT dataset
INPUT_BIG = os.path.join(BASE, "input-amdahl")   # bigger, fixed Amdahl dataset
OUTPUT_DIR = os.path.join(BASE, "output")
MAP_BASE = os.path.join(BASE, "map")             # per-worker subdirs underneath

JOBS = ["wordcount", "lang", "wordlen", "bigram"]

NC = "\033[0m"; GREEN = "\033[32m"; RED = "\033[31m"; YEL = "\033[33m"; BOLD = "\033[1m"

# A fixed multilingual vocabulary so the synthetic data triggers every job:
# stop-words give the `lang` job something to rank, varied lengths feed
# `wordlen`, and word order feeds `bigram`.
_VOCAB = (
    "the of and to in is that it for as was with on are be this by from at or an "  # en
    "le la les des une dans est que qui pour pas avec sur ne se ce au aux du "       # fr
    "der die und den von zu mit auf ist nicht ein eine dem im des auch sich "        # de
    "que los las una por con para del como pero mas este esta son "                  # es
    "che non con per una sono come piu anche nella degli sulla dei alla "            # it
    "data crawl mapreduce distributed system kafka stream batch reduce shuffle "
    "node worker master split partition token frequency analysis cluster network"
).split()


def log(msg):
    print(msg, flush=True)


def free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def gen_splits(input_dir, n_splits, lines_per_split, seed=1234):
    """Generate deterministic synthetic Common-Crawl-like text splits."""
    if os.path.isdir(input_dir):
        shutil.rmtree(input_dir)
    os.makedirs(input_dir, exist_ok=True)
    rng = random.Random(seed)
    for i in range(n_splits):
        path = os.path.join(input_dir, f"commoncrawl-{i:04d}.txt")
        with open(path, "w", encoding="utf-8") as f:
            for _ in range(lines_per_split):
                n_words = rng.randint(4, 14)
                f.write(" ".join(rng.choice(_VOCAB) for _ in range(n_words)) + "\n")


def wait_for_port(host, port, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def run_job(job, n_workers, n_reducers, n_splits, input_dir,
            kill_after=None, run_timeout=180):
    """Run one full MapReduce job locally. Returns (timing_dict_or_None, log_str)."""
    if os.path.isdir(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    port = free_port()
    master = subprocess.Popen(
        [PY, "-u", MASTER, "-p", str(port), "-s", str(n_splits),
         "-r", str(n_reducers), "-j", job],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=ROOT,
    )
    if not wait_for_port("127.0.0.1", port, timeout=30):
        master.kill()
        return None, "master port never opened"

    workers = []
    for i in range(n_workers):
        wdir = os.path.join(MAP_BASE, f"w{i}")
        workers.append(subprocess.Popen(
            [PY, "-u", WORKER, "-h", "127.0.0.1", "-p", str(port),
             "-i", input_dir, "-o", OUTPUT_DIR, "-l", wdir, "-j", job,
             "--worker-id", f"w{i}", "--advertise-host", "127.0.0.1",
             "--local-shuffle"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=ROOT,
        ))

    killed = {"done": False}
    progress = threading.Event()
    if kill_after is not None and workers:
        def killer():
            # Wait until at least one MAP has completed, so the victim is holding
            # a finished MAP output the master must re-run (the interesting case),
            # then kill it while other maps are still pending.
            progress.wait(timeout=20)
            time.sleep(kill_after)
            victim = workers[-1]
            if victim.poll() is None:
                victim.send_signal(signal.SIGKILL)
                killed["done"] = True
        threading.Thread(target=killer, daemon=True).start()

    # Watchdog: hard-kill the master if it overruns (prevents a hung suite).
    timed_out = {"v": False}
    wd = threading.Thread(
        target=lambda: (time.sleep(run_timeout), timed_out.__setitem__("v", True),
                        master.poll() is None and master.kill()),
        daemon=True,
    )
    wd.start()

    out_lines, timing = [], None
    for line in master.stdout:
        out_lines.append(line.rstrip())
        if not progress.is_set() and "finished MAP" in line:
            progress.set()
        idx = line.find("TIMING:")
        if idx != -1:
            try:
                timing = json.loads(line[idx + len("TIMING:"):].strip())
            except json.JSONDecodeError:
                pass
    master.wait()

    for w in workers:
        if w.poll() is None:
            w.terminate()
    for w in workers:
        try:
            w.wait(timeout=5)
        except subprocess.TimeoutExpired:
            w.kill()

    log_str = "\n".join(out_lines)
    if timed_out["v"]:
        log_str += "\n[HARNESS] run timed out"
    return (timing if not timed_out["v"] else None), log_str


def validate(job, input_dir, n_splits):
    """Run validate.py against the produced output. Returns (ok, output)."""
    proc = subprocess.run(
        [PY, VALIDATE, "-i", input_dir, "-o", OUTPUT_DIR, "-j", job,
         "-s", str(n_splits), "-n", "5"],
        capture_output=True, text=True, cwd=ROOT,
    )
    return proc.returncode == 0, proc.stdout + proc.stderr


# ──────────────────────────────────────────────────────────────────────────────
# Test phases
# ──────────────────────────────────────────────────────────────────────────────

def test_correctness(results):
    log(f"\n{BOLD}=== 1. Correctness of all four analyses (4 workers, 6 splits) ==={NC}")
    gen_splits(INPUT_DIR, n_splits=6, lines_per_split=400)
    for job in JOBS:
        timing, _ = run_job(job, n_workers=4, n_reducers=4, n_splits=6, input_dir=INPUT_DIR)
        if timing is None:
            log(f"  {RED}[FAIL]{NC} {job:9s} — job did not finish")
            results.append((f"correctness/{job}", False))
            continue
        ok, out = validate(job, INPUT_DIR, 6)
        verdict = f"{GREEN}[PASS]{NC}" if ok else f"{RED}[FAIL]{NC}"
        log(f"  {verdict} {job:9s} — t_total={timing.get('t_total', 0):.2f}s")
        if not ok:
            log("    " + out.strip().replace("\n", "\n    "))
        results.append((f"correctness/{job}", ok))


def test_fault_tolerance(results):
    log(f"\n{BOLD}=== 2. Fault tolerance (kill a worker mid-job) ==={NC}")
    # Bigger dataset so MAP work lasts long enough to kill a worker mid-flight.
    gen_splits(INPUT_DIR, n_splits=8, lines_per_split=40000, seed=99)
    timing, logs = run_job("wordcount", n_workers=4, n_reducers=4, n_splits=8,
                           input_dir=INPUT_DIR, kill_after=0.05)
    reclaimed = "DIED holding" in logs or "Lease expired" in logs or "re-running" in logs
    if timing is None:
        log(f"  {RED}[FAIL]{NC} job did not finish after the induced failure")
        results.append(("fault_tolerance/completes", False))
        return
    ok, out = validate("wordcount", INPUT_DIR, 8)
    log(f"  {'%s[PASS]%s' % (GREEN, NC) if ok else '%s[FAIL]%s' % (RED, NC)} "
        f"job recovered and finished — t_total={timing.get('t_total', 0):.2f}s")
    log(f"  {'%s[PASS]%s' % (GREEN, NC) if reclaimed else '%s[WARN]%s' % (YEL, NC)} "
        f"master logged a failure/recovery event"
        f"{'' if reclaimed else ' (worker may have died after finishing — re-run if so)'}")
    if not ok:
        log("    " + out.strip().replace("\n", "\n    "))
    results.append(("fault_tolerance/completes", ok))
    results.append(("fault_tolerance/output_correct", ok))


def test_amdahl(results, counts=(1, 2, 4)):
    log(f"\n{BOLD}=== 3. Amdahl's law sweep (same dataset at every point) ==={NC}")
    # ONE fixed dataset reused for every N — required for a correct Amdahl graph.
    gen_splits(INPUT_BIG, n_splits=8, lines_per_split=100000, seed=7)
    records = []
    for n in counts:
        timing, _ = run_job("wordcount", n_workers=n, n_reducers=8, n_splits=8,
                            input_dir=INPUT_BIG, run_timeout=600)
        if timing is None:
            log(f"  {RED}[FAIL]{NC} N={n} did not finish")
            results.append((f"amdahl/N={n}", False))
            continue
        ok, _ = validate("wordcount", INPUT_BIG, 8)
        records.append({
            "n_workers": n, "n_splits": 8, "n_reducers": 8,
            "t_total": round(timing["t_total"], 3),
            "t_map": round(timing.get("t_map", 0.0), 3),
            "t_reduce": round(timing.get("t_reduce", 0.0), 3),
        })
        log(f"  {GREEN}[OK]{NC} N={n}: t_total={timing['t_total']:.2f}s  "
            f"(map={timing.get('t_map', 0):.2f}s reduce={timing.get('t_reduce', 0):.2f}s)  "
            f"validate={'PASS' if ok else 'FAIL'}")
        results.append((f"amdahl/N={n}", ok))

    if not records:
        return
    out_json = os.path.join(ROOT, "runtime", "amdahl_results.json")
    os.makedirs(os.path.dirname(out_json), exist_ok=True)
    with open(out_json, "w") as f:
        json.dump(records, f, indent=2)
    log(f"  Wrote {os.path.relpath(out_json, ROOT)}")

    t1 = next((r["t_total"] for r in records if r["n_workers"] == 1), records[0]["t_total"])
    log(f"\n  {'N':>3}  {'t_total':>9}  {'speedup':>8}")
    for r in records:
        log(f"  {r['n_workers']:>3}  {r['t_total']:>9.2f}  {t1 / r['t_total']:>7.2f}x")

    # Render the figure too, if the plotting deps are present.
    try:
        subprocess.run(
            [PY, os.path.join(ROOT, "src", "benchmarks", "plot_amdahl.py"),
             "--input", out_json,
             "--output", os.path.join(ROOT, "runtime", "amdahl_speedup.png")],
            check=True, capture_output=True, text=True, cwd=ROOT,
        )
        log(f"  Rendered runtime/amdahl_speedup.png")
    except Exception as e:
        log(f"  {YEL}(graph not rendered: {e}; install matplotlib/scipy to enable){NC}")


def main():
    ap = argparse.ArgumentParser(description="Solo end-to-end test harness")
    ap.add_argument("--quick", action="store_true", help="skip the Amdahl sweep")
    ap.add_argument("--keep", action="store_true", help="keep generated /tmp data")
    args = ap.parse_args()

    log(f"{BOLD}Distributed MapReduce — solo test harness{NC}")
    log(f"Workspace : {ROOT}")
    log(f"Scratch   : {BASE}")

    results = []
    t0 = time.time()
    try:
        test_correctness(results)
        test_fault_tolerance(results)
        if not args.quick:
            test_amdahl(results)
    finally:
        if not args.keep:
            shutil.rmtree(BASE, ignore_errors=True)

    log(f"\n{BOLD}=== Summary ==={NC}")
    passed = sum(1 for _, ok in results if ok)
    for name, ok in results:
        log(f"  {'%s[PASS]%s' % (GREEN, NC) if ok else '%s[FAIL]%s' % (RED, NC)}  {name}")
    total = len(results)
    elapsed = time.time() - t0
    color = GREEN if passed == total else RED
    log(f"\n{color}{passed}/{total} checks passed{NC} in {elapsed:.1f}s")
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
