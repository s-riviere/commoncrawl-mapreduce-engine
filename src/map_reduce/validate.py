#!/usr/bin/env python3

# ==============================================================================
# DESCRIPTION
# ==============================================================================
# Single-machine reference checker for the distributed MapReduce output.
#
# It recomputes the chosen analysis on ONE machine over the SAME input splits,
# then compares the result against the aggregated distributed output (the
# part-*.txt files produced by the reducers). This is the sanity check asked for
# in the evaluation:
#   "run a simple single-machine word count on the same small dataset and
#    confirm it gives exactly the same counts."
#
# Checks performed:
#   1. Number of distinct keys   (reference vs distributed)
#   2. Sum of all counts         (reference vs distributed)
#   3. Per-key equality          (every key has the same count on both sides)
#
# Arguments:
#   -i, --input-dir   : Directory holding commoncrawl-*.txt splits.
#   -o, --output-dir  : Directory holding the reducers' part-*.txt files.
#   -j, --job         : wordcount | lang | wordlen | bigram (must match the run).
#   -n, --top         : How many top entries to print (default: 10).
#   -s, --splits      : Only validate splits [0, N) (default: all present).
#
# Usage:
#   python3 src/map_reduce/validate.py -i ~/slr207.../input -o ~/slr207.../output
#   python3 src/map_reduce/validate.py -i input -o output -j lang -n 5
# ==============================================================================

import argparse
import glob
import os
import re
import sys

# Reuse the EXACT tokenisation / keying logic the workers use, so the reference
# computation cannot drift from the distributed one.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from worker import MapReduceWorker  # noqa: E402

SPLIT_RE = re.compile(r"commoncrawl-(\d{4})\.txt$")


def reference_counts(input_dir, job, max_splits=None):
    """Compute the analysis locally over the input splits → {key(str): count}."""
    counts: dict[str, int] = {}
    files = sorted(glob.glob(os.path.join(input_dir, "commoncrawl-*.txt")))
    used = 0
    for path in files:
        m = SPLIT_RE.search(os.path.basename(path))
        if m and max_splits is not None and int(m.group(1)) >= max_splits:
            continue
        with open(path, "rb") as f:
            raw = f.read()
        _, partial = MapReduceWorker._map_emit(job, raw)
        for key_b, c in partial.items():
            k = key_b.decode("utf-8", "ignore")
            counts[k] = counts.get(k, 0) + c
        used += 1
    return counts, used


def distributed_counts(output_dir):
    """Aggregate the reducers' part-*.txt files → {key(str): count}."""
    counts: dict[str, int] = {}
    files = sorted(glob.glob(os.path.join(output_dir, "part-*.txt")))
    for path in files:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                key, _, val = line.rpartition("\t")
                if not val:
                    continue
                counts[key] = counts.get(key, 0) + int(val)
    return counts, len(files)


def main():
    parser = argparse.ArgumentParser(description="Validate distributed MapReduce output against a single-machine reference.")
    parser.add_argument("-i", "--input-dir", required=True, help="Directory with commoncrawl-*.txt splits")
    parser.add_argument("-o", "--output-dir", required=True, help="Directory with reducers' part-*.txt output")
    parser.add_argument("-j", "--job", default="wordcount", choices=["wordcount", "lang", "wordlen", "bigram"])
    parser.add_argument("-n", "--top", type=int, default=10, help="Top-N entries to print")
    parser.add_argument("-s", "--splits", type=int, default=None, help="Validate only splits [0, N)")
    args = parser.parse_args()

    input_dir = os.path.expanduser(args.input_dir)
    output_dir = os.path.expanduser(args.output_dir)

    print(f"[VALIDATE] job={args.job}")
    print(f"[VALIDATE] reference  ← {input_dir}")
    print(f"[VALIDATE] distributed ← {output_dir}")

    ref, n_splits = reference_counts(input_dir, args.job, args.splits)
    dist, n_parts = distributed_counts(output_dir)

    if n_splits == 0:
        print("[VALIDATE] ERROR: no input splits found.")
        sys.exit(2)
    if n_parts == 0:
        print("[VALIDATE] ERROR: no part-*.txt found (did the job run / write here?).")
        sys.exit(2)

    ref_keys, dist_keys = len(ref), len(dist)
    ref_sum, dist_sum = sum(ref.values()), sum(dist.values())

    print(f"[VALIDATE] splits read = {n_splits} | reducer files = {n_parts}")
    print(f"[VALIDATE] distinct keys : reference={ref_keys:,}  distributed={dist_keys:,}")
    print(f"[VALIDATE] total counts  : reference={ref_sum:,}  distributed={dist_sum:,}")

    # Per-key diff
    mismatches = []
    for k, c in ref.items():
        if dist.get(k) != c:
            mismatches.append((k, c, dist.get(k)))
    extra = [k for k in dist if k not in ref]

    ok = (ref_keys == dist_keys) and (ref_sum == dist_sum) and not mismatches and not extra

    print("\n[VALIDATE] Top entries (reference):")
    for k, c in sorted(ref.items(), key=lambda x: x[1], reverse=True)[: args.top]:
        print(f"    {c:>10,}  {k}")

    if ok:
        print("\n[VALIDATE] ✅ PASS — distributed output matches the single-machine reference exactly.")
        sys.exit(0)

    print("\n[VALIDATE] ❌ FAIL — differences detected:")
    if ref_keys != dist_keys:
        print(f"    distinct-key count differs ({ref_keys} vs {dist_keys})")
    if ref_sum != dist_sum:
        print(f"    total-count sum differs ({ref_sum} vs {dist_sum})")
    for k, rc, dc in mismatches[:20]:
        print(f"    key={k!r}: reference={rc} distributed={dc}")
    for k in extra[:20]:
        print(f"    key={k!r}: present in distributed but not reference")
    print(f"    ... {len(mismatches)} mismatched key(s), {len(extra)} extra key(s) total")
    sys.exit(1)


if __name__ == "__main__":
    main()
