#!/bin/bash

# ==============================================================================
# DESCRIPTION
# ==============================================================================
# Real-cluster Amdahl's law sweep for the distributed MapReduce engine.
#
# For a FIXED dataset (same number of CommonCrawl splits at every point), it runs
# the full MapReduce job with a varying number of workers N in {1,2,4,8,16,...}
# and records the per-phase timings (t_total / t_map / t_reduce). The result is a
# JSON file consumable by plot_amdahl.py to draw the speedup curve.
#
# It REUSES the exact remote commands proven by demo_verified_run.sh (NFS upload,
# master bootstrap, worker bootstrap), but replaces the interactive `ssh -t`
# monitor with a NON-interactive poll (wait for the master PID to exit, then
# scrape the TIMING line). This makes it safe to run unattended in a loop.
#
# Amdahl validity: the dataset is identical for every N (same split files on NFS,
# downloaded once up front). Only the worker count changes.
#
# IMPORTANT: no command runs on the SSH gateway (ProxyJump). Every ssh/scp targets
# a tp-* node directly; the gateway is only a tunnel (configured in ~/.ssh/config).
#
# Options:
#   -h        : show this help
#   -s num    : number of CommonCrawl splits (fixed dataset)        (default: 16)
#   -r num    : number of reducers (fixed across all points)        (default: 8)
#   -p num    : base master port (each N uses base+index)           (default: 55000)
#   -c list   : comma-separated worker counts                       (default: 1,2,4,8,16)
#   -j job    : wordcount|lang|wordlen|bigram                       (default: wordcount)
#   -m file   : verified machine list                               (default: runtime/machines.verified.txt)
#   -o file   : output JSON                                         (default: runtime/amdahl_cluster.json)
#
# Usage:
#   bash src/benchmarks/amdahl_cluster_sweep.sh                         # 16 splits, N=1,2,4,8,16
#   bash src/benchmarks/amdahl_cluster_sweep.sh -s 32 -c 1,2,4,8,16,32
# ==============================================================================

NC="\e[0m"; GREEN="\e[32m"; YELLOW="\e[33m"; RED="\e[31m"; BLUE="\e[34m"

set -uo pipefail
cd "$(dirname "$0")/../.."

# ── Arguments ─────────────────────────────────────────────────────────────────
N_SPLITS="16"
N_REDUCERS="8"
BASE_PORT="55000"
COUNTS="1,2,4,8,16"
JOB="wordcount"
VERIFIED_FILE="runtime/machines.verified.txt"
OUTPUT="runtime/amdahl_cluster.json"

usage() {
    sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
    exit "${1:-1}"
}

while getopts "hs:r:p:c:j:m:o:" opt; do
    case "${opt}" in
        h) usage 0 ;;
        s) N_SPLITS="${OPTARG}" ;;
        r) N_REDUCERS="${OPTARG}" ;;
        p) BASE_PORT="${OPTARG}" ;;
        c) COUNTS="${OPTARG}" ;;
        j) JOB="${OPTARG}" ;;
        m) VERIFIED_FILE="${OPTARG}" ;;
        o) OUTPUT="${OPTARG}" ;;
        *) usage 1 ;;
    esac
done

case "$JOB" in
    wordcount|lang|wordlen|bigram) ;;
    *) echo -e "${RED}Error: invalid job (${JOB}).${NC}" >&2; exit 1 ;;
esac

if [[ ! -f "$VERIFIED_FILE" ]]; then
    echo -e "${RED}FATAL: verified machine list not found: ${VERIFIED_FILE}${NC}" >&2
    exit 1
fi

IFS=',' read -r -a WORKER_COUNTS <<< "$COUNTS"
MAX_N=0
for n in "${WORKER_COUNTS[@]}"; do
    [[ "$n" =~ ^[0-9]+$ ]] || { echo -e "${RED}Error: bad count '${n}'.${NC}" >&2; exit 1; }
    (( n > MAX_N )) && MAX_N="$n"
done

# ── Machine pool ──────────────────────────────────────────────────────────────
mapfile -t MACHINES < <(grep '[^[:space:]]' "$VERIFIED_FILE" | sed 's/\r$//')
NEEDED="$((MAX_N + 1))"
if (( ${#MACHINES[@]} < NEEDED )); then
    echo -e "${RED}FATAL: need ${NEEDED} machines (1 master + ${MAX_N} workers) but only ${#MACHINES[@]} verified.${NC}" >&2
    exit 1
fi
MASTER_HOST="${MACHINES[0]}"
WORKERS_POOL=("${MACHINES[@]:1}")

# ── Paths (identical to demo_verified_run.sh, NFS home is shared) ──────────────
DIR_NAME="slr207-group1-commoncrawl-${USER}"
NFS_DIR="~/${DIR_NAME}"
TMP_DIR="/tmp/${DIR_NAME}"
NFS_INPUT_DIR="${NFS_DIR}/input"
NFS_OUTPUT_DIR="${NFS_DIR}/output"
LOCAL_MAP_DIR="${TMP_DIR}/map-outputs"
FILES_TO_UPLOAD=("src/mapreduce/master.py" "src/mapreduce/worker.py" "src/mapreduce/download_commoncrawl.py")

SSH_OPTS="-4 \
  -o StrictHostKeyChecking=no \
  -o BatchMode=yes \
  -o LogLevel=ERROR \
  -o ServerAliveInterval=15 \
  -o ServerAliveCountMax=4"

logf() { printf "$@" >&2; }

# ── One-time: upload code (NFS shared → once is enough) + ensure splits ───────
prepare() {
    logf "${BLUE}Preparing on %s (NFS shared)...${NC}\n" "$MASTER_HOST"

    logf "  mkdir + upload code%-12s" "..."
    if timeout 20 ssh -n $SSH_OPTS "$MASTER_HOST" "mkdir -p ${NFS_DIR}" >/dev/null 2>&1 \
       && timeout 30 scp $SSH_OPTS "${FILES_TO_UPLOAD[@]}" "${MASTER_HOST}:${NFS_DIR}/" >/dev/null 2>&1; then
        logf "${GREEN}[OK]${NC}\n"
    else
        logf "${RED}[FAILED]${NC}\n"; return 1
    fi

    logf "  ensure %s splits%-9s" "$N_SPLITS" "..."
    local existing
    existing="$(timeout 20 ssh -n $SSH_OPTS "$MASTER_HOST" \
        "mkdir -p ${NFS_INPUT_DIR} && find ${NFS_INPUT_DIR} -maxdepth 1 -type f -name 'commoncrawl-*.txt' | wc -l" 2>/dev/null | tr -d '[:space:]')"
    [[ "$existing" =~ ^[0-9]+$ ]] || { logf "${RED}[FAILED]${NC}\n"; return 1; }

    if (( existing < N_SPLITS )); then
        logf "${YELLOW}[%s/%s] downloading...${NC}\n" "$existing" "$N_SPLITS"
        if timeout 1800 ssh -n $SSH_OPTS "$MASTER_HOST" \
            "python3 -u ${NFS_DIR}/download_commoncrawl.py -o ${NFS_INPUT_DIR} -n ${N_SPLITS} --missing-only" >&2; then
            logf "  ${GREEN}[splits ready]${NC}\n"
        else
            logf "  ${RED}[download FAILED]${NC}\n"; return 1
        fi
    else
        logf "${GREEN}[%s/%s present]${NC}\n" "$existing" "$N_SPLITS"
    fi
    return 0
}

start_master() {
    local host="$1" port="$2" logdir="$3"
    timeout 25 ssh -n $SSH_OPTS "$host" \
        "rm -rf ${NFS_OUTPUT_DIR} && mkdir -p ${NFS_OUTPUT_DIR} ${logdir} &&
         nohup python3 -u ${NFS_DIR}/master.py -p ${port} -r ${N_REDUCERS} -s ${N_SPLITS} -j ${JOB} > ${logdir}/master_${host}.log 2>&1 &
         for i in {1..15}; do pgrep -f \"master.py -p ${port}\" >/dev/null && exit 0; sleep 1; done; exit 1" >/dev/null 2>&1
}

start_worker() {
    local host="$1" master_host="$2" port="$3" logdir="$4"
    timeout 25 ssh -n $SSH_OPTS "$host" \
        "mkdir -p ${logdir} &&
         nohup python3 -u ${NFS_DIR}/worker.py -h ${master_host} -p ${port} -i ${NFS_INPUT_DIR} -o ${NFS_OUTPUT_DIR} -l ${LOCAL_MAP_DIR} -j ${JOB} --worker-id ${host} > ${logdir}/worker_${host}.log 2>&1 &
         for i in {1..15}; do pgrep -f \"worker.py -h ${master_host} -p ${port}\" >/dev/null && exit 0; sleep 1; done; exit 1" >/dev/null 2>&1
}

# Block until the master PID exits (or hard timeout), then echo the TIMING JSON.
# NOTE: the regex uses the [m] bracket trick so this very polling command does
# NOT match itself in `pgrep -f` (its own cmdline contains the literal pattern).
wait_timing() {
    local master_host="$1" port="$2" logfile="$3" hard="$4"
    timeout "$hard" ssh $SSH_OPTS "$master_host" \
        "for i in \$(seq 1 100000); do pgrep -f '[m]aster\.py -p ${port}' >/dev/null || break; sleep 2; done;
         grep -h 'TIMING:' ${logfile} 2>/dev/null | tail -n 1" 2>/dev/null
}

cleanup_hosts() {
    local port="$1"; shift
    local h
    for h in "$@"; do
        # `timeout 15` is mandatory: without it, a single slow/unreachable host
        # makes the `wait` below block the whole sweep forever (no ssh timeout).
        timeout 15 ssh -n $SSH_OPTS "$h" "pkill -f '[m]aster\.py -p ${port}' 2>/dev/null; pkill -f '[w]orker\.py -h .* -p ${port}' 2>/dev/null; true" >/dev/null 2>&1 &
    done
    wait
}

# ── Run ───────────────────────────────────────────────────────────────────────
echo -e "================================================="
echo -e " Amdahl cluster sweep"
echo -e "================================================="
echo -e "Dataset (fixed) : ${N_SPLITS} splits | reducers=${N_REDUCERS} | job=${JOB}"
echo -e "Worker counts   : ${COUNTS}"
echo -e "Master          : ${MASTER_HOST}"
echo -e "Workers pool    : ${#WORKERS_POOL[@]} machines available"
echo -e ""

prepare || { echo -e "${RED}FATAL: preparation failed.${NC}" >&2; exit 1; }
echo -e ""

mkdir -p "$(dirname "$OUTPUT")"
RESULTS_TMP="$(mktemp)"
idx=0

for N in "${WORKER_COUNTS[@]}"; do
    PORT="$((BASE_PORT + idx))"
    STAMP="$(date +%Y%m%d_%H%M%S)"
    LOGDIR="${TMP_DIR}/logs/amdahl_N${N}_${STAMP}"
    MASTER_LOG="${LOGDIR}/master_${MASTER_HOST}.log"
    USED=("$MASTER_HOST" "${WORKERS_POOL[@]:0:N}")

    echo -e "${BLUE}--- N=${N} workers (port ${PORT}) ---${NC}"

    cleanup_hosts "$PORT" "${USED[@]}"

    logf "  master %-22s" "$MASTER_HOST"
    if start_master "$MASTER_HOST" "$PORT" "$LOGDIR"; then
        logf "${GREEN}[up]${NC}\n"
    else
        logf "${RED}[FAILED — skip N=${N}]${NC}\n"; idx=$((idx+1)); continue
    fi

    started=0
    for w in "${WORKERS_POOL[@]:0:N}"; do
        if start_worker "$w" "$MASTER_HOST" "$PORT" "$LOGDIR"; then
            started=$((started+1))
        else
            logf "  ${YELLOW}worker ${w} failed to start${NC}\n"
        fi
    done
    logf "  workers started : %s/%s\n" "$started" "$N"

    logf "  running (waiting for master to finish)...\n"
    TIMING_LINE="$(wait_timing "$MASTER_HOST" "$PORT" "$MASTER_LOG" 2400)"

    if [[ "$TIMING_LINE" == *TIMING:* ]]; then
        JSON="${TIMING_LINE#*TIMING:}"
        echo -e "${GREEN}  ✓ ${TIMING_LINE#*TIMING: }${NC}"
        echo "${N}|${JSON}" >> "$RESULTS_TMP"
    else
        echo -e "${RED}  ✗ no TIMING captured for N=${N}${NC}"
    fi

    cleanup_hosts "$PORT" "${USED[@]}"
    idx=$((idx+1))
    echo -e ""
done

# ── Assemble JSON + print table ───────────────────────────────────────────────
python3 - "$RESULTS_TMP" "$OUTPUT" "$N_SPLITS" "$N_REDUCERS" <<'PY'
import json, sys
tmp, out, nsplits, nreducers = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
records = []
with open(tmp) as f:
    for line in f:
        line = line.strip()
        if not line or "|" not in line:
            continue
        n, js = line.split("|", 1)
        try:
            d = json.loads(js)
        except json.JSONDecodeError:
            continue
        records.append({
            "n_workers": int(n),
            "n_splits": nsplits,
            "n_reducers": nreducers,
            "t_total": d.get("t_total"),
            "t_map": d.get("t_map"),
            "t_reduce": d.get("t_reduce"),
        })
records.sort(key=lambda r: r["n_workers"])
with open(out, "w") as f:
    json.dump(records, f, indent=2)

if records:
    base = next((r["t_total"] for r in records if r["n_workers"] == 1), records[0]["t_total"])
    print("\n  N    t_total   t_map   t_reduce   speedup")
    print("  " + "-" * 44)
    for r in records:
        sp = base / r["t_total"] if r["t_total"] else 0
        print(f"  {r['n_workers']:<4} {r['t_total']:>7.2f}  {r['t_map']:>6.2f}  {r['t_reduce']:>8.2f}   {sp:>5.2f}x")
print(f"\nWrote {out} ({len(records)} points)")
PY

rm -f "$RESULTS_TMP"
echo -e ""
echo -e "${GREEN}Sweep complete.${NC} Plot with: python3 src/benchmarks/plot_amdahl.py --input ${OUTPUT} --output runtime/amdahl_cluster.png"
exit 0
