#!/usr/bin/env bash
#
# local_cluster.sh — run a full MapReduce job on THIS machine only.
#
# Spins up the real master plus N real worker processes on localhost, using
# --local-shuffle so reducers read map partitions straight from local disk
# (no sshd, no cluster, no teammates required). Handy for the live demo and
# for poking at the system by hand.
#
# The engine is identical to the cluster path; only the worker identity
# (--worker-id / --advertise-host) and the shuffle transport (--local-shuffle)
# differ. For the fully-automated PASS/FAIL suite, use tests/run_all.py instead.
#
# Usage:
#   scripts/local_cluster.sh -i <input_dir> [-j job] [-n workers]
#                            [-r reducers] [-o output_dir] [--validate]
#
#   -i  DIR   directory containing commoncrawl-*.txt splits          (required)
#   -j  JOB   wordcount | lang | wordlen | bigram                    (default: wordcount)
#   -n  N     number of local worker processes                      (default: 4)
#   -r  N     number of reducers                                    (default: 8)
#   -o  DIR   output directory for part-*.txt                       (default: /tmp/mr-local/out)
#       --validate   run validate.py against a single-machine reference at the end
#
set -euo pipefail

JOB="wordcount"
N_WORKERS=4
N_REDUCERS=8
INPUT_DIR=""
OUTPUT_DIR="/tmp/mr-local-$USER/out"
DO_VALIDATE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    -i) INPUT_DIR="$2"; shift 2 ;;
    -j) JOB="$2"; shift 2 ;;
    -n) N_WORKERS="$2"; shift 2 ;;
    -r) N_REDUCERS="$2"; shift 2 ;;
    -o) OUTPUT_DIR="$2"; shift 2 ;;
    --validate) DO_VALIDATE=1; shift ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//' | head -n 28
      exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$INPUT_DIR" ]]; then
  echo "error: -i <input_dir> is required (directory with commoncrawl-*.txt splits)" >&2
  exit 2
fi
if [[ ! -d "$INPUT_DIR" ]]; then
  echo "error: input dir not found: $INPUT_DIR" >&2
  exit 2
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MASTER="$ROOT/src/map_reduce/master.py"
WORKER="$ROOT/src/map_reduce/worker.py"
VALIDATE="$ROOT/src/map_reduce/validate.py"
PY="${PYTHON:-python3}"
MAP_BASE="/tmp/mr-local-$USER/map"

# Count input splits (commoncrawl-*.txt). The master must be told how many MAP
# tasks to schedule.
N_SPLITS="$(find "$INPUT_DIR" -maxdepth 1 -name 'commoncrawl-*.txt' | wc -l | tr -d ' ')"
if [[ "$N_SPLITS" -eq 0 ]]; then
  echo "error: no commoncrawl-*.txt splits found in $INPUT_DIR" >&2
  exit 2
fi

# Pick a free TCP port without racing.
PORT="$("$PY" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"

rm -rf "$OUTPUT_DIR" "$MAP_BASE"
mkdir -p "$OUTPUT_DIR" "$MAP_BASE"

echo "Local MapReduce demo"
echo "  job      : $JOB"
echo "  splits   : $N_SPLITS   (from $INPUT_DIR)"
echo "  workers  : $N_WORKERS"
echo "  reducers : $N_REDUCERS"
echo "  port     : $PORT"
echo "  output   : $OUTPUT_DIR"
echo

PIDS=()
cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

# Start the master.
"$PY" -u "$MASTER" -p "$PORT" -s "$N_SPLITS" -r "$N_REDUCERS" -j "$JOB" &
MASTER_PID=$!
PIDS+=("$MASTER_PID")

# Wait for the master to accept connections.
for _ in $(seq 1 60); do
  if "$PY" -c "import socket,sys; s=socket.socket(); s.settimeout(0.5)
sys.exit(0 if s.connect_ex(('127.0.0.1',$PORT))==0 else 1)" 2>/dev/null; then
    break
  fi
  sleep 0.25
done

# Start N local workers, each with its own local map dir and a unique id.
for i in $(seq 0 $((N_WORKERS - 1))); do
  "$PY" -u "$WORKER" -h 127.0.0.1 -p "$PORT" \
    -i "$INPUT_DIR" -o "$OUTPUT_DIR" -l "$MAP_BASE/w$i" -j "$JOB" \
    --worker-id "w$i" --advertise-host 127.0.0.1 --local-shuffle &
  PIDS+=("$!")
done

# Wait for the master to finish (it exits once the job is complete).
wait "$MASTER_PID"
MASTER_RC=$?
trap - EXIT INT TERM
cleanup

echo
if [[ "$MASTER_RC" -ne 0 ]]; then
  echo "master exited with code $MASTER_RC" >&2
  exit "$MASTER_RC"
fi
echo "Job done. Output in $OUTPUT_DIR"

if [[ "$DO_VALIDATE" -eq 1 ]]; then
  echo
  echo "Validating distributed output against single-machine reference..."
  "$PY" "$VALIDATE" -i "$INPUT_DIR" -o "$OUTPUT_DIR" -j "$JOB"
fi
