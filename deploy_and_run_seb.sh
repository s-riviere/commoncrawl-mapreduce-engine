#!/bin/bash
# Robust parallel deployment for WSL2 over Campus Wi-Fi.
# Uploads server.py AND machines.txt to /tmp on one lab machine,
# then SSH-starts one server process per machine in machines.txt.
#
# After deployment, other team members sync their local machines.txt by
# running:  python3 client.py <port> --sync <any-reachable-lab-machine>
#
# Usage: ./deploy.sh [port]

set -euo pipefail

PORT="${1:-54321}"
MACHINES_FILE="machines.txt"

# Phase timing (deploy / fetch / upload / bootstrap). See bench.sh.
source "$(dirname "$0")/bench.sh"
bench_init

# -4  : force IPv4 — avoids WSL2 IPv6 resolution bugs
# -n  : no stdin (essential when running in a background subshell)
SSH_OPTS="-4 -n \
  -o StrictHostKeyChecking=no \
  -o ConnectTimeout=15 \
  -o BatchMode=yes \
  -o LogLevel=ERROR \
  -o ServerAliveInterval=3 \
  -o ServerAliveCountMax=2"

# ── Phase 0: fetch alive machines ─────────────────────────────────────────────
echo "================================================="
echo " Phase 0: Fetching Alive Machines from API"
echo "================================================="
bench_start fetch_machines
python3 get_machines.py
bench_end fetch_machines

if [[ ! -f "$MACHINES_FILE" ]]; then
    echo "Error: $MACHINES_FILE not created by get_machines.py."
    exit 1
fi

TOTAL=$(wc -l < "$MACHINES_FILE")
echo "  → $TOTAL machines loaded from $MACHINES_FILE"

# ── Phase 1: NFS upload (one SCP is enough — home dir is shared) ──────────────
echo ""
echo "================================================="
echo " Phase 1: Parallel Deployment"
echo "================================================="

echo "[1/2] Uploading server.py (NFS) + machines.txt (/tmp) ..."
bench_start upload
UPLOADED=0
NFS_HOST=""
REMOTE_DIR="/tmp/slr207-group1"

while IFS= read -r host; do
    [[ -z "$host" ]] && continue
    printf "      Trying %-35s ... " "$host"
    # master.py → ~/  (NFS, visible from all machines)
    # worker.py → ~/  (NFS, visible from all machines)
    # wordcount.py → ~/  (NFS, visible from all machines)
    # machines.txt → /tmp/slr207-group1/ (local disk on this host only)
    if timeout 15 ssh $SSH_OPTS "$host" "mkdir -p $REMOTE_DIR" && \
       timeout 15 scp -4 \
        -o StrictHostKeyChecking=no \
        -o ConnectTimeout=10 \
        -o BatchMode=yes \
        -o LogLevel=ERROR \
        "map_reduce/master.py" "map_reduce/worker.py" "map_reduce/wordcount.py" \
        "map_reduce/cluster.py" "download_commoncrawl.py" "${host}:~/" && \
       timeout 15 scp -4 \
        -o StrictHostKeyChecking=no \
        -o ConnectTimeout=10 \
        -o BatchMode=yes \
        -o LogLevel=ERROR \
        "machines.txt" "${host}:${REMOTE_DIR}/"; then
        echo "[OK]"
        if timeout 20 ssh $SSH_OPTS "$host" \
            "rm -rf ${REMOTE_DIR}; mkdir ${REMOTE_DIR}; nohup python3 ~/master.py wordcount ${PORT} > ${REMOTE_DIR}/logs 2>&1 & echo ok" \
            | grep -q "^ok$"; then
            echo "  [MAIN STARTED] -> $host"
        else
            echo "  [MAIN FAILED]  -> $host"
        fi
        UPLOADED=1
        NFS_HOST="$host"
        break
    fi
    echo "[timeout]"
    sleep 1
done < "$MACHINES_FILE"
bench_end upload

if [[ $UPLOADED -eq 0 ]]; then
    echo ""
    echo "FATAL: Could not upload — all machines are unreachable."
    exit 1
fi

# ── Phase 2: parallel SSH bootstrap ───────────────────────────────────────────
echo "[2/2] Bootstrapping cluster processes..."
bench_start bootstrap

declare -a PIDS=()

while IFS= read -r host; do
    [[ -z "$host" ]] && continue
    [[ $host == $NFS_HOST ]] && continue

    (
        # worker.py is on NFS (~/), visible from all machines.
        if timeout 20 ssh $SSH_OPTS "$host" \
            "rm -rf ${REMOTE_DIR}; mkdir ${REMOTE_DIR}; nohup python3 ~/worker.py ${NFS_HOST} > ${REMOTE_DIR}/logs 2>&1 & echo ok" \
            | grep -q "^ok$"; then
            echo "  [STARTED] -> $host"
        else
            echo "  [FAILED]  -> $host"
        fi
    ) &
    PIDS+=($!)

    # Stagger to avoid overwhelming proxy
    sleep 1
done < "$MACHINES_FILE"

for pid in "${PIDS[@]}"; do
    wait "$pid"
done
bench_end bootstrap

echo "================================================="
echo " Deployment complete."
echo "================================================="

bench_report
