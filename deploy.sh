#!/bin/bash
# Robust parallel deployment for WSL2 over Campus Wi-Fi.
# Uploads server.py once via NFS (SCP to one machine), then SSH-starts
# one server process per machine in machines.txt.
#
# Usage: ./deploy.sh [port]

set -euo pipefail

PORT="${1:-54321}"
MACHINES_FILE="machines.txt"

# -4  : force IPv4 — avoids WSL2 IPv6 resolution bugs
# -n  : no stdin (essential when running in a background subshell)
SSH_OPTS="-4 -n \
  -o StrictHostKeyChecking=no \
  -o ConnectTimeout=4 \
  -o BatchMode=yes \
  -o LogLevel=ERROR \
  -o ServerAliveInterval=3 \
  -o ServerAliveCountMax=2"

# ── Phase 0: fetch alive machines ─────────────────────────────────────────────
echo "================================================="
echo " Phase 0: Fetching Alive Machines from API"
echo "================================================="
python3 get_machines.py

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

echo "[1/2] Syncing server.py to NFS home..."
UPLOADED=0

while IFS= read -r host; do
    [[ -z "$host" ]] && continue
    printf "      Trying %-35s ... " "$host"
    # Hard 10-second wall-clock limit to avoid hanging on ghost connections
    if timeout 10 scp -4 \
        -o StrictHostKeyChecking=no \
        -o ConnectTimeout=4 \
        -o BatchMode=yes \
        -o LogLevel=ERROR \
        "server.py" "${host}:~/server.py" 2>/dev/null; then
        echo "[OK]"
        UPLOADED=1
        break
    fi
    echo "[timeout]"
done < "$MACHINES_FILE"

if [[ $UPLOADED -eq 0 ]]; then
    echo ""
    echo "FATAL: Could not upload server.py — all machines are unreachable."
    exit 1
fi

# ── Phase 2: parallel SSH bootstrap ───────────────────────────────────────────
echo "[2/2] Bootstrapping cluster processes..."

STARTED=0
FAILED=0
declare -a PIDS=()

while IFS= read -r host; do
    [[ -z "$host" ]] && continue

    (
        # 'nohup … &' detaches the server from this SSH session.
        # We immediately exit the SSH session after the process is spawned —
        # that is why we need 'echo ok': it confirms the fork happened.
        if timeout 6 ssh $SSH_OPTS "$host" \
            "nohup python3 ~/server.py ${PORT} >/dev/null 2>&1 & echo ok" \
            2>/dev/null | grep -q "^ok$"; then
            echo "  [STARTED] -> $host"
        else
            echo "  [FAILED]  -> $host"
        fi
    ) &
    PIDS+=($!)
done < "$MACHINES_FILE"

# Wait for every background subshell
for pid in "${PIDS[@]}"; do
    wait "$pid"
done

echo "================================================="
echo " Deployment complete."
echo " Verify: python3 client.py ${PORT}"
echo "================================================="