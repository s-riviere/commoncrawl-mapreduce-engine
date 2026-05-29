#!/bin/bash
# Robust parallel deployment for WSL2 over Campus Wi-Fi.
# Uploads server.py AND machines.txt once via NFS (SCP to one machine),
# then SSH-starts one server process per machine in machines.txt.
#
# After deployment, other team members sync their local machines.txt by
# running:  python3 client.py <port> --sync <any-reachable-lab-machine>
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

echo "[1/2] Syncing server.py + machines.txt to NFS home..."
UPLOADED=0
NFS_HOST=""

while IFS= read -r host; do
    [[ -z "$host" ]] && continue
    printf "      Trying %-35s ... " "$host"
    # Upload both server.py and machines.txt in one scp call.
    # machines.txt on NFS lets team members sync via --sync flag in client.py.
    if timeout 10 scp -4 \
        -o StrictHostKeyChecking=no \
        -o ConnectTimeout=4 \
        -o BatchMode=yes \
        -o LogLevel=ERROR \
        "server.py" "machines.txt" "${host}:~/" 2>/dev/null; then
        echo "[OK]"
        UPLOADED=1
        NFS_HOST="$host"
        break
    fi
    echo "[timeout]"
done < "$MACHINES_FILE"

if [[ $UPLOADED -eq 0 ]]; then
    echo ""
    echo "FATAL: Could not upload — all machines are unreachable."
    exit 1
fi

# ── Phase 2: parallel SSH bootstrap ───────────────────────────────────────────
echo "[2/2] Bootstrapping cluster processes..."

declare -a PIDS=()

while IFS= read -r host; do
    [[ -z "$host" ]] && continue

    (
        # 'nohup … &' detaches the server from this SSH session.
        # 'echo ok' confirms the fork happened before SSH exits.
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

for pid in "${PIDS[@]}"; do
    wait "$pid"
done

echo "================================================="
echo " Deployment complete."
echo ""
echo " Verify:      python3 client.py ${PORT}"
echo " Team sync:   python3 client.py ${PORT} --sync ${NFS_HOST}"
echo "================================================="