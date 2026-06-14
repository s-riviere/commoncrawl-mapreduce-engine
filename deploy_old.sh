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

echo "[1/2] Uploading server.py (NFS) + machines.txt (/tmp) ..."
UPLOADED=0
NFS_HOST=""
REMOTE_DIR="/tmp/slr207-group1-$USER"

while IFS= read -r host; do
    [[ -z "$host" ]] && continue
    printf "      Trying %-35s ... " "$host"
    # server.py → ~/  (NFS, visible from all machines)
    # machines.txt → /tmp/slr207-group1/ (local disk on this host only)
    if timeout 15 ssh $SSH_OPTS "$host" "mkdir -p $REMOTE_DIR; chmod 777 $REMOTE_DIR" && \
       timeout 15 scp -4 \
        -o StrictHostKeyChecking=no \
        -o ConnectTimeout=10 \
        -o BatchMode=yes \
        -o LogLevel=ERROR \
        "server.py" "${host}:~/" && \
       timeout 15 scp -4 \
        -o StrictHostKeyChecking=no \
        -o ConnectTimeout=10 \
        -o BatchMode=yes \
        -o LogLevel=ERROR \
        "machines.txt" "${host}:${REMOTE_DIR}/" ; then
        echo "[OK]"
        UPLOADED=1
        NFS_HOST="$host"
        break
    fi
    echo "[timeout]"
    sleep 1
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
        ERR_LOG=$(mktemp)

        if timeout 20 ssh $SSH_OPTS "$host" \
            "nohup python3 ~/server.py ${PORT} < /dev/null > /tmp/server_${PORT}.log 2>&1 & echo ok" \
            2> "$ERR_LOG" | grep -q "^ok$"; then
            echo "  [STARTED] -> $host"
        else
            echo "  [FAILED]  -> $host"
            if [[ -s "$ERR_LOG" ]]; then
                sed 's/^/      [ERREUR] /' "$ERR_LOG"
            else
                echo "      [ERREUR] Timeout ou absence de réponse 'ok'"
            fi
        fi
        
        rm -f "$ERR_LOG"
    ) &
    PIDS+=($!)

    # Stagger to avoid overwhelming proxy
    sleep 1
done < "$MACHINES_FILE"

for pid in "${PIDS[@]}"; do
    wait "$pid"
done

echo "================================================="
echo " Deployment complete."
echo ""
echo " machines.txt stored on: ${NFS_HOST}:${REMOTE_DIR}/ ATTENTION: NE PAS METTRE .enst.fr"
echo ""
echo " Verify:      python3 client.py ${PORT}"
echo " Team sync:   python3 client.py ${PORT} --sync ${NFS_HOST}"
echo "================================================="
