#!/bin/bash
# Phase 1 Deployment - Robust version for WSL2
set -euo pipefail

PORT=${1:-54321}
MACHINES_FILE="machines.txt"
# L'option -4 force l'IPv4 pour contourner le bug IPv6 de WSL
SSH_OPTS="-4 -o StrictHostKeyChecking=no -o ConnectTimeout=3 -o BatchMode=yes -o LogLevel=ERROR"

echo "================================================="
echo " Phase 0: Fetching 100 Alive Machines from API"
echo "================================================="
python3 get_machines.py

if [[ ! -f "$MACHINES_FILE" ]]; then
    echo "Error: $MACHINES_FILE not created."
    exit 1
fi

echo ""
echo "================================================="
echo " Phase 1: Parallel Deployment Loop"
echo "================================================="

echo "[1/2] Syncing server.py to NFS home..."
UPLOAD_SUCCESS=0

# On boucle jusqu'à trouver une machine allumée pour faire le SCP
while IFS= read -r host; do
    [[ -z "$host" ]] && continue
    echo "      Trying to upload via $host..."
    if scp $SSH_OPTS "server.py" "${host}:~/server.py"; then
        echo "      [SUCCESS] Uploaded via $host"
        UPLOAD_SUCCESS=1
        break
    fi
done < "$MACHINES_FILE"

if [[ $UPLOAD_SUCCESS -eq 0 ]]; then
    echo "FATAL ERROR: Could not upload server.py. All tried machines are unreachable."
    exit 1
fi

echo "[2/2] Bootstrapping cluster processes..."

while IFS= read -r host; do
    [[ -z "$host" ]] && continue
    
    (
        if ssh $SSH_OPTS "$host" "nohup python3 ~/server.py ${PORT} >/dev/null 2>&1 & echo ok" 2>/dev/null | grep -q "ok"; then
            echo "  [STARTED] -> $host"
        else
            echo "  [FAILED]  -> $host"
        fi
    ) &
done < "$MACHINES_FILE"

wait
echo "================================================="
echo " Deployment sequence finished!"
echo " Run client: python3 client.py $PORT"
echo "================================================="