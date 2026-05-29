#!/bin/bash
# Deploy load server on all reachable machines.
# Strategy:
#   1. This computer uploads server.py directly to each machine's local disk.
#   2. This computer opens one sequential ssh hop per lab machine via
#      the jump host, so all SSH sessions originate locally.
# Usage: ./deploy.sh [port]

set -euo pipefail

PORT=${1:-54321}
REMOTE_USER="rdeloye-24"
REMOTE_USER="kabil-25"
JUMP="ssh.enst.fr"
SERVER="server.py"
ALL_MACHINES="machines.txt"
ALIVE_FILE="machines_alive.txt"
REMOTE_DIR="/tmp/${REMOTE_USER}/slr207-project"
REMOTE_SERVER_PATH="${REMOTE_DIR}/${SERVER}"

if [[ ! -f "$ALL_MACHINES" ]]; then
    echo "error: $ALL_MACHINES not found."
    exit 1
fi

echo "=========================================="
echo "  Load Server Deployment  (port $PORT)"
echo "=========================================="
echo ""
echo "[1/2] Copying $SERVER to each machine's local disk..."
echo ""
echo "[2/2] Starting server on each machine from this computer..."

ROPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=6 -o BatchMode=yes -o LogLevel=ERROR"
JUMP_SPEC="${REMOTE_USER}@${JUMP}"
ENCODED=$(base64 -w 0 < "$SERVER")
: > "$ALIVE_FILE"
count=0
total=0

while IFS= read -r host; do
    host="${host%%$'\r'}"
    [[ -z "$host" ]] && continue
    total=$((total + 1))
    result=$(ssh -n $ROPTS -J "$JUMP_SPEC" "${REMOTE_USER}@${host}" \
        "mkdir -p '${REMOTE_DIR}' && echo '${ENCODED}' | base64 -d > '${REMOTE_SERVER_PATH}' && nohup python3 '${REMOTE_SERVER_PATH}' ${PORT} </dev/null >/dev/null 2>&1 & echo ok" \
        2>&1 || true)
    if [[ "$result" == "ok" ]]; then
        echo "  [OK]  $host"
        echo "$host" >> "$ALIVE_FILE"
        count=$((count + 1))
    else
        echo "  [--]  $host"
    fi
done < "$ALL_MACHINES"

echo ""
echo "  --> $count / $total machines deployed."

ALIVE_COUNT=$(wc -l < "$ALIVE_FILE")
TOTAL=$(wc -l < "$ALL_MACHINES")
echo ""
echo "=========================================="
echo "  Alive: $ALIVE_COUNT / $TOTAL  -->  $ALIVE_FILE"
echo "=========================================="
echo ""
echo "Run client:  python3 client.py $PORT $ALIVE_FILE"
