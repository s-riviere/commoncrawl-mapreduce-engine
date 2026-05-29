#!/bin/bash
# Deploy load server on all reachable machines.
# Strategy:
#   1. ONE scp uploads server.py to the shared NFS home.
#   2. This computer opens one sequential ssh hop per lab machine via
#      the jump host, so all SSH sessions originate locally.
# Usage: ./deploy.sh [port]

set -euo pipefail

PORT=${1:-54321}
REMOTE_USER="kabil-25"
JUMP="ssh.enst.fr"
SERVER="server.py"
ALL_MACHINES="machines.txt"
ALIVE_FILE="machines_alive.txt"
L_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=15 -o BatchMode=yes -o LogLevel=ERROR"

if [[ ! -f "$ALL_MACHINES" ]]; then
    echo "error: $ALL_MACHINES not found."
    exit 1
fi

echo "=========================================="
echo "  Load Server Deployment  (port $PORT)"
echo "=========================================="
echo ""
echo "[1/2] Uploading $SERVER via NFS..."
# shellcheck disable=SC2086
scp -q $L_OPTS "$SERVER" "${REMOTE_USER}@${JUMP}:"
echo "      Done."
echo ""
echo "[2/2] Starting server on each machine from this computer..."

ROPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=6 -o BatchMode=yes -o LogLevel=ERROR"
JUMP_SPEC="${REMOTE_USER}@${JUMP}"
: > "$ALIVE_FILE"
count=0
total=0

while IFS= read -r host; do
    [[ -z "$host" ]] && continue
    total=$((total + 1))
    result=$(ssh -n $ROPTS -J "$JUMP_SPEC" "${REMOTE_USER}@${host}" \
        "nohup python3 ~/server.py ${PORT} >/dev/null 2>&1 & echo ok" \
        2>/dev/null || true)
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
