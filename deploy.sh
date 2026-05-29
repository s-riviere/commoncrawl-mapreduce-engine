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
ALIVE_FILE="machines_alive.txt"
REMOTE_DIR="/tmp/${REMOTE_USER}/slr207-project"
REMOTE_SERVER_PATH="${REMOTE_DIR}/${SERVER}"

# Generate the list of alive machines
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
bash "$SCRIPT_DIR/gen_machines.sh" "$ALIVE_FILE"

if [[ ! -s "$ALIVE_FILE" ]]; then
    echo "error: no alive machines found."
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
CANDIDATES="$ALIVE_FILE"
: > "${ALIVE_FILE}.tmp"
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
        echo "$host" >> "${ALIVE_FILE}.tmp"
        count=$((count + 1))
    else
        echo "  [--]  $host"
    fi
done < "$CANDIDATES"

mv "${ALIVE_FILE}.tmp" "$ALIVE_FILE"

echo ""
echo "  --> $count / $total machines deployed."

echo ""
echo "=========================================="
echo "  Alive: $count / $total  -->  $ALIVE_FILE"
echo "=========================================="
echo ""
echo "Run client:  python3 client.py $PORT $ALIVE_FILE"
