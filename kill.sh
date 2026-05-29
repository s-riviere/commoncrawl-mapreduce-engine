#!/bin/bash
# Stop all load servers we own and clean up.
# Uses fuser -k on the port: reliably kills any process (ours) listening
# there, even orphaned ones from previous deploys.
# Usage: ./kill.sh [alive_file] [port]

set -uo pipefail

ALIVE_FILE="${1:-machines_alive.txt}"
PORT="${2:-54321}"
REMOTE_USER="rdeloye-24"
REMOTE_USER="kabil-25"
JUMP="ssh.enst.fr"
REMOTE_DIR="/tmp/slr207-group1"
REMOTE_SERVER_PATH="${REMOTE_DIR}/server.py"
REMOTE_PID_PATH="${REMOTE_DIR}/.server.pid"

if [[ ! -f "$ALIVE_FILE" ]]; then
    echo "No $ALIVE_FILE found. Nothing to kill."
    exit 0
fi

echo "Stopping servers on port $PORT..."

ROPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=6 -o BatchMode=yes -o LogLevel=ERROR"
JUMP_SPEC="${REMOTE_USER}@${JUMP}"

while IFS= read -r host; do
    host="${host%%$'\r'}"
    [[ -z "$host" ]] && continue
    result=$(ssh -n $ROPTS -J "$JUMP_SPEC" "${REMOTE_USER}@${host}" \
        "if fuser -k -TERM ${PORT}/tcp >/dev/null 2>&1; then
            sleep 0.2
            fuser -k -KILL ${PORT}/tcp >/dev/null 2>&1 || true
            rm -f '${REMOTE_PID_PATH}' '${REMOTE_SERVER_PATH}'
            echo killed
        else
            rm -f '${REMOTE_PID_PATH}' '${REMOTE_SERVER_PATH}'
            echo not_running
        fi" \
        2>/dev/null || echo unreachable)
    echo "  [${result}]  $host"
done < "$ALIVE_FILE"

echo ""
echo "  Removed ${REMOTE_SERVER_PATH} from each machine's local disk."

rm -f "$ALIVE_FILE"
echo "  Removed $ALIVE_FILE."
echo ""
echo "All clean."
