#!/bin/bash
# Stop all load servers we own and clean up.
# Uses fuser -k on the port: reliably kills any process (ours) listening
# there, even orphaned ones from previous deploys.
# Usage: ./kill.sh [alive_file] [port]

set -uo pipefail

ALIVE_FILE="${1:-machines_alive.txt}"
PORT="${2:-54321}"
REMOTE_USER="kabil-25"
JUMP="ssh.enst.fr"
L_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=15 -o BatchMode=yes -o LogLevel=ERROR"

if [[ ! -f "$ALIVE_FILE" ]]; then
    echo "No $ALIVE_FILE found. Nothing to kill."
    exit 0
fi

echo "Stopping servers on port $PORT..."

ROPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=6 -o BatchMode=yes -o LogLevel=ERROR"
JUMP_SPEC="${REMOTE_USER}@${JUMP}"

while IFS= read -r host; do
    [[ -z "$host" ]] && continue
    result=$(ssh -n $ROPTS -J "$JUMP_SPEC" "${REMOTE_USER}@${host}" \
        "if fuser -k -TERM ${PORT}/tcp >/dev/null 2>&1; then
            sleep 0.2
            fuser -k -KILL ${PORT}/tcp >/dev/null 2>&1 || true
            rm -f ~/.server.pid
            echo killed
        else
            rm -f ~/.server.pid
            echo not_running
        fi" \
        2>/dev/null || echo unreachable)
    echo "  [${result}]  $host"
done < "$ALIVE_FILE"

# shellcheck disable=SC2086
ssh -n $L_OPTS "${REMOTE_USER}@${JUMP}" "rm -f ~/server.py" 2>/dev/null || true
echo ""
echo "  Removed ~/server.py from NFS home."

rm -f "$ALIVE_FILE"
echo "  Removed $ALIVE_FILE."
echo ""
echo "All clean."
