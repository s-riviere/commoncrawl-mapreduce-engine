#!/bin/bash
# Stop all load servers and clean up.
# Uses PID file written by server.py; no fragile pattern matching.
# Usage: ./kill.sh [alive_file]

set -uo pipefail

ALIVE_FILE="${1:-machines_alive.txt}"
REMOTE_USER="kabil-25"
JUMP="ssh.enst.fr"
L_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=15 -o BatchMode=yes -o LogLevel=ERROR"

if [[ ! -f "$ALIVE_FILE" ]]; then
    echo "No $ALIVE_FILE found. Nothing to kill."
    exit 0
fi

echo "Stopping servers..."

ROPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=6 -o BatchMode=yes -o LogLevel=ERROR"
JUMP_SPEC="${REMOTE_USER}@${JUMP}"

while IFS= read -r host; do
    [[ -z "$host" ]] && continue
    result=$(ssh -n $ROPTS -J "$JUMP_SPEC" "${REMOTE_USER}@${host}" \
        'xargs kill 2>/dev/null < ~/.server.pid; rm -f ~/.server.pid; echo killed' \
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
