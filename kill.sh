#!/bin/bash
# Stop all load servers directly over Campus Wi-Fi.
# Usage: ./kill.sh [port]

set -uo pipefail

MACHINES_FILE="machines.txt"
PORT="${1:-54321}"
SSH_OPTS="-4 -o StrictHostKeyChecking=no -o ConnectTimeout=3 -o BatchMode=yes -o LogLevel=ERROR"

if [[ ! -f "$MACHINES_FILE" ]]; then
    echo "No $MACHINES_FILE found. Nothing to kill."
    exit 0
fi

echo "Stopping servers on port $PORT concurrently..."

# Send kill signals in parallel
while IFS= read -r host; do
    [[ -z "$host" ]] && continue
    
    (
        if timeout 5 ssh -n $SSH_OPTS "$host" "fuser -k -TERM ${PORT}/tcp >/dev/null 2>&1 || true" 2>/dev/null; then
            echo "  [CLEANED]  $host"
        else
            echo "  [TIMEOUT]  $host (Ignored)"
        fi
    ) &
done < "$MACHINES_FILE"

wait # Wait for all background tasks to finish. With 'timeout 5', this will never block > 5s.

# Delete server.py from the shared NFS home (also protected by timeout)
FIRST_NODE=$(head -n 1 "$MACHINES_FILE")
if [[ -n "$FIRST_NODE" ]]; then
    timeout 5 ssh -n $SSH_OPTS "$FIRST_NODE" "rm -f ~/server.py" 2>/dev/null || true
    echo "Removed ~/server.py from NFS."
fi

echo "All cleaned! machines.txt kept intact."