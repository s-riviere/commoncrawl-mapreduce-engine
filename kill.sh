#!/bin/bash
# Stop all load servers we own and clean up.
# Uses fuser -k on the port: reliably kills any process (ours) listening
# there, even orphaned ones from previous deploys.
# Usage: ./kill.sh [alive_file] [port]

set -uo pipefail

ALIVE_FILE="${1:-machines.txt}"
PORT="${2:-54321}"
REMOTE_DIR="/tmp/slr207-group1"

# Phase timing (cleanup). See bench.sh.
source "$(dirname "$0")/bench.sh"
bench_init

if [[ ! -f "$ALIVE_FILE" ]]; then
    echo "No $ALIVE_FILE found. Nothing to kill."
    exit 0
fi

echo "Stopping servers on port $PORT..."

ROPTS="-4 -o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes -o LogLevel=ERROR"

bench_start cleanup
while IFS= read -r host; do
    host="${host%%$'\r'}"
    [[ -z "$host" ]] && continue
    result=$(ssh -n $ROPTS "${host}" \
        "pid=\$(fuser ${PORT}/tcp 2>/dev/null | tr -dc '0-9')
        echo -n \"PID=[\$pid] \" >&2
        if [[ -n \"\$pid\" ]]; then
            kill -TERM \$pid >&2 || true
            sleep 0.3
            kill -9 \$pid 2>/dev/null || true
            echo killed
        else
            echo not_running
        fi" \
        2>&1 || echo unreachable)
    echo "  [${result}]  $host"
done < "$ALIVE_FILE"
bench_end cleanup

echo ""
echo "All clean."

bench_report
