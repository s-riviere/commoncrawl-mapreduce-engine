#!/bin/bash

# ==============================================================================
# DESCRIPTION
# ==============================================================================
# Stop all load servers we own and clean up.
# Uses fuser -k on the port: reliably kills any process (ours) 
# listening there, even orphaned ones from previous deploys.
#
# Usage : ./kill.sh [port]

# ==============================================================================
# SH CONFIGURATION
# ==============================================================================
set -uo pipefail

CURRENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
while [[ "$(basename "$CURRENT_DIR")" != "src" && "$CURRENT_DIR" != "/" ]]; do
    CURRENT_DIR="$(dirname "$CURRENT_DIR")"
done

if [[ "$CURRENT_DIR" == "/" ]]; then
    echo "Erreur critique : Impossible de localiser le dossier racine 'src'." >&2
    exit 1
fi
cd "$CURRENT_DIR"

# ==============================================================================
# ARGUMENTS
# ==============================================================================
PORT="${1:-54321}"

# ==============================================================================
# CONTENT
# ==============================================================================
ALIVE_FILE="runtime/machines.txt"
REMOTE_DIR="/tmp/slr207-group1-$USER"
ROPTS="-4 -o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes -o LogLevel=ERROR"

source "common/bench.sh"
bench_init

if [[ ! -f "$ALIVE_FILE" ]]; then
    echo "No $ALIVE_FILE found. Nothing to kill."
    exit 0
fi

echo "Stopping servers on port $PORT..."

bench_start cleanup
while IFS= read -r host; do
    host="${host%%$'\r'}"
    [[ -z "$host" ]] && continue
    result=$(ssh -n $ROPTS "${host}" \
        "pid=\$(fuser ${PORT}/tcp 2>/dev/null | tr -dc '0-9')
        rm -rf $REMOTE_DIR
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
