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
# Colors
NC="\e[0m"
GREEN="\e[32m"
YELLOW="\e[33m"
RED="\e[31m"

# Failure behavior
set -uo pipefail

# Set the current directory to the root of the project (src/)
CURRENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
while [[ "$(basename "$CURRENT_DIR")" != "src" && "$CURRENT_DIR" != "/" ]]; do
    CURRENT_DIR="$(dirname "$CURRENT_DIR")"
done

if [[ "$CURRENT_DIR" == "/" ]]; then
    echo -e "${RED}Erreur critique : Impossible de localiser le dossier racine 'src'.${NC}" >&2
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
MACHINES_FILE="runtime/machines.txt"
TIMEOUT=5
SSH_OPTS="-4 \
  -o StrictHostKeyChecking=no \
  -o BatchMode=yes \
  -o LogLevel=ERROR"

kill_server() {
    local host="$1"
    timeout $TIMEOUT ssh -n $SSH_OPTS "$host" \
    "rm -rf /tmp/slr207-group1-$USER
    rm -f ~/server.py

    pid=\$(fuser ${PORT}/tcp 2>/dev/null | tr -dc '0-9')

    if [[ -n \"\$pid\" ]]; then
        kill -TERM \$pid >&2 || true
        sleep 0.3
        kill -9 \$pid 2>/dev/null || true
        echo killed
    else
        echo not_running
    fi" 2>&1 || echo "unreachable"
}

if [[ ! -f "$MACHINES_FILE" ]]; then
    echo -e "${YELLOW}Info : No $MACHINES_FILE found.${NC}" >&2
    exit 0
fi


# ── Stopping servers ──────────────────────────────────────────────────────────
echo -e "================================================="
echo -e " Stopping servers on port ${PORT}.                 "
echo -e "================================================="
echo -e ""

source "common/bench.sh"
bench_init
bench_start cleanup

declare -a PIDS=()

while IFS= read -r host; do
    host="${host%%$'\r'}"
    [[ -z "$host" ]] && continue
    
    (
        result=$(kill_server "$host")
        if [[ "$result" == *"killed"* ]]; then
            printf "    Killing %-25s ${GREEN}[KILLED]${NC}\n" "$host"
        elif [[ "$result" == *"not_running"* ]]; then
            printf "    Killing %-25s ${YELLOW}[NOT RUNNING]${NC}\n" "$host"
        else
            printf "    Killing %-25s ${RED}[UNREACHABLE]${NC}\n" "$host"
        fi
    ) &
    PIDS+=($!)

    sleep 0.1
done < "$MACHINES_FILE"

for pid in "${PIDS[@]}"; do
    wait "$pid"
done

bench_end cleanup

echo -e ""


# ── Final Report ──────────────────────────────────────────────────────────────
echo -e "================================================="
echo -e " All clean.                                      "
echo -e "================================================="

bench_report

exit 0
