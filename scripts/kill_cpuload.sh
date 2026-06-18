#!/bin/bash

# ==============================================================================
# DESCRIPTION
# ==============================================================================
# Stop all processes we own and clean up.
# Finds the process listening on the specified port, terminates it gracefully 
# (SIGTERM), then forcefully (SIGKILL) if it persists. Cleans remote directories.
#
# Arguments:
#   [port]  : Network port number to clear (default: 54321).
#
# Usage : ./kill.sh [port]
# ==============================================================================


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

# Set the current directory to the root of the project
cd "$(dirname "$0")/.."


# ==============================================================================
# ARGUMENTS
# ==============================================================================
PORT="${1:-54321}"


# ==============================================================================
# CONTENT
# ==============================================================================
MACHINES_FILE="runtime/machines.txt"

DIR_NAME="slr207-group1-cpuload"
NFS_DIR="~/${DIR_NAME}"
TMP_DIR="/tmp/${DIR_NAME}"

SLEEP=0.5
SSH_OPTS="-4 \
  -o StrictHostKeyChecking=no \
  -o BatchMode=yes \
  -o LogLevel=ERROR"

kill_server() {
    local host="$1"
    timeout 5 ssh -n $SSH_OPTS "$host" \
    "rm -rf ${TMP_DIR} ${NFS_DIR}

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
echo -e " Stopping servers on port ${PORT}.               "
echo -e "================================================="
echo -e ""

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

    sleep "$SLEEP"
done < "$MACHINES_FILE"

for pid in "${PIDS[@]}"; do
    wait "$pid"
done

echo -e ""


# ── Final Report ──────────────────────────────────────────────────────────────
echo -e "================================================="
echo -e " All clean.                                      "
echo -e "================================================="

exit 0
