#!/bin/bash

# ==============================================================================
# DESCRIPTION
# ==============================================================================
# Robust parallel deployment for WSL2 over Campus Wi-Fi.
# Uploads remote files (NFS) and machines.txt (/tmp) on one lab machine.
# Then SSH-starts one server process per machine in machines.txt.
#
# Arguments:
#   [port]  : Network port number for the server process to listen on.
#
# Usage : ./deploy.sh [port]
#
# Examples:
#   ./deploy.sh 54321
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
N_WORKERS=50


# ==============================================================================
# CONTENT
# ==============================================================================
MACHINES_FILE="runtime/machines.txt"
FILES_TO_UPLOAD=("src/cpu_load/server.py")

DIR_NAME="slr207-group1"
NFS_DIR="~/${DIR_NAME}"
TMP_DIR="/tmp/${DIR_NAME}"

SLEEP=0.5
SSH_OPTS="-4 \
  -o StrictHostKeyChecking=no \
  -o BatchMode=yes \
  -o LogLevel=ERROR \
  -o ServerAliveInterval=3 \
  -o ServerAliveCountMax=2"

upload_to_host() {
    local host="$1"
    timeout 5 ssh -n $SSH_OPTS "$host" "mkdir -p ${TMP_DIR}; chmod 777 ${TMP_DIR}" && \
    timeout 5 scp $SSH_OPTS "$MACHINES_FILE" "${host}:${TMP_DIR}/" && \
    timeout 5 ssh -n $SSH_OPTS "$host" "mkdir -p ${NFS_DIR}" && \
    timeout 5 scp $SSH_OPTS "${FILES_TO_UPLOAD[@]}" "${host}:${NFS_DIR}/"
}

start_server() {
    local host="$1"
    timeout 5 ssh -n $SSH_OPTS "$host" \
    "nohup python3 ${NFS_DIR}/server.py ${PORT} > /dev/null 2>&1 & 
    sleep 1
    if ss -tln | grep -q \":${PORT} \"; then
        echo ok
    else
        echo failed
    fi" 2>/dev/null | grep -q "^ok$"
}


# ── Phase 0: fetch alive machines ─────────────────────────────────────────────
echo -e "================================================="
echo -e " Phase 0: Fetching Alive Machines from API       "
echo -e "================================================="
echo -e ""

mkdir -p "$(dirname "$MACHINES_FILE")"
bash scripts/get_machines.sh -n "${N_WORKERS}" > "$MACHINES_FILE" || exit 1

echo -e ""


# ── Phase 1: NFS upload (one SCP is enough — home dir is shared) ──────────────
echo -e "================================================="
echo -e " Phase 1: Parallel Deployment                    "
echo -e "================================================="
echo -e ""
echo -e "[1/2] Uploading remote files (NFS) and machines.txt (/tmp)"
echo -e ""

NFS_HOST=""

while IFS= read -r host; do
    host="${host%%$'\r'}"
    [[ -z "$host" ]] && continue
        
    if upload_to_host "$host"; then
        NFS_HOST="$host"
        printf "    Trying %-25s ${GREEN}[OK]${NC}\n" "$host"
        break
    else
        printf "    Trying %-25s ${YELLOW}[TIMEOUT]${NC}\n" "$host"
    fi

    sleep "$SLEEP"
done < "$MACHINES_FILE"

echo -e ""

if [[ -z "$NFS_HOST" ]]; then
    echo -e "${RED}FATAL: Could not upload — all machines are unreachable.${NC}" >&2
    exit 1
fi


# ── Phase 2: parallel SSH bootstrap ───────────────────────────────────────────
echo -e "[2/2] Bootstrapping cluster processes (port ${PORT})"
echo -e ""

declare -a PIDS=()

while IFS= read -r host; do
    host="${host%%$'\r'}"
    [[ -z "$host" ]] && continue

    (
        if start_server "$host"; then
            printf "    Starting %-25s ${GREEN}[STARTED]${NC}\n" "$host"
        else
            printf "    Starting %-25s ${RED}[FAILED]${NC}\n" "$host"
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
echo -e " Deployment complete."
echo -e "================================================="

exit 0
