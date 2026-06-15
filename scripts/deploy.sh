#!/bin/bash

# ==============================================================================
# DESCRIPTION
# ==============================================================================
# Robust parallel deployment for WSL2 over Campus Wi-Fi.
# Uploads remote files (NFS) and machines.txt (/tmp) on one lab machine.
# Then SSH-starts one server process per machine in machines.txt.
#
# Arguments:
#   [type]  : Type of job to deploy (cpu_load, wordcount).
#   [port]  : Network port number for the server process to listen on.
#
# Usage : ./deploy.sh [type] [port]
#
# Examples:
#   ./deploy.sh cpu_load 9090
#   ./deploy.sh wordcount 5000
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
TYPE="${1:-cpu_load}"
PORT="${2:-54321}"

case "$TYPE" in
    "cpu_load")
        FILES_TO_UPLOAD=("src/cpu_load/server.py")
        SCRIPT_TO_START="server.py"
        ;;
    "wordcount")
        FILES_TO_UPLOAD=("src/map_reduce/worker.py" "src/map_reduce/wordcount.py")
        SCRIPT_TO_START="worker.py"
        ;;
    *)
        echo -e "${RED}Erreur : Type de job inconnu '$TYPE'. Valeurs valides : cpu_load, wordcount${NC}" >&2
        exit 1
        ;;
esac


# ==============================================================================
# CONTENT
# ==============================================================================
GET_MACHINES_SCRIPT="src/common/get_machines.py"
MACHINES_FILE="runtime/machines.txt"
REMOTE_LOCAL_DIR="/tmp/slr207-group1-bis"
REMOTE_NFS_DIR="~/slr207-group1-bis"
TIMEOUT=5
SLEEP=0.5
SSH_OPTS="-4 \
  -o StrictHostKeyChecking=no \
  -o BatchMode=yes \
  -o LogLevel=ERROR \
  -o ServerAliveInterval=3 \
  -o ServerAliveCountMax=2"

upload_to_host() {
    local host="$1"
    timeout $TIMEOUT ssh -n $SSH_OPTS "$host" "mkdir -p ${REMOTE_LOCAL_DIR}; chmod 777 ${REMOTE_LOCAL_DIR}" && \
    timeout $TIMEOUT scp $SSH_OPTS "$MACHINES_FILE" "${host}:${REMOTE_LOCAL_DIR}/" && \
    timeout $TIMEOUT ssh -n $SSH_OPTS "$host" "mkdir -p ${REMOTE_NFS_DIR}" && \
    timeout $TIMEOUT scp $SSH_OPTS "${FILES_TO_UPLOAD[@]}" "${host}:${REMOTE_NFS_DIR}/"
}

start_server() {
    local host="$1"
    timeout $TIMEOUT ssh -n $SSH_OPTS "$host" \
    "nohup python3 ${REMOTE_NFS_DIR}/${SCRIPT_TO_START} ${PORT} > /dev/null 2>&1 & 
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

python3 "$GET_MACHINES_SCRIPT"

if [[ ! -f "$MACHINES_FILE" ]]; then
    echo -e "${RED}Error : No $MACHINES_FILE found.${NC}" >&2
    exit 1
fi

echo -e ""
echo -e "$(wc -l < "$MACHINES_FILE") machines loaded."
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
