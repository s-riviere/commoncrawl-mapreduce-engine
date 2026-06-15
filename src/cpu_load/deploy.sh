#!/bin/bash

# ==============================================================================
# DESCRIPTION
# ==============================================================================
# Robust parallel deployment for WSL2 over Campus Wi-Fi.
# Uploads server.py AND machines.txt to /tmp on one lab machine,
# then SSH-starts one server process per machine in machines.txt.
# After deployment, other team members sync their local machines.txt by
# running:  python3 cpu_load/client.py <port> --sync <any-reachable-lab-machine>
#
# Usage : ./deploy.sh [port]


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
REMOTE_DIR="/tmp/slr207-group1-$USER"
TIMEOUT=5
SSH_OPTS="-4 \
  -o StrictHostKeyChecking=no \
  -o BatchMode=yes \
  -o LogLevel=ERROR \
  -o ServerAliveInterval=3 \
  -o ServerAliveCountMax=2"

upload_to_host() {
    local host="$1"
    timeout $TIMEOUT ssh -n $SSH_OPTS "$host" "mkdir -p $REMOTE_DIR; chmod 777 $REMOTE_DIR" && \
    timeout $TIMEOUT scp $SSH_OPTS "cpu_load/server.py" "${host}:~/" && \
    timeout $TIMEOUT scp $SSH_OPTS "$MACHINES_FILE" "${host}:${REMOTE_DIR}/"
}

start_server() {
    local host="$1"
    timeout $TIMEOUT ssh -n $SSH_OPTS "$host" \
    "nohup python3 ~/server.py ${PORT} > /dev/null 2>&1 & 
    sleep 0.5
    if fuser ${PORT}/tcp >/dev/null 2>&1; then
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

python3 common/get_machines.py

if [[ ! -f "$MACHINES_FILE" ]]; then
    echo -e "${RED}Error : No $MACHINES_FILE found.${NC}" >&2
    exit 1
fi

echo -e ""
echo -e "$(wc -l < "$MACHINES_FILE") machines loaded from $MACHINES_FILE"
echo -e ""

# ── Phase 1: NFS upload (one SCP is enough — home dir is shared) ──────────────
echo -e "================================================="
echo -e " Phase 1: Parallel Deployment                    "
echo -e "================================================="
echo -e ""
echo -e "[1/2] Uploading server.py (NFS) and machines.txt (/tmp)"
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

    sleep 0.1
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

    sleep 0.1
done < "$MACHINES_FILE"

for pid in "${PIDS[@]}"; do
    wait "$pid"
done

echo -e ""


# ── Final Report ──────────────────────────────────────────────────────────────
echo -e "================================================="
echo -e " Deployment complete."
echo -e ""
echo -e " machines.txt stored on: ${NFS_HOST}:${REMOTE_DIR}/"
echo -e " ATTENTION: NE PAS METTRE .enst.fr"
echo -e ""
echo -e " Verify:      python3 cpu_load/client.py ${PORT}"
echo -e " Team sync:   python3 cpu_load/client.py ${PORT} --sync ${NFS_HOST}"
echo -e "================================================="

exit 0
