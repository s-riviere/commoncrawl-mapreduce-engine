#!/bin/bash

# ==============================================================================
# DESCRIPTION
# ==============================================================================
# Cleaning / teardown script for the CommonCrawl + MapReduce workflow.
# It is the counterpart of deploy_commoncrawl.sh and is idempotent: running it
# several times is safe.
#
# For every machine in runtime/machines.txt it will, over SSH:
#   1. Kill any master.py / worker.py process (optionally only on a given port).
#   2. Remove the local intermediate MAP partitions in /tmp (LOCAL disk).
#   3. Tear down stale SSH ControlMaster sockets used by the shuffle.
#   4. Optionally (-d) delete the NFS working directory (input/output/logs).
#
# Options:
#   -h        : Display this help message
#   -p port   : Only kill the master/worker bound to this port (default: all)
#   -d        : Also delete the NFS working directory (input + output + logs)
#   -k crawl  : (unused placeholder, reserved)
#
# Usage:
#   bash scripts/kill_commoncrawl.sh
#   bash scripts/kill_commoncrawl.sh -p 54321
#   bash scripts/kill_commoncrawl.sh -d           # full wipe incl. NFS data
# ==============================================================================


# ==============================================================================
# SH CONFIGURATION
# ==============================================================================
NC="\e[0m"
GREEN="\e[32m"
YELLOW="\e[33m"
RED="\e[31m"

set -uo pipefail
cd "$(dirname "$0")/.."


# ==============================================================================
# ARGUMENTS
# ==============================================================================
MASTER_PORT=""
DELETE_NFS="0"

usage() {
    local exit_code="${1:-1}"
    echo "Usage: $0 [-h] [-p port] [-d]" >&2
    echo "  -h : Display this help message" >&2
    echo "  -p : Only kill processes bound to this port (default: all MapReduce procs)" >&2
    echo "  -d : Also delete the NFS working directory (input/output/logs)" >&2
    exit "$exit_code"
}

while getopts "hp:d" opt; do
    case "${opt}" in
        h) usage 0 ;;
        p) MASTER_PORT="${OPTARG}" ;;
        d) DELETE_NFS="1" ;;
        *) usage 1 ;;
    esac
done


# ==============================================================================
# CONTENT
# ==============================================================================
MACHINES_FILE="runtime/machines.txt"
DIR_NAME="slr207-group1-commoncrawl-${USER}"
NFS_DIR="~/${DIR_NAME}"
TMP_DIR="/tmp/${DIR_NAME}"

SSH_OPTS="-4 \
  -o StrictHostKeyChecking=no \
  -o BatchMode=yes \
  -o LogLevel=ERROR \
  -o ConnectTimeout=5 \
  -o ServerAliveInterval=3 \
  -o ServerAliveCountMax=2"

if [[ ! -f "$MACHINES_FILE" ]]; then
    echo -e "${RED}FATAL: ${MACHINES_FILE} not found. Run a deploy first (it generates the machine list).${NC}" >&2
    exit 1
fi

# Build the pkill pattern: by default kill any master/worker; with -p narrow it.
if [[ -n "$MASTER_PORT" ]]; then
    if [[ ! "$MASTER_PORT" =~ ^[0-9]+$ ]]; then
        echo -e "${RED}Error: port (${MASTER_PORT}) must be numeric.${NC}" >&2
        exit 1
    fi
    KILL_PATTERN="(master|worker)\.py.*-p ${MASTER_PORT}"
else
    KILL_PATTERN="(master|worker)\.py"
fi

echo -e "================================================="
echo -e " Cleaning CommonCrawl + MapReduce deployment"
echo -e "================================================="
echo -e "Pattern : ${KILL_PATTERN}"
echo -e "NFS wipe: $([[ "$DELETE_NFS" == "1" ]] && echo yes || echo no)"
echo -e ""

clean_host() {
    local host="$1"

    # Kill processes, remove /tmp intermediates and stale ssh control sockets.
    local remote_cmd="pkill -f '${KILL_PATTERN}' 2>/dev/null
        rm -rf ${TMP_DIR} 2>/dev/null
        rm -f /tmp/ssh-ctrl-* /tmp/commoncrawl-*.txt /tmp/worker_*.log 2>/dev/null"

    if [[ "$DELETE_NFS" == "1" ]]; then
        remote_cmd="${remote_cmd}
        rm -rf ${NFS_DIR} 2>/dev/null"
    fi
    # pkill returns non-zero when nothing matched; treat the host as cleaned anyway.
    remote_cmd="${remote_cmd}
        exit 0"

    if timeout 15 ssh -n $SSH_OPTS "$host" "$remote_cmd" >/dev/null 2>&1; then
        printf "Cleaning %-28s ${GREEN}[OK]${NC}\n" "$host" >&2
    else
        printf "Cleaning %-28s ${YELLOW}[UNREACHABLE]${NC}\n" "$host" >&2
    fi
}

declare -a PIDS=()
while IFS= read -r host; do
    host="${host%%$'\r'}"
    [[ -z "$host" ]] && continue
    clean_host "$host" &
    PIDS+=($!)
done < "$MACHINES_FILE"

for pid in "${PIDS[@]}"; do
    wait "$pid"
done

echo -e ""
echo -e "${GREEN}Cleanup complete.${NC}"
exit 0
