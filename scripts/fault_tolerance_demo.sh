#!/bin/bash

# ==============================================================================
# DESCRIPTION
# ==============================================================================
# Fault-tolerance demonstration helper (Google MapReduce §3.1 style).
# While a MapReduce job is running (started with deploy_commoncrawl.sh), this
# script SSHes into one or more worker machines and kills their worker.py
# process to simulate a crash. The master should detect the dead worker (lease
# timeout / TCP close), re-queue its lost MAP outputs, and the job should still
# finish with correct results.
#
# Run it in a SECOND terminal a few seconds after the deploy starts streaming
# master logs. Then re-run validate.py to confirm the output is still correct.
#
# Options:
#   -h        : Display this help message
#   -n num    : Number of workers to kill (default: 1)
#   -p port   : Restrict the kill to worker.py bound to this port (default: any)
#   -d secs   : Wait this many seconds before killing (default: 0)
#
# Usage:
#   bash scripts/fault_tolerance_demo.sh                 # kill 1 worker now
#   bash scripts/fault_tolerance_demo.sh -n 2 -d 5       # wait 5s, kill 2
# ==============================================================================

NC="\e[0m"; GREEN="\e[32m"; YELLOW="\e[33m"; RED="\e[31m"
set -uo pipefail
cd "$(dirname "$0")/.."

N_KILL="1"
PORT=""
DELAY="0"

usage() {
    echo "Usage: $0 [-h] [-n num] [-p port] [-d secs]" >&2
    exit "${1:-1}"
}

while getopts "hn:p:d:" opt; do
    case "${opt}" in
        h) usage 0 ;;
        n) N_KILL="${OPTARG}" ;;
        p) PORT="${OPTARG}" ;;
        d) DELAY="${OPTARG}" ;;
        *) usage 1 ;;
    esac
done

MACHINES_FILE="runtime/machines.txt"
SSH_OPTS="-4 -o StrictHostKeyChecking=no -o BatchMode=yes -o LogLevel=ERROR -o ConnectTimeout=5"

if [[ ! -f "$MACHINES_FILE" ]]; then
    echo -e "${RED}FATAL: ${MACHINES_FILE} not found. Deploy a job first.${NC}" >&2
    exit 1
fi

if [[ -n "$PORT" ]]; then
    PATTERN="worker\.py.*-p ${PORT}"
else
    PATTERN="worker\.py"
fi

if [[ "$DELAY" -gt 0 ]] 2>/dev/null; then
    echo -e "${YELLOW}Waiting ${DELAY}s before injecting the fault...${NC}"
    sleep "$DELAY"
fi

# The first machine in the list is the master (deploy starts the master there);
# pick workers from the rest so we never accidentally kill the master.
mapfile -t HOSTS < <(grep -v '^[[:space:]]*$' "$MACHINES_FILE")
WORKERS=("${HOSTS[@]:1}")

if [[ "${#WORKERS[@]}" -eq 0 ]]; then
    echo -e "${RED}No worker machines found (only a master?).${NC}" >&2
    exit 1
fi

killed=0
for host in "${WORKERS[@]}"; do
    [[ "$killed" -ge "$N_KILL" ]] && break
    host="${host%%$'\r'}"
    if timeout 10 ssh -n $SSH_OPTS "$host" "pkill -9 -f '${PATTERN}'" >/dev/null 2>&1; then
        echo -e "${RED}💀 Killed worker on ${host}${NC}"
    else
        echo -e "${YELLOW}No worker.py to kill on ${host} (already gone?)${NC}"
    fi
    killed=$((killed + 1))
done

echo -e ""
echo -e "${GREEN}Injected ${killed} worker failure(s).${NC} Watch the master log: it should report"
echo -e "the dead worker, re-queue its MAP output(s), and still finish the job."
exit 0
