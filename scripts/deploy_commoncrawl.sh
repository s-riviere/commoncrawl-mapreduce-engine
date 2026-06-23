#!/bin/bash

# ==============================================================================
# DESCRIPTION
# ==============================================================================
# Robust parallel deployment for WSL2 over Campus Wi-Fi.
# Uploads remote files (NFS) on the first available lab machine and starts Master.
# Then SSH-starts one server process per machine in machines.txt.
#
# Options:
#   -h      : Display this help message
#   -p port : Master port number (default: 54321)
#   -w num  : Number of worker machines to deploy (default: 10)
#   -r num  : Number of reducers for the Master (default: 10)
#   -s num  : Number of splits to process (download only if input has fewer)
#   -j job  : Analysis to run: wordcount|lang|wordlen|bigram (default: wordcount)
#
# Usage : 
#   ./deploy.sh -h
#   ./deploy.sh -p 60000 -w 5 -r 4
#   ./deploy.sh -p 54321 -w 12 -r 12 -s 20
#   ./deploy.sh -w 8 -r 8 -j lang
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
MASTER_PORT="54321"
N_REDUCERS="10"
N_SPLITS="10"
N_WORKERS="10"
JOB="wordcount"

usage() {
    local exit_code="${1:-1}"
    echo "Usage: $0 [-h] [-p master_port] [-w n_workers] [-r n_reducers] [-s cc_splits] [-j job]" >&2
    echo "" >&2
    echo "Options:" >&2
    echo "  -h : Display this help message" >&2
    echo "  -p : Master port number (default: 54321)" >&2
    echo "  -w : Number of worker machines to deploy (default: 10)" >&2
    echo "  -r : Number of reducers (default: 10)" >&2
    echo "  -s : Number of splits to process (default: 10)" >&2
    echo "  -j : Analysis job wordcount|lang|wordlen|bigram (default: wordcount)" >&2
    exit "$exit_code"
}

# Analyse des options avec getopts
while getopts "hp:w:r:s:j:" opt; do
    case "${opt}" in
        h) usage 0 ;;
        p) MASTER_PORT="${OPTARG}" ;;
        w) N_WORKERS="${OPTARG}" ;;
        r) N_REDUCERS="${OPTARG}" ;;
        s) N_SPLITS="${OPTARG}" ;;
        j) JOB="${OPTARG}" ;;
        *) usage 1 ;;
    esac
done
shift $((OPTIND-1))

# Validation du port (doit être numérique)
if [[ ! "$MASTER_PORT" =~ ^[0-9]+$ ]]; then
    echo -e "${RED}Error: Le port spécifié (${MASTER_PORT}) doit être un nombre valide.${NC}" >&2
    exit 1
fi

# Validation du nombre de reducers (doit être numérique)
if [[ ! "$N_REDUCERS" =~ ^[0-9]+$ ]]; then
    echo -e "${RED}Error: Le nombre de reducers spécifié (${N_REDUCERS}) doit être un entier valide.${NC}" >&2
    exit 1
fi

# Validation du nombre de splits (doit être numérique)
if [[ ! "$N_SPLITS" =~ ^[0-9]+$ ]]; then
    echo -e "${RED}Error: Le nombre de splits spécifié (${N_SPLITS}) doit être un entier valide.${NC}" >&2
    exit 1
fi

# Validation du nombre de workers (doit être numérique)
if [[ ! "$N_WORKERS" =~ ^[0-9]+$ ]]; then
    echo -e "${RED}Error: Le nombre de workers spécifié (${N_WORKERS}) doit être un entier valide.${NC}" >&2
    exit 1
fi

# Validation du job (doit appartenir à la liste supportée)
case "$JOB" in
    wordcount|lang|wordlen|bigram) ;;
    *)
        echo -e "${RED}Error: Job invalide (${JOB}). Choix: wordcount, lang, wordlen, bigram.${NC}" >&2
        exit 1
        ;;
esac


# ==============================================================================
# CONTENT
# ==============================================================================

# ── Constants and functions ─────────────────────────────────────────────
MACHINES_FILE="runtime/machines.txt"
FILES_TO_UPLOAD=("src/map_reduce/master.py" "src/map_reduce/worker.py" "src/map_reduce/download_commoncrawl.py")

DIR_NAME="slr207-group1-commoncrawl-${USER}"
NFS_DIR="~/${DIR_NAME}"
TMP_DIR="/tmp/${DIR_NAME}"

NFS_INPUT_DIR="${NFS_DIR}/input"
NFS_OUTPUT_DIR="${NFS_DIR}/output"

LOCAL_MAP_DIR="${TMP_DIR}/map-outputs"
REMOTE_LOG_DIR="${TMP_DIR}/logs/$(date +%Y%m%d_%H%M%S)"

SLEEP=0.5
SSH_OPTS="-4 \
  -o StrictHostKeyChecking=no \
  -o BatchMode=yes \
  -o LogLevel=ERROR \
  -o ServerAliveInterval=3 \
  -o ServerAliveCountMax=2"

logf() {
    printf "$@" >&2
}

upload_and_download() {
    local host="$1"
    local existing_splits=""
    
    # Étape 1 : Connexion et création du dossier
    logf "Connecting to %-25s" "${host}..."
    if timeout 10 ssh -n $SSH_OPTS "$host" "mkdir -p ${NFS_DIR}" >/dev/null 2>&1; then
        logf "${GREEN}[OK]${NC}\n"
    else
        logf "${RED}[FAILED]${NC}\n"
        return 1
    fi
    
    # Étape 2 : Téléversement des fichiers indispensables
    logf "Uploading files on NFS%-17s" "..."
    if timeout 10 scp $SSH_OPTS "${FILES_TO_UPLOAD[@]}" "${host}:${NFS_DIR}/" >/dev/null 2>&1; then
        logf "${GREEN}[OK]${NC}\n"
    else
        logf "${RED}[FAILED]${NC}\n"
        return 1
    fi
    
    # Étape 3 : Téléchargement conditionnel si le stock de splits est insuffisant
    logf "Checking NFS splits%-20s" "..."
    existing_splits="$(
        timeout 10 ssh -n $SSH_OPTS "$host" \
        "mkdir -p ${NFS_INPUT_DIR} &&
        find ${NFS_INPUT_DIR} -maxdepth 1 -type f -name 'commoncrawl-*.txt' 2>/dev/null |
        wc -l" 2>/dev/null | tr -d '[:space:]'
    )"

    if [[ ! "$existing_splits" =~ ^[0-9]+$ ]]; then
        logf "${RED}[FAILED]${NC}\n"
        return 1
    fi

    if [[ "$existing_splits" -lt "$N_SPLITS" ]]; then
        logf "${YELLOW}[${existing_splits}/${N_SPLITS}]${NC}\n"
        if timeout 300 ssh -n $SSH_OPTS "$host" \
        "python3 -u ${NFS_DIR}/download_commoncrawl.py -o ${NFS_INPUT_DIR} -n ${N_SPLITS} --missing-only"; then
            logf "Downloading splits on NFS%-14s ${GREEN}[OK]${NC}\n" "..."
        else
            logf "Downloading splits on NFS%-14s ${RED}[FAILED]${NC}\n" "..."
            return 1
        fi
    else
        logf "${GREEN}[${existing_splits}/${N_SPLITS}]${NC}\n"
    fi
    
    return 0
}

start_master() {
    local host="$1"
    local master_log_file="${REMOTE_LOG_DIR}/master_${host}.log"
    
    logf "Starting master process%-16s" "..."

    if timeout 25 ssh -n $SSH_OPTS "$host" \
    "rm -rf ${NFS_OUTPUT_DIR} &&
    mkdir -p ${NFS_OUTPUT_DIR} ${REMOTE_LOG_DIR} &&
    nohup python3 -u ${NFS_DIR}/master.py -p ${MASTER_PORT} -r ${N_REDUCERS} -s ${N_SPLITS} -j ${JOB} > ${master_log_file} 2>&1 &
    for i in {1..15}; do
        if pgrep -f \"master.py -p ${MASTER_PORT}\" >/dev/null; then
            exit 0
        fi
        sleep 1
    done
    exit 1" >/dev/null 2>&1; then
        logf "${GREEN}[STARTED]${NC}\n"
        return 0
    else
        logf "${RED}[FAILED]${NC}\n"
        return 1
    fi
}

start_worker() {
    local host="$1"
    local master_host="$2"
    local master_port="$3"
    local worker_log_file="${REMOTE_LOG_DIR}/worker_${host}.log"
    
    if timeout 25 ssh -n $SSH_OPTS "$host" \
    "mkdir -p ${REMOTE_LOG_DIR} &&
    nohup python3 -u ${NFS_DIR}/worker.py -h ${master_host} -p ${master_port} -i ${NFS_INPUT_DIR} -o ${NFS_OUTPUT_DIR} -l ${LOCAL_MAP_DIR} -j ${JOB} --worker-id ${host} > ${worker_log_file} 2>&1 &
    for i in {1..15}; do
        if pgrep -f \"worker.py -h ${master_host} -p ${master_port}\" >/dev/null; then
            exit 0
        fi
        sleep 1
    done
    exit 1" >/dev/null 2>&1; then
        logf "Starting worker %-25s ${GREEN}[STARTED]${NC}\n" "$host"
        return 0
    else
        logf "Starting worker %-25s ${RED}[FAILED]${NC}\n" "$host"
        return 1
    fi
}


# ── Phase 0: fetch alive machines ────────────────────────────────────────────────────
echo -e "================================================="
echo -e " Phase 0 : Fetching Alive Machines from API      "
echo -e "================================================="
echo -e ""

mkdir -p "$(dirname "$MACHINES_FILE")"
bash scripts/get_machines.sh -n "$((N_WORKERS + 1))" > "$MACHINES_FILE" || exit 1

# ── Validate machines count ──────────────────────────────────────────────────
NEEDED="$((N_WORKERS + 1))"  # 1 master + N_WORKERS workers
AVAILABLE="$(grep -c '[^[:space:]]' "${MACHINES_FILE}" 2>/dev/null || echo 0)"

if [[ "$AVAILABLE" -lt "$NEEDED" ]]; then
    echo -e "${RED}FATAL: Not enough machines in ${MACHINES_FILE}: ${AVAILABLE} available, ${NEEDED} needed (1 master + ${N_WORKERS} workers).${NC}" >&2
    exit 1
fi

if [[ "$AVAILABLE" -gt "$NEEDED" ]]; then
    echo -e "${YELLOW}Trimming machines list from ${AVAILABLE} to ${NEEDED} (1 master + ${N_WORKERS} workers).${NC}"
    head -n "${NEEDED}" "${MACHINES_FILE}" > "${MACHINES_FILE}.tmp" && mv "${MACHINES_FILE}.tmp" "${MACHINES_FILE}"
fi

echo -e ""


# ── Phase 1: NFS upload and Master Bootstrap ──────────────────────────────────
echo -e "================================================="
echo -e " Phase 1 : NFS Upload & Master Bootstrap         "
echo -e "================================================="
echo -e ""
echo -e "Processing files, dependencies and starting Master on first available machine."
echo -e "Requested splits : ${N_SPLITS} (download only if input has fewer)."
echo -e "Analysis job     : ${JOB}."
echo -e ""

master_host=""

while IFS= read -r worker_host; do
    worker_host="${worker_host%%$'\r'}"
    [[ -z "$worker_host" ]] && continue
        
    if upload_and_download "$worker_host"; then
        if start_master "$worker_host"; then
            master_host="$worker_host"
            break
        fi
    fi

    sleep "$SLEEP"
done < "$MACHINES_FILE"

echo -e ""

if [[ -z "$master_host" ]]; then
    echo -e "${RED}FATAL: Could not initialize master — all tested machines are unreachable or failed.${NC}" >&2
    exit 1
fi

echo -e "${YELLOW}Master infrastructure successfully established on ${master_host}:${MASTER_PORT}${NC}."
echo -e "${YELLOW}Remote logs directory: ${REMOTE_LOG_DIR}${NC}."
echo -e ""

# ── Phase 2: Workers Bootstrap ───────────────────────────────────────────
echo -e "================================================="
echo -e " Phase 2 : Workers Bootstrap                     "
echo -e "================================================="
echo -e ""
echo -e "Starting worker processes on all available machines."
echo -e ""

declare -a PIDS=()

while IFS= read -r worker_host; do
    worker_host="${worker_host%%$'\r'}"
    [[ -z "$worker_host" ]] && continue

    if [[ "$worker_host" == "$master_host" ]]; then
        continue
    fi

    start_worker "$worker_host" "$master_host" "$MASTER_PORT" &
    PIDS+=($!)

    sleep "$SLEEP"
done < "$MACHINES_FILE"

for pid in "${PIDS[@]}"; do
    wait "$pid"
done

echo -e ""


# ── Phase 3: Monitoring ───────────────────────────────────────────────────────
echo -e "================================================="
echo -e " Phase 3 : Monitoring MapReduce Execution        "
echo -e "================================================="
echo -e ""
echo -e "Streaming master logs. Exits automatically when master finishes. Press Ctrl+C to detach (job continues in background)."
echo -e ""

master_log_file="${REMOTE_LOG_DIR}/master_${master_host}.log"

# Suit le fichier de log en direct jusqu'à la fin du process master.
# --pid arrête tail automatiquement quand master s'éteint.
ssh -t $SSH_OPTS "$master_host" \
    "master_pid=\$(pgrep -f 'master.py -p ${MASTER_PORT}' | head -n 1)
    if [ -n \"\$master_pid\" ]; then
        tail -n +1 -f --pid=\$master_pid ${master_log_file}
    else
        echo '[MONITOR] Master already finished.'
        cat ${master_log_file}
    fi"

echo -e ""


# ── Final Report ──────────────────────────────────────────────────────────────
echo -e "================================================="
echo -e " Deployment complete."
echo -e "================================================="

exit 0
