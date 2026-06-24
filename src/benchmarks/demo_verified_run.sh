#!/bin/bash

# ==============================================================================
# DESCRIPTION
# ==============================================================================
# Variante "demo" de deploy_commoncrawl.sh, pensee pour une presentation fiable.
#
# Difference unique avec le script officiel :
#   * il NE contacte PAS l'API (src/deploy/get_machines.sh). Il lit une liste de
#     machines DEJA VERIFIEES (testees joignables par SSH au prealable), ce qui
#     evite de tomber sur des machines bloquees pendant les TP/concours.
#   * il demarre les workers SEQUENTIELLEMENT (parallelisme SSH reduit).
#
# Tout le reste (chemins NFS/tmp, commandes distantes, processus master/worker)
# est IDENTIQUE a deploy_commoncrawl.sh, donc kill_commoncrawl.sh et validate.py
# restent compatibles sans changement.
#
# IMPORTANT : aucune commande n'est executee sur la passerelle SSH (ProxyJump) ;
# chaque connexion exécute son travail sur la machine tp-* cible, la passerelle
# ne sert que de tunnel.
#
# Options :
#   -h        : Affiche cette aide
#   -p port   : Port du master (defaut: 54321)
#   -w num    : Nombre de workers (defaut: 4)
#   -r num    : Nombre de reducers (defaut: 4)
#   -s num    : Nombre de splits CommonCrawl (telecharge seulement les manquants) (defaut: 8)
#   -j job    : wordcount|lang|wordlen|bigram (defaut: wordcount)
#   -m fichier: Liste de machines VERIFIEES (defaut: runtime/machines.verified.txt)
#
# Usage :
#   bash src/benchmarks/demo_verified_run.sh                       # 4 workers, wordcount
#   bash src/benchmarks/demo_verified_run.sh -w 8 -r 8 -s 20 -j bigram
#   bash src/benchmarks/demo_verified_run.sh -m runtime/machines.verified.txt -w 12
# ==============================================================================


# ==============================================================================
# SH CONFIGURATION
# ==============================================================================
NC="\e[0m"
GREEN="\e[32m"
YELLOW="\e[33m"
RED="\e[31m"

set -uo pipefail
cd "$(dirname "$0")/../.."


# ==============================================================================
# ARGUMENTS
# ==============================================================================
MASTER_PORT="54321"
N_REDUCERS="4"
N_SPLITS="8"
N_WORKERS="4"
JOB="wordcount"
VERIFIED_FILE="runtime/machines.verified.txt"

usage() {
    local exit_code="${1:-1}"
    echo "Usage: $0 [-h] [-p master_port] [-w n_workers] [-r n_reducers] [-s cc_splits] [-j job] [-m verified_file]" >&2
    echo "" >&2
    echo "Options:" >&2
    echo "  -h : Affiche cette aide" >&2
    echo "  -p : Port du master (defaut: 54321)" >&2
    echo "  -w : Nombre de workers (defaut: 4)" >&2
    echo "  -r : Nombre de reducers (defaut: 4)" >&2
    echo "  -s : Nombre de splits (defaut: 8)" >&2
    echo "  -j : Job wordcount|lang|wordlen|bigram (defaut: wordcount)" >&2
    echo "  -m : Liste de machines verifiees (defaut: runtime/machines.verified.txt)" >&2
    exit "$exit_code"
}

while getopts "hp:w:r:s:j:m:" opt; do
    case "${opt}" in
        h) usage 0 ;;
        p) MASTER_PORT="${OPTARG}" ;;
        w) N_WORKERS="${OPTARG}" ;;
        r) N_REDUCERS="${OPTARG}" ;;
        s) N_SPLITS="${OPTARG}" ;;
        j) JOB="${OPTARG}" ;;
        m) VERIFIED_FILE="${OPTARG}" ;;
        *) usage 1 ;;
    esac
done
shift $((OPTIND-1))

for val in "$MASTER_PORT" "$N_REDUCERS" "$N_SPLITS" "$N_WORKERS"; do
    if [[ ! "$val" =~ ^[0-9]+$ ]]; then
        echo -e "${RED}Error: '${val}' doit etre un entier valide.${NC}" >&2
        exit 1
    fi
done

case "$JOB" in
    wordcount|lang|wordlen|bigram) ;;
    *)
        echo -e "${RED}Error: Job invalide (${JOB}). Choix: wordcount, lang, wordlen, bigram.${NC}" >&2
        exit 1
        ;;
esac

if [[ ! -f "$VERIFIED_FILE" ]]; then
    echo -e "${RED}FATAL: liste de machines verifiees introuvable: ${VERIFIED_FILE}${NC}" >&2
    echo -e "${YELLOW}Generez-la d'abord (machines testees joignables par SSH).${NC}" >&2
    exit 1
fi


# ==============================================================================
# CONTENT (identique a deploy_commoncrawl.sh, hormis la source des machines)
# ==============================================================================
MACHINES_FILE="runtime/machines.txt"
FILES_TO_UPLOAD=("src/mapreduce/master.py" "src/mapreduce/worker.py" "src/mapreduce/download_commoncrawl.py")

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

    logf "Connecting to %-25s" "${host}..."
    if timeout 10 ssh -n $SSH_OPTS "$host" "mkdir -p ${NFS_DIR}" >/dev/null 2>&1; then
        logf "${GREEN}[OK]${NC}\n"
    else
        logf "${RED}[FAILED]${NC}\n"
        return 1
    fi

    logf "Uploading files on NFS%-17s" "..."
    if timeout 10 scp $SSH_OPTS "${FILES_TO_UPLOAD[@]}" "${host}:${NFS_DIR}/" >/dev/null 2>&1; then
        logf "${GREEN}[OK]${NC}\n"
    else
        logf "${RED}[FAILED]${NC}\n"
        return 1
    fi

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


# ── Phase 0: liste VERIFIEE (pas d'appel API) ─────────────────────────────────
echo -e "================================================="
echo -e " Phase 0 : Using VERIFIED machine list (no API)  "
echo -e "================================================="
echo -e ""

NEEDED="$((N_WORKERS + 1))"  # 1 master + N_WORKERS workers
AVAILABLE="$(grep -c '[^[:space:]]' "${VERIFIED_FILE}" 2>/dev/null || echo 0)"

if [[ "$AVAILABLE" -lt "$NEEDED" ]]; then
    echo -e "${RED}FATAL: pas assez de machines verifiees: ${AVAILABLE} dispo, ${NEEDED} requises (1 master + ${N_WORKERS} workers).${NC}" >&2
    exit 1
fi

mkdir -p "$(dirname "$MACHINES_FILE")"
grep '[^[:space:]]' "${VERIFIED_FILE}" | head -n "${NEEDED}" > "${MACHINES_FILE}"
echo -e "${GREEN}Selectionne ${NEEDED} machines verifiees (sur ${AVAILABLE}) dans ${MACHINES_FILE}.${NC}"
echo -e ""


# ── Phase 1: NFS upload and Master Bootstrap ──────────────────────────────────
echo -e "================================================="
echo -e " Phase 1 : NFS Upload & Master Bootstrap         "
echo -e "================================================="
echo -e ""
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
    echo -e "${RED}FATAL: Could not initialize master — machines verifiees injoignables.${NC}" >&2
    exit 1
fi

echo -e "${YELLOW}Master etabli sur ${master_host}:${MASTER_PORT}${NC}."
echo -e "${YELLOW}Remote logs directory: ${REMOTE_LOG_DIR}${NC}."
echo -e ""


# ── Phase 2: Workers Bootstrap (SEQUENTIEL, parallelisme reduit) ──────────────
echo -e "================================================="
echo -e " Phase 2 : Workers Bootstrap (sequential)        "
echo -e "================================================="
echo -e ""

while IFS= read -r worker_host; do
    worker_host="${worker_host%%$'\r'}"
    [[ -z "$worker_host" ]] && continue
    [[ "$worker_host" == "$master_host" ]] && continue

    start_worker "$worker_host" "$master_host" "$MASTER_PORT"
    sleep "$SLEEP"
done < "$MACHINES_FILE"

echo -e ""


# ── Phase 3: Monitoring ───────────────────────────────────────────────────────
echo -e "================================================="
echo -e " Phase 3 : Monitoring MapReduce Execution        "
echo -e "================================================="
echo -e ""
echo -e "Streaming master logs. Exits automatically when master finishes. Press Ctrl+C to detach (job continues in background)."
echo -e ""

master_log_file="${REMOTE_LOG_DIR}/master_${master_host}.log"

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
echo -e " Demo run complete."
echo -e "================================================="
echo -e ""
echo -e "${YELLOW}Master         :${NC} ${master_host}:${MASTER_PORT}"
echo -e "${YELLOW}Sortie (NFS)   :${NC} ${NFS_OUTPUT_DIR}"
echo -e "${YELLOW}Valider        :${NC} voir TESTING.md section 5.3 (validate.py)"
echo -e "${YELLOW}Nettoyer       :${NC} bash src/deploy/kill_commoncrawl.sh        (garde les splits pour la démo)"
echo -e "${YELLOW}Wipe complet   :${NC} bash src/deploy/kill_commoncrawl.sh -d     (supprime aussi le dossier NFS)"

exit 0
