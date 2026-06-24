#!/bin/bash

# ==============================================================================
# DESCRIPTION
# ==============================================================================
# Queries a live room tracker API (by default Telecom Paris).
# Parses the JSON directly to filter for ALIVE and FREE machines,
# sorting them to prioritize machines with 0 users.
# Outputs the results directly to stdout.
#
# Options:
#   -n number : Number of machines to track (default: 10)
#   -u url    : API target URL (default: Telecom Paris live tracker)
#
# Usage : 
#   ./fetch-hosts.sh > runtime/machines.txt
#   ./fetch-hosts.sh -n 5 -u "https://autre-api.fr/ajax.php"
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


# ==============================================================================
# ARGUMENTS
# ==============================================================================
N_MACHINES="10"
API_URL="https://tp.telecom-paris.fr/ajax.php"

while getopts "n:u:" opt; do
    case "${opt}" in
        n) N_MACHINES="${OPTARG}" ;;
        u) API_URL="${OPTARG}" ;;
        *) echo -e "Usage: $0 [-n number_of_machines] [-u api_url]" >&2; exit 1 ;;
    esac
done
shift $((OPTIND-1))

# Validation de l'argument -n (doit être numérique)
if [[ ! "$N_MACHINES" =~ ^[0-9]+$ ]]; then
    echo -e "${RED}Error: Le nombre spécifié (${N_MACHINES}) doit être un entier valide.${NC}" >&2
    exit 1
fi


# ==============================================================================
# CONTENT
# ==============================================================================

# Les messages d'information sont envoyés sur stderr (>&2)
echo -e "Querying API : ${API_URL}" >&2
echo -e "" >&2
printf "Gathering the ${N_MACHINES} best machines    " >&2

# 1. Requête API avec curl
raw_data=$(curl -s -m 10 -A "Mozilla/5.0" "${API_URL}")
if [[ $? -ne 0 ]] || [[ -z "${raw_data}" ]]; then
    echo -e "${RED}[FAILED]${NC}" >&2
    echo -e "" >&2
    echo -e "${RED}Error : Can't fetch from API, make sure you are actively connected to the network.${NC}" >&2
    exit 1
fi

# Vérification de la présence de l'utilitaire jq
if ! command -v jq &> /dev/null; then
    echo -e "${RED}[FAILED]${NC}" >&2
    echo -e "" >&2
    echo -e "${RED}Error: 'jq' is required but not installed.${NC}" >&2
    exit 1
fi

# 2. Extraction, filtrage et tri des données avec jq
best_machines=$(echo -e "${raw_data}" | jq -r --argjson n "$N_MACHINES" '
  .data | 
  map(select(length >= 5 and .[1] == true)) |
  map({
    name: .[0],
    score: ((.[2] | tonumber? // 999) + (.[3] | tonumber? // 999) + (.[4] | tonumber? // 999))
  }) |
  sort_by(.score) |
  .[0:$n] |
  .[].name
')

if [[ -z "${best_machines}" ]]; then
    echo -e "${RED}[FAILED]${NC}" >&2
    echo -e "" >&2
    echo -e "${RED}Error : No active machines found in JSON data.${NC}" >&2
    exit 1
fi

echo -e "${GREEN}[OK]${NC}" >&2
echo -e "" >&2
echo -e "Successfully found $(echo -e "${best_machines}" | wc -l) machines." >&2

# 3. Envoi du résultat final sur stdout
echo -e "${best_machines}" | awk '{print $1".enst.fr"}'

exit 0
