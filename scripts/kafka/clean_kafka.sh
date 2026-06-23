#!/bin/bash

# ==============================================================================
# DESCRIPTION
# ==============================================================================
# Tear down the single-node Kafka deployment created by deploy_kafka.sh.
# Idempotent: safe to run several times.
#
# It will:
#   1. Stop the Kafka Streams WordCount app (if running).
#   2. Stop the Kafka broker (via the shipped stop script, then by PID).
#   3. Delete the local data/log directories.
#   4. With -a, also remove the downloaded Kafka install (full wipe).
#
# Options:
#   -h : Display this help message
#   -a : Also delete the extracted Kafka binaries and the downloaded .tgz
#
# Usage:
#   bash scripts/kafka/clean_kafka.sh
#   bash scripts/kafka/clean_kafka.sh -a
# ==============================================================================

set -uo pipefail
NC="\e[0m"; GREEN="\e[32m"; YELLOW="\e[33m"; RED="\e[31m"

KAFKA_VERSION="${KAFKA_VERSION:-4.3.0}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
KAFKA_BASE="${KAFKA_BASE:-/tmp/${USER}-kafka}"
KAFKA_HOME="${KAFKA_HOME:-${KAFKA_BASE}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}}"

FULL_WIPE="0"
while getopts "ha" opt; do
    case "${opt}" in
        h) echo "Usage: $0 [-h] [-a]"; exit 0 ;;
        a) FULL_WIPE="1" ;;
        *) echo "Usage: $0 [-h] [-a]" >&2; exit 1 ;;
    esac
done

echo -e "================================================="
echo -e " Cleaning Kafka deployment"
echo -e "================================================="

# 1. Stop the streams app.
if [[ -f "${KAFKA_BASE}/streams.pid" ]]; then
    kill "$(cat "${KAFKA_BASE}/streams.pid")" >/dev/null 2>&1 || true
    rm -f "${KAFKA_BASE}/streams.pid"
fi
pkill -f "streams.examples.wordcount.WordCountDemo" >/dev/null 2>&1 || true
echo -e "${GREEN}[OK]${NC} Streams app stopped."

# 2. Stop the broker.
if [[ -x "${KAFKA_HOME}/bin/kafka-server-stop.sh" ]]; then
    "${KAFKA_HOME}/bin/kafka-server-stop.sh" >/dev/null 2>&1 || true
fi
if [[ -f "${KAFKA_BASE}/broker.pid" ]]; then
    kill "$(cat "${KAFKA_BASE}/broker.pid")" >/dev/null 2>&1 || true
    rm -f "${KAFKA_BASE}/broker.pid"
fi
pkill -f "kafka.Kafka" >/dev/null 2>&1 || true
sleep 1
echo -e "${GREEN}[OK]${NC} Broker stopped."

# 3. Delete local data + logs + private config.
rm -rf "${KAFKA_BASE}/data" "${KAFKA_BASE}/broker.log" \
       "${KAFKA_BASE}/streams-wordcount.log" "${KAFKA_BASE}/server.local.properties" \
       "${KAFKA_BASE}/sample-input.txt" 2>/dev/null || true
echo -e "${GREEN}[OK]${NC} Local data/logs removed."

# 4. Optional full wipe of binaries.
if [[ "$FULL_WIPE" == "1" ]]; then
    rm -rf "$KAFKA_HOME" "${KAFKA_BASE}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" 2>/dev/null || true
    rmdir "$KAFKA_BASE" 2>/dev/null || true
    echo -e "${GREEN}[OK]${NC} Kafka binaries removed (full wipe)."
fi

echo -e ""
echo -e "${GREEN}Kafka cleanup complete.${NC}"
