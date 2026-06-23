#!/bin/bash

# ==============================================================================
# DESCRIPTION
# ==============================================================================
# Deploy a single-node Apache Kafka broker (KRaft mode) from the official
# downloaded release — NO Docker, NO root privileges. Everything lives under a
# per-user directory on the LOCAL disk (/tmp), so it works on the school
# machines and never touches the overloaded NFS.
#
# Mirrors the official quickstart:
#   https://kafka.apache.org/43/getting-started/quickstart/  (steps 1-2)
#
# Steps performed:
#   1. Download + extract the Kafka release (cached; skipped if present).
#   2. Create a private config with local log.dirs and the chosen port.
#   3. Generate a cluster UUID and format the KRaft log directory.
#   4. Start the broker in the background (nohup) and wait until it is ready.
#
# Environment overrides:
#   KAFKA_VERSION (default 4.3.0)   SCALA_VERSION (default 2.13)
#   KAFKA_BASE    (default /tmp/<user>-kafka)
#   KAFKA_PORT    (default 9092)    KAFKA_MIRROR  (download URL)
#
# Usage:
#   bash scripts/kafka/deploy_kafka.sh
#   KAFKA_PORT=9095 bash scripts/kafka/deploy_kafka.sh
# ==============================================================================

set -uo pipefail
NC="\e[0m"; GREEN="\e[32m"; YELLOW="\e[33m"; RED="\e[31m"

KAFKA_VERSION="${KAFKA_VERSION:-4.3.0}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
KAFKA_TGZ="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
KAFKA_DIRNAME="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"

KAFKA_BASE="${KAFKA_BASE:-/tmp/${USER}-kafka}"
export KAFKA_HOME="${KAFKA_BASE}/${KAFKA_DIRNAME}"
KAFKA_DATA="${KAFKA_BASE}/data"
KAFKA_CONF="${KAFKA_BASE}/server.local.properties"
KAFKA_LOG="${KAFKA_BASE}/broker.log"
KAFKA_PORT="${KAFKA_PORT:-9092}"
CTRL_PORT="$((KAFKA_PORT + 1))"
KAFKA_MIRROR="${KAFKA_MIRROR:-https://downloads.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}}"

echo -e "================================================="
echo -e " Deploying Kafka ${KAFKA_VERSION} (KRaft, no Docker/root)"
echo -e "================================================="
echo -e "Install dir : ${KAFKA_HOME}"
echo -e "Data dir    : ${KAFKA_DATA}"
echo -e "Broker port : ${KAFKA_PORT}  (controller ${CTRL_PORT})"
echo -e ""

# ── Prerequisite: Java 17+ ────────────────────────────────────────────────────
if ! command -v java >/dev/null 2>&1; then
    echo -e "${RED}FATAL: java not found. Kafka needs Java 17+ on PATH.${NC}" >&2
    exit 1
fi

mkdir -p "$KAFKA_BASE"

# ── Step 1: download + extract (cached) ───────────────────────────────────────
if [[ ! -d "$KAFKA_HOME" ]]; then
    if [[ ! -f "${KAFKA_BASE}/${KAFKA_TGZ}" ]]; then
        echo -e "Downloading ${KAFKA_TGZ} ..."
        if command -v curl >/dev/null 2>&1; then
            curl -fSL "$KAFKA_MIRROR" -o "${KAFKA_BASE}/${KAFKA_TGZ}" || {
                echo -e "${RED}Download failed from ${KAFKA_MIRROR}${NC}" >&2; exit 1; }
        else
            wget -O "${KAFKA_BASE}/${KAFKA_TGZ}" "$KAFKA_MIRROR" || {
                echo -e "${RED}Download failed from ${KAFKA_MIRROR}${NC}" >&2; exit 1; }
        fi
    fi
    echo -e "Extracting ..."
    tar -xzf "${KAFKA_BASE}/${KAFKA_TGZ}" -C "$KAFKA_BASE"
fi
echo -e "${GREEN}[OK]${NC} Kafka binaries ready."

# ── Already running? ──────────────────────────────────────────────────────────
if "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "localhost:${KAFKA_PORT}" --list >/dev/null 2>&1; then
    echo -e "${YELLOW}A broker is already responding on localhost:${KAFKA_PORT}. Nothing to do.${NC}"
    exit 0
fi

# ── Step 2: private config (local log.dirs + chosen ports) ────────────────────
cp "${KAFKA_HOME}/config/server.properties" "$KAFKA_CONF"
mkdir -p "$KAFKA_DATA"
# Override log dir and listeners without editing the shipped file.
sed -i "s|^log.dirs=.*|log.dirs=${KAFKA_DATA}|" "$KAFKA_CONF"
sed -i "s|^listeners=.*|listeners=PLAINTEXT://:${KAFKA_PORT},CONTROLLER://:${CTRL_PORT}|" "$KAFKA_CONF"
sed -i "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://localhost:${KAFKA_PORT}|" "$KAFKA_CONF"
# controller.quorum.voters references the node id (default 1) and controller port.
if grep -q '^controller.quorum.voters=' "$KAFKA_CONF"; then
    sed -i "s|^controller.quorum.voters=.*|controller.quorum.voters=1@localhost:${CTRL_PORT}|" "$KAFKA_CONF"
fi

# ── Step 3: format KRaft storage ──────────────────────────────────────────────
KAFKA_CLUSTER_ID="$("${KAFKA_HOME}/bin/kafka-storage.sh" random-uuid)"
echo -e "Cluster UUID: ${KAFKA_CLUSTER_ID}"
"${KAFKA_HOME}/bin/kafka-storage.sh" format --standalone -t "$KAFKA_CLUSTER_ID" -c "$KAFKA_CONF" >/dev/null

# ── Step 4: start broker in the background ────────────────────────────────────
echo -e "Starting broker (log: ${KAFKA_LOG}) ..."
nohup "${KAFKA_HOME}/bin/kafka-server-start.sh" "$KAFKA_CONF" > "$KAFKA_LOG" 2>&1 &
echo $! > "${KAFKA_BASE}/broker.pid"

# Wait until the broker answers API calls.
for i in $(seq 1 30); do
    if "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "localhost:${KAFKA_PORT}" --list >/dev/null 2>&1; then
        echo -e "${GREEN}[READY]${NC} Broker is up on localhost:${KAFKA_PORT}."
        echo -e ""
        echo -e "Next: bash scripts/kafka/run_wordcount.sh <input.txt>"
        exit 0
    fi
    sleep 1
done

echo -e "${RED}FATAL: broker did not become ready in time. Tail of ${KAFKA_LOG}:${NC}" >&2
tail -n 20 "$KAFKA_LOG" >&2
exit 1
