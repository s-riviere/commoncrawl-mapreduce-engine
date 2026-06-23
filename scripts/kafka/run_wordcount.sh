#!/bin/bash

# ==============================================================================
# DESCRIPTION
# ==============================================================================
# Run the official Kafka Streams WordCount demo end-to-end on a single broker
# (started by deploy_kafka.sh). It feeds a text file (e.g. a CommonCrawl split)
# through the streaming pipeline and prints the resulting word counts.
#
# Mirrors the official streams quickstart (steps 3-5):
#   https://kafka.apache.org/43/streams/quickstart/
# Uses the built-in demo class:
#   org.apache.kafka.streams.examples.wordcount.WordCountDemo
#
# Pipeline:
#   input file ──producer──▶ streams-plaintext-input
#                              │ (Kafka Streams WordCountDemo)
#                              ▼
#                        streams-wordcount-output ──consumer──▶ stdout (top-N)
#
# Arguments:
#   $1 : input text file (default: a tiny built-in sample)
#
# Environment overrides:
#   KAFKA_BASE (default /tmp/<user>-kafka)   KAFKA_PORT (default 9092)
#   TOP_N      (default 20)   CONSUME_MS (default 15000)
#
# Usage:
#   bash scripts/kafka/run_wordcount.sh
#   bash scripts/kafka/run_wordcount.sh ~/slr207-group1-commoncrawl-$USER/input/commoncrawl-0000.txt
# ==============================================================================

set -uo pipefail
NC="\e[0m"; GREEN="\e[32m"; YELLOW="\e[33m"; RED="\e[31m"
cd "$(dirname "$0")/../.."   # project root

KAFKA_VERSION="${KAFKA_VERSION:-4.3.0}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
KAFKA_BASE="${KAFKA_BASE:-/tmp/${USER}-kafka}"
KAFKA_HOME="${KAFKA_HOME:-${KAFKA_BASE}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
BOOTSTRAP="localhost:${KAFKA_PORT}"
TOP_N="${TOP_N:-20}"
CONSUME_MS="${CONSUME_MS:-15000}"

IN_TOPIC="streams-plaintext-input"
OUT_TOPIC="streams-wordcount-output"

# Input source: a local text file (default) OR a direct Common Crawl stream via
# the Kafka source connector (--crawl <ID> [--index N], §8).
CC_CRAWL=""; CC_INDEX="0"; INPUT_FILE=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -c|--crawl) CC_CRAWL="$2"; shift 2 ;;
        -i|--index) CC_INDEX="$2"; shift 2 ;;
        *)          INPUT_FILE="$1"; shift ;;
    esac
done

BIN="${KAFKA_HOME}/bin"
if [[ ! -x "${BIN}/kafka-topics.sh" ]]; then
    echo -e "${RED}FATAL: Kafka not found at ${KAFKA_HOME}. Run deploy_kafka.sh first.${NC}" >&2
    exit 1
fi
if ! "${BIN}/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
    echo -e "${RED}FATAL: no broker on ${BOOTSTRAP}. Run deploy_kafka.sh first.${NC}" >&2
    exit 1
fi

# Built-in sample if neither a Common Crawl source nor an input file was given.
if [[ -z "$CC_CRAWL" ]]; then
    if [[ -z "$INPUT_FILE" ]]; then
        INPUT_FILE="${KAFKA_BASE}/sample-input.txt"
        cat > "$INPUT_FILE" <<'EOF'
all streams lead to kafka
hello kafka streams
join kafka summit
batch processing versus stream processing
the cat sat on the mat the cat ran
EOF
        echo -e "${YELLOW}No input given — using built-in sample (${INPUT_FILE}).${NC}"
    fi
    if [[ ! -f "$INPUT_FILE" ]]; then
        echo -e "${RED}FATAL: input file not found: ${INPUT_FILE}${NC}" >&2
        exit 1
    fi
fi

echo -e "================================================="
echo -e " Kafka Streams WordCount demo"
echo -e "================================================="
echo -e "Broker : ${BOOTSTRAP}"
if [[ -n "$CC_CRAWL" ]]; then
    echo -e "Input  : Common Crawl ${CC_CRAWL} #${CC_INDEX} (direct stream, no file)"
else
    echo -e "Input  : ${INPUT_FILE}"
fi
echo -e ""

# ── Step 3: (re)create input + output topics ──────────────────────────────────
# Delete first so re-runs start clean (the output is a compacted changelog).
"${BIN}/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$IN_TOPIC"  >/dev/null 2>&1 || true
"${BIN}/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$OUT_TOPIC" >/dev/null 2>&1 || true
sleep 1
"${BIN}/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --replication-factor 1 --partitions 1 \
    --topic "$IN_TOPIC" >/dev/null
"${BIN}/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --replication-factor 1 --partitions 1 \
    --topic "$OUT_TOPIC" --config cleanup.policy=compact >/dev/null
echo -e "${GREEN}[OK]${NC} Topics ${IN_TOPIC} / ${OUT_TOPIC} ready."

# ── Step 4: start the WordCount streams application in the background ──────────
# Reset any previous application state so counts start from zero each run.
"${BIN}/kafka-streams-application-reset.sh" --bootstrap-server "$BOOTSTRAP" \
    --application-id streams-wordcount \
    --input-topics "$IN_TOPIC" >/dev/null 2>&1 || true

STREAMS_LOG="${KAFKA_BASE}/streams-wordcount.log"
echo -e "Starting WordCountDemo (log: ${STREAMS_LOG}) ..."
nohup "${BIN}/kafka-run-class.sh" org.apache.kafka.streams.examples.wordcount.WordCountDemo \
    > "$STREAMS_LOG" 2>&1 &
STREAMS_PID=$!
echo "$STREAMS_PID" > "${KAFKA_BASE}/streams.pid"
sleep 5   # give the app time to join and start consuming

# ── Step 5: feed the input through the producer ───────────────────────────────
if [[ -n "$CC_CRAWL" ]]; then
    echo -e "Streaming Common Crawl ${CC_CRAWL} #${CC_INDEX} directly into ${IN_TOPIC} ..."
    KAFKA_BASE="$KAFKA_BASE" KAFKA_PORT="$KAFKA_PORT" KAFKA_HOME="$KAFKA_HOME" \
        bash "$(dirname "$0")/commoncrawl_source.sh" \
            --crawl "$CC_CRAWL" --index "$CC_INDEX" --topic "$IN_TOPIC"
else
    echo -e "Producing $(wc -l < "$INPUT_FILE") line(s) into ${IN_TOPIC} ..."
    "${BIN}/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$IN_TOPIC" < "$INPUT_FILE"
fi

echo -e "Waiting for the stream to process (${CONSUME_MS} ms) ..."
RAW_OUT="$("${BIN}/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \
    --topic "$OUT_TOPIC" --from-beginning --timeout-ms "$CONSUME_MS" \
    --formatter-property print.key=true \
    --formatter-property print.value=true \
    --formatter-property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --formatter-property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer \
    2>/dev/null)"

# The output topic is a CHANGELOG: each key may appear several times, the LAST
# value is the final count. Keep the last value per key, then sort desc.
echo -e ""
echo -e "================================================="
echo -e " Top ${TOP_N} words (final counts)"
echo -e "================================================="
echo "$RAW_OUT" | awk -F'\t' 'NF==2 {last[$1]=$2} END {for (k in last) print last[k]"\t"k}' \
    | sort -rn | head -n "$TOP_N"

# Stop the streams app (the broker keeps running for further demos).
kill "$STREAMS_PID" >/dev/null 2>&1 || true
echo -e ""
echo -e "${GREEN}Done.${NC} Broker still running. Tear down with scripts/kafka/clean_kafka.sh."
