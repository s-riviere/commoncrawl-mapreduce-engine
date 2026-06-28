#!/bin/bash

# ==============================================================================
# DESCRIPTION
# ==============================================================================
# Common Crawl -> Kafka SOURCE connector (KISS, no Docker / no root / no Connect
# cluster). It connects an EXTERNAL source (Common Crawl on AWS S3/HTTPS) to a
# Kafka topic with ZERO intermediate files: the .wet.gz is streamed, gunzipped
# and its WARC/HTTP headers stripped on the fly, then each text line is produced
# as one message into the input topic the Kafka Streams WordCount app consumes.
#
#   data.commoncrawl.org (S3/HTTPS)
#        │  curl  (streamed, no file)
#        ▼  gunzip │ strip headers
#   kafka-console-producer.sh ──▶ topic  streams-plaintext-input
#
# This is the §8 "direct Common Crawl read via a Kafka source" item: the NFS is
# never touched and nothing is staged on local disk.
#
# Arguments / environment:
#   -c, --crawl   ID   Common Crawl id      (default CC-MAIN-2024-10)
#   -i, --index   N    WET file index in wet.paths.gz   (default 0)
#   -u, --url     URL  full WET .wet.gz URL (overrides --crawl/--index)
#   -t, --topic   T    target topic         (default streams-plaintext-input)
#   -m, --max-lines N  cap produced lines (0 = all; default 200000 for demos)
#   KAFKA_BASE (default /tmp/<user>-kafka)   KAFKA_PORT (default 9092)
#
# Usage:
#   bash src/kafka/commoncrawl_source.sh                 # split 0 of default crawl
#   bash src/kafka/commoncrawl_source.sh -c CC-MAIN-2024-10 -i 3
#   bash src/kafka/commoncrawl_source.sh -u https://data.commoncrawl.org/crawl-data/.../xxx.wet.gz
# ==============================================================================

set -uo pipefail
NC="\e[0m"; GREEN="\e[32m"; YELLOW="\e[33m"; RED="\e[31m"

BASE_URL="https://data.commoncrawl.org/"
CRAWL_ID="${CRAWL_ID:-CC-MAIN-2024-10}"
INDEX="${INDEX:-0}"
WET_URL="${WET_URL:-}"
TOPIC="${TOPIC:-streams-plaintext-input}"
MAX_LINES="${MAX_LINES:-200000}"

KAFKA_VERSION="${KAFKA_VERSION:-4.3.0}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
KAFKA_BASE="${KAFKA_BASE:-/tmp/${USER}-kafka}"
KAFKA_HOME="${KAFKA_HOME:-${KAFKA_BASE}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
BOOTSTRAP="localhost:${KAFKA_PORT}"

# ── Parse flags ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        -c|--crawl)     CRAWL_ID="$2"; shift 2 ;;
        -i|--index)     INDEX="$2";    shift 2 ;;
        -u|--url)       WET_URL="$2";  shift 2 ;;
        -t|--topic)     TOPIC="$2";    shift 2 ;;
        -m|--max-lines) MAX_LINES="$2"; shift 2 ;;
        -h|--help)      sed -n '2,40p' "$0"; exit 0 ;;
        *) echo -e "${RED}Unknown argument: $1${NC}" >&2; exit 2 ;;
    esac
done

PRODUCER="${KAFKA_HOME}/bin/kafka-console-producer.sh"
if [[ ! -x "$PRODUCER" ]]; then
    echo -e "${RED}FATAL: Kafka not found at ${KAFKA_HOME}. Run deploy_kafka.sh first.${NC}" >&2
    exit 1
fi
if ! "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
    echo -e "${RED}FATAL: no broker on ${BOOTSTRAP}. Run deploy_kafka.sh first.${NC}" >&2
    exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
    echo -e "${RED}FATAL: curl is required to stream from Common Crawl.${NC}" >&2
    exit 1
fi

# ── Resolve the WET URL from wet.paths.gz if not given explicitly ─────────────
if [[ -z "$WET_URL" ]]; then
    PATHS_URL="${BASE_URL}crawl-data/${CRAWL_ID}/wet.paths.gz"
    echo -e "Resolving split #${INDEX} from ${PATHS_URL} ..."
    # Stream the path list, gunzip, take the (INDEX+1)-th line — no temp file.
    WET_PATH="$(curl -fsSL "$PATHS_URL" | gunzip | sed -n "$((INDEX + 1))p")"
    if [[ -z "$WET_PATH" ]]; then
        echo -e "${RED}FATAL: could not resolve WET path for index ${INDEX} in ${CRAWL_ID}.${NC}" >&2
        exit 1
    fi
    WET_URL="${BASE_URL}${WET_PATH}"
fi

echo -e "================================================="
echo -e " Common Crawl -> Kafka source"
echo -e "================================================="
echo -e "Source : ${WET_URL}"
echo -e "Topic  : ${TOPIC}   (broker ${BOOTSTRAP})"
echo -e "Cap    : ${MAX_LINES} line(s) (0 = all)"
echo -e ""

# ── Stream: curl | gunzip | strip WARC/HTTP headers | [cap] | producer ────────
# Everything is piped; no NFS file and no on-disk staging is created.
# WET record headers (WARC/…, WARC-…, Content-…, Metadata-…) are dropped so only
# the plain-text body is produced into Kafka.
set -o pipefail
if [[ "$MAX_LINES" -gt 0 ]]; then
    curl -fsSL "$WET_URL" \
        | gunzip \
        | grep -aviE '^(WARC/|WARC-|Content-|Metadata-)' \
        | head -n "$MAX_LINES" \
        | "$PRODUCER" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"
else
    curl -fsSL "$WET_URL" \
        | gunzip \
        | grep -aviE '^(WARC/|WARC-|Content-|Metadata-)' \
        | "$PRODUCER" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"
fi
status=$?

if [[ $status -ne 0 ]]; then
    echo -e "${RED}FATAL: streaming Common Crawl into Kafka failed (exit ${status}).${NC}" >&2
    exit "$status"
fi
echo -e "${GREEN}[OK]${NC} Common Crawl streamed directly into topic ${TOPIC}."
