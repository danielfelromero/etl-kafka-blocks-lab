#!/usr/bin/env bash
set -Eeuo pipefail

KAFKA_VERSION="3.9.1"
KAFKA_DIR="kafka_2.13-${KAFKA_VERSION}"
KAFKA_ARCHIVE="${KAFKA_DIR}.tgz"
KAFKA_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}"
KAFKA_HOME="$PWD/${KAFKA_DIR}"
KAFKA_CONFIG="${KAFKA_HOME}/config/kraft/server.properties"
STATE_DIR="$PWD/.kafka"
KAFKA_LOG="${KAFKA_LOG:-${STATE_DIR}/kafka-server.log}"
KAFKA_PID="${KAFKA_PID:-${STATE_DIR}/kafka-server.pid}"
KAFKA_DATA="${KAFKA_DATA:-${STATE_DIR}/data}"

mkdir -p "$STATE_DIR" "$KAFKA_DATA"
if [[ ! -x "${KAFKA_HOME}/bin/kafka-server-start.sh" ]]; then
  echo "Downloading Apache Kafka ${KAFKA_VERSION}..."
  curl -fL --retry 3 --retry-delay 2 "$KAFKA_URL" -o "$KAFKA_ARCHIVE"
  tar -xzf "$KAFKA_ARCHIVE"
fi

if [[ -f "$KAFKA_PID" ]] && kill -0 "$(cat "$KAFKA_PID")" 2>/dev/null; then
  if "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    echo "Kafka is already running (PID $(cat "$KAFKA_PID")) on localhost:9092."
    exit 0
  fi
  echo "PID file exists but Kafka is not responding; inspect $KAFKA_LOG." >&2
  tail -n 50 "$KAFKA_LOG" || true
  exit 1
fi
rm -f "$KAFKA_PID"

# Point the broker to persistent project-local storage before formatting KRaft.
if ! grep -q '^log.dirs=' "$KAFKA_CONFIG"; then
  printf '\nlog.dirs=%s\n' "$KAFKA_DATA" >> "$KAFKA_CONFIG"
else
  sed -i.bak "s#^log.dirs=.*#log.dirs=$KAFKA_DATA#" "$KAFKA_CONFIG"
fi

if [[ ! -f "$KAFKA_DATA/meta.properties" ]]; then
  echo "Formatting KRaft storage for a single broker (not fault tolerant)..."
  CLUSTER_ID=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$KAFKA_CONFIG"
fi

echo "Starting Kafka broker..."
nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_CONFIG" >"$KAFKA_LOG" 2>&1 &
echo $! > "$KAFKA_PID"

for attempt in $(seq 1 60); do
  if ! kill -0 "$(cat "$KAFKA_PID")" 2>/dev/null; then
    echo "Kafka process exited. Recent log:" >&2
    tail -n 80 "$KAFKA_LOG" >&2 || true
    rm -f "$KAFKA_PID"
    exit 1
  fi
  if "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    echo "Kafka is ready on localhost:9092 (single broker)."
    echo "Run ./diagnose_kafka.sh to verify the broker and list topics."
    echo "Stop it with ./stop_kafka.sh. Log: $KAFKA_LOG"
    exit 0
  fi
  sleep 2
done

echo "Kafka did not become ready. Recent log:" >&2
tail -n 80 "$KAFKA_LOG" >&2 || true
exit 1
