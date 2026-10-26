#!/usr/bin/env bash
set -Eeuo pipefail

KAFKA_VERSION="3.9.1"
KAFKA_SCALA_VERSION="2.13"
KAFKA_DIR="${KAFKA_SCALA_VERSION:+kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}}"
KAFKA_ARCHIVE="${KAFKA_DIR}.tgz"
KAFKA_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}"
KAFKA_HOME="$PWD/${KAFKA_DIR}"
KAFKA_CONFIG="${KAFKA_HOME}/config/kraft/server.properties"
KAFKA_LOG="${KAFKA_LOG:-$PWD/.kafka/kafka-server.log}"
KAFKA_PID="${KAFKA_PID:-$PWD/.kafka/kafka-server.pid}"
KAFKA_DATA="${KAFKA_DATA:-$PWD/.kafka/data}"

mkdir -p "$(dirname "$KAFKA_LOG")" "$KAFKA_DATA"
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
  echo "Found a PID file but the broker is not responding; inspecting logs."
  tail -n 40 "$KAFKA_LOG" || true
  exit 1
fi
rm -f "$KAFKA_PID"

# Keep the default Kafka config but use a project-local data directory.
if [[ ! -f "$KAFKA_DATA/meta.properties" ]]; then
  echo "Formatting KRaft storage (one broker; no fault tolerance)."
  CLUSTER_ID=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$KAFKA_CONFIG" --add-scram '' >/dev/null 2>&1 || {
    # Kafka 3.9 supports the standard format command; the config's log dirs are rewritten below.
    sed -i.bak "s#^log.dirs=.*#log.dirs=$KAFKA_DATA#" "$KAFKA_CONFIG"
    "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$KAFKA_CONFIG"
  }
fi

# Ensure the broker persists data under this repository's ignored .kafka directory.
if ! grep -q "^log.dirs=$KAFKA_DATA$" "$KAFKA_CONFIG"; then
  sed -i.bak "s#^log.dirs=.*#log.dirs=$KAFKA_DATA#" "$KAFKA_CONFIG"
fi
if [[ ! -f "$KAFKA_DATA/meta.properties" ]]; then
  echo "KRaft storage was not initialized in $KAFKA_DATA; check $KAFKA_LOG." >&2
  exit 1
fi

echo "Starting Kafka broker in the background..."
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
    echo "Log: $KAFKA_LOG"
    echo "Stop it with: ./stop_kafka.sh"
    exit 0
  fi
  sleep 2
done

echo "Kafka did not become ready. Recent log:" >&2
tail -n 80 "$KAFKA_LOG" >&2 || true
exit 1
