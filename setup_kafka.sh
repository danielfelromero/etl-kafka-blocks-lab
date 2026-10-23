#!/usr/bin/env bash
set -euo pipefail

KAFKA_VERSION="3.9.1"
KAFKA_SCALA_VERSION="2.13"
KAFKA_DIR="kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}"
KAFKA_ARCHIVE="${KAFKA_DIR}.tgz"
KAFKA_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}"

if [[ ! -x "${KAFKA_DIR}/bin/kafka-server-start.sh" ]]; then
  echo "Downloading Apache Kafka ${KAFKA_VERSION}..."
  curl -fL --retry 3 "${KAFKA_URL}" -o "${KAFKA_ARCHIVE}"
  tar -xzf "${KAFKA_ARCHIVE}"
fi

mkdir -p /tmp/kraft-combined-logs

if [[ ! -f /tmp/kraft-combined-logs/meta.properties ]]; then
  echo "Formatting Kafka storage for KRaft..."
  CLUSTER_ID=$("${KAFKA_DIR}/bin/kafka-storage.sh" random-uuid)
  "${KAFKA_DIR}/bin/kafka-storage.sh" format -t "${CLUSTER_ID}" -c "${KAFKA_DIR}/config/kraft/server.properties"
fi

if pgrep -f 'kafka.Kafka' >/dev/null 2>&1; then
  echo "Kafka appears to be running already."
else
  echo "Starting Kafka in the background..."
  nohup "${KAFKA_DIR}/bin/kafka-server-start.sh" "${KAFKA_DIR}/config/kraft/server.properties" > /tmp/kafka-server.log 2>&1 &
fi

echo "Waiting for Kafka on localhost:9092..."
READY=0
for attempt in $(seq 1 60); do
  if "${KAFKA_DIR}/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    READY=1
    break
  fi
  sleep 2
done

if [[ "${READY}" != "1" ]]; then
  echo "Kafka did not become ready. Recent broker log:"
  tail -n 80 /tmp/kafka-server.log || true
  exit 1
fi

echo "Kafka is ready on localhost:9092."
