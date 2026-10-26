#!/usr/bin/env bash
set -Eeuo pipefail

KAFKA_VERSION="3.9.1"
KAFKA_DIR="kafka_2.13-${KAFKA_VERSION}"
KAFKA_ARCHIVE="${KAFKA_DIR}.tgz"
KAFKA_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}"
PROJECT_ROOT="$(pwd -P)"
KAFKA_HOME="${PROJECT_ROOT}/${KAFKA_DIR}"
KAFKA_CONFIG="${KAFKA_HOME}/config/kraft/server.properties"
STATE_DIR="${PROJECT_ROOT}/.kafka"
KAFKA_LOG="${KAFKA_LOG:-${STATE_DIR}/kafka-server.log}"
KAFKA_PID="${KAFKA_PID:-${STATE_DIR}/kafka-server.pid}"
KAFKA_DATA="${KAFKA_DATA:-${STATE_DIR}/data}"

mkdir -p "$STATE_DIR" "$KAFKA_DATA"
if [[ ! -x "${KAFKA_HOME}/bin/kafka-server-start.sh" ]]; then
  if [[ ! -f "$KAFKA_ARCHIVE" ]]; then
    echo "Descargando Apache Kafka ${KAFKA_VERSION} (solo la primera vez)..."
    curl -fL --retry 3 --retry-delay 2 "$KAFKA_URL" -o "$KAFKA_ARCHIVE"
  else
    echo "Usando el archivo Kafka ya descargado: $KAFKA_ARCHIVE"
  fi
  tar -xzf "$KAFKA_ARCHIVE"
fi

if [[ -f "$KAFKA_PID" ]] && kill -0 "$(cat "$KAFKA_PID")" 2>/dev/null; then
  if "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    echo "Kafka ya está activo (PID $(cat "$KAFKA_PID")) en localhost:9092."
    exit 0
  fi
  echo "Hay un PID registrado, pero Kafka no responde. Revisa $KAFKA_LOG." >&2
  tail -n 50 "$KAFKA_LOG" || true
  exit 1
fi
rm -f "$KAFKA_PID"

# Kafka requires an absolute log.dirs path. Use project-local data, not /tmp.
if ! grep -q '^log.dirs=' "$KAFKA_CONFIG"; then
  printf '\nlog.dirs=%s\n' "$KAFKA_DATA" >> "$KAFKA_CONFIG"
else
  sed -i.bak "s#^log.dirs=.*#log.dirs=$KAFKA_DATA#" "$KAFKA_CONFIG"
fi

if [[ ! -f "$KAFKA_DATA/meta.properties" ]]; then
  echo "Inicializando almacenamiento KRaft para un solo broker (sin tolerancia a fallos)..."
  CLUSTER_ID=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$KAFKA_CONFIG"
fi

echo "Iniciando Kafka..."
nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_CONFIG" >"$KAFKA_LOG" 2>&1 &
echo $! > "$KAFKA_PID"

ready=false
for attempt in $(seq 1 90); do
  if ! kill -0 "$(cat "$KAFKA_PID")" 2>/dev/null; then
    echo "Kafka terminó durante el inicio. Últimas líneas del log:" >&2
    tail -n 80 "$KAFKA_LOG" >&2 || true
    rm -f "$KAFKA_PID"
    exit 1
  fi
  if "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 2
done
if [[ "$ready" != true ]]; then
  echo "Kafka no estuvo listo en 180 segundos. Últimas líneas del log:" >&2
  tail -n 80 "$KAFKA_LOG" >&2 || true
  exit 1
fi

"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create --if-not-exists --topic mediciones_aire --partitions 3 --replication-factor 1
PARTITIONS=$("$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --describe --topic mediciones_aire | sed -n 's/.*PartitionCount:\([0-9][0-9]*\).*/\1/p' | head -n 1)
if [[ -n "$PARTITIONS" && "$PARTITIONS" -lt 3 ]]; then
  echo "Ampliando mediciones_aire de $PARTITIONS a 3 particiones para las actividades..."
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --alter --topic mediciones_aire --partitions 3
fi

echo "Kafka está listo en localhost:9092. Topic mediciones_aire: 3 particiones."
echo "Para ver topics, particiones y offsets usa ./diagnose_kafka.sh."
echo "Detén el broker con ./stop_kafka.sh. Log: $KAFKA_LOG"
