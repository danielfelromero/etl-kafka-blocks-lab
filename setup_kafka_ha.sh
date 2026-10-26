#!/usr/bin/env bash
set -Eeuo pipefail

VERSION="3.9.1"
DIST="kafka_2.13-$VERSION"
ARCHIVE="$DIST.tgz"
URL="https://archive.apache.org/dist/kafka/$VERSION/$ARCHIVE"
ROOT="$(pwd -P)"
HOME_KAFKA="$ROOT/$DIST"
STATE="$ROOT/.kafka-ha"
CONFIG_DIR="$STATE/config"
LOG_DIR="$STATE/logs"
DATA_DIR="$STATE/data"
PID_DIR="$STATE/pids"
BOOTSTRAP="localhost:19092,localhost:19093,localhost:19094"

mkdir -p "$STATE" "$CONFIG_DIR" "$LOG_DIR" "$DATA_DIR" "$PID_DIR"
if [[ ! -x "$HOME_KAFKA/bin/kafka-server-start.sh" ]]; then
  if [[ ! -f "$ARCHIVE" ]]; then
    echo "Descargando Kafka $VERSION (se reutilizará para el modo básico y avanzado)..."
    curl -fL --retry 3 --retry-delay 2 "$URL" -o "$ARCHIVE"
  fi
  tar -xzf "$ARCHIVE"
fi

# Do not overwrite a running cluster's config or reformat its storage.
for id in 1 2 3; do
  pidfile="$PID_DIR/kafka-$id.pid"
  if [[ -f "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    echo "El nodo $id ya tiene un proceso activo (PID $(cat "$pidfile"))." >&2
    echo "Si es un inicio parcial, ejecuta ./stop_kafka_ha.sh y vuelve a intentar." >&2
    exit 1
  fi
done

CLUSTER_ID_FILE="$STATE/cluster.id"
if [[ ! -s "$CLUSTER_ID_FILE" ]]; then
  "$HOME_KAFKA/bin/kafka-storage.sh" random-uuid > "$CLUSTER_ID_FILE"
fi
CLUSTER_ID="$(cat "$CLUSTER_ID_FILE")"

for id in 1 2 3; do
  config="$CONFIG_DIR/server-$id.properties"
  data="$DATA_DIR/node-$id"
  mkdir -p "$data"
  case "$id" in
    1) controller_port=19103; internal_port=19292; external_port=19092 ;;
    2) controller_port=19104; internal_port=19293; external_port=19093 ;;
    3) controller_port=19105; internal_port=19294; external_port=19094 ;;
  esac
  cat > "$config" <<EOF
process.roles=broker,controller
node.id=$id
controller.quorum.voters=1@localhost:19103,2@localhost:19104,3@localhost:19105
listeners=CONTROLLER://localhost:$controller_port,INTERNAL://localhost:$internal_port,EXTERNAL://0.0.0.0:$external_port
advertised.listeners=INTERNAL://localhost:$internal_port,EXTERNAL://localhost:$external_port
listener.security.protocol.map=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=INTERNAL
log.dirs=$data
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
group.initial.rebalance.delay.ms=0
EOF
  # Storage format is idempotent; never force a format over existing node data.
  if [[ ! -f "$data/meta.properties" ]]; then
    "$HOME_KAFKA/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$config"
  fi
done

echo "Iniciando tres nodos Kafka locales. Esto consume más memoria que el modo básico..."
for id in 1 2 3; do
  nohup "$HOME_KAFKA/bin/kafka-server-start.sh" "$CONFIG_DIR/server-$id.properties" > "$LOG_DIR/kafka-$id.log" 2>&1 &
  echo $! > "$PID_DIR/kafka-$id.pid"
done

ready=false
for attempt in $(seq 1 90); do
  if "$HOME_KAFKA/bin/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 2
done
if [[ "$ready" != true ]]; then
  echo "El clúster no estuvo listo en 180 segundos. Logs recientes:" >&2
  for id in 1 2 3; do echo "--- nodo $id ---" >&2; tail -n 35 "$LOG_DIR/kafka-$id.log" >&2 || true; done
  exit 1
fi

"$HOME_KAFKA/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic mediciones_aire_ha --partitions 3 --replication-factor 3 --config min.insync.replicas=2
echo "Clúster avanzado listo. Bootstrap: $BOOTSTRAP"
echo "Topic mediciones_aire_ha: 3 particiones, replication-factor=3, min.insync.replicas=2."
echo "Inspecciona: $HOME_KAFKA/bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic mediciones_aire_ha"
echo "Detén con ./stop_kafka_ha.sh. Logs en $LOG_DIR."
