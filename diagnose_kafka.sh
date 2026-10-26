#!/usr/bin/env bash
set -euo pipefail
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
KAFKA_VERSION="3.9.1"
KAFKA_HOME="$PWD/kafka_2.13-$KAFKA_VERSION"
if [[ ! -x "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" ]]; then
  echo "No se encuentra Kafka CLI. Inicia el broker básico con ./setup_kafka.sh" >&2
  exit 2
fi

echo "Comprobando broker en $BOOTSTRAP_SERVERS..."
"$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP_SERVERS"
echo
echo "Topics:"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVERS" --list
echo
echo "Descripción de mediciones_aire (PartitionCount y líderes):"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVERS" --describe --topic mediciones_aire
echo
echo "Offsets por partición (earliest/latest):"
"$KAFKA_HOME/bin/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP_SERVERS" --topic mediciones_aire
