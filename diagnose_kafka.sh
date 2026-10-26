#!/usr/bin/env bash
set -euo pipefail
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
KAFKA_VERSION="3.9.1"
KAFKA_HOME="$PWD/kafka_2.13-$KAFKA_VERSION"
if [[ ! -x "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" ]]; then
  echo "Kafka CLI not found. Start the basic broker first with ./setup_kafka.sh" >&2
  exit 2
fi
echo "Checking broker at $BOOTSTRAP_SERVERS ..."
"$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP_SERVERS"
echo
echo "Topics:"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVERS" --list
