#!/usr/bin/env bash
set -euo pipefail
KAFKA_PID="${KAFKA_PID:-$PWD/.kafka/kafka-server.pid}"
if [[ ! -f "$KAFKA_PID" ]]; then
  echo "No Kafka PID file found at $KAFKA_PID; broker may already be stopped."
  exit 0
fi
pid="$(cat "$KAFKA_PID")"
if kill -0 "$pid" 2>/dev/null; then
  kill "$pid"
  for _ in $(seq 1 30); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "Kafka did not stop gracefully. Inspect .kafka/kafka-server.log before forcing termination." >&2
    exit 1
  fi
fi
rm -f "$KAFKA_PID"
echo "Kafka stopped. Data remains in .kafka/data."
