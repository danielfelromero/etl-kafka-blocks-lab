#!/usr/bin/env bash
set -euo pipefail
PID_DIR="$PWD/.kafka-ha/pids"
if [[ ! -d "$PID_DIR" ]]; then
  echo "No se encuentra el directorio de PID del clúster avanzado."
  exit 0
fi
for id in 1 2 3; do
  pidfile="$PID_DIR/kafka-$id.pid"
  [[ -f "$pidfile" ]] || continue
  pid="$(cat "$pidfile")"
  if kill -0 "$pid" 2>/dev/null; then
    echo "Deteniendo nodo $id (PID $pid)..."
    kill "$pid"
    for _ in $(seq 1 30); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
      echo "El nodo $id no terminó de forma ordenada; revisa .kafka-ha/logs/kafka-$id.log." >&2
      exit 1
    fi
  fi
  rm -f "$pidfile"
done
echo "Nodos detenidos. Los datos de .kafka-ha/data se conservaron."
