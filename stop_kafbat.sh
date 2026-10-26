#!/usr/bin/env bash
set -euo pipefail
PIDFILE="$PWD/.kafbat/kafbat-ui.pid"
LOG="$PWD/.kafbat/kafbat-ui.log"

if [[ ! -f "$PIDFILE" ]]; then
  echo "No hay un PID registrado para Kafbat UI; puede que ya esté detenido."
  exit 0
fi

pid="$(cat "$PIDFILE")"
if kill -0 "$pid" 2>/dev/null; then
  echo "Deteniendo Kafbat UI (PID $pid)..."
  kill "$pid"
  for _ in $(seq 1 30); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "Kafbat UI no se detuvo dentro del tiempo esperado. Revisa $LOG." >&2
    exit 1
  fi
fi
rm -f "$PIDFILE"
echo "Kafbat UI detenido. El código descargado se conserva en .kafbat/source."
