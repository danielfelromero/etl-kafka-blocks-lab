
#!/usr/bin/env bash
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
PID_DIR="$ROOT/.kafka-ha/pids"
CONFIG_DIR="$ROOT/.kafka-ha/config"
TIMEOUT=60

if [[ ! -d "$PID_DIR" ]]; then
  echo "No se encuentra el directorio de PID: $PID_DIR"
  exit 1
fi

declare -A PIDS
declare -A STOPPED

# 1. Validar los PID y solicitar la parada de todos los brokers.
for id in 1 2 3; do
  pidfile="$PID_DIR/kafka-$id.pid"

  [[ -f "$pidfile" ]] || continue

  pid="$(cat "$pidfile")"

  if [[ ! "$pid" =~ ^[0-9]+$ ]] || ! kill -0 "$pid" 2>/dev/null; then
    echo "Nodo $id: proceso inexistente; eliminando PID obsoleto."
    rm -f "$pidfile"
    continue
  fi

  # Evitar señalizar un proceso que no sea el broker esperado.
  cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"
  expected_config="$CONFIG_DIR/server-$id.properties"

  if [[ "$cmd" != *"kafka.Kafka"* ||
        "$cmd" != *"$expected_config"* ]]; then
    echo "ERROR: el PID $pid no corresponde al nodo Kafka $id."
    echo "No se enviará ninguna señal a ese proceso."
    exit 1
  fi

  PIDS["$id"]="$pid"
  STOPPED["$id"]=0
done

if [[ ${#PIDS[@]} -eq 0 ]]; then
  echo "No hay brokers registrados en ejecución."
  exit 0
fi

# 2. Enviar SIGTERM a todos antes de esperar por cualquiera.
for id in 1 2 3; do
  [[ -n "${PIDS[$id]:-}" ]] || continue
  pid="${PIDS[$id]}"

  echo "Solicitando parada del nodo $id (PID $pid)..."
  if ! kill -TERM "$pid" 2>/dev/null; then
    if ! kill -0 "$pid" 2>/dev/null; then
      STOPPED["$id"]=1
    else
      echo "ADVERTENCIA: no se pudo enviar SIGTERM al nodo $id."
    fi
  fi
done

# 3. Esperar hasta que terminen, con un límite global.
for ((elapsed=0; elapsed<TIMEOUT; elapsed++)); do
  all_stopped=1

  for id in 1 2 3; do
    [[ -n "${PIDS[$id]:-}" ]] || continue
    [[ "${STOPPED[$id]}" -eq 1 ]] && continue

    pid="${PIDS[$id]}"

    if ! kill -0 "$pid" 2>/dev/null; then
      STOPPED["$id"]=1
      rm -f "$PID_DIR/kafka-$id.pid"
      echo "Nodo $id detenido."
    else
      all_stopped=0
    fi
  done

  [[ "$all_stopped" -eq 1 ]] && break
  sleep 1
done

# 4. Informar el resultado sin borrar PID de procesos aún activos.
failed=0

for id in 1 2 3; do
  [[ -n "${PIDS[$id]:-}" ]] || continue

  if [[ "${STOPPED[$id]}" -eq 1 ]] ||
     ! kill -0 "${PIDS[$id]}" 2>/dev/null; then
    rm -f "$PID_DIR/kafka-$id.pid"
    echo "OK: nodo $id detenido."
  else
    echo "ERROR: nodo $id (PID ${PIDS[$id]}) sigue activo tras ${TIMEOUT}s."
    echo "Revisa .kafka-ha/logs/kafka-$id.log"
    failed=1
  fi
done

if [[ "$failed" -eq 1 ]]; then
  echo "La parada no se completó. No se eliminaron los datos de Kafka."
  exit 1
fi

echo "Todos los brokers registrados se han detenido."
echo "Los datos de .kafka-ha/data se conservaron."
