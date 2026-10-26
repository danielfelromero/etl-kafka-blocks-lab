#!/usr/bin/env bash
set -Eeuo pipefail

VERSION="1.3.0"
REPO_URL="https://github.com/kafbat/kafka-ui.git"
ROOT="$(pwd -P)"
STATE="$ROOT/.kafbat"
SOURCE="$STATE/source"
LOG="$STATE/kafbat-ui.log"
PIDFILE="$STATE/kafbat-ui.pid"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092,localhost:19093,localhost:19094}"
HEALTH_URL="http://localhost:8080/actuator/health"

if ! command -v java >/dev/null 2>&1; then
  echo "Java no está instalado. Kafbat UI necesita Java para ejecutarse sin contenedor." >&2
  exit 2
fi
if ! command -v git >/dev/null 2>&1; then
  echo "Se necesita git para descargar el código de Kafbat UI." >&2
  exit 2
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "Se necesita curl para comprobar que Kafbat UI está listo." >&2
  exit 2
fi

JAVA_MAJOR="$(java -version 2>&1 | sed -n '1s/.*version "\([0-9]*\).*/\1/p')"
if [[ -z "$JAVA_MAJOR" || "$JAVA_MAJOR" -lt 17 ]]; then
  echo "Se requiere Java 17 o superior. Versión detectada: ${JAVA_MAJOR:-desconocida}" >&2
  exit 2
fi

mkdir -p "$STATE"
if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
    echo "Kafbat UI ya está activo en http://localhost:8080."
    exit 0
  fi
  echo "Hay un proceso Kafbat registrado, pero la interfaz no responde. Revisa $LOG." >&2
  exit 1
fi
rm -f "$PIDFILE"

if [[ ! -x "$SOURCE/gradlew" ]]; then
  echo "Descargando Kafbat UI $VERSION. El primer inicio puede tardar y usar memoria adicional..."
  rm -rf "$SOURCE"
  git clone --depth 1 --branch "$VERSION" "$REPO_URL" "$SOURCE"
fi

echo "Iniciando Kafbat UI para Kafka en $BOOTSTRAP..."
(
  cd "$SOURCE"
  export DYNAMIC_CONFIG_ENABLED=true
  export SWAGGER_UI_ENABLED=true
  export KAFKA_CLUSTERS_0_NAME="etl-kafka-ha"
  export KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS="$BOOTSTRAP"
  export KAFKA_CLUSTERS_0_PROPERTIES_SECURITY_PROTOCOL="PLAINTEXT"
  exec ./gradlew bootRun --no-daemon -x test -Pinclude-frontend=true
) >"$LOG" 2>&1 &
echo $! > "$PIDFILE"

ready=false
for attempt in $(seq 1 300); do
  pid="$(cat "$PIDFILE")"
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "Kafbat UI terminó durante el inicio. Últimas líneas del log:" >&2
    tail -n 80 "$LOG" >&2 || true
    rm -f "$PIDFILE"
    exit 1
  fi
  if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 2
done

if [[ "$ready" != true ]]; then
  echo "Kafbat UI no estuvo listo en 600 segundos. Revisa $LOG." >&2
  tail -n 60 "$LOG" >&2 || true
  exit 1
fi

echo "Kafbat UI está listo: http://localhost:8080"
echo "Broker(s): $BOOTSTRAP"
echo "Log: $LOG"
echo "Para detener la interfaz, ejecuta ./stop_kafbat.sh"
