# Práctica 2 — Avanzada: replicación, Kafbat y tolerancia a fallos

## Objetivo

Explorar replicación de particiones, líderes, ISR (in-sync replicas), confirmaciones de productor y disponibilidad cuando falla un nodo.

## Elegir el entorno

Hay dos opciones para ejecutar el clúster avanzado:

- **Opción A — Docker Compose (incluye Kafbat UI):** recomendada en un equipo local o VM donde Docker Engine y Compose v2 ya funcionan.
- **Opción B — tres procesos Kafka sin Docker:** útil en Codespaces si el broker básico ya funciona, pero consume más memoria y Kafbat no se incluye en esta opción.

Docker-in-Docker no es requisito del curso. Añadir la feature al `.devcontainer/devcontainer.json` no garantiza que Codespaces pueda construir y arrancar el contenedor con las capacidades necesarias; por eso se conserva una ruta sin Docker.

## Opción A — Docker Compose y Kafbat

Desde la raíz:

~~~bash
docker compose -f docker-compose.ha.yml up -d --wait
docker compose -f docker-compose.ha.yml ps
~~~

Abre `http://localhost:8080` y selecciona el clúster `etl-kafka-ha`. El topic `mediciones_aire_ha` se puede crear con:

~~~bash
docker compose -f docker-compose.ha.yml exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:29092 --create --if-not-exists --topic mediciones_aire_ha --partitions 3 --replication-factor 3 --config min.insync.replicas=2
docker compose -f docker-compose.ha.yml exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:29092 --describe --topic mediciones_aire_ha
~~~

En Kafbat, inspecciona particiones, líderes, réplicas e ISR. En Blockly selecciona **Modo avanzado · 3 brokers**. La dirección sugerida para clientes que se ejecutan en el host es `localhost:19092,localhost:19093,localhost:19094`.

Para simular una interrupción:

~~~bash
docker compose -f docker-compose.ha.yml stop kafka-2
# Observa el estado del topic y prueba la producción con acks=all.
docker compose -f docker-compose.ha.yml start kafka-2
~~~

Con `min.insync.replicas=2` y `acks=all`, la escritura requiere al menos dos réplicas sincronizadas. Si quedan menos, las escrituras pueden fallar temporalmente. Comprueba el resultado real en vez de asumir que toda escritura siempre tiene éxito.

## Opción B — tres procesos Kafka locales, sin Docker

Desde la raíz:

~~~bash
chmod +x setup_kafka_ha.sh stop_kafka_ha.sh
./setup_kafka_ha.sh
~~~

El script inicia tres nodos Kafka KRaft con IDs, almacenamiento y logs separados. El bootstrap es `localhost:19092,localhost:19093,localhost:19094`; el topic `mediciones_aire_ha` queda preparado con tres particiones y factor de replicación 3. En Blockly selecciona el modo avanzado y utiliza ese bootstrap.

Inspecciona el topic:

~~~bash
kafka_2.13-3.9.1/bin/kafka-topics.sh --bootstrap-server localhost:19092,localhost:19093,localhost:19094 --describe --topic mediciones_aire_ha
~~~

Detén solo uno de los procesos para observar el efecto:

~~~bash
kill "$(cat .kafka-ha/pids/kafka-2.pid)"
~~~

Observa los logs en `.kafka-ha/logs/`, la descripción del topic y los resultados del productor. Luego detén el clúster de forma ordenada con `./stop_kafka_ha.sh`. Si el proceso se detuvo manualmente, el script de parada limpiará los PID restantes.

Esta opción demuestra la caída de un proceso en la misma máquina, no la pérdida de un host físico o de una zona de disponibilidad. Los tres procesos comparten CPU, memoria, disco y sistema operativo.

## Comparación y discusión

1. ¿Qué diferencia hay entre número de particiones y factor de replicación?
2. ¿Qué indican líder, réplicas y ISR?
3. ¿Qué cambia entre `acks=1` y `acks=all`?
4. ¿Por qué `min.insync.replicas=2` puede hacer que una escritura falle si un nodo está caído?
5. ¿Qué riesgo no se puede estudiar al ejecutar los tres nodos en la misma máquina?
6. ¿Qué información facilita Kafbat frente al CLI?

## Parar y limpiar

Docker Compose: `docker compose -f docker-compose.ha.yml down`. Añade `-v` solo si quieres eliminar también los volúmenes.

Tres procesos locales: `./stop_kafka_ha.sh`. Los datos permanecen en `.kafka-ha/data`; no borres esos datos a menos que quieras reinicializar el clúster.
