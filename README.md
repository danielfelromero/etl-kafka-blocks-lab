# ETL Kafka Blocks Lab

Laboratorio didáctico y no calificable para aprender productores, consumidores, topics, claves, particiones, headers, grupos, offsets y opciones de Kafka con Blockly y Python generado. Los ejemplos ejecutables usan **confluent-kafka** y un broker Kafka real.

## Escenarios

### A. Básico — recomendado para GitHub Codespaces

Un broker Kafka en KRaft, iniciado dentro del contenedor de desarrollo. No necesita Docker-in-Docker y **no ofrece tolerancia a la pérdida del broker**.

~~~bash
./setup_kafka.sh
./diagnose_kafka.sh
~~~

En una terminal aparte, arranca Blockly:

~~~bash
cd frontend
npm install
npm run dev -- --host 0.0.0.0
~~~

Abre el puerto 5173. Selecciona **Modo básico · 1 broker** en la interfaz. El broker sugerido es `localhost:9092`. La interfaz no puede verificar el estado del broker por sí sola; ejecuta el diagnóstico en terminal.

### B. Avanzado — tres brokers, replicación y tolerancia a fallos

Requiere Docker Engine y Docker Compose v2 en un equipo local o VM compatible. No se garantiza Docker dentro de Codespaces.

~~~bash
docker compose -f docker-compose.ha.yml up -d --wait
docker compose -f docker-compose.ha.yml ps
~~~

La topología contiene tres nodos Kafka 3.9.1 con roles broker/controller y un quorum KRaft de tres controladores. Esto supone tres procesos Kafka, no seis; una topología con tres controladores dedicados y tres brokers requeriría seis procesos y más recursos. Los datos se guardan en volúmenes diferenciados. Puertos externos: 19092, 19093 y 19094. En Blockly selecciona **Modo avanzado · 3 brokers**; el cliente usa `localhost:19092,localhost:19093,localhost:19094`.

Crea un topic replicado y observa el estado:

~~~bash
docker compose -f docker-compose.ha.yml exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 --create --if-not-exists \
  --topic mediciones_aire_ha --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2
docker compose -f docker-compose.ha.yml exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 --describe --topic mediciones_aire_ha
~~~

Para observar la tolerancia a fallos, detén temporalmente un broker:

~~~bash
docker compose -f docker-compose.ha.yml stop kafka-2
# Observa líderes/ISR, disponibilidad y resultados de acks=all.
docker compose -f docker-compose.ha.yml start kafka-2
~~~

No declares el experimento exitoso hasta comprobar el estado del topic y los mensajes realmente entregados. Con `min.insync.replicas=2` y `acks=all`, las escrituras dejan de estar disponibles si quedan menos de dos réplicas sincronizadas. El comportamiento depende del estado real del clúster.

## Kafbat UI

El archivo Compose básico con Docker y el avanzado incluyen Kafbat UI en la imagen versionada `ghcr.io/kafbat/kafka-ui:v1.3.0`, publicado en el puerto 8080. Abre `http://localhost:8080` en el equipo donde se ejecuta Compose.

- En Docker, Kafbat se conecta al listener interno `kafka:29092` (básico) o `kafka-1:29092,kafka-2:29092,kafka-3:29092` (avanzado).
- No uses `localhost:9092` desde Kafbat: dentro del contenedor, localhost significa el propio contenedor Kafbat.
- El modo básico de Codespaces iniciado con `setup_kafka.sh` no puede iniciar Kafbat sin un daemon Docker disponible. Usa el CLI de Kafka y `./diagnose_kafka.sh`, o ejecuta el Compose básico en un host con Docker.

## Blockly y Python

1. Selecciona el escenario; la selección cambia la dirección sugerida, no inicia el clúster.
2. Carga el ejemplo de productor o consumidor.
3. Modifica los bloques y estudia el Python generado.
4. Copia/descarga el script y ejecútalo desde la raíz con `.venv/bin/python archivo.py`.
5. Comprueba topic, partición, offset, clave y headers en la salida del consumidor o en Kafbat.

La generación visual no ejecuta Python en el navegador. Kafka se configura fuera del código cliente; el número de brokers no es una opción que un productor pueda crear.

## Schema Registry (extensión conceptual)

Schema Registry permite versionar contratos de eventos y validar compatibilidad entre productores y consumidores con serializadores compatibles. Kafka no valida automáticamente los mensajes contra un esquema. Avro, JSON Schema y Protobuf son formatos distintos. Compatibilidad hacia atrás permite que consumidores nuevos lean datos antiguos; hacia adelante, que consumidores antiguos lean datos nuevos; completa combina ambas direcciones, según la política configurada. JSON sintácticamente válido no garantiza que el evento cumpla el contrato. Esta versión no instala ni simula un Registry.

## Cliente Python

Se utiliza `confluent-kafka` como cliente principal. `kafka-python` tiene una API distinta, pero los conceptos Kafka de topic, partición, clave, offset, grupo y confirmación no dependen de la biblioteca. No se mantienen dos implementaciones ejecutables paralelas.

## Verificaciones

~~~bash
cd frontend && npm ci && npm run build
cd ..
python -m compileall -q runtime
bash -n setup_kafka.sh stop_kafka.sh diagnose_kafka.sh
~~~

Valida los archivos Compose con `docker compose -f docker-compose.ha.yml config` y prueba brokers/Kafbat solo en un host con Docker. La compilación y las comprobaciones de sintaxis no sustituyen una prueba de integración con Kafka real.

## Parar y limpiar

Básico: `./stop_kafka.sh`. Los datos persisten en `.kafka/data`.

Avanzado: `docker compose -f docker-compose.ha.yml down` conserva los volúmenes. Para borrar también los datos del laboratorio, usa `docker compose -f docker-compose.ha.yml down -v`. Elimina volúmenes solo si deseas perder el estado persistido.
