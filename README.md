# ETL Kafka Blocks Lab

Laboratorio didáctico no calificable para explorar productores, consumidores, topics, claves, particiones, headers, grupos, offsets y replicación con Blockly y Python generado. Los ejemplos usan **confluent-kafka** y un broker Kafka real.

## Prácticas

1. **[Práctica 1 — Básica](docs/practica-basica.md):** un broker KRaft, productor/consumidor, particiones, offsets y claves.
2. **[Práctica 2 — Avanzada](docs/practica-avanzada.md):** tres nodos Kafka, replicación, Kafbat UI ejecutado sin Docker, multitópico y prueba de tolerancia a fallos.

Guía general: [docs/guia-estudiantes.md](docs/guia-estudiantes.md).

## Inicio rápido — práctica básica

Desde la raíz del repositorio:

~~~bash
chmod +x setup_kafka.sh
./setup_kafka.sh
./diagnose_kafka.sh
~~~

El primer inicio descarga Kafka y puede tardar; las siguientes ejecuciones reutilizan el archivo descargado. El script configura almacenamiento en .kafka/data y prepara mediciones_aire con tres particiones para que los ejemplos puedan enviar a las particiones 0, 1 o 2. No es necesario borrar /tmp/kraft-combined-logs como parte de la operación normal. Si ya existían datos de pruebas antiguas y el broker falla, consulta primero el log en .kafka/kafka-server.log.

En otra terminal inicia Blockly:

~~~bash
cd frontend
npm install
npm run dev -- --host 0.0.0.0
~~~

Abre el puerto 5173 y selecciona el escenario correspondiente. Para ejecutar un script desde la raíz del repositorio:

~~~bash
source .venv/bin/activate
python producer_generated.py
# En otra terminal, con el entorno activado:
python consumer_generated.py
~~~

También puedes usar .venv/bin/python producer_generated.py desde la raíz sin activar el entorno. El script generado imprime los metadatos de entrega, incluida la partición y el offset asignados. Para conocer el número de particiones: ./diagnose_kafka.sh.

## Notas importantes

- Una clave no selecciona directamente un número de partición; con asignación automática, Kafka la usa para calcular una partición. Para elegir un número exacto, selecciona asignación manual y confirma que esa partición exista.
- Dos consumidores con el mismo group.id se reparten las particiones. No necesitas crear un archivo por consumidor: ejecuta el mismo script en dos terminales. Grupos distintos leen de forma independiente.
- La práctica avanzada se inicia con `./setup_kafka_ha.sh`; la interfaz visual se ejecuta con `./setup_kafbat.sh`. Ambas usan puertos y estado separados del broker básico.
- Kafka no valida automáticamente los mensajes JSON contra un esquema. Schema Registry se explica como extensión conceptual; no está instalado en este laboratorio.

## Validación del código

~~~bash
cd frontend && npm ci && npm run build
cd ..
python -m compileall -q runtime
bash -n setup_kafka.sh stop_kafka.sh diagnose_kafka.sh setup_kafka_ha.sh stop_kafka_ha.sh setup_kafbat.sh stop_kafbat.sh
~~~

Las comprobaciones de sintaxis no sustituyen la prueba de integración con un clúster Kafka real.
