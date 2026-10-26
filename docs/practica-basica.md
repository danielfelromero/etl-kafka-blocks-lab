# Práctica 1 — Básica: productor, consumidor, particiones y offsets

## Objetivo

Explorar la producción y el consumo de eventos con un broker Kafka real, sin Docker-in-Docker. El modo básico no ofrece tolerancia a la pérdida del broker.

## 1. Iniciar Kafka

Desde la raíz del repositorio:

~~~bash
chmod +x setup_kafka.sh diagnose_kafka.sh stop_kafka.sh
./setup_kafka.sh
./diagnose_kafka.sh
~~~

La primera ejecución descarga Kafka y puede tardar varios minutos. No borres `/tmp/kraft-combined-logs` como paso rutinario: el script utiliza `.kafka/data`. Si falla, revisa `.kafka/kafka-server.log`. El topic `mediciones_aire` queda preparado con tres particiones.

## 2. Abrir Blockly y ejecutar scripts

En otra terminal:

~~~bash
cd frontend
npm install
npm run dev -- --host 0.0.0.0
~~~

Abre el puerto 5173. En Blockly elige **Modo básico · 1 broker**, carga el productor, modifica los bloques y descarga el código como `producer_generated.py`. Desde la raíz del repositorio, ejecuta:

~~~bash
source .venv/bin/activate
python producer_generated.py
~~~

Si no activas el entorno, ejecuta `.venv/bin/python producer_generated.py` desde la raíz. Para el consumidor, carga el ejemplo y guárdalo como `consumer_generated.py`; en otra terminal ejecuta `.venv/bin/python consumer_generated.py`.

## 3. Ver partición y offset antes de ejecutar un consumidor

El productor generado registra un callback de entrega. Cuando Kafka confirma cada mensaje, la salida muestra `topic`, `partition` y `offset`. Por tanto, no necesitas un consumidor para observar esos metadatos.

También puedes inspeccionar el topic desde la terminal:

~~~bash
./diagnose_kafka.sh
~~~

Busca `PartitionCount:3` en la descripción del topic. `kafka-get-offsets.sh` informa offsets earliest/latest por partición; esos offsets son posiciones del log, no necesariamente el número de mensajes visibles para un consumidor concreto.

## 4. Claves y particiones

- Con particionado automático, la clave se usa para calcular una partición; cambiar de `A01` a `A02` no garantiza una partición distinta.
- Si eliges una partición manual, el número debe existir. Las particiones empiezan en cero: para tres particiones válidas, los números son 0, 1 y 2.
- El inicio crea `mediciones_aire` con tres particiones. Si utilizas otro topic, consulta su número con `./diagnose_kafka.sh` o crea el topic con el número de particiones deseado.
- El orden se garantiza dentro de una partición, no globalmente entre todas las particiones.

Experimenta con clave vacía, clave fija, clave distinta y selección manual. Compara la salida del productor con la del consumidor.

## 5. Ejecutar varios consumidores

No hace falta crear un archivo por consumidor. Ejecuta el mismo `consumer_generated.py` en dos terminales con el mismo `group.id`: Kafka repartirá las particiones entre los miembros del grupo. Si cada consumidor usa un grupo distinto, ambos grupos pueden leer el mismo topic de forma independiente.

Cambia `group.id` y `auto.offset.reset`. Recuerda que `auto.offset.reset` se usa cuando el grupo no tiene un offset válido; no rebobina el grupo en cada inicio.

## Preguntas de discusión

1. ¿Qué diferencia hay entre valor JSON, clave y headers?
2. ¿Qué indica el callback de entrega y cuándo se conoce el offset?
3. ¿Por qué una clave no equivale a un número de partición?
4. ¿Qué ocurre cuando dos consumidores comparten un grupo?
5. ¿Qué límites tiene un único broker frente a un clúster replicado?

## Parar

~~~bash
./stop_kafka.sh
~~~

El script conserva `.kafka/data`. Solo elimina ese directorio si quieres reiniciar deliberadamente el estado persistido.
