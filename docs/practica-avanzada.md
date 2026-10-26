# Práctica 2 — Avanzada: replicación, interfaz visual y tolerancia a fallos

## Objetivos

Al terminar, podrás comparar un broker con un clúster replicado, inspeccionar particiones y réplicas en Kafbat UI, producir y consumir desde varios topics, y observar qué sucede cuando un nodo deja de estar disponible.

Esta práctica es independiente de la básica. No cambies ni borres `.kafka/data`, que pertenece al broker básico.

## 1. Preparar el clúster avanzado sin Docker

Desde la raíz del repositorio:

~~~bash
chmod +x setup_kafka_ha.sh stop_kafka_ha.sh setup_kafbat.sh stop_kafbat.sh
./setup_kafka_ha.sh
~~~

El script inicia tres nodos Kafka KRaft en la misma máquina, cada uno con su propio ID, puerto, log y directorio de datos. El bootstrap de clientes es:

~~~text
localhost:19092,localhost:19093,localhost:19094
~~~

El clúster crea los topics `mediciones_aire` y `mediciones_aire_ha`, ambos con tres particiones, factor de replicación 3 y `min.insync.replicas=2`.

Verifica el estado:

~~~bash
kafka_2.13-3.9.1/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:19092,localhost:19093,localhost:19094
kafka_2.13-3.9.1/bin/kafka-topics.sh --bootstrap-server localhost:19092,localhost:19093,localhost:19094 --describe --topic mediciones_aire_ha
~~~

En la descripción identifica `PartitionCount`, `ReplicationFactor`, `Leader`, `Replicas` e `Isr`. El líder puede variar entre particiones; no asumas que todos tienen el mismo líder.

## 2. Iniciar Kafbat UI sin Docker

En otra terminal, desde la raíz del repositorio:

~~~bash
./setup_kafbat.sh
~~~

El primer inicio descarga el código fuente oficial de Kafbat UI 1.3.0 y lo ejecuta con Gradle. Requiere Java 17 o superior, Git, curl y acceso a Internet; la compilación inicial puede tardar varios minutos y consumir memoria adicional. No instala Docker ni modifica el broker básico.

Cuando el script indique que está listo, abre el puerto **8080** en Codespaces y entra a la interfaz. El clúster aparece como `etl-kafka-ha`.

En Kafbat explora:

1. **Brokers:** comprueba que aparecen tres brokers y revisa cuál actúa como controlador.
2. **Topics:** abre `mediciones_aire_ha` y compara las particiones, los líderes, las réplicas y el ISR.
3. **Messages:** consulta mensajes después de producir eventos en los siguientes pasos.
4. **Consumer groups:** cuando haya un consumidor activo, observa sus offsets y el lag si está disponible.

Kafbat es una interfaz de administración y observabilidad; los scripts y Blockly siguen siendo las herramientas para generar y ejecutar los ejemplos de la práctica.

## 3. Producir en un topic replicado desde Blockly

Abre la aplicación Blockly en una terminal separada:

~~~bash
cd frontend
npm install
npm run dev -- --host 0.0.0.0
~~~

Abre el puerto 5173 y selecciona **Modo avanzado · 3 brokers**. En el bloque de configuración del productor, utiliza el bootstrap avanzado anterior.

Construye un productor con estos pasos:

1. Configura el productor con los tres brokers.
2. Añade la configuración avanzada del productor.
3. Selecciona primero `acks=all`, reintentos mayores que cero e idempotencia activada.
4. Crea un evento JSON de medición, por ejemplo `station_id`, `pm25` y `temperature_c`.
5. En el bloque de envío escribe el topic `mediciones_aire_ha`, una clave como `A01` y un header como `source=sensor`.
6. Envía varios eventos y descarga el Python generado en la raíz del repositorio.
7. Ejecuta el script con el entorno del laboratorio: `.venv/bin/python producer_generated.py`.

Anota los valores de `topic`, `partition` y `offset` que imprime el callback. En Kafbat, abre el topic y confirma que los eventos aparecen en la partición correspondiente.

Repite con `acks=1`. Registra diferencias observadas, sin asumir que una prueba breve necesariamente provocará pérdida de mensajes.

## 4. Trabajar con varios topics

La interfaz Blockly permite escribir el nombre del topic en cada bloque de envío. Esto permite enviar eventos de categorías distintas con el mismo productor.

Crea un topic adicional para alertas:

~~~bash
kafka_2.13-3.9.1/bin/kafka-topics.sh --bootstrap-server localhost:19092,localhost:19093,localhost:19094 --create --if-not-exists --topic alertas_aire --partitions 3 --replication-factor 3 --config min.insync.replicas=2
~~~

Desde Blockly, construye un productor que envíe:
- mediciones normales a `mediciones_aire_ha`;
- alertas de PM2.5 elevado a `alertas_aire`.

Usa valores JSON distintos para distinguir una medición de una alerta. En Kafbat, abre cada topic y verifica que los eventos se almacenaron en el destino esperado.

Ahora crea un consumidor desde Blockly:

1. En el bloque de configuración, utiliza el mismo bootstrap de tres brokers.
2. En el campo de topics escribe `mediciones_aire_ha, alertas_aire`.
3. Usa un `group.id` nuevo, por ejemplo `grupo-multitopic-01`.
4. Elige `earliest` para intentar leer los eventos existentes si el grupo no tiene offsets válidos.
5. Añade el bloque para mostrar topic, partición, offset, clave, headers y JSON.
6. Ejecuta el consumidor y vuelve a producir eventos en ambos topics.

Comprueba que el consumidor informa de qué topic procede cada mensaje. La suscripción a varios topics no fusiona ni ordena globalmente los mensajes; el orden se conserva dentro de cada partición.

## 5. Observar la tolerancia a la caída de un nodo

Antes de detener un nodo, deja Kafbat abierto y confirma que el topic replicado tiene factor de replicación 3 y un ISR completo. Mantén activo un productor que use `acks=all`.

En otra terminal, detén únicamente el proceso del nodo 2:

~~~bash
kill "$(cat .kafka-ha/pids/kafka-2.pid)"
~~~

Observa en Kafbat y en la terminal:

- si el broker sigue apareciendo o pasa a estar desconectado;
- si cambia el líder de alguna partición;
- cómo cambia la lista ISR;
- si las escrituras continúan o fallan temporalmente;
- qué errores informa el callback del productor.

Vuelve a describir el topic con el CLI:

~~~bash
kafka_2.13-3.9.1/bin/kafka-topics.sh --bootstrap-server localhost:19092,localhost:19093,localhost:19094 --describe --topic mediciones_aire_ha
~~~

Con `min.insync.replicas=2` y `acks=all`, las escrituras requieren al menos dos réplicas sincronizadas. La caída de un nodo no implica que todas las operaciones vayan a fallar ni garantiza que todas vayan a continuar: el resultado depende del ISR, del líder y del estado del quórum. Observa la evidencia real.

No detengas dos nodos a la vez para esta actividad. Los tres procesos comparten la misma máquina; esta prueba no simula la pérdida de un host o de una zona de disponibilidad.

## 6. Recuperar el clúster y comprobar la recuperación

Como el nodo 2 se detuvo manualmente, detén de forma ordenada los procesos restantes y vuelve a iniciar el clúster:

~~~bash
./stop_kafka_ha.sh
./setup_kafka_ha.sh
~~~

El script conserva los datos persistidos de los nodos y no debe reformatearlos cuando los archivos de metadatos existen. Espera a que el inicio termine antes de continuar.

Vuelve a Kafbat y al CLI. Comprueba que los tres brokers están disponibles, que las réplicas vuelven a sincronizarse y que el ISR se recupera. Repite una producción breve y verifica la llegada de mensajes.

## 7. Comparación y preguntas de discusión

1. ¿Qué diferencia hay entre particiones y factor de replicación?
2. ¿Qué significan líder, réplicas e ISR?
3. ¿Qué diferencia observaste entre `acks=1` y `acks=all`?
4. ¿Por qué una configuración con `min.insync.replicas=2` puede rechazar escrituras si el ISR baja de dos?
5. ¿Qué ventaja ofrece Kafbat frente a consultar solo el CLI?
6. ¿Qué cambia cuando un consumidor se suscribe a dos topics?
7. ¿Por qué tres procesos en un único Codespace no equivalen a tres máquinas independientes?

## 8. Detener servicios

Detén primero la interfaz y después el clúster avanzado:

~~~bash
./stop_kafbat.sh
./stop_kafka_ha.sh
~~~

Los datos avanzados permanecen en `.kafka-ha/data`; no borres ese directorio salvo que quieras reinicializar deliberadamente el clúster. La práctica básica conserva su propio estado en `.kafka/data` y no debe borrarse para limpiar la práctica avanzada.
