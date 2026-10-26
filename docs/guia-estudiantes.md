# Guía de exploración: Kafka desde bloques

Práctica de exploración, no calificable. No hay entrega: construye, cambia valores, ejecuta y discute lo que observas.

## Preparar el laboratorio

1. Abre el repositorio en GitHub Codespaces.
2. En una terminal, inicia el broker básico: `./setup_kafka.sh`.
3. Comprueba la conexión con `./diagnose_kafka.sh`.
4. En otra terminal, ejecuta `cd frontend && npm run dev -- --host 0.0.0.0` y abre el puerto 5173.
5. En Blockly selecciona **Modo básico · 1 broker**. El selector no verifica ni inicia Kafka por sí mismo.

## Actividad 1 — Productor, topic y evento

Carga el ejemplo de productor. Identifica la configuración del cliente, el evento JSON, el topic, la clave, la partición y los headers. Cambia el topic y modifica un campo del evento. Copia el código generado a `producer_generated.py` y ejecútalo con `.venv/bin/python producer_generated.py`.

Pregunta: ¿qué pertenece al valor JSON y qué se transporta como metadato del mensaje? ¿Qué confirma un callback de entrega?

## Actividad 2 — Claves y particiones

Envía varios eventos con la misma clave y después cambia la clave. Observa la partición y el offset. Con el particionador habitual, la misma clave suele asignarse de forma consistente a una partición bajo la misma configuración; cambiar el número de particiones o el particionador puede cambiar el mapeo. Si no se especifica clave, se aplica otra estrategia de particionado. El orden se garantiza dentro de una partición, no globalmente entre todas las particiones.

Prueba la partición manual solo si sabes que el topic tiene esa partición. ¿Qué error produce una partición inexistente?

## Actividad 3 — Grupos y offsets

Carga el consumidor y ejecuta el script generado. Después:
- Cambia `group.id` y vuelve a ejecutar: el grupo nuevo tiene su propio estado de offsets.
- Ejecuta dos consumidores con el mismo grupo: las particiones se reparten entre miembros del grupo; no es una copia de cada mensaje a cada consumidor.
- Ejecuta consumidores con grupos distintos: cada grupo puede leer el mismo topic de forma independiente.
- Detén un consumidor mientras el productor publica y reinícialo. Observa el offset confirmado y el lag cuando esté disponible.

`auto.offset.reset` se aplica cuando el grupo no tiene un offset válido; no significa que se rebobine cada vez.

## Actividad 4 — Validación de eventos

Elimina `station_id`, escribe texto en `pm25` o prueba un valor negativo. Distingue:
1. Kafka recibió el mensaje.
2. El consumidor lo leyó.
3. La validación de aplicación lo aceptó o rechazó.
4. El consumidor confirmó el offset.

La guía de referencia confirma el offset después de procesar o rechazar explícitamente el evento. Para JSON malformado, el consumidor de referencia no confirma el offset, por lo que puede volver a encontrarse con el mensaje al reiniciar. En producción habría que definir una política explícita: detenerse, reintentar o publicar en un dead-letter topic.

## Actividad 5 — Replicación y tolerancia a fallos (solo escenario avanzado)

Necesitas Docker Compose en un equipo o VM compatible. El escenario avanzado usa tres nodos Kafka con roles broker/controller y quorum KRaft de tres nodos. No se garantiza que Docker esté disponible dentro de Codespaces.

~~~bash
docker compose -f docker-compose.ha.yml up -d --wait
docker compose -f docker-compose.ha.yml exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:29092 --create --if-not-exists --topic mediciones_aire_ha --partitions 3 --replication-factor 3 --config min.insync.replicas=2
docker compose -f docker-compose.ha.yml exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:29092 --describe --topic mediciones_aire_ha
~~~

Abre Kafbat en `http://localhost:8080`. Inspecciona brokers, particiones, líderes, réplicas e ISR. Publica mensajes con `acks=1` y después con `acks=all`. Detén un nodo con `docker compose -f docker-compose.ha.yml stop kafka-2`, vuelve a inspeccionar el topic y luego recupéralo con `docker compose -f docker-compose.ha.yml start kafka-2`.

Con `min.insync.replicas=2` y `acks=all`, las escrituras requieren al menos dos réplicas sincronizadas. Si quedan menos, las escrituras pueden fallar; no confundas esa falta de disponibilidad temporal con pérdida automática de todos los datos. Registra únicamente lo que realmente observas: la guía no presupone que el experimento se haya ejecutado.

## Kafbat UI

Kafbat viene en los archivos Compose, no en el modo básico de Codespaces sin Docker. En la red Docker usa los nombres de servicio y el listener interno; desde el navegador se abre el puerto publicado 8080. No configures Kafbat con `localhost:9092` dentro de su contenedor.

## Schema Registry: extensión opcional

Schema Registry administra contratos versionados y políticas de compatibilidad; no es un validador automático incorporado a Kafka. Avro, JSON Schema y Protobuf son formatos distintos. JSON válido sintácticamente puede no cumplir el contrato. Este laboratorio no inicia un Registry ni pretende simularlo.

## Cierre de discusión

- ¿Qué diferencia hay entre topic, partición, mensaje y grupo?
- ¿Qué responsabilidad tiene Kafka y cuál el código ETL?
- ¿Qué cambia entre confirmar un offset antes o después del procesamiento?
- ¿Qué garantiza `acks=all` y qué papel tiene `min.insync.replicas`?
- ¿Qué cambia al pasar de un broker a tres brokers?
- ¿Qué necesitarías para persistir los eventos válidos en una base de datos?

## Parada

Modo básico: `./stop_kafka.sh`. Modo avanzado: `docker compose -f docker-compose.ha.yml down`. Añade `-v` solo si quieres borrar también los datos persistidos.
