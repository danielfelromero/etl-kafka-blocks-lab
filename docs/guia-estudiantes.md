# Guía de exploración: Kafka desde bloques

Esta guía es para experimentar en clase. **No es una entrega calificable y no hay que entregar un archivo final.** El propósito es conectar conceptos con comportamiento real y hacer preguntas mientras se prueba el código.

## Objetivo

Construir un productor y un consumidor para un flujo de mediciones de calidad del aire, primero con bloques y después con Python. Kafka se ejecuta como broker real en el Codespace.

## Antes de empezar

1. Abre el repositorio en GitHub y selecciona **Code → Codespaces → Create codespace on main**.
2. Espera a que termine la configuración. Kafka se inicia junto con el entorno.
3. En la terminal de Codespaces, ejecuta:

   ~~~bash
   cd frontend
   npm run dev -- --host 0.0.0.0
   ~~~

4. Abre el puerto **5173** desde la notificación de Codespaces o desde la pestaña **Ports**.

## Parte 1. Construir el productor (aprox. 30 minutos)

1. Pulsa **Ejemplo: productor**.
2. Identifica los bloques de configuración del productor, medición, publicación, repetición y espera.
3. Cambia el identificador de estación, el valor de PM2.5 y el tópico. Observa cómo cambia el código.
4. Pulsa **Copiar Python** y crea un archivo **producer_generated.py** en la raíz del repositorio; pega allí el código.
5. En otra terminal situada en la raíz, ejecuta:

   ~~~bash
   .venv/bin/python producer_generated.py
   ~~~

6. Observa las confirmaciones de entrega, tópico, partición y offset.

Preguntas para discutir:

- ¿Por qué el productor no escribe directamente en la base de datos?
- ¿Cuál es la diferencia entre la clave del mensaje y su valor JSON?
- ¿Qué cambia si se modifica el tópico?

## Parte 2. Construir y observar el consumidor (aprox. 30 minutos)

1. Pulsa **Ejemplo: consumidor**.
2. Observa los campos de broker, tópico, grupo de consumidores y estrategia de procesamiento.
3. Deja seleccionada la estrategia **Validar PM2.5 y clasificar** y copia el código en **consumer_generated.py**.
4. En una terminal, inicia el consumidor:

   ~~~bash
   .venv/bin/python consumer_generated.py
   ~~~

5. En otra terminal, ejecuta **producer_generated.py**. Comprueba que el consumidor recibe los eventos, clasifica los valores y rechaza el PM2.5 negativo.

Prueba también la estrategia **Solo mostrar eventos**. Compara el código y el resultado de las dos opciones.

## Parte 3. Leer y modificar Python (aprox. 30 minutos)

Relaciona los conceptos visuales con estas partes del código:

- La configuración de bootstrap servers.
- El nombre del tópico.
- La llamada producer.produce(...).
- La clave del mensaje y la serialización JSON.
- Las operaciones consumer.subscribe(...) y consumer.poll(...).
- La validación de PM2.5 y el commit del offset.

Haz una modificación pequeña directamente en el archivo Python y vuelve a ejecutarlo. El objetivo es comprobar que los bloques son una representación visual del código, no un sustituto de Python.

## Parte 4. Experimentos de Kafka (aprox. 25 minutos)

Elige dos o tres experimentos y observa los resultados:

- Cambia la clave del mensaje. ¿Qué pasa con la partición asignada?
- Cambia el identificador del grupo de consumidores y vuelve a consumir.
- Detén el consumidor mientras el productor publica mensajes; luego vuelve a iniciarlo.
- Cambia un valor de PM2.5 a uno negativo y observa la validación.
- Envía un mensaje con un campo obligatorio ausente y analiza cómo debería tratarse.

## Parte 5. Cierre de discusión (aprox. 15 minutos)

Utiliza las preguntas como conversación, no como examen:

- ¿Qué responsabilidad tiene Kafka y cuál tiene el consumidor?
- ¿Qué diferencia hay entre un tópico, una partición, un mensaje y un grupo de consumidores?
- ¿Qué ocurre si un mensaje se lee pero el procesamiento falla antes de confirmar el offset?
- ¿Qué faltaría para que el consumidor guardara los registros válidos en una base de datos?
- ¿Qué partes deberían cambiar al pasar de esta práctica a un pipeline ETL más real?

## Ejecutar los scripts de referencia

El repositorio incluye ejemplos escritos directamente en Python, por si quieres comparar con lo generado:

~~~bash
# Terminal 1: consumidor
.venv/bin/python runtime/consumer.py

# Terminal 2: productor
.venv/bin/python runtime/producer.py
~~~

Desde Codespaces, los scripts usan kafka:29092 para conectarse al servicio Kafka. Si los ejecutas desde una terminal local fuera de Codespaces, utiliza localhost:9092 en la configuración.

## Alcance didáctico

El consumidor de esta versión transforma y muestra los datos en terminal; no persiste todavía en una base de datos. Tampoco se incluyen Airflow, esquemas centralizados ni despliegues productivos. Se incorporarán solo después de comprender el recorrido básico productor → Kafka → consumidor.
