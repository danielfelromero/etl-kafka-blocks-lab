# ETL Kafka Blocks Lab

Laboratorio didáctico para aprender el flujo de eventos de Kafka mediante bloques visuales y conectar esos conceptos con Python. Es una práctica exploratoria, no calificable.

## Objetivo

1. Construir un evento de medición de calidad del aire con bloques.
2. Ver el código Python generado.
3. Ejecutar un productor y un consumidor contra un broker Kafka real.
4. Comprender broker, tópico, clave, valor, productor, consumidor y grupo de consumidores.
5. Modificar el Python generado para conectar la representación visual con el código.

## Abrir en GitHub Codespaces

1. Abre el repositorio y selecciona **Code → Codespaces → Create codespace on main**.
2. Espera a que termine la configuración de `.devcontainer`.
3. Kafka se inicia automáticamente con el Codespace. El consumidor y el productor usarán la dirección interna `kafka:29092`.
4. Inicia la interfaz Blockly:

   ```bash
   cd frontend
   npm install
   npm run dev -- --host 0.0.0.0
   ```

5. Abre el puerto **5173** cuando Codespaces lo solicite.
6. Pulsa **Ejemplo: productor** o **Ejemplo: consumidor**, observa el código Python y usa **Copiar Python** o **Descargar Python**.
7. En otra terminal, situada en la raíz del repositorio, inicia el consumidor de ejemplo:

   ```bash
   .venv/bin/python runtime/consumer.py
   ```

8. En una tercera terminal, también desde la raíz del repositorio, inicia el productor:

   ```bash
   .venv/bin/python runtime/producer.py
   ```

   Para ejecutar el código generado en Blockly dentro del Codespace, pulsa **Copiar Python**, crea un archivo nuevo en la raíz del repositorio (por ejemplo, `producer_generated.py` o `consumer_generated.py`), pega el código y ejecútalo con `.venv/bin/python producer_generated.py`. El botón **Descargar Python** guarda el archivo en el equipo desde el que se abre el navegador; no lo copia automáticamente al Codespace.

Desde el contenedor de desarrollo, Python y los scripts generados se conectan a Kafka mediante `kafka:29092`. El puerto `localhost:9092` se conserva para conexiones desde una terminal local cuando Kafka se ejecuta fuera de Codespaces.

## Guía de exploración

### 1. Mensaje
- Cambia el identificador de estación y los valores.
- Observa cómo cambia la estructura del evento y el JSON generado.

### 2. Productor
- Cambia el tópico en el bloque.
- Genera el Python, descárgalo y busca la llamada `producer.produce(...)`.
- Ejecuta el productor y mira los mensajes de confirmación.

### 3. Consumidor
- Ejecuta `runtime/consumer.py` y comprueba cómo lee los mensajes.
- Cambia `group.id` y discute la diferencia entre grupos.
- Observa qué ocurre con los mensajes inválidos.

### 4. Pasar a Python
- Localiza en el código generado la configuración del broker, el tópico, la clave y el valor.
- Modifica el Python generado y vuelve a ejecutarlo.
- Compara el resultado con el ejemplo del consumidor.

## Arquitectura

```text
Blockly -> generador Python -> archivo .py -> Productor -> Kafka / mediciones_aire
                                                           |
                                                           v
                                                       Consumidor
                                                           |
                                                           v
                                                   validar y transformar
```

Blockly genera código; no simula Kafka. El broker es real y se ejecuta en Docker. El consumidor valida eventos y muestra los resultados procesados en la terminal. La persistencia en una base de datos queda para una extensión posterior.

## Requisitos

- GitHub Codespaces con Docker disponible.
- Node.js/npm y Python (incluidos en el devcontainer).
- Docker Compose.

## Detener el entorno

Al terminar, detén el Codespace desde GitHub o Visual Studio Code; el broker forma parte del entorno Compose y se detiene con él. Si ejecutas Kafka por separado en una instalación local, puedes detenerlo con `docker compose down` desde la raíz del repositorio.

No hay entrega ni calificación. El propósito es experimentar, hacer preguntas y relacionar los bloques con el código.
