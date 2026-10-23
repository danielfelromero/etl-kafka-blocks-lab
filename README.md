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
2. Espera a que termine la configuración del contenedor Python y la instalación de dependencias.
3. En una terminal, inicia Kafka con el script incluido:

   ```bash
   chmod +x setup_kafka.sh
   ./setup_kafka.sh
   ```

   El script descarga Kafka 3.9.1, inicializa KRaft y arranca el broker en segundo plano. Espera a que confirme que está listo.

4. En una segunda terminal, inicia Blockly:

   ```bash
   cd frontend
   npm run dev -- --host 0.0.0.0
   ```

5. Abre el puerto **5173** en Codespaces.
6. Usa **Ejemplo: productor** o **Ejemplo: consumidor**, revisa el Python generado y pulsa **Copiar Python**.
7. Pega el código en un archivo `.py` en la raíz del repositorio y ejecútalo con el entorno virtual:

   ```bash
   .venv/bin/python producer_generated.py
   ```

   Para probar los scripts de referencia, abre otras terminales y ejecuta primero el consumidor y luego el productor:

   ```bash
   .venv/bin/python runtime/consumer.py
   .venv/bin/python runtime/producer.py
   ```

Desde el Codespace, los procesos Python usan `localhost:9092`, porque Kafka se ejecuta en el mismo contenedor de desarrollo, igual que en el repositorio docente de referencia.

## Guía de exploración

### Mensaje y productor
- Cambia el identificador de estación, los valores de PM2.5 y el tópico.
- Observa cómo cambian el JSON y el Python generado.
- Identifica la llamada `producer.produce(...)` y la clave del mensaje.

### Consumidor
- Ejecuta el consumidor y observa los eventos leídos.
- Cambia el grupo `group.id` y compara el comportamiento.
- Explora la validación de PM2.5 y los registros rechazados.

### Pasar a Python
- Copia el código generado a un archivo Python y ejecútalo.
- Modifica una instrucción y compara su resultado con los bloques.

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

Blockly genera código; no simula Kafka. El consumidor transforma y muestra los datos en la terminal. No se incluye todavía persistencia en base de datos, Airflow ni un despliegue de producción.

## Detener el entorno

Cuando termines, detén el Codespace desde [GitHub Codespaces](https://github.com/codespaces) para liberar sus recursos.

No hay entrega ni calificación. El propósito es experimentar, hacer preguntas y relacionar los bloques con el código.
