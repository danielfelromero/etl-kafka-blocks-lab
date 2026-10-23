# Interfaz Blockly

Aplicación frontend del laboratorio. Ejecutar desde esta carpeta:

```bash
npm install
npm run dev -- --host 0.0.0.0
```

Abre el puerto 5173 en Codespaces.

Los bloques personalizados representan configuración, mediciones y publicación. El generador produce Python legible usando `confluent-kafka`; el botón de descarga guarda el script.

En esta versión, el editor se concentra en el productor. El consumidor se proporciona como Python en `runtime/consumer.py`, para que el grupo compare ambos roles sin saturar de conceptos el primer editor.
