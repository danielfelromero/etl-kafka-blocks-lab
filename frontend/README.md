# Interfaz Blockly

~~~bash
npm install
npm run dev -- --host 0.0.0.0
~~~

Abre el puerto 5173 en Codespaces.

## Selector de escenario

- **Básico:** broker sugerido `localhost:9092`, iniciado mediante `./setup_kafka.sh` en el contenedor Codespaces.
- **Avanzado:** direcciones `localhost:19092,localhost:19093,localhost:19094`; requiere que el clúster definido en `../docker-compose.ha.yml` esté realmente activo en un host con Docker.

El selector cambia sugerencias e instrucciones, pero no inicia ni verifica infraestructura. El estado se muestra explícitamente como no verificado. La aplicación Blockly genera código; no ejecuta el código Python ni administra brokers.

## Ejemplos y bloques

Productor: conexión, configuración avanzada, evento JSON, topic, clave, partición automática/manual y headers. Consumidor: broker, lista de topics, grupo, offset inicial e inspección/validación del mensaje.

Los scripts se ejecutan desde la raíz del repositorio con el entorno virtual, por ejemplo `.venv/bin/python producer_generated.py`. Consulta `../docs/guia-estudiantes.md` para las actividades.
