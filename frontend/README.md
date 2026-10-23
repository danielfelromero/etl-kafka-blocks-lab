# Interfaz Blockly

Aplicación visual del laboratorio. Ejecuta desde la carpeta `frontend`:

~~~bash
npm install
npm run dev -- --host 0.0.0.0
~~~

En Codespaces, abre el puerto 5173 desde la pestaña **Ports**.

## Ejemplos incluidos

- **Ejemplo: productor:** configura el broker, crea mediciones, repite la publicación y espera entre eventos.
- **Ejemplo: consumidor:** configura broker, tópico y grupo; lee eventos y conecta una etapa de procesamiento.
- **Validar PM2.5 y clasificar:** comprueba campos obligatorios, rechaza valores negativos y añade una categoría.
- **Solo mostrar evento JSON:** imprime el evento sin transformarlo.

El panel muestra el Python generado mientras se modifican los bloques. **Copiar Python** facilita pegarlo en un archivo del Codespace; **Descargar Python** lo descarga al equipo desde el que se abre el navegador.

El broker Kafka se inicia mediante el entorno de Codespaces configurado en `.devcontainer`. La interfaz genera código, pero no ejecuta Python dentro del navegador. Los scripts generados se ejecutan por separado con el entorno virtual Python de la raíz del repositorio.
