# Guía de prácticas: Kafka desde bloques

Este laboratorio contiene dos prácticas independientes. No hay entrega calificable: construye, cambia valores, ejecuta y discute lo que observas.

- [Práctica 1 — Básica: productor, consumidor, particiones y offsets](practica-basica.md)
- [Práctica 2 — Avanzada: replicación, interfaz visual, multitópico y tolerancia a fallos](practica-avanzada.md)

## Antes de comenzar

1. Abre el repositorio en GitHub Codespaces.
2. Completa primero la práctica básica. Su broker único se inicia con `./setup_kafka.sh` y no requiere Docker.
3. La práctica avanzada inicia tres procesos Kafka adicionales con `./setup_kafka_ha.sh`; utiliza puertos diferentes y conserva sus datos en `.kafka-ha/`.
4. Para la interfaz visual, ejecuta `./setup_kafbat.sh`. La primera vez descarga y compila Kafbat UI; necesita Java 17 o superior, Git, curl, Internet y memoria adicional.
5. La selección del escenario en Blockly solo cambia la dirección sugerida del broker; no arranca ni verifica la infraestructura.
6. Los tres procesos avanzados comparten máquina y sistema operativo: la práctica permite estudiar la caída de un proceso, no la pérdida de una máquina física.
