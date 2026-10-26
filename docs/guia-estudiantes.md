# Guía de prácticas: Kafka desde bloques

Este laboratorio contiene dos prácticas independientes. No hay entrega calificable: construye, cambia valores, ejecuta y discute lo que observas.

- [Práctica 1 — Básica: productor, consumidor, particiones y offsets](practica-basica.md)
- [Práctica 2 — Avanzada: replicación, Kafbat y tolerancia a fallos](practica-avanzada.md)

## Antes de comenzar

1. Abre el repositorio en GitHub Codespaces.
2. Completa primero la práctica básica, que no necesita Docker.
3. Para la práctica avanzada, usa Docker Compose en un equipo/VM compatible o el script sin Docker setup_kafka_ha.sh. La opción de tres procesos locales consume más memoria y demuestra la caída de un proceso, no la pérdida de una máquina física.
4. La selección del escenario en Blockly solo cambia la dirección sugerida del broker; no arranca ni verifica la infraestructura.
