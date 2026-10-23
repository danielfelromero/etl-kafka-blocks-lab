"""Educational producer: emit sample air-quality events to Kafka."""

import json
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "mediciones_aire")


def delivery_report(error, message):
    """Print whether Kafka confirmed delivery of a message."""
    if error is not None:
        print(f"Delivery failed: {error}")
    else:
        print(
            f"Delivered to {message.topic()} "
            f"[partition {message.partition()}] at offset {message.offset()}"
        )


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    sample_measurements = [
        {"station_id": "A01", "pm25": 18.4, "temperature_c": 27.1, "humidity_pct": 68},
        {"station_id": "A02", "pm25": 25.2, "temperature_c": 29.3, "humidity_pct": 61},
        {"station_id": "A01", "pm25": 16.8, "temperature_c": 27.4, "humidity_pct": 66},
        {"station_id": "A03", "pm25": -1.0, "temperature_c": 28.0, "humidity_pct": 70},
        {"station_id": "A02", "pm25": 31.7, "temperature_c": 30.1, "humidity_pct": 57},
    ]

    try:
        for measurement in sample_measurements:
            event = {
                **measurement,
                "event_time": datetime.now(timezone.utc).isoformat(),
            }
            producer.produce(
                TOPIC,
                key=event["station_id"],
                value=json.dumps(event),
                callback=delivery_report,
            )
            producer.poll(0)
            print(f"Queued event: {event}")
            time.sleep(1)
        remaining = producer.flush(10)
        if remaining:
            print(f"Warning: {remaining} message(s) remained undelivered at shutdown.")
    finally:
        producer.flush(5)


if __name__ == "__main__":
    main()
