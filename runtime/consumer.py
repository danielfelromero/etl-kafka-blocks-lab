"""Educational consumer: read, validate, and transform air-quality events."""

import json
import os

from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "mediciones_aire")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "etl-kafka-blocks-demo")


def transform_event(event):
    """Minimal ETL: validate required fields and add a simple category."""
    required = ("station_id", "pm25", "event_time")
    missing = [field for field in required if field not in event]
    if missing:
        return None, f"Missing required fields: {', '.join(missing)}"

    try:
        pm25 = float(event["pm25"])
    except (TypeError, ValueError):
        return None, "pm25 must be numeric"

    if pm25 < 0:
        return None, "pm25 cannot be negative"

    transformed = {
        **event,
        "pm25": pm25,
        "pm25_category": "high" if pm25 >= 25 else "normal",
    }
    return transformed, None


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    print(f"Listening on topic '{TOPIC}'. Press Ctrl+C to stop.")

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Kafka error: {message.error()}")
                continue

            try:
                event = json.loads(message.value().decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                print(f"Invalid JSON at offset {message.offset()}: {error}")
                # This introductory example does not commit malformed JSON.
                # Production systems need an explicit retry or dead-letter policy.
                continue

            result, error = transform_event(event)
            if error:
                print(f"REJECTED offset={message.offset()}: {error} | event={event}")
            else:
                print(f"PROCESSED offset={message.offset()}: {json.dumps(result)}")

            # Commit after the event has been processed or explicitly rejected.
            consumer.commit(message=message, asynchronous=False)
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
