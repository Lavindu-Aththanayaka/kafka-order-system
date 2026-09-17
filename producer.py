"""
producer.py
-----------
Generates simulated e-commerce order events and publishes them to the
'orders' Kafka topic, serialized with Avro against the Confluent Schema
Registry.

Usage:
    python producer.py                 # send 50 messages, one every 0.5s
    python producer.py --count 200     # send 200 messages
    python producer.py --interval 0.1  # send faster
    python producer.py --forever       # keep producing until Ctrl+C
"""

import argparse
import logging
import random
import time
import uuid

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | PRODUCER | %(levelname)s | %(message)s",
)
log = logging.getLogger("producer")

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def load_schema() -> avro.schema.Schema:
    with open("schemas/order.avsc", "r") as f:
        return avro.loads(f.read())


def build_producer(value_schema: avro.schema.Schema) -> AvroProducer:
    return AvroProducer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "schema.registry.url": config.SCHEMA_REGISTRY_URL,
        },
        default_value_schema=value_schema,
    )


def make_random_order() -> dict:
    """Create one random order matching the Avro schema."""
    return {
        "orderId": str(uuid.uuid4().int)[:8],   # e.g. "10029384"
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
    }


def delivery_report(err, msg):
    if err is not None:
        log.error(f"Delivery failed for record {msg.key()}: {err}")
    else:
        log.info(
            f"Delivered order -> topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def run(count: int, interval: float, forever: bool):
    schema = load_schema()
    producer = build_producer(schema)

    log.info(f"Starting producer. Target topic: '{config.ORDERS_TOPIC}'")
    sent = 0
    try:
        while forever or sent < count:
            order = make_random_order()
            producer.produce(
                topic=config.ORDERS_TOPIC,
                value=order,
                key=order["orderId"],
                callback=delivery_report,
            )
            # poll(0) triggers delivery callbacks for previously produced messages
            producer.poll(0)
            sent += 1
            time.sleep(interval)
    except KeyboardInterrupt:
        log.info("Interrupted by user.")
    finally:
        log.info(f"Flushing producer, {sent} messages sent this run...")
        producer.flush()
        log.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Avro order producer")
    parser.add_argument("--count", type=int, default=50, help="Number of messages to send")
    parser.add_argument("--interval", type=float, default=0.5, help="Seconds between messages")
    parser.add_argument("--forever", action="store_true", help="Produce indefinitely until Ctrl+C")
    args = parser.parse_args()

    run(count=args.count, interval=args.interval, forever=args.forever)
