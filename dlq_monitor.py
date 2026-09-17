"""
dlq_monitor.py
--------------
Small utility to tail the Dead Letter Queue and pretty-print failed
messages -- handy to keep open in a second terminal during the live demo
so the marker can see permanently-failed / exhausted-retry messages
arriving in real time.

Usage:
    python dlq_monitor.py
"""

import logging

from confluent_kafka.avro import AvroConsumer

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | DLQ-MONITOR | %(levelname)s | %(message)s",
)
log = logging.getLogger("dlq_monitor")


def run():
    consumer = AvroConsumer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "schema.registry.url": config.SCHEMA_REGISTRY_URL,
            "group.id": "dlq-monitor-group",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([config.DLQ_TOPIC])
    log.info(f"Watching DLQ topic '{config.DLQ_TOPIC}'...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error(msg.error())
                continue

            record = msg.value()
            log.warning(
                "\n--- DEAD LETTER ---\n"
                f"  orderId       : {record.get('orderId')}\n"
                f"  product       : {record.get('product')}\n"
                f"  price         : {record.get('price')}\n"
                f"  errorType     : {record.get('errorType')}\n"
                f"  errorMessage  : {record.get('errorMessage')}\n"
                f"  retryCount    : {record.get('retryCount')}\n"
                f"  source        : {record.get('sourceTopic')}"
                f"[{record.get('sourcePartition')}]@{record.get('sourceOffset')}\n"
                "--------------------"
            )
    except KeyboardInterrupt:
        log.info("Stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
