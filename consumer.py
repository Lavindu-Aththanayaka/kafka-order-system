"""
consumer.py
-----------
Consumes order events from the 'orders' topic and:

  1. Performs real-time aggregation: maintains a running average price
     per product (and overall), updated incrementally per message.
  2. Applies retry logic with exponential backoff for messages that fail
     with a *transient* error (simulating e.g. a flaky downstream call).
  3. Routes messages that fail permanently, or exhaust their retries,
     to a Dead Letter Queue topic ('orders-dlq') with error metadata,
     rather than blocking the pipeline or silently dropping them.

Usage:
    python consumer.py
"""

import logging
import random
import time
from collections import defaultdict

from confluent_kafka import avro, KafkaError
from confluent_kafka.avro import AvroConsumer, AvroProducer, SerializerError

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | CONSUMER | %(levelname)s | %(message)s",
)
log = logging.getLogger("consumer")


# --------------------------------------------------------------------------------------
# Custom exceptions distinguish *transient* problems (worth retrying) from
# *permanent* ones (retrying would never help -> straight to DLQ).
# --------------------------------------------------------------------------------------
class TransientProcessingError(Exception):
    """Represents a temporary failure, e.g. a downstream timeout or connection blip."""


class PermanentProcessingError(Exception):
    """Represents a non-recoverable failure, e.g. invalid business data (poison pill)."""


# --------------------------------------------------------------------------------------
# Real-time aggregation state
# --------------------------------------------------------------------------------------
class RunningAverageAggregator:
    """
    Maintains a running (incremental) average price per product and overall,
    without storing the full history of prices -- O(1) memory per key.
    """

    def __init__(self):
        self._count = defaultdict(int)
        self._sum = defaultdict(float)

    def update(self, product: str, price: float):
        for key in (product, "__OVERALL__"):
            self._count[key] += 1
            self._sum[key] += price

    def average(self, key: str) -> float:
        if self._count[key] == 0:
            return 0.0
        return self._sum[key] / self._count[key]

    def report(self) -> str:
        lines = [f"Overall running average: {self.average('__OVERALL__'):.2f}"
                 f"  (n={self._count['__OVERALL__']})"]
        for product in sorted(k for k in self._count if k != "__OVERALL__"):
            lines.append(
                f"  {product:10s}: avg={self.average(product):.2f}  n={self._count[product]}"
            )
        return "\n".join(lines)


# --------------------------------------------------------------------------------------
# Business logic (where a real downstream call -- DB write, API call, etc. -- would go)
# --------------------------------------------------------------------------------------
def process_order(order: dict, aggregator: RunningAverageAggregator):
    """
    'Processes' an order: updates the running average and simulates a
    downstream dependency that occasionally fails.

    Failure injection is intentional and configurable via config.py so the
    retry/DLQ behaviour can be demonstrated live without a real flaky service.
    """
    roll = random.random()

    if roll < config.PERMANENT_FAILURE_PROBABILITY:
        raise PermanentProcessingError(
            f"Order {order.get('orderId')} failed business validation (simulated poison pill)."
        )

    if roll < config.PERMANENT_FAILURE_PROBABILITY + config.TRANSIENT_FAILURE_PROBABILITY:
        raise TransientProcessingError(
            f"Downstream service timeout while processing order {order.get('orderId')} (simulated)."
        )

    # "Happy path" -- update the real-time aggregation.
    aggregator.update(order["product"], float(order["price"]))


# --------------------------------------------------------------------------------------
# Retry wrapper
# --------------------------------------------------------------------------------------
def process_with_retry(order: dict, aggregator: RunningAverageAggregator) -> tuple:
    """
    Attempts to process `order`, retrying on TransientProcessingError with
    exponential backoff, up to config.MAX_RETRIES times.

    Returns:
        (success: bool, retries_used: int, error: Exception | None)
    """
    backoff = config.INITIAL_BACKOFF_SECONDS

    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            process_order(order, aggregator)
            return True, attempt - 1, None

        except PermanentProcessingError as e:
            log.error(f"Permanent failure for order {order.get('orderId')}: {e}")
            return False, attempt - 1, e

        except TransientProcessingError as e:
            if attempt < config.MAX_RETRIES:
                log.warning(
                    f"Transient failure for order {order.get('orderId')} "
                    f"(attempt {attempt}/{config.MAX_RETRIES}): {e}. "
                    f"Retrying in {backoff:.1f}s..."
                )
                time.sleep(backoff)
                backoff *= config.BACKOFF_MULTIPLIER
            else:
                log.error(
                    f"Order {order.get('orderId')} exhausted {config.MAX_RETRIES} retries. "
                    f"Sending to DLQ."
                )
                return False, attempt, e

    return False, config.MAX_RETRIES, RuntimeError("Unreachable retry state")


# --------------------------------------------------------------------------------------
# DLQ producer
# --------------------------------------------------------------------------------------
def build_dlq_producer() -> AvroProducer:
    with open("schemas/order_dlq.avsc") as f:
        dlq_schema = avro.loads(f.read())
    return AvroProducer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "schema.registry.url": config.SCHEMA_REGISTRY_URL,
        },
        default_value_schema=dlq_schema,
    )


def send_to_dlq(dlq_producer: AvroProducer, order: dict, error: Exception,
                 retries_used: int, msg):
    dlq_record = {
        "orderId": order.get("orderId"),
        "product": order.get("product"),
        "price": order.get("price"),
        "rawPayload": str(order),
        "errorMessage": str(error),
        "errorType": type(error).__name__,
        "retryCount": retries_used,
        "failedAtEpochMs": int(time.time() * 1000),
        "sourceTopic": msg.topic(),
        "sourcePartition": msg.partition(),
        "sourceOffset": msg.offset(),
    }
    dlq_producer.produce(topic=config.DLQ_TOPIC, value=dlq_record, key=order.get("orderId"))
    dlq_producer.poll(0)


# --------------------------------------------------------------------------------------
# Main consume loop
# --------------------------------------------------------------------------------------
def run():
    consumer = AvroConsumer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "schema.registry.url": config.SCHEMA_REGISTRY_URL,
            "group.id": config.CONSUMER_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # we commit manually, only after successful handling
        }
    )
    consumer.subscribe([config.ORDERS_TOPIC])

    dlq_producer = build_dlq_producer()
    aggregator = RunningAverageAggregator()

    log.info(f"Listening on topic '{config.ORDERS_TOPIC}'... (Ctrl+C to stop)")
    processed, failed = 0, 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error(f"Kafka error: {msg.error()}")
                continue

            try:
                order = msg.value()
            except SerializerError as e:
                log.error(f"Message deserialization failed: {e}. Routing to DLQ as-is.")
                send_to_dlq(dlq_producer, {}, e, 0, msg)
                consumer.commit(msg)
                failed += 1
                continue

            success, retries_used, error = process_with_retry(order, aggregator)

            if success:
                processed += 1
                if processed % 5 == 0:
                    log.info("Aggregation snapshot:\n" + aggregator.report())
            else:
                send_to_dlq(dlq_producer, order, error, retries_used, msg)
                failed += 1

            # Commit only after the message has either been processed
            # successfully or safely routed to the DLQ -- never lose it silently.
            consumer.commit(msg)

    except KeyboardInterrupt:
        log.info("Interrupted by user.")
    finally:
        log.info(f"Final aggregation report:\n{aggregator.report()}")
        log.info(f"Processed OK: {processed} | Sent to DLQ: {failed}")
        dlq_producer.flush()
        consumer.close()


if __name__ == "__main__":
    run()
