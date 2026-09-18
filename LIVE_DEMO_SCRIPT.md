# Kafka Order Processing Live Demo

## Before Recording

Keep these ready:

- GitHub repository: https://github.com/Lavindu-Aththanayaka/kafka-order-system
- The project folder open in VS Code
- Three or four terminal windows
- A browser open to http://localhost:8080

Do not open or display `.env` during the recording because it contains a
credential.

## Scene 1: Introduce the Project

**Show:** The GitHub repository and the project files.

**Say:**

> This is a Kafka-based order processing system implemented in Python. It uses
> Avro serialization, Confluent Schema Registry, retry handling, running-average
> aggregation, and a dead-letter queue.
>
> The main components are the order producer, order consumer, DLQ monitor,
> Docker Compose configuration, and Avro schemas.

Briefly show:

- `producer.py`
- `consumer.py`
- `dlq_monitor.py`
- `config.py`
- `docker-compose.yml`
- `schemas/order.avsc`
- `schemas/order_dlq.avsc`

## Scene 2: Start Docker Services

**Show:** A terminal in the project root.

```bash
docker compose up -d
docker compose ps
```

**Say:**

> Docker Compose starts Zookeeper, Kafka, Schema Registry, and Kafka UI. These
> services provide the messaging infrastructure for the application.

Show all four services with an `Up` or `Running` status.

## Scene 3: Open Kafka UI

**Show:** http://localhost:8080

**Say:**

> Kafka UI allows us to inspect the Kafka cluster, topics, messages, and consumer
> activity through a browser.

Show the Kafka cluster and the `orders` topic. Later, show `orders-dlq` if a
failed message appears.

## Scene 4: Start the Consumer

**Show:** A second terminal.

```bash
.venv/bin/python consumer.py
```

Wait for:

```text
Listening on topic 'orders'...
```

**Say:**

> This is the consumer. It subscribes to the `orders` topic and waits for
> incoming order messages.
>
> For every successful order, it updates a running average for each product and
> for all products combined. It stores only the count and total price instead of
> keeping every order in memory.

## Scene 5: Start the DLQ Monitor

**Show:** A third terminal.

```bash
.venv/bin/python dlq_monitor.py
```

Wait for:

```text
Watching DLQ topic 'orders-dlq'...
```

**Say:**

> This process monitors the dead-letter queue. If an order cannot be processed
> successfully, the consumer sends it to the `orders-dlq` topic with error
> details.

## Scene 6: Produce Orders

**Show:** A fourth terminal.

```bash
.venv/bin/python producer.py --count 20 --interval 0.3
```

**Say:**

> I am now producing 20 simulated orders. Each order contains an order ID,
> product, and price.
>
> The producer serializes each order using Avro and publishes it to the `orders`
> Kafka topic.

Show output similar to:

```text
Starting producer. Target topic: 'orders'
Delivered order -> topic=orders partition=0 offset=...
Flushing producer, 20 messages sent this run...
Done.
```

**Say:**

> Kafka has accepted these messages and assigned them to a partition and offset.

## Scene 7: Show Aggregation

**Show:** The consumer terminal after messages arrive.

Example output:

```text
Aggregation snapshot:
Overall running average: 319.04 (n=10)
Item1 : avg=265.53 n=2
Item3 : avg=300.69 n=3
```

**Say:**

> The consumer has processed the orders and calculated running averages.
>
> The average is maintained using a count and a sum, which keeps the aggregation
> efficient even when many messages are processed.

## Scene 8: Show Retry Handling

If this appears in the consumer terminal:

```text
Transient failure for order ...
Retrying in 1.0s...
```

**Say:**

> This order experienced a simulated transient failure. The consumer does not
> immediately discard it. Instead, it retries the operation using exponential
> backoff.
>
> The retry delay increases between attempts, giving a temporary downstream
> problem time to recover.

## Scene 9: Show the Dead-Letter Queue

**Show:** The DLQ monitor terminal and the `orders-dlq` topic in Kafka UI.

**Say:**

> This message could not be processed successfully, so it was sent to the
> dead-letter queue.
>
> The DLQ record preserves the original order and includes error metadata such as
> the error type, retry count, source topic, partition, and offset.
>
> This prevents a failed message from blocking the main processing pipeline while
> preserving it for investigation or reprocessing.

If no DLQ message appears, say:

> No message failed permanently during this run. The DLQ monitor is still active
> and ready to receive failed messages.

## Scene 10: Optional Failure Demonstration

To make retry and DLQ output more visible, stop the consumer with `Ctrl+C` and
temporarily change these values in `config.py`:

```python
TRANSIENT_FAILURE_PROBABILITY = 0.40
PERMANENT_FAILURE_PROBABILITY = 0.15
```

Restart the consumer:

```bash
.venv/bin/python consumer.py
```

Then run:

```bash
.venv/bin/python producer.py --count 30 --interval 0.2
```

**Say:**

> I increased the simulated failure probabilities so the retry and dead-letter
> queue behavior is easier to observe during this demonstration.

Restore the original values after recording. Do not commit temporary demo
settings unless they are intended for the project.

## Scene 11: Explain Offset Management

**Show:** The consumer code containing `enable.auto.commit` and
`consumer.commit(msg)`.

**Say:**

> Automatic offset commits are disabled.
>
> The consumer commits the Kafka offset only after the message has been
> processed successfully or safely sent to the DLQ.
>
> This prevents Kafka from marking a message as complete before the application
> has handled it.

## Scene 12: Closing Summary

**Show:** Kafka UI, consumer output, producer output, and DLQ monitor.

**Say:**

> This demonstration showed a complete Kafka order processing pipeline.
>
> The producer publishes Avro-encoded order events. The consumer processes them
> and calculates running averages. Transient failures are retried with
> exponential backoff, while permanent failures are routed to a dead-letter
> queue.
>
> Docker Compose provides Kafka, Zookeeper, Schema Registry, and Kafka UI. The
> system also uses manual offset commits to prevent messages from being
> acknowledged before they are safely handled.

## Cleanup

Stop the Python programs with `Ctrl+C`.

To stop the Docker stack:

```bash
docker compose down
```

To also remove local Kafka data:

```bash
docker compose down -v
```