# Live Demo Guide

## 1. Prepare the project

Open a terminal and run:

```bash
cd "/Users/lavindu/Downloads/files (5)"
docker compose up -d
docker compose ps
```

Confirm that Zookeeper, Kafka, Schema Registry, and Kafka UI are running.

Open Kafka UI in a browser:

<http://localhost:8080>

Do not open or display `.env` during the recording. It contains credentials.

## 2. Open three terminals

Run the consumer in Terminal 1:

```bash
cd "/Users/lavindu/Downloads/files (5)"
.venv/bin/python consumer.py
```

Run the dead-letter queue monitor in Terminal 2:

```bash
cd "/Users/lavindu/Downloads/files (5)"
.venv/bin/python dlq_monitor.py
```

Run the producer in Terminal 3:

```bash
cd "/Users/lavindu/Downloads/files (5)"
.venv/bin/python producer.py --count 20 --interval 0.3
```

Start screen recording after the consumer and DLQ monitor are running.

## 3. Recommended recording order

1. Show the GitHub repository and project files.
2. Show `docker-compose.yml` briefly.
3. Show the Docker services running with `docker compose ps`.
4. Open Kafka UI at <http://localhost:8080>.
5. Show the consumer waiting for messages.
6. Start the producer.
7. Show delivered orders in the producer terminal.
8. Show aggregation results in the consumer terminal.
9. Show retry warnings and DLQ messages when they occur.
10. Stop the producer after the batch finishes.

## 4. What should appear on screen

### Docker terminal

After `docker compose ps`, each service should show a running status:

```text
zookeeper          Up
kafka              Up
schema-registry   Up
kafka-ui           Up
```

The exact spacing and creation times may differ. The important part is that all four services are running.

### Terminal 1: consumer

Before messages arrive, the consumer should show something similar to:

```text
Listening on topic 'orders'...
```

After the producer starts, it should show aggregation snapshots similar to:

```text
Aggregation snapshot:
Overall running average: 245.37  (n=5)
	Item1     : avg=210.42  n=2
	Item3     : avg=280.32  n=1
```

The numbers and products will be different because orders are randomly generated. You may also see retry warnings such as:

```text
Transient failure for order 12345678 (attempt 1/3) ... Retrying in 1.0s...
```

At shutdown, the consumer should display a final report containing `Processed OK` and `Sent to DLQ` counts. Pressing `Ctrl+C` after the demo is complete is normal.

### Terminal 2: DLQ monitor

The DLQ monitor should remain open and wait for failed messages. If a message is sent to the dead-letter queue, it should display information similar to:

```text
DLQ message received:
orderId: 12345678
errorType: PermanentProcessingError
retryCount: 0
sourceTopic: orders
```

If no message fails during the normal run, the monitor may remain quiet. That is expected; increase the failure probabilities temporarily by following Section 6.

### Terminal 3: producer

The producer should show messages similar to:

```text
Starting producer. Target topic: 'orders'
Flushing producer, 20 messages sent this run...
Delivered order -> topic=orders partition=0 offset=0
Done.
```

The partition, offsets, and order count may differ. After the configured batch is sent, the producer exits normally with status code `0`. This is expected and confirms that the batch completed.

### Kafka UI

In Kafka UI, show the `orders` topic and, when failures occur, the `orders-dlq` topic. The `orders` topic should contain produced order messages. The DLQ topic should contain failed messages together with their error metadata.

## 5. Presentation script

### Introduction

> This project is a Kafka-based order processing system implemented in Python. It uses Avro serialization and Confluent Schema Registry to define and validate the message structure.

> The system contains an order producer, an order consumer, and a dead-letter queue monitor. Docker Compose runs Kafka, Zookeeper, Schema Registry, and Kafka UI locally.

### Architecture

> The producer generates simulated e-commerce orders containing an order ID, product, and price. These messages are serialized with Avro and published to the `orders` Kafka topic.

> The consumer reads messages from the topic and maintains a running average for each product and for all products combined.

> Kafka UI provides a visual way to inspect topics, messages, and consumer activity.

### Producer demonstration

After running the producer:

> I am now publishing 20 order events. Each event is sent to Kafka with an Avro-encoded value and a string order ID as the key.

### Aggregation demonstration

When the consumer displays an aggregation snapshot:

> The consumer has processed the orders and calculated a running average for each product. It stores only a count and a sum, so it does not need to keep the complete message history in memory.

### Retry demonstration

When a retry warning appears:

> This order experienced a simulated transient failure. The consumer retries the operation using exponential backoff instead of immediately discarding the message.

> The retry delay increases between attempts, giving a temporary downstream problem time to recover.

### Dead-letter queue demonstration

When a DLQ message appears:

> This order could not be processed successfully. It was routed to the `orders-dlq` topic with error metadata, including the error type, retry count, original topic, partition, and offset.

> This prevents failed messages from blocking the main consumer while still preserving them for inspection or later reprocessing.

### Offset management

> The consumer disables automatic offset commits. It commits an offset only after the message is processed successfully or safely sent to the dead-letter queue. This prevents messages from being acknowledged before they are handled.

### Closing

> This demo shows reliable Kafka message processing with Avro schemas, Schema Registry, retry handling, running aggregation, manual offset management, and a dead-letter queue.

## 6. Scene-by-scene recording script

### Scene 1: Introduction

**Show:** The GitHub repository and the project files.

**Say:**

> This is a Kafka-based order processing system built with Python. It uses Kafka for messaging, Avro for data serialization, and Confluent Schema Registry for message validation.

> The project includes a producer, consumer, dead-letter queue monitor, Docker Compose configuration, and Avro schemas.

### Scene 2: Docker infrastructure

**Show:** A terminal running `docker compose ps`.

**Say:**

> Docker Compose runs the complete local infrastructure. This includes Zookeeper, Kafka, Schema Registry, and Kafka UI. All four services are currently running successfully.

### Scene 3: Kafka UI

**Show:** <http://localhost:8080>, then the `orders` topic and later the `orders-dlq` topic.

**Say:**

> Kafka UI allows us to inspect the Kafka cluster, topics, messages, and consumer activity through a browser. The main topic is `orders`, and failed messages are sent to `orders-dlq`.

### Scene 4: Consumer

**Show:** Terminal 1 running:

```bash
.venv/bin/python consumer.py
```

When `Listening on topic 'orders'...` appears, say:

> This is the consumer. It is subscribed to the `orders` topic and is waiting for order messages. It also maintains a running average price for each product and for all products combined.

### Scene 5: DLQ monitor

**Show:** Terminal 2 running:

```bash
.venv/bin/python dlq_monitor.py
```

**Say:**

> This terminal monitors the dead-letter queue. If an order cannot be processed after retries, the consumer sends it to the `orders-dlq` topic, where it can be inspected here.

If the terminal is quiet, say:

> The DLQ monitor is running and waiting for failed messages.

### Scene 6: Producer

**Show:** Terminal 3 running:

```bash
.venv/bin/python producer.py --count 20 --interval 0.3
```

**Say:**

> I am now starting the producer and sending 20 simulated orders to Kafka. Each order contains an order ID, product, and price. The messages are serialized using Avro before being published to the `orders` topic.

When a delivery message appears, say:

> Kafka has accepted the order and assigned it to a partition and offset.

When `Done.` appears, say:

> The producer has completed its batch successfully. The producer exiting after sending all messages is expected behavior.

### Scene 7: Aggregation

**Show:** Terminal 1 with an aggregation snapshot.

**Say:**

> The consumer is now processing the orders and calculating running averages. It stores only the total count and total price for each product, so it does not need to store every order.

The exact numbers will be different because the producer generates random orders.

### Scene 8: Retry handling

**Show:** A consumer warning containing `Transient failure` and `Retrying`.

**Say:**

> This order experienced a simulated transient failure. Instead of losing the message, the consumer retries it using exponential backoff. The delay increases between attempts to avoid repeatedly overwhelming a failing service.

### Scene 9: Dead-letter queue

**Show:** Terminal 2 and the `orders-dlq` topic in Kafka UI.

**Say:**

> This message could not be processed successfully, so it was sent to the dead-letter queue. The DLQ record contains the original order details, error information, retry count, source topic, partition, and offset.

> This allows the failed message to be investigated or reprocessed later without blocking the main Kafka pipeline.

If no DLQ message appears, say:

> No messages failed permanently during this run, so the DLQ monitor remains ready but has no output. The failure behavior can be demonstrated by temporarily increasing the failure probabilities in `config.py`.

### Scene 10: Offset commits

**Show:** The consumer code around `enable.auto.commit` and `consumer.commit(msg)`.

**Say:**

> Automatic offset commits are disabled. The consumer commits the Kafka offset only after successful processing or after safely sending the message to the DLQ. This prevents Kafka from marking a message as complete before the system has handled it.

### Scene 11: Closing

**Show:** The three terminals and Kafka UI together.

**Say:**

> This demonstration showed a complete Kafka order processing pipeline with Avro serialization, Schema Registry, running aggregation, retry handling, manual offset commits, and dead-letter queue processing.

> The Docker-based setup makes the entire system easy to run locally and inspect through Kafka UI.

## 7. Make failures visible

Failures are random, so a short run may not show a retry or DLQ message. For a stronger demonstration, temporarily change these values in `config.py`:

```python
TRANSIENT_FAILURE_PROBABILITY = 0.40
PERMANENT_FAILURE_PROBABILITY = 0.15
```

Restart the consumer after changing the configuration, then run:

```bash
.venv/bin/python producer.py --count 30 --interval 0.2
```

Restore the original values after recording:

```python
TRANSIENT_FAILURE_PROBABILITY = 0.25
PERMANENT_FAILURE_PROBABILITY = 0.05
```

Do not commit temporary demo-only configuration changes unless they are intended for the project.

## 8. Cleanup after recording

Stop the Python processes with `Ctrl+C`. Keep Docker running if you want to inspect Kafka UI afterward. To stop the Docker stack completely, run:

```bash
docker compose down
```

To stop the stack and remove its local Kafka data as well:

```bash
docker compose down -v
```
