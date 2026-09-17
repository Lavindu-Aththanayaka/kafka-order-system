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

## 4. Presentation script

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

## 5. Make failures visible

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

## 6. Cleanup after recording

Stop the Python processes with `Ctrl+C`. Keep Docker running if you want to inspect Kafka UI afterward. To stop the Docker stack completely, run:

```bash
docker compose down
```

To stop the stack and remove its local Kafka data as well:

```bash
docker compose down -v
```
