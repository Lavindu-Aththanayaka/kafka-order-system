# Live Demo Guide

## 1. Start the infrastructure

From the project root, run:

```bash
docker compose up -d
docker compose ps
```

Confirm that Zookeeper, Kafka, Schema Registry, and Kafka UI are running. Open
Kafka UI at [http://localhost:8080](http://localhost:8080).

Do not open or display `.env` during the recording because it contains a
credential.

## 2. Run the application

Open three terminals in the project root.

Terminal 1, start the consumer:

```bash
.venv/bin/python consumer.py
```

Terminal 2, start the dead-letter queue monitor:

```bash
.venv/bin/python dlq_monitor.py
```

Terminal 3, publish a batch of orders:

```bash
.venv/bin/python producer.py --count 20 --interval 0.3
```

The consumer calculates running averages by product and overall. It retries
transient failures with exponential backoff and sends permanent or exhausted
failures to the `orders-dlq` topic. Offsets are committed only after successful
processing or safe DLQ delivery.

## 3. Suggested recording order

1. Show the public GitHub repository and project files.
2. Show `docker compose ps` with all four services running.
3. Open Kafka UI and show the `orders` topic.
4. Show the consumer waiting for messages.
5. Show the DLQ monitor waiting for failed messages.
6. Start the producer and show delivered orders.
7. Show aggregation snapshots in the consumer terminal.
8. Show retry warnings and DLQ messages when they occur.

## 4. Expected output

The consumer should display output similar to:

```text
Listening on topic 'orders'...
Aggregation snapshot:
Overall running average: 245.37 (n=5)
```

The exact values differ because orders are randomly generated. Retry output may
look like:

```text
Transient failure for order 12345678 (attempt 1/3)
```

The producer should finish with a message similar to:

```text
Flushing producer, 20 messages sent this run...
Done.
```

If a message enters the DLQ, the monitor displays its order details, error type,
retry count, source topic, partition, and offset. If no message fails during a
short run, the monitor may remain quiet.

## 5. Make failures visible

For a demonstration with more retry and DLQ output, temporarily change the
failure probabilities in `config.py`:

```python
TRANSIENT_FAILURE_PROBABILITY = 0.40
PERMANENT_FAILURE_PROBABILITY = 0.15
```

Restart the consumer, then run:

```bash
.venv/bin/python producer.py --count 30 --interval 0.2
```

Restore the original values after recording and do not commit temporary demo
settings unless they are intended for the project.

## 6. Cleanup

Stop the Python processes with `Ctrl+C`. Stop the Docker stack with:

```bash
docker compose down
```

To also remove local Kafka data, use:

```bash
docker compose down -v
```