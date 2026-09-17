"""
Shared configuration for the order-processing pipeline.
Centralising this avoids magic strings being duplicated across
producer.py, consumer.py and dlq_monitor.py.
"""

import os

# --- Kafka / Schema Registry connection -------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# --- Topics -------------------------------------------------------------------------------
ORDERS_TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"

# --- Consumer group -------------------------------------------------------------------------------
CONSUMER_GROUP_ID = "order-aggregator-group"

# --- Retry policy ---------------------------------------------------------------------------------
MAX_RETRIES = 3          # number of retry attempts for a transient failure before going to DLQ
INITIAL_BACKOFF_SECONDS = 1.0   # backoff before the first retry
BACKOFF_MULTIPLIER = 2.0        # exponential backoff factor (1s, 2s, 4s, ...)

# --- Simulated failure rates (purely for demonstrating retry/DLQ behaviour) ----------------------
# On each message, the consumer randomly injects failures so the retry
# logic and DLQ path can be observed live without needing a real broken
# downstream dependency.
TRANSIENT_FAILURE_PROBABILITY = 0.25   # e.g. simulates a flaky downstream DB/API timeout
PERMANENT_FAILURE_PROBABILITY = 0.05   # e.g. simulates a poison-pill / malformed business rule
