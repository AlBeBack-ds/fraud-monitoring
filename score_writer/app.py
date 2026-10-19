import json
import logging
import os
import signal
import sys
import time

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from psycopg2.extras import execute_values

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("score_writer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCORES_TOPIC = os.getenv("KAFKA_SCORES_TOPIC", "scores")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "score-writer")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "fraud"),
    "user": os.getenv("POSTGRES_USER", "fraud"),
    "password": os.getenv("POSTGRES_PASSWORD", "fraud"),
}

INSERT_SQL = """
    INSERT INTO fraud_scores (transaction_id, score, fraud_flag, model_version, us_state, merch, cat_id, amount)
    VALUES %s
    ON CONFLICT (transaction_id) DO NOTHING
"""

SCORE_BUCKETS = [round(i / 20, 2) for i in range(1, 21)]
WRITE_BUCKETS = [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]

WRITTEN = Counter(
    "score_writer_scores_written_total",
    "Scores written to PostgreSQL",
    ["model_version"],
)
FRAUD_WRITTEN = Counter(
    "score_writer_fraud_written_total",
    "Written scores with fraud_flag = 1",
)
DUPLICATES = Counter(
    "score_writer_duplicates_total",
    "Scores skipped because the transaction was already in the table",
)
MALFORMED = Counter(
    "score_writer_malformed_total",
    "Messages from the scores topic that could not be parsed",
)
WRITE_SECONDS = Histogram(
    "score_writer_write_seconds",
    "Time to insert a batch of scores and commit the transaction",
    buckets=WRITE_BUCKETS,
)
BATCH_ROWS = Histogram(
    "score_writer_batch_rows",
    "Number of scores in one database write",
    buckets=[1, 2, 5, 10, 20, 50, 100, 200, 500],
)
SCORE = Histogram(
    "score_writer_score",
    "Distribution of written fraud scores",
    buckets=SCORE_BUCKETS,
)
LAST_WRITE = Gauge(
    "score_writer_last_write_timestamp_seconds",
    "Unix time of the last successful write",
)

running = True


def stop(*_):
    global running
    running = False


def connect_db(retries=30):
    for attempt in range(1, retries + 1):
        try:
            return psycopg2.connect(**DB_CONFIG)
        except psycopg2.OperationalError as e:
            logger.info("Waiting for Postgres (%d/%d): %s", attempt, retries, e)
            time.sleep(2)
    raise RuntimeError("Postgres is not available")


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_score(msg):
    payload = json.loads(msg.value().decode("utf-8"))
    fraud_flag = int(payload["fraud_flag"])
    if fraud_flag not in (0, 1):
        raise ValueError(f"bad fraud_flag {fraud_flag}")
    return (
        str(payload["transaction_id"]),
        float(payload["score"]),
        fraud_flag,
        payload.get("model_version"),
        payload.get("us_state"),
        payload.get("merch"),
        payload.get("cat_id"),
        to_float(payload.get("amount")),
    )


def write_rows(conn, rows):
    started = time.perf_counter()
    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, rows, page_size=len(rows))
        inserted = cur.rowcount
    conn.commit()
    WRITE_SECONDS.observe(time.perf_counter() - started)
    BATCH_ROWS.observe(len(rows))
    LAST_WRITE.set_to_current_time()

    for _, score, fraud_flag, model_version, *_ in rows:
        WRITTEN.labels(model_version=model_version or "unknown").inc()
        SCORE.observe(score)
        if fraud_flag:
            FRAUD_WRITTEN.inc()
    DUPLICATES.inc(len(rows) - inserted)


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    start_http_server(METRICS_PORT)
    logger.info("Prometheus metrics on port %d", METRICS_PORT)

    conn = connect_db()
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([SCORES_TOPIC])
    logger.info("Consuming '%s' -> table fraud_scores", SCORES_TOPIC)

    saved = 0
    last_report = time.monotonic()
    try:
        while running:
            rows = []
            for msg in consumer.consume(num_messages=BATCH_SIZE, timeout=1.0):
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("Kafka error: %s", msg.error())
                    continue
                try:
                    rows.append(parse_score(msg))
                except Exception as e:
                    MALFORMED.inc()
                    logger.error("Skipping malformed message at %s[%d]@%d: %s",
                                 msg.topic(), msg.partition(), msg.offset(), e)
            if rows:
                write_rows(conn, rows)
                saved += len(rows)
            try:
                consumer.commit(asynchronous=False)
            except KafkaException as e:
                if e.args[0].code() != KafkaError._NO_OFFSET:
                    raise
            if time.monotonic() - last_report >= 30:
                logger.info("Saved %d scores in the last 30 seconds", saved)
                saved = 0
                last_report = time.monotonic()
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
