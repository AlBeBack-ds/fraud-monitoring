import json
import logging
import os
import random
import sys
import time
import uuid

import pandas as pd
from confluent_kafka import KafkaException, Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("simulator")

CSV_PATH = os.getenv("CSV_PATH", "/data/train.csv")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
MESSAGES_PER_SECOND = float(os.getenv("MESSAGES_PER_SECOND", "10"))
SAMPLE_FRACTION = float(os.getenv("SAMPLE_FRACTION", "0.05"))
FRAUD_SHARE = float(os.getenv("FRAUD_SHARE", "0.006"))


def to_records(frame):
    frame = frame.drop(columns=["target"], errors="ignore")
    return frame.astype(object).where(frame.notna(), None).to_dict(orient="records")


def load_sample():
    regular, fraud = [], []
    for i, chunk in enumerate(pd.read_csv(CSV_PATH, chunksize=100_000)):
        is_fraud = chunk["target"] == 1
        regular.append(chunk[~is_fraud].sample(frac=SAMPLE_FRACTION, random_state=i))
        fraud.append(chunk[is_fraud])
    regular, fraud = pd.concat(regular), pd.concat(fraud)
    logger.info("Loaded %d regular and %d fraud transactions from %s", len(regular), len(fraud), CSV_PATH)
    return to_records(regular), to_records(fraud)


def wait_for_kafka(producer):
    for attempt in range(1, 61):
        try:
            if TOPIC in producer.list_topics(timeout=5).topics:
                return
        except KafkaException as e:
            logger.info("Waiting for Kafka (%d/60): %s", attempt, e)
        time.sleep(3)
    raise RuntimeError(f"Topic {TOPIC} is not available")


def main():
    regular, fraud = load_sample()
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "linger.ms": 50})
    wait_for_kafka(producer)
    logger.info("Sending %.1f messages per second to '%s', fraud share %.3f", MESSAGES_PER_SECOND, TOPIC, FRAUD_SHARE)

    pause = 1.0 / MESSAGES_PER_SECOND
    sent = 0
    while True:
        transaction_id = str(uuid.uuid4())
        row = random.choice(fraud if random.random() < FRAUD_SHARE else regular)
        message = {"transaction_id": transaction_id, "sent_at": time.time(), "data": row}
        producer.produce(TOPIC, key=transaction_id, value=json.dumps(message, ensure_ascii=False))
        producer.poll(0)
        sent += 1
        if sent % 1000 == 0:
            producer.flush()
            logger.info("Sent %d messages", sent)
        time.sleep(pause)


if __name__ == "__main__":
    main()
