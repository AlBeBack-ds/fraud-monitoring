import json
import logging
import time
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

logger = logging.getLogger(__name__)

CONTEXT_FIELDS = ("us_state", "merch", "cat_id", "amount")


@dataclass
class Transaction:
    transaction_id: str
    row: dict
    sent_at: float | None


def wait_for_topics(bootstrap_servers, topics, timeout=180):
    admin = Producer({"bootstrap.servers": bootstrap_servers})
    deadline = time.time() + timeout
    while True:
        try:
            existing = admin.list_topics(timeout=5).topics
            missing = [t for t in topics if t not in existing]
            if not missing:
                return
            logger.info("Waiting for topics: %s", missing)
        except KafkaException as e:
            logger.info("Waiting for Kafka: %s", e)
        if time.time() > deadline:
            raise RuntimeError(f"Kafka topics {topics} are not available")
        time.sleep(3)


def parse_transaction(msg):
    payload = json.loads(msg.value().decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("message must be a JSON object")

    if isinstance(payload.get("data"), dict):
        row = dict(payload["data"])
    else:
        row = {k: v for k, v in payload.items() if k not in ("transaction_id", "sent_at")}

    transaction_id = payload.get("transaction_id") or f"{msg.topic()}-{msg.partition()}-{msg.offset()}"
    sent_at = payload.get("sent_at")
    return Transaction(str(transaction_id), row, float(sent_at) if sent_at is not None else None)


class TransactionReader:
    def __init__(self, bootstrap_servers, topic, group_id):
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self.consumer.subscribe([topic])

    def read_batch(self, max_messages=500, timeout=1.0, on_malformed=None):
        batch = []
        for msg in self.consumer.consume(num_messages=max_messages, timeout=timeout):
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka error: %s", msg.error())
                continue
            try:
                batch.append(parse_transaction(msg))
            except Exception as e:
                logger.error("Skipping malformed message at %s[%d]@%d: %s",
                             msg.topic(), msg.partition(), msg.offset(), e)
                if on_malformed is not None:
                    on_malformed()
        return batch

    def commit(self):
        try:
            self.consumer.commit(asynchronous=False)
        except KafkaException as e:
            if e.args[0].code() != KafkaError._NO_OFFSET:
                raise

    def close(self):
        self.consumer.close()


class ScoreWriter:
    def __init__(self, bootstrap_servers, topic):
        self.topic = topic
        self.producer = Producer({"bootstrap.servers": bootstrap_servers, "linger.ms": 20, "acks": "all"})

    @staticmethod
    def _on_delivery(err, msg):
        if err is not None:
            logger.error("Failed to deliver score for %s: %s", msg.key(), err)

    def write(self, transaction, score, fraud_flag, model_version):
        transaction_id = transaction.transaction_id
        value = json.dumps({
            "transaction_id": transaction_id,
            "score": float(score),
            "fraud_flag": int(fraud_flag),
            "model_version": model_version,
            **{field: transaction.row.get(field) for field in CONTEXT_FIELDS},
        }, ensure_ascii=False)
        while True:
            try:
                self.producer.produce(self.topic, key=transaction_id, value=value, on_delivery=self._on_delivery)
                break
            except BufferError:
                self.producer.poll(1)
        self.producer.poll(0)

    def flush(self):
        remaining = self.producer.flush(30)
        if remaining:
            raise RuntimeError(f"{remaining} score messages were not delivered")
