import logging
import os
import signal
import sys
import time

import pandas as pd

from kafka_io import ScoreWriter, TransactionReader, wait_for_topics
from metrics import ScoringMetrics
from registry import ModelRegistry

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("fraud_detector")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORES_TOPIC = os.getenv("KAFKA_SCORES_TOPIC", "scores")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "fraud-detector")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
POLL_TIMEOUT = float(os.getenv("POLL_TIMEOUT", "0.2"))
MODELS_DIR = os.getenv("MODELS_DIR", "/models")
INITIAL_MODEL_DIR = os.getenv("INITIAL_MODEL_DIR", "/app/initial_model")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
FRAUD_RATIO_WINDOW = int(os.getenv("FRAUD_RATIO_WINDOW", "1000"))
REPORT_EVERY_SECONDS = 30


def to_amount(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class ScoringService:
    def __init__(self):
        self.metrics = ScoringMetrics(METRICS_PORT, FRAUD_RATIO_WINDOW)
        logger.info("Prometheus metrics on port %d", METRICS_PORT)

        self.registry = ModelRegistry(MODELS_DIR, INITIAL_MODEL_DIR)
        self.registry.ensure_initial()
        bundle = self.registry.refresh()
        if bundle is None:
            raise RuntimeError(f"No model in {MODELS_DIR}")
        self.metrics.model_loaded(bundle)

        wait_for_topics(KAFKA_BOOTSTRAP_SERVERS, [TRANSACTIONS_TOPIC, SCORES_TOPIC])
        self.reader = TransactionReader(KAFKA_BOOTSTRAP_SERVERS, TRANSACTIONS_TOPIC, CONSUMER_GROUP)
        self.writer = ScoreWriter(KAFKA_BOOTSTRAP_SERVERS, SCORES_TOPIC)
        self.running = True

    def stop(self, *_):
        self.running = False

    def current_bundle(self):
        bundle = self.registry.refresh()
        if self.metrics.model_loaded(bundle):
            logger.info("Model switch registered in metrics: %s", bundle.version)
        return bundle

    def score_one(self, bundle, transaction):
        started = time.perf_counter()
        try:
            scores, flags = bundle.predict(pd.DataFrame([transaction.row]))
        except Exception as e:
            self.metrics.failed("predict")
            logger.error("Skipping transaction %s: %s", transaction.transaction_id, e)
            return None
        score, flag = float(scores[0]), int(flags[0])
        self.writer.write(transaction, score, flag, bundle.version)
        self.metrics.scored(
            score=score,
            flag=flag,
            amount=to_amount(transaction.row.get("amount")),
            seconds=time.perf_counter() - started,
            sent_at=transaction.sent_at,
            now=time.time(),
        )
        return flag

    def run(self):
        logger.info("Consuming '%s' -> producing '%s'", TRANSACTIONS_TOPIC, SCORES_TOPIC)
        scored = flagged = 0
        last_report = time.monotonic()
        try:
            while self.running:
                bundle = self.current_bundle()
                batch = self.reader.read_batch(
                    max_messages=BATCH_SIZE,
                    timeout=POLL_TIMEOUT,
                    on_malformed=lambda: self.metrics.failed("parse"),
                )
                for transaction in batch:
                    flag = self.score_one(bundle, transaction)
                    if flag is not None:
                        scored += 1
                        flagged += flag
                if batch:
                    self.writer.flush()
                self.reader.commit()

                if time.monotonic() - last_report >= REPORT_EVERY_SECONDS:
                    logger.info("Scored %d transactions with model %s, fraud flagged: %d", scored, bundle.version, flagged)
                    scored = flagged = 0
                    last_report = time.monotonic()
        finally:
            self.reader.close()
            self.writer.flush()


if __name__ == "__main__":
    service = ScoringService()
    signal.signal(signal.SIGTERM, service.stop)
    signal.signal(signal.SIGINT, service.stop)
    service.run()
