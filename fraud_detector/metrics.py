from collections import deque
from datetime import datetime

from prometheus_client import Counter, Gauge, Histogram, start_http_server

SCORE_BUCKETS = [round(i / 20, 2) for i in range(1, 21)]
PROCESSING_BUCKETS = [0.001, 0.0025, 0.005, 0.0075, 0.01, 0.015, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
DELAY_BUCKETS = [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0]

TRANSACTIONS = Counter(
    "fraud_scorer_transactions_total",
    "Transactions scored by the service",
    ["model_version"],
)
FLAGGED = Counter(
    "fraud_scorer_flagged_total",
    "Transactions flagged as fraud",
    ["model_version"],
)
FLAGGED_AMOUNT = Counter(
    "fraud_scorer_flagged_amount_dollars_total",
    "Sum of amounts of transactions flagged as fraud, dollars",
)
ERRORS = Counter(
    "fraud_scorer_errors_total",
    "Messages that could not be scored",
    ["stage"],
)
PROCESSING_SECONDS = Histogram(
    "fraud_scorer_processing_seconds",
    "Time to score one transaction: preprocessing, inference and sending the result",
    buckets=PROCESSING_BUCKETS,
)
END_TO_END_SECONDS = Histogram(
    "fraud_scorer_end_to_end_seconds",
    "Time from the moment the simulator sent a transaction to the moment its score was sent",
    buckets=DELAY_BUCKETS,
)
SCORE = Histogram(
    "fraud_scorer_score",
    "Distribution of fraud scores",
    buckets=SCORE_BUCKETS,
)
FRAUD_RATIO = Gauge(
    "fraud_scorer_fraud_ratio",
    "Share of flagged transactions among the last scored ones",
)
MODEL_SWITCHES = Counter(
    "fraud_scorer_model_switches_total",
    "Hot reloads of the model without restarting the container, labelled with the new version",
    ["version"],
)
MODEL_INFO = Gauge(
    "fraud_scorer_model_info",
    "Model that is currently used for scoring, value is always 1",
    ["version"],
)
MODEL_THRESHOLD = Gauge(
    "fraud_scorer_model_threshold",
    "Decision threshold of the current model",
)
MODEL_TRAINED_AT = Gauge(
    "fraud_scorer_model_trained_timestamp_seconds",
    "Unix time when the current model was trained",
)


class ScoringMetrics:
    def __init__(self, port, window=1000):
        self.recent_flags = deque(maxlen=window)
        self.version = None
        start_http_server(port)

    def model_loaded(self, bundle):
        if bundle.version == self.version:
            return False
        switched = self.version is not None
        if switched:
            MODEL_SWITCHES.labels(version=bundle.version).inc()
            MODEL_INFO.remove(self.version)
        MODEL_INFO.labels(version=bundle.version).set(1)
        MODEL_THRESHOLD.set(bundle.threshold)
        trained_at = bundle.meta.get("trained_at")
        if trained_at:
            MODEL_TRAINED_AT.set(datetime.fromisoformat(trained_at).timestamp())
        TRANSACTIONS.labels(model_version=bundle.version)
        FLAGGED.labels(model_version=bundle.version)
        self.version = bundle.version
        return switched

    def scored(self, score, flag, amount, seconds, sent_at, now):
        TRANSACTIONS.labels(model_version=self.version).inc()
        SCORE.observe(score)
        PROCESSING_SECONDS.observe(seconds)
        if flag:
            FLAGGED.labels(model_version=self.version).inc()
            if amount is not None:
                FLAGGED_AMOUNT.inc(amount)
        if sent_at is not None:
            END_TO_END_SECONDS.observe(max(now - sent_at, 0.0))
        self.recent_flags.append(flag)
        FRAUD_RATIO.set(sum(self.recent_flags) / len(self.recent_flags))

    @staticmethod
    def failed(stage):
        ERRORS.labels(stage=stage).inc()
