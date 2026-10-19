import logging
import os
import sys
import time
from pathlib import Path

import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("data_loader")

CSV_PATH = Path(os.getenv("CSV_PATH", "/data/train.csv"))
FIRST_TEST_MONTH = os.getenv("FIRST_TEST_MONTH", "2019-05-01")

COLUMNS = [
    "transaction_time", "merch", "cat_id", "amount", "name_1", "name_2", "gender", "street",
    "one_city", "us_state", "post_code", "lat", "lon", "population_city", "jobs",
    "merchant_lat", "merchant_lon", "target",
]


def connect():
    for attempt in range(1, 31):
        try:
            return psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                dbname=os.getenv("POSTGRES_DB", "fraud"),
                user=os.getenv("POSTGRES_USER", "fraud"),
                password=os.getenv("POSTGRES_PASSWORD", "fraud"),
            )
        except psycopg2.OperationalError as e:
            logger.info("Waiting for Postgres (%d/30): %s", attempt, e)
            time.sleep(2)
    raise RuntimeError("Postgres is not available")


def load_csv(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM labeled_transactions")
        if cur.fetchone()[0]:
            logger.info("labeled_transactions is already filled, skipping CSV load")
            return

        if not CSV_PATH.exists():
            raise FileNotFoundError(f"{CSV_PATH} not found, put train.csv into ./data")
        with CSV_PATH.open(encoding="utf-8") as f:
            header = f.readline().strip().split(",")
            if header != COLUMNS:
                raise ValueError(f"Unexpected CSV header: {header}")
            cur.copy_expert(f"COPY labeled_transactions ({', '.join(COLUMNS)}) FROM STDIN WITH (FORMAT csv)", f)
        cur.execute("SELECT count(*), min(transaction_time), max(transaction_time) FROM labeled_transactions")
        rows, first, last = cur.fetchone()
    conn.commit()
    logger.info("Loaded %d rows from %s, %s .. %s", rows, CSV_PATH, first, last)


def init_state(conn):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO training_state (id, next_test_month) VALUES (1, %s) ON CONFLICT (id) DO NOTHING",
            (FIRST_TEST_MONTH,),
        )
        cur.execute("SELECT next_test_month FROM training_state WHERE id = 1")
        logger.info("Next test month for retraining: %s", cur.fetchone()[0])
    conn.commit()


def main():
    conn = connect()
    try:
        load_csv(conn)
        init_state(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
