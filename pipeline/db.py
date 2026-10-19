import os
from contextlib import contextmanager
from datetime import date

import pandas as pd
import psycopg2
from dateutil.relativedelta import relativedelta

LABELED_COLUMNS = [
    "transaction_time", "merch", "cat_id", "amount", "name_1", "name_2", "gender", "street",
    "one_city", "us_state", "post_code", "lat", "lon", "population_city", "jobs",
    "merchant_lat", "merchant_lon", "target",
]

@contextmanager
def connect():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "fraud"),
        user=os.getenv("POSTGRES_USER", "fraud"),
        password=os.getenv("POSTGRES_PASSWORD", "fraud"),
    )
    try:
        yield conn
    finally:
        conn.close()


def add_months(month: date, n: int) -> date:
    return month + relativedelta(months=n)


def month_label(month: date) -> str:
    return month.strftime("%Y-%m")


def read_labeled(conn, start: date, end: date) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {', '.join(LABELED_COLUMNS)} FROM labeled_transactions "
            "WHERE transaction_time >= %s AND transaction_time < %s "
            'ORDER BY transaction_time, merch COLLATE "C", amount, street COLLATE "C", merchant_lat',
            (start, end),
        )
        frame = pd.DataFrame(cur.fetchall(), columns=LABELED_COLUMNS)
    frame["transaction_time"] = pd.to_datetime(frame["transaction_time"])
    return frame


def read_next_test_month(conn) -> date:
    with conn.cursor() as cur:
        cur.execute("SELECT next_test_month FROM training_state WHERE id = 1")
        row = cur.fetchone()
    if row is None:
        raise RuntimeError("training_state is empty, data_loader has not finished")
    return row[0]
