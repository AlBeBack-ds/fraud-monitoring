import logging
from pathlib import Path

import pandas as pd
from airflow.exceptions import AirflowSkipException

from pipeline.config import WINDOW_MONTHS, WORK_DIR
from pipeline.db import add_months, connect, month_label, read_labeled, read_next_test_month

logger = logging.getLogger(__name__)


def run_dir(test_month: str) -> Path:
    return WORK_DIR / "retrain" / test_month[:7]


def prepare_window() -> dict:
    """Готовит обучающую выборку: скользящее окно из WINDOW_MONTHS месяцев.

    Читает: указатель next_test_month из training_state и строки витрины
    labeled_transactions за окно, которое заканчивается этим месяцем. Новый месяц
    входит в окно, а месяц, который был в окне самым старым, из него выпадает.
    Пишет: work/retrain/<месяц>/window.parquet.

    Повторный запуск безопасен: указатель здесь не меняется, а файл каждый раз
    перезаписывается целиком, поэтому после ошибки задача возьмёт тот же месяц и
    соберёт то же окно. Если в витрине нет хотя бы одного месяца окна (например,
    DAG поставки его ещё не дописал), задача помечается как skipped и ничего не трогает.
    """
    with connect() as conn:
        test_month = read_next_test_month(conn)
        window_start = add_months(test_month, -(WINDOW_MONTHS - 1))
        frame = read_labeled(conn, window_start, add_months(test_month, 1))

    expected = [month_label(add_months(window_start, i)) for i in range(WINDOW_MONTHS)]
    present = set(frame["transaction_time"].dt.strftime("%Y-%m"))
    missing = [m for m in expected if m not in present]
    if missing:
        raise AirflowSkipException(f"No data for months {missing} in labeled_transactions yet")

    directory = run_dir(test_month.isoformat())
    directory.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(directory / "window.parquet", index=False)
    logger.info("Window %s: %d rows", expected, len(frame))
    return {"test_month": test_month.isoformat(), "window_months": expected, "rows": int(len(frame))}


def split_by_time(window: dict) -> dict:
    """Делит окно по времени: ранние месяцы идут в train, самый свежий в test.

    Читает: work/retrain/<месяц>/window.parquet.
    Пишет: train.parquet и test.parquet в той же папке.

    Случайного перемешивания нет, граница проходит по календарному месяцу, поэтому
    модель проверяется на данных из будущего относительно обучения. Повторный
    запуск перезаписывает те же два файла из того же окна и даёт тот же результат.
    """
    directory = run_dir(window["test_month"])
    frame = pd.read_parquet(directory / "window.parquet")
    month = frame["transaction_time"].dt.strftime("%Y-%m")
    test_label = window["test_month"][:7]

    train = frame[month < test_label]
    test = frame[month == test_label]
    if train.empty or test.empty:
        raise RuntimeError(f"Bad split: train={len(train)}, test={len(test)}")

    train.to_parquet(directory / "train.parquet", index=False)
    test.to_parquet(directory / "test.parquet", index=False)
    train_months = sorted(month[month < test_label].unique())
    logger.info("Train %s: %d rows, test %s: %d rows", train_months, len(train), test_label, len(test))
    return {
        **window,
        "train_months": train_months,
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
    }
