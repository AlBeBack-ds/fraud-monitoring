import calendar
import io
import logging
from datetime import date

import pandas as pd

from pipeline.config import HISTORY_MONTHS, SUPPLY_BATCH_ROWS, WORK_DIR
from pipeline.db import LABELED_COLUMNS, add_months, connect, month_label

logger = logging.getLogger(__name__)

SUPPLY_DIR = WORK_DIR / "supply"


def _batch_path(batch_month: date):
    return SUPPLY_DIR / f"{month_label(batch_month)}.parquet"


def shift_to_month(frame: pd.DataFrame, month: date) -> pd.DataFrame:
    ts = pd.to_datetime(frame["transaction_time"])
    last_day = calendar.monthrange(month.year, month.month)[1]
    shifted = frame.copy()
    shifted["transaction_time"] = pd.to_datetime({
        "year": month.year,
        "month": month.month,
        "day": ts.dt.day.clip(upper=last_day),
        "hour": ts.dt.hour,
        "minute": ts.dt.minute,
        "second": ts.dt.second,
    })
    return shifted


def sample_next_month(run_id=None) -> dict:
    """Готовит батч нового месяца из строк, которые уже лежат в витрине.

    Читает: max(transaction_time) из labeled_transactions и случайные
    SUPPLY_BATCH_ROWS строк витрины. Время каждой строки переносится в следующий
    календарный месяц с сохранением дня, часа и минуты (31 число в коротком
    месяце становится последним днём месяца). Строка и её копия из прошлой
    поставки после переноса совпадут полностью, поэтому дубли выбрасываются.
    Пишет: work/supply/<месяц>.parquet. Витрину не меняет.

    Если этот же прогон DAG уже дописывал месяц (запись в supply_batches с тем
    же run_id), берётся тот же месяц, а не следующий, поэтому перезапуск
    прогона не добавит лишний месяц. Выборка детерминирована: setseed зависит
    от месяца, и повторная попытка соберёт тот же батч.
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT batch_month FROM supply_batches WHERE run_id = %s", (run_id,))
            row = cur.fetchone()
            if row:
                batch_month = row[0]
            else:
                cur.execute("SELECT max(transaction_time) FROM labeled_transactions")
                newest = cur.fetchone()[0]
                if newest is None:
                    raise RuntimeError("labeled_transactions is empty, data_loader has not finished")
                batch_month = add_months(newest.date().replace(day=1), 1)

            cur.execute("SELECT setseed(%s)", ((batch_month.year * 12 + batch_month.month) % 997 / 997,))
            cur.execute(
                f"SELECT {', '.join(LABELED_COLUMNS)} FROM labeled_transactions "
                "WHERE transaction_time < %s ORDER BY random() LIMIT %s",
                (batch_month, SUPPLY_BATCH_ROWS),
            )
            frame = pd.DataFrame(cur.fetchall(), columns=LABELED_COLUMNS)

    batch = shift_to_month(frame, batch_month).drop_duplicates(subset=LABELED_COLUMNS)
    SUPPLY_DIR.mkdir(parents=True, exist_ok=True)
    batch.to_parquet(_batch_path(batch_month), index=False)
    logger.info("Batch for %s: %d rows, fraud rate %.4f", month_label(batch_month), len(batch), batch["target"].mean())
    return {"batch_month": batch_month.isoformat(), "rows": int(len(batch))}


def append_month(batch: dict, run_id=None) -> dict:
    """Дописывает подготовленный месяц в витрину labeled_transactions.

    Читает: work/supply/<месяц>.parquet.
    Пишет: строки месяца в labeled_transactions и запись о поставке в
    supply_batches.

    Всё делается в одной транзакции: сначала удаляются строки этого месяца,
    если они остались от предыдущей попытки, затем COPY нового батча и запись в
    supply_batches. Повторный запуск поэтому не создаёт дублей, а сбой посередине
    откатывает транзакцию целиком.
    """
    batch_month = date.fromisoformat(batch["batch_month"])
    frame = pd.read_parquet(_batch_path(batch_month))

    buffer = io.StringIO()
    frame[LABELED_COLUMNS].to_csv(buffer, index=False, header=False, date_format="%Y-%m-%d %H:%M:%S")
    buffer.seek(0)

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM labeled_transactions WHERE transaction_time >= %s AND transaction_time < %s",
                (batch_month, add_months(batch_month, 1)),
            )
            replaced = cur.rowcount
            cur.copy_expert(f"COPY labeled_transactions ({', '.join(LABELED_COLUMNS)}) FROM STDIN WITH (FORMAT csv)", buffer)
            cur.execute(
                "INSERT INTO supply_batches (run_id, batch_month, rows_added) VALUES (%s, %s, %s) "
                "ON CONFLICT (run_id) DO UPDATE SET rows_added = EXCLUDED.rows_added, created_at = now()",
                (run_id, batch_month, len(frame)),
            )
        conn.commit()

    logger.info("Appended %d rows for %s (replaced %d)", len(frame), month_label(batch_month), replaced)
    return {**batch, "replaced_rows": int(replaced)}


def prune_history(batch: dict) -> dict:
    """Удаляет из витрины наблюдения старше HISTORY_MONTHS месяцев.

    Читает: месяц самой свежей транзакции в labeled_transactions.
    Пишет: удаляет строки раньше первого месяца допустимой истории. Остаются
    HISTORY_MONTHS последних календарных месяцев, включая самый свежий.

    Граница считается от данных, а не от времени запуска, поэтому повторный
    запуск после успешной очистки ничего не удалит: строк старше границы уже нет.
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT date_trunc('month', max(transaction_time))::date FROM labeled_transactions")
            newest_month = cur.fetchone()[0]
            cutoff = add_months(newest_month, -(HISTORY_MONTHS - 1))
            cur.execute("DELETE FROM labeled_transactions WHERE transaction_time < %s", (cutoff,))
            deleted = cur.rowcount
        conn.commit()

    logger.info("History kept from %s to %s, deleted %d rows", cutoff, newest_month, deleted)
    return {**batch, "newest_month": newest_month.isoformat(), "cutoff": cutoff.isoformat(), "deleted_rows": int(deleted)}
