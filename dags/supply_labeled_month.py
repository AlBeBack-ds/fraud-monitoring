from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.supply.month_batch import append_month, prune_history, sample_next_month


@dag(
    dag_id="supply_labeled_month",
    description="Дописывает в витрину новый размеченный месяц и чистит старую историю",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["fraud", "etl"],
    doc_md="""
Каждый запуск берёт 50 000 случайных строк витрины, переносит их в следующий календарный месяц
и дописывает в labeled_transactions. После этого удаляются строки старше 24 месяцев.
""",
)
def supply_labeled_month():
    batch = task(sample_next_month, task_id="sample_next_month")()
    appended = task(append_month, task_id="append_month")(batch)
    task(prune_history, task_id="prune_history")(appended)


supply_labeled_month()
