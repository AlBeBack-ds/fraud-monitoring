from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.quality.checks import (check_categories, check_completeness, check_duplicates, check_fraud_rate,
                                     check_month_continuity, check_schema_and_volume, check_value_ranges)
from pipeline.quality.report import assert_quality, build_report

CHECKS = [
    check_schema_and_volume,
    check_completeness,
    check_value_ranges,
    check_categories,
    check_month_continuity,
    check_fraud_rate,
    check_duplicates,
]


@dag(
    dag_id="data_quality",
    description="Проверки витрины labeled_transactions и HTML-отчёт в artifacts/dq_reports",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    dagrun_timeout=timedelta(minutes=15),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["fraud", "dq"],
    doc_md="Семь DQ-правил по витрине. Отчёт: artifacts/dq_reports/latest.html.",
)
def data_quality():
    results = [task(check, task_id=check.__name__)() for check in CHECKS]
    report = task(build_report, task_id="build_report")(results)
    verdict = task(assert_quality, task_id="assert_quality")(results)
    report >> verdict


data_quality()
