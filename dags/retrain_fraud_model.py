from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param

from pipeline.retrain.dataset import prepare_window, split_by_time
from pipeline.retrain.modeling import build_features, train_candidate
from pipeline.retrain.release import (PUBLISH_TASK_ID, REJECT_TASK_ID, advance_window, choose_release,
                                      compare_with_production, publish_model, reject_model)


@dag(
    dag_id="retrain_fraud_model",
    description="Скользящее переобучение CatBoost и публикация модели в сервис скоринга",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["fraud", "retrain"],
    params={
        "force_publish": Param(
            False,
            type="boolean",
            description="Опубликовать кандидата без сравнения с прод-моделью",
        ),
    },
    doc_md="""
Окно из 4 месяцев: 3 ранних идут в train, самый свежий в test.
Кандидат публикуется, только если его PR-AUC на свежем месяце не ниже, чем у модели в проде.
Trigger DAG с параметром force_publish=true публикует кандидата без сравнения.
Указатель окна сдвигается задачей advance_window после сохранения модели.
""",
)
def retrain_fraud_model():
    window = task(prepare_window, task_id="prepare_window")()
    split = task(split_by_time, task_id="split_by_time")(window)
    features = task(build_features, task_id="build_features")(split)
    candidate = task(train_candidate, task_id="train_candidate")(features)
    decision = task(compare_with_production, task_id="compare_with_production")(candidate)
    branch = task.branch(choose_release, task_id="choose_release")(decision)

    published = task(publish_model, task_id=PUBLISH_TASK_ID)(candidate)
    rejected = task(reject_model, task_id=REJECT_TASK_ID)(candidate)
    advanced = task(advance_window, task_id="advance_window", trigger_rule="none_failed_min_one_success")(candidate)

    branch >> [published, rejected]
    [published, rejected] >> advanced


retrain_fraud_model()
