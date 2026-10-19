import logging
import os
import shutil
from datetime import date, datetime, timezone

import pandas as pd

from fraud_core.bundle import META_FILE, load_bundle, read_json, read_latest, write_json, write_latest
from fraud_core.preprocessing import TARGET_COL
from fraud_core.training import evaluate
from pipeline.config import GATE_METRIC, KEEP_PUBLISHED_MODELS, KEEP_REJECTED_MODELS, MODELS_DIR
from pipeline.db import add_months, connect
from pipeline.retrain.dataset import run_dir

logger = logging.getLogger(__name__)

PUBLISH_TASK_ID = "publish_model"
REJECT_TASK_ID = "reject_model"
REJECTED_DIR = MODELS_DIR / "rejected"


def compare_with_production(candidate: dict, params=None) -> dict:
    """Сравнивает кандидата с моделью, которая сейчас в проде.

    Читает: latest.json и бандл текущей модели из каталога моделей, test.parquet
    прогона. Текущая модель скорит тот же свежий месяц своим препроцессингом и
    порогом, после чего обе модели сравниваются по PR-AUC (GATE_METRIC).
    Пишет: decision.json в папке прогона.

    Кандидат публикуется, если его метрика не ниже, чем у текущей модели, или
    если опубликованной модели ещё нет. Параметр запуска force_publish=true
    публикует кандидата без сравнения, это отмечается в decision.json. Задача
    только читает прод и пишет решение в рабочую папку, поэтому её можно
    перезапускать сколько угодно.
    """
    directory = run_dir(candidate["test_month"])
    candidate_metric = candidate["metrics"][GATE_METRIC]
    decision = {
        "metric": GATE_METRIC,
        "test_month": candidate["test_month"][:7],
        "candidate_version": candidate["version"],
        "candidate_value": candidate_metric,
        "production_version": read_latest(MODELS_DIR),
        "production_value": None,
    }

    if decision["production_version"] is None:
        decision.update(publish=True, reason="no published model yet")
    else:
        production = load_bundle(MODELS_DIR / decision["production_version"])
        test = pd.read_parquet(directory / "test.parquet")
        scores = production.predict_proba(test)
        production_metric = evaluate(test[TARGET_COL], scores, production.threshold)[GATE_METRIC]
        decision["production_value"] = production_metric
        if candidate_metric is None:
            decision.update(publish=False, reason=f"{GATE_METRIC} of candidate is undefined")
        elif production_metric is None or candidate_metric >= production_metric:
            decision.update(publish=True, reason=f"{GATE_METRIC} is not worse than production")
        else:
            decision.update(publish=False, reason=f"{GATE_METRIC} is worse than production")

    if (params or {}).get("force_publish") and not decision["publish"]:
        decision.update(publish=True, reason=f"force_publish=true, quality gate skipped ({decision['reason']})")

    write_json(directory / "decision.json", decision)
    logger.info("Decision: %s", decision)
    return decision


def choose_release(decision: dict) -> str:
    """Выбирает ветку графа по решению compare_with_production.

    Читает только XCom с решением и возвращает task_id следующей задачи:
    publish_model или reject_model. Вторая ветка будет помечена как skipped.
    Побочных эффектов нет, повторный запуск возвращает ту же ветку.
    """
    return PUBLISH_TASK_ID if decision["publish"] else REJECT_TASK_ID


def _copy_bundle(source, target, decision):
    if target.exists():
        return
    tmp = target.with_name(f".{target.name}.tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    shutil.copytree(source, tmp)
    write_json(tmp / "decision.json", decision)
    os.replace(tmp, target)


def _cleanup(directory, keep, protected):
    versions = sorted(
        (p for p in directory.iterdir() if p.is_dir() and (p / META_FILE).exists() and p.name not in protected),
        key=lambda p: read_json(p / META_FILE).get("trained_at", ""),
    )
    for old in versions[:-keep] if keep else versions:
        shutil.rmtree(old, ignore_errors=True)
        logger.info("Removed old model %s", old)


def publish_model(candidate: dict) -> str:
    """Публикует кандидата в каталог моделей, который читает сервис скоринга.

    Читает: candidate/ и decision.json из папки прогона.
    Пишет: каталог models/<version>/ и затем latest.json с новой версией.

    Бандл сначала копируется во временную папку и переименовывается одним
    os.replace, latest.json тоже пишется через временный файл. Сервис скоринга
    поэтому никогда не увидит недописанную модель. Повторный запуск не копирует
    модель второй раз, если папка версии уже есть, и снова пишет тот же
    latest.json. Старые версии сверх KEEP_PUBLISHED_MODELS удаляются, текущая
    и стартовая initial не трогаются.
    """
    directory = run_dir(candidate["test_month"])
    version = candidate["version"]
    decision = read_json(directory / "decision.json")

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    _copy_bundle(directory / "candidate", MODELS_DIR / version, decision)
    write_latest(
        MODELS_DIR,
        version,
        published_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        test_month=candidate["test_month"][:7],
        metrics=candidate["metrics"],
    )
    logger.info("Published model %s", version)
    _cleanup(MODELS_DIR, KEEP_PUBLISHED_MODELS, protected={version, "initial", REJECTED_DIR.name})
    return version


def reject_model(candidate: dict) -> str:
    """Сохраняет отклонённого кандидата отдельно от прода.

    Читает: candidate/ и decision.json из папки прогона.
    Пишет: models/rejected/<version>/ вместе с decision.json, где видно, какие
    метрики сравнивались. latest.json не меняется, сервис продолжает скорить
    прежней моделью.

    Повторный запуск не копирует модель второй раз, если папка уже есть.
    Хранятся последние KEEP_REJECTED_MODELS отклонённых версий.
    """
    directory = run_dir(candidate["test_month"])
    version = candidate["version"]
    decision = read_json(directory / "decision.json")

    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
    _copy_bundle(directory / "candidate", REJECTED_DIR / version, decision)
    logger.info("Rejected model %s: %s", version, decision["reason"])
    _cleanup(REJECTED_DIR, KEEP_REJECTED_MODELS, protected={version})
    return version


def advance_window(candidate: dict) -> str:
    """Сдвигает указатель окна на следующий месяц после сохранения модели.

    Читает и пишет: training_state.next_test_month.

    Запускается, только когда одна из веток publish_model или reject_model
    завершилась успешно, то есть модель уже сохранена в каталог. Если что-то
    упало раньше, указатель остаётся на месте и следующий запуск обработает
    тот же месяц. UPDATE выполняется с условием на текущий месяц, поэтому
    повторный запуск этой задачи не сдвинет окно второй раз.
    """
    test_month = date.fromisoformat(candidate["test_month"])
    next_month = add_months(test_month, 1)
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE training_state SET next_test_month = %s, updated_at = now() "
                "WHERE id = 1 AND next_test_month = %s",
                (next_month, test_month),
            )
            updated = cur.rowcount
        conn.commit()

    if updated:
        logger.info("Window moved: next test month is %s", next_month)
    else:
        logger.info("Pointer was already moved past %s, nothing to do", test_month)
    return next_month.isoformat()
