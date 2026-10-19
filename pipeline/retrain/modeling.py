import logging
from datetime import datetime, timezone

import pandas as pd

from fraud_core.bundle import read_json, save_bundle, write_json
from fraud_core.preprocessing import FEATURES, TARGET_COL, fit_artifacts, transform
from fraud_core.training import MODEL_PARAMS, best_f1_threshold, evaluate, fit_model
from pipeline.retrain.dataset import run_dir

logger = logging.getLogger(__name__)


def build_features(split: dict) -> dict:
    """Строит признаки собственным препроцессингом из fraud_core.preprocessing.

    Читает: train.parquet и test.parquet.
    Пишет: preprocessing.json со статистиками, x_train.parquet и x_test.parquet.

    Все статистики (топ-категории, сглаженный target encoding, средние для
    пропусков) считаются только по train, test лишь трансформируется ими, так что
    информация из проверочного месяца в признаки не утекает. Повторный запуск
    пересчитывает те же статистики из тех же файлов и перезаписывает результат.
    """
    directory = run_dir(split["test_month"])
    train = pd.read_parquet(directory / "train.parquet")
    test = pd.read_parquet(directory / "test.parquet")

    artifacts = fit_artifacts(train)
    x_train = transform(train, artifacts)
    x_train[TARGET_COL] = train[TARGET_COL].to_numpy()
    x_test = transform(test, artifacts)
    x_test[TARGET_COL] = test[TARGET_COL].to_numpy()

    write_json(directory / "preprocessing.json", artifacts)
    x_train.to_parquet(directory / "x_train.parquet", index=False)
    x_test.to_parquet(directory / "x_test.parquet", index=False)
    logger.info("Features: train %s, test %s", x_train.shape, x_test.shape)
    return split


def train_candidate(features: dict) -> dict:
    """Обучает CatBoost на train и считает метрики на свежем месяце.

    Читает: x_train.parquet, x_test.parquet и preprocessing.json.
    Пишет: папку candidate/ с бандлом модели (model.cbm, preprocessing.json,
    meta.json). Порог fraud_flag выбирается по максимуму F1 на test-месяце.

    В общий каталог моделей задача ничего не пишет: кандидат лежит в рабочей
    папке прогона и попадает в прод только через publish_model. Повторный запуск
    обучает модель заново и перезаписывает candidate/ целиком, поэтому
    недописанный бандл после сбоя никуда не уйдёт.
    """
    directory = run_dir(features["test_month"])
    x_train = pd.read_parquet(directory / "x_train.parquet")
    x_test = pd.read_parquet(directory / "x_test.parquet")
    artifacts = read_json(directory / "preprocessing.json")

    model = fit_model(x_train[FEATURES], x_train[TARGET_COL])
    scores = model.predict_proba(x_test[FEATURES])[:, 1]
    threshold = best_f1_threshold(x_test[TARGET_COL], scores)
    metrics = evaluate(x_test[TARGET_COL], scores, threshold)

    trained_at = datetime.now(timezone.utc)
    version = f"{features['test_month'][:7]}_{trained_at:%Y%m%dT%H%M%S}"
    meta = {
        "version": version,
        "threshold": threshold,
        "train_months": features["train_months"],
        "test_month": features["test_month"][:7],
        "train_rows": features["train_rows"],
        "metrics": metrics,
        "model_params": MODEL_PARAMS,
        "trained_at": trained_at.isoformat(timespec="seconds"),
    }
    save_bundle(directory / "candidate", model, artifacts, meta)
    logger.info("Candidate %s: %s", version, metrics)
    return {**features, "version": version, "threshold": threshold, "metrics": metrics}
