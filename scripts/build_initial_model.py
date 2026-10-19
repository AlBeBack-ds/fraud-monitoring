import argparse
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from fraud_core.bundle import ModelBundle, save_bundle  # noqa: E402
from fraud_core.preprocessing import TARGET_COL, fit_artifacts, transform  # noqa: E402
from fraud_core.training import MODEL_PARAMS, best_f1_threshold, evaluate, fit_model  # noqa: E402
from pipeline.db import connect, read_labeled  # noqa: E402

TRAIN_START = date(2019, 1, 1)
TEST_MONTH = date(2019, 4, 1)
TEST_END = date(2019, 5, 1)


def main():
    parser = argparse.ArgumentParser(description="Стартовая модель для сервиса скоринга")
    parser.add_argument("--out", default=str(ROOT / "fraud_detector" / "initial_model"))
    args = parser.parse_args()

    with connect() as conn:
        train = read_labeled(conn, TRAIN_START, TEST_MONTH)
        test = read_labeled(conn, TEST_MONTH, TEST_END)

    artifacts = fit_artifacts(train)
    model = fit_model(transform(train, artifacts), train[TARGET_COL])

    scores = ModelBundle("initial", model, artifacts, {"threshold": 0.5}).predict_proba(test)
    threshold = best_f1_threshold(test[TARGET_COL], scores)
    metrics = evaluate(test[TARGET_COL], scores, threshold)

    meta = {
        "version": "initial",
        "threshold": threshold,
        "train_months": ["2019-01", "2019-02", "2019-03"],
        "test_month": "2019-04",
        "train_rows": int(len(train)),
        "metrics": metrics,
        "model_params": MODEL_PARAMS,
        "trained_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    save_bundle(args.out, model, artifacts, meta)
    print(f"saved to {args.out}: {metrics}")


if __name__ == "__main__":
    main()
