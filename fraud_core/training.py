import numpy as np
from catboost import CatBoostClassifier
from sklearn.metrics import (average_precision_score, f1_score, precision_recall_curve,
                             precision_score, recall_score, roc_auc_score)

from fraud_core.preprocessing import CAT_FEATURES, FEATURES

MODEL_PARAMS = dict(
    iterations=300,
    depth=6,
    learning_rate=0.1,
    l2_leaf_reg=3,
    max_ctr_complexity=2,
    random_seed=42,
    thread_count=2,
    allow_writing_files=False,
    verbose=0,
)


def fit_model(features, target):
    model = CatBoostClassifier(**MODEL_PARAMS)
    model.fit(features[FEATURES], target, cat_features=CAT_FEATURES)
    return model


def best_f1_threshold(target, scores):
    precision, recall, thresholds = precision_recall_curve(target, scores)
    f1 = 2 * precision * recall / np.clip(precision + recall, 1e-12, None)
    return float(thresholds[int(np.argmax(f1[:-1]))])


def evaluate(target, scores, threshold):
    target = np.asarray(target)
    flags = (scores >= threshold).astype(int)
    has_both = len(np.unique(target)) == 2
    return {
        "pr_auc": float(average_precision_score(target, scores)) if has_both else None,
        "roc_auc": float(roc_auc_score(target, scores)) if has_both else None,
        "f1": float(f1_score(target, flags, zero_division=0)),
        "precision": float(precision_score(target, flags, zero_division=0)),
        "recall": float(recall_score(target, flags, zero_division=0)),
        "rows": int(len(target)),
        "fraud_rate": float(target.mean()) if len(target) else None,
    }
