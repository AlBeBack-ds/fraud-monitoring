import json
import os
from dataclasses import dataclass
from pathlib import Path

from catboost import CatBoostClassifier

from fraud_core.preprocessing import CAT_FEATURES, FEATURES, transform

MODEL_FILE = "model.cbm"
PREPROCESSING_FILE = "preprocessing.json"
META_FILE = "meta.json"
LATEST_FILE = "latest.json"


def write_json(path, payload):
    path = Path(path)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


@dataclass
class ModelBundle:
    version: str
    model: CatBoostClassifier
    artifacts: dict
    meta: dict

    @property
    def threshold(self):
        return float(self.meta["threshold"])

    def predict_proba(self, raw):
        features = transform(raw, self.artifacts)
        for col in CAT_FEATURES:
            features[col] = features[col].astype(str)
        return self.model.predict_proba(features[FEATURES])[:, 1]

    def predict(self, raw):
        scores = self.predict_proba(raw)
        return scores, (scores >= self.threshold).astype(int)


def save_bundle(directory, model, artifacts, meta):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    model.save_model(str(directory / MODEL_FILE))
    write_json(directory / PREPROCESSING_FILE, artifacts)
    write_json(directory / META_FILE, meta)


def load_bundle(directory):
    directory = Path(directory)
    meta = read_json(directory / META_FILE)
    model = CatBoostClassifier()
    model.load_model(str(directory / MODEL_FILE))
    return ModelBundle(meta["version"], model, read_json(directory / PREPROCESSING_FILE), meta)


def read_latest(models_dir):
    path = Path(models_dir) / LATEST_FILE
    if not path.exists():
        return None
    return read_json(path)["version"]


def write_latest(models_dir, version, **extra):
    write_json(Path(models_dir) / LATEST_FILE, {"version": version, **extra})
