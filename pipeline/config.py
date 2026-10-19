import os
from pathlib import Path

WORK_DIR = Path(os.getenv("WORK_DIR", "/opt/airflow/work"))
MODELS_DIR = Path(os.getenv("MODELS_DIR", "/opt/airflow/artifacts/models"))
DQ_REPORTS_DIR = Path(os.getenv("DQ_REPORTS_DIR", "/opt/airflow/artifacts/dq_reports"))

WINDOW_MONTHS = 4
SUPPLY_BATCH_ROWS = 50_000
HISTORY_MONTHS = 24

GATE_METRIC = "pr_auc"
KEEP_PUBLISHED_MODELS = 5
KEEP_REJECTED_MODELS = 5
