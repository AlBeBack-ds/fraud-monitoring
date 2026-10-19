FROM apache/airflow:2.10.5-python3.11

ENV PIP_DEFAULT_TIMEOUT=120 \
    PIP_RETRIES=10

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && python -c "import catboost, sklearn, pandas, numpy, pyarrow; print(catboost.__version__, pandas.__version__, numpy.__version__)"
