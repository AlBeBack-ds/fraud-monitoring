FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=120 \
    PIP_RETRIES=10 \
    PYTHONPATH=/app

WORKDIR /app

COPY fraud_detector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fraud_core ./fraud_core
COPY fraud_detector/*.py ./
COPY fraud_detector/initial_model ./initial_model

CMD ["python", "app.py"]
