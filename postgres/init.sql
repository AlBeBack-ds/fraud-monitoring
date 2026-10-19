CREATE DATABASE airflow;

CREATE TABLE IF NOT EXISTS labeled_transactions (
    transaction_time TIMESTAMP NOT NULL,
    merch TEXT,
    cat_id TEXT,
    amount DOUBLE PRECISION,
    name_1 TEXT,
    name_2 TEXT,
    gender TEXT,
    street TEXT,
    one_city TEXT,
    us_state TEXT,
    post_code INTEGER,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    population_city INTEGER,
    jobs TEXT,
    merchant_lat DOUBLE PRECISION,
    merchant_lon DOUBLE PRECISION,
    target SMALLINT
);

CREATE INDEX IF NOT EXISTS labeled_transactions_time_idx ON labeled_transactions (transaction_time);

CREATE TABLE IF NOT EXISTS training_state (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    next_test_month DATE NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS supply_batches (
    run_id TEXT PRIMARY KEY,
    batch_month DATE NOT NULL UNIQUE,
    rows_added INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fraud_scores (
    id BIGSERIAL PRIMARY KEY,
    transaction_id TEXT NOT NULL UNIQUE,
    score DOUBLE PRECISION NOT NULL,
    fraud_flag SMALLINT NOT NULL CHECK (fraud_flag IN (0, 1)),
    model_version TEXT,
    us_state TEXT,
    merch TEXT,
    cat_id TEXT,
    amount DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS fraud_scores_version_idx ON fraud_scores (model_version, id DESC);
CREATE INDEX IF NOT EXISTS fraud_scores_created_idx ON fraud_scores (created_at);
CREATE INDEX IF NOT EXISTS fraud_scores_state_merch_idx ON fraud_scores (us_state, merch, created_at);

CREATE ROLE grafana LOGIN PASSWORD 'grafana';
GRANT SELECT ON fraud_scores TO grafana;
