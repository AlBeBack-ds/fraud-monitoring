import numpy as np
import pandas as pd

TARGET_COL = "target"
DROP_COLS = ["name_1", "name_2", "street", "post_code"]
RAW_CAT_COLS = ["gender", "merch", "cat_id", "one_city", "us_state", "jobs"]
TIME_COLS = ["hour", "day_of_week", "day_of_month", "month"]
CONT_COLS = ["amount", "population_city", "distance"]
GEO_COLS = ["lat", "lon", "merchant_lat", "merchant_lon"]
REQUIRED_COLS = ["transaction_time", "amount", "population_city"] + RAW_CAT_COLS + GEO_COLS

N_TOP_CATS = 50
NAN_CAT = "cat_NAN"
OTHER_CAT = f"cat_{N_TOP_CATS}+"
MEAN_ENC_SMOOTHING = 20

CAT_FEATURES = [c + "_cat" for c in RAW_CAT_COLS] + TIME_COLS
NUM_FEATURES = [c + "_mean_enc" for c in CAT_FEATURES] + [c + "_log" for c in CONT_COLS] + ["night", "amount_to_cat_mean"]
FEATURES = CAT_FEATURES + NUM_FEATURES

EARTH_RADIUS_KM = 6371.009


def haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = (np.radians(x) for x in (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))


def _int_to_cat(series):
    return series.astype("Int64").astype(str).replace("<NA>", NAN_CAT)


def _raw_to_str(value):
    return NAN_CAT if pd.isna(value) else str(value)


def add_base_features(raw):
    df = raw.drop(columns=[c for c in DROP_COLS if c in raw.columns]).copy()
    for col in REQUIRED_COLS:
        if col not in df.columns:
            df[col] = np.nan

    ts = pd.to_datetime(df["transaction_time"], errors="coerce", format="mixed")
    df["hour"] = _int_to_cat(ts.dt.hour)
    df["day_of_week"] = _int_to_cat(ts.dt.dayofweek)
    df["day_of_month"] = _int_to_cat(ts.dt.day)
    df["month"] = _int_to_cat(ts.dt.month)
    df["night"] = ts.dt.hour.isin([22, 23, 0, 1, 2, 3]).astype(float)

    for col in GEO_COLS + ["amount", "population_city"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["distance"] = haversine_km(df["lat"], df["lon"], df["merchant_lat"], df["merchant_lon"])
    return df


def _apply_cat_mapping(df, artifacts):
    for col in RAW_CAT_COLS:
        mapping = artifacts["cat_mapping"][col]
        df[col + "_cat"] = [
            NAN_CAT if key == NAN_CAT else mapping.get(key, OTHER_CAT)
            for key in map(_raw_to_str, df[col])
        ]
    return df


def fit_artifacts(train):
    df = add_base_features(train)
    target = train[TARGET_COL].astype(float).values
    prior = float(target.mean())

    artifacts = {"prior": prior, "cat_mapping": {}, "mean_enc": {}, "cont_means": {}, "cat_amount_mean": {}}

    for col in RAW_CAT_COLS:
        counts = df[col].dropna().map(str).value_counts()
        artifacts["cat_mapping"][col] = {v: f"cat_{i}" for i, v in enumerate(counts.index[:N_TOP_CATS])}
    df = _apply_cat_mapping(df, artifacts)

    for col in CAT_FEATURES:
        stats = pd.DataFrame({"cat": df[col].values, "y": target}).groupby("cat")["y"].agg(["sum", "count"])
        enc = (stats["sum"] + prior * MEAN_ENC_SMOOTHING) / (stats["count"] + MEAN_ENC_SMOOTHING)
        artifacts["mean_enc"][col] = {str(k): float(v) for k, v in enc.items()}

    for col in CONT_COLS:
        artifacts["cont_means"][col] = float(df[col].mean())

    artifacts["cat_amount_mean"] = {str(k): float(v) for k, v in df.groupby("cat_id_cat")["amount"].mean().items()}
    return artifacts


def transform(raw, artifacts):
    df = _apply_cat_mapping(add_base_features(raw), artifacts)

    for col in CAT_FEATURES:
        df[col + "_mean_enc"] = df[col].map(artifacts["mean_enc"][col]).fillna(artifacts["prior"]).astype(float)

    for col in CONT_COLS:
        df[col] = df[col].fillna(artifacts["cont_means"][col]).clip(lower=0)
        df[col + "_log"] = np.log1p(df[col])

    cat_mean = df["cat_id_cat"].map(artifacts["cat_amount_mean"]).fillna(artifacts["cont_means"]["amount"])
    df["amount_to_cat_mean"] = df["amount"] / cat_mean.clip(lower=1)

    return df[FEATURES].reset_index(drop=True)
