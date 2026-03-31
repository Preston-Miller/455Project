"""
Training Job: Load from warehouse.db, train a late-delivery classifier, and save artifacts.
Run this after etl_build_warehouse.py.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import json
from datetime import datetime
import pandas as pd
import joblib

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score, classification_report

from config import WH_DB_PATH, ARTIFACTS_DIR, MODEL_PATH, MODEL_METADATA_PATH, METRICS_PATH
from utils_db import sqlite_conn

MODEL_VERSION = "1.1.0"

FEATURE_COLS = [
    "num_items",
    "customer_age",
    "order_dow",
    "order_month"
]
LABEL_COL = "late_delivery"


def train_and_save():
    with sqlite_conn(WH_DB_PATH) as conn:
        df = pd.read_sql("SELECT * FROM modeling_orders", conn)

    print(f"Loaded {len(df)} rows from warehouse.")

    X = df[FEATURE_COLS]
    y = df[LABEL_COL].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("clf", RandomForestClassifier(n_estimators=200, max_depth=8, random_state=42, n_jobs=-1))
    ])

    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    acc = float(accuracy_score(y_test, y_pred))
    f1 = float(f1_score(y_test, y_pred))
    roc = float(roc_auc_score(y_test, y_prob))

    metrics = {
        "accuracy": acc,
        "f1": f1,
        "roc_auc": roc,
        "row_count_train": int(len(X_train)),
        "row_count_test": int(len(X_test)),
        "classification_report": classification_report(y_test, y_pred, output_dict=True)
    }

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, str(MODEL_PATH))

    metadata = {
        "model_version": MODEL_VERSION,
        "trained_at_utc": datetime.utcnow().isoformat(),
        "feature_list": FEATURE_COLS,
        "label": LABEL_COL,
        "warehouse_table": "modeling_orders",
        "warehouse_rows": int(len(df))
    }

    with open(MODEL_METADATA_PATH, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    with open(METRICS_PATH, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    print(f"Training complete.")
    print(f"  Accuracy : {acc:.4f}")
    print(f"  F1       : {f1:.4f}")
    print(f"  ROC-AUC  : {roc:.4f}")
    print(f"Saved model    : {MODEL_PATH}")
    print(f"Saved metadata : {MODEL_METADATA_PATH}")
    print(f"Saved metrics  : {METRICS_PATH}")


if __name__ == "__main__":
    train_and_save()
