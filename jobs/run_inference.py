"""
Inference Job: Load the saved model, score unfulfilled orders, and write predictions to shop.db.
Run this after train_model.py, or on demand via the web app's "Run Scoring" button.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
import joblib
from datetime import datetime

from config import OP_DB_PATH, MODEL_PATH
from utils_db import sqlite_conn, ensure_predictions_table

FEATURE_COLS = [
    "num_items",
    "customer_age",
    "order_dow",
    "order_month"
]


def run_inference():
    if not MODEL_PATH.exists():
        print(f"ERROR: Model file not found at {MODEL_PATH}")
        print("Run etl_build_warehouse.py and train_model.py first.")
        sys.exit(1)

    model = joblib.load(str(MODEL_PATH))

    with sqlite_conn(OP_DB_PATH) as conn:
        query = """
        SELECT
            o.order_id,
            o.order_datetime,
            o.fulfilled,
            c.birthdate,
            oi_agg.num_items
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN (
            SELECT
                order_id,
                SUM(quantity) AS num_items
            FROM order_items
            GROUP BY order_id
        ) oi_agg ON oi_agg.order_id = o.order_id
        WHERE o.fulfilled = 0
        """
        df_live = pd.read_sql(query, conn)

    if df_live.empty:
        print("No unfulfilled orders found. Place an order via the app first.")
        return 0

    df_live["order_datetime"] = pd.to_datetime(df_live["order_datetime"], errors="coerce")
    df_live["birthdate"] = pd.to_datetime(df_live["birthdate"], errors="coerce")

    now_year = datetime.now().year
    df_live["customer_age"] = now_year - df_live["birthdate"].dt.year
    df_live["order_dow"] = df_live["order_datetime"].dt.dayofweek
    df_live["order_month"] = df_live["order_datetime"].dt.month

    X_live = df_live[FEATURE_COLS]

    probs = model.predict_proba(X_live)[:, 1]
    preds = model.predict(X_live)

    ts = datetime.utcnow().isoformat()
    out_rows = [
        (int(oid), float(p), int(yhat), ts)
        for oid, p, yhat in zip(df_live["order_id"], probs, preds)
    ]

    with sqlite_conn(OP_DB_PATH) as conn:
        ensure_predictions_table(conn)
        cur = conn.cursor()
        cur.executemany("""
        INSERT OR REPLACE INTO order_predictions
        (order_id, late_delivery_probability, predicted_late_delivery, prediction_timestamp)
        VALUES (?, ?, ?, ?)
        """, out_rows)
        conn.commit()

    print(f"Inference complete. Predictions written: {len(out_rows)}")
    return len(out_rows)


if __name__ == "__main__":
    run_inference()
