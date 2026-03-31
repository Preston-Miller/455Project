"""
ETL Job: Extract from shop.db, denormalize, engineer features, and load into warehouse.db.
Run this before train_model.py.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
from datetime import datetime
from config import OP_DB_PATH, WH_DB_PATH
from utils_db import sqlite_conn


def build_modeling_table():
    with sqlite_conn(OP_DB_PATH) as conn:
        orders = pd.read_sql("SELECT * FROM orders", conn)
        customers = pd.read_sql("SELECT * FROM customers", conn)
        order_items = pd.read_sql("SELECT * FROM order_items", conn)
        shipments = pd.read_sql("SELECT * FROM shipments", conn)

    # Aggregate order-item level features per order
    item_features = (
        order_items
        .groupby("order_id")
        .agg(
            num_items=("quantity", "sum")
        )
        .reset_index()
    )

    # Join everything into one table
    df = (
        orders
        .merge(customers[["customer_id", "birthdate", "gender", "customer_segment", "loyalty_tier"]], on="customer_id", how="left")
        .merge(item_features, on="order_id", how="left")
        .merge(shipments[["order_id", "late_delivery"]], on="order_id", how="inner")
    )

    # Date feature engineering
    df["order_datetime"] = pd.to_datetime(df["order_datetime"], errors="coerce")
    df["birthdate"] = pd.to_datetime(df["birthdate"], errors="coerce")

    now_year = datetime.now().year
    df["customer_age"] = now_year - df["birthdate"].dt.year
    df["order_dow"] = df["order_datetime"].dt.dayofweek
    df["order_month"] = df["order_datetime"].dt.month

    # Select only the columns needed for modeling
    modeling_cols = [
        "order_id",
        "num_items",
        "customer_age",
        "order_dow",
        "order_month",
        "late_delivery"
    ]

    df_model = df[modeling_cols].dropna(subset=["late_delivery"])

    with sqlite_conn(WH_DB_PATH) as wh_conn:
        df_model.to_sql("modeling_orders", wh_conn, if_exists="replace", index=False)

    return len(df_model)


if __name__ == "__main__":
    row_count = build_modeling_table()
    print(f"Warehouse updated. modeling_orders rows: {row_count}")
