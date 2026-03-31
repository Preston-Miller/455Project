"""
One-time migration: add fulfilled column to orders if it doesn't exist.
Also creates the order_predictions table used by the inference job.
Run once before starting the app or the inference pipeline.
"""
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OP_DB_PATH = PROJECT_ROOT / "shop.db"


def migrate():
    conn = sqlite3.connect(str(OP_DB_PATH))
    cur = conn.cursor()

    # Check if fulfilled column exists
    cur.execute("PRAGMA table_info(orders)")
    cols = [row[1] for row in cur.fetchall()]
    if "fulfilled" not in cols:
        cur.execute("ALTER TABLE orders ADD COLUMN fulfilled INTEGER DEFAULT 0")
        # Mark existing orders that already have shipments as fulfilled
        cur.execute("UPDATE orders SET fulfilled = 1 WHERE order_id IN (SELECT order_id FROM shipments)")
        print(f"Added 'fulfilled' column. Marked {cur.rowcount} historical orders as fulfilled.")
    else:
        print("'fulfilled' column already exists.")

    # Create order_predictions table if it doesn't exist
    cur.execute("""
    CREATE TABLE IF NOT EXISTS order_predictions (
        order_id INTEGER PRIMARY KEY,
        late_delivery_probability REAL,
        predicted_late_delivery INTEGER,
        prediction_timestamp TEXT
    )
    """)
    print("Ensured 'order_predictions' table exists.")

    conn.commit()
    conn.close()
    print("Migration complete.")


if __name__ == "__main__":
    migrate()
