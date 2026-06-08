"""
One-time script: load the cleaned CSV into MongoDB.
Run once before starting the dashboard:
    python scripts/seed_mongo.py
"""
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pandas as pd
from pymongo import MongoClient

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.environ.get("MONGO_DB", "ecommerce_db")

CSV_PATH = os.path.join(
    os.path.dirname(__file__), "..", "data", "processed", "analytics", "products_raw.csv"
)


def seed():
    df = pd.read_csv(CSV_PATH, low_memory=False)
    df["rating"] = pd.to_numeric(df.get("rating"), errors="coerce")
    print(f"Loaded {len(df):,} rows from {CSV_PATH}")

    client = MongoClient(MONGO_URI)
    col    = client[DB_NAME]["products"]
    col.drop()

    records = df.where(df.notna(), None).to_dict("records")
    if records:
        col.insert_many(records)
        print(f"Inserted {len(records):,} documents into {DB_NAME}.products")

    col.create_index("source")
    col.create_index("category")
    col.create_index("title")
    col.create_index("rating")
    print("Indexes created.")
    client.close()


if __name__ == "__main__":
    seed()