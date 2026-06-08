"""
MongoDB data access helpers for the dashboard.
All query logic lives here; callbacks import these functions.

Column mapping for this project's cleaned CSV:
  source    → data source / review file name
  category  → product category
  rating    → product rating (0-5)
  title     → product/review title
  author    → reviewer name
"""
import os
import pandas as pd
from pymongo import MongoClient

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.environ.get("MONGO_DB", "ecommerce_db")

_client = None

# Canonical column names used throughout the dashboard.
# Maps internal dashboard key → actual CSV/DB column name.
COL_SOURCE   = "source"
COL_CATEGORY = "category"
COL_RATING   = "rating"
COL_TITLE    = "title"
COL_AUTHOR   = "author"


def get_client() -> MongoClient:
    """Return a shared MongoClient (lazy singleton)."""
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return _client


def get_collection(name: str):
    """Return a MongoDB collection by name."""
    return get_client()[DB_NAME][name]


def load_products_df() -> pd.DataFrame:
    """
    Load all products from MongoDB into a DataFrame.
    Falls back to the cleaned CSV if MongoDB is unavailable.
    """
    try:
        col  = get_collection("products")
        docs = list(col.find({}, {"_id": 0}))
        if docs:
            df = pd.DataFrame(docs)
            df["rating"] = pd.to_numeric(df.get("rating"), errors="coerce")
            return df
    except Exception:
        pass

    # Fallback: cleaned CSV from previous labs
    base = os.path.join(os.path.dirname(__file__), "..", "..", "data", "processed")
    path = os.path.join(base, "analytics", "products_raw.csv")
    if os.path.exists(path):
        df = pd.read_csv(path, low_memory=False)
        df["rating"] = pd.to_numeric(df.get("rating"), errors="coerce")
        return df

    return pd.DataFrame()


def get_sources(df: pd.DataFrame) -> list:
    """Return a sorted list of unique sources for the dropdown."""
    if COL_SOURCE in df.columns:
        return sorted(df[COL_SOURCE].dropna().unique().tolist())
    return []


def get_categories(df: pd.DataFrame) -> list:
    """Return a sorted list of unique categories for the dropdown."""
    if COL_CATEGORY in df.columns:
        return sorted(df[COL_CATEGORY].dropna().unique().tolist())
    return []


def get_rating_range(df: pd.DataFrame) -> tuple:
    """Return (min_rating, max_rating) from the dataset."""
    if COL_RATING in df.columns:
        valid = df[COL_RATING].dropna()
        if not valid.empty:
            return float(valid.min()), float(valid.max())
    return 0.0, 5.0


def filter_products(
    df: pd.DataFrame,
    source=None,
    category=None,
    search=None,
) -> pd.DataFrame:
    """
    Apply filters to the products DataFrame.
    All filters are optional; passing None skips that filter.
    """
    result = df.copy()

    if source and COL_SOURCE in result.columns:
        result = result[result[COL_SOURCE] == source]

    if category and COL_CATEGORY in result.columns:
        result = result[result[COL_CATEGORY] == category]

    if search and search.strip() and COL_TITLE in result.columns:
        mask = result[COL_TITLE].str.contains(search.strip(), case=False, na=False)
        result = result[mask]

    return result