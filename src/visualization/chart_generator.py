"""
Automated chart generation module – Lab 12.
Loads the cleaned e-commerce dataset and generates all static and interactive charts.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

DATA_PATH = Path("data/processed/analytics/products_raw.csv")
STATIC_OUT = Path("outputs/visualizations/static")
INTERACTIVE_OUT = Path("outputs/visualizations/interactive")


def load_data(path: Path = DATA_PATH) -> pd.DataFrame:
    """Load and lightly prepare the cleaned products dataset."""
    if not path.exists():
        raise FileNotFoundError(
            f"Cleaned dataset not found at '{path}'. "
            "Run the cleaning pipeline first (Lab 9)."
        )
    df = pd.read_csv(path, low_memory=False)
    required = ["title", "rating", "helpful_votes", "source"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in dataset: {missing}")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["helpful_votes"] = pd.to_numeric(df["helpful_votes"], errors="coerce")
    df = df.dropna(subset=["rating"])
    logger.info("Loaded %d products from %s", len(df), path)
    return df


def run_static_charts(df: pd.DataFrame) -> dict:
    """Generate and save all matplotlib / seaborn charts."""
    from .static_charts import (
        plot_top_products_by_rating,
        plot_avg_rating_per_source,
        plot_reviews_vs_rating_scatter,
        plot_rating_distribution,
        plot_rating_by_source_boxplot,
        plot_correlation_heatmap,
        plot_products_per_source_bar,
        plot_dashboard_subplots,
    )

    STATIC_OUT.mkdir(parents=True, exist_ok=True)
    results = {}

    charts = [
        ("top_products_by_rating",      plot_top_products_by_rating),
        ("avg_rating_per_source_dual",  plot_avg_rating_per_source),
        ("reviews_vs_rating_scatter",   plot_reviews_vs_rating_scatter),
        ("rating_distribution",         plot_rating_distribution),
        ("rating_by_source_boxplot",    plot_rating_by_source_boxplot),
        ("correlation_heatmap",         plot_correlation_heatmap),
        ("products_per_source_bar",     plot_products_per_source_bar),
        ("dashboard_subplots",          plot_dashboard_subplots),
    ]

    for name, fn in charts:
        try:
            paths = fn(df, out_dir=STATIC_OUT)
            results[name] = paths
            print(f"  [static]  {name}")
            print(f"            PNG → {paths['png']}")
            print(f"            PDF → {paths['pdf']}")
        except Exception as exc:
            logger.error("Failed to generate '%s': %s", name, exc)
            print(f"  [static]  {name}  FAILED: {exc}")

    return results


def run_interactive_charts(df: pd.DataFrame) -> dict:
    """Generate and save all Plotly interactive charts."""
    from .interactive_charts import (
        interactive_reviews_vs_rating,
        interactive_top_products_bar,
        interactive_avg_rating_per_source,
        interactive_rating_by_source_box,
        interactive_multi_layout,
    )

    INTERACTIVE_OUT.mkdir(parents=True, exist_ok=True)
    results = {}

    charts = [
        ("reviews_vs_rating_interactive",        interactive_reviews_vs_rating),
        ("top_products_bar",                     interactive_top_products_bar),
        ("avg_rating_per_source_line",           interactive_avg_rating_per_source),
        ("rating_by_source_boxplot_interactive", interactive_rating_by_source_box),
        ("interactive_dashboard",                interactive_multi_layout),
    ]

    for name, fn in charts:
        try:
            html_path = fn(df, out_dir=INTERACTIVE_OUT)
            results[name] = html_path
            print(f"  [interactive]  {name}")
            print(f"                 HTML → {html_path}")
        except Exception as exc:
            logger.error("Failed to generate '%s': %s", name, exc)
            print(f"  [interactive]  {name}  FAILED: {exc}")

    return results


def generate_all(data_path: Path = DATA_PATH) -> dict:
    """Full pipeline: load data → static charts → interactive charts."""
    print("\n========================================")
    print("  Lab 12 – Data Visualization Generator")
    print("========================================\n")

    df = load_data(data_path)
    print(f"Dataset: {len(df)} products, {len(df.columns)} columns\n")

    print("── Static charts (matplotlib / seaborn) ──")
    static = run_static_charts(df)

    print("\n── Interactive charts (Plotly Express) ──")
    interactive = run_interactive_charts(df)

    print("\n========================================")
    print(f"  Done!  {len(static)} static + {len(interactive)} interactive")
    print(f"  Static      → {STATIC_OUT}")
    print(f"  Interactive → {INTERACTIVE_OUT}")
    print("========================================\n")

    return {"static": static, "interactive": interactive}