"""
Static visualization module for E-Commerce Product Intelligence Pipeline.
Uses matplotlib (object-oriented API) and seaborn for all static charts.
Lab 12 - Data Visualization
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import pandas as pd
import numpy as np
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Global style ──────────────────────────────────────────────────────────────
sns.set_theme(style="whitegrid")
sns.set_context("notebook")
sns.set_palette("viridis")

STATIC_OUT = Path("outputs/visualizations/static")


def _save(fig: plt.Figure, stem: str, out_dir: Path = STATIC_OUT) -> dict:
    """Save figure as PNG (300 dpi) and PDF. Returns dict of saved paths."""
    out_dir.mkdir(parents=True, exist_ok=True)
    png_path = out_dir / f"{stem}.png"
    pdf_path = out_dir / f"{stem}.pdf"
    fig.savefig(png_path, dpi=300, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved %s  (PNG + PDF)", stem)
    return {"png": str(png_path), "pdf": str(pdf_path)}


# ── 1. Bar chart – Top 10 products by rating ─────────────────────────────────
def plot_top_products_by_rating(df: pd.DataFrame, n: int = 10,
                                out_dir: Path = STATIC_OUT) -> dict:
    """Horizontal bar chart: top-n products by average rating."""
    top = (df[df["rating"] > 0]
           .nlargest(n, "rating")[["title", "rating"]]
           .drop_duplicates("title")
           .sort_values("rating"))

    fig, ax = plt.subplots(figsize=(10, 6))
    colors = sns.color_palette("viridis", n)
    bars = ax.barh(top["title"].str[:50], top["rating"], color=colors)

    for bar in bars:
        ax.text(bar.get_width() + 0.02, bar.get_y() + bar.get_height() / 2,
                f"{bar.get_width():.2f}", va="center", fontsize=9)

    ax.set_xlabel("Average Rating (0–5)", fontsize=11)
    ax.set_title(f"Top {n} Products by Rating", fontsize=14, fontweight="bold")
    ax.set_xlim(0, top["rating"].max() + 0.5)
    fig.tight_layout()

    return _save(fig, "top_products_by_rating", out_dir)


# ── 2. Dual-axis chart – Product count and avg rating per source ──────────────
def plot_avg_rating_per_source(df: pd.DataFrame,
                               out_dir: Path = STATIC_OUT) -> dict:
    """Bar + line dual-axis: product count and mean rating per source."""
    agg = (df.groupby("source")
             .agg(avg_rating=("rating", "mean"),
                  product_count=("title", "count"))
             .reset_index()
             .sort_values("avg_rating", ascending=True))

    fig, ax1 = plt.subplots(figsize=(12, 5))
    color_bar  = "#a8d5e2"
    color_line = "#1a6faf"

    x = range(len(agg))
    ax1.bar(x, agg["product_count"], color=color_bar, alpha=0.5, label="Product Count")
    ax1.set_ylabel("Number of Products", color=color_bar, fontsize=11)
    ax1.tick_params(axis="y", labelcolor=color_bar)
    ax1.set_xticks(list(x))
    ax1.set_xticklabels(agg["source"], rotation=30, ha="right")

    ax2 = ax1.twinx()
    ax2.plot(list(x), agg["avg_rating"], color=color_line, linewidth=2.5,
             marker="o", markersize=7, label="Avg. Rating")
    ax2.set_ylabel("Average Rating (0–5)", color=color_line, fontsize=11)
    ax2.tick_params(axis="y", labelcolor=color_line)
    ax2.set_ylim(0, 5.5)

    ax1.set_xlabel("Source", fontsize=11)
    ax1.set_title("Product Count and Average Rating per Source",
                  fontsize=14, fontweight="bold")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=9)

    fig.tight_layout()
    return _save(fig, "avg_rating_per_source_dual", out_dir)


# ── 3. Scatter plot – Review count vs Rating ──────────────────────────────────
def plot_reviews_vs_rating_scatter(df: pd.DataFrame,
                                   out_dir: Path = STATIC_OUT) -> dict:
    """Scatter: number of reviews vs rating, coloured by source."""
    data = df.dropna(subset=["rating"]).copy()
    data["helpful_votes"] = data["helpful_votes"].fillna(0)
    data = data[data["helpful_votes"] >= 0]

    fig, ax = plt.subplots(figsize=(11, 7))
    sns.scatterplot(data=data, x="helpful_votes", y="rating",
                    hue="source", palette="viridis",
                    alpha=0.65, ax=ax, s=40)

    if data["helpful_votes"].nunique() > 1:
        z = np.polyfit(np.log1p(data["helpful_votes"]), data["rating"], 1)
        p = np.poly1d(z)
        xs = np.linspace(data["helpful_votes"].min(), data["helpful_votes"].max(), 300)
        ax.plot(xs, p(np.log1p(xs)), "--", color="gray",
                linewidth=1.5, label="Trend (log)")

    ax.set_xscale("log")
    ax.set_xlabel("Number of Reviews (log scale)", fontsize=11)
    ax.set_ylabel("Average Rating (0–5)", fontsize=11)
    ax.set_title("Review Count vs Rating – Coloured by Source",
                 fontsize=14, fontweight="bold")
    ax.legend(title="Source", bbox_to_anchor=(1.02, 1), loc="upper left",
              fontsize=9, title_fontsize=9)
    fig.tight_layout()

    return _save(fig, "reviews_vs_rating_scatter", out_dir)


# ── 4. Histogram – Distribution of ratings ───────────────────────────────────
def plot_rating_distribution(df: pd.DataFrame,
                             out_dir: Path = STATIC_OUT) -> dict:
    """Histogram + KDE of product ratings."""
    fig, ax = plt.subplots(figsize=(9, 5))
    sns.histplot(df["rating"].dropna(), bins=20, kde=True,
                 color="#1a6faf", edgecolor="white", ax=ax)
    ax.axvline(df["rating"].mean(), color="firebrick",
               linestyle="--", linewidth=1.8,
               label=f"Mean = {df['rating'].mean():.2f}")
    ax.axvline(df["rating"].median(), color="darkorange",
               linestyle="-.", linewidth=1.8,
               label=f"Median = {df['rating'].median():.2f}")
    ax.set_xlabel("Rating (0–5)", fontsize=11)
    ax.set_ylabel("Number of Products", fontsize=11)
    ax.set_title("Distribution of Product Ratings", fontsize=14, fontweight="bold")
    ax.legend(fontsize=10)
    fig.tight_layout()

    return _save(fig, "rating_distribution", out_dir)


# ── 5. Box plot – Rating by source ───────────────────────────────────────────
def plot_rating_by_source_boxplot(df: pd.DataFrame,
                                  out_dir: Path = STATIC_OUT) -> dict:
    """Box-and-whisker plot: rating distribution per data source."""
    order = (df.groupby("source")["rating"]
               .median().sort_values(ascending=False).index.tolist())

    fig, ax = plt.subplots(figsize=(12, 6))
    sns.boxplot(data=df, x="source", y="rating",
                order=order, hue="source", palette="viridis",
                legend=False, ax=ax)
    ax.set_xlabel("Source", fontsize=11)
    ax.set_ylabel("Rating (0–5)", fontsize=11)
    ax.set_title("Product Rating Distribution by Source",
                 fontsize=14, fontweight="bold")
    ax.tick_params(axis="x", rotation=30)
    fig.tight_layout()

    return _save(fig, "rating_by_source_boxplot", out_dir)


# ── 6. Heatmap – Correlation matrix ──────────────────────────────────────────
def plot_correlation_heatmap(df: pd.DataFrame,
                             out_dir: Path = STATIC_OUT) -> dict:
    """seaborn heatmap of numeric feature correlations."""
    numeric_cols = ["rating", "helpful_votes"]
    extra = [c for c in df.select_dtypes(include="number").columns
             if c not in numeric_cols]
    corr_cols = numeric_cols + extra[:3]
    corr_cols = [c for c in corr_cols if c in df.columns]
    corr = df[corr_cols].corr()

    fig, ax = plt.subplots(figsize=(8, 6))
    sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm",
                center=0, vmin=-1, vmax=1, square=True,
                linewidths=0.5, ax=ax, annot_kws={"size": 10})
    ax.set_title("Correlation Matrix – Product Numeric Features",
                 fontsize=13, fontweight="bold")
    fig.tight_layout()

    return _save(fig, "correlation_heatmap", out_dir)


# ── 7. Bar chart – Product count per source ──────────────────────────────────
def plot_products_per_source_bar(df: pd.DataFrame,
                                 out_dir: Path = STATIC_OUT) -> dict:
    """Vertical bar chart: number of products per data source."""
    counts = df["source"].value_counts()

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.bar(counts.index, counts.values,
                  color=sns.color_palette("viridis", len(counts)))
    for bar in bars:
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.5,
                str(int(bar.get_height())),
                ha="center", va="bottom", fontsize=9)

    ax.set_xlabel("Source", fontsize=11)
    ax.set_ylabel("Number of Products", fontsize=11)
    ax.set_title("Number of Products per Source",
                 fontsize=14, fontweight="bold")
    ax.tick_params(axis="x", rotation=30)
    fig.tight_layout()

    return _save(fig, "products_per_source_bar", out_dir)


# ── 8. Subplot layout – 2×2 dashboard ────────────────────────────────────────
def plot_dashboard_subplots(df: pd.DataFrame,
                            out_dir: Path = STATIC_OUT) -> dict:
    """2×2 multi-panel matplotlib figure combining four key charts."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("E-Commerce Product Intelligence Dashboard",
                 fontsize=16, fontweight="bold", y=1.01)

    # Panel A – top 10 products by rating
    top = (df[df["rating"] > 0]
           .drop_duplicates("title")
           .nlargest(10, "rating")[["title", "rating"]]
           .sort_values("rating"))
    axes[0, 0].barh(top["title"].str[:35], top["rating"],
                    color=sns.color_palette("viridis", 10))
    axes[0, 0].set_title("Top 10 Products by Rating", fontsize=11, fontweight="bold")
    axes[0, 0].set_xlabel("Rating")

    # Panel B – rating distribution
    sns.histplot(df["rating"].dropna(), bins=15, kde=True,
                 color="#1a6faf", ax=axes[0, 1])
    axes[0, 1].set_title("Rating Distribution", fontsize=11, fontweight="bold")
    axes[0, 1].set_xlabel("Rating")
    axes[0, 1].set_ylabel("Count")

    # Panel C – products per source
    counts = df["source"].value_counts().head(8)
    axes[1, 0].bar(counts.index, counts.values,
                   color=sns.color_palette("viridis", len(counts)))
    axes[1, 0].set_title("Products per Source (top 8)", fontsize=11, fontweight="bold")
    axes[1, 0].set_xlabel("Source")
    axes[1, 0].set_ylabel("Count")
    axes[1, 0].tick_params(axis="x", rotation=30)

    # Panel D – review count vs rating scatter
    data = df.dropna(subset=["helpful_votes"]).copy()
    sc = axes[1, 1].scatter(data["helpful_votes"], data["rating"],
                            c=data["rating"], cmap="viridis",
                            alpha=0.5, edgecolors="none", s=20)
    axes[1, 1].set_xscale("log")
    axes[1, 1].set_title("Reviews vs Rating", fontsize=11, fontweight="bold")
    axes[1, 1].set_xlabel("Number of Reviews (log)")
    axes[1, 1].set_ylabel("Rating")
    fig.colorbar(sc, ax=axes[1, 1], label="Rating")

    for a in axes.flat:
        sns.despine(ax=a)

    fig.tight_layout()
    return _save(fig, "dashboard_subplots", out_dir)