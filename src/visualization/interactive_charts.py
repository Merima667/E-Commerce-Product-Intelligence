"""
Interactive visualization module for E-Commerce Product Intelligence Pipeline.
Uses Plotly Express (and Graph Objects for the multi-layout chart).
Lab 12 - Data Visualization
"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

INTERACTIVE_OUT = Path("outputs/visualizations/interactive")
TEMPLATE = "plotly_white"


def _save_html(fig: go.Figure, stem: str,
               out_dir: Path = INTERACTIVE_OUT) -> str:
    """Write an interactive HTML file. Returns the file path as string."""
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{stem}.html"
    fig.write_html(str(path))
    logger.info("Saved interactive chart: %s", path)
    return str(path)


# ── 1. Scatter – Review count vs Rating (interactive) ────────────────────────
def interactive_reviews_vs_rating(df: pd.DataFrame,
                                  out_dir: Path = INTERACTIVE_OUT) -> str:
    """Interactive scatter: rating distribution by source."""
    data = df.dropna(subset=["rating"]).copy()

    fig = px.strip(
        data,
        x="source",
        y="rating",
        color="source",
        hover_name="title",
        hover_data={
            "source": True,
            "rating": ":.2f",
            "author": True,
        },
        labels={"source": "Source", "rating": "Rating (0–5)"},
        title="Rating Distribution by Source – Interactive Explorer",
        template=TEMPLATE,
        color_discrete_sequence=px.colors.qualitative.Vivid,
    )
    fig.update_traces(marker=dict(size=5, opacity=0.6))
    fig.update_layout(
        legend_title="Source",
        font=dict(family="Inter", size=13),
        height=580,
        showlegend=False,
    )
    return _save_html(fig, "reviews_vs_rating_interactive", out_dir)


# ── 2. Bar – Top 10 products by rating ───────────────────────────────────────
def interactive_top_products_bar(df: pd.DataFrame, n: int = 10,
                                 out_dir: Path = INTERACTIVE_OUT) -> str:
    """Interactive horizontal bar: top-n products by rating."""
    top = (df[df["rating"] > 0]
           .drop_duplicates("title")
           .nlargest(n, "rating")[["title", "rating", "helpful_votes", "source"]]
           .sort_values("rating", ascending=True))

    fig = px.bar(
        top,
        x="rating",
        y="title",
        orientation="h",
        color="rating",
        color_continuous_scale="Blues",
        hover_name="title",
        hover_data={
            "rating": ":.2f",
            "helpful_votes": True,
            "source": True,
        },
        labels={"rating": "Rating (0–5)", "title": ""},
        title=f"Top {n} Products by Rating",
        template=TEMPLATE,
    )
    fig.update_layout(
        height=500,
        coloraxis_showscale=False,
        font=dict(family="Inter", size=13),
    )
    return _save_html(fig, "top_products_bar", out_dir)


# ── 3. Line – Average rating per source ──────────────────────────────────────
def interactive_avg_rating_per_source(df: pd.DataFrame,
                                      out_dir: Path = INTERACTIVE_OUT) -> str:
    """Interactive line: average rating and product count per source."""
    agg = (df.groupby("source")
             .agg(
                 product_count=("title", "count"),
                 avg_rating=("rating", "mean"),
                 avg_reviews=("helpful_votes", "mean"),
             )
             .reset_index()
             .sort_values("avg_rating", ascending=False))

    fig = px.line(
        agg,
        x="source",
        y="avg_rating",
        markers=True,
        hover_data={
            "avg_rating": ":.2f",
            "product_count": True,
            "avg_reviews": ":.0f",
        },
        labels={"source": "Source", "avg_rating": "Average Rating"},
        title="Average Product Rating per Source",
        template=TEMPLATE,
    )
    fig.update_traces(line_color="#1a6faf", line_width=2.5,
                      marker=dict(size=9))
    fig.update_layout(font=dict(family="Inter", size=13), height=450)
    return _save_html(fig, "avg_rating_per_source_line", out_dir)


# ── 4. Box – Rating by source (interactive) ──────────────────────────────────
def interactive_rating_by_source_box(df: pd.DataFrame,
                                     out_dir: Path = INTERACTIVE_OUT) -> str:
    """Interactive box plot: rating distribution per source."""
    order = (df.groupby("source")["rating"]
               .median().sort_values(ascending=False).index.tolist())

    fig = px.box(
        df,
        x="source",
        y="rating",
        category_orders={"source": order},
        color="source",
        hover_name="title",
        hover_data={"rating": ":.2f", "helpful_votes": True},
        labels={"source": "Source", "rating": "Rating (0–5)"},
        title="Product Rating Distribution by Source",
        template=TEMPLATE,
        color_discrete_sequence=px.colors.qualitative.Vivid,
        points="outliers",
    )
    fig.update_layout(
        showlegend=False,
        font=dict(family="Inter", size=13),
        height=500,
    )
    return _save_html(fig, "rating_by_source_boxplot_interactive", out_dir)


# ── 5. Multi-layout – 2×2 interactive dashboard ──────────────────────────────
def interactive_multi_layout(df: pd.DataFrame,
                              out_dir: Path = INTERACTIVE_OUT) -> str:
    """2×2 Plotly subplot combining ratings, reviews, source counts, scatter."""
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            "Top 10 Products by Rating",
            "Rating Distribution",
            "Products per Source",
            "Reviews vs Rating",
        ),
        vertical_spacing=0.14,
        horizontal_spacing=0.10,
    )

    # Panel 1 – top 10 rating bar
    top10 = (df[df["rating"] > 0]
             .drop_duplicates("title")
             .nlargest(10, "rating")
             .sort_values("rating"))
    fig.add_trace(
        go.Bar(x=top10["rating"], y=top10["title"].str[:40],
               orientation="h",
               marker_color="#1a6faf",
               name="Rating",
               hovertemplate="%{y}<br>Rating: %{x:.2f}<extra></extra>"),
        row=1, col=1,
    )

    # Panel 2 – rating histogram
    fig.add_trace(
        go.Histogram(x=df["rating"].dropna(), nbinsx=20,
                     marker_color="#2ca02c", name="Ratings",
                     hovertemplate="Rating: %{x}<br>Count: %{y}<extra></extra>"),
        row=1, col=2,
    )

    # Panel 3 – products per source bar
    counts = df["source"].value_counts()
    fig.add_trace(
        go.Bar(x=counts.index, y=counts.values,
               marker_color="#ff7f0e", name="Source count",
               hovertemplate="%{x}: %{y} products<extra></extra>"),
        row=2, col=1,
    )

    # Panel 4 – review count vs rating scatter
    scatter_data = df[df["helpful_votes"] > 0]
    fig.add_trace(
        go.Scatter(
            x=scatter_data["helpful_votes"],
            y=scatter_data["rating"],
            mode="markers",
            marker=dict(
                color=scatter_data["rating"],
                colorscale="Viridis",
                showscale=True,
                colorbar=dict(title="Rating", x=1.02, len=0.45, y=0.12),
                size=6, opacity=0.6,
            ),
            text=scatter_data["title"],
            name="Products",
            hovertemplate="%{text}<br>Reviews: %{x}<br>Rating: %{y:.2f}<extra></extra>",
        ),
        row=2, col=2,
    )

    fig.update_layout(
        title_text="E-Commerce Product Intelligence Dashboard",
        title_font=dict(size=18),
        template=TEMPLATE,
        height=700,
        width=1100,
        showlegend=False,
        font=dict(family="Inter", size=11),
    )
    fig.update_xaxes(title_text="Rating", row=1, col=1)
    fig.update_xaxes(title_text="Rating", row=1, col=2)
    fig.update_xaxes(title_text="Source", row=2, col=1)
    fig.update_xaxes(title_text="Number of Reviews (log)", type="log", row=2, col=2)
    fig.update_yaxes(title_text="Count", row=1, col=2)
    fig.update_yaxes(title_text="Count", row=2, col=1)
    fig.update_yaxes(title_text="Rating", row=2, col=2)

    return _save_html(fig, "interactive_dashboard", out_dir)