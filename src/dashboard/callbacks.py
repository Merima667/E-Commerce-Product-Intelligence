"""
All Dash callbacks for the E-Commerce Product Intelligence Dashboard.
Three categories:
  1. Filter callbacks — update charts from dropdowns / search.
  2. Live callback   — update the ticker chart on every dcc.Interval tick.
"""
import collections
import random
import time

import plotly.express as px
import plotly.graph_objects as go
from dash import Input, Output

from .data_access import (
    COL_RATING, COL_SOURCE, COL_CATEGORY, COL_TITLE, COL_AUTHOR,
    filter_products, load_products_df,
)

_DF = load_products_df()

# Bounded circular buffer for the live ticker (last 60 ticks).
_LIVE_BUFFER = collections.deque(maxlen=60)

DARK_TEMPLATE = "plotly_dark"
CHART_BG      = "#112236"
_MARGIN       = dict(l=10, r=10, t=10, b=10)


def register_callbacks(app):
    """Register all callbacks on the given Dash app instance."""

    # ── Callback 1: Top products by rating bar chart ───────────────────
    @app.callback(
        Output("revenue-chart", "figure"),
        Input("source-filter", "value"),
        Input("category-filter", "value"),
        Input("search-input", "value"),
    )
    def update_revenue_chart(source, category, search):
        """
        Horizontal bar of top 10 products by average rating after filters.
        Horizontal orientation chosen because product titles are long strings
        that read more naturally left-to-right (Tufte: match chart to data).
        """
        df = filter_products(_DF, source or None, category or None, search)

        if COL_RATING not in df.columns or df.empty:
            return _empty_figure("Rating data not available.")

        top10 = (
            df.dropna(subset=[COL_RATING, COL_TITLE])
            .drop_duplicates(COL_TITLE)
            .nlargest(10, COL_RATING)[[COL_TITLE, COL_RATING]]
            .sort_values(COL_RATING)
        )

        if top10.empty:
            return _empty_figure("No products match the current filters.")

        fig = px.bar(
            top10,
            x=COL_RATING,
            y=COL_TITLE,
            orientation="h",
            text=top10[COL_RATING].map(lambda v: f"{v:.1f} ★"),
            color=COL_RATING,
            color_continuous_scale="Blues",
            template=DARK_TEMPLATE,
            labels={COL_RATING: "Rating (0–5)", COL_TITLE: ""},
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(
            paper_bgcolor=CHART_BG,
            plot_bgcolor=CHART_BG,
            coloraxis_showscale=False,
            margin=_MARGIN,
            yaxis={"categoryorder": "total ascending"},
        )
        return fig

    # ── Callback 2: Rating distribution box plot by source ─────────────
    @app.callback(
        Output("rating-chart", "figure"),
        Input("source-filter", "value"),
        Input("category-filter", "value"),
        Input("search-input", "value"),
    )
    def update_rating_chart(source, category, search):
        """
        Box plot of ratings grouped by source.
        Box plots show the full distribution (median, IQR, outliers) rather
        than just the mean — correct choice for comparing rating spreads
        across different data sources.
        """
        df = filter_products(_DF, source or None, category or None, search)

        if COL_RATING not in df.columns or df.empty:
            return _empty_figure("Rating data not available.")

        fig = px.box(
            df,
            x=COL_SOURCE,
            y=COL_RATING,
            color=COL_SOURCE,
            template=DARK_TEMPLATE,
            labels={COL_RATING: "Rating (0–5)", COL_SOURCE: ""},
            points="outliers",
        )
        fig.update_layout(
            paper_bgcolor=CHART_BG,
            plot_bgcolor=CHART_BG,
            showlegend=False,
            margin=_MARGIN,
        )
        return fig

    # ── Callback 3: Rating distribution histogram ──────────────────────
    @app.callback(
        Output("scatter-chart", "figure"),
        Input("source-filter", "value"),
        Input("category-filter", "value"),
        Input("search-input", "value"),
    )
    def update_scatter_chart(source, category, search):
        """
        Histogram of rating distribution across all filtered products.
        Histograms are the correct choice for showing frequency distributions
        of a single continuous variable such as rating.
        """
        df = filter_products(_DF, source or None, category or None, search)

        if COL_RATING not in df.columns or df.empty:
            return _empty_figure("Rating data not available.")

        plot_df = df.dropna(subset=[COL_RATING])

        if plot_df.empty:
            return _empty_figure("No data with valid ratings.")

        fig = px.histogram(
            plot_df,
            x=COL_RATING,
            nbins=20,
            color_discrete_sequence=["#4a9eff"],
            template=DARK_TEMPLATE,
            labels={COL_RATING: "Rating (0–5)", "count": "Number of Reviews"},
        )
        fig.update_layout(
            paper_bgcolor=CHART_BG,
            plot_bgcolor=CHART_BG,
            margin=_MARGIN,
            bargap=0.05,
        )
        return fig

    # ── Callback 4: Products per source bar chart ──────────────────────
    @app.callback(
        Output("trend-chart", "figure"),
        Input("source-filter", "value"),
        Input("category-filter", "value"),
        Input("search-input", "value"),
    )
    def update_trend_chart(source, category, search):
        """
        Bar chart of review counts per source file.
        Bar charts are the correct choice for comparing discrete categories
        (sources) by a single numeric measure (count).
        """
        df = filter_products(_DF, source or None, category or None, search)

        if COL_SOURCE not in df.columns or df.empty:
            return _empty_figure("Source data not available.")

        counts = df.groupby(COL_SOURCE).size().reset_index(name="count")
        fig = px.bar(
            counts,
            x=COL_SOURCE,
            y="count",
            color="count",
            color_continuous_scale="Blues",
            template=DARK_TEMPLATE,
            labels={COL_SOURCE: "Source", "count": "Number of Reviews"},
        )
        fig.update_layout(
            paper_bgcolor=CHART_BG,
            plot_bgcolor=CHART_BG,
            coloraxis_showscale=False,
            margin=_MARGIN,
        )
        return fig

    # ── Callback 5: Live ticker ────────────────────────────────────────
    @app.callback(
        Output("live-chart", "figure"),
        Input("live-interval", "n_intervals"),
    )
    def update_live_chart(n):
        """
        Simulates a live data stream by appending a new point on every
        dcc.Interval tick (every 3 seconds).

        In production this would read from a WebSocket or a time-series
        database. Here random.gauss() simulates a fluctuating average
        product rating metric.

        collections.deque(maxlen=60) keeps memory bounded: new points
        automatically push out the oldest ones.
        """
        _LIVE_BUFFER.append({
            "tick":  n,
            "value": 4.0 + random.gauss(0, 0.3) + 0.05 * (n % 10),
            "ts":    time.strftime("%H:%M:%S"),
        })

        ticks  = [p["tick"]  for p in _LIVE_BUFFER]
        values = [p["value"] for p in _LIVE_BUFFER]
        labels = [p["ts"]    for p in _LIVE_BUFFER]

        fig = go.Figure(
            go.Scatter(
                x=ticks,
                y=values,
                mode="lines+markers",
                line=dict(color="#34d399", width=2),
                marker=dict(size=4, color="#34d399"),
                hovertext=labels,
                hoverinfo="text+y",
                fill="tozeroy",
                fillcolor="rgba(52,211,153,0.1)",
            )
        )
        fig.update_layout(
            template=DARK_TEMPLATE,
            paper_bgcolor=CHART_BG,
            plot_bgcolor=CHART_BG,
            xaxis_title="Tick",
            yaxis_title="Simulated Avg Rating",
            yaxis=dict(range=[2.5, 5.5]),
            margin=_MARGIN,
            height=200,
        )
        return fig


# ─────────────────────────────────────────────────────────────
# Private helpers
# ─────────────────────────────────────────────────────────────

def _empty_figure(message: str) -> go.Figure:
    """Return a blank dark figure with a centred annotation."""
    fig = go.Figure()
    fig.update_layout(
        template=DARK_TEMPLATE,
        paper_bgcolor=CHART_BG,
        plot_bgcolor=CHART_BG,
        annotations=[dict(
            text=message, showarrow=False,
            xref="paper", yref="paper",
            x=0.5, y=0.5, font=dict(color="#8899aa"),
        )],
    )
    return fig