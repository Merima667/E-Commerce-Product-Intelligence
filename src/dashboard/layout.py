"""
Page layout for the E-Commerce Product Intelligence Dashboard.
Uses dash-bootstrap-components for the responsive grid.
"""
import dash_bootstrap_components as dbc
from dash import dcc, html

from .data_access import (
    COL_RATING, COL_SOURCE, COL_CATEGORY, COL_TITLE,
    get_sources, get_categories, load_products_df,
)

_df       = load_products_df()
_sources  = get_sources(_df)
_cats     = get_categories(_df)

SOURCE_OPTIONS   = [{"label": s, "value": s} for s in _sources]
CATEGORY_OPTIONS = [{"label": c, "value": c} for c in _cats]


def create_layout() -> dbc.Container:
    """Return the full page layout as a Bootstrap Container."""
    return dbc.Container(
        fluid=True,
        style={"backgroundColor": "#0a1628", "minHeight": "100vh", "padding": "20px"},
        children=[
            # ── Header ───────────────────────────────────────────────
            dbc.Row(
                dbc.Col(
                    html.Div([
                        html.H1(
                            "E-Commerce Product Intelligence Dashboard",
                            style={"color": "#ffffff", "fontWeight": "700", "marginBottom": "4px"},
                        ),
                        html.P(
                            "Interactive exploration of product reviews and ratings",
                            style={"color": "#8899aa", "fontSize": "14px"},
                        ),
                    ]),
                    width=12,
                ),
                className="mb-4",
            ),

            # ── KPI Cards ────────────────────────────────────────────
            dbc.Row(
                id="kpi-row",
                children=_build_kpi_cards(_df),
                className="mb-4",
            ),

            # ── Filter Bar ───────────────────────────────────────────
            dbc.Row([
                dbc.Col([
                    html.Label("Source", style={"color": "#8899aa", "fontSize": "12px"}),
                    dcc.Dropdown(
                        id="source-filter",
                        options=[{"label": "All Sources", "value": ""}] + SOURCE_OPTIONS,
                        value="",
                        clearable=False,
                        style={"backgroundColor": "#1a2840", "color": "#000"},
                    ),
                ], width=3),

                dbc.Col([
                    html.Label("Category", style={"color": "#8899aa", "fontSize": "12px"}),
                    dcc.Dropdown(
                        id="category-filter",
                        options=[{"label": "All Categories", "value": ""}] + CATEGORY_OPTIONS,
                        value="",
                        clearable=False,
                        style={"backgroundColor": "#1a2840", "color": "#000"},
                    ),
                ], width=3),

                dbc.Col([
                    html.Label("Search Title", style={"color": "#8899aa", "fontSize": "12px"}),
                    dcc.Input(
                        id="search-input",
                        type="text",
                        placeholder="Type a product title...",
                        debounce=True,
                        style={
                            "width": "100%", "padding": "8px",
                            "backgroundColor": "#1a2840",
                            "border": "1px solid #2a3850",
                            "color": "#ffffff", "borderRadius": "4px",
                        },
                    ),
                ], width=4),
            ], className="mb-4", style={
                "backgroundColor": "#112236", "padding": "16px", "borderRadius": "8px",
            }),

            # ── Main Charts Row ───────────────────────────────────────
            dbc.Row([
                dbc.Col(
                    dbc.Card([
                        dbc.CardHeader(
                            "Top 10 Products by Rating",
                            style={"backgroundColor": "#112236", "color": "#ffffff"},
                        ),
                        dbc.CardBody(dcc.Graph(id="revenue-chart", config={"displayModeBar": False})),
                    ], style={"backgroundColor": "#112236", "border": "1px solid #1e3a5f"}),
                    width=6,
                ),
                dbc.Col(
                    dbc.Card([
                        dbc.CardHeader(
                            "Rating Distribution by Source",
                            style={"backgroundColor": "#112236", "color": "#ffffff"},
                        ),
                        dbc.CardBody(dcc.Graph(id="rating-chart", config={"displayModeBar": False})),
                    ], style={"backgroundColor": "#112236", "border": "1px solid #1e3a5f"}),
                    width=6,
                ),
            ], className="mb-4"),

            # ── Second Charts Row ─────────────────────────────────────
            dbc.Row([
                dbc.Col(
                    dbc.Card([
                        dbc.CardHeader(
                            "Rating Distribution Histogram",
                            style={"backgroundColor": "#112236", "color": "#ffffff"},
                        ),
                        dbc.CardBody(dcc.Graph(id="scatter-chart", config={"displayModeBar": False})),
                    ], style={"backgroundColor": "#112236", "border": "1px solid #1e3a5f"}),
                    width=6,
                ),
                dbc.Col(
                    dbc.Card([
                        dbc.CardHeader(
                            "Products per Source",
                            style={"backgroundColor": "#112236", "color": "#ffffff"},
                        ),
                        dbc.CardBody(dcc.Graph(id="trend-chart", config={"displayModeBar": False})),
                    ], style={"backgroundColor": "#112236", "border": "1px solid #1e3a5f"}),
                    width=6,
                ),
            ], className="mb-4"),

            # ── Live Ticker ───────────────────────────────────────────
            dbc.Row(
                dbc.Col(
                    dbc.Card([
                        dbc.CardHeader(
                            "Live Rating Ticker (simulated stream)",
                            style={"backgroundColor": "#112236", "color": "#ffffff"},
                        ),
                        dbc.CardBody([
                            dcc.Graph(id="live-chart", config={"displayModeBar": False}),
                            dcc.Interval(
                                id="live-interval",
                                interval=3000,
                                n_intervals=0,
                            ),
                        ]),
                    ], style={"backgroundColor": "#112236", "border": "1px solid #1e3a5f"}),
                    width=12,
                ),
            ),
        ],
    )


# ─────────────────────────────────────────────────────────────
# Private helpers
# ─────────────────────────────────────────────────────────────

def _build_kpi_cards(df) -> list:
    """Return four KPI metric cards for the header row."""
    total    = len(df)
    avg_rate = df[COL_RATING].mean() if COL_RATING in df.columns else 0
    sources  = df[COL_SOURCE].nunique() if COL_SOURCE in df.columns else 0
    cats     = df[COL_CATEGORY].nunique() if COL_CATEGORY in df.columns else 0

    cards = [
        ("Total Reviews",    f"{total:,}",        "#4a9eff"),
        ("Avg Rating",       f"{avg_rate:.2f}",   "#34d399"),
        ("Unique Sources",   str(sources),         "#f59e0b"),
        ("Unique Categories", str(cats),           "#a78bfa"),
    ]

    return [
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H5(label, style={"color": "#8899aa", "fontSize": "13px", "marginBottom": "4px"}),
                    html.H2(value, style={"color": color, "fontWeight": "700", "marginBottom": "0"}),
                ]),
                style={"backgroundColor": "#112236", "border": f"1px solid {color}"},
            ),
            width=3,
        )
        for label, value, color in cards
    ]