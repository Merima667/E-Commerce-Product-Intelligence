
"""
src/visualization/__init__.py
Re-exports all chart functions from a single import location.
"""

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

from .interactive_charts import (
    interactive_reviews_vs_rating,
    interactive_top_products_bar,
    interactive_avg_rating_per_source,
    interactive_rating_by_source_box,
    interactive_multi_layout,
)

__all__ = [
    # static
    "plot_top_products_by_rating",
    "plot_avg_rating_per_source",
    "plot_reviews_vs_rating_scatter",
    "plot_rating_distribution",
    "plot_rating_by_source_boxplot",
    "plot_correlation_heatmap",
    "plot_products_per_source_bar",
    "plot_dashboard_subplots",
    # interactive
    "interactive_reviews_vs_rating",
    "interactive_top_products_bar",
    "interactive_avg_rating_per_source",
    "interactive_rating_by_source_box",
    "interactive_multi_layout",
]