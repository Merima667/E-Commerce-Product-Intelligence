"""
src/analytics/explorer.py
Exploratory data analysis functions for the TMDB DataFrame.
"""
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')   
import matplotlib.pyplot as plt
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def inspect_shape(df: pd.DataFrame) -> dict:
    info = {
        'rows':    df.shape[0],
        'columns': df.shape[1],
        'cells':   df.size,
        'column_names': df.columns.tolist(),
    }
    logger.info('Shape: %d rows × %d columns', info['rows'], info['columns'])
    return info


def print_info(df: pd.DataFrame) -> None:
    df.info(memory_usage='deep')


def describe_numeric(df: pd.DataFrame) -> pd.DataFrame:
    return df.describe()


def value_counts_report(df: pd.DataFrame, cols: list = None, top_n: int = 15) -> dict:
    if cols is None:
        cols = [c for c in ['source', 'status', 'category', 'verified'] if c in df.columns]
    report = {}
    for col in cols:
        counts  = df[col].value_counts().head(top_n)
        n_unique = df[col].nunique()
        report[col] = {'counts': counts, 'nunique': n_unique}
        logger.info('%s: %d unique values', col, n_unique)
    return report


def extract_review_year(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if 'date' in df.columns:
        df['review_year'] = pd.to_datetime(
            df['date'], errors='coerce').dt.year
        logger.info('Extracted review_year: %d non-null',
                    df['review_year'].notna().sum())
    return df


def plot_distributions(df: pd.DataFrame, output_path: str) -> None:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    fig.suptitle('E-Commerce Product Intelligence – Distributions', fontsize=14, fontweight='bold')

    if 'rating' in df.columns:
        pd.to_numeric(df['rating'], errors='coerce').dropna().plot(
            kind='hist', bins=30, ax=axes[0, 0],
            color='steelblue', edgecolor='white')
        axes[0, 0].set_title('Rating Distribution')
        axes[0, 0].set_xlabel('Rating')

    if 'helpful_votes' in df.columns:
        pd.to_numeric(df['helpful_votes'], errors='coerce').dropna().plot(
            kind='hist', bins=30, ax=axes[0, 1],
            color='teal', edgecolor='white')
        axes[0, 1].set_title('Helpful Votes Distribution (log scale)')
        axes[0, 1].set_xlabel('Helpful Votes')

    if 'source' in df.columns:
        top_langs = df['source'].value_counts().head(10)
        top_langs.plot(kind='bar', ax=axes[1, 0], color='coral', edgecolor='white')
        axes[1, 0].set_title('Top 10 Sources')
        axes[1, 0].tick_params(axis='x', rotation=45)

    if 'review_year' in df.columns:
        year_counts = df['review_year'].dropna().value_counts().sort_index()
        year_counts.plot(kind='line', ax=axes[1, 1], color='purple', linewidth=2)
        axes[1, 1].set_title('Review per Year')
        axes[1, 1].set_xlabel('Year')

    plt.tight_layout()
    plt.savefig(output_path, dpi=120, bbox_inches='tight')
    plt.close()
    logger.info('Saved distribution chart → %s', output_path)