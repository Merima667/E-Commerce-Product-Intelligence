import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def missing_value_report(df: pd.DataFrame) -> pd.DataFrame:
    missing     = df.isna().sum()
    missing_pct = (missing / len(df) * 100).round(2)
    report = pd.DataFrame({
        'missing_count': missing,
        'missing_pct':   missing_pct,
    }).sort_values('missing_count', ascending=False)
    report = report[report['missing_count'] > 0].copy()
    report['severity'] = report['missing_pct'].apply(
        lambda p: 'HIGH' if p > 30 else ('MEDIUM' if p > 10 else 'LOW')
    )
    logger.info('Columns with missing values: %d', len(report))
    return report


def zero_as_missing(df: pd.DataFrame,
                    cols: list = None) -> pd.DataFrame:
    if cols is None:
        cols = [c for c in ['budgeted', 'actual'] if c in df.columns]
    rows = []
    for col in cols:
        n_zero = (df[col].fillna(0) == 0).sum()
        pct    = n_zero / len(df) * 100
        rows.append({
            'column':   col,
            'issue':    'Zero values (likely missing)',
            'count':    int(n_zero),
            'pct':      round(pct, 1),
            'severity': 'HIGH' if pct > 50 else 'MEDIUM',
        })
    logger.info('Zero-as-missing check complete for: %s', cols)
    return pd.DataFrame(rows)


def outlier_report(df: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    rows = []
    for col in numeric_cols:
        series = df[col].dropna()
        if len(series) == 0:
            continue
        q1, q3 = series.quantile(0.25), series.quantile(0.75)
        iqr    = q3 - q1
        if iqr == 0:
            continue
        lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        n_out  = int(((series < lower) | (series > upper)).sum())
        rows.append({
            'column': col, 'q1': round(q1, 1), 'q3': round(q3, 1),
            'iqr': round(iqr, 1), 'outliers': n_out,
            'outlier_pct': round(n_out / len(series) * 100, 1),
        })
    logger.info('Outlier detection complete: %d columns checked', len(rows))
    return pd.DataFrame(rows)


def full_quality_report(df: pd.DataFrame) -> pd.DataFrame:
    issues = []
    mv = missing_value_report(df)
    for _, row in mv.iterrows():
        issues.append({'column': row.name, 'issue': 'Missing values',
                       'count': row['missing_count'], 'pct': row['missing_pct'],
                       'severity': row['severity']})

    zm = zero_as_missing(df)
    issues.extend(zm.to_dict('records'))

    if 'rating' in df.columns:
        df_num = df.copy()
        df_num['rating'] = pd.to_numeric(df_num['rating'], errors='coerce')
        out = df_num[(df_num['rating'] < 1) | (df_num['rating'] > 5)]
        if len(out):
            issues.append({'column': 'rating',
                           'issue': 'Values outside 1-5',
                           'count': len(out),
                           'pct': round(len(out)/len(df)*100, 1),
                           'severity': 'HIGH'})

    if 'id' in df.columns:
        n_dup = int(df['id'].duplicated().sum())
        if n_dup:
            issues.append({'column': 'id', 'issue': 'Duplicate IDs',
                           'count': n_dup,
                           'pct': round(n_dup/len(df)*100, 1),
                           'severity': 'HIGH'})

    if 'title' in df.columns:
        n_empty = int((df['title'].str.strip().str.len() < 1).sum())
        if n_empty:
            issues.append({'column': 'title', 'issue': 'Empty title',
                           'count': n_empty,
                           'pct': round(n_empty/len(df)*100, 1),
                           'severity': 'MEDIUM'})

    quality_df = pd.DataFrame(issues)
    if not quality_df.empty:
        quality_df = quality_df.sort_values(
            ['severity', 'pct'], ascending=[True, False])
    logger.info('Quality report: %d issues found', len(quality_df))
    return quality_df


def save_missing_heatmap(df: pd.DataFrame, output_path: str) -> None:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    cols_with_missing = df.columns[df.isna().any()].tolist()
    if not cols_with_missing:
        logger.info('No missing values — heatmap skipped')
        return
    sample = df[cols_with_missing].sample(min(200, len(df)), random_state=42)
    plt.figure(figsize=(12, 5))
    plt.imshow(sample.isna().T, aspect='auto', cmap='RdYlGn_r',
               interpolation='nearest')
    plt.colorbar(label='Missing (1=yes, 0=no)')
    plt.yticks(range(len(cols_with_missing)), cols_with_missing, fontsize=9)
    plt.xlabel(f'Sample rows (n={len(sample)})')
    plt.title('Missing Value Pattern – E-Commerce Product Intelligence')
    plt.tight_layout()
    plt.savefig(output_path, dpi=120, bbox_inches='tight')
    plt.close()
    logger.info('Saved missing heatmap → %s', output_path)