import pandas as pd
import logging

logger = logging.getLogger(__name__)


def select_columns(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    existing = [c for c in cols if c in df.columns]
    logger.info('Selecting %d/%d columns', len(existing), len(cols))
    return df[existing]


def loc_filter(df: pd.DataFrame,
               min_rating: float = 8.0,
               result_cols: list = None) -> pd.DataFrame:
    if result_cols is None:
        result_cols = ['title', 'rating', 'helpful_votes', 'source']
    result_cols = [c for c in result_cols if c in df.columns]
    df_num = df.copy()
    df_num['rating'] = pd.to_numeric(df_num['rating'], errors='coerce')
    mask   = df_num['rating'] >= min_rating
    result = df.loc[mask, result_cols]
    logger.info('loc filter (vote_avg >= %.1f): %d rows', min_rating, len(result))
    return result


def iloc_sample(df: pd.DataFrame, step: int = 100) -> pd.DataFrame:
    result = df.iloc[::step]
    logger.info('iloc sample (step=%d): %d rows', step, len(result))
    return result


def boolean_filter(df: pd.DataFrame,
                   min_rating: float = 4.0,
                   max_rating: float = 5.0) -> pd.DataFrame:
    df_num = df.copy()
    df_num['rating'] = pd.to_numeric(df_num['rating'], errors='coerce')
    mask = (
        (df_num['rating'] >= min_rating) &
        (df_num['rating'] <= max_rating)
    )
    result = df[mask]
    logger.info('boolean_filter: %d rows match', len(result))
    return result


def isin_filter(df: pd.DataFrame,
                sources: list = None,
                exclude: bool = False) -> pd.DataFrame:
    if sources is None:
        sources = ['API Source', 'ecommerce_api', 'reviews_page_1.json',
                   'reviews_page_2.json', 'reviews_page_3.json']
    mask = df['source'].isin(sources)
    if exclude:
        mask = ~mask
    result = df[mask]
    logger.info('isin_filter (exclude=%s): %d rows', exclude, len(result))
    return result


def between_filter(df: pd.DataFrame,
                   col: str = 'rating',
                   low: float = 3.0,
                   high: float = 4.5) -> pd.DataFrame:
    df_num = df.copy()
    df_num[col] = pd.to_numeric(df_num[col], errors='coerce')
    result = df[df_num[col].between(low, high)]
    logger.info('between_filter %s [%.1f, %.1f]: %d rows', col, low, high, len(result))
    return result