import re
import pandas as pd
import collections
import logging

logger = logging.getLogger(__name__)

_YEAR_IN_TITLE   = re.compile(r'\((\d{4})\)')
_VALID_YEAR      = re.compile(r'\b(19|20)\d{2}\b')
_CATEGORY_NAME   = re.compile(r'\b([A-Z][a-z]+)\b')
_PRODUCT_TERMS   = re.compile(r'\b(great|excellent|perfect|amazing|love|best|quality)\b', re.IGNORECASE)
_REVIEW_ID       = re.compile(r'^R[A-Z0-9]{10,}$')


def extract_year_from_title(titles: pd.Series) -> pd.Series:
    result = titles.str.extract(r'\((\d{4})\)', expand=False)
    logger.info('Titles with year in parentheses: %d', result.notna().sum())
    return result


def filter_titles_starting_with(df: pd.DataFrame, prefix: str = 'The') -> pd.DataFrame:
    pattern = rf'^{re.escape(prefix)}\s'
    mask    = df['title'].str.contains(pattern, na=False)
    result  = df[mask]
    logger.info('Titles starting with "%s": %d', prefix, len(result))
    return result


def extract_number_from_title(df: pd.Series) -> pd.Series:
    df = df.copy()
    df['title_number'] = df['title'].str.extract(r'(\d+)', expand=False)
    count = df['title_number'].notna().sum()
    logger.info('Titles containing a number: %d', count)
    return df


def positive_comment_count(df: pd.DataFrame) -> int:
    if 'comment' not in df.columns:
        return 0
    mask  = df['comment'].str.contains(_PRODUCT_TERMS.pattern,
                                         case=False, na=False, regex=True)
    count = int(mask.sum())
    logger.info('Positive comments : %d', count)
    return count


def short_comments(df: pd.DataFrame, max_chars: int = 20) -> pd.DataFrame:
    if 'comment' not in df.columns:
        return df.iloc[0:0]
    mask   = df['comment'].str.len() < max_chars
    result = df.loc[mask, ['title', 'comment']]
    logger.info('Very short comments (<%d chars): %d', max_chars, len(result))
    return result


def extract_categories(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if 'category' not in df.columns:
        logger.warning('No category column found')
        return df
    df['category_list'] = df['category'].str.findall(r'\b([A-Z][a-zA-Z]+)\b')
    has_categories = df['category_list'].apply(lambda x: isinstance(x, list) and len(x) > 0)
    logger.info('Rows with extracted categories: %d', has_categories.sum())
    return df


def top_categories(df: pd.DataFrame, n: int = 15) -> list:
    if 'category_list' not in df.columns:
        return []
    all_categories: list = []
    df['category_list'].dropna().apply(all_categories.extend)
    return collections.Counter(all_categories).most_common(n)


def validate_review_id(id_str: str) -> bool:
    return bool(_REVIEW_ID.match(str(id_str)))

def detect_invalid_dates(df: pd.DataFrame, col: str = 'date') -> pd.Series:
    if col not in df.columns:
        return pd.Series(dtype=bool)
    pattern = r'^\w+ \d{1,2}, \d{4}$'
    invalid = ~df[col].str.match(pattern, na=False)
    logger.info('detect_invalid_dates: %d invalid dates found', invalid.sum())
    return invalid

def extract_numeric_from_text(series: pd.Series) -> pd.Series:
    result = series.str.extract(r'(\d+\.?\d*)', expand=False)
    logger.info('extract_numeric_from_text: done')
    return pd.to_numeric(result, errors='coerce')

def flag_short_comments(df: pd.DataFrame, col: str = 'comment', min_chars: int = 20) -> pd.Series:
    if col not in df.columns:
        return pd.Series(dtype=bool)
    flag = df[col].str.len() < min_chars
    logger.info('flag_short_comments: %d short comments found', flag.sum())
    return flag