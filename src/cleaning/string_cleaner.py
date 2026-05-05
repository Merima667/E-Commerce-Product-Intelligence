import re
import pandas as pd
import logging
logger = logging.getLogger(__name__)

RE_MULTI_SPACE = re.compile(r'\s+')
RE_YEAR_PARENS = re.compile(r'\(\d{4}\)')
RE_SPECIAL_CHARS = re.compile(r'[^\w\s\-\'\"\.,!?:]')

def clean_title(df: pd.DataFrame) -> pd.DataFrame:
    if 'title' not in df.columns:
        return df
    before = df['title'].copy()
    df['title'] = (
        df['title']
        .str.strip()
        .str.replace(RE_MULTI_SPACE, ' ', regex=True)
        .str.replace(RE_YEAR_PARENS, '', regex=True)
        .str.strip()   
    )
    changed = (before != df['title']).sum()
    logger.info('clean_title: %d titles modified', changed)
    return df

def clean_language_code(df: pd.DataFrame) -> pd.DataFrame:
    col = 'source'
    if col not in df.columns:
        return df
    df[col] = df[col].str.strip().str.lower()
    logger.info('clean_language_code: normalised to lowercase')
    return df

def clean_comment_text(df: pd.DataFrame) -> pd.DataFrame:
    if 'comment' not in df.columns:
        return df
    RE_HTML = re.compile(r'<[^>]+>')
    df['comment'] = (
        df['comment']
        .str.strip()
        .str.replace(RE_HTML, ' ', regex=True)
        .str.replace(RE_MULTI_SPACE, ' ', regex=True)
        .str.strip()
    )
    logger.info('clean_comment_text: comment column cleaned')
    return df

def extract_year_from_release_date(df: pd.DataFrame) -> pd.DataFrame:
    if 'date' not in df.columns:
        return df
    
    df['review_year'] = (
        df['date']
        .str.extract(r'^(\d{4})', expand=False)  
        .astype('Int64', errors='ignore')          
    )
    valid_years = df['review_year'].notna().sum()
    logger.info('extract_year_from_release_date: extracted %d years', valid_years)
    return df

def clean_category_string(df: pd.DataFrame, col: str = 'category') -> pd.DataFrame:
    if col not in df.columns:
        return df
    df[col] = (
        df[col]
        .str.strip()
        .str.title()                                
        .str.replace(r'\s*,\s*', ', ', regex=True)  
    )
    logger.info('clean_category_string: category normalised')
    return df