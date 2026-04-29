import pandas as pd
import logging
from pathlib import Path

from cleaning.missing_handler import (
    drop_rows_missing_title,
    fill_missing_comment,
    replace_zero_with_nan,
    fill_numeric_with_median,
)
from cleaning.string_cleaner import (
    clean_title,
    clean_language_code,
    clean_comment_text,
    extract_year_from_release_date,
)
from cleaning.deduplicator import (
    drop_exact_duplicates,
    drop_duplicate_ids,
)
from cleaning.type_converter import (
    convert_dates,
    convert_numeric_columns,
    convert_category_columns,
)
from cleaning.validator import run_all_validations

logger = logging.getLogger(__name__)
OUTPUT_PATH = Path('../../data/processed/cleaned/movies_clean.csv')


def run_cleaning_pipeline(df_raw: pd.DataFrame, save: bool = True) -> pd.DataFrame:
    logger.info('=== Starting cleaning pipeline: %d rows ===', len(df_raw))
    df = df_raw.copy()

    logger.info('Step 1: drop rows missing title')
    df = drop_rows_missing_title(df)

    logger.info('Step 2: replace zero-as-missing in financial columns')
    df = replace_zero_with_nan(df, columns=['budgeted', 'actual'])

    logger.info('Step 3: drop exact duplicates')
    df = drop_exact_duplicates(df)

    logger.info('Step 4: drop duplicate IDs')
    df = drop_duplicate_ids(df, id_col='review_id')

    logger.info('Step 5: clean string columns')
    df = clean_title(df)
    df = clean_language_code(df)
    df = clean_comment_text(df)

    logger.info('Step 6: extract release year from date string')
    df = extract_year_from_release_date(df)

    logger.info('Step 7: fill missing comments')
    df = fill_missing_comment(df)

    logger.info('Step 8: fill numeric NaN with median')
    df = fill_numeric_with_median(
    df, columns=['rating', 'helpful_votes']
)
    df = fill_numeric_with_median(
    df, columns=['budgeted', 'actual']
)

    logger.info('Step 9: convert data types')
    df = convert_dates(df)
    df = convert_numeric_columns(df)
    df = convert_category_columns(df)

    logger.info('Step 10: run validation')
    run_all_validations(df)

    if save:
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(OUTPUT_PATH, index=False)
        logger.info('Saved clean dataset to %s (%d rows)', OUTPUT_PATH, len(df))

    logger.info('=== Cleaning pipeline complete: %d rows remain ===', len(df))
    return df