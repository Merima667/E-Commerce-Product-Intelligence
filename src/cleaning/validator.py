import pandas as pd
import logging

logger = logging.getLogger(__name__)

MIN_RATING = 1.0
MAX_RATING = 5.0
MIN_REVIEW_YEAR = 2000
MAX_REVIEW_YEAR = 2026
VALID_SOURCES = {'API Source', 'ecommerce_api', 'reviews_page_1.json',
                 'reviews_page_2.json', 'reviews_page_3.json'}


def validate_no_null_titles(df: pd.DataFrame) -> None:
    assert df['title'].notna().all(), \
        f'Found {df["title"].isna().sum()} null titles'
    assert (df['title'].str.strip() != '').all(), \
        'Found rows with empty string titles'
    logger.info('validate_no_null_titles: PASSED')


def validate_rating_range(df: pd.DataFrame) -> None:
    if 'rating' not in df.columns:
        return
    non_null = pd.to_numeric(df['rating'], errors='coerce').dropna()
    assert non_null.between(MIN_RATING, MAX_RATING).all(), \
        f'rating out of range [{MIN_RATING}, {MAX_RATING}]'
    logger.info('validate_rating_range: PASSED')


def validate_review_year_range(df: pd.DataFrame) -> None:
    if 'review_year' not in df.columns:
        return
    non_null = df['review_year'].dropna()
    assert non_null.between(MIN_REVIEW_YEAR, MAX_REVIEW_YEAR).all(), \
        f'review_year out of range [{MIN_REVIEW_YEAR}, {MAX_REVIEW_YEAR}]'
    logger.info('validate_review_year_range: PASSED')


def validate_no_duplicate_ids(df: pd.DataFrame, id_col: str = 'review_id') -> None:
    if id_col not in df.columns:
        return
    has_id = df[id_col].notna()
    dup_count = df[has_id].duplicated(subset=[id_col]).sum()
    assert dup_count == 0, \
        f'Found {dup_count} duplicate values in column {id_col}'
    logger.info('validate_no_duplicate_ids (%s): PASSED', id_col)


def run_all_validations(df: pd.DataFrame) -> None:
    checks = [
        validate_no_null_titles,
        validate_rating_range,
        validate_review_year_range,
        validate_no_duplicate_ids,
    ]
    passed = 0
    failed = 0
    for check in checks:
        try:
            check(df)
            print(f'  PASSED: {check.__name__}')
            passed += 1
        except AssertionError as e:
            print(f'  FAILED: {check.__name__} -> {e}')
            failed += 1
    print(f'\nValidation complete: {passed} passed, {failed} failed')