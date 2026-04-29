import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from cleaning.missing_handler import (
    drop_rows_missing_title,
    fill_missing_comment,
    replace_zero_with_nan,
    fill_numeric_with_median,
)
from cleaning.string_cleaner import (
    clean_title,
    clean_language_code,
    extract_year_from_release_date,
)
from cleaning.deduplicator import (
    drop_exact_duplicates,
    drop_duplicate_ids,
    count_duplicates,
)
from cleaning.type_converter import convert_dates
from cleaning.validator import (
    validate_no_null_titles,
    validate_rating_range,
)


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'review_id':  ['R001', 'R002', 'R002', 'R003', 'R004'],
        'title':      ['Great Product', '  Good Item  ', None, 'Best Buy', ''],
        'comment':    ['Love it!', None, 'Bad product', '<b>HTML</b>', 'OK'],
        'rating':     ['5', '3', None, '4', '12'],
        'source':     ['API Source', 'API Source', 'ecommerce_api', 'ecommerce_api', 'API Source'],
        'date':       ['January 1, 2024', 'March 5, 2023', None, 'June 10, 2025', 'BAD_DATE'],
        'budgeted':   [100.0, 200.0, 0.0, 150.0, 0.0],
    })


def test_drop_rows_missing_title_removes_null(sample_df):
    result = drop_rows_missing_title(sample_df)
    assert result['title'].isna().sum() == 0


def test_drop_rows_missing_title_removes_empty_string(sample_df):
    result = drop_rows_missing_title(sample_df)
    assert (result['title'].str.strip() == '').sum() == 0


def test_fill_missing_comment_no_nulls_remain(sample_df):
    result = fill_missing_comment(sample_df)
    assert result['comment'].isna().sum() == 0


def test_fill_missing_comment_uses_placeholder(sample_df):
    result = fill_missing_comment(sample_df)
    filled = result.loc[sample_df['comment'].isna(), 'comment']
    assert filled.str.contains('available', case=False).all()


def test_replace_zero_with_nan_on_budgeted(sample_df):
    result = replace_zero_with_nan(sample_df, columns=['budgeted'])
    zero_count = (result['budgeted'] == 0).sum()
    assert zero_count == 0


def test_clean_title_strips_whitespace(sample_df):
    result = clean_title(sample_df.dropna(subset=['title']))
    assert not result['title'].str.startswith(' ').any()
    assert not result['title'].str.endswith(' ').any()


def test_clean_language_code_lowercases(sample_df):
    result = clean_language_code(sample_df)
    assert result['source'].str.islower().all()


def test_extract_year_creates_column(sample_df):
    result = extract_year_from_release_date(sample_df)
    assert 'review_year' in result.columns


def test_drop_exact_duplicates_removes_copies():
    df = pd.DataFrame({
        'review_id': ['R001', 'R001', 'R002'],
        'title': ['Product A', 'Product A', 'Product B']
    })
    result = drop_exact_duplicates(df)
    assert len(result) == 2


def test_drop_duplicate_ids_keeps_first():
    df = pd.DataFrame({
        'review_id': ['R001', 'R001', 'R002'],
        'title': ['Version A', 'Version B', 'Other']
    })
    result = drop_duplicate_ids(df, id_col='review_id')
    assert len(result) == 2
    assert result.loc[result['review_id'] == 'R001', 'title'].values[0] == 'Version A'


def test_count_duplicates_returns_correct_number():
    df = pd.DataFrame({'review_id': ['R001', 'R001', 'R001', 'R002', 'R003']})
    assert count_duplicates(df, col='review_id') == 2


def test_convert_dates_produces_datetime_type(sample_df):
    result = convert_dates(sample_df)
    assert pd.api.types.is_datetime64_any_dtype(result['date'])


def test_convert_dates_bad_values_become_nat(sample_df):
    result = convert_dates(sample_df)
    nat_count = result['date'].isna().sum()
    assert nat_count >= 2


def test_validate_no_null_titles_passes_on_clean_data():
    df = pd.DataFrame({'title': ['Product A', 'Product B']})
    validate_no_null_titles(df)


def test_validate_no_null_titles_fails_on_null():
    df = pd.DataFrame({'title': ['Product A', None]})
    with pytest.raises(AssertionError):
        validate_no_null_titles(df)


def test_validate_rating_range_fails_on_out_of_range():
    df = pd.DataFrame({'rating': [4.5, 12.0]})
    with pytest.raises(AssertionError):
        validate_rating_range(df)