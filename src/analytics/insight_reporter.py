import pandas as pd
import logging

logger = logging.getLogger(__name__)


def q1_top_sources_by_reviews(df, source_col='source', top_n=10):
    """
    Question 1: Which sources have the most reviews?
    """
    if source_col not in df.columns:
        logger.warning('q1: source_col "%s" missing', source_col)
        return pd.DataFrame()

    result = (
        df.groupby(source_col)
          .size()
          .nlargest(top_n)
          .reset_index(name='review_count')
    )
    logger.info('Q1 top sources by reviews: %d rows', len(result))
    return result


def q2_avg_rating_by_source(df, source_col='source', rating_col='rating', top_n=10):
    """
    Question 2: Which sources have the highest average rating?
    """
    if source_col not in df.columns or rating_col not in df.columns:
        logger.warning('q2: required columns missing')
        return pd.DataFrame()

    df = df.copy()
    df[rating_col] = pd.to_numeric(df[rating_col], errors='coerce')

    result = (
        df.groupby(source_col)[rating_col]
          .mean()
          .sort_values(ascending=False)
          .head(top_n)
          .reset_index()
          .rename(columns={rating_col: 'avg_rating'})
    )
    logger.info('Q2 avg rating by source: %d rows', len(result))
    return result


def q3_reviews_per_year(df, year_col='review_year'):
    """
    Question 3: How has the number of reviews changed over time?
    """
    if year_col not in df.columns:
        logger.warning('q3: year_col "%s" missing', year_col)
        return pd.DataFrame()

    result = (
        df[pd.to_numeric(df[year_col], errors='coerce').fillna(0) > 2000]
          .groupby(year_col)
          .size()
          .reset_index(name='review_count')
          .sort_values(year_col)
    )
    logger.info('Q3 reviews per year: %d years', len(result))
    return result


def q4_verified_distribution(df, verified_col='verified', top_n=10):
    """
    Question 4: What is the distribution of verified vs unverified reviews?
    """
    if verified_col not in df.columns:
        logger.warning('q4: verified_col "%s" missing', verified_col)
        return pd.DataFrame()

    result = (
        df[verified_col]
          .value_counts()
          .head(top_n)
          .reset_index()
          .rename(columns={verified_col: 'verified', 'count': 'count'})
    )
    logger.info('Q4 verified distribution: %d rows', len(result))
    return result


def run_all_questions(df):
    """
    Run all four analytical questions and print a formatted summary.
    """
    results = {}

    print('\n===========================')
    print('ANALYTICAL FINDINGS SUMMARY')
    print('===========================\n')

    # Q1
    top_sources = q1_top_sources_by_reviews(df)
    results['top_sources'] = top_sources
    if not top_sources.empty:
        best = top_sources.iloc[0]
        print(f'Q1 - Top Source by Reviews: {best["source"]} '
              f'({best["review_count"]} reviews)')
    else:
        print('Q1 - Top Source by Reviews: no data')

    # Q2
    avg_rating = q2_avg_rating_by_source(df)
    results['avg_rating_by_source'] = avg_rating
    if not avg_rating.empty:
        best = avg_rating.iloc[0]
        print(f'Q2 - Highest Rated Source: {best["source"]} '
              f'(avg rating {best["avg_rating"]:.2f})')
    else:
        print('Q2 - Highest Rated Source: no data')

    # Q3
    yearly = q3_reviews_per_year(df)
    results['reviews_per_year'] = yearly
    if not yearly.empty:
        peak = yearly.loc[yearly['review_count'].idxmax()]
        print(f'Q3 - Peak Review Year: {int(peak["review_year"])} '
              f'({int(peak["review_count"])} reviews)')
    else:
        print('Q3 - Peak Review Year: no data')

    # Q4
    verified = q4_verified_distribution(df)
    results['verified_distribution'] = verified
    if not verified.empty:
        top = verified.iloc[0]
        pct = top['count'] / len(df) * 100
        print(f'Q4 - Most Common Verified Status: {top["verified"]} '
              f'({top["count"]} reviews, {pct:.1f}% of dataset)')
    else:
        print('Q4 - Verified Distribution: no data')

    print('\n===========================')
    return results