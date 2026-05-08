import pandas as pd
import logging
 
logger = logging.getLogger(__name__)
 
 
def parse_review_dates(df, date_col='date'):
    if date_col not in df.columns:
        logger.warning('date_col "%s" not found in DataFrame', date_col)
        return df
 
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
 
    df['review_year']    = df[date_col].dt.year
    df['review_month']   = df[date_col].dt.month
    df['review_day']     = df[date_col].dt.day
    df['review_weekday'] = df[date_col].dt.dayofweek   # 0 = Monday
    df['review_quarter'] = df[date_col].dt.quarter
 
    parsed = df[date_col].notna().sum()
    logger.info('Parsed %d / %d dates from "%s"', parsed, len(df), date_col)
    return df
 
 
def build_monthly_rating(df, date_col='date', value_col='rating'):
    if date_col not in df.columns or value_col not in df.columns:
        logger.warning('Missing columns for build_monthly_rating')
        return pd.Series(dtype=float)
 
    ts = (
        df.dropna(subset=[date_col])
          .set_index(date_col)[value_col]
          .resample('ME')
          .sum()
    )
    logger.info('Monthly rating: %d periods, total=%.0f', len(ts), ts.sum())
    return ts
 
 
def resample_rating(ts, freq='YE', agg='sum'):
    resampled = ts.resample(freq).agg(agg)
    logger.info('Resampled to freq=%s agg=%s -> %d periods', freq, agg, len(resampled))
    return resampled
 
 
def rolling_averages(ts, windows=(3, 6, 12)):
    result = pd.DataFrame({'value': ts})
    for w in windows:
        col = f'rolling_{w}'
        result[col] = ts.rolling(w, min_periods=1).mean()
        logger.info('Rolling average window=%d computed', w)
    return result