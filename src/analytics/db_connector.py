import pymysql
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def get_connection(host='localhost', user='root', password='root', database='ecommerce_analytics'):
    conn = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        charset='utf8mb4'
    )
    logger.info('Connected to MySQL database: %s', database)
    return conn


def create_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reviews (
            id INT AUTO_INCREMENT PRIMARY KEY,
            review_id VARCHAR(20),
            title VARCHAR(500),
            rating FLOAT,
            source VARCHAR(200),
            category VARCHAR(100),
            helpful_votes FLOAT
        )
    ''')
    conn.commit()
    cursor.close()
    logger.info('Table reviews created')


def populate_reviews(conn, df):
    required = ['review_id', 'title', 'rating', 'source', 'category', 'helpful_votes']
    available = [c for c in required if c in df.columns]
    data = df[available].copy()

    for col in ['rating', 'helpful_votes']:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors='coerce')

    cursor = conn.cursor()
    inserted = 0
    skipped = 0

    for _, row in data.iterrows():
        try:
            cursor.execute(
                '''INSERT INTO reviews (review_id, title, rating, source, category, helpful_votes)
                   VALUES (%s, %s, %s, %s, %s, %s)''',
                (
                    str(row.get('review_id', '')),
                    str(row.get('title', '')),
                    float(row['rating']) if pd.notna(row.get('rating')) else None,
                    str(row.get('source', '')),
                    str(row.get('category', '')),
                    float(row['helpful_votes']) if pd.notna(row.get('helpful_votes')) else None,
                )
            )
            inserted += 1
        except Exception as e:
            logger.warning('Skipped row: %s', e)
            skipped += 1

    conn.commit()
    cursor.close()
    logger.info('Inserted %d rows, skipped %d rows', inserted, skipped)
    return inserted, skipped


def query_reviews(conn, sql=None):
    if sql is None:
        sql = 'SELECT * FROM reviews'
    df = pd.read_sql(sql, conn)
    logger.info('Queried %d rows from MySQL', len(df))
    return df