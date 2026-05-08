import pandas as pd
import logging
from pymongo import MongoClient

logger = logging.getLogger(__name__)

def get_mongo_collection(db_name='ecommerce_product_intelligence', collection='products'):
    client = MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    return db[collection], client

def run_review_pipeline(min_rating: float = 4.0) -> pd.DataFrame:
    collection, client = get_mongo_collection()
    try:
        pipeline = [
            {'$match': {'data.rating': {'$exists': True}}},
            {'$group': {
                '_id': '$source',
                'review_count': {'$sum': 1}
            }},
            {'$sort': {'review_count': -1}},
            {'$project': {
                'source': '$_id',
                'review_count': 1,
                '_id': 0
            }}
        ]
        result = list(collection.aggregate(pipeline))
        df = pd.DataFrame(result)
        logger.info('MongoDB pipeline returned %d rows', len(df))
        return df
    finally:
        client.close()