from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce_product_intelligence"]
collection = db["products"]

def save_to_mongo(data, source_url):
    document = {
        "data": data,
        "source": source_url,
        "fetched_at": datetime.utcnow(),
        "version": 1
    }
    
    collection.insert_one(document)