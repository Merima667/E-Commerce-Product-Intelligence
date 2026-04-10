from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce_product_intelligence"]
collection = db["products"]

def save_to_mongo(data, source_url, extra_metadata=None):
    document = {
        "data": data,
        "source": source_url,
        "fetched_at": datetime.utcnow(),
        "version": 1
    }

    if extra_metadata:
        document.update(extra_metadata)

    collection.insert_one(document)

    print("Inserted document:", document)

image_collection = db["images"]

def save_image_metadata(metadata, source="amazon_api"):
    document = {
        "source": source,
        "type": "product_image",
        "processed_at": datetime.utcnow(),
        **metadata
    }
    image_collection.insert_one(document)
    print(f"Image metadata saved: {metadata.get('filename')}")