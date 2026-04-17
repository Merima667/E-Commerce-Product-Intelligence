import logging

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


def save_transcript_to_mongo(db, transcript_result: dict, source_path: str, source_type: str = 'audio', json_path: str = None, txt_path:  str = None, srt_path:  str = None) -> str:
    doc = {
        'source_file':           transcript_result.get('source_file', ''),
        'source_path':           source_path,
        'source_type':           source_type,
        'model':                 transcript_result.get('model', 'unknown'),
        'language':              transcript_result.get('language', ''),
        'language_probability':  transcript_result.get('language_probability', 0),
        'duration_s':            transcript_result.get('duration_s', 0),
        'full_text':             transcript_result.get('full_text', ''),
        'segments':              transcript_result.get('segments', []),
        'transcript_json_path':  json_path,
        'transcript_txt_path':   txt_path,
        'transcript_srt_path':   srt_path,
        'transcribed_at':        datetime.utcnow().isoformat(),
    }
    result = db['transcripts'].insert_one(doc)
    logging.getLogger(__name__).info(
        f'Saved transcript to MongoDB: {doc["source_file"]} '
        f'(id={result.inserted_id})'
    )
    return str(result.inserted_id)