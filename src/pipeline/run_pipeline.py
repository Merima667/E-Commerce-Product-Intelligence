import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src')))

from utils.logger import logging  
from storage.mongo import save_to_mongo 
from api.client import fetch_reviews_pagination
from parsing.parsers import extract_review_fields

def run_pipeline():
    reviews = fetch_reviews_pagination("B00939I7EK")

    for page in reviews:
        for review in page.get("data", {}).get("reviews", []):
            parsed = extract_review_fields(review)
            save_to_mongo(parsed, "ecommerce_api")

    logging.info("Pipeline finished successfully")

if __name__ == "__main__":
    run_pipeline()