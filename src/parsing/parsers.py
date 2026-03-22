import csv
import xml.etree.ElementTree as ET
import os
import json
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src')))
from storage.mongo import save_to_mongo 

raw_api_folder = "../../data/raw/api/"
raw_csv_folder = "../../data/raw/csv/"
raw_xml_folder = "../../data/raw/xml/"

def extract_review_fields(review):
    return {
        "review_id": review.get("review_id"),
        "title": review.get("review_title"),
        "comment": review.get("review_comment"),
        "rating": review.get("review_star_rating"),
        "author": review.get("review_author"),
        "author_id": review.get("review_author_id"),
        "date": review.get("review_date"),
        "verified": review.get("is_verified_purchase"),
        "helpful_votes": review.get("helpful_vote_statement"),
        "variant": review.get("reviewed_product_variant"),
        "review_link": review.get("review_link")
    }


def parse_json_files():
    for file_name in os.listdir(raw_api_folder):
        if file_name.endswith(".json"):
            file_path = os.path.join(raw_api_folder, file_name)
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            reviews = data.get("data", {}).get("reviews", [])
            for review in reviews:
                review_fields = extract_review_fields(review)
                print(f"Saving to MongoDB (JSON): {review_fields}")
                save_to_mongo(review_fields, file_name)

def parse_csv_file(csv_path):
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            review_fields = extract_review_fields(row)
            print(f"Saving to MongoDB (CSV): {review_fields}")
            save_to_mongo(review_fields, csv_path)

def parse_xml_file(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    for item in root.findall("review"):
        review = {
            "review_id": item.findtext("review_id"),
            "review_title": item.findtext("review_title"),
            "review_comment": item.findtext("review_comment"),
            "review_star_rating": item.findtext("review_star_rating"),
            "author_id": item.findtext("review_author"),
            "review_author_id": item.findtext("review_author_id"),
            "review_date": item.findtext("review_date"),
            "is_verified_purchase": item.findtext("is_verified_purchase") == "True",
            "helpful_vote_statement": item.findtext("helpful_vote_statement"),
            "reviewed_product_variant": item.findtext("reviewed_product_variant"),
            "review_link": item.findtext("review_link")
        }
        review_fields = extract_review_fields(review)
        print(f"Saving to MongoDB (XML): {review_fields}")
        save_to_mongo(review_fields, xml_path)

if __name__ == "__main__":
    parse_json_files()
    parse_csv_file(os.path.join(raw_csv_folder, "sample.csv"))
    parse_xml_file(os.path.join(raw_xml_folder, "sample.xml"))
    