import requests
import os
from dotenv import load_dotenv
import json

load_dotenv()

API_KEY = os.getenv("RAPID_API_KEY")
print(API_KEY)

url = "https://real-time-amazon-data.p.rapidapi.com/top-product-reviews"

headers = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com"
}

def save_raw_data(data, page_num):
    
    folder_path = os.path.join("../../data/raw/api/")
    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, f"reviews_page_{page_num}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    print(f"Page {page_num} saved to {file_path}")

def fetch_reviews_pagination(asin, country="US", pages=3):
    all_pages = []

    for page in range(1, pages+1):
        params = {
            "asin": asin,
            "country": country,
            "page": page
        }
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            save_raw_data(data, page)

            all_pages.append(data)
            print(f"Page {page} fetched successfully!")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page}: {e}")
    return all_pages



if __name__ == "__main__":
    asin = "B00939I7EK"
    fetch_reviews_pagination(asin, pages=3)