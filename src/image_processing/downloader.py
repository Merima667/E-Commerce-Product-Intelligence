import os
import time
import logging
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

API_KEY = os.getenv("RAPID_API_KEY")
HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com"
}

def fetch_product_asins(query="smartphone", country="US", pages=1):
    asins = []
    for page in range(1, pages+1):
        url = "https://real-time-amazon-data.p.rapidapi.com/search"
        params = {
            "query": query,
            "country": country,
            "page": page
        }
        resp = requests.get(url, headers=HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()
        products = data.get("data", {}).get("products", [])
        for product in products:
            asin = product.get("asin")
            if asin:
                asins.append(asin)
        time.sleep(0.5)
    logger.info(f"Found {len(asins)} ASINs")
    return asins

def fetch_products(asins):
    products = []
    for asin in asins:
        url = "https://real-time-amazon-data.p.rapidapi.com/product-details"
        params = {"asin": asin, "country": "US"}
        resp = requests.get(url, headers=HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()
        products.append(data.get("data", {}))
        logger.info(f"Fetched product: {asin}")
        time.sleep(0.5)
    return products

def download_product_image(image_url, dest_dir, filename):
    if not image_url:
        return None
    dest = Path(dest_dir) / filename
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        logger.debug(f"Already exists: {dest}")
        return str(dest)
    resp = requests.get(image_url, stream=True, timeout=15)
    resp.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    logger.info(f"Download: {dest}")
    return str(dest)

def download_product_images(products, dest_dir="../../data/raw/images"):
    downloaded = []
    for product in products:
        image_url = product.get("product_photo")
        asin = product.get("asin", "unknown")
        if image_url:
            filename = f"{asin}.jpg"
            local = download_product_image(image_url, dest_dir, filename)
            if local:
                downloaded.append({
                    "asin": asin,
                    "title": product.get("product_title", ""),
                    "local_path": local,
                    "image_url": image_url
                })
        time.sleep(0.1)
    logger.info(f"Downloaded {len(downloaded)} images.")
    return downloaded



if __name__ == "__main__":
    asins = ["B00930I7EK", "B09SM24S8C"]
    products = fetch_products(asins)
    downloaded = download_product_images(products)
    for d in downloaded:
        print(d)