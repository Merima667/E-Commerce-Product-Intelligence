from datetime import datetime

import requests
from bs4 import BeautifulSoup
import time
import os
import json

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src')))
from scraping.robots_utils import check_robots
from storage.mongo import save_to_mongo

HEADERS = {"User-Agent": "ResearchBot/1.0"}
BASE_URL = "https://books.toscrape.com"
RAW_HTML_DIR = "../../data/raw/html"
SCRAPED_JSON_DIR = "../../data/raw/scraped"

os.makedirs(RAW_HTML_DIR, exist_ok=True)
os.makedirs(SCRAPED_JSON_DIR, exist_ok=True)

def scrape_single_page(url):
    response = requests.get(url, headers=HEADERS, timeout=20)
    response.raise_for_status()
    response.encoding = "utf-8"
    save_html("single_page.html", response.text)

    soup = BeautifulSoup(response.text, "lxml")

    results = []
    for article in soup.select("article.product_pod"):
        record = {
            "title": article.select_one("h3 > a")["title"] if article.select_one("h3 > a") else "",
            "price": article.select_one("p.price_color").get_text(strip=True) if article.select_one("p.price_color") else "",
            "rating": article.select_one("p.star-rating")["class"][1] if article.select_one("p.star-rating") else "",
            "link": BASE_URL + "/catalogue/" + article.select_one("h3 > a")["href"].replace("catalogue/", "") if article.select_one("h3 > a") else ""
        }
        results.append(record)
        save_to_mongo(
            record,
            BASE_URL,
            extra_metadata={
                "file_name": "single_page.html",
                "type": "scraped",
                "fetched_at": datetime.utcnow()
            }
        )
    
    return results

def scrape_multiple_pages(base_url, max_pages=3):
    all_results = []

    for page in range(1, max_pages + 1):
        url = f"{base_url}/catalogue/page-{page}.html"
        print(f"Scraping page {page}: {url}")

        response = requests.get(url, headers=HEADERS, timeout=20)
        response.raise_for_status()
        response.encoding = "utf-8"
        save_html(f"books_page_{page}.html", response.text)

        soup = BeautifulSoup(response.text, "lxml")

        for article in soup.select("article.product_pod"):
            record = {
                "title": article.select_one("h3 > a")["title"] if article.select_one("h3 > a") else "",
                "price": article.select_one("p.price_color").get_text(strip=True) if article.select_one("p.price_color") else "",
                "rating": article.select_one("p.star-rating")["class"][1] if article.select_one("p.star-rating") else "",
                "link": BASE_URL + "/catalogue/" + article.select_one("h3 > a")["href"].replace("catalogue/", "") if article.select_one("h3 > a") else "",
                "page_scraped": page
            }
            all_results.append(record)
            save_to_mongo(
                record,
                BASE_URL,
                extra_metadata={
                    "file_name": f"books_page_{page}.html",
                    "type": "scraped",
                    "page_number": page
                }
            )

        time.sleep(1.5)
    print(f"Ukupno scraped: {len(all_results)} knjiga")
    save_json("books_multiple_pages.json", all_results)
    return all_results

def save_html(filename, html_text):
    path = os.path.join(RAW_HTML_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html_text)

def save_json(filename, data):
    path = os.path.join(SCRAPED_JSON_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    if check_robots(BASE_URL):
        data = scrape_single_page(BASE_URL)
        for item in data:
            print(item)

        all_data = scrape_multiple_pages(BASE_URL, max_pages=3)
        for item in all_data[:5]:
            print(item)