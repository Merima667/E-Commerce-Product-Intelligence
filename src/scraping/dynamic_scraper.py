import requests
import time
import os
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src')))
from scraping.robots_utils import check_robots
from storage.mongo import save_to_mongo

HEADERS = {"User-Agent": "ResearchBot/1.0"}
SCRAPED_JSON_DIR = "../../data/raw/scraped"
os.makedirs(SCRAPED_JSON_DIR, exist_ok=True)

def scrape_oscar_films(years=True):
    if years is None:
        years = list(range(2010, 2016))
    
    base_url = "https://www.scrapethissite.com/pages/ajax-javascript/"
    all_films = []

    for year in years:
        url = f"{base_url}?ajax=true&year={year}"
        print(f"Fetching films for: {year}...")
        response = requests.get(url, headers=HEADERS, timeout=20)
        response.raise_for_status()
        films = response.json()
        for film in films:
            film["year_scraped"] = year
            all_films.append(film)
            save_to_mongo(
                film,
                base_url,
                extra_metadata={
                    "file_name": "oscar_films.json",
                    "type": "scraped_dynamic"
                }
            )
        time.sleep(1)
    print(f"Ukupno filmova: {len(all_films)}")
    path = os.path.join(SCRAPED_JSON_DIR, "oscar_films.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(all_films, f, indent=4, ensure_ascii=False)
    return all_films

def scrape_dynamic_page(url):
    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    try:
        driver.get(url)
        html = driver.page_source
        soup = BeautifulSoup(html, "lxml")
        return soup.title.get_text(strip=True) if soup.title else "No title"
    finally:
        driver.quit()


if __name__ == "__main__":
    films = scrape_oscar_films(years=[2010, 2011, 2012, 2013])
    for f in films[:3]:
        print(f)
        
    url = "https://www.scrapethissite.com/pages/ajax-javascript/"
    title = scrape_dynamic_page(url)
    print("Page title:", title)