import logging
logging.basicConfig(level=logging.INFO)
from downloader import fetch_product_asins ,fetch_products, download_product_images

# Fetch 20 popular movies
asins = fetch_product_asins(query="smartphone", pages=1)
print(f'Fetched {len(asins)} ASINs')

products = fetch_products(asins[:5])
results = download_product_images(products, dest_dir='../../data/raw/images')

for r in results:
    print(r['title'], '->', r['local_path'])