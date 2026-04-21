import json
import logging
from pathlib import Path

logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def read_json(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        logging.info(f"Successfully read JSON file: {file_path}")
        return data

    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")

    except json.JSONDecodeError:
        logging.error(f"Invalid JSON format in file: {file_path}")

def read_text(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            text = f.read()

        logging.info(f"Successfully read text file: {file_path}")
        return text

    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")

if __name__ == "__main__":

    movie_file = Path("data/raw/tmdb/movie_11.json")
    review_file = Path("data/raw/reviews/review_1.txt")

    movie_data = read_json(movie_file)
    review_text = read_text(review_file)

    if movie_data:
        print("Movie title:", movie_data["title"])

    if review_text:
        print("First 100 characters of review:")
        print(review_text[:100])