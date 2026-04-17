import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src')))
from utils.logger import logging  
from storage.mongo import save_to_mongo 
from api.client import fetch_reviews_pagination
from parsing.parsers import (
    extract_review_fields,
    extract_text_from_pdf,
    extract_text_from_two_column_pdf,
    extract_text_from_word,
    extract_text_from_two_column_word,
    extract_data_from_excel,
    extract_transactions_from_excel,
    extract_savings_goals_from_excel,
    extract_trend_from_excel
)

from scraping.scraper import scrape_single_page, scrape_multiple_pages
from scraping.dynamic_scraper import scrape_oscar_films, scrape_dynamic_page
from ocr.ocr_utils import compare_ocr, ocr_scanned_pdf

def run_pipeline():
    logging.info("Pipeline started")
    try:
        reviews = fetch_reviews_pagination("B00939I7EK")
        for page in reviews:
            for review in page.get("data", {}).get("reviews", []):
                parsed = extract_review_fields(review)
                save_to_mongo(parsed, "API Source", {"file_name": "api_reviews.json", "type": "api"})
            logging.info("API reviews processed")
    except Exception as e:
        logging.error(f"Error processing API reviews: {e}")

    pdf_files = [
        ("../../data/raw/pdf/student_finance_normal.pdf", "pdf"),
        ("../../data/raw/pdf/student_finance_twocol.pdf", "pdf")
    ]
    for pdf_path, doc_type in pdf_files:
        try:
            if "_twocol" in pdf_path:
                text = extract_text_from_two_column_pdf(pdf_path)
            else:
                text = extract_text_from_pdf(pdf_path)
            save_to_mongo(
                {"text": text},
                pdf_path,
                extra_metadata={"file_name": os.path.basename(pdf_path), "type": doc_type}
            )
            logging.info(f"PDF processed: {pdf_path}")
        except Exception as e:
            logging.error(f"Error processing PDF {pdf_path}: {e}")
    
    word_files = [
        ("../../data/raw/word/student_finance_report.docx", "word"),
        ("../../data/raw/word/student_finance_report_twocol.docx", "word")
    ]

    for word_path, doc_type in word_files:
        try:
            if "_twocol" in word_path:
                text = extract_text_from_two_column_word(word_path)
            else:
                text = extract_text_from_word(word_path)

            save_to_mongo(
                {"text": text},
                word_path,
                extra_metadata={"file_name": os.path.basename(word_path), "type": doc_type}
            )
            logging.info(f"Word processed: {word_path}")
        except Exception as e:
            logging.error(f"Error processing Word {word_path}: {e}")

    excel_files = [
        "../../data/raw/excel/student_finance_data.xlsx"
    ]

    for excel_path in excel_files:
        try:
            budget_data = extract_data_from_excel(excel_path)
            for row in budget_data:
                save_to_mongo(
                    row,
                    excel_path,
                    extra_metadata={"file_name": os.path.basename(excel_path), "type": "excel_budget"}
                )

            transactions = extract_transactions_from_excel(excel_path)
            for row in transactions:
                save_to_mongo(
                    row,
                    excel_path,
                    extra_metadata={"file_name": os.path.basename(excel_path), "type": "excel_transactions"}
                )

            goals = extract_savings_goals_from_excel(excel_path)
            for row in goals:
                save_to_mongo(
                    row,
                    excel_path,
                    extra_metadata={"file_name": os.path.basename(excel_path), "type": "excel_savings_goals"}
                )
            trend = extract_trend_from_excel(excel_path)
            for row in trend:
                save_to_mongo(
                    row,
                    excel_path,
                    extra_metadata={"file_name": os.path.basename(excel_path), "type": "excel_trend"}
                )

            logging.info(f"Excel processed: {excel_path}")
        except Exception as e:
            logging.error(f"Error processing Excel {excel_path}: {e}")

    try:
        logging.info("Wen scraping started")
        scrape_single_page("https://books.toscrape.com")
        logging.info("Single page scraping done")
        scrape_multiple_pages("https://books.toscrape.com", max_pages=3)
        logging.info("Multi-page scraping done")
        scrape_oscar_films(years=[2010, 2011, 2012, 2013])
        logging.info("Dynamic scraping done")
    except Exception as e:
        logging.error(f"Error processing wen scraping: {e}")

    try:
        title = scrape_dynamic_page("https://www.scrapethissite.com/pages/ajax-javascript/")
        logging.info(f"Selenium scraping done: {title}")
    except Exception as e:
        logging.error(f"Erro processing Selenium: {e}")
    
    try:
        logging.info("OCR started")
        compare_ocr("../../data/raw/images/test_scan.png")
        logging.info("OCR image done")
        ocr_scanned_pdf("../../data/raw/scanned/sample.pdf")
        logging.info("OCR scanned PDF done")
    except Exception as e:
        logging.error(f"Erro processing OCR: {e}")

    logging.info("Pipeline finished successfully")



if __name__ == "__main__":
    run_pipeline()