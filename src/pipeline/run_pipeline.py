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

    logging.info("Pipeline finished successfully")

if __name__ == "__main__":
    run_pipeline()