import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src', 'image_processing')))
from image_processing.batch import batch_process_images
from utils.logger import logging  
from storage.mongo import save_to_mongo, save_transcript_to_mongo
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


from audio_processing.loader      import inspect_audio, load_audio
from audio_processing.processor   import trim_audio, apply_fades, export_audio
from audio_processing.transcriber import (
    transcribe_audio, save_transcript_json,
    save_transcript_txt, save_transcript_srt
)
from video_processing.loader       import inspect_video, extract_audio_from_video
from video_processing.frame_extractor import extract_keyframes

from pathlib import Path
from storage.mongo import db


def run_audio_video_stage():
    logging.info('=== Audio/Video Processing Stage ===')

    # AUDIO
    for audio_file in Path('../../data/raw/audio').glob('*.mp3'):
        try:
            logging.info(f'Processing audio: {audio_file.name}')

            audio = load_audio(str(audio_file))
            trimmed = trim_audio(audio, 0, min(30000, len(audio)))
            faded = apply_fades(trimmed)

            export_audio(
                faded,
                f'../../data/processed/audio/{audio_file.stem}_clip.mp3'
            )

            result = transcribe_audio(str(audio_file))

            j = f'../../data/processed/transcripts/{audio_file.stem}.json'
            t = f'../../data/processed/transcripts/{audio_file.stem}.txt'
            s = f'../../data/processed/transcripts/{audio_file.stem}.srt'

            save_transcript_json(result, j)
            save_transcript_txt(result, t)
            save_transcript_srt(result, s)

            save_transcript_to_mongo(db, result, str(audio_file), 'audio',
                            json_path=j, txt_path=t, srt_path=s)

        except Exception as e:
            logging.error(f'Audio error: {e}')

    # VIDEO
    for video_file in Path('../../data/raw/video').glob('*.mp4'):
        try:
            logging.info(f'Processing video: {video_file.name}')

            extract_keyframes(
                str(video_file),
                f'../../data/processed/frames/{video_file.stem}/'
            )

            audio_out = f'../../data/processed/audio/{video_file.stem}.mp3'
            extract_audio_from_video(str(video_file), audio_out)

            result = transcribe_audio(audio_out)

            j = f'../../data/processed/transcripts/{video_file.stem}.json'
            t = f'../../data/processed/transcripts/{video_file.stem}.txt'
            s = f'../../data/processed/transcripts/{video_file.stem}.srt'

            save_transcript_json(result, j)
            save_transcript_txt(result, t)
            save_transcript_srt(result, s)

            save_transcript_to_mongo(db, result, str(video_file), 'video',
                            json_path=j, txt_path=t, srt_path=s)

        except Exception as e:
            logging.error(f'Video error: {e}')

    logging.info('=== Audio/Video Processing Stage Complete ===')

def run_pipeline():
    # logging.info("Pipeline started")
    # try:
    #     reviews = fetch_reviews_pagination("B00939I7EK")
    #     for page in reviews:
    #         for review in page.get("data", {}).get("reviews", []):
    #             parsed = extract_review_fields(review)
    #             save_to_mongo(parsed, "API Source", {"file_name": "api_reviews.json", "type": "api"})
    #         logging.info("API reviews processed")
    # except Exception as e:
    #     logging.error(f"Error processing API reviews: {e}")

    # pdf_files = [
    #     ("../../data/raw/pdf/student_finance_normal.pdf", "pdf"),
    #     ("../../data/raw/pdf/student_finance_twocol.pdf", "pdf")
    # ]
    # for pdf_path, doc_type in pdf_files:
    #     try:
    #         if "_twocol" in pdf_path:
    #             text = extract_text_from_two_column_pdf(pdf_path)
    #         else:
    #             text = extract_text_from_pdf(pdf_path)
    #         save_to_mongo(
    #             {"text": text},
    #             pdf_path,
    #             extra_metadata={"file_name": os.path.basename(pdf_path), "type": doc_type}
    #         )
    #         logging.info(f"PDF processed: {pdf_path}")
    #     except Exception as e:
    #         logging.error(f"Error processing PDF {pdf_path}: {e}")
    
    # word_files = [
    #     ("../../data/raw/word/student_finance_report.docx", "word"),
    #     ("../../data/raw/word/student_finance_report_twocol.docx", "word")
    # ]

    # for word_path, doc_type in word_files:
    #     try:
    #         if "_twocol" in word_path:
    #             text = extract_text_from_two_column_word(word_path)
    #         else:
    #             text = extract_text_from_word(word_path)

    #         save_to_mongo(
    #             {"text": text},
    #             word_path,
    #             extra_metadata={"file_name": os.path.basename(word_path), "type": doc_type}
    #         )
    #         logging.info(f"Word processed: {word_path}")
    #     except Exception as e:
    #         logging.error(f"Error processing Word {word_path}: {e}")

    # excel_files = [
    #     "../../data/raw/excel/student_finance_data.xlsx"
    # ]

    # for excel_path in excel_files:
    #     try:
    #         budget_data = extract_data_from_excel(excel_path)
    #         for row in budget_data:
    #             save_to_mongo(
    #                 row,
    #                 excel_path,
    #                 extra_metadata={"file_name": os.path.basename(excel_path), "type": "excel_budget"}
    #             )

    #         transactions = extract_transactions_from_excel(excel_path)
    #         for row in transactions:
    #             save_to_mongo(
    #                 row,
    #                 excel_path,
    #                 extra_metadata={"file_name": os.path.basename(excel_path), "type": "excel_transactions"}
    #             )

    #         goals = extract_savings_goals_from_excel(excel_path)
    #         for row in goals:
    #             save_to_mongo(
    #                 row,
    #                 excel_path,
    #                 extra_metadata={"file_name": os.path.basename(excel_path), "type": "excel_savings_goals"}
    #             )
    #         trend = extract_trend_from_excel(excel_path)
    #         for row in trend:
    #             save_to_mongo(
    #                 row,
    #                 excel_path,
    #                 extra_metadata={"file_name": os.path.basename(excel_path), "type": "excel_trend"}
    #             )

    #         logging.info(f"Excel processed: {excel_path}")
    #     except Exception as e:
    #         logging.error(f"Error processing Excel {excel_path}: {e}")

    # try:
    #     logging.info("Wen scraping started")
    #     scrape_single_page("https://books.toscrape.com")
    #     logging.info("Single page scraping done")
    #     scrape_multiple_pages("https://books.toscrape.com", max_pages=3)
    #     logging.info("Multi-page scraping done")
    #     scrape_oscar_films(years=[2010, 2011, 2012, 2013])
    #     logging.info("Dynamic scraping done")
    # except Exception as e:
    #     logging.error(f"Error processing wen scraping: {e}")

    # try:
    #     title = scrape_dynamic_page("https://www.scrapethissite.com/pages/ajax-javascript/")
    #     logging.info(f"Selenium scraping done: {title}")
    # except Exception as e:
    #     logging.error(f"Erro processing Selenium: {e}")
    
    # try:
    #     logging.info("OCR started")
    #     compare_ocr("../../data/raw/images/test_scan.png")
    #     logging.info("OCR image done")
    #     ocr_scanned_pdf("../../data/raw/scanned/sample.pdf")
    #     logging.info("OCR scanned PDF done")
    # except Exception as e:
    #     logging.error(f"Erro processing OCR: {e}")

    # #Image processing
    # try:
    #     logging.info("Image processing started")
    #     from image_processing.batch import batch_process_images
    #     results, errors = batch_process_images(
    #         input_dir="../../data/raw/images",
    #         output_dir="../../data/processed",
    #         max_width=500,
    #         thumb_size=(128, 128),
    #         convert_webp=True,
    #         extract_metadata=True
    #     )
    #     logging.info(f"Image processing done. Success: {len(results)}, Errors: {len(errors)}")
    # except Exception as e:
    #     logging.error(f"Error processing images: {e}")
    
    try: 
        run_audio_video_stage()
    except Exception as e:
        logging.error(f"Error processing audio/video: {e}")

    logging.info("Pipeline finished successfully")


if __name__ == "__main__":
    run_pipeline()