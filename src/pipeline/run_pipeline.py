import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src')))
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src', 'image_processing')))
# from image_processing.batch import batch_process_images
from utils.logger import logging  
# from storage.mongo import save_to_mongo, save_transcript_to_mongo
# from api.client import fetch_reviews_pagination
# from parsing.parsers import (
#     extract_review_fields,
#     extract_text_from_pdf,
#     extract_text_from_two_column_pdf,
#     extract_text_from_word,
#     extract_text_from_two_column_word,
#     extract_data_from_excel,
#     extract_transactions_from_excel,
#     extract_savings_goals_from_excel,
#     extract_trend_from_excel
# )

# from scraping.scraper import scrape_single_page, scrape_multiple_pages
# from scraping.dynamic_scraper import scrape_oscar_films, scrape_dynamic_page
# from ocr.ocr_utils import compare_ocr, ocr_scanned_pdf

# import numpy as np
# from analytics.numpy_ops import demonstrate_array_creation, vectorized_operations
# from analytics.data_loader import load_from_mongodb, save_to_csv, chunked_stats, optimise_dtypes, memory_comparison
# from analytics.explorer import inspect_shape, extract_review_year, plot_distributions
# from analytics.selector import loc_filter, boolean_filter
# from analytics.regex_ops import extract_categories, top_categories, positive_comment_count
# from analytics.quality_report import full_quality_report, outlier_report, save_missing_heatmap

# from audio_processing.loader      import inspect_audio, load_audio
# from audio_processing.processor   import trim_audio, apply_fades, export_audio
# from audio_processing.transcriber import (
#     transcribe_audio, save_transcript_json,
#     save_transcript_txt, save_transcript_srt
# )
# from video_processing.loader       import inspect_video, extract_audio_from_video
# from video_processing.frame_extractor import extract_keyframes

# from pathlib import Path
# from storage.mongo import db


# def run_analytics():

#     PROCESSED_DIR = Path("../../data/processed/analytics")
#     PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

#     CSV_PATH = str(PROCESSED_DIR / "products_raw.csv")
#     OPT_CSV_PATH = str(PROCESSED_DIR / "products_optimised.csv")

#     #Numpy
#     arrays = demonstrate_array_creation()
#     logging.info("NumPy arrays created: %s", list(arrays.keys()))

#     ratings_arr = np.array([4.5, 3.2, 5.0, 2.8, 4.1, 3.9, 4.7, 2.5, 5.0, 3.6])
#     review_count = np.array([120, 45, 200, 30, 89, 67, 150, 25, 310, 55])

#     results = vectorized_operations(ratings_arr, review_count)
#     logging.info(
#         "Vectorized ops: mean=%.2f",
#         results["stats"]["mean"]
#     )
#     df = load_from_mongodb()

#     if df is None or df.empty:
#         logging.warning("MongoDB returned empty dataset")
#         return
    
#     save_to_csv(df, CSV_PATH)

#     chunk_results = chunked_stats(CSV_PATH)
#     logging.info(
#         "Chunked mean rating: %.4f over %d rows",
#         chunk_results["global_mean"],
#         chunk_results["total_rows"]
#     )


#     df_opt = optimise_dtypes(df)
#     mem = memory_comparison(df, df_opt)
#     logging.info("Memory reduction: %.1f%%", mem["reduction_pct"])
#     save_to_csv(df_opt, OPT_CSV_PATH)

#     # EDA
#     shape_info = inspect_shape(df)
#     logging.info("Dataset shape: %dx%d", shape_info["rows"], shape_info["columns"])

#     df = extract_review_year(df)
#     plot_distributions(df, str(PROCESSED_DIR / "distributions.png"))

#     high_rated = loc_filter(df, min_rating=4.0)
#     logging.info("High rated reviews: %d", len(high_rated))

#     quality = boolean_filter(df, min_rating=4.0, max_rating=5.0)
#     logging.info("Quality reviews: %d", len(quality))

#     # REGEX & QUALITY
#     df_cat = extract_categories(df)

#     logging.info("Top categories: %s", top_categories(df_cat, n=10))
#     logging.info("Positive comments: %d", positive_comment_count(df))

#     quality_df = full_quality_report(df)
#     quality_df.to_csv(str(PROCESSED_DIR / "quality_report.csv"), index=False)

#     save_missing_heatmap(df, str(PROCESSED_DIR / "missing_heatmap.png"))

#     outliers = outlier_report(df)
#     logging.info("Outlier report columns: %d", len(outliers))

#     logging.info("Analytics stage COMPLETED")

# def run_audio_video_stage():
#     logging.info('=== Audio/Video Processing Stage ===')

#     # AUDIO
#     for audio_file in Path('../../data/raw/audio').glob('*.mp3'):
#         try:
#             logging.info(f'Processing audio: {audio_file.name}')

#             audio = load_audio(str(audio_file))
#             trimmed = trim_audio(audio, 0, min(30000, len(audio)))
#             faded = apply_fades(trimmed)

#             export_audio(
#                 faded,
#                 f'../../data/processed/audio/{audio_file.stem}_clip.mp3'
#             )

#             result = transcribe_audio(str(audio_file))

#             j = f'../../data/processed/transcripts/{audio_file.stem}.json'
#             t = f'../../data/processed/transcripts/{audio_file.stem}.txt'
#             s = f'../../data/processed/transcripts/{audio_file.stem}.srt'

#             save_transcript_json(result, j)
#             save_transcript_txt(result, t)
#             save_transcript_srt(result, s)

#             save_transcript_to_mongo(db, result, str(audio_file), 'audio',
#                             json_path=j, txt_path=t, srt_path=s)

#         except Exception as e:
#             logging.error(f'Audio error: {e}')

#     # VIDEO
#     for video_file in Path('../../data/raw/video').glob('*.mp4'):
#         try:
#             logging.info(f'Processing video: {video_file.name}')

#             extract_keyframes(
#                 str(video_file),
#                 f'../../data/processed/frames/{video_file.stem}/'
#             )

#             audio_out = f'../../data/processed/audio/{video_file.stem}.mp3'
#             extract_audio_from_video(str(video_file), audio_out)

#             result = transcribe_audio(audio_out)

#             j = f'../../data/processed/transcripts/{video_file.stem}.json'
#             t = f'../../data/processed/transcripts/{video_file.stem}.txt'
#             s = f'../../data/processed/transcripts/{video_file.stem}.srt'

#             save_transcript_json(result, j)
#             save_transcript_txt(result, t)
#             save_transcript_srt(result, s)

#             save_transcript_to_mongo(db, result, str(video_file), 'video',
#                             json_path=j, txt_path=t, srt_path=s)

#         except Exception as e:
#             logging.error(f'Video error: {e}')

#     logging.info('=== Audio/Video Processing Stage Complete ===')

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
    
    #try: 
    #    run_audio_video_stage()
    #except Exception as e:
    #    logging.error(f"Error processing audio/video: {e}")

    # try:
    #     run_analytics()
    # except Exception as e:
    #     logging.error(f"Error in analytics stage: {e}")

    # logging.info("Pipeline finished successfully")

    # try:
    #     logging.info("Cleaning stage started")
    #     from cleaning.clean_pipeline import run_cleaning_pipeline
    #     import pandas as pd
    #     csv_path = "../../data/processed/analytics/products_raw.csv"
    #     df_raw = pd.read_csv(csv_path)
    #     df_cleaned = run_cleaning_pipeline(df_raw, save=True)
    #     logging.info(f"Cleaning stage complete: {len(df_cleaned)} rows")
    # except Exception as e:
    #     logging.error(f"Error in cleaning stage: {e}")

    try:
        logging.info("Lab 10 analytics stage started")
        import pandas as pd
        from analytics.db_connector import get_connection, create_table, populate_reviews, query_reviews
        from analytics.insight_reporter import run_all_questions
        from analytics.mongo_pipeline import run_review_pipeline

        csv_path = "../../data/processed/cleaned/movies_clean.csv"
        df = pd.read_csv(csv_path)
        logging.info(f"Loaded cleaned data: {df.shape}")

        conn = get_connection()
        create_table(conn)
        populate_reviews(conn, df)
        conn.close()
        logging.info("MySQL populated")

        df_mongo_agg = run_review_pipeline()
        logging.info(f"MongoDB pipeline: {len(df_mongo_agg)} rows")

        # Analytical questions
        run_all_questions(df)

        logging.info("Lab 10 analytics stage COMPLETED")
    except Exception as e:
        logging.error(f"Error in Lab 10 analytics stage: {e}")

    logging.info("Pipeline finished successfully")


if __name__ == "__main__":
    run_pipeline()