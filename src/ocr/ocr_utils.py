import os

import pytesseract

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src')))
from storage.mongo import save_to_mongo
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
from PIL import Image, ImageFilter
import pytesseract
from pdf2image import convert_from_path
import pdfplumber

def show_image(image):
    image.show()

def ocr_raw(image_path):
    """OCR without preprocessing - raw result"""
    img = Image.open(image_path)
    text = pytesseract.image_to_string(img, lang="eng")
    return text

def preprocess_image(image_path):
    """Preprocessing: greyscale->denoise->binarize"""
    img = Image.open(image_path)
    img = img.convert("L")
    img = img.filter(ImageFilter.MedianFilter(size=3))
    img = img.point(lambda x: 0 if x < 90 else 255, "1")
    return img

def ocr_preprocessed(image_path):
    """OCR with preprocessingom — better result"""
    processed = preprocess_image(image_path)
    text = pytesseract.image_to_string(processed, lang="eng")
    return text

def compare_ocr(image_path):
    """Comparison raw vs preprocessed OCR output"""
    print("=== RAW OCR ===")
    raw = ocr_raw(image_path)
    print(raw)

    print("=== PREPROCESSED OCR ===")
    preprocessed = ocr_preprocessed(image_path)
    print(preprocessed)

    save_to_mongo(
        {"raw": raw, "preprocessed": preprocessed},
        image_path,
        extra_metadata={
            "file_name": os.path.basename(image_path),
            "type": "ocr_image"
        }
    )

    return raw, preprocessed

def ocr_scanned_pdf(pdf_path, dpi=300):
    """OCR for scanned pdf"""

    pages = convert_from_path(pdf_path, dpi=dpi, poppler_path=r'C:\poppler\bin')
    if(len(pages)==1):
        print(f"PDF has {len(pages)} page")
    else:
        print(f"PDF have {len(pages)} pages")

    page_texts = {}

    for i, page in enumerate(pages):
        print(f"OCR page {i+1}...")

        #preprocessing
        page = page.convert("L")
        page = page.filter(ImageFilter.MedianFilter(size=3))
        page = page.point(lambda x: 0 if x < 128 else 255, "1")

        text = pytesseract.image_to_string(page, lang="eng")
        page_texts[f"page_{i+1}"] = text

        save_to_mongo(
            {"text": text},
            pdf_path,
            extra_metadata={
                "file_name": os.path.basename(pdf_path),
                "type": "ocr_pdf",
                "page_number": i + 1
            }
        )
    
    return page_texts

if __name__ == "__main__":
    print("=== OCR ON IMAGE ===")
    compare_ocr("../../data/raw/images/test_scan.png")

    with pdfplumber.open("../../data/raw/scanned/sample.pdf") as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text()
            print(f"Page {i+1}: {repr(text)}")
    
    print("\n--- OCR now ---\n")

    texts = ocr_scanned_pdf("../../data/raw/scanned/sample.pdf")
    for page, text in texts.items():
        print(f"\n=== {page} ===")
        print(text)