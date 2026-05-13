from sentence_transformers import SentenceTransformer
import numpy as np

MODEL_NAME = "all-MiniLM-L6-v2"
_model = None 


def get_model():
    global _model
    if _model is None:
        print(f"Loading model: {MODEL_NAME}")
        _model = SentenceTransformer(MODEL_NAME)
        print("Model loaded successfully")
    return _model


def build_product_text(row):
    parts = []
    if 'title' in row and str(row['title']) != 'nan':
        parts.append(str(row['title']))
    if 'comment' in row and str(row['comment']) != 'nan':
        parts.append(str(row['comment']))
    if 'category' in row and str(row['category']) != 'nan':
        parts.append(f"Category: {row['category']}")
    if 'rating' in row and str(row['rating']) != 'nan':
        parts.append(f"Rating: {row['rating']}")
    return ' | '.join(parts) if parts else 'Unknown product'


def embed_texts(texts, batch_size=64, normalize=True):
    model = get_model()
    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        normalize_embeddings=normalize,
        show_progress_bar=len(texts) > 100
    )
    return embeddings


def embed_single(text, normalize=True):
    model = get_model()
    return model.encode(text, normalize_embeddings=normalize)