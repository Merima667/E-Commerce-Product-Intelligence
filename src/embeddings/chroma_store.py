import chromadb
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
import pandas as pd
from pathlib import Path
from embeddings.embedder import build_product_text

CHROMA_PATH = Path("../../data/embeddings/chroma_db")
COLLECTION_NAME = "products"


def get_chroma_client():
    CHROMA_PATH.mkdir(parents=True, exist_ok=True)
    return chromadb.PersistentClient(path=str(CHROMA_PATH))


def get_collection(client=None, reset=False):
    if client is None:
        client = get_chroma_client()

    ef = SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")

    if reset:
        try:
            client.delete_collection(COLLECTION_NAME)
            print(f"Deleted existing collection '{COLLECTION_NAME}'")
        except Exception:
            pass

    collection = client.get_or_create_collection(
        name=COLLECTION_NAME,
        embedding_function=ef,
        metadata={"hnsw:space": "cosine"}  
    )
    return collection


def add_products_to_collection(df, collection, batch_size=100):
    existing_ids = set(collection.get()["ids"])
    print(f"Collection already has {len(existing_ids)} products")

    documents, metadatas, ids = [], [], []

    for _, row in df.iterrows():
        product_id = f"review_{row['review_id']}_{row.name}" if pd.notna(row.get("review_id")) else f"row_{row.name}"

        if product_id in existing_ids:
            continue

        text = build_product_text(row)
        if not text or text == 'Unknown product':
            continue

        meta = {
            "title":    str(row.get("title", "Unknown"))[:500],
            "source":   str(row.get("source", "Unknown"))[:200],
            "rating":   float(pd.to_numeric(row.get("rating"), errors='coerce') or 0.0),
            "category": str(row.get("category", "Unknown"))[:100],
        }

        documents.append(text)
        metadatas.append(meta)
        ids.append(product_id)

        if len(documents) >= batch_size:
            collection.add(documents=documents, metadatas=metadatas, ids=ids)
            documents, metadatas, ids = [], [], []

    if documents:
        collection.add(documents=documents, metadatas=metadatas, ids=ids)

    total = collection.count()
    print(f"Collection now contains {total} products")
    return total