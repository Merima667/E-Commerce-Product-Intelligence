import pandas as pd
from embeddings.chroma_store import get_chroma_client, get_collection


def semantic_search(query, n_results=10, filters=None, collection=None):
    if collection is None:
        client = get_chroma_client()
        collection = get_collection(client)

    kwargs = {"query_texts": [query], "n_results": min(n_results, collection.count())}
    if filters:
        kwargs["where"] = filters

    results = collection.query(**kwargs, include=["documents", "metadatas", "distances"])

    rows = []
    for doc, meta, dist in zip(
        results["documents"][0],
        results["metadatas"][0],
        results["distances"][0]
    ):
        rows.append({
            "title":      meta.get("title", "Unknown"),
            "source":     meta.get("source", "Unknown"),
            "rating":     meta.get("rating", 0.0),
            "category":   meta.get("category", "Unknown"),
            "similarity": round(1 - dist, 4),
            "text":       doc[:200],
        })
    return pd.DataFrame(rows)


def keyword_search(query, df, text_col="comment", n_results=10):
    query_lower = query.lower()
    mask = (
        df["title"].str.lower().str.contains(query_lower, na=False) |
        df[text_col].str.lower().str.contains(query_lower, na=False)
    )
    results = df[mask][["title", "rating", "source", "category"]].copy()
    results["search_type"] = "keyword"
    return results.head(n_results).reset_index(drop=True)


def compare_search(query, df, collection=None, n_results=5):
    kw = keyword_search(query, df, n_results=n_results)
    sem = semantic_search(query, n_results=n_results, collection=collection)

    print(f"--- Query: '{query}' ---")
    print()
    print(f"Keyword search found {len(kw)} results:")
    for _, row in kw.iterrows():
        print(f"  {row['title']} | rating: {row['rating']} | source: {row['source']}")

    print()
    print(f"Semantic search found {len(sem)} results:")
    for _, row in sem.iterrows():
        print(f"  [{row['similarity']:.3f}] {row['title']} | rating: {row['rating']}")

    return {"keyword": kw, "semantic": sem}