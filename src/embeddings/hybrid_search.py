import pandas as pd
from embeddings.search_engine import keyword_search, semantic_search


def reciprocal_rank_fusion(kw_results, sem_results, k=60):
    scores = {}
    metadata = {}

    for rank, (_, row) in enumerate(kw_results.iterrows()):
        title = row["title"]
        scores[title]   = scores.get(title, 0) + 1.0 / (k + rank + 1)
        metadata[title] = row.to_dict()

    for rank, (_, row) in enumerate(sem_results.iterrows()):
        title = row["title"]
        scores[title]   = scores.get(title, 0) + 1.0 / (k + rank + 1)
        if title not in metadata:
            metadata[title] = row.to_dict()

    rows = []
    for title, score in sorted(scores.items(), key=lambda x: -x[1]):
        row = metadata[title].copy()
        row["rrf_score"] = round(score, 6)
        rows.append(row)

    return pd.DataFrame(rows)


def hybrid_search(query, df, collection, n_results=10, k=60):
    n_candidates = n_results * 3
    kw  = keyword_search(query, df,  n_results=n_candidates)
    sem = semantic_search(query,       n_results=n_candidates, collection=collection)

    combined = reciprocal_rank_fusion(kw, sem, k=k)
    return combined.head(n_results).reset_index(drop=True)