import numpy as np
import logging

logger = logging.getLogger(__name__)


def demonstrate_array_creation() -> dict:
    logger.info('Demonstrating NumPy array creation methods')
    ratings = np.array([4.5, 3.2, 5.0, 2.8, 4.1, 3.9])
    price_placeholder = np.zeros(6)
    category_weights = np.ones((3, 4))  
    years = np.arange(2010, 2025)    
    score_buffer = np.empty((2, 3))

    logger.info('Array creation complete')
    return {
        'ratings':          ratings,
        'price_placeholder': price_placeholder,
        'category_weights':       category_weights,
        'years':               years,
        'score_buffer':        score_buffer,
    }


def print_array_info(arrays: dict) -> None:
    for name, arr in arrays.items():
        print(f'  {name:<22s} shape={str(arr.shape):<12} dtype={arr.dtype}  ndim={arr.ndim}  size={arr.size}')


def vectorized_operations(ratings: np.ndarray, review_count: np.ndarray) -> dict:
    logger.info('Running vectorized operations')

    normalised = ratings * 10
    weighted   = ratings * np.log(review_count)
    high_rated = ratings > 4.0
    quality    = (ratings > 3.5) & (review_count > 100)

    stats = {
        'mean':  float(ratings.mean()),
        'std':   float(ratings.std()),
        'max':   float(ratings.max()),
        'min_votes': int(review_count.min()),
        'total_votes': int(review_count.sum()),
    }

    logger.info('Vectorized operations complete: mean=%.2f', stats['mean'])
    return {
        'normalised':  normalised,
        'weighted':    np.round(weighted, 2),
        'high_rated':  high_rated,
        'quality':     quality,
        'stats':       stats,
    }


def axis_reductions(matrix: np.ndarray) -> dict:
    col_means = matrix.mean(axis=0)   # one mean per column
    row_means = matrix.mean(axis=1)   # one mean per row
    col_stds  = matrix.std(axis=0)
    return {
        'col_means': col_means,
        'row_means': row_means,
        'col_stds':  col_stds,
    }


def broadcasting_example(vote_avg: np.ndarray) -> np.ndarray:
    min_v, max_v = vote_avg.min(), vote_avg.max()
    if max_v == min_v:
        return np.zeros_like(vote_avg, dtype=float)
    return (vote_avg - min_v) / (max_v - min_v)


if __name__ == "__main__":
    arrays = demonstrate_array_creation()
    print("=== Array Info ===")
    print_array_info(arrays)

    vote_avg   = np.array([4.5, 3.2, 5.0, 2.8, 4.1, 3.9, 4.7, 2.5, 5.0, 3.6])
    vote_count = np.array([120, 45, 200, 30, 89, 67, 150, 25, 310, 55])

    print("\n=== Vectorized Operations ===")
    results = vectorized_operations(vote_avg, vote_count)
    for k, v in results['stats'].items():
        print(f"  {k}: {v}")

    print("\n=== Axis Reductions ===")
    matrix = np.vstack([vote_avg, vote_count])
    reductions = axis_reductions(matrix)
    for k, v in reductions.items():
        print(f"  {k}: {v}")

    print("\n=== Broadcasting (normalized) ===")
    normalized = broadcasting_example(vote_avg)
    print(f"  {normalized.round(3)}")