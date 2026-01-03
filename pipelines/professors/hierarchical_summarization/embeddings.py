"""
Embedding generation module with caching.
"""

import os
import pickle
import hashlib
from typing import List, Optional
import numpy as np
from sentence_transformers import SentenceTransformer

from pipelines.professors.hierarchical_summarization.config import (
    EMBEDDING_MODEL,
    EMBEDDING_BATCH_SIZE,
    EMBEDDINGS_CACHE_DIR,
)


class EmbeddingGenerator:
    """Generates and caches sentence embeddings"""

    def __init__(self):
        print(f"Loading embedding model: {EMBEDDING_MODEL}")
        import torch

        print(f"CUDA available: {torch.cuda.is_available()}")
        if torch.cuda.is_available():
            print(f"CUDA device: {torch.cuda.get_device_name(0)}")
        device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = SentenceTransformer(EMBEDDING_MODEL, device=device)
        print(f"Embedding model loaded on {device}")

        # Ensure cache directory exists
        os.makedirs(EMBEDDINGS_CACHE_DIR, exist_ok=True)

    def _get_cache_path(self, review_id: str) -> str:
        """Get cache file path for a review ID"""
        # Use hash to avoid long filenames
        cache_key = hashlib.md5(review_id.encode()).hexdigest()
        return os.path.join(EMBEDDINGS_CACHE_DIR, f"{cache_key}.pkl")

    def _load_cached_embedding(self, review_id: str) -> Optional[np.ndarray]:
        """Load cached embedding if it exists"""
        cache_path = self._get_cache_path(review_id)
        if os.path.exists(cache_path):
            try:
                with open(cache_path, "rb") as f:
                    return pickle.load(f)
            except Exception as e:
                print(f"Error loading cache for {review_id}: {e}")
                return None
        return None

    def _save_embedding(self, review_id: str, embedding: np.ndarray):
        """Save embedding to cache"""
        cache_path = self._get_cache_path(review_id)
        try:
            with open(cache_path, "wb") as f:
                pickle.dump(embedding, f)
        except Exception as e:
            print(f"Error saving cache for {review_id}: {e}")

    def generate_embeddings(
        self, texts: List[str], review_ids: List[str], use_cache: bool = True
    ) -> np.ndarray:
        """
        Generate embeddings for texts, using cache when available.

        Args:
            texts: List of text strings
            review_ids: List of review IDs for caching
            use_cache: Whether to use cached embeddings

        Returns:
            numpy array of embeddings (n_reviews, embedding_dim)
        """
        if len(texts) != len(review_ids):
            raise ValueError("texts and review_ids must have same length")

        embeddings = np.zeros(
            (len(texts), self.model.get_sentence_embedding_dimension())
        )
        texts_to_encode = []
        indices_to_encode = []
        cached_count = 0

        # Check cache first
        if use_cache:
            for i, (text, review_id) in enumerate(zip(texts, review_ids)):
                cached_embedding = self._load_cached_embedding(review_id)
                if cached_embedding is not None:
                    embeddings[i] = cached_embedding
                    cached_count += 1
                else:
                    texts_to_encode.append(text)
                    indices_to_encode.append(i)
        else:
            texts_to_encode = texts
            indices_to_encode = list(range(len(texts)))

        # Generate embeddings for uncached texts
        if texts_to_encode:
            new_embeddings = self.model.encode(
                texts_to_encode,
                batch_size=EMBEDDING_BATCH_SIZE,
                show_progress_bar=True,
                convert_to_numpy=True,
            )

            # Store in result array and cache
            for idx, embedding in zip(indices_to_encode, new_embeddings):
                embeddings[idx] = embedding
                if use_cache:
                    self._save_embedding(review_ids[idx], embedding)

        return embeddings

    def generate_embeddings_for_reviews(
        self, reviews: List, use_cache: bool = True
    ) -> np.ndarray:
        """
        Generate embeddings for a list of processed reviews.

        Args:
            reviews: List of ProcessedReview objects
            use_cache: Whether to use cached embeddings

        Returns:
            numpy array of embeddings
        """
        texts = [review.text for review in reviews]
        review_ids = [review.review_id for review in reviews]
        return self.generate_embeddings(texts, review_ids, use_cache)
