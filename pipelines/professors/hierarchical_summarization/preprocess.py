"""
Text preprocessing module for review cleaning and deduplication.
"""

import re
import hashlib
from typing import List, Set
from collections import Counter

import numpy as np
from sentence_transformers import SentenceTransformer

from pipelines.professors.hierarchical_summarization.config import (
    MIN_REVIEW_LENGTH,
    DEDUPLICATION_SIMILARITY_THRESHOLD,
    EMBEDDING_MODEL,
)
from pipelines.professors.schemas import ProcessedReview


class ReviewPreprocessor:
    """Preprocesses reviews: normalizes, cleans, and deduplicates"""
    
    def __init__(self):
        # Load embedding model for deduplication
        print("Loading embedding model for deduplication...")
        import torch
        print(f"CUDA available: {torch.cuda.is_available()}")
        if torch.cuda.is_available():
            print(f"CUDA device: {torch.cuda.get_device_name(0)}")
        device = "cuda" if torch.cuda.is_available() else "cpu"
        self.embedding_model = SentenceTransformer(EMBEDDING_MODEL, device=device)
        print(f"Embedding model loaded on {device}")
    
    def normalize_text(self, text: str) -> str:
        """
        Normalize text: remove URLs, emojis, repeated punctuation.
        Preserve sentiment-bearing terms.
        """
        if not text or not isinstance(text, str):
            return ""
        
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # Remove emojis (keep basic punctuation)
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"  # emoticons
            "\U0001F300-\U0001F5FF"  # symbols & pictographs
            "\U0001F680-\U0001F6FF"  # transport & map symbols
            "\U0001F1E0-\U0001F1FF"  # flags
            "\U00002702-\U000027B0"
            "\U000024C2-\U0001F251"
            "]+", flags=re.UNICODE
        )
        text = emoji_pattern.sub('', text)
        
        # Normalize repeated punctuation (keep up to 2)
        text = re.sub(r'!{3,}', '!!', text)
        text = re.sub(r'\?{3,}', '??', text)
        text = re.sub(r'\.{3,}', '..', text)
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        
        return text
    
    def word_count(self, text: str) -> int:
        """Count words in text"""
        return len(text.split())
    
    def filter_by_length(self, reviews: List[ProcessedReview]) -> List[ProcessedReview]:
        """Remove reviews shorter than minimum length"""
        return [
            review for review in reviews
            if review.word_count >= MIN_REVIEW_LENGTH
        ]
    
    def deduplicate_reviews(
        self,
        reviews: List[ProcessedReview],
        similarity_threshold: float = DEDUPLICATION_SIMILARITY_THRESHOLD
    ) -> List[ProcessedReview]:
        """
        Remove near-identical reviews using cosine similarity.
        Keeps the first occurrence of each duplicate group.
        """
        if len(reviews) < 2:
            return reviews
        
        print(f"Deduplicating {len(reviews)} reviews...")
        
        # Generate embeddings for all reviews
        texts = [review.text for review in reviews]
        embeddings = self.embedding_model.encode(
            texts,
            batch_size=64,
            show_progress_bar=True,
            convert_to_numpy=True
        )
        
        # Compute pairwise similarities
        # Normalize embeddings for cosine similarity
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        norms[norms == 0] = 1  # Avoid division by zero
        normalized_embeddings = embeddings / norms
        
        # Compute similarity matrix
        similarity_matrix = np.dot(normalized_embeddings, normalized_embeddings.T)
        
        # Find duplicates
        seen_indices: Set[int] = set()
        unique_reviews: List[ProcessedReview] = []
        
        for i, review in enumerate(reviews):
            if i in seen_indices:
                continue
            
            unique_reviews.append(review)
            
            # Mark similar reviews as seen
            similar_indices = np.where(similarity_matrix[i] >= similarity_threshold)[0]
            seen_indices.update(similar_indices)
        
        removed = len(reviews) - len(unique_reviews)
        print(f"Removed {removed} duplicate reviews ({len(unique_reviews)} unique)")
        
        return unique_reviews
    
    def process_reviews(
        self,
        reviews: List[dict],
        deduplicate: bool = True
    ) -> List[ProcessedReview]:
        """
        Process raw reviews: normalize, clean, filter, and deduplicate.
        
        Args:
            reviews: List of review dicts with keys: id, professor_id, course_code, review_text
            deduplicate: Whether to deduplicate reviews
        
        Returns:
            List of ProcessedReview objects
        """
        processed = []
        for review in reviews:
            original_text = review.get("review_text", "") or ""
            normalized_text = self.normalize_text(original_text)
            word_count = self.word_count(normalized_text)
            
            processed.append(ProcessedReview(
                review_id=review.get("id", ""),
                professor_id=review.get("professor_id", ""),
                course_code=review.get("course_code"),
                text=normalized_text,
                original_text=original_text,
                word_count=word_count
            ))
        
        # Filter by length
        processed = self.filter_by_length(processed)
        
        # Deduplicate if requested
        if deduplicate and len(processed) > 1:
            processed = self.deduplicate_reviews(processed)
        
        return processed

