"""
Hierarchical summarization module using BART.
"""

import re
from typing import List, Dict
import torch
from transformers import BartForConditionalGeneration, BartTokenizer

from pipelines.professors.hierarchical_summarization.config import (
    SUMMARIZATION_MODEL,
)
from pipelines.professors.schemas import (
    ProcessedReview,
    ClusterSummary,
)


class HierarchicalSummarizer:
    """Generates summaries using BART in a hierarchical manner"""

    def __init__(self):
        # Check CUDA availability BEFORE loading model
        if torch.cuda.is_available():
            self.device = torch.device("cuda")
        else:
            self.device = torch.device("cpu")

        self.tokenizer = BartTokenizer.from_pretrained(SUMMARIZATION_MODEL)
        self.model = BartForConditionalGeneration.from_pretrained(SUMMARIZATION_MODEL)

        # Set to evaluation mode
        self.model.eval()

        # Move model to device
        self.model.to(self.device)

    def _chunk_text(self, text: str, max_length: int = 512) -> List[str]:
        """Split text into chunks that fit within token limit"""
        # Simple sentence-based chunking
        sentences = re.split(r"[.!?]+", text)
        chunks = []
        current_chunk = []
        current_length = 0

        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue

            # Rough token estimate (1 token ≈ 4 characters)
            sentence_length = len(sentence) // 4

            if current_length + sentence_length > max_length and current_chunk:
                chunks.append(" ".join(current_chunk))
                current_chunk = [sentence]
                current_length = sentence_length
            else:
                current_chunk.append(sentence)
                current_length += sentence_length

        if current_chunk:
            chunks.append(" ".join(current_chunk))

        return chunks

    def _summarize_text(
        self, text: str, max_length: int = 102, min_length: int = 56
    ) -> str:
        """
        Summarize a single text using BART.
        BART uses token-based lengths, so we convert character lengths to tokens.
        """
        if not text or len(text.strip()) < min_length:
            return text

        try:
            # Tokenize
            inputs = self.tokenizer(
                text,
                max_length=1024,  # BART's max input length
                truncation=True,
                padding=True,
                return_tensors="pt",
            ).to(self.device)

            # Generate summary
            with torch.no_grad():
                summary_ids = self.model.generate(
                    inputs["input_ids"],
                    max_length=max_length,  # Token-based max length
                    min_length=min_length,  # Token-based min length
                    num_beams=4,
                    length_penalty=2.0,  # Prefer longer summaries for BART
                    early_stopping=True,
                    do_sample=False,
                )

            # Decode
            summary = self.tokenizer.decode(summary_ids[0], skip_special_tokens=True)
            return summary.strip()

        except Exception as e:
            print(f"Error summarizing text: {e}")
            # Fallback: return first few sentences from actual text
            sentences = re.split(r"[.!?]+", text)
            return ". ".join(sentences[:3]) + "."

    def _summarize_chunks(self, chunks: List[str]) -> str:
        """Summarize multiple chunks and merge"""
        if len(chunks) == 1:
            return self._summarize_text(chunks[0])

        # Summarize each chunk
        chunk_summaries = []
        for chunk in chunks:
            # Use token-based lengths for BART (approximately 4 chars per token)
            summary = self._summarize_text(chunk, max_length=80, min_length=30)
            chunk_summaries.append(summary)

        # Merge chunk summaries
        merged = " ".join(chunk_summaries)

        # If merged is still too long, summarize again
        if len(merged) > 400:
            return self._summarize_text(merged, max_length=120, min_length=40)

        return merged

    def _extractive_summary(
        self, reviews: List[ProcessedReview], max_sentences: int = 3
    ) -> str:
        """
        Create an extractive summary by selecting key sentences from reviews.
        Used as fallback for very small clusters or when abstractive summarization fails.
        """
        all_sentences = []
        for review in reviews:
            if review.text:
                sentences = re.split(r"[.!?]+", review.text)
                for sent in sentences:
                    sent = sent.strip()
                    if len(sent) > 20:  # Only meaningful sentences
                        all_sentences.append(sent)

        # Return first N sentences that give a good overview
        selected = all_sentences[:max_sentences]
        return ". ".join(selected) + "." if selected else "No review text available."

    def summarize_cluster(
        self, cluster_reviews: List[ProcessedReview], cluster_type: str
    ) -> ClusterSummary:
        """
        Summarize a single cluster of reviews.

        Args:
            cluster_reviews: List of reviews in the cluster
            cluster_type: Semantic type of the cluster

        Returns:
            ClusterSummary object
        """
        if not cluster_reviews:
            return ClusterSummary(
                cluster_type=cluster_type,
                summary="No reviews in cluster",
                review_count=0,
                sentiment="neutral",
                confidence=0.0,
            )

        # Combine review texts (needed for sentiment analysis)
        combined_text = " ".join([review.text for review in cluster_reviews])

        # For very small clusters (< 3 reviews), use extractive summarization
        if len(cluster_reviews) < 3:
            summary = self._extractive_summary(cluster_reviews, max_sentences=3)
        else:
            # Chunk if necessary (BART max input is 1024 tokens, roughly 3000-4000 chars)
            chunks = self._chunk_text(combined_text, max_length=3000)

            # Summarize - use appropriate token lengths for BART
            if len(chunks) == 1:
                # For single chunk, use token-based lengths (roughly 4 chars per token)
                summary = self._summarize_text(chunks[0], max_length=102, min_length=30)
            else:
                summary = self._summarize_chunks(chunks)

            # Validate summary - if it contains academic paper phrases, fall back to extractive
            academic_phrases = [
                "in this paper",
                "we study",
                "we show",
                "we propose",
                "abstract",
                "introduction to",
            ]
            if any(phrase in summary.lower() for phrase in academic_phrases):
                print(
                    "Warning: Summary contains academic phrases, using extractive fallback"
                )
                summary = self._extractive_summary(cluster_reviews, max_sentences=4)

        # Determine sentiment (simple heuristic)
        text_lower = combined_text.lower()
        positive_words = [
            "good",
            "great",
            "excellent",
            "amazing",
            "love",
            "best",
            "helpful",
            "clear",
        ]
        negative_words = [
            "bad",
            "terrible",
            "awful",
            "worst",
            "hate",
            "confusing",
            "unclear",
            "difficult",
        ]

        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)

        if positive_count > negative_count * 1.5:
            sentiment = "positive"
        elif negative_count > positive_count * 1.5:
            sentiment = "negative"
        else:
            sentiment = "mixed"

        # Confidence based on cluster size
        confidence = min(0.95, 0.5 + (len(cluster_reviews) / 20) * 0.45)

        return ClusterSummary(
            cluster_type=cluster_type,
            summary=summary,
            review_count=len(cluster_reviews),
            sentiment=sentiment,
            confidence=confidence,
        )

    def summarize_clusters(
        self, clusters: Dict[int, List[ProcessedReview]], cluster_types: Dict[int, str]
    ) -> List[ClusterSummary]:
        """
        Summarize multiple clusters.

        Args:
            clusters: Dictionary mapping cluster_id to reviews
            cluster_types: Dictionary mapping cluster_id to cluster type

        Returns:
            List of ClusterSummary objects
        """
        summaries = []

        for cluster_id, cluster_reviews in clusters.items():
            cluster_type = cluster_types.get(cluster_id, "other")
            summary = self.summarize_cluster(cluster_reviews, cluster_type)
            summaries.append(summary)

        return summaries
