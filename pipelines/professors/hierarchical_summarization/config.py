"""
Configuration constants for hierarchical summarization system.
"""

import os

# Model configurations
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
# Using BART instead of Pegasus-arxiv for better general text summarization
# Pegasus-arxiv was generating academic paper-style summaries instead of review summaries
SUMMARIZATION_MODEL = "facebook/bart-large-cnn"

# Batch sizes
EMBEDDING_BATCH_SIZE = 64
SUMMARIZATION_BATCH_SIZE = 4

# Clustering parameters
HDBSCAN_MIN_CLUSTER_SIZE = 3
HDBSCAN_MIN_SAMPLES = 2

# Preprocessing parameters
MIN_REVIEW_LENGTH = 8  # words
DEDUPLICATION_SIMILARITY_THRESHOLD = 0.95

# Cache directories
CACHE_DIR = os.path.join(os.path.dirname(__file__), ".cache")
EMBEDDINGS_CACHE_DIR = os.path.join(CACHE_DIR, "embeddings")
SUMMARIES_CACHE_DIR = os.path.join(CACHE_DIR, "summaries")

# Performance settings
OMP_NUM_THREADS = int(os.getenv("OMP_NUM_THREADS", "4"))
TOKENIZERS_PARALLELISM = os.getenv("TOKENIZERS_PARALLELISM", "true")

# Cluster types to identify
CLUSTER_TYPES = [
    "teaching",
    "exams",
    "grading",
    "workload",
    "personality",
    "policies",
    "other"
]

# Keywords for cluster type identification
CLUSTER_KEYWORDS = {
    "teaching": ["teach", "lecture", "explain", "clear", "confusing", "understand", "present"],
    "exams": ["exam", "test", "quiz", "midterm", "final", "assessment"],
    "grading": ["grade", "point", "curve", "fair", "harsh", "strict", "lenient"],
    "workload": ["work", "homework", "assignment", "project", "busy", "time", "heavy", "light"],
    "personality": ["nice", "friendly", "helpful", "approachable", "rude", "mean", "strict"],
    "policies": ["attendance", "late", "policy", "rule", "allow", "permit", "require"]
}

