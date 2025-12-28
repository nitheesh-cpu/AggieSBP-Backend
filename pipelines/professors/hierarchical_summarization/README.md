# Hierarchical Summarization System

A low-hallucination, hierarchical summarization system for professor reviews.

## Architecture

```
Raw Reviews
  → Clean & Deduplicate (preprocess.py)
  → Normalize Course Codes (course_normalizer.py)
  → Sentence Embeddings (embeddings.py)
  → Clustering per course (clustering.py)
  → Cluster-level summaries (summarizer.py)
  → Course-level summary (pipeline.py)
  → Professor-level summary (pipeline.py)
```

## Modules

- **preprocess.py**: Text normalization, cleaning, deduplication
- **embeddings.py**: Sentence embedding generation with caching
- **clustering.py**: HDBSCAN-based semantic clustering
- **summarizer.py**: Hierarchical summarization using BART
- **course_normalizer.py**: Course code normalization (aliases, cross-listings, number extraction)
- **pipeline.py**: Main orchestration
- **schemas.py**: Output data structures
- **config.py**: Configuration constants

## Usage

```python
from pipelines.professors.hierarchical_summarization import HierarchicalSummarizationPipeline

pipeline = HierarchicalSummarizationPipeline()

# Process all reviews for a professor
professor_summary = pipeline.process_professor_reviews(raw_reviews, professor_id)

# Or process a single course
course_summary = pipeline.process_single_course(course_reviews, course_code)
```

## Output Format

### Course Summary

```python
{
    "course": "CSCE 221",
    "teaching": "...",
    "exams": "...",
    "grading": "...",
    "workload": "...",
    "confidence": 0.89,
    "total_reviews": 45
}
```

### Professor Summary

```python
{
    "professor_id": "...",
    "overall_sentiment": "Generally positive",
    "strengths": ["Clear lectures", "Fair grading"],
    "complaints": ["Heavy workload"],
    "consistency": "Varies by course difficulty",
    "confidence": 0.93,
    "course_summaries": [...]
}
```

## Performance

- **CPU-only**: Optimized for ARM Ampere
- **Caching**: Aggressive caching of embeddings and summaries
- **Memory**: < 10GB peak usage
- **Deterministic**: Same input produces same output

## Dependencies

- sentence-transformers
- hdbscan
- transformers (BART)
- torch (CUDA support if available)

## Course Code Normalization

The system includes intelligent course code normalization that:

- **Normalizes department aliases**: CPSC → CSCE, CPCS → CSCE, ELEN → ECEN, etc.
- **Extracts course numbers**: Handles malformed numbers like "221222" → "221"
- **Uses cross-listings**: Loads cross-listings from the database to group courses like CSCE 222 and ECEN 222
- **Groups variations**: Reviews for "CSCE221", "CPSC221", "CPCS221" all get grouped together

This ensures that reviews for the same course (with different naming conventions) are properly grouped for summarization.

## Configuration

Set environment variables:

```bash
export OMP_NUM_THREADS=4
export TOKENIZERS_PARALLELISM=true
```

## Example

See `example_usage.py` for a complete example.
