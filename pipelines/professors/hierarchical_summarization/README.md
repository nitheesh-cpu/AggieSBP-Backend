# Hierarchical Summarization System

A low-hallucination, hierarchical summarization system for professor reviews.

## Architecture

With TAMU AI configured (the production path):

```
Raw Reviews
  → Review stats in Python (review_stats.py)
  → Clean & Deduplicate (preprocess.py)
  → Normalize Course Codes (course_normalizer.py)
  → Group by course, r1..rN IDs, one request per professor (professor_request.py)
  → TAMU AI structured summary (ai_clients.py, response_schema.py)
  → Optional TypeSafe support check (ai_clients.py)
  → Course rows + professor row (pipeline.py)
```

Without TAMU credentials (local fallback):

```
Raw Reviews
  → Clean & Deduplicate → Normalize Course Codes
  → Sentence Embeddings (embeddings.py) → Clustering per course (clustering.py)
  → Cluster-level BART summaries (summarizer.py)
  → Course-level and professor-level summaries (pipeline.py)
```

## Modules

- **preprocess.py**: Text normalization, cleaning, deduplication
- **embeddings.py**: Sentence embedding generation with caching
- **clustering.py**: HDBSCAN-based semantic clustering
- **summarizer.py**: Hierarchical summarization using BART
- **course_normalizer.py**: Course code normalization (aliases, cross-listings, number extraction)
- **professor_request.py**: Course grouping, short review IDs, request splitting, response mapping
- **review_stats.py**: Would-take-again, grades, attendance, textbook, recent-vs-older ratings
- **response_schema.py**: Strict JSON schema for the TAMU response
- **ai_clients.py**: TAMU AI Chat client and optional TypeSafe verifier
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
    "course": "CSCE221",
    "teaching": "...",
    "exams": "...",
    "grading": "...",
    "workload": "...",
    "policies": "...",        # attendance_policies
    "tips": ["..."],
    "review_stats": {"would_take_again_pct": 81.0, "grade_distribution": {"A": 20}, ...},
    "citations": {"exams": [["<rmp review id>", ...]], ...},
    "confidence": 0.89,
    "total_reviews": 45
}
```

### Professor Summary

```python
{
    "professor_id": "...",
    "tldr": "...",
    "overall_sentiment": "Generally positive",
    "strengths": ["Teaching: Clear lectures", "Grading: Fair grading"],
    "complaints": ["Workload: Heavy workload"],   # watch_outs
    "review_stats": {...},
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
- **Resolves subject names and shorthand**: ASTRONOMY111 and ASTRO111 → ASTR111,
  PSYCH107 → PSYC107, Chemistry-119 → CHEM119 (full names come from the
  `departments` table when a session is available). Codes are stored without a
  space, e.g. `ASTR111`.
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

### TAMU AI Chat: one request per professor

Copy `.env.example` to `.env` and provide credentials. When `TAMU_AI_API_KEY`
and `TAMU_AI_MODEL` are set, each professor costs **one** TAMU AI Chat request
(`TAMU_AI_BASE_URL` defaults to `https://chat-api.tamu.ai/openai`). Without TAMU
credentials the pipeline uses the local HDBSCAN + BART path described above.

How the request is built (`professor_request.py`):

- All of a professor's deduplicated reviews are grouped by normalized course,
  newest first, each clipped to `SUMMARY_REVIEW_CLIP_CHARS` (default 600).
  Courses with fewer than 3 reviews share one `OTHER` group and never get a
  request of their own; claims from that group are routed back to the course of
  the reviews they cite.
- Reviews are cited with short local IDs (`r1`..`rN`) that map back to RMP review
  IDs locally. A claim citing any ID that was not in its request is discarded,
  as is any course entry the request did not contain.
- If the prompt would exceed `TAMU_AI_MAX_INPUT_CHARS`, whole course groups are
  packed into as few requests as fit (at most `SUMMARY_MAX_COURSES_PER_REQUEST`
  groups each). A course is never split; a single course larger than the budget
  keeps its newest reviews. A truncated multi-course response is split in half by
  course group; a single-course truncation is retried once with a doubled output
  budget. Schema violations get one strict retry; after that the professor fails
  and is retried on the next run instead of saving a fallback.

The response uses a strict JSON schema aimed at students (`response_schema.py`):
`tldr` (at most 2 sentences), per-course `exams`, `grading`, `workload`,
`attendance_policies`, `teaching`, and `tips[]`, plus overall `strengths[]` and
`watch_outs[]`. Every claim cites IDs and carries its own sentiment (course
claims) or topic (overall claims), so no separate classification call is made.
Course fields are stored in the existing columns (`attendance_policies` in
`policies`, tips in `other` and `tips`); strengths and watch-outs are stored as
`"Topic: text"` in `strengths` and `complaints`. Overall items that repeat a
course claim nearly verbatim are dropped, and openers such as "Other
students..." or "However," that refer to an unseen sentence are rewritten.

Computed in Python, without a model (`review_stats.py`), and stored in
`review_stats` for the overall row and every course row: would-take-again %,
self-reported grade distribution, attendance-mandatory %, textbook use (RMP's
binary encodings since 2016: -1/0 = no, 3/5 = yes), and the average rating for
the last 2 years versus older reviews. The headline sentiment uses review
ratings; "Mixed - varies by course" is used only when two or more courses with at
least 3 reviews differ by a full rating point, otherwise "Mixed reviews".

New columns (`tldr`, `tips`, `review_stats`, `citations`) are added by
`pipelines/professors/migrations/001_structured_professor_summaries.sql`. The ORM
columns are deferred and the upsert skips them until the migration is applied,
so existing readers and writers keep working before it runs.

### TypeSafe verification (optional)

Set `TYPESAFE_VERIFY=1` with `TYPESAFE_API_KEY` to check each generated claim
against its cited reviews. A claim is rejected only when its support probability
is below `TYPESAFE_SUPPORT_THRESHOLD`; overgeneralization and disagreement scores
no longer reject claims. An unanswered check leaves the claim in place (it has
already passed the local citation check). Accepted, citation-rejected,
support-rejected, and unverified counts are logged for each professor.

### Running

For a quota-bounded daily run:

```bash
python -m pipelines.professors.upsert_reviews_and_summaries --max-summaries 25
```

To preview specific professors without writing anything (stored reviews only,
no checkpoint, summaries printed as JSON with request and claim counts):

```bash
python -m pipelines.professors.upsert_reviews_and_summaries --dry-run --professor-id ID1 ID2 ID3
```

Full runs process professors teaching a section in the current or next term
(terms that have not ended and start within about six months, matched from
`section_instructors` by name) before the rest, in database order.

The default console view reports batch progress and one result per professor,
including its TAMU request count and accepted/rejected claims; routine model,
HTTP, and TypeSafe logs are hidden. Add `--verbose` when diagnosing a request.

The runner shares one API queue across all eligible professors in each batch
(at most 50 professors), with four workers by default (`SUMMARY_API_WORKERS=1`
disables overlap; maximum 4). Completed professors are consumed immediately.
Local deduplication, assembly, and database writes stay sequential. TAMU retries
temporary HTTP 500/502/503/504 and connection/timeout failures up to
`TAMU_AI_MAX_RETRIES=2` times with backoff; every attempt consumes the request
budget. A 429 stops new TAMU requests; already in-flight requests may finish.

`--force-update` bypasses the review-count skip. The skip compares the overall
review count only; course rows are keyed by normalized codes, so comparing them
with raw RMP codes used to force needless regeneration. Completed professors are
checkpointed after each save using atomic replacement. Runs with errors retain
their checkpoint and exit unsuccessfully; Ctrl+C preserves completed progress.

Before fetching reviews, the runner reads paginated school-level RMP rating
counts (100 professors per requested page). Review fetching is skipped only when
the fresh count exactly equals the number of stored review IDs. Any difference
or missing count triggers the existing incremental fetch. Failed school scans
fall back to individual checks. This does not detect same-count edits or reconcile
deleted reviews.

To regenerate summaries without contacting RMP, run:

```bash
python -m pipelines.professors.upsert_reviews_and_summaries --stored-reviews-only --force-update
```

`--force-update` controls summary regeneration and retains the count gate.
Stored-only mode bypasses both the count scan and review fetching.

TAMU sometimes returns budget errors inside HTTP 200 responses, with
`error.type=budget_exceeded` and `error.code=400`. These are treated as allowance
stops, including when delivered through SSE: nothing is saved for the
interrupted professor, completed checkpoints remain, and subsequent TAMU requests
are blocked for that client. Resume only after the provider allowance is available.

The job checkpoints progress, stops cleanly on a TAMU HTTP 429 response, and
can resume the next day. Scheduling at `00:10 UTC` runs just after the documented
daily TAMU allowance reset regardless of Central daylight-saving changes.

## Example

Incremental runs defer existing summaries until new stored reviews reach 10% of
the last overall summary's review count, rounded up. Thus 100 summarized reviews
require 10 additions; 10 require 1. Deferred reviews accumulate across runs.
Professors without an overall summary are processed immediately. Configure with
`--min-new-review-percent 5` or `SUMMARY_MIN_NEW_REVIEW_PERCENT`; use `0` to update
after any addition. `--force-update` bypasses the threshold.

On the local BART path, TypeSafe requests are scoped to individual clusters and
bounded using conservative
serialized UTF-8 byte budgets: `TYPESAFE_MAX_STATE_BYTES=24000` and
`TYPESAFE_MAX_REQUEST_BYTES=48000`. These are not exact token counts. Small claims
remain batched; oversized classification inputs are split without truncating
reviews, and verification shards retain the claim's citations while checking
the remaining evidence in groups. Any shard reporting a risk is preserved in
the combined result. Missing verification fails closed there. A single review or set
of citations that cannot fit uses the existing fallback rather than discarding
evidence. Provider token-limit errors also trigger smaller question batches.
Large inputs can therefore require more API calls, within the existing global
concurrency limit.

See `example_usage.py` for a complete example.
