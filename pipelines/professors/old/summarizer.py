"""
Summarizer for professors pipeline.

Contains the ReviewSummarizer class for generating summaries from reviews using
hybrid extractive + abstractive approaches with BART model.
"""

import os
import re
from collections import Counter
from typing import Dict, List, Optional, Tuple

import numpy as np
import torch
from dotenv import load_dotenv
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from transformers import BartForConditionalGeneration, BartTokenizer
from typing import Set, Any, Union

from aggiermp.database.base import ReviewDB
from pipelines.professors.schemas import ReviewData

load_dotenv()


class ReviewSummarizer:
    def __init__(self, model_name: str = "facebook/bart-large-cnn"):
        """
        Initialize the review summarizer with database connection and BART model

        Args:
            model_name: HuggingFace model name for BART
        """
        # Database setup
        url = "postgresql://{0}:{1}@{2}:{3}/{4}".format(
            os.getenv("POSTGRES_USER"),
            os.getenv("POSTGRES_PASSWORD"),
            os.getenv("POSTGRES_HOST"),
            os.getenv("POSTGRES_PORT"),
            os.getenv("POSTGRES_DATABASE"),
        )
        self.engine = create_engine(url)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        # Course normalization mappings
        self.dept_aliases = {
            # Computer Science department changes
            "CPCS": "CSCE",
            "CPSC": "CSCE",
            "COSC": "CSCE",
            "CS": "CSCE",
            # Electrical Engineering variations
            "ELEN": "ECEN",
            "EE": "ECEN",
            "ELEC": "ECEN",
            # Mathematics variations
            "MATH": "MATH",
            "MATHS": "MATH",
            # Engineering variations
            "ENGR": "ENGR",
            "ENGI": "ENGR",
            # Physics variations
            "PHYS": "PHYS",
            "PHY": "PHYS",
            # Chemistry variations
            "CHEM": "CHEM",
            "CHM": "CHEM",
            # Business variations
            "MGMT": "MGMT",
            "MKTG": "MKTG",
            "FINC": "FINC",
            "ACCT": "ACCT",
            "ID": "IDIS",
        }

        # Known cross-listings (courses that are the same but different departments)
        self.cross_listings = {
            # Computer Science/Electrical Engineering cross-listings
            ("CSCE", "222"): "CSCE222",  # Digital Logic Design
            ("ECEN", "222"): "CSCE222",
            ("CSCE", "314"): "CSCE314",  # Programming Languages
            ("ECEN", "314"): "CSCE314",
            # Add more known cross-listings here
            # Format: (department, number): 'canonical_code'
        }

        # Load BART model and tokenizer
        print("Loading BART model...")
        self.tokenizer = BartTokenizer.from_pretrained(model_name)
        self.model = BartForConditionalGeneration.from_pretrained(model_name)

        # Move to GPU if available
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model.to(self.device)
        print(f"Model loaded on {self.device}")

    def extract_key_sentences(
        self,
        reviews: List[ReviewData],
        num_sentences: int = 20,
        is_course_specific: bool = False,
    ) -> List[str]:
        """
        Extract most representative sentences using TF-IDF and diversity selection

        Args:
            reviews: List of review data objects
            num_sentences: Number of key sentences to extract
            is_course_specific: If True, skip course contextualization

        Returns:
            List of most important and diverse sentences
        """
        all_sentences = []
        sentence_metadata = []  # Track metadata for each sentence

        # Step 1: Extract sentences from all reviews
        for review in reviews:
            if review.review_text and len(review.review_text.strip()) > 10:
                # Split into sentences using multiple delimiters
                sentences = re.split(r"[.!?]+", review.review_text)

                for sentence in sentences:
                    sentence = sentence.strip()
                    if len(sentence) > 20:  # Filter out very short sentences
                        # Add minimal context only for overall summaries
                        if not is_course_specific and review.course_code:
                            contextualized = f"In {review.course_code}: {sentence}"
                        else:
                            # For course-specific summaries, use original sentence
                            contextualized = sentence

                        all_sentences.append(contextualized)
                        sentence_metadata.append(
                            {
                                "review_id": review.id,
                                "course_code": review.course_code,
                                "clarity_rating": review.clarity_rating,
                                "difficulty_rating": review.difficulty_rating,
                                "grade": review.grade,
                                "original_sentence": sentence,
                            }
                        )

        if len(all_sentences) <= num_sentences:
            return all_sentences

        try:
            # Step 2: Use TF-IDF to calculate sentence importance
            print(f"Analyzing {len(all_sentences)} sentences for key content...")

            vectorizer = TfidfVectorizer(
                max_features=1000,
                stop_words="english",
                ngram_range=(1, 2),  # Include both unigrams and bigrams
                min_df=1,  # Include terms that appear at least once
                max_df=0.8,  # Exclude terms that appear in >80% of sentences
            )

            tfidf_matrix = vectorizer.fit_transform(all_sentences)

            # Calculate base importance scores (sum of TF-IDF values)
            sentence_scores = np.array(tfidf_matrix.sum(axis=1)).flatten()

            # Step 3: Add bonus scores for sentences with valuable metadata
            for i, metadata in enumerate(sentence_metadata):
                bonus = 0.0

                # Boost sentences from reviews with extreme ratings
                if metadata["clarity_rating"]:
                    try:
                        rating = float(metadata["clarity_rating"])
                        if rating <= 2 or rating >= 4:
                            bonus += 0.2
                    except (ValueError, TypeError):
                        pass

                # Boost sentences mentioning specific grades
                if metadata["grade"] and metadata["grade"] in ["A", "F", "A+", "A-"]:
                    bonus += 0.1

                # Boost sentences from courses (more specific context)
                if metadata["course_code"]:
                    bonus += 0.1

                sentence_scores[i] += bonus

            # Step 4: Select diverse, high-scoring sentences
            similarity_matrix = cosine_similarity(tfidf_matrix)

            selected_indices: List[int] = []
            remaining_indices = list(range(len(all_sentences)))

            # Start with the highest scoring sentence
            best_idx = int(np.argmax(sentence_scores))
            selected_indices.append(best_idx)
            if best_idx in remaining_indices:
                remaining_indices.remove(best_idx)

            # Select remaining sentences balancing importance and diversity
            while len(selected_indices) < num_sentences and remaining_indices:
                best_score = -1.0
                best_idx = -1

                for idx in remaining_indices:
                    # Base importance score
                    base_score = sentence_scores[idx]

                    # Calculate maximum similarity to already selected sentences
                    max_similarity = max(
                        [
                            similarity_matrix[idx][selected_idx]
                            for selected_idx in selected_indices
                        ]
                    )

                    # Diversity penalty (stronger penalty for very similar sentences)
                    diversity_penalty = max_similarity * 0.8

                    # Final adjusted score
                    adjusted_score = base_score * (1 - diversity_penalty)

                    if adjusted_score > best_score:
                        best_score = adjusted_score
                        best_idx = idx

                if best_idx != -1:
                    selected_indices.append(best_idx)
                    remaining_indices.remove(best_idx)
                else:
                    break

            # Step 5: Sort selected sentences by their original importance for better flow
            selected_with_scores = [
                (idx, sentence_scores[idx]) for idx in selected_indices
            ]
            selected_with_scores.sort(key=lambda x: x[1], reverse=True)

            selected_sentences = [all_sentences[idx] for idx, _ in selected_with_scores]

            print(
                f"Extracted {len(selected_sentences)} key sentences from {len(all_sentences)} total sentences"
            )
            return selected_sentences

        except Exception as e:
            print(f"Error in extractive summarization: {e}")
            # Fallback to simple selection by length and position
            sentence_lengths = [len(s) for s in all_sentences]
            avg_length = np.mean(sentence_lengths)

            # Select sentences that are around average length (not too short/long)
            good_sentences = [
                s
                for s, length in zip(all_sentences, sentence_lengths)
                if avg_length * 0.7 <= length <= avg_length * 1.5
            ]

            return good_sentences[:num_sentences]

    def generate_abstractive_summary(self, text: str, max_length: int = 8000) -> str:
        """
        Generate abstractive summary using BART model with 8000 character limit

        Args:
            text: Input text to summarize
            max_length: Maximum length of generated summary (in characters, not tokens)

        Returns:
            Generated summary text
        """
        if not text.strip():
            return "No text available for summarization."

        try:
            # Calculate token-based max_length (roughly 4 chars per token)
            # remove the metadata from the text
            text = re.sub(r"^[^\w\s]", "", text)
            text = re.sub(r"^\s+", "", text)
            text = re.sub(r"Course: \w+", "", text)
            text = re.sub(r"Grade: \w+\+\.", "", text)
            text = re.sub(r"Grade: \w+\.", "", text)
            text = re.sub(r"(\s\W\s)", "", text)
            text = re.sub(r"In \w+:", "", text)

            token_max_length = min(max_length // 4, 1024)  # BART's max output length

            # Tokenize input with truncation
            inputs = self.tokenizer.encode(
                text, return_tensors="pt", max_length=1024, truncation=True
            ).to(self.device)

            # Generate summary with optimized parameters
            with torch.no_grad():
                summary_ids = self.model.generate(
                    inputs,
                    max_length=token_max_length,
                    min_length=max(50, token_max_length // 8),  # Dynamic minimum length
                    length_penalty=1.5,  # Moderate penalty for balanced length
                    num_beams=4,  # Beam search for better quality
                    early_stopping=True,
                    do_sample=False,  # Deterministic output
                    repetition_penalty=1.1,  # Avoid repetition
                )

            # Decode summary
            summary = str(
                self.tokenizer.decode(summary_ids[0], skip_special_tokens=True)
            )

            # Ensure we don't exceed character limit
            if len(summary) > max_length:
                # Truncate at last complete sentence
                truncated = summary[:max_length]
                last_sentence_end = max(
                    truncated.rfind("."), truncated.rfind("!"), truncated.rfind("?")
                )
                if (
                    last_sentence_end > max_length * 0.8
                ):  # Only truncate if we don't lose too much
                    summary = truncated[: last_sentence_end + 1]
                else:
                    summary = truncated

            return summary.strip()

        except Exception as e:
            return f"Error generating summary: {str(e)}"

    def generate_hierarchical_summary(self, text: str, max_length: int = 8000) -> str:
        """
        Generate summary using hierarchical chunking for very long texts

        Args:
            text: Input text to summarize
            max_length: Maximum length of final summary

        Returns:
            Generated summary text
        """
        # Split text into chunks
        chunk_size = 4000
        chunks = [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)]

        # Summarize each chunk
        chunk_summaries = []
        for chunk in chunks:
            chunk_summary = self.generate_abstractive_summary(chunk, max_length // 2)
            chunk_summaries.append(chunk_summary)

        # Combine and summarize again
        combined = " ".join(chunk_summaries)
        if len(combined) > max_length:
            return self.generate_abstractive_summary(combined, max_length)
        return combined

    def generate_hybrid_summary(
        self,
        reviews: List[ReviewData],
        max_length: int = 8000,
        num_key_sentences: int = 15,
        is_course_specific: bool = False,
    ) -> Dict[str, Any]:
        """
        Generate comprehensive summary using hybrid extractive + abstractive approach

        Args:
            reviews: List of review data objects
            max_length: Maximum length of final summary (8000 characters)
            num_key_sentences: Number of key sentences to extract
            is_course_specific: If True, skip course contextualization

        Returns:
            Dictionary containing summary and metadata
        """
        if not reviews:
            return {
                "summary": "No reviews available for summarization.",
                "method": "none",
                "num_reviews": 0,
                "extractive_length": 0,
                "final_length": 0,
            }

        try:
            # Step 1: Extractive phase - get key sentences
            key_sentences = self.extract_key_sentences(
                reviews, num_key_sentences, is_course_specific
            )

            if not key_sentences:
                return {
                    "summary": "No meaningful content found in reviews.",
                    "method": "failed",
                    "num_reviews": len(reviews),
                    "extractive_length": 0,
                    "final_length": 0,
                }

            # Combine key sentences
            extractive_summary = " ".join(key_sentences)
            extractive_length = len(extractive_summary)

            # Step 2: Check if extractive summary needs further processing
            if extractive_length <= 10000:  # Give some buffer for BART processing
                # Step 3: Abstractive phase - generate final summary
                final_summary = self.generate_abstractive_summary(
                    extractive_summary, max_length
                )
                method_used = "hybrid"
            else:
                # If extractive summary is still too long, use hierarchical approach
                final_summary = self.generate_hierarchical_summary(
                    extractive_summary, max_length
                )
                method_used = "hybrid_hierarchical"

            return {
                "summary": final_summary,
                "method": method_used,
                "num_reviews": len(reviews),
                "extractive_length": extractive_length,
                "final_length": len(final_summary),
                "key_sentences_count": len(key_sentences),
                "compression_ratio": len(final_summary)
                / sum(len(r.review_text or "") for r in reviews),
                "is_course_specific": is_course_specific,
            }

        except Exception as e:
            return {
                "summary": f"Error generating summary: {str(e)}",
                "method": "error",
                "num_reviews": len(reviews),
                "extractive_length": 0,
                "final_length": 0,
            }

    def normalize_course_code(self, course_code: Optional[str]) -> str:
        """
        Normalize course codes to handle typos, cross-listings, and department changes

        Args:
            course_code: Raw course code from review

        Returns:
            Normalized course code
        """
        if not course_code:
            return "Unknown"

        # Clean the input
        original = course_code.strip().upper()

        # Remove common noise
        cleaned = re.sub(r"[^\w\s]", "", original)  # Remove punctuation
        cleaned = re.sub(r"\s+", " ", cleaned).strip()  # Normalize whitespace

        # Try to extract department and number
        # Common patterns: "CSCE 222", "CSCE222", "CS222", etc.
        match = re.match(r"([A-Z]+)\s*(\d+[A-Z]*)", cleaned)

        if not match:
            return original

        dept, number = match.groups()

        # Normalize department name
        normalized_dept = self.dept_aliases.get(dept, dept)

        # Check for cross-listings
        cross_listing_key = (normalized_dept, number)
        if cross_listing_key in self.cross_listings:
            normalized = self.cross_listings[cross_listing_key]
            return normalized

        # Also check original department for cross-listings
        original_cross_listing = (dept, number)
        if original_cross_listing in self.cross_listings:
            normalized = self.cross_listings[original_cross_listing]
            return normalized

        # Standard normalization
        normalized = f"{normalized_dept}{number}"

        return normalized

    def get_professor_primary_department(self, reviews: List[ReviewData]) -> str:
        """
        Determine a professor's primary department based on their course reviews

        Args:
            reviews: List of review data for a professor

        Returns:
            Primary department code (e.g., 'MATH', 'CSCE', etc.)
        """
        # Count normalized departments from all courses
        dept_counts: Dict[str, int] = {}

        for review in reviews:
            if review.course_code:
                normalized_code = self.normalize_course_code(review.course_code)

                # Extract department from normalized code
                match = re.match(r"([A-Z]+)", normalized_code)
                if match:
                    dept = match.group(1)
                    dept_counts[dept] = dept_counts.get(dept, 0) + 1

        if not dept_counts:
            print("No department information found, defaulting to 'UNKN'")
            return "UNKN"

        # Get most common department
        if dept_counts:
            primary_dept = max(list(dept_counts.keys()), key=lambda k: dept_counts[k])
            return primary_dept
        return "UNKN"

    def normalize_course_code_with_context(
        self, course_code: Optional[str], professor_dept: str
    ) -> str:
        """
        Normalize course codes with professor department context for handling incomplete codes

        Args:
            course_code: Raw course code from review
            professor_dept: Professor's primary department

        Returns:
            Normalized course code with proper department
        """
        if not course_code:
            return "Unknown"

        original = course_code.strip().upper()

        # Remove common noise
        cleaned = re.sub(r"[^\w\s]", "", original)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()

        # Try to extract department and number
        match = re.match(r"([A-Z]+)\s*(\d+[A-Z]*)", cleaned)

        if match:
            dept, number = match.groups()

            # Normalize department name
            normalized_dept = self.dept_aliases.get(dept, dept)

            # Check for cross-listings
            cross_listing_key = (normalized_dept, number)
            if cross_listing_key in self.cross_listings:
                result = self.cross_listings[cross_listing_key]
                return result

            # Standard normalization
            result = f"{normalized_dept}{number}"
            return result

        # Try to extract just a number (for cases like "152", "M152", etc.)
        number_match = re.search(r"(\d+[A-Z]*)", cleaned)
        if number_match:
            number = number_match.group(1)

            # Check for partial department matches (like "M152" -> "MATH152")
            partial_dept_match = re.match(r"([A-Z]{1,3})", cleaned)
            if partial_dept_match:
                partial_dept = partial_dept_match.group(1)

                # Try to expand partial department based on professor context
                if professor_dept.startswith(partial_dept):
                    result = f"{professor_dept}{number}"
                    return result

                # Try common expansions
                partial_expansions = {
                    "M": "MATH",
                    "C": "CSCE",
                    "E": "ECEN",
                    "P": "PHYS",
                    "CH": "CHEM",
                    "A": "ACCT",  # Default to accounting for 'A'
                }

                if partial_dept in partial_expansions:
                    expanded_dept = partial_expansions[partial_dept]
                    result = f"{expanded_dept}{number}"
                    return result

            # Just a number - use professor's department context
            result = f"{professor_dept}{number}"
            return result

        # Couldn't parse - return as-is
        return original

    def extract_course_number(self, course_code: Optional[str]) -> str:
        """
        Extract just the course number from a course code

        Args:
            course_code: Raw course code from review

        Returns:
            Course number (e.g., "151", "489", etc.)
        """
        if not course_code:
            return "Unknown"

        # Clean the input
        original = course_code.strip().upper()

        # Remove common noise
        cleaned = re.sub(r"[^\w\s]", "", original)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()

        # Try to extract number
        match = re.search(r"(\d+[A-Z]*)", cleaned)

        if match:
            number = match.group(1)
            return number
        else:
            return "Unknown"

    def group_reviews_by_course_number(
        self, reviews: List[ReviewData], professor_dept: str
    ) -> Dict[str, List[ReviewData]]:
        """
        Group reviews by course number, using professor department context for proper formatting

        Args:
            reviews: List of review data
            professor_dept: Professor's primary department for context

        Returns:
            Dictionary mapping properly formatted course codes to review lists
        """
        # Track course number mappings for transparency
        number_mappings: Dict[str, Set[str]] = {}
        course_reviews: Dict[str, List[ReviewData]] = {}

        for review in reviews:
            original_code = review.course_code

            # Extract just the number
            number = self.extract_course_number(original_code)

            # Create properly formatted course code using professor's department
            if number != "Unknown" and number.isdigit():
                formatted_code = f"{professor_dept}{number}"
            else:
                # Fallback to regular normalization for non-numeric or complex codes
                formatted_code = self.normalize_course_code_with_context(
                    original_code, professor_dept
                )

            # Track the mapping
            if formatted_code not in number_mappings:
                number_mappings[formatted_code] = set()
            number_mappings[formatted_code].add(original_code or "None")

            # Group reviews
            if formatted_code not in course_reviews:
                course_reviews[formatted_code] = []
            course_reviews[formatted_code].append(review)

        return course_reviews

    def group_reviews_by_normalized_course(
        self,
        reviews: List[ReviewData],
        group_by_number_only: bool = False,
        professor_dept: Optional[str] = None,
    ) -> Dict[str, List[ReviewData]]:
        """
        Group reviews by normalized course codes or by number with context

        Args:
            reviews: List of review data
            group_by_number_only: If True, group by course number only with department context
            professor_dept: Professor's primary department for context

        Returns:
            Dictionary mapping course codes to review lists
        """
        if group_by_number_only and professor_dept:
            return self.group_reviews_by_course_number(reviews, professor_dept)
        else:
            return self._group_by_full_course_code(reviews)

    def _group_by_full_course_code(
        self, reviews: List[ReviewData]
    ) -> Dict[str, List[ReviewData]]:
        """Group reviews by full normalized course codes (department + number)"""

        # Track course code mappings for transparency
        course_mappings: Dict[str, Set[str]] = {}
        course_reviews: Dict[str, List[ReviewData]] = {}

        for review in reviews:
            original_code = review.course_code
            normalized_code = self.normalize_course_code(original_code)

            # Track the mapping
            if normalized_code not in course_mappings:
                course_mappings[normalized_code] = set()
            course_mappings[normalized_code].add(original_code or "None")

            # Group reviews
            if normalized_code not in course_reviews:
                course_reviews[normalized_code] = []
            course_reviews[normalized_code].append(review)

        return course_reviews

    def fetch_reviews_for_professor(self, professor_id: str) -> List[ReviewData]:
        """Fetch all reviews for a specific professor"""
        reviews_data = []
        reviews = (
            self.session.query(ReviewDB)
            .filter(ReviewDB.professor_id == professor_id)
            .all()
        )
        # Cast to Any list to avoid strict mypy Column type checking
        from typing import Any, cast

        reviews_any = cast(List[Any], reviews)

        for review in reviews_any:
            reviews_data.append(
                ReviewData(
                    id=review.id,
                    professor_id=review.professor_id,
                    course_code=review.course_code,
                    review_text=review.review_text,
                    clarity_rating=review.clarity_rating,
                    difficulty_rating=review.difficulty_rating,
                    helpful_rating=review.helpful_rating,
                    rating_tags=review.rating_tags,
                    grade=review.grade,
                )
            )

        return reviews_data

    def aggregate_tags(
        self, reviews: List[ReviewData]
    ) -> Tuple[List[str], Dict[str, int]]:
        """Aggregate and count rating tags from reviews"""
        all_tags = []
        reviews_with_tags = 0

        for review in reviews:
            if review.rating_tags:
                all_tags.extend(review.rating_tags)
                reviews_with_tags += 1

        tag_counts = Counter(all_tags)
        # Get top 10 most common tags
        common_tags = [tag for tag, count in tag_counts.most_common(10)]

        return common_tags, dict(tag_counts)

    def prepare_text_for_summarization(
        self, reviews: List[ReviewData], overall: bool = False
    ) -> Union[str, List[ReviewData]]:
        """Combine review texts into a single document for summarization"""
        texts = []
        valid_reviews = 0

        for review in reviews:
            if review.review_text and len(review.review_text.strip()) > 10:
                # Add some context about the review
                context = ""
                if overall:
                    context = f"Course: {review.course_code}"
                if review.grade:
                    context += f", Grade: {review.grade}"

                text = f"{context}. {review.review_text.strip()}"
                texts.append(text)
                valid_reviews += 1

        # Combine all texts
        combined_text = " ".join(texts)
        len(combined_text)

        if len(combined_text) > 8000:  # Conservative limit
            # return reviews  # Previously returning list, violating return type str
            return ""  # Return empty string instead or handle differently

        return combined_text

    def generate_summary(self, text: str, max_length: int = 600) -> str:
        """Generate summary using BART model"""
        token_max_length = min(max_length // 4, 1024)

        if not text.strip():
            return "No review text available for summarization."

        try:
            text = re.sub(r"^(\W)*", "", text)
            text = re.sub(r"Course: \w+", "", text)
            text = re.sub(r"Grade: Rather not say.", "", text)
            text = re.sub(r"Grade: \w*+\W*\w*\.", "", text)
            text = re.sub(r"(\s\W\s)", "", text)

            # Tokenize input
            inputs = self.tokenizer.encode(
                text, return_tensors="pt", max_length=1024, truncation=True
            ).to(self.device)

            # Generate summary
            with torch.no_grad():
                summary_ids = self.model.generate(
                    inputs,
                    max_length=token_max_length,
                    min_length=max(50, token_max_length // 8),  # Dynamic minimum length
                    length_penalty=1.5,  # Moderate penalty for balanced length
                    num_beams=4,  # Beam search for better quality
                    early_stopping=True,
                    do_sample=False,  # Deterministic output
                    repetition_penalty=1.1,  # Avoid repetition
                )

            # Decode summary
            summary = str(
                self.tokenizer.decode(summary_ids[0], skip_special_tokens=True)
            )

            return summary

        except Exception as e:
            return f"Error generating summary: {str(e)}"

    def calculate_averages(
        self, reviews: List[ReviewData]
    ) -> Dict[str, Optional[float]]:
        """Calculate average ratings from reviews"""
        clarity_ratings = [
            r.clarity_rating for r in reviews if r.clarity_rating is not None
        ]
        difficulty_ratings = [
            r.difficulty_rating for r in reviews if r.difficulty_rating is not None
        ]
        helpful_ratings = [
            r.helpful_rating for r in reviews if r.helpful_rating is not None
        ]

        averages = {
            "avg_clarity": sum(clarity_ratings) / len(clarity_ratings)
            if clarity_ratings
            else None,
            "avg_difficulty": sum(difficulty_ratings) / len(difficulty_ratings)
            if difficulty_ratings
            else None,
            "avg_helpful": sum(helpful_ratings) / len(helpful_ratings)
            if helpful_ratings
            else None,
        }

        return averages

    def close(self) -> None:
        """Close database connection"""
        self.session.close()

    def add_course_mapping(self, from_dept: str, to_dept: str) -> None:
        """
        Add a department alias mapping

        Args:
            from_dept: Original department code
            to_dept: Normalized department code
        """
        self.dept_aliases[from_dept.upper()] = to_dept.upper()
        print(f"Added department mapping: {from_dept} -> {to_dept}")

    def add_cross_listing(self, dept1: str, number: str, canonical_course: str) -> None:
        """
        Add a cross-listing mapping

        Args:
            dept1: Department code for cross-listed course
            number: Course number
            canonical_course: The canonical course code to use
        """
        key = (dept1.upper(), number)
        self.cross_listings[key] = canonical_course.upper()
        print(f"Added cross-listing: {dept1}{number} -> {canonical_course}")

    def get_normalization_stats(self, reviews: List[ReviewData]) -> Dict[str, Any]:
        """
        Get statistics about course code normalization for debugging

        Args:
            reviews: List of review data

        Returns:
            Dictionary with normalization statistics
        """
        original_codes = [r.course_code for r in reviews if r.course_code]
        normalized_codes = [self.normalize_course_code(code) for code in original_codes]

        # Count reductions
        unique_original = len(set(original_codes))
        unique_normalized = len(set(normalized_codes))
        reduction = unique_original - unique_normalized

        # Find merged courses
        merged_courses: Dict[str, Set[str]] = {}
        for orig, norm in zip(original_codes, normalized_codes):
            if norm not in merged_courses:
                merged_courses[norm] = set()
            merged_courses[norm].add(orig)

        merged_courses = {k: v for k, v in merged_courses.items() if len(v) > 1}

        return {
            "total_reviews": len(original_codes),
            "unique_original_codes": unique_original,
            "unique_normalized_codes": unique_normalized,
            "courses_merged": reduction,
            "merged_course_details": {k: list(v) for k, v in merged_courses.items()},
        }
