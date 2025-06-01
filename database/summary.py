import asyncio
from datetime import datetime
import os
import re
from typing import List, Dict, Optional, Tuple
from collections import Counter
import numpy as np
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime, Text, ARRAY
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from transformers import BartTokenizer, BartForConditionalGeneration
import torch
from dataclasses import dataclass
from dotenv import load_dotenv
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from database.base import ProfessorDB, ReviewDB

load_dotenv()
# Database Models
Base = declarative_base()

class SummaryDB(Base):
    __tablename__ = 'professor_summaries'
    
    id = Column(String, primary_key=True)
    professor_id = Column(String, nullable=False)
    course_code = Column(String, nullable=True)  # None for overall summary
    summary_type = Column(String, nullable=False)  # 'overall' or 'course_specific'
    summary_text = Column(Text, nullable=False)
    total_reviews = Column(Integer, nullable=False)
    avg_rating = Column(Float, nullable=True)
    avg_difficulty = Column(Float, nullable=True)
    common_tags = Column(ARRAY(String), nullable=True)
    tag_frequencies = Column(Text, nullable=True)  # JSON string of tag counts
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now)

@dataclass
class ReviewData:
    id: str
    professor_id: str
    course_code: Optional[str]
    review_text: Optional[str]
    clarity_rating: Optional[float]
    difficulty_rating: Optional[float]
    helpful_rating: Optional[float]
    rating_tags: Optional[List[str]]
    grade: Optional[str]

class ReviewSummarizer:
    def __init__(self, model_name: str = "facebook/bart-large-cnn"):
        """
        Initialize the review summarizer with database connection and BART model
        
        Args:
            db_url: PostgreSQL connection string
            model_name: HuggingFace model name for BART
        """
        # Database setup
        url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
            os.getenv("POSTGRES_USER"),
            os.getenv("POSTGRES_PASSWORD"),
            os.getenv("POSTGRES_HOST"),
            os.getenv("POSTGRES_PORT"),
            os.getenv("POSTGRES_DATABASE")
        )
        self.engine = create_engine(url)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
        # Course normalization mappings
        self.dept_aliases = {
            # Computer Science department changes
            'CPCS': 'CSCE',
            'CPSC': 'CSCE',
            'COSC': 'CSCE',
            'CS': 'CSCE',
            
            # Electrical Engineering variations
            'ELEN': 'ECEN',
            'EE': 'ECEN',
            'ELEC': 'ECEN',
            
            # Mathematics variations
            'MATH': 'MATH',
            'MATHS': 'MATH',
            
            # Engineering variations
            'ENGR': 'ENGR',
            'ENGI': 'ENGR',
            
            # Physics variations
            'PHYS': 'PHYS',
            'PHY': 'PHYS',
            
            # Chemistry variations
            'CHEM': 'CHEM',
            'CHM': 'CHEM',
            
            # Business variations
            'MGMT': 'MGMT',
            'MKTG': 'MKTG',
            'FINC': 'FINC',
            'ACCT': 'ACCT',
            
            "ID": "IDIS",
        }
        
        # Known cross-listings (courses that are the same but different departments)
        self.cross_listings = {
            # Computer Science/Electrical Engineering cross-listings
            ('CSCE', '222'): 'CSCE222',  # Digital Logic Design
            ('ECEN', '222'): 'CSCE222',
            
            ('CSCE', '314'): 'CSCE314',  # Programming Languages
            ('ECEN', '314'): 'CSCE314',
            
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
        
    def extract_key_sentences(self, reviews: List[ReviewData], num_sentences: int = 20, is_course_specific: bool = False) -> List[str]:
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
                sentences = re.split(r'[.!?]+', review.review_text)
                
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
                        sentence_metadata.append({
                            'review_id': review.id,
                            'course_code': review.course_code,
                            'clarity_rating': review.clarity_rating,
                            'difficulty_rating': review.difficulty_rating,
                            'grade': review.grade,
                            'original_sentence': sentence
                        })
        
        if len(all_sentences) <= num_sentences:
            return all_sentences
        
        try:
            # Step 2: Use TF-IDF to calculate sentence importance
            print(f"Analyzing {len(all_sentences)} sentences for key content...")
            
            vectorizer = TfidfVectorizer(
                max_features=1000,
                stop_words='english',
                ngram_range=(1, 2),  # Include both unigrams and bigrams
                min_df=1,  # Include terms that appear at least once
                max_df=0.8  # Exclude terms that appear in >80% of sentences
            )
            
            tfidf_matrix = vectorizer.fit_transform(all_sentences)
            
            # Calculate base importance scores (sum of TF-IDF values)
            sentence_scores = np.array(tfidf_matrix.sum(axis=1)).flatten()
            
            # Step 3: Add bonus scores for sentences with valuable metadata
            for i, metadata in enumerate(sentence_metadata):
                bonus = 0
                
                # Boost sentences from reviews with extreme ratings
                if metadata['clarity_rating']:
                    if metadata['clarity_rating'] <= 2 or metadata['clarity_rating'] >= 4:
                        bonus += 0.2
                
                # Boost sentences mentioning specific grades
                if metadata['grade'] and metadata['grade'] in ['A', 'F', 'A+', 'A-']:
                    bonus += 0.1
                
                # Boost sentences from courses (more specific context)
                if metadata['course_code']:
                    bonus += 0.1
                
                sentence_scores[i] += bonus
            
            # Step 4: Select diverse, high-scoring sentences
            similarity_matrix = cosine_similarity(tfidf_matrix)
            
            selected_indices = []
            remaining_indices = list(range(len(all_sentences)))
            
            # Start with the highest scoring sentence
            best_idx = np.argmax(sentence_scores)
            selected_indices.append(best_idx)
            remaining_indices.remove(best_idx)
            
            print(f"Selected first sentence (score: {sentence_scores[best_idx]:.3f})")
            
            # Select remaining sentences balancing importance and diversity
            while len(selected_indices) < num_sentences and remaining_indices:
                best_score = -1
                best_idx = -1
                
                for idx in remaining_indices:
                    # Base importance score
                    base_score = sentence_scores[idx]
                    
                    # Calculate maximum similarity to already selected sentences
                    max_similarity = max([similarity_matrix[idx][selected_idx] 
                                         for selected_idx in selected_indices])
                    
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
                    print(f"Selected sentence {len(selected_indices)} (score: {sentence_scores[best_idx]:.3f}, similarity penalty: {max_similarity:.3f})")
                else:
                    break
            
            # Step 5: Sort selected sentences by their original importance for better flow
            selected_with_scores = [(idx, sentence_scores[idx]) for idx in selected_indices]
            selected_with_scores.sort(key=lambda x: x[1], reverse=True)
            
            selected_sentences = [all_sentences[idx] for idx, _ in selected_with_scores]
            
            print(f"Extracted {len(selected_sentences)} key sentences from {len(all_sentences)} total sentences")
            return selected_sentences
            
        except Exception as e:
            print(f"Error in extractive summarization: {e}")
            # Fallback to simple selection by length and position
            sentence_lengths = [len(s) for s in all_sentences]
            avg_length = np.mean(sentence_lengths)
            
            # Select sentences that are around average length (not too short/long)
            good_sentences = [s for s, length in zip(all_sentences, sentence_lengths) 
                            if avg_length * 0.7 <= length <= avg_length * 1.5]
            
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
            text = re.sub(r'^[^\w\s]', '', text)
            text = re.sub(r'^\s+', '', text)
            text = re.sub(r'Course: \w+', '', text)
            text = re.sub(r'Grade: \w+\+\.', '', text)
            text = re.sub(r'Grade: \w+\.', '', text)
            text = re.sub(r'(\s\W\s)', '', text)
            text = re.sub(r'In \w+:', '', text)
            
            token_max_length = min(max_length // 4, 1024)  # BART's max output length
            
            # Tokenize input with truncation
            inputs = self.tokenizer.encode(
                text, 
                return_tensors="pt", 
                max_length=1024, 
                truncation=True
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
                    repetition_penalty=1.1  # Avoid repetition
                )
            
            # Decode summary
            summary = self.tokenizer.decode(
                summary_ids[0], 
                skip_special_tokens=True
            )
            
            # Ensure we don't exceed character limit
            if len(summary) > max_length:
                # Truncate at last complete sentence
                truncated = summary[:max_length]
                last_sentence_end = max(
                    truncated.rfind('.'),
                    truncated.rfind('!'),
                    truncated.rfind('?')
                )
                if last_sentence_end > max_length * 0.8:  # Only truncate if we don't lose too much
                    summary = truncated[:last_sentence_end + 1]
                else:
                    summary = truncated
            
            return summary.strip()
            
        except Exception as e:
            print(f"Error generating abstractive summary: {e}")
            return f"Error generating summary: {str(e)}"
    
    def generate_hybrid_summary(self, reviews: List[ReviewData], max_length: int = 8000, 
                              num_key_sentences: int = 15, is_course_specific: bool = False) -> Dict[str, any]:
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
                'summary': "No reviews available for summarization.",
                'method': 'none',
                'num_reviews': 0,
                'extractive_length': 0,
                'final_length': 0
            }
        
        summary_type = "course-specific" if is_course_specific else "overall"
        print(f"Starting hybrid summarization for {len(reviews)} reviews ({summary_type})...")
        
        try:
            # Step 1: Extractive phase - get key sentences
            print("Phase 1: Extractive summarization...")
            key_sentences = self.extract_key_sentences(reviews, num_key_sentences, is_course_specific)
            
            if not key_sentences:
                return {
                    'summary': "No meaningful content found in reviews.",
                    'method': 'failed',
                    'num_reviews': len(reviews),
                    'extractive_length': 0,
                    'final_length': 0
                }
            
            # Combine key sentences
            extractive_summary = " ".join(key_sentences)
            extractive_length = len(extractive_summary)
            
            print(f"Extractive summary: {extractive_length} characters from {len(key_sentences)} sentences")
            
            # Step 2: Check if extractive summary needs further processing
            if extractive_length <= 10000:  # Give some buffer for BART processing
                # Step 3: Abstractive phase - generate final summary
                print("Phase 2: Abstractive summarization...")
                final_summary = self.generate_abstractive_summary(extractive_summary, max_length)
                method_used = 'hybrid'
            else:
                # If extractive summary is still too long, use hierarchical approach
                print("Extractive summary still too long, applying hierarchical chunking...")
                final_summary = self.generate_hierarchical_summary(extractive_summary, max_length)
                method_used = 'hybrid_hierarchical'
            
            return {
                'summary': final_summary,
                'method': method_used,
                'num_reviews': len(reviews),
                'extractive_length': extractive_length,
                'final_length': len(final_summary),
                'key_sentences_count': len(key_sentences),
                'compression_ratio': len(final_summary) / sum(len(r.review_text or '') for r in reviews),
                'is_course_specific': is_course_specific
            }
            
        except Exception as e:
            print(f"Error in hybrid summarization: {e}")
            return {
                'summary': f"Error generating summary: {str(e)}",
                'method': 'error',
                'num_reviews': len(reviews),
                'extractive_length': 0,
                'final_length': 0
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
        print(f"🔧 Normalizing course code: '{course_code}' -> '{original}'")
        
        # Remove common noise
        cleaned = re.sub(r'[^\w\s]', '', original)  # Remove punctuation
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()  # Normalize whitespace
        
        # Try to extract department and number
        # Common patterns: "CSCE 222", "CSCE222", "CS222", etc.
        match = re.match(r'([A-Z]+)\s*(\d+[A-Z]*)', cleaned)
        
        if not match:
            print(f"⚠️  Could not parse course code: '{original}' -> using as-is")
            return original
        
        dept, number = match.groups()
        
        # Normalize department name
        normalized_dept = self.dept_aliases.get(dept, dept)
        
        # Check for cross-listings
        cross_listing_key = (normalized_dept, number)
        if cross_listing_key in self.cross_listings:
            normalized = self.cross_listings[cross_listing_key]
            print(f"✅ Cross-listing found: '{original}' -> '{normalized}'")
            return normalized
        
        # Also check original department for cross-listings
        original_cross_listing = (dept, number)
        if original_cross_listing in self.cross_listings:
            normalized = self.cross_listings[original_cross_listing]
            print(f"✅ Cross-listing found: '{original}' -> '{normalized}'")
            return normalized
        
        # Standard normalization
        normalized = f"{normalized_dept}{number}"
        
        if normalized != original:
            print(f"✅ Normalized: '{original}' -> '{normalized}'")
        else:
            print(f"✅ No changes needed: '{original}'")
        
        return normalized
    
    def get_professor_primary_department(self, reviews: List[ReviewData]) -> str:
        """
        Determine a professor's primary department based on their course reviews
        
        Args:
            reviews: List of review data for a professor
            
        Returns:
            Primary department code (e.g., 'MATH', 'CSCE', etc.)
        """
        print(f"🔍 Determining primary department from {len(reviews)} reviews...")
        
        # Count normalized departments from all courses
        dept_counts = {}
        
        for review in reviews:
            if review.course_code:
                normalized_code = self.normalize_course_code(review.course_code)
                
                # Extract department from normalized code
                match = re.match(r'([A-Z]+)', normalized_code)
                if match:
                    dept = match.group(1)
                    dept_counts[dept] = dept_counts.get(dept, 0) + 1
        
        if not dept_counts:
            print("⚠️  No department information found, defaulting to 'UNKN'")
            return "UNKN"
        
        # Get most common department
        primary_dept = max(dept_counts, key=dept_counts.get)
        total_reviews = sum(dept_counts.values())
        primary_percentage = (dept_counts[primary_dept] / total_reviews) * 100
        
        print(f"📊 Department distribution: {dict(dept_counts)}")
        print(f"🎯 Primary department: {primary_dept} ({dept_counts[primary_dept]}/{total_reviews} reviews, {primary_percentage:.1f}%)")
        
        return primary_dept
    
    def normalize_course_code_with_context(self, course_code: Optional[str], professor_dept: str) -> str:
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
        print(f"🔧 Context-aware normalizing: '{course_code}' with dept context '{professor_dept}'")
        
        # Remove common noise
        cleaned = re.sub(r'[^\w\s]', '', original)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # Try to extract department and number
        match = re.match(r'([A-Z]+)\s*(\d+[A-Z]*)', cleaned)
        
        if match:
            dept, number = match.groups()
            
            # Normalize department name
            normalized_dept = self.dept_aliases.get(dept, dept)
            
            # Check for cross-listings
            cross_listing_key = (normalized_dept, number)
            if cross_listing_key in self.cross_listings:
                result = self.cross_listings[cross_listing_key]
                print(f"✅ Cross-listing found: '{original}' -> '{result}'")
                return result
            
            # Standard normalization
            result = f"{normalized_dept}{number}"
            print(f"✅ Standard normalization: '{original}' -> '{result}'")
            return result
        
        # Try to extract just a number (for cases like "152", "M152", etc.)
        number_match = re.search(r'(\d+[A-Z]*)', cleaned)
        if number_match:
            number = number_match.group(1)
            
            # Check for partial department matches (like "M152" -> "MATH152")
            partial_dept_match = re.match(r'([A-Z]{1,3})', cleaned)
            if partial_dept_match:
                partial_dept = partial_dept_match.group(1)
                
                # Try to expand partial department based on professor context
                if professor_dept.startswith(partial_dept):
                    result = f"{professor_dept}{number}"
                    print(f"✅ Partial dept expansion: '{original}' -> '{result}' (using context {professor_dept})")
                    return result
                
                # Try common expansions
                partial_expansions = {
                    'M': 'MATH',
                    'C': 'CSCE', 
                    'E': 'ECEN',
                    'P': 'PHYS',
                    'CH': 'CHEM',
                    'A': 'ACCT'  # Default to accounting for 'A'
                }
                
                if partial_dept in partial_expansions:
                    expanded_dept = partial_expansions[partial_dept]
                    result = f"{expanded_dept}{number}"
                    print(f"✅ Partial dept expansion: '{original}' -> '{result}' (standard expansion)")
                    return result
            
            # Just a number - use professor's department context
            result = f"{professor_dept}{number}"
            print(f"✅ Context-based completion: '{original}' -> '{result}' (using professor dept {professor_dept})")
            return result
        
        # Couldn't parse - return as-is
        print(f"⚠️  Could not parse: '{original}' -> using as-is")
        return original
    
    def group_reviews_by_course_number(self, reviews: List[ReviewData], professor_dept: str) -> Dict[str, List[ReviewData]]:
        """
        Group reviews by course number, using professor department context for proper formatting
        
        Args:
            reviews: List of review data
            professor_dept: Professor's primary department for context
            
        Returns:
            Dictionary mapping properly formatted course codes to review lists
        """
        print("🔢 Grouping reviews by course NUMBER with department context...")
        
        # Track course number mappings for transparency
        number_mappings = {}
        course_reviews = {}
        
        for review in reviews:
            original_code = review.course_code
            
            # Extract just the number
            number = self.extract_course_number(original_code)
            
            # Create properly formatted course code using professor's department
            if number != "Unknown" and number.isdigit():
                formatted_code = f"{professor_dept}{number}"
            else:
                # Fallback to regular normalization for non-numeric or complex codes
                formatted_code = self.normalize_course_code_with_context(original_code, professor_dept)
            
            # Track the mapping
            if formatted_code not in number_mappings:
                number_mappings[formatted_code] = set()
            number_mappings[formatted_code].add(original_code or "None")
            
            # Group reviews
            if formatted_code not in course_reviews:
                course_reviews[formatted_code] = []
            course_reviews[formatted_code].append(review)
        
        # Print mapping summary
        print(f"📊 Course number grouping summary (with {professor_dept} context):")
        for formatted_code, originals in number_mappings.items():
            if len(originals) > 1:
                print(f"   🔗 {formatted_code}: {', '.join(sorted(originals))} ({len(course_reviews[formatted_code])} reviews)")
            else:
                print(f"   📖 {formatted_code}: {len(course_reviews[formatted_code])} reviews")
        
        return course_reviews
    
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
        cleaned = re.sub(r'[^\w\s]', '', original)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # Try to extract number
        match = re.search(r'(\d+[A-Z]*)', cleaned)
        
        if match:
            number = match.group(1)
            return number
        else:
            return "Unknown"

    def group_reviews_by_normalized_course(self, reviews: List[ReviewData], group_by_number_only: bool = False, professor_dept: str = None) -> Dict[str, List[ReviewData]]:
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
            print("🔢 Grouping reviews by course NUMBER with department context...")
            return self.group_reviews_by_course_number(reviews, professor_dept)
        else:
            print("🔍 Grouping reviews by normalized course codes...")
            return self._group_by_full_course_code(reviews)
    
    def _group_by_full_course_code(self, reviews: List[ReviewData]) -> Dict[str, List[ReviewData]]:
        """Group reviews by full normalized course codes (department + number)"""
        
        # Track course code mappings for transparency
        course_mappings = {}
        course_reviews = {}
        
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
        
        # Print mapping summary
        print(f"📊 Course normalization summary:")
        for normalized, originals in course_mappings.items():
            if len(originals) > 1:
                print(f"   🔗 {normalized}: {', '.join(sorted(originals))} ({len(course_reviews[normalized])} reviews)")
            else:
                print(f"   📖 {normalized}: {len(course_reviews[normalized])} reviews")
        
        return course_reviews
    
    def fetch_reviews_for_professor(self, professor_id: str) -> List[ReviewData]:
        """Fetch all reviews for a specific professor"""
        print(f"🔍 Fetching reviews for professor: {professor_id}")
        
        reviews_data = []
        reviews = self.session.query(ReviewDB).filter(ReviewDB.professor_id == professor_id).all()
        print(f"📊 Found {len(reviews)} reviews in database for professor {professor_id}")
        
        for review in reviews:
            reviews_data.append(ReviewData(
                id=review.id,
                professor_id=review.professor_id,
                course_code=review.course_code,
                review_text=review.review_text,
                clarity_rating=review.clarity_rating,
                difficulty_rating=review.difficulty_rating,
                helpful_rating=review.helpful_rating,
                rating_tags=review.rating_tags,
                grade=review.grade
            ))
        
        print(f"✅ Successfully processed {len(reviews_data)} review records")
        return reviews_data
    
    def aggregate_tags(self, reviews: List[ReviewData]) -> Tuple[List[str], Dict[str, int]]:
        """Aggregate and count rating tags from reviews"""
        print(f"🏷️  Aggregating tags from {len(reviews)} reviews...")
        
        all_tags = []
        reviews_with_tags = 0
        
        for review in reviews:
            if review.rating_tags:
                all_tags.extend(review.rating_tags)
                reviews_with_tags += 1
        
        print(f"📈 Found {len(all_tags)} total tags from {reviews_with_tags} reviews with tags")
        
        tag_counts = Counter(all_tags)
        # Get top 10 most common tags
        common_tags = [tag for tag, count in tag_counts.most_common(10)]
        
        print(f"🔟 Top 10 most common tags: {common_tags}")
        return common_tags, dict(tag_counts)
    
    def prepare_text_for_summarization(self, reviews: List[ReviewData], overall: bool = False) -> str:
        """Combine review texts into a single document for summarization"""
        print(f"📝 Preparing text for summarization from {len(reviews)} reviews...")
        
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
        
        print(f"✅ Found {valid_reviews} reviews with valid text content")
        
        # Combine all texts
        combined_text = " ".join(texts)
        original_length = len(combined_text)
        
        # Truncate if too long (BART has token limits)
        if len(combined_text) > 8000:  # Conservative limit
            return reviews
            print(f"⚠️  Text truncated from {original_length} to {len(combined_text)} characters")
        else:
            print(f"📏 Combined text length: {len(combined_text)} characters")
        
        return combined_text
    
    def generate_summary(self, text: str, max_length: int = 600) -> str:
        """Generate summary using BART model"""
        print(f"🤖 Generating summary with BART model (max_length: {max_length})...")
        token_max_length = min(max_length // 4, 1024)
        
        if not text.strip():
            print("⚠️  No text available for summarization")
            return "No review text available for summarization."
        
        try:
            print(f"🔄 Tokenizing input text ({len(text)} characters)...")
            text = re.sub(r'^(\W)*', '', text)
            text = re.sub(r'Course: \w+', '', text)
            text = re.sub(r'Grade: Rather not say.', '', text)
            text = re.sub(r'Grade: \w*+\W*\w*\.', '', text)
            text = re.sub(r'(\s\W\s)', '', text)
            
            # Tokenize input
            inputs = self.tokenizer.encode(
                text, 
                return_tensors="pt", 
                max_length=1024, 
                truncation=True
            ).to(self.device)
            
            print(f"📊 Input tokens: {inputs.shape[1]}")
            print("⚡ Generating summary...")
            
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
                    repetition_penalty=1.1  # Avoid repetition
                )
            
            print("🔤 Decoding summary...")
            # Decode summary
            summary = self.tokenizer.decode(
                summary_ids[0], 
                skip_special_tokens=True
            )
            
            print(f"✅ Summary generated successfully ({len(summary)} characters)")
            print(f"📄 Summary preview: {summary[:100]}...")
            return summary
            
        except Exception as e:
            print(f"❌ Error generating summary: {e}")
            return f"Error generating summary: {str(e)}"
    
    def calculate_averages(self, reviews: List[ReviewData]) -> Dict[str, float]:
        """Calculate average ratings from reviews"""
        print(f"📊 Calculating average ratings from {len(reviews)} reviews...")
        
        clarity_ratings = [r.clarity_rating for r in reviews if r.clarity_rating is not None]
        difficulty_ratings = [r.difficulty_rating for r in reviews if r.difficulty_rating is not None]
        helpful_ratings = [r.helpful_rating for r in reviews if r.helpful_rating is not None]
        
        print(f"📈 Rating counts - Clarity: {len(clarity_ratings)}, Difficulty: {len(difficulty_ratings)}, Helpful: {len(helpful_ratings)}")
        
        averages = {
            'avg_clarity': sum(clarity_ratings) / len(clarity_ratings) if clarity_ratings else None,
            'avg_difficulty': sum(difficulty_ratings) / len(difficulty_ratings) if difficulty_ratings else None,
            'avg_helpful': sum(helpful_ratings) / len(helpful_ratings) if helpful_ratings else None,
        }
        
        print(f"🎯 Calculated averages: {averages}")
        return averages
    
    def create_overall_summary(self, professor_id: str) -> Optional[str]:
        """Create an overall summary for a professor across all courses"""
        print(f"\n🎓 Creating overall summary for professor {professor_id}")
        print("=" * 60)
        
        reviews = self.fetch_reviews_for_professor(professor_id)
        
        if not reviews:
            print(f"❌ No reviews found for professor {professor_id}")
            return None
        
        print(f"📚 Processing {len(reviews)} reviews for overall summary")
        
        # Prepare text and generate summary
        combined_text = self.prepare_text_for_summarization(reviews, overall=True)
        if type(combined_text) == list:
            course_result = self.generate_hybrid_summary(combined_text, is_course_specific=False)
            summary_text = course_result['summary']
            # remove random punctuation from the beginning
            summary_text = re.sub(r'^[^\w\s]', '', summary_text)
        else:
            summary_text = self.generate_summary(combined_text)
        
        # Aggregate tags and calculate averagesge
        common_tags, tag_frequencies = self.aggregate_tags(reviews)
        averages = self.calculate_averages(reviews)
        
        # Save to database
        summary_id = f"{professor_id}_overall"
        print(f"💾 Saving overall summary to database with ID: {summary_id}")
        
        summary_record = SummaryDB(
            id=summary_id,
            professor_id=professor_id,
            course_code=None,
            summary_type='overall',
            summary_text=summary_text,
            total_reviews=len(reviews),
            avg_rating=averages['avg_clarity'],
            avg_difficulty=averages['avg_difficulty'],
            common_tags=common_tags,
            tag_frequencies=str(tag_frequencies),
            updated_at=datetime.now()
        )
        
        # Upsert logic
        existing = self.session.query(SummaryDB).filter_by(id=summary_id).first()
        if existing:
            print(f"🔄 Updating existing summary record")
            existing.summary_text = summary_text
            existing.total_reviews = len(reviews)
            existing.avg_rating = averages['avg_clarity']
            existing.avg_difficulty = averages['avg_difficulty']
            existing.common_tags = common_tags
            existing.tag_frequencies = str(tag_frequencies)
            existing.updated_at = datetime.now()
        else:
            print(f"➕ Creating new summary record")
            self.session.add(summary_record)
        
        self.session.commit()
        print(f"✅ Successfully saved overall summary for professor {professor_id}")
        return summary_id
    
    def create_course_specific_summaries(self, professor_id: str) -> List[str]:
        """Create course-specific summaries for a professor"""
        print(f"\n📚 Creating course-specific summaries for professor {professor_id}")
        print("=" * 60)
        
        reviews = self.fetch_reviews_for_professor(professor_id)
        
        if not reviews:
            print("❌ No reviews found for course-specific summaries")
            return []
        
        # Group reviews by normalized course
        print("🔍 Grouping reviews by normalized course codes...")
        course_reviews = self.group_reviews_by_normalized_course(reviews)
        
        print(f"📊 Found {len(course_reviews)} unique courses:")
        for course_code, course_review_list in course_reviews.items():
            print(f"   📖 {course_code}: {len(course_review_list)} reviews")
        
        summary_ids = []
        
        for course_code, course_review_list in course_reviews.items():
            if len(course_review_list) < 2:  # Skip if too few reviews
                print(f"⚠️  Skipping {course_code} - only {len(course_review_list)} review(s)")
                continue
                
            print(f"\n📖 Processing normalized course: {course_code} ({len(course_review_list)} reviews)")
            
            # Generate summary for this course
            combined_text = self.prepare_text_for_summarization(course_review_list, overall=False)
            if type(combined_text) == list:
                course_result = self.generate_hybrid_summary(combined_text, is_course_specific=True)
                summary_text = course_result['summary']
            else:
                summary_text = self.generate_summary(combined_text)
            
            # Aggregate tags and calculate averages for this course
            common_tags, tag_frequencies = self.aggregate_tags(course_review_list)
            averages = self.calculate_averages(course_review_list)
            
            # Save to database using normalized course code
            summary_id = f"{professor_id}_{course_code}"
            print(f"💾 Saving course summary to database with ID: {summary_id}")
            
            summary_record = SummaryDB(
                id=summary_id,
                professor_id=professor_id,
                course_code=course_code,  # Use normalized course code
                summary_type='course_specific',
                summary_text=summary_text,
                total_reviews=len(course_review_list),
                avg_rating=averages['avg_clarity'],
                avg_difficulty=averages['avg_difficulty'],
                common_tags=common_tags,
                tag_frequencies=str(tag_frequencies),
                updated_at=datetime.now()
            )
            
            # Upsert logic
            existing = self.session.query(SummaryDB).filter_by(id=summary_id).first()
            if existing:
                print(f"🔄 Updating existing course summary")
                existing.summary_text = summary_text
                existing.total_reviews = len(course_review_list)
                existing.avg_rating = averages['avg_clarity']
                existing.avg_difficulty = averages['avg_difficulty']
                existing.common_tags = common_tags
                existing.tag_frequencies = str(tag_frequencies)
                existing.updated_at = datetime.now()
            else:
                print(f"➕ Creating new course summary")
                self.session.add(summary_record)
            
            summary_ids.append(summary_id)
            print(f"✅ Completed summary for {course_code}")
        
        self.session.commit()
        print(f"\n🎉 Created {len(summary_ids)} course-specific summaries for professor {professor_id}")
        return summary_ids
    
    def create_course_number_summaries(self, professor_id: str) -> List[str]:
        """Create course number summaries using professor's department context for proper formatting"""
        print(f"\n🔢 Creating course NUMBER summaries for professor {professor_id}")
        print("=" * 60)
        
        reviews = self.fetch_reviews_for_professor(professor_id)
        
        if not reviews:
            print("❌ No reviews found for course number summaries")
            return []
        
        # Determine professor's primary department
        professor_dept = self.get_professor_primary_department(reviews)
        
        # Group reviews by course number with department context
        course_reviews = self.group_reviews_by_course_number(reviews, professor_dept)
        
        print(f"📊 Found {len(course_reviews)} unique course numbers (formatted with {professor_dept}):")
        for course_code, course_review_list in course_reviews.items():
            print(f"   🔢 {course_code}: {len(course_review_list)} reviews")
        
        summary_ids = []
        
        for formatted_course_code, course_review_list in course_reviews.items():
            if len(course_review_list) < 2:  # Skip if too few reviews
                print(f"⚠️  Skipping {formatted_course_code} - only {len(course_review_list)} review(s)")
                continue
                
            print(f"\n🔢 Processing course number: {formatted_course_code} ({len(course_review_list)} reviews)")
            
            # Show which original course codes were combined
            original_codes = list(set([r.course_code for r in course_review_list if r.course_code]))
            if len(original_codes) > 1:
                print(f"   📝 Combined from: {', '.join(original_codes)}")
            
            # Generate summary for this course number
            combined_text = self.prepare_text_for_summarization(course_review_list, overall=False)
            if type(combined_text) == list:
                course_result = self.generate_hybrid_summary(combined_text, is_course_specific=True)
                summary_text = course_result['summary']
            else:
                summary_text = self.generate_summary(combined_text)
            
            # Aggregate tags and calculate averages for this course number
            common_tags, tag_frequencies = self.aggregate_tags(course_review_list)
            averages = self.calculate_averages(course_review_list)
            
            # Save to database using formatted course code with NUM prefix to distinguish from course-specific
            summary_id = f"{professor_id}_NUM{formatted_course_code}"
            print(f"💾 Saving course number summary to database with ID: {summary_id}")
            
            summary_record = SummaryDB(
                id=summary_id,
                professor_id=professor_id,
                course_code=formatted_course_code,  # Use properly formatted course code
                summary_type='course_number',
                summary_text=summary_text,
                total_reviews=len(course_review_list),
                avg_rating=averages['avg_clarity'],
                avg_difficulty=averages['avg_difficulty'],
                common_tags=common_tags,
                tag_frequencies=str(tag_frequencies),
                updated_at=datetime.now()
            )
            
            # Upsert logic
            existing = self.session.query(SummaryDB).filter_by(id=summary_id).first()
            if existing:
                print(f"🔄 Updating existing course number summary")
                existing.summary_text = summary_text
                existing.total_reviews = len(course_review_list)
                existing.avg_rating = averages['avg_clarity']
                existing.avg_difficulty = averages['avg_difficulty']
                existing.common_tags = common_tags
                existing.tag_frequencies = str(tag_frequencies)
                existing.updated_at = datetime.now()
            else:
                print(f"➕ Creating new course number summary")
                self.session.add(summary_record)
            
            summary_ids.append(summary_id)
            print(f"✅ Completed number summary for {formatted_course_code}")
        
        self.session.commit()
        print(f"\n🎉 Created {len(summary_ids)} course number summaries for professor {professor_id}")
        return summary_ids

    def process_professor(self, professor_id: str, include_course_numbers: bool = True) -> Dict[str, any]:
        """Process both overall and course-specific summaries for a professor"""
        print(f"\n🚀 PROCESSING PROFESSOR: {professor_id}")
        print("=" * 80)
        
        results = {
            'professor_id': professor_id,
            'overall_summary_id': None,
            'course_summary_ids': [],
            'course_number_summary_ids': [],
            'error': None
        }
        
        try:
            # Create overall summary
            print("🎯 Step 1: Creating overall summary...")
            overall_id = self.create_overall_summary(professor_id)
            results['overall_summary_id'] = overall_id
            print(f"✅ Overall summary completed: {overall_id}")
            
            # # Create course-specific summaries
            # print("\n🎯 Step 2: Creating course-specific summaries...")
            # course_ids = self.create_course_specific_summaries(professor_id)
            # results['course_summary_ids'] = course_ids
            # print(f"✅ Course summaries completed: {len(course_ids)} summaries")
            
            # Create course number summaries (handles typos and incomplete codes)
            if include_course_numbers:
                print("\n🎯 Step 3: Creating course NUMBER summaries...")
                number_ids = self.create_course_number_summaries(professor_id)
                results['course_number_summary_ids'] = number_ids
                print(f"✅ Course number summaries completed: {len(number_ids)} summaries")
            
            print(f"\n🎉 PROFESSOR {professor_id} PROCESSING COMPLETE!")
            print(f"   📊 Overall summary: {'✅' if overall_id else '❌'}")
            # print(f"   📚 Course summaries: {len(course_ids)}")
            if include_course_numbers:
                print(f"   🔢 Course number summaries: {len(results['course_number_summary_ids'])}")
            
        except Exception as e:
            results['error'] = str(e)
            print(f"❌ ERROR processing professor {professor_id}: {e}")
            import traceback
            traceback.print_exc()
        
        return results
    
    def process_all_professors(self, include_course_numbers: bool = True) -> List[Dict[str, any]]:
        """Process summaries for all professors with reviews"""
        print("\n🚀 STARTING BATCH PROCESSING OF ALL PROFESSORS")
        print("=" * 80)
        
        # Get all professor IDs that have reviews        
        professors = self.session.query(ProfessorDB).all()
        professor_ids = [professor.id for professor in professors][3:100]
        
        print(f"📊 Found {len(professor_ids)} professors to process")
        if include_course_numbers:
            print(f"🔢 Will create course-specific AND course-number summaries")
        else:
            print(f"📚 Will create course-specific summaries only")
        print(f"🎯 Starting batch processing...")
        
        results = []
        successful = 0
        failed = 0
        
        for i, professor_id in enumerate(professor_ids, 1):
            print(f"\n📍 PROFESSOR {i}/{len(professor_ids)}")
            print(f"🆔 ID: {professor_id}")
            
            result = self.process_professor(professor_id, include_course_numbers)
            results.append(result)
            
            if result['error']:
                failed += 1
                print(f"❌ Professor {i} failed")
            else:
                successful += 1
                print(f"✅ Professor {i} completed successfully")
            
            print(f"📊 Progress: {successful} successful, {failed} failed, {len(professor_ids) - i} remaining")
            
            # Optional: Add delay to prevent overwhelming the system
            # time.sleep(1)
        
        print(f"\n🎉 BATCH PROCESSING COMPLETE!")
        print(f"📊 Final Stats:")
        print(f"   ✅ Successful: {successful}/{len(professor_ids)}")
        print(f"   ❌ Failed: {failed}/{len(professor_ids)}")
        print(f"   📈 Success Rate: {successful/len(professor_ids)*100:.1f}%")
        
        return results
    
    def get_summary(self, professor_id: str, course_code: Optional[str] = None, summary_type: str = 'overall') -> Optional[SummaryDB]:
        """
        Retrieve a summary from the database
        
        Args:
            professor_id: Professor ID
            course_code: Course code (optional, for course-specific or number summaries)
            summary_type: 'overall', 'course_specific', or 'course_number'
            
        Returns:
            SummaryDB record or None
        """
        if summary_type == 'overall':
            summary_id = f"{professor_id}_overall"
        elif summary_type == 'course_number':
            if not course_code:
                raise ValueError("course_code required for course_number summaries")
            summary_id = f"{professor_id}_NUM{course_code}"
        else:  # course_specific
            if not course_code:
                raise ValueError("course_code required for course_specific summaries")
            summary_id = f"{professor_id}_{course_code}"
        
        return self.session.query(SummaryDB).filter_by(id=summary_id).first()
    
    def get_all_summaries_for_professor(self, professor_id: str) -> Dict[str, List[SummaryDB]]:
        """
        Get all summaries for a professor organized by type
        
        Args:
            professor_id: Professor ID
            
        Returns:
            Dictionary with 'overall', 'course_specific', and 'course_number' keys
        """
        all_summaries = self.session.query(SummaryDB).filter_by(professor_id=professor_id).all()
        
        organized = {
            'overall': [],
            'course_specific': [],
            'course_number': []
        }
        
        for summary in all_summaries:
            organized[summary.summary_type].append(summary)
        
        return organized
    
    def close(self):
        """Close database connection"""
        self.session.close()
    
    def add_course_mapping(self, from_dept: str, to_dept: str):
        """
        Add a department alias mapping
        
        Args:
            from_dept: Original department code
            to_dept: Normalized department code
        """
        self.dept_aliases[from_dept.upper()] = to_dept.upper()
        print(f"✅ Added department mapping: {from_dept} -> {to_dept}")
    
    def add_cross_listing(self, dept1: str, number: str, canonical_course: str):
        """
        Add a cross-listing mapping
        
        Args:
            dept1: Department code for cross-listed course
            number: Course number
            canonical_course: The canonical course code to use
        """
        key = (dept1.upper(), number)
        self.cross_listings[key] = canonical_course.upper()
        print(f"✅ Added cross-listing: {dept1}{number} -> {canonical_course}")
    
    def get_normalization_stats(self, reviews: List[ReviewData]) -> Dict[str, any]:
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
        merged_courses = {}
        for orig, norm in zip(original_codes, normalized_codes):
            if norm not in merged_courses:
                merged_courses[norm] = set()
            merged_courses[norm].add(orig)
        
        merged_courses = {k: v for k, v in merged_courses.items() if len(v) > 1}
        
        return {
            'total_reviews': len(original_codes),
            'unique_original_codes': unique_original,
            'unique_normalized_codes': unique_normalized,
            'courses_merged': reduction,
            'merged_course_details': {k: list(v) for k, v in merged_courses.items()}
        }

# Usage example
def main():
    # Database connection string
    summarizer = ReviewSummarizer()
    
    try:
        # Add custom course mappings if needed
        # summarizer.add_course_mapping("CPSC", "CSCE")  # Already included by default
        # summarizer.add_cross_listing("ECEN", "222", "CSCE222")  # Already included by default
        
        # Add any additional mappings specific to your data
        # summarizer.add_course_mapping("COMP", "CSCE")  # If you find COMP as computer science
        # summarizer.add_cross_listing("ELEN", "314", "CSCE314")  # Additional cross-listing
        
        # Test normalization on sample data first
        print("🧪 Testing course normalization...")
        test_codes = ["CPSC 222", "csce222", "ECEN-222", "CS 314", "CSCE314", "Unknown"]
        for code in test_codes:
            normalized = summarizer.normalize_course_code(code)
            print(f"   {code} -> {normalized}")
        
        # Test professor department context normalization
        print("\n🧪 Testing department context normalization...")
        test_codes_math = ["152", "M152", "MATH152", "151", "M151"]
        test_codes_acct = ["152", "A152", "ACCT152", "301", "A301"]
        
        print("   For MATH professor:")
        for code in test_codes_math:
            normalized = summarizer.normalize_course_code_with_context(code, "MATH")
            print(f"      {code:10} -> {normalized}")
        
        print("   For ACCT professor:")
        for code in test_codes_acct:
            normalized = summarizer.normalize_course_code_with_context(code, "ACCT")
            print(f"      {code:10} -> {normalized}")
        
        # Process a single professor
        professor_id = "VGVhY2hlci02MDkxMDE="
        print(f"\n🎯 Processing professor: {professor_id}")
        
        # Get normalization stats for this professor
        reviews = summarizer.fetch_reviews_for_professor(professor_id)
        if reviews:
            # Show professor's primary department
            primary_dept = summarizer.get_professor_primary_department(reviews)
            
            stats = summarizer.get_normalization_stats(reviews)
            print(f"\n📊 Course Normalization Stats:")
            print(f"   📝 Total reviews: {stats['total_reviews']}")
            print(f"   📚 Original unique courses: {stats['unique_original_codes']}")
            print(f"   🔗 Normalized unique courses: {stats['unique_normalized_codes']}")
            print(f"   ⚡ Courses merged: {stats['courses_merged']}")
            
            if stats['merged_course_details']:
                print(f"   🔀 Merged course details:")
                for normalized, originals in stats['merged_course_details'].items():
                    print(f"      {normalized}: {', '.join(originals)}")
            
            # Test course number grouping with department context
            print(f"\n🔢 Testing course number grouping for {primary_dept} professor...")
            course_number_groups = summarizer.group_reviews_by_course_number(reviews, primary_dept)
            
            print(f"   📊 Course number groups:")
            for formatted_code, review_list in course_number_groups.items():
                original_codes = list(set([r.course_code for r in review_list if r.course_code]))
                print(f"      {formatted_code}: {len(review_list)} reviews from {', '.join(original_codes)}")
        
        # Process the professor with both types of summaries
        print(f"\n🚀 Processing professor with all summary types...")
        result = summarizer.process_professor(professor_id, include_course_numbers=True)
        print(f"\n✅ Processing result:")
        print(f"   📊 Overall: {result['overall_summary_id']}")
        print(f"   📚 Course summaries: {len(result['course_summary_ids'])} created")
        print(f"   🔢 Course number summaries: {len(result['course_number_summary_ids'])} created")
        
        if result['course_number_summary_ids']:
            print(f"   🎯 Course number summary IDs: {result['course_number_summary_ids']}")
        
        # Example of how to use for batch processing
        # results = summarizer.process_all_professors(include_course_numbers=True)
        
        # Print summary statistics
        # successful = sum(1 for r in results if not r['error'])
        # print(f"\nProcessing complete: {successful}/{len(results)} professors processed successfully")
        
        # Example: Retrieve different types of summaries
        # overall_summary = summarizer.get_summary(professor_id)  # Overall summary
        # course_summary = summarizer.get_summary(professor_id, "CSCE222")  # Specific course
        # number_summary = summarizer.get_summary(professor_id, "NUMMATH152")  # Course number summary
        
    finally:
        summarizer.close()

if __name__ == "__main__":
    main()