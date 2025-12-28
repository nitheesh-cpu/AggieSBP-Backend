"""
Clustering module using HDBSCAN for semantic grouping.
"""

import numpy as np
from typing import List, Dict, Tuple
import hdbscan

from pipelines.professors.hierarchical_summarization.config import (
    HDBSCAN_MIN_CLUSTER_SIZE,
    HDBSCAN_MIN_SAMPLES,
    CLUSTER_KEYWORDS,
)
from pipelines.professors.schemas import ProcessedReview


class ReviewClusterer:
    """Clusters reviews using HDBSCAN"""
    
    def __init__(self):
        self.min_cluster_size = HDBSCAN_MIN_CLUSTER_SIZE
        self.min_samples = HDBSCAN_MIN_SAMPLES
    
    def cluster_reviews(
        self,
        reviews: List[ProcessedReview],
        embeddings: np.ndarray
    ) -> Dict[int, List[ProcessedReview]]:
        """
        Cluster reviews using HDBSCAN.
        
        Args:
            reviews: List of ProcessedReview objects
            embeddings: numpy array of embeddings (n_reviews, embedding_dim)
        
        Returns:
            Dictionary mapping cluster_id to list of reviews
            Cluster ID -1 indicates noise (ignored)
        """
        if len(reviews) < self.min_cluster_size:
            return {0: reviews}
        
        # Fit HDBSCAN
        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=self.min_cluster_size,
            min_samples=self.min_samples,
            metric='euclidean',
            cluster_selection_method='eom'
        )
        
        cluster_labels = clusterer.fit_predict(embeddings)
        
        # Group reviews by cluster
        clusters: Dict[int, List[ProcessedReview]] = {}
        noise_count = 0
        
        for review, label in zip(reviews, cluster_labels):
            if label == -1:  # Noise
                noise_count += 1
                continue
            
            if label not in clusters:
                clusters[label] = []
            clusters[label].append(review)
        
        print(f"Found {len(clusters)} clusters, {noise_count} noise points")
        
        return clusters
    
    def identify_cluster_type(self, cluster_reviews: List[ProcessedReview]) -> str:
        """
        Identify the semantic type of a cluster based on keywords.
        
        Args:
            cluster_reviews: List of reviews in the cluster
        
        Returns:
            Cluster type string
        """
        # Combine all review texts
        combined_text = " ".join([review.text.lower() for review in cluster_reviews])
        
        # Score each cluster type
        scores = {}
        for cluster_type, keywords in CLUSTER_KEYWORDS.items():
            score = sum(1 for keyword in keywords if keyword in combined_text)
            scores[cluster_type] = score
        
        # Return type with highest score, or "other" if no match
        if scores and max(scores.values()) > 0:
            return max(scores, key=scores.get)
        return "other"
    
    def cluster_by_course(
        self,
        reviews: List[ProcessedReview],
        embeddings: np.ndarray
    ) -> Dict[str, Dict[int, List[ProcessedReview]]]:
        """
        Cluster reviews grouped by course.
        
        Args:
            reviews: List of ProcessedReview objects
            embeddings: numpy array of embeddings
        
        Returns:
            Dictionary mapping course_code to cluster dictionary
        """
        # Group reviews by course
        reviews_by_course: Dict[str, List[ProcessedReview]] = {}
        embeddings_by_course: Dict[str, List[int]] = {}
        
        for i, review in enumerate(reviews):
            course = review.course_code or "UNKNOWN"
            if course not in reviews_by_course:
                reviews_by_course[course] = []
                embeddings_by_course[course] = []
            reviews_by_course[course].append(review)
            embeddings_by_course[course].append(i)
        
        # Cluster each course separately
        course_clusters: Dict[str, Dict[int, List[ProcessedReview]]] = {}
        
        for course, course_reviews in reviews_by_course.items():
            if len(course_reviews) < self.min_cluster_size:
                # Too few reviews, put all in one cluster
                course_clusters[course] = {0: course_reviews}
                continue
            
            # Get embeddings for this course
            course_indices = embeddings_by_course[course]
            course_embeddings = embeddings[course_indices]
            
            # Cluster
            clusters = self.cluster_reviews(course_reviews, course_embeddings)
            course_clusters[course] = clusters
        
        return course_clusters

