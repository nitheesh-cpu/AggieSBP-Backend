#!/usr/bin/env python3
"""
RMP Review Collector - Collects reviews for professors from Rate My Professor

This module:
1. Fetches reviews for specific professors using RMP's GraphQL API
2. Processes review data according to the new Review model schema
3. Stores reviews in Appwrite database
4. Handles pagination and rate limiting
5. Supports incremental updates
"""

import uuid
import requests
import json
import time
import os
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional
import asyncio
from dataclasses import dataclass
import requests
from sqlalchemy import select

from database.base import ProfessorDB, create_db_engine, get_session
from models.schema import Professor, Review, University



# Add parent directory for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@dataclass
class ReviewCollectionStats:
    """Statistics for review collection process"""

    total_professors: int = 0
    total_reviews_collected: int = 0
    successful_professors: int = 0
    failed_professors: int = 0
    skipped_professors: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


class RMPReviewCollector:
    """Collects reviews from Rate My Professor for Texas A&M professors"""

    def __init__(self):
        self.url = "https://www.ratemyprofessors.com/graphql"
        self.session = requests.Session()
        self.cookies = self.get_cookies()

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "authorization": "null",
            "content-type": "application/json",
            "priority": "u=1, i",
            "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "referer": "https://www.ratemyprofessors.com/",
        }

        # Rate limiting
        self.request_delay = 1.0  # Seconds between requests
        self.max_retries = 3

        # Statistics
        self.stats = ReviewCollectionStats()
    
    def get_cookies(self):
        self.session.get("https://www.ratemyprofessors.com/")
        return self.session.cookies.get_dict()

    def get_all_professors(self, university_id, limit: int = 1000) -> List[Professor]:
        """Fetches all professors from Texas A&M University"""
        professors = []
        response = self.session.post(
            "https://www.ratemyprofessors.com/graphql",
            headers=self.headers,
            cookies=self.cookies,
            json={
                "query": 'query TeacherSearchResultsPageQuery(\n  $query: TeacherSearchQuery!\n  $schoolID: ID\n  $includeSchoolFilter: Boolean!\n) {\n  search: newSearch {\n    ...TeacherSearchPagination_search_1ZLmLD\n  }\n  school: node(id: $schoolID) @include(if: $includeSchoolFilter) {\n    __typename\n    ... on School {\n      name\n      ...StickyHeaderContent_school\n    }\n    id\n  }\n}\n\nfragment CardFeedback_teacher on Teacher {\n  wouldTakeAgainPercent\n  avgDifficulty\n}\n\nfragment CardName_teacher on Teacher {\n  firstName\n  lastName\n}\n\nfragment CardSchool_teacher on Teacher {\n  department\n  school {\n    name\n    id\n  }\n}\n\nfragment CompareSchoolLink_school on School {\n  legacyId\n}\n\nfragment HeaderDescription_school on School {\n  name\n  city\n  state\n  legacyId\n  ...RateSchoolLink_school\n  ...CompareSchoolLink_school\n}\n\nfragment HeaderRateButton_school on School {\n  ...RateSchoolLink_school\n  ...CompareSchoolLink_school\n}\n\nfragment RateSchoolLink_school on School {\n  legacyId\n}\n\nfragment StickyHeaderContent_school on School {\n  name\n  ...HeaderDescription_school\n  ...HeaderRateButton_school\n}\n\nfragment TeacherBookmark_teacher on Teacher {\n  id\n  isSaved\n}\n\nfragment TeacherCard_teacher on Teacher {\n  id\n  legacyId\n  avgRating\n  numRatings\n  ...CardFeedback_teacher\n  ...CardSchool_teacher\n  ...CardName_teacher\n  ...TeacherBookmark_teacher\n}\n\nfragment TeacherSearchPagination_search_1ZLmLD on newSearch {\n  teachers(query: $query, first: 8, after: "") {\n    didFallback\n    edges {\n      cursor\n      node {\n        ...TeacherCard_teacher\n        id\n        __typename\n      }\n    }\n    pageInfo {\n      hasNextPage\n      endCursor\n    }\n    resultCount\n    filters {\n      field\n      options {\n        value\n        id\n      }\n    }\n  }\n}\n',
                "variables": {
                    "query": {
                        "text": "",
                        "schoolID": university_id,
                        "fallback": True,
                    },
                    "schoolID": university_id,
                    "count": limit,
                    "includeSchoolFilter": True,
                },
            },
        )
        data = (
            response.json()
            .get("data", {})
            .get("search", {})
            .get("teachers", {})
            .get("edges", [])
        )
        more_data = (
            response.json()
            .get("data", {})
            .get("search", {})
            .get("teachers", {})
            .get("pageInfo", {})
            .get("hasNextPage", False)
        )
        cursor = (
            response.json()
            .get("data", {})
            .get("search", {})
            .get("teachers", {})
            .get("pageInfo", {})
            .get("endCursor", "")
        )
        professors = [
            Professor(
                **edge["node"],
                university_id=university_id
            )
            for edge in data
        ]
        

        while more_data:
            response = self.session.post(
                "https://www.ratemyprofessors.com/graphql",
                headers=self.headers,
                cookies=self.cookies,
                json={
                    "query": "query TeacherSearchPaginationQuery(\n  $count: Int!\n  $cursor: String\n  $query: TeacherSearchQuery!\n) {\n  search: newSearch {\n    ...TeacherSearchPagination_search_1jWD3d\n  }\n}\n\nfragment CardFeedback_teacher on Teacher {\n  wouldTakeAgainPercent\n  avgDifficulty\n}\n\nfragment CardName_teacher on Teacher {\n  firstName\n  lastName\n}\n\nfragment CardSchool_teacher on Teacher {\n  department\n  school {\n    name\n    id\n  }\n}\n\nfragment TeacherBookmark_teacher on Teacher {\n  id\n  isSaved\n}\n\nfragment TeacherCard_teacher on Teacher {\n  id\n  legacyId\n  avgRating\n  numRatings\n  ...CardFeedback_teacher\n  ...CardSchool_teacher\n  ...CardName_teacher\n  ...TeacherBookmark_teacher\n}\n\nfragment TeacherSearchPagination_search_1jWD3d on newSearch {\n  teachers(query: $query, first: $count, after: $cursor) {\n    didFallback\n    edges {\n      cursor\n      node {\n        ...TeacherCard_teacher\n        id\n        __typename\n      }\n    }\n    pageInfo {\n      hasNextPage\n      endCursor\n    }\n    resultCount\n    filters {\n      field\n      options {\n        value\n        id\n      }\n    }\n  }\n}\n",
                    "variables": {
                        "count": limit,
                        "cursor": cursor,
                        "query": {
                            "text": "",
                            "schoolID": university_id,
                            "fallback": True,
                        },
                    },
                },
            )
            data = response.json().get("data", {}).get("search", {}).get("teachers", {}).get("edges", [])
            more_data = response.json().get("data", {}).get("search", {}).get("teachers", {}).get("pageInfo", {}).get("hasNextPage", False)
            cursor = response.json().get("data", {}).get("search", {}).get("teachers", {}).get("pageInfo", {}).get("endCursor", "")
            professors.extend([Professor(
                **edge["node"],
                university_id=university_id
            ) for edge in data])
            print(f"Collected {len(professors)} professors")
        return professors

    def get_professor(self, professor_name, university_id) -> Professor:
        """Returns the professor details for a given professor name"""
        response = self.session.post(
            "https://www.ratemyprofessors.com/graphql",
            headers=self.headers,
            cookies=self.cookies,
            json={
                "query": 'query TeacherSearchResultsPageQuery(\n  $query: TeacherSearchQuery!\n  $schoolID: ID\n  $includeSchoolFilter: Boolean!\n) {\n  search: newSearch {\n    ...TeacherSearchPagination_search_1ZLmLD\n  }\n  school: node(id: $schoolID) @include(if: $includeSchoolFilter) {\n    __typename\n    ... on School {\n      name\n      ...StickyHeaderContent_school\n    }\n    id\n  }\n}\n\nfragment CardFeedback_teacher on Teacher {\n  wouldTakeAgainPercent\n  avgDifficulty\n}\n\nfragment CardName_teacher on Teacher {\n  firstName\n  lastName\n}\n\nfragment CardSchool_teacher on Teacher {\n  department\n  school {\n    name\n    id\n  }\n}\n\nfragment CompareSchoolLink_school on School {\n  legacyId\n}\n\nfragment HeaderDescription_school on School {\n  name\n  city\n  state\n  legacyId\n  ...RateSchoolLink_school\n  ...CompareSchoolLink_school\n}\n\nfragment HeaderRateButton_school on School {\n  ...RateSchoolLink_school\n  ...CompareSchoolLink_school\n}\n\nfragment RateSchoolLink_school on School {\n  legacyId\n}\n\nfragment StickyHeaderContent_school on School {\n  name\n  ...HeaderDescription_school\n  ...HeaderRateButton_school\n}\n\nfragment TeacherBookmark_teacher on Teacher {\n  id\n  isSaved\n}\n\nfragment TeacherCard_teacher on Teacher {\n  id\n  legacyId\n  avgRating\n  numRatings\n  ...CardFeedback_teacher\n  ...CardSchool_teacher\n  ...CardName_teacher\n  ...TeacherBookmark_teacher\n}\n\nfragment TeacherSearchPagination_search_1ZLmLD on newSearch {\n  teachers(query: $query, first: 8, after: "") {\n    didFallback\n    edges {\n      cursor\n      node {\n        ...TeacherCard_teacher\n        id\n        __typename\n      }\n    }\n    pageInfo {\n      hasNextPage\n      endCursor\n    }\n    resultCount\n    filters {\n      field\n      options {\n        value\n        id\n      }\n    }\n  }\n}\n',
                "variables": {
                    "query": {
                        "text": professor_name,
                        "schoolID": university_id,
                        "fallback": True,
                    },
                    "schoolID": university_id,
                    "count": 1,
                    "includeSchoolFilter": True,
                },
            },
        )
        data = response.json()
        professor_data = (
            data.get("data", {})
            .get("search", {})
            .get("teachers", {})
            .get("edges", [])[0]
            .get("node", {})
        )
        return Professor(
            **professor_data,
            university_id=university_id,
        )

    def get_university_id(self, search_str) -> University:
        """Returns the university ID for Texas A&M"""
        response = self.session.post(
            "https://www.ratemyprofessors.com/graphql",
            headers=self.headers,
            cookies=self.cookies,
            json={
                "query": "query NewSearchSchoolsQuery(\n  $query: SchoolSearchQuery\n  $includeCompare: Boolean!\n) {\n  newSearch {\n    schools(query: $query) {\n      edges {\n        cursor\n        node {\n          id\n          legacyId\n          name\n          city\n          state\n          departments {\n            id\n            name\n          }\n          name @include(if: $includeCompare)\n          city @include(if: $includeCompare)\n          state @include(if: $includeCompare)\n          country @include(if: $includeCompare)\n          numRatings @include(if: $includeCompare)\n          avgRatingRounded @include(if: $includeCompare)\n          legacyId @include(if: $includeCompare)\n          summary @include(if: $includeCompare) {\n            campusCondition\n            campusLocation\n            careerOpportunities\n            clubAndEventActivities\n            foodQuality\n            internetSpeed\n            libraryCondition\n            schoolReputation\n            schoolSafety\n            schoolSatisfaction\n            socialActivities\n          }\n        }\n      }\n      pageInfo {\n        hasNextPage\n        endCursor\n      }\n    }\n  }\n}\n",
                "variables": {
                    "query": {"text": search_str},
                    "includeCompare": False,
                },
            },
        )
        data = response.json()
        schools = (
            data.get("data", {})
            .get("newSearch", {})
            .get("schools", {})
            .get("edges", [])
        )
        if schools:
            return University(
                **schools[0]["node"]
            )
        else:
            raise ValueError("No university found with that name")

    def get_reviews(self, professor_id, session):
        num_ratings = session.execute(select(ProfessorDB.num_ratings).where(ProfessorDB.id == professor_id)).scalar_one()

        response = self.session.post(
            "https://www.ratemyprofessors.com/graphql",
            headers=self.headers,
            cookies=self.cookies,
            json={
                    "query": "query RatingsListQuery(\n  $count: Int!\n  $id: ID!\n  $courseFilter: String\n  $cursor: String\n) {\n  node(id: $id) {\n    __typename\n    ... on Teacher {\n      ...RatingsList_teacher_4pguUW\n    }\n    id\n  }\n}\n\nfragment CourseMeta_rating on Rating {\n  attendanceMandatory\n  wouldTakeAgain\n  grade\n  textbookUse\n  isForOnlineClass\n  isForCredit\n}\n\nfragment NoRatingsArea_teacher on Teacher {\n  lastName\n  ...RateTeacherLink_teacher\n}\n\nfragment ProfessorNoteEditor_rating on Rating {\n  id\n  legacyId\n  class\n  teacherNote {\n    id\n    teacherId\n    comment\n  }\n}\n\nfragment ProfessorNoteEditor_teacher on Teacher {\n  id\n}\n\nfragment ProfessorNoteFooter_note on TeacherNotes {\n  legacyId\n  flagStatus\n}\n\nfragment ProfessorNoteFooter_teacher on Teacher {\n  legacyId\n  isProfCurrentUser\n}\n\nfragment ProfessorNoteHeader_note on TeacherNotes {\n  createdAt\n  updatedAt\n}\n\nfragment ProfessorNoteHeader_teacher on Teacher {\n  lastName\n}\n\nfragment ProfessorNoteSection_rating on Rating {\n  teacherNote {\n    ...ProfessorNote_note\n    id\n  }\n  ...ProfessorNoteEditor_rating\n}\n\nfragment ProfessorNoteSection_teacher on Teacher {\n  ...ProfessorNote_teacher\n  ...ProfessorNoteEditor_teacher\n}\n\nfragment ProfessorNote_note on TeacherNotes {\n  comment\n  ...ProfessorNoteHeader_note\n  ...ProfessorNoteFooter_note\n}\n\nfragment ProfessorNote_teacher on Teacher {\n  ...ProfessorNoteHeader_teacher\n  ...ProfessorNoteFooter_teacher\n}\n\nfragment RateTeacherLink_teacher on Teacher {\n  legacyId\n  numRatings\n  lockStatus\n}\n\nfragment RatingFooter_rating on Rating {\n  id\n  comment\n  adminReviewedAt\n  flagStatus\n  legacyId\n  thumbsUpTotal\n  thumbsDownTotal\n  thumbs {\n    thumbsUp\n    thumbsDown\n    computerId\n    id\n  }\n  teacherNote {\n    id\n  }\n  ...Thumbs_rating\n}\n\nfragment RatingFooter_teacher on Teacher {\n  id\n  legacyId\n  lockStatus\n  isProfCurrentUser\n  ...Thumbs_teacher\n}\n\nfragment RatingHeader_rating on Rating {\n  legacyId\n  date\n  class\n  helpfulRating\n  clarityRating\n  isForOnlineClass\n}\n\nfragment RatingSuperHeader_rating on Rating {\n  legacyId\n}\n\nfragment RatingSuperHeader_teacher on Teacher {\n  firstName\n  lastName\n  legacyId\n  school {\n    name\n    id\n  }\n}\n\nfragment RatingTags_rating on Rating {\n  ratingTags\n}\n\nfragment RatingValues_rating on Rating {\n  helpfulRating\n  clarityRating\n  difficultyRating\n}\n\nfragment Rating_rating on Rating {\n  comment\n  flagStatus\n  createdByUser\n  teacherNote {\n    id\n  }\n  ...RatingHeader_rating\n  ...RatingSuperHeader_rating\n  ...RatingValues_rating\n  ...CourseMeta_rating\n  ...RatingTags_rating\n  ...RatingFooter_rating\n  ...ProfessorNoteSection_rating\n}\n\nfragment Rating_teacher on Teacher {\n  ...RatingFooter_teacher\n  ...RatingSuperHeader_teacher\n  ...ProfessorNoteSection_teacher\n}\n\nfragment RatingsList_teacher_4pguUW on Teacher {\n  id\n  legacyId\n  lastName\n  numRatings\n  school {\n    id\n    legacyId\n    name\n    city\n    state\n    avgRating\n    numRatings\n  }\n  ...Rating_teacher\n  ...NoRatingsArea_teacher\n  ratings(first: $count, after: $cursor, courseFilter: $courseFilter) {\n    edges {\n      cursor\n      node {\n        ...Rating_rating\n        id\n        __typename\n      }\n    }\n    pageInfo {\n      hasNextPage\n      endCursor\n    }\n  }\n}\n\nfragment Thumbs_rating on Rating {\n  id\n  comment\n  adminReviewedAt\n  flagStatus\n  legacyId\n  thumbsUpTotal\n  thumbsDownTotal\n  thumbs {\n    computerId\n    thumbsUp\n    thumbsDown\n    id\n  }\n  teacherNote {\n    id\n  }\n}\n\nfragment Thumbs_teacher on Teacher {\n  id\n  legacyId\n  lockStatus\n  isProfCurrentUser\n}\n",
                    "variables": {
                    "count": num_ratings,
                    "id": professor_id,
                    "courseFilter": None,
                }
            }
        )
        reviews = response.json().get("data", {}).get("node", {}).get("ratings", {}).get("edges", [])
        reviews = [Review(**review["node"], professor_id=professor_id) for review in reviews]
        return reviews
    
    def get_all_reviews(self, professor_id):
        """Returns all reviews for a given professor"""
        
        reviews = []
        response = self.session.post(
            "https://www.ratemyprofessors.com/graphql",
            headers=self.headers,
            cookies=self.cookies,
            json={
                    "query": "query RatingsListQuery(\n  $count: Int!\n  $id: ID!\n  $courseFilter: String\n  $cursor: String\n) {\n  node(id: $id) {\n    __typename\n    ... on Teacher {\n      ...RatingsList_teacher_4pguUW\n    }\n    id\n  }\n}\n\nfragment CourseMeta_rating on Rating {\n  attendanceMandatory\n  wouldTakeAgain\n  grade\n  textbookUse\n  isForOnlineClass\n  isForCredit\n}\n\nfragment NoRatingsArea_teacher on Teacher {\n  lastName\n  ...RateTeacherLink_teacher\n}\n\nfragment ProfessorNoteEditor_rating on Rating {\n  id\n  legacyId\n  class\n  teacherNote {\n    id\n    teacherId\n    comment\n  }\n}\n\nfragment ProfessorNoteEditor_teacher on Teacher {\n  id\n}\n\nfragment ProfessorNoteFooter_note on TeacherNotes {\n  legacyId\n  flagStatus\n}\n\nfragment ProfessorNoteFooter_teacher on Teacher {\n  legacyId\n  isProfCurrentUser\n}\n\nfragment ProfessorNoteHeader_note on TeacherNotes {\n  createdAt\n  updatedAt\n}\n\nfragment ProfessorNoteHeader_teacher on Teacher {\n  lastName\n}\n\nfragment ProfessorNoteSection_rating on Rating {\n  teacherNote {\n    ...ProfessorNote_note\n    id\n  }\n  ...ProfessorNoteEditor_rating\n}\n\nfragment ProfessorNoteSection_teacher on Teacher {\n  ...ProfessorNote_teacher\n  ...ProfessorNoteEditor_teacher\n}\n\nfragment ProfessorNote_note on TeacherNotes {\n  comment\n  ...ProfessorNoteHeader_note\n  ...ProfessorNoteFooter_note\n}\n\nfragment ProfessorNote_teacher on Teacher {\n  ...ProfessorNoteHeader_teacher\n  ...ProfessorNoteFooter_teacher\n}\n\nfragment RateTeacherLink_teacher on Teacher {\n  legacyId\n  numRatings\n  lockStatus\n}\n\nfragment RatingFooter_rating on Rating {\n  id\n  comment\n  adminReviewedAt\n  flagStatus\n  legacyId\n  thumbsUpTotal\n  thumbsDownTotal\n  thumbs {\n    thumbsUp\n    thumbsDown\n    computerId\n    id\n  }\n  teacherNote {\n    id\n  }\n  ...Thumbs_rating\n}\n\nfragment RatingFooter_teacher on Teacher {\n  id\n  legacyId\n  lockStatus\n  isProfCurrentUser\n  ...Thumbs_teacher\n}\n\nfragment RatingHeader_rating on Rating {\n  legacyId\n  date\n  class\n  helpfulRating\n  clarityRating\n  isForOnlineClass\n}\n\nfragment RatingSuperHeader_rating on Rating {\n  legacyId\n}\n\nfragment RatingSuperHeader_teacher on Teacher {\n  firstName\n  lastName\n  legacyId\n  school {\n    name\n    id\n  }\n}\n\nfragment RatingTags_rating on Rating {\n  ratingTags\n}\n\nfragment RatingValues_rating on Rating {\n  helpfulRating\n  clarityRating\n  difficultyRating\n}\n\nfragment Rating_rating on Rating {\n  comment\n  flagStatus\n  createdByUser\n  teacherNote {\n    id\n  }\n  ...RatingHeader_rating\n  ...RatingSuperHeader_rating\n  ...RatingValues_rating\n  ...CourseMeta_rating\n  ...RatingTags_rating\n  ...RatingFooter_rating\n  ...ProfessorNoteSection_rating\n}\n\nfragment Rating_teacher on Teacher {\n  ...RatingFooter_teacher\n  ...RatingSuperHeader_teacher\n  ...ProfessorNoteSection_teacher\n}\n\nfragment RatingsList_teacher_4pguUW on Teacher {\n  id\n  legacyId\n  lastName\n  numRatings\n  school {\n    id\n    legacyId\n    name\n    city\n    state\n    avgRating\n    numRatings\n  }\n  ...Rating_teacher\n  ...NoRatingsArea_teacher\n  ratings(first: $count, after: $cursor, courseFilter: $courseFilter) {\n    edges {\n      cursor\n      node {\n        ...Rating_rating\n        id\n        __typename\n      }\n    }\n    pageInfo {\n      hasNextPage\n      endCursor\n    }\n  }\n}\n\nfragment Thumbs_rating on Rating {\n  id\n  comment\n  adminReviewedAt\n  flagStatus\n  legacyId\n  thumbsUpTotal\n  thumbsDownTotal\n  thumbs {\n    computerId\n    thumbsUp\n    thumbsDown\n    id\n  }\n  teacherNote {\n    id\n  }\n}\n\nfragment Thumbs_teacher on Teacher {\n  id\n  legacyId\n  lockStatus\n  isProfCurrentUser\n}\n",
                    "variables": {
                    "count": 200,
                    "id": professor_id,
                    "courseFilter": None,
                }
            }
        )
        data = response.json().get("data", {}).get("node", {}).get("ratings", {}).get("edges", [])
        more_data = response.json().get("data", {}).get("node", {}).get("ratings", {}).get("pageInfo", {}).get("hasNextPage", False)
        cursor = response.json().get("data", {}).get("node", {}).get("ratings", {}).get("pageInfo", {}).get("endCursor", "")
        reviews = [Review(**review["node"], professor_id=professor_id) for review in data]
        
        while more_data:
            response = self.session.post(
                "https://www.ratemyprofessors.com/graphql",
                headers=self.headers,
                cookies=self.cookies,
                json={
                    "query": "query RatingsListQuery(\n  $count: Int!\n  $id: ID!\n  $courseFilter: String\n  $cursor: String\n) {\n  node(id: $id) {\n    __typename\n    ... on Teacher {\n      ...RatingsList_teacher_4pguUW\n    }\n    id\n  }\n}\n\nfragment CourseMeta_rating on Rating {\n  attendanceMandatory\n  wouldTakeAgain\n  grade\n  textbookUse\n  isForOnlineClass\n  isForCredit\n}\n\nfragment NoRatingsArea_teacher on Teacher {\n  lastName\n  ...RateTeacherLink_teacher\n}\n\nfragment ProfessorNoteEditor_rating on Rating {\n  id\n  legacyId\n  class\n  teacherNote {\n    id\n    teacherId\n    comment\n  }\n}\n\nfragment ProfessorNoteEditor_teacher on Teacher {\n  id\n}\n\nfragment ProfessorNoteFooter_note on TeacherNotes {\n  legacyId\n  flagStatus\n}\n\nfragment ProfessorNoteFooter_teacher on Teacher {\n  legacyId\n  isProfCurrentUser\n}\n\nfragment ProfessorNoteHeader_note on TeacherNotes {\n  createdAt\n  updatedAt\n}\n\nfragment ProfessorNoteHeader_teacher on Teacher {\n  lastName\n}\n\nfragment ProfessorNoteSection_rating on Rating {\n  teacherNote {\n    ...ProfessorNote_note\n    id\n  }\n  ...ProfessorNoteEditor_rating\n}\n\nfragment ProfessorNoteSection_teacher on Teacher {\n  ...ProfessorNote_teacher\n  ...ProfessorNoteEditor_teacher\n}\n\nfragment ProfessorNote_note on TeacherNotes {\n  comment\n  ...ProfessorNoteHeader_note\n  ...ProfessorNoteFooter_note\n}\n\nfragment ProfessorNote_teacher on Teacher {\n  ...ProfessorNoteHeader_teacher\n  ...ProfessorNoteFooter_teacher\n}\n\nfragment RateTeacherLink_teacher on Teacher {\n  legacyId\n  numRatings\n  lockStatus\n}\n\nfragment RatingFooter_rating on Rating {\n  id\n  comment\n  adminReviewedAt\n  flagStatus\n  legacyId\n  thumbsUpTotal\n  thumbsDownTotal\n  thumbs {\n    thumbsUp\n    thumbsDown\n    computerId\n    id\n  }\n  teacherNote {\n    id\n  }\n  ...Thumbs_rating\n}\n\nfragment RatingFooter_teacher on Teacher {\n  id\n  legacyId\n  lockStatus\n  isProfCurrentUser\n  ...Thumbs_teacher\n}\n\nfragment RatingHeader_rating on Rating {\n  legacyId\n  date\n  class\n  helpfulRating\n  clarityRating\n  isForOnlineClass\n}\n\nfragment RatingSuperHeader_rating on Rating {\n  legacyId\n}\n\nfragment RatingSuperHeader_teacher on Teacher {\n  firstName\n  lastName\n  legacyId\n  school {\n    name\n    id\n  }\n}\n\nfragment RatingTags_rating on Rating {\n  ratingTags\n}\n\nfragment RatingValues_rating on Rating {\n  helpfulRating\n  clarityRating\n  difficultyRating\n}\n\nfragment Rating_rating on Rating {\n  comment\n  flagStatus\n  createdByUser\n  teacherNote {\n    id\n  }\n  ...RatingHeader_rating\n  ...RatingSuperHeader_rating\n  ...RatingValues_rating\n  ...CourseMeta_rating\n  ...RatingTags_rating\n  ...RatingFooter_rating\n  ...ProfessorNoteSection_rating\n}\n\nfragment Rating_teacher on Teacher {\n  ...RatingFooter_teacher\n  ...RatingSuperHeader_teacher\n  ...ProfessorNoteSection_teacher\n}\n\nfragment RatingsList_teacher_4pguUW on Teacher {\n  id\n  legacyId\n  lastName\n  numRatings\n  school {\n    id\n    legacyId\n    name\n    city\n    state\n    avgRating\n    numRatings\n  }\n  ...Rating_teacher\n  ...NoRatingsArea_teacher\n  ratings(first: $count, after: $cursor, courseFilter: $courseFilter) {\n    edges {\n      cursor\n      node {\n        ...Rating_rating\n        id\n        __typename\n      }\n    }\n    pageInfo {\n      hasNextPage\n      endCursor\n    }\n  }\n}\n\nfragment Thumbs_rating on Rating {\n  id\n  comment\n  adminReviewedAt\n  flagStatus\n  legacyId\n  thumbsUpTotal\n  thumbsDownTotal\n  thumbs {\n    computerId\n    thumbsUp\n    thumbsDown\n    id\n  }\n  teacherNote {\n    id\n  }\n}\n\nfragment Thumbs_teacher on Teacher {\n  id\n  legacyId\n  lockStatus\n  isProfCurrentUser\n}\n",
                    "variables": {
                    "count": 200,
                    "id": professor_id,
                    "courseFilter": None,
                    "cursor": cursor,
                }
            }
            )
            data = response.json().get("data", {}).get("node", {}).get("ratings", {}).get("edges", [])
            more_data = response.json().get("data", {}).get("node", {}).get("ratings", {}).get("pageInfo", {}).get("hasNextPage", False)
            cursor = response.json().get("data", {}).get("node", {}).get("ratings", {}).get("pageInfo", {}).get("endCursor", "")
            reviews.extend([Review(**review["node"], professor_id=professor_id) for review in data])
        return reviews
            
    def get_reviews_since_date(self, professor_id, last_review_date):
        """Returns all reviews for a given professor since a given date"""
        reviews = self.get_all_reviews(professor_id)
        return [review for review in reviews if review.review_date > last_review_date]
            
            
def main():
    """Main function for testing"""
    collector = RMPReviewCollector()
    # university = collector.get_university_id("Texas A&M University")
    # print(university.model_dump())
    
    # professor = collector.get_professor("leyk", "U2Nob29sLTEwMDM=")
    # print(professor.model_dump())

    # professors = collector.get_all_professors("U2Nob29sLTEwMDM=")
    # print(len(professors))
    
    reviews = collector.get_reviews("VGVhY2hlci02MDkxMDE=")
    print(len(reviews))

    # Test with a few professors first
    # print("🧪 Testing review collection with limited professors...")
    # collector.collect_all_reviews(limit=5)  # Start with 5 professors


if __name__ == "__main__":
    main()
