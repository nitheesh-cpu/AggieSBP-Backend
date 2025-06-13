# AggieRMP API Documentation

## Overview

The AggieRMP API provides access to Texas A&M University course and professor data, including ratings, GPA statistics, and course information. The API integrates data from Rate My Professor reviews and anex.us GPA data.

**Base URL**: `http://localhost:8000`

## Authentication

Currently, no authentication is required for API access.

## Response Format

All responses are in JSON format. Successful responses return the requested data, while errors return an error object with details.

---

## Endpoints

### 1. Health Check Endpoints

#### `GET /`
**Description**: Root endpoint with welcome message.

**Response**:
```json
{
  "message": "Welcome to AggieRMP API"
}
```

#### `GET /health`
**Description**: Health check with database status.

**Response**:
```json
{
  "status": "healthy",
  "database": {
    "status": "healthy",
    "pool_status": {
      "pool_size": 10,
      "checked_in": 1,
      "checked_out": 0,
      "overflow": -9,
      "pool_type": "QueuePool"
    }
  },
  "api_version": "1.0.0"
}
```

#### `GET /db-status`
**Description**: Detailed database connection pool status.

**Response**:
```json
{
  "status": "healthy",
  "pool_status": {
    "pool_size": 10,
    "checked_in": 1,
    "checked_out": 0,
    "overflow": -9,
    "pool_type": "QueuePool"
  }
}
```

---

### 2. Departments Endpoints

#### `GET /departments_info`
**Description**: Get aggregate statistics about all departments, including total counts and top departments.

**Parameters**: None

**Example Request**:
```bash
# Get overall department statistics
GET /departments_info
```

**Response**:
```json
{
  "summary": {
    "total_departments": 212,
    "total_courses": 1947,
    "total_professors": 2112,
    "overall_avg_gpa": 3.45,
    "overall_avg_rating": 2.8,
    "stem_departments": 45,
    "liberal_arts_departments": 167
  },
  "top_departments_by_courses": [
    {
      "code": "CSCE",
      "name": "CSCE - Computer Science & Engineering",
      "courses": 78,
      "professors": 85,
      "avgGpa": 3.21,
      "rating": 3.4
    }
  ],
  "recent_semesters": [
    {
      "year": "2024",
      "semester": "FALL",
      "departments": 146,
      "courses": 1947,
      "professors": 2112,
      "enrollment": 220554
    }
  ],
  "data_sources": {
    "gpa_data": "anex.us",
    "reviews": "Rate My Professor",
    "course_catalog": "Texas A&M University",
    "last_updated": "Fall 2024"
  }
}
```

**Response Fields**:
- `summary`: Overall statistics across all departments
  - `total_departments`: Total number of departments
  - `total_courses`: Total courses offered (Fall 2024)
  - `total_professors`: Total professors teaching (Fall 2024)
  - `overall_avg_gpa`: University-wide weighted average GPA
  - `overall_avg_rating`: University-wide average difficulty rating
  - `stem_departments`: Number of STEM departments
  - `liberal_arts_departments`: Number of Liberal Arts departments
- `top_departments_by_courses`: Top 5 departments by number of courses offered
- `recent_semesters`: Statistics for the last 4 semesters
- `data_sources`: Information about data sources and last update

#### `GET /departments`
**Description**: Get all departments with aggregated statistics.

**Parameters**:
- `search` (optional, string): Search by department code or name (case-insensitive)
- `limit` (optional, integer, default: 30): Number of results to return
- `skip` (optional, integer, default: 0): Number of results to skip for pagination

**Example Requests**:
```bash
# Get all departments (first 30)
GET /departments

# Search for computer science departments
GET /departments?search=CSCE

# Search for engineering departments with pagination
GET /departments?search=Engineering&limit=5&skip=0

# Get departments with pagination
GET /departments?limit=10&skip=20
```

**Response**:
```json
[
  {
    "id": "csce",
    "code": "CSCE",
    "name": "CSCE - Computer Science & Engineering",
    "category": "stem",
    "courses": 45,
    "professors": 28,
    "avgGpa": 3.2,
    "rating": 3.4,
    "topCourses": ["CSCE 120", "CSCE 121", "CSCE 221"],
    "description": "Computer Science and Engineering Department"
  }
]
```

**Response Fields**:
- `id`: Department ID (lowercase)
- `code`: Department code (e.g., "CSCE", "MATH")
- `name`: Full department name
- `category`: "stem" or "liberal-arts"
- `courses`: Number of courses offered (Fall 2024)
- `professors`: Number of professors teaching (Fall 2024)
- `avgGpa`: Weighted average GPA from Fall 2024
- `rating`: Difficulty rating from reviews (1-5, where 5 is easiest)
- `topCourses`: Top 3 courses by section count
- `description`: Department description

---

### 3. Courses Endpoint

#### `GET /courses`
**Description**: Get courses with anex data and difficulty ratings.

**Parameters**:
- `department` (optional, string): Filter by department code
- `search` (optional, string): Search by course code or name (case-insensitive)
- `limit` (optional, integer, default: 30): Number of results to return
- `skip` (optional, integer, default: 0): Number of results to skip for pagination

**Example Requests**:
```bash
# Get all courses (first 30)
GET /courses

# Get CSCE courses
GET /courses?department=CSCE

# Search for calculus courses
GET /courses?search=calculus

# Search with pagination
GET /courses?search=CSCE&limit=10&skip=0
```

**Response**:
```json
[
  {
    "id": "csce120",
    "code": "CSCE120",
    "name": "Program Design and Concepts",
    "department": {
      "id": "csce",
      "name": "Computer Science & Engineering"
    },
    "credits": 4,
    "gpa": 3.45,
    "difficulty": "Moderate",
    "enrollment": 980,
    "sections": 43,
    "rating": 3.2,
    "professors": 8,
    "attributes": ["Core Curriculum", "Engineering"]
  }
]
```

**Response Fields**:
- `id`: Course ID (e.g., "csce120")
- `code`: Course code (e.g., "CSCE120")
- `name`: Course title
- `department`: Department object with id and name
- `credits`: Credit hours
- `gpa`: Weighted GPA from last 4 semesters (-1 if no data)
- `difficulty`: Difficulty level based on GPA
  - "Light" (GPA ≥ 3.7)
  - "Moderate" (GPA ≥ 3.3)
  - "Challenging" (GPA ≥ 2.7)
  - "Intensive" (GPA ≥ 2.0)
  - "Rigorous" (GPA < 2.0)
- `enrollment`: Total enrollment from Fall 2024
- `sections`: Number of sections from Fall 2024
- `rating`: Average rating from reviews
- `professors`: Number of professors teaching the course
- `attributes`: Course attributes/tags

---

### 4. Course Details Endpoint

#### `GET /course/{course_id}`
**Description**: Get detailed information for a specific course.

**Parameters**:
- `course_id` (required, string): Course code (e.g., "CSCE120", "MATH151")

**Example Requests**:
```bash
# Get CSCE 120 details
GET /course/CSCE120

# Get MATH 151 details
GET /course/MATH151
```

**Response**:
```json
{
  "id": "csce120",
  "code": "CSCE120",
  "name": "Program Design and Concepts",
  "department": {
    "id": "csce",
    "name": "Computer Science & Engineering"
  },
  "credits": 4,
  "description": "Computation to facilitate problem solving...",
  "gpa": 3.45,
  "difficulty": "Moderate",
  "enrollment": 980,
  "sections": 43,
  "professors": [
    {
      "id": "prof123",
      "name": "Dr. John Smith",
      "rating": 4.2,
      "difficulty": 3.1,
      "reviews": 25
    }
  ],
  "gradeDistribution": {
    "A": 35.2,
    "B": 28.7,
    "C": 20.1,
    "D": 8.9,
    "F": 7.1
  },
  "prerequisites": ["MATH 151"],
  "corequisites": [],
  "relatedCourses": ["CSCE 121", "CSCE 221"],
  "attributes": ["Core Curriculum", "Engineering"],
  "sections": [
    {
      "section": "500",
      "professor": "Dr. John Smith",
      "time": "MWF 10:20-11:10",
      "attributes": ["Honors"]
    }
  ]
}
```

**Response Fields**:
- `id`: Course ID
- `code`: Course code
- `name`: Course title
- `department`: Department information
- `credits`: Credit hours
- `description`: Course description
- `gpa`: Weighted GPA from last 4 semesters
- `difficulty`: Difficulty categorization
- `enrollment`: Total enrollment (Fall 2024)
- `sections`: Number of sections (Fall 2024)
- `professors`: Array of professors teaching the course
- `gradeDistribution`: Percentage distribution of grades
- `prerequisites`: Required prerequisite courses
- `corequisites`: Required corequisite courses
- `relatedCourses`: Suggested related courses
- `attributes`: Course attributes/tags
- `sections`: Section details with professors and times

---

### 5. Course Reviews Endpoint

#### `GET /course/{course_id}/reviews/{professor_id}`
**Description**: Get reviews for a specific course-professor combination.

**Parameters**:
- `course_id` (required, string): Course code (e.g., "CSCE120")
- `professor_id` (required, string): Professor ID
- `limit` (optional, integer, default: 50): Number of reviews to return
- `skip` (optional, integer, default: 0): Number of reviews to skip for pagination

**Example Requests**:
```bash
# Get reviews for CSCE 120 with specific professor
GET /course/CSCE120/reviews/prof123

# Get reviews with pagination
GET /course/CSCE120/reviews/prof123?limit=20&skip=0
```

**Response**:
```json
{
  "course": {
    "id": "csce120",
    "code": "CSCE120",
    "name": "Program Design and Concepts"
  },
  "professor": {
    "id": "prof123",
    "name": "Dr. John Smith"
  },
  "summary": {
    "total_reviews": 42,
    "avg_clarity": 4.2,
    "avg_difficulty": 3.1,
    "avg_helpfulness": 4.0,
    "avg_overall": 4.1,
    "would_take_again_percent": 78.5
  },
  "reviews": [
    {
      "id": "review123",
      "clarity_rating": 4.0,
      "difficulty_rating": 3.0,
      "helpful_rating": 4.0,
      "overall_rating": 4.0,
      "would_take_again": true,
      "grade": "A",
      "review_text": "Great professor, explains concepts clearly...",
      "review_date": "2024-05-15",
      "tags": ["Clear", "Helpful", "Tough Grader"],
      "thumbs_up": 15,
      "thumbs_down": 2,
      "attendance_mandatory": "Yes",
      "textbook_use": "Required"
    }
  ],
  "pagination": {
    "total": 42,
    "limit": 50,
    "skip": 0,
    "has_more": false
  }
}
```

**Response Fields**:
- `course`: Course information
- `professor`: Professor information
- `summary`: Aggregated statistics for all reviews
- `reviews`: Array of individual reviews
- `pagination`: Pagination information

---

## Error Responses

All endpoints may return error responses in the following format:

```json
{
  "detail": "Error message describing what went wrong"
}
```

**Common HTTP Status Codes**:
- `200`: Success
- `404`: Resource not found (invalid course ID, professor ID, etc.)
- `422`: Validation error (invalid parameters)
- `500`: Internal server error (database issues, etc.)

---

## Rate Limiting

Currently, no rate limiting is implemented. For production use, consider implementing rate limiting based on your requirements.

---

## Data Sources

- **Course Data**: Texas A&M course catalog
- **GPA Data**: anex.us (last 4 semesters for trends, Fall 2024 for current data)
- **Review Data**: Rate My Professor reviews
- **Professor Data**: Aggregated from reviews and course assignments

---

## Performance Notes

- The API uses connection pooling for optimal database performance
- Complex queries may take 1-3 seconds for remote database connections
- Local database setup can reduce response times to 10-50ms
- See `database_config.md` for performance optimization guide

---

## Examples

### Get Overall Department Statistics
```bash
curl "http://localhost:8000/departments_info"
```

### Get Computer Science Departments
```bash
curl "http://localhost:8000/departments?search=Computer"
```

### Get All CSCE Courses
```bash
curl "http://localhost:8000/courses?department=CSCE&limit=10"
```

### Get Course Details
```bash
curl "http://localhost:8000/course/CSCE120"
```

### Get Course Reviews
```bash
curl "http://localhost:8000/course/CSCE120/reviews/prof123?limit=10"
```

### Check API Health
```bash
curl "http://localhost:8000/health"
``` 