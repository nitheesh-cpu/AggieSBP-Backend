1. Create the following api endpoints

a.
POST /courses/compare
Purpose: Bulk fetch course details for comparison
Body:
{
  "course_ids": ["CSCE120", "MATH151", "PHYS218", "CHEM107"]
}
Response: Array of detailed course objects (same format as /course/{id})

b.
GET /professors
Purpose: List all professors with basic statistics
Parameters:
- search (string): Search by name
- department (string): Filter by department
- limit, skip: Pagination
- min_rating (float): Filter by minimum rating
Response:
[
  {
    "id": "prof123",
    "name": "Dr. John Smith", 
    "overall_rating": 4.2,
    "total_reviews": 156,
    "departments": ["CSCE", "MATH"],
    "courses_taught": ["CSCE120", "CSCE121", "MATH151"]
  }
]

c.
GET /professor/{professor_id}
Purpose: Professor profile with courses and summary stats
Response:
{
  "id": "prof123",
  "name": "Dr. John Smith",
  "overall_rating": 4.2,
  "total_reviews": 156,
  "would_take_again_percent": 84.5,
  "courses": [
    {
      "course_id": "CSCE120",
      "course_name": "Program Design and Concepts", 
      "reviews_count": 45,
      "avg_rating": 4.1
    }
  ],
  "departments": ["CSCE"],
  "recent_reviews": [...] // Last 5 reviews
}

d.
GET /professor/{professor_id}/reviews
Purpose: All reviews for professor across all courses
Parameters:
course_filter (string): Filter by specific course
limit, skip: Pagination
sort_by: date, rating, course
min_rating, max_rating: Filter by rating range
Response: Same format as current course reviews but includes course info for each review

e.
GET /professors/search
Purpose: Advanced professor search
Parameters: name, department, min_rating, courses_taught


2. think about files that will never be used again and list them