from datetime import datetime
import json
import re
from playwright.sync_api import Playwright, sync_playwright, expect
from sqlalchemy import insert, select, update

from database.base import CourseDB, DepartmentDB, get_session


def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    
    # Set up response interception to catch JSON responses
    api_data = None
    
    def handle_response(response):
        nonlocal api_data
        if "subjects" in response.url and response.status == 200:
            try:
                if "application/json" in response.headers.get("content-type", ""):
                    print(f"Found JSON response from: {response.url}")
                    api_data = response
            except Exception as e:
                print(f"Error checking response: {e}")
    
    page.on("response", handle_response)
    
    # Navigate to login page
    page.goto("https://tamu.collegescheduler.com/api/terms/Fall%202025%20-%20College%20Station/subjects")
    page.get_by_role("textbox", name="NetID@tamu.edu").click()
    page.get_by_role("textbox", name="NetID@tamu.edu").fill("nitheeshk@tamu.edu")
    page.get_by_role("button", name="Next").click()
    page.locator("#i0118").fill("Narutokagebunshin!")
    page.get_by_role("button", name="Sign in").click()
    page.get_by_role("button", name="Yes, this is my device").click()
    page.get_by_role("button", name="Yes").click()
    
    # Navigate to main application first
    print("Navigating to main application...")
    page.goto("https://tamu.collegescheduler.com/", wait_until="networkidle")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(3000)
    
    # Now try to access the API endpoint directly
    print("Accessing API endpoint...")
    response = page.goto("https://tamu.collegescheduler.com/api/terms/Fall%202025%20-%20College%20Station/subjects", wait_until="networkidle")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(3000)
    
    print(f"Response status: {response.status}")
    print(f"Response headers: {response.headers}")
    
    # Try to get JSON from intercepted response
    if api_data:
        try:
            import json
            departments = api_data.json()
            print(f"Successfully got {len(departments)} departments from intercepted response")
            
            # Save to file
            with open('scraped_departments_final.json', 'w', encoding='utf-8') as f:
                json.dump(departments, f, indent=2, ensure_ascii=False)
            
            print("\nFirst 10 departments:")
            for i, dept in enumerate(departments[:10]):
                print(f"{i+1}. {dept}")
            return
                
        except Exception as e:
            print(f"Failed to parse intercepted JSON: {e}")
    
    # Fallback: try to parse page content
    content = page.content()
    print("Page content received, length:", len(content))
    
    content_stripped = content.strip()
    if content_stripped.startswith('[') or content_stripped.startswith('{'):
        try:
            import json
            departments = json.loads(content_stripped)
            print(f"Successfully parsed {len(departments)} departments from page content")
            
            # Save to file
            with open('scraped_departments_final.json', 'w', encoding='utf-8') as f:
                json.dump(departments, f, indent=2, ensure_ascii=False)
            
            print("\nFirst 10 departments:")
            for i, dept in enumerate(departments[:10]):
                print(f"{i+1}. {dept}")
                
            upsert_departments(departments)
            
            page.wait_for_timeout(10000)        
                
        except json.JSONDecodeError as e:
            print(f"Failed to parse as JSON: {e}")
            print("Raw content preview:")
            print(content[:1000])
    else:
        print("Content doesn't appear to be JSON")
        print("Raw content preview:")
        print(content[:500])
    

    # ---------------------
    context.close()
    browser.close()


def upsert_departments(departments: list[dict]):
    session = get_session()
    departments_db = session.execute(select(DepartmentDB)).scalars().all()
    
    updated_departments = []
    new_departments = []
    
    for department in departments:
        department_db = next(
            (
                x
                for x in departments_db
                if x.id == department['id']
            ),
            None
        )
        if department_db:
            updated_department = {
                'id': department['id'],
                'short_name': department['short'],
                'long_name': department['long'],
                'title': department['title'],
                'updated_at': datetime.now(),
                'created_at': department_db.created_at
            }
            
            updated_departments.append(updated_department)
        else:
            new_department = {
                'id': department['id'],
                'short_name': department['short'],
                'long_name': department['long'],
                'title': department['title'],
                'created_at': datetime.now(),
                'updated_at': datetime.now(),
            }
            new_departments.append(new_department)
    print(f"Updating {len(updated_departments)} departments")
    print(f"Adding {len(new_departments)} departments")
  
    if len(updated_departments) > 0:
        session.execute(update(DepartmentDB), updated_departments)
    
    if len(new_departments) > 0:
        session.execute(insert(DepartmentDB), new_departments)
    
    session.commit()
    session.close()
    
def insert_courses(courses: list[dict]):
    session = get_session()
    courses_db = session.execute(select(CourseDB)).scalars().all()
    
    updated_courses = []
    new_courses = []
    
    for course in courses:
        course_db = next(
            (
                x
                for x in courses_db
                if x.id == course.id
            ),
            None
        )
        if course_db:
            updated_course = {
                'id': course['id'],
                'subject_long_name': course['subjectLong'],
                'subject_short_name': course['subjectShort'],
                'subject_id': course['subjectId'],
                'course_number': course['number'],
                'course_topic': course['topic'],
                'course_display_title': course['displayTitle'],
                'course_title': course['title'],
                'course_title_long': course['titleLong'],
                'description': course['description'],
                'has_topics': course['hasTopics'],
                'has_corequisites': course['corequisites'],
                'has_prerequisites': course['prerequisites'],
                'has_restrictions': course['hasRestrictions'],
                'updated_at': datetime.now(),
                'created_at': course_db.created_at
            }
            updated_courses.append(updated_course)
        else:
            new_course = {
                'id': course['id'],
                'subject_long_name': course['subjectLong'],
                'subject_short_name': course['subjectShort'],
                'subject_id': course['subjectId'],
                'course_number': course['number'],
                'course_topic': course['topic'],
                'course_display_title': course['displayTitle'],
                'course_title': course['title'],
                'course_title_long': course['titleLong'],
                'description': course['description'],
                'has_topics': course['hasTopics'],
                'has_corequisites': course['corequisites'],
                'has_prerequisites': course['prerequisites'],
                'has_restrictions': course['hasRestrictions'],
                'created_at': datetime.now(),
            }
            new_courses.append(new_course)
            
    print(f"Updating {len(updated_courses)} courses")
    print(f"Adding {len(new_courses)} courses")
    
    if len(updated_courses) > 0:
        session.execute(update(CourseDB), updated_courses)
    
    if len(new_courses) > 0:
        session.execute(insert(CourseDB), new_courses)
    
    session.commit()
        
    
    
    
if __name__ == "__main__":
    # with sync_playwright() as playwright:
    #     run(playwright)
    
    # with open('scraped_departments_final.json', 'r', encoding='utf-8') as f:
    #     departments = json.load(f)
        
    # upsert_departments(departments)
    
    with open('scraped_courses_final.json', 'r', encoding='utf-8') as f:
        courses = json.load(f)
        
    insert_courses(courses)
    