#!/usr/bin/env python3
"""
Script to fetch historical GPA data from anex.us for ALL courses in the database
BULK VERSION - Truly concurrent with bulk database operations for maximum speed
"""

import asyncio
import aiohttp
import json
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Dict, Any

# Add src to path for imports
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

from aggiermp.database.base import get_session, GpaDataDB
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text

# Configuration
MAX_CONCURRENT_REQUESTS = 5  # High concurrency for speed
BULK_INSERT_SIZE = 10000  # Insert 5000 records at once
REQUEST_TIMEOUT = 30

def get_all_courses() -> List[Tuple[str, str]]:
    """Get all unique department/course combinations from the database"""
    session = get_session()
    try:
        query = text("""
            SELECT subject_short_name, course_number 
            FROM courses 
            WHERE subject_short_name IS NOT NULL 
            AND course_number IS NOT NULL
            AND course_number ~ '^[0-9]+$'
            GROUP BY subject_short_name, course_number
            ORDER BY subject_short_name, CAST(course_number AS INTEGER)
        """)
        
        result = session.execute(query)
        courses = [(row.subject_short_name, row.course_number) for row in result]
        
        print(f"📊 Found {len(courses)} unique courses in database")
        return courses
        
    except Exception as e:
        print(f"❌ Error getting courses from database: {e}")
        return []
    finally:
        session.close()

async def fetch_course_data(session: aiohttp.ClientSession, dept: str, number: str, 
                           semaphore: asyncio.Semaphore) -> Tuple[str, Dict[str, Any]]:
    """
    Fetch GPA data for a single course with concurrency control
    """
    course_key = f"{dept}_{number}"
    
    async with semaphore:  # Control concurrency
        url = "https://anex.us/grades/getData/"
        
        headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://anex.us",
            "referer": "https://anex.us/grades/",
            "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "x-requested-with": "XMLHttpRequest",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
        }
        
        data = {"dept": dept, "number": number}
        
        try:
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            async with session.post(url, headers=headers, data=data, timeout=timeout) as response:
                if response.status == 200:
                    try:
                        text_response = await response.text()
                        json_data = json.loads(text_response)
                        
                        if 'classes' in json_data and json_data['classes']:
                            return course_key, {'success': True, 'data': json_data, 'course': (dept, number)}
                        else:
                            return course_key, {'success': False, 'error': 'No data available'}
                    except json.JSONDecodeError:
                        return course_key, {'success': False, 'error': 'Invalid JSON response'}
                else:
                    return course_key, {'success': False, 'error': f'HTTP {response.status}'}
        
        except asyncio.TimeoutError:
            return course_key, {'success': False, 'error': 'Request timeout'}
        except Exception as e:
            return course_key, {'success': False, 'error': str(e)}

def extract_class_records(course_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract individual class records from a course's data
    """
    if not course_data.get('success') or 'data' not in course_data:
        return []
    
    records = []
    classes = course_data['data'].get('classes', [])
    
    for class_info in classes:
        try:
            # Calculate total students
            total_students = sum([
                int(class_info.get('A', 0)), int(class_info.get('B', 0)), 
                int(class_info.get('C', 0)), int(class_info.get('D', 0)), 
                int(class_info.get('F', 0)), int(class_info.get('I', 0)), 
                int(class_info.get('S', 0)), int(class_info.get('U', 0)), 
                int(class_info.get('Q', 0)), int(class_info.get('X', 0))
            ])
            
            # Create unique ID
            unique_id = f"{class_info['dept']}_{class_info['number']}_{class_info['section']}_{class_info['year']}_{class_info['semester']}_{class_info['prof']}"
            
            gpa_record = {
                'id': unique_id,
                'dept': class_info['dept'],
                'course_number': class_info['number'],
                'section': class_info['section'],
                'professor': class_info['prof'],
                'year': class_info['year'],
                'semester': class_info['semester'],
                'gpa': float(class_info['gpa']) if class_info['gpa'] != '' else None,
                'grade_a': int(class_info.get('A', 0)),
                'grade_b': int(class_info.get('B', 0)),
                'grade_c': int(class_info.get('C', 0)),
                'grade_d': int(class_info.get('D', 0)),
                'grade_f': int(class_info.get('F', 0)),
                'grade_i': int(class_info.get('I', 0)),
                'grade_s': int(class_info.get('S', 0)),
                'grade_u': int(class_info.get('U', 0)),
                'grade_q': int(class_info.get('Q', 0)),
                'grade_x': int(class_info.get('X', 0)),
                'total_students': total_students,
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            
            records.append(gpa_record)
            
        except Exception as e:
            print(f"⚠️  Error processing class record: {e}")
            continue
    
    return records

def bulk_insert_records(records: List[Dict[str, Any]]) -> int:
    """
    Insert a batch of records into the database using bulk insert
    """
    if not records:
        return 0
    
    session = get_session()
    try:
        stmt = insert(GpaDataDB).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=['id'],
            set_=dict(
                gpa=stmt.excluded.gpa,
                grade_a=stmt.excluded.grade_a,
                grade_b=stmt.excluded.grade_b,
                grade_c=stmt.excluded.grade_c,
                grade_d=stmt.excluded.grade_d,
                grade_f=stmt.excluded.grade_f,
                grade_i=stmt.excluded.grade_i,
                grade_s=stmt.excluded.grade_s,
                grade_u=stmt.excluded.grade_u,
                grade_q=stmt.excluded.grade_q,
                grade_x=stmt.excluded.grade_x,
                total_students=stmt.excluded.total_students,
                updated_at=stmt.excluded.updated_at
            )
        )
        session.execute(stmt)
        session.commit()
        return len(records)
        
    except Exception as e:
        print(f"❌ Error in bulk insert: {e}")
        session.rollback()
        return 0
    finally:
        session.close()

def chunks(lst: List, n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

async def fetch_all_courses_concurrent(courses: List[Tuple[str, str]]) -> List[Dict[str, Any]]:
    """
    Fetch data for all courses concurrently
    """
    print(f"🚀 Starting concurrent fetch of {len(courses)} courses...")
    print(f"⚡ Using {MAX_CONCURRENT_REQUESTS} concurrent connections")
    
    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    # Create connector with appropriate limits
    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT_REQUESTS * 2,
        limit_per_host=MAX_CONCURRENT_REQUESTS
    )
    
    async with aiohttp.ClientSession(connector=connector) as session:
        # Create tasks for all courses
        tasks = [
            fetch_course_data(session, dept, number, semaphore)
            for dept, number in courses
        ]
        
        print(f"📡 Created {len(tasks)} concurrent fetch tasks")
        
        # Execute all tasks concurrently
        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        duration = time.time() - start_time
        
        print(f"⏱️  Fetch completed in {duration:.1f} seconds")
        print(f"⚡ Average: {len(courses) / duration:.1f} requests/second")
        
        # Process results
        successful_responses = []
        failed_count = 0
        
        for result in results:
            if isinstance(result, Exception):
                failed_count += 1
                print(f"❌ Task failed with exception: {result}")
            else:
                course_key, response = result
                if response.get('success'):
                    successful_responses.append(response)
                else:
                    failed_count += 1
        
        print(f"✅ Successful: {len(successful_responses)}")
        print(f"❌ Failed: {failed_count}")
        
        return successful_responses

async def main_async():
    """
    Main async function - fetch all data then bulk insert
    """
    print("🚀 AggieRMP - BULK GPA Data Fetcher")
    print(f"⚡ Max concurrent requests: {MAX_CONCURRENT_REQUESTS}")
    print(f"📦 Bulk insert size: {BULK_INSERT_SIZE} records")
    print("=" * 60)
    
    # Get all courses
    all_courses = get_all_courses()
    if not all_courses:
        print("❌ No courses found!")
        return
    
    # Confirm before starting
    estimated_time = len(all_courses) / MAX_CONCURRENT_REQUESTS + 30  # rough estimate
    print(f"⏱️  Estimated time: {estimated_time/60:.1f} minutes")
    
    response = input(f"\n🔥 Start BULK fetch of {len(all_courses)} courses? (y/N): ")
    if response.lower() != 'y':
        print("Cancelled.")
        return
    
    start_time = datetime.now()
    print(f"\n🔄 Starting bulk operation at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 60)
    
    try:
        # PHASE 1: Fetch all course data concurrently
        print("\n📡 PHASE 1: Concurrent Data Fetching")
        successful_responses = await fetch_all_courses_concurrent(all_courses)
        
        if not successful_responses:
            print("❌ No successful responses received!")
            return
        
        # PHASE 2: Extract all class records
        print(f"\n🔄 PHASE 2: Processing {len(successful_responses)} successful responses")
        all_records = []
        
        for response in successful_responses:
            records = extract_class_records(response)
            all_records.extend(records)
        
        print(f"📝 Extracted {len(all_records)} total class records")
        
        if not all_records:
            print("❌ No records to insert!")
            return
        
        # PHASE 3: Bulk insert in chunks
        print(f"\n💾 PHASE 3: Bulk Database Insert ({BULK_INSERT_SIZE} records per batch)")
        
        total_inserted = 0
        chunk_num = 0
        
        for chunk in chunks(all_records, BULK_INSERT_SIZE):
            chunk_num += 1
            chunk_size = len(chunk)
            
            print(f"📦 Inserting chunk {chunk_num} ({chunk_size} records)...", end=" ")
            
            inserted = bulk_insert_records(chunk)
            total_inserted += inserted
            
            if inserted == chunk_size:
                print("✅")
            else:
                print(f"⚠️  {inserted}/{chunk_size} inserted")
        
        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\n" + "=" * 60)
        print("🎉 BULK OPERATION COMPLETE!")
        print("=" * 60)
        print(f"⏱️  Total duration: {duration}")
        print(f"📊 Courses processed: {len(successful_responses)}")
        print(f"📝 Records inserted: {total_inserted}")
        print(f"⚡ Speed: {len(all_courses) / duration.total_seconds():.2f} courses/second")
        
        if total_inserted > 0:
            print(f"\n🚀 SUCCESS! {total_inserted} GPA records added to database!")
            print("🔄 Your API now has comprehensive GPA data!")
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Operation interrupted by user")
    except Exception as e:
        print(f"\n❌ Error during bulk operation: {e}")

def main():
    """Main function wrapper"""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\n⏹️  Interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main() 