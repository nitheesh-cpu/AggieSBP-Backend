#!/usr/bin/env python3
"""
Script to load GPA data from anex.us into the database
"""

import json
import sys
import uuid
from pathlib import Path
from datetime import datetime

# Add src to path for imports
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

from aggiermp.database.base import get_session, GpaDataDB
from sqlalchemy.dialects.postgresql import insert

def load_gpa_data_from_file(file_path: str):
    """Load GPA data from JSON file into database"""
    session = get_session()
    
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        insert_count = 0
        
        for course_key, course_data in data.items():
            if 'classes' not in course_data:
                print(f"Skipping {course_key} - no classes data")
                continue
                
            print(f"Processing {course_key}...")
            
            insert_courses = []
            
            for class_info in course_data['classes']:
                try:
                    # Calculate total students
                    total_students = sum([
                        int(class_info.get('A', 0)),
                        int(class_info.get('B', 0)),
                        int(class_info.get('C', 0)),
                        int(class_info.get('D', 0)),
                        int(class_info.get('F', 0)),
                        int(class_info.get('I', 0)),
                        int(class_info.get('S', 0)),
                        int(class_info.get('U', 0)),
                        int(class_info.get('Q', 0)),
                        int(class_info.get('X', 0))
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
                    
                    insert_courses.append(gpa_record)
                    insert_count += 1
                        
                except Exception as e:
                    print(f"Error processing record: {e}")
                    print(f"Record: {class_info}")
                    continue
            
            if insert_courses:
                remaining = session.execute(insert(GpaDataDB).values(insert_courses))
                remaining = remaining.on_conflict_do_update(
                    index_elements=['id'],
                    set_=dict(
                        gpa=remaining.excluded.gpa,
                        grade_a=remaining.excluded.grade_a,
                        grade_b=remaining.excluded.grade_b,
                        grade_c=remaining.excluded.grade_c,
                        grade_d=remaining.excluded.grade_d,
                        grade_f=remaining.excluded.grade_f,
                        grade_i=remaining.excluded.grade_i,
                        grade_s=remaining.excluded.grade_s,
                        grade_u=remaining.excluded.grade_u,
                        grade_q=remaining.excluded.grade_q,
                        grade_x=remaining.excluded.grade_x,
                        total_students=remaining.excluded.total_students,
                        updated_at=remaining.excluded.updated_at
                    )
                )
                session.execute(remaining)
                
                
        session.commit()
        print(f"Successfully loaded {insert_count} GPA records")
        
    except Exception as e:
        print(f"Error loading GPA data: {e}")
        session.rollback()
    finally:
        session.close()

def main():
    """Main function"""
    print("GPA Data Loader")
    print("=" * 40)
    
    # Default file path
    gpa_file = Path(__file__).parent.parent / "data" / "raw" / "gpa_data_test.json"
    
    if len(sys.argv) > 1:
        gpa_file = Path(sys.argv[1])
    
    if not gpa_file.exists():
        print(f"GPA data file not found: {gpa_file}")
        print("Run fetch_gpa_data.py first to collect data")
        return
    
    print(f"Loading GPA data from: {gpa_file}")
    load_gpa_data_from_file(str(gpa_file))

if __name__ == "__main__":
    main() 