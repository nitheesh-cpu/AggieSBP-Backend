#!/usr/bin/env python3
"""
Update common section attributes with proper names
Based on typical Texas A&M attribute codes and meanings
"""

import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.aggiermp.database.base import get_session
from sqlalchemy import text

# Common Texas A&M section attribute mappings
COMMON_ATTRIBUTES = {
    # Core Curriculum attributes
    "KCOM": "Core Curriculum - Communication",
    "KMAT": "Core Curriculum - Mathematics", 
    "KNAT": "Core Curriculum - Natural Sciences",
    "KSOC": "Core Curriculum - Social Sciences",
    "KHIS": "Core Curriculum - History",
    "KGOV": "Core Curriculum - Government",
    "KENG": "Core Curriculum - Engineering",
    "KART": "Core Curriculum - Arts",
    "KLIT": "Core Curriculum - Literature",
    "KPHI": "Core Curriculum - Philosophy",
    
    # Special program attributes
    "HONR": "Honors",
    "ACST": "Academic Studies",
    "DIST": "Distance Learning",
    "OPEN": "Open Enrollment",
    "INAB": "International Study",
    "ZLSA": "Study Abroad",
    
    # Campus/Location attributes
    "GALV": "Galveston Campus",
    "QATAR": "Qatar Campus", 
    "AHOU": "Houston Center",
    "ADAL": "Dallas Center",
    "AMCA": "McAllen Center",
    
    # Special sections
    "LABS": "Laboratory Section",
    "RECIT": "Recitation Section",
    "STUD": "Study Group",
    "WRIT": "Writing Intensive",
    "SERV": "Service Learning",
    "COOP": "Cooperative Education",
    "INTERN": "Internship",
    
    # Time/Format attributes  
    "EVEN": "Evening Section",
    "WEEK": "Weekend Section",
    "ONLN": "Online Section",
    "HYBR": "Hybrid Section",
    "SYNC": "Synchronous Online",
    "ASYN": "Asynchronous Online",
    
    # Engineering specific
    "ENGR": "Engineering",
    "AACD": "Alamo Community College Dual Credit",
    "AAUC": "Accelerated Undergraduate Certificate",
    
    # Other common codes
    "TECH": "Technology Enhanced",
    "CLIN": "Clinical Experience", 
    "FLDW": "Field Work",
    "PRAC": "Practicum",
    "SEMI": "Seminar",
    "RSCH": "Research",
    "THES": "Thesis",
    "DISS": "Dissertation",
    "PROJ": "Project Course",
    "PROB": "Problem Course",
    "INDE": "Independent Study",
}

def update_section_attributes():
    """Update section attributes with common attribute names"""
    session = get_session()
    try:
        print("Updating section attributes with common Texas A&M attribute names...")
        print(f"Will update {len(COMMON_ATTRIBUTES)} attribute types")
        
        updated_count = 0
        
        for attr_id, attr_name in COMMON_ATTRIBUTES.items():
            # Format as "attribute name - attribute code"
            formatted_title = f"{attr_name} - {attr_id}"
            
            # Update all records with this attribute_id
            update_query = text("""
                UPDATE section_attributes 
                SET attribute_title = :attr_title,
                    updated_at = NOW()
                WHERE attribute_id = :attr_id
            """)
            
            result = session.execute(update_query, {
                'attr_title': formatted_title,
                'attr_id': attr_id
            })
            
            rows_updated = result.rowcount
            if rows_updated > 0:
                updated_count += rows_updated
                print(f"  Updated {rows_updated} records for {attr_id}: {formatted_title}")
        
        session.commit()
        print(f"\nUpdate complete: {updated_count} total records updated")
        
    except Exception as e:
        session.rollback()
        print(f"Error updating attributes: {e}")
        raise
    finally:
        session.close()

def verify_updates():
    """Verify the attribute updates"""
    session = get_session()
    try:
        print(f"\nVerifying updates...")
        
        # Check sample updated attributes
        result = session.execute(text("""
            SELECT DISTINCT attribute_id, attribute_title 
            FROM section_attributes 
            WHERE attribute_title LIKE '% - %'
            ORDER BY attribute_id 
            LIMIT 15
        """))
        
        print("Sample updated attributes:")
        for row in result:
            print(f"  {row.attribute_id}: {row.attribute_title}")
        
        # Count coverage
        count_result = session.execute(text("""
            SELECT 
                COUNT(DISTINCT attribute_id) as total_unique_attrs,
                COUNT(DISTINCT CASE WHEN attribute_title LIKE '% - %' THEN attribute_id END) as updated_attrs,
                COUNT(*) as total_records,
                COUNT(CASE WHEN attribute_title LIKE '% - %' THEN 1 END) as updated_records
            FROM section_attributes
        """))
        
        counts = count_result.fetchone()
        print(f"\nCoverage Summary:")
        print(f"  Unique attribute codes: {counts.total_unique_attrs}")
        print(f"  Updated attribute codes: {counts.updated_attrs}")
        print(f"  Total section records: {counts.total_records}")
        print(f"  Updated section records: {counts.updated_records}")
        
        if counts.total_unique_attrs > 0:
            attr_coverage = (counts.updated_attrs / counts.total_unique_attrs) * 100
            print(f"  Attribute coverage: {attr_coverage:.1f}%")
            
        if counts.total_records > 0:
            record_coverage = (counts.updated_records / counts.total_records) * 100
            print(f"  Record coverage: {record_coverage:.1f}%")
        
    finally:
        session.close()

def main():
    """Main function"""
    print("Texas A&M Section Attributes Updater")
    print("=" * 50)
    
    # Update with common attribute names
    update_section_attributes()
    
    # Verify updates
    verify_updates()
    
    print("\nSection attributes update completed!")
    print("Note: Some attributes may still show as just codes if they're not in the common mapping.")

if __name__ == "__main__":
    main() 