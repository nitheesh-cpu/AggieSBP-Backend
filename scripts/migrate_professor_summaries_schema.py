"""
Migration script to convert professor_summaries_new from JSON to separate rows.

This script:
1. Drops the old table (backup first if needed)
2. Creates the new table with separate rows schema
3. Migrates existing data if any (would need to be adapted based on current data structure)
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from aggiermp.database.base import get_session, create_db_engine, Base
from sqlalchemy import text


def migrate_schema():
    """Migrate professor_summaries_new to separate rows schema"""
    session = get_session()
    engine = create_db_engine()
    
    try:
        print("Checking current table structure...")
        
        # Check if table exists
        result = session.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'professor_summaries_new'
            );
        """))
        table_exists = result.scalar()
        
        if table_exists:
            # Check current schema
            result = session.execute(text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'professor_summaries_new'
                ORDER BY ordinal_position;
            """))
            columns = result.fetchall()
            
            print("\nCurrent columns:")
            for col_name, col_type in columns:
                print(f"  {col_name}: {col_type}")
            
            # Check if it's the old schema (has course_summaries JSON column)
            has_json_column = any(col[0] == 'course_summaries' for col in columns)
            
            if has_json_column:
                print("\n⚠️  WARNING: Table has old schema (JSON column).")
                print("   Migration will DROP and recreate the table.")
                print("   All existing data will be LOST!")
                response = input("\nContinue? (yes/no): ")
                if response.lower() != 'yes':
                    print("Migration cancelled.")
                    return
                
                print("\nDropping old table...")
                session.execute(text("DROP TABLE IF EXISTS professor_summaries_new CASCADE;"))
                session.commit()
        
        print("\nCreating new table with separate rows schema...")
        Base.metadata.create_all(engine, tables=[Base.metadata.tables['professor_summaries_new']])
        
        # Create indexes for better query performance
        print("Creating indexes...")
        session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_professor_summaries_professor_id 
            ON professor_summaries_new(professor_id);
        """))
        session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_professor_summaries_course_code 
            ON professor_summaries_new(course_code) WHERE course_code IS NOT NULL;
        """))
        session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_professor_summaries_professor_course 
            ON professor_summaries_new(professor_id, course_code);
        """))
        
        session.commit()
        
        print("\n✅ Migration complete!")
        print("\nNew schema:")
        print("  - One row per course (course_code IS NOT NULL)")
        print("  - One row per professor for overall summary (course_code IS NULL)")
        print("  - Indexes on professor_id, course_code, and (professor_id, course_code)")
        
    except Exception as e:
        print(f"\n❌ Error during migration: {e}")
        import traceback
        traceback.print_exc()
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    migrate_schema()

