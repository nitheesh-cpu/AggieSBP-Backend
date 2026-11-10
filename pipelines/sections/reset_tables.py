import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).resolve().parent.parent.parent
src_dir = project_root / "src"

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

from dotenv import load_dotenv
env_path = project_root / ".env"
load_dotenv(dotenv_path=env_path)

from aggiermp.database.base import (
    create_db_engine,
    SectionDB,
    SectionInstructorDB,
    SectionMeetingDB,
    SectionAttributeDetailedDB,
    SectionPrereqDB,
    SectionRestrictionDB,
    SectionBookstoreLinkDB,
    TermDB,
)

def reset_section_tables():
    print("Connecting to database...")
    engine = create_db_engine()
    
    tables = [
        SectionAttributeDetailedDB.__table__,
        SectionPrereqDB.__table__,
        SectionRestrictionDB.__table__,
        SectionBookstoreLinkDB.__table__,
        SectionMeetingDB.__table__,
        SectionInstructorDB.__table__,
        SectionDB.__table__,
        TermDB.__table__,
    ]
    
    print("Dropping old section tables...")
    SectionDB.metadata.drop_all(engine, tables=tables)
    
    print("Recreating section tables with new schema...")
    SectionDB.metadata.create_all(engine, tables=tables)
    
    print("Done! You can now run the upsert script.")

if __name__ == "__main__":
    reset_section_tables()
