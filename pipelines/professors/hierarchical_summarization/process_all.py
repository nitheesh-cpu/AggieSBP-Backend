"""
Script to process all professors through the hierarchical summarization pipeline.

This script:
1. Fetches all professors from the database
2. Processes their reviews through the hierarchical summarization pipeline
3. Upserts the results to professor_summaries_new table
"""

import os
import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent.parent.parent / "src"
sys.path.insert(0, str(src_path))

from aggiermp.database.base import ProfessorDB, ReviewDB, get_session
from pipelines.professors.hierarchical_summarization import HierarchicalSummarizationPipeline
from pipelines.professors.upsert import (
    upsert_professor_summary,
    process_all_professors
)


def main():
    """Process all professors through the hierarchical summarization pipeline"""
    
    # Set environment variables for performance
    os.environ["OMP_NUM_THREADS"] = "4"
    os.environ["TOKENIZERS_PARALLELISM"] = "true"
    
    print("="*80)
    print("HIERARCHICAL SUMMARIZATION - PROCESS ALL PROFESSORS")
    print("="*80)
    
    # Get database session
    session = get_session()
    
    try:
        # Initialize pipeline with database session for cross-listings
        print("\nInitializing pipeline...")
        pipeline = HierarchicalSummarizationPipeline(session=session)
        
        # Get all professors
        print("\nFetching all professors from database...")
        professors = session.query(ProfessorDB).all()
        professor_ids = [prof.id for prof in professors]
        
        print(f"Found {len(professor_ids)} professors to process")
        
        # Process all professors
        results = process_all_professors(
            professor_ids=professor_ids,
            pipeline=pipeline,
            session=session
        )
        
        # Print results
        print("\n" + "="*80)
        print("PROCESSING COMPLETE")
        print("="*80)
        print(f"Successfully processed: {results['processed']}")
        print(f"Failed: {results['failed']}")
        
        if results['errors']:
            print(f"\nErrors ({len(results['errors'])}):")
            for error in results['errors'][:10]:  # Show first 10 errors
                print(f"  - {error}")
            if len(results['errors']) > 10:
                print(f"  ... and {len(results['errors']) - 10} more errors")
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        session.close()


if __name__ == "__main__":
    main()

