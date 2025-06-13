#!/usr/bin/env python3
"""
Main entry point for the AggieRMP application
"""

import os
import asyncio
from concurrent.futures import ThreadPoolExecutor
from collectors.rmp_review_collector import RMPReviewCollector
from collectors.utils import update_professors
from database.base import ProfessorDB, ReviewDB, get_session, upsert_universities, upsert_professors, upsert_reviews


async def collect_reviews_for_professor(collector, professor, semaphore):
    """Collect reviews for a single professor with semaphore control"""
    async with semaphore:
        # Run the synchronous review collection in a thread pool
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            reviews = await loop.run_in_executor(
                executor, 
                collector.get_all_reviews, 
                professor.id
            )
        
        if reviews:
            print(f"✅ {professor.first_name} {professor.last_name}: {len(reviews)} reviews")
            return reviews
        else:
            print(f"⚠️  {professor.first_name} {professor.last_name}: No reviews found")
            return []


async def collect_all_reviews_async(collector, professors, session, max_concurrent=15, batch_size=10000):
    """Collect reviews for all professors concurrently with batched upserts"""
    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(max_concurrent)
    
    print(f"🚀 Starting concurrent review collection with max {max_concurrent} concurrent requests...")
    print(f"📦 Will batch upsert every {batch_size:,} reviews")
    
    # Create tasks for all professors
    tasks = [
        collect_reviews_for_professor(collector, prof, semaphore)
        for prof in professors
    ]
    
    # Execute all tasks concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Process results and batch upserts
    all_reviews = []
    total_reviews = 0
    successful_profs = 0
    failed_profs = 0
    batch_count = 1
    
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"❌ Error for {professors[i].first_name} {professors[i].last_name}: {result}")
            failed_profs += 1
        else:
            reviews = result
            if reviews:
                all_reviews.extend(reviews)
                total_reviews += len(reviews)
                
                # Batch upsert when we hit the batch size
                if len(all_reviews) >= batch_size:
                    print(f"📦 Batch {batch_count}: Upserting {len(all_reviews):,} reviews...")
                    try:
                        upsert_reviews(session, all_reviews)
                        print(f"✅ Batch {batch_count}: Successfully upserted {len(all_reviews):,} reviews")
                    except Exception as e:
                        print(f"❌ Batch {batch_count}: Error upserting reviews: {e}")
                    
                    all_reviews = []  # Clear the batch
                    batch_count += 1
            
            successful_profs += 1
    
    # Handle final batch (if any remaining reviews)
    if all_reviews:
        print(f"📦 Final batch {batch_count}: Upserting {len(all_reviews):,} reviews...")
        try:
            upsert_reviews(session, all_reviews)
            print(f"✅ Final batch {batch_count}: Successfully upserted {len(all_reviews):,} reviews")
        except Exception as e:
            print(f"❌ Final batch {batch_count}: Error upserting reviews: {e}")
    
    print(f"\n📊 Collection Summary:")
    print(f"   ✅ Successful: {successful_profs} professors")
    print(f"   ❌ Failed: {failed_profs} professors")
    print(f"   📝 Total reviews collected: {total_reviews:,}")
    print(f"   📦 Total batches processed: {batch_count}")
    
    return total_reviews

async def get_missing_professors(session):
    """Get professors that have no reviews"""
    professors_with_reviews = session.query(ReviewDB.professor_id).distinct().all()
    professors_with_reviews = [prof[0] for prof in professors_with_reviews]
    professors = session.query(ProfessorDB).all()
    
    professors_without_reviews = [prof for prof in professors if prof.id not in professors_with_reviews]

    session = get_session()
    print(f"✅ Found {len(professors_without_reviews)} professors with no reviews")
    collector = RMPReviewCollector()
    total_reviews = await collect_all_reviews_async(
            collector, 
            professors_without_reviews, 
            session, 
            max_concurrent=15,  # Adjust this based on your needs and RMP's rate limits
            batch_size=10000   # Upsert every 10,000 reviews
        )
    return total_reviews



async def main():
    """Main function to test the RMP collector and database setup"""
    print("🚀 Starting AggieRMP application...")

    
    try:
        session = get_session()
        collector = RMPReviewCollector()
        university_id = collector.get_university_id("college station").id
        updated_professors = await update_professors(session, university_id)
        print(f"✅ Updated {updated_professors} professors")

    except Exception as e:
        print(f"❌ Error occurred: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())





