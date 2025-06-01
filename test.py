from database.summary import ReviewSummarizer

summarizer = ReviewSummarizer()
    
try:
    # Process a single professor
    # result = summarizer.process_professor("VGVhY2hlci02MDkxMDE=")
    # print(result)
    
    # Or process all professors
    results = summarizer.process_all_professors()
    
    # Print summary statistics
    successful = sum(1 for r in results if not r['error'])
    print(f"\nProcessing complete: {successful}/{len(results)} professors processed successfully")
    
    # Example: Retrieve a summary
    # summary = summarizer.get_summary("professor_id_here")
    # if summary:
    #     print(f"Summary: {summary.summary_text}")
    #     print(f"Common tags: {summary.common_tags}")
    
finally:
    summarizer.close()