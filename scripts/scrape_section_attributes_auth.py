from datetime import datetime
import json
import time
from playwright.sync_api import Playwright, sync_playwright
from sqlalchemy.dialects.postgresql import insert
from aggiermp.database.base import SectionAttributeDB, get_session

SECTION_ATTRIBS = [
    "AACD", "AAUC", "FDIA", "ABRM", "ABRY", "ACAN", "ACCH", "ACST", "COOP", "KHIS", "KCOM", "KCRA", "KPLF", "KLPC", "KLPS", "KPLL", "KMTH", "KSOC", "KHTX", "ADAL", "DIST", "AECC", "FYEX", "AFTW", "AGAL", "HONR", "AHEM", "AHOU", "AHWH", "AHCC", "AHMD", "INAB", "HKAE", "HKCO", "HKMB", "AKIN", "ZLSA", "AMCA", "AMID", "OPEN", "ARDR", "ASAN", "SABR", "SAOF", "ATEM", "TXHS", "KUCD", "KICD", "UCRT", "UWRT", "WAIT", "AWDC"
]

API_URL_TEMPLATE = "https://tamu.collegescheduler.com/api/terms/Fall%202025%20-%20College%20Station/sectionattributevalues/{}/courses"

BATCH_SIZE = 1000


def upsert_section_attributes(records):
    if not records:
        return 0
    session = get_session()
    try:
        stmt = insert(SectionAttributeDB).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=['id'])
        session.execute(stmt)
        session.commit()
        return len(records)
    except Exception as e:
        print(f"❌ DB error: {e}")
        session.rollback()
        return 0
    finally:
        session.close()

def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()

    # Login flow (customize as needed for your SSO)
    print("Logging in...")
    page.goto("https://tamu.collegescheduler.com/api/terms/Fall%202025%20-%20College%20Station/subjects")
    page.get_by_role("textbox", name="NetID@tamu.edu").click()
    page.get_by_role("textbox", name="NetID@tamu.edu").fill("nitheeshk@tamu.edu")
    page.get_by_role("button", name="Next").click()
    page.locator("#i0118").fill("Narutokagebunshin!")
    page.get_by_role("button", name="Sign in").click()
    page.get_by_role("button", name="Yes, this is my device").click()
    page.get_by_role("button", name="Yes").click()
    page.goto("https://tamu.collegescheduler.com/", wait_until="networkidle")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(3000)

    all_records = []
    
    for attr_id in SECTION_ATTRIBS:
        url = API_URL_TEMPLATE.format(attr_id)
        print(f"🔎 Fetching attribute {attr_id}...")
        
        try:
            # Navigate to the API URL directly
            response = page.goto(url, wait_until="networkidle")
            
            if response and response.status == 200:
                # Get the page content (should be JSON)
                content = page.content()
                
                # Extract JSON from the page content
                # The page should display raw JSON
                if content and "application/json" in response.headers.get("content-type", ""):
                    # Parse the JSON content
                    json_start = content.find('[')
                    json_end = content.rfind(']') + 1
                    
                    if json_start != -1 and json_end > json_start:
                        json_str = content[json_start:json_end]
                        courses = json.loads(json_str)
                        
                        print(f"✅ Found {len(courses)} courses for attribute {attr_id}")
                        
                        for course in courses:
                            try:
                                # Extract subject and number from the course data
                                subject = course.get('subjectId', course.get('subjectShort', ''))
                                number = course.get('number', '')
                                
                                # Create a record for this course having this attribute
                                record_id = f"{subject}_{number}_Fall_2025_{attr_id}"
                                record = {
                                    'id': record_id,
                                    'dept': subject,
                                    'course_number': number,
                                    'section': 'ALL',  # This API returns courses, not specific sections
                                    'year': 2025,
                                    'semester': 'Fall',
                                    'attribute_id': attr_id,
                                    'attribute_title': course.get('title', ''),
                                    'attribute_value': course.get('displayTitle', ''),
                                    'created_at': datetime.now(),
                                    'updated_at': datetime.now()
                                }
                                all_records.append(record)
                            except Exception as e:
                                print(f"⚠️  Error processing course for {attr_id}: {e}")
                                continue
                    else:
                        print(f"❌ Could not extract JSON from response for {attr_id}")
                else:
                    print(f"❌ Response is not JSON for {attr_id}")
            else:
                print(f"❌ Failed to fetch {attr_id}: status {response.status if response else 'None'}")
                
        except Exception as e:
            print(f"❌ Error fetching {attr_id}: {e}")
            
        # Small delay between requests
        page.wait_for_timeout(1000)
    
    print(f"📝 Total records to insert: {len(all_records)}")
    
    # Insert records in batches
    total_inserted = 0
    for i in range(0, len(all_records), BATCH_SIZE):
        batch = all_records[i:i+BATCH_SIZE]
        inserted = upsert_section_attributes(batch)
        print(f"💾 Inserted batch {i//BATCH_SIZE+1}: {inserted} records")
        total_inserted += inserted
    
    print(f"🎉 Done! Inserted {total_inserted} section attribute records.")
    
    context.close()
    browser.close()

if __name__ == "__main__":
    with sync_playwright() as playwright:
        run(playwright) 