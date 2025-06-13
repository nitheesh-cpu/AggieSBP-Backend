#!/usr/bin/env python3
"""
Fetch section attributes from Texas A&M API using Playwright for authentication
Updates section_attributes table with proper attribute titles
"""

import asyncio
import json
import sys
from pathlib import Path
from playwright.async_api import async_playwright
import requests

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.aggiermp.database.base import get_session
from sqlalchemy import text

class TAMUAttributesFetcher:
    def __init__(self):
        self.base_url = "https://tamu.collegescheduler.com"
        self.api_url = f"{self.base_url}/api/terms/Fall%202025%20-%20College%20Station/sectionattributes"
        self.cookies = None
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

    async def authenticate_and_fetch(self):
        """Use Playwright to authenticate and fetch section attributes"""
        async with async_playwright() as p:
            # Launch browser
            browser = await p.chromium.launch(headless=False)  # Set to False for debugging
            context = await browser.new_context(
                user_agent=self.headers['User-Agent']
            )
            page = await context.new_page()

            try:
                print("Navigating to Texas A&M Course Scheduler...")
                await page.goto(self.base_url)
                await page.get_by_role("textbox", name="NetID@tamu.edu").click()
                await page.get_by_role("textbox", name="NetID@tamu.edu").fill("nitheeshk@tamu.edu")
                await page.get_by_role("button", name="Next").click()
                await page.locator("#i0118").fill("Narutokagebunshin!")
                await page.get_by_role("button", name="Sign in").click()
                await page.get_by_role("button", name="Yes, this is my device").click()
                await page.get_by_role("button", name="Yes").click()
                # Wait for page to load
                await page.wait_for_load_state('networkidle')
                await asyncio.sleep(3)

                print("Current page title:", await page.title())
                print("Current URL:", page.url)

                # Try different approaches to get to the API
                print("Attempting to navigate to course search/browse...")
                
                # Method 1: Try to find and select Fall 2025 term
                try:
                    # Look for dropdown or select elements
                    term_selector = page.locator('select, .dropdown, [data-testid*="term"], [data-testid*="semester"]').first
                    if await term_selector.is_visible(timeout=5000):
                        print("Found term selector, attempting to select Fall 2025...")
                        await term_selector.click()
                        await asyncio.sleep(1)
                        
                        # Try to find Fall 2025 option
                        fall_option = page.locator('text*="Fall 2025"').first
                        if await fall_option.is_visible(timeout=3000):
                            await fall_option.click()
                            print("Selected Fall 2025")
                            await page.wait_for_load_state('networkidle')
                except Exception as e:
                    print(f"Term selection failed: {e}")

                # Method 2: Try to navigate to course search
                try:
                    # Look for navigation links or buttons
                    nav_elements = await page.locator('a, button').all()
                    for element in nav_elements[:20]:  # Check first 20 elements
                        text = await element.inner_text()
                        if any(keyword in text.lower() for keyword in ['search', 'courses', 'browse', 'catalog']):
                            print(f"Found navigation element: {text}")
                            await element.click()
                            await page.wait_for_load_state('networkidle')
                            await asyncio.sleep(2)
                            break
                except Exception as e:
                    print(f"Navigation failed: {e}")

                # Method 3: Try to directly access the API URL in browser
                print("Attempting direct navigation to API URL...")
                try:
                    await page.goto(self.api_url)
                    await page.wait_for_load_state('networkidle')
                    
                    # Check if we got JSON data
                    content = await page.content()
                    if content.strip().startswith('[') or content.strip().startswith('{'):
                        print("Found JSON content on API page")
                        try:
                            api_data = json.loads(content)
                            print(f"Successfully parsed {len(api_data)} attributes from direct API access")
                            return api_data
                        except json.JSONDecodeError as e:
                            print(f"Failed to parse JSON: {e}")
                    else:
                        print("API page did not return JSON")
                        print(f"Content preview: {content[:200]}")
                except Exception as e:
                    print(f"Direct API access failed: {e}")

                # Method 4: Try making fetch request with different approaches
                print("Trying fetch API with CORS handling...")
                
                # First, go back to main site to establish session
                await page.goto(self.base_url)
                await page.wait_for_load_state('networkidle')
                await asyncio.sleep(2)
                
                # Try fetch with different configurations
                fetch_attempts = [
                    # Attempt 1: Basic fetch
                    f"""
                    fetch('{self.api_url}')
                        .then(response => {{
                            console.log('Response status:', response.status);
                            console.log('Response headers:', Object.fromEntries(response.headers.entries()));
                            return response.text();
                        }})
                        .then(text => {{
                            console.log('Response text length:', text.length);
                            try {{
                                return JSON.parse(text);
                            }} catch(e) {{
                                return {{ error: 'JSON parse error', text: text.slice(0, 500) }};
                            }}
                        }})
                        .catch(error => {{ 
                            console.error('Fetch error:', error);
                            return {{ error: error.toString() }}; 
                        }})
                    """,
                    
                    # Attempt 2: Fetch with credentials
                    f"""
                    fetch('{self.api_url}', {{
                        credentials: 'include',
                        headers: {{
                            'Accept': 'application/json',
                            'Content-Type': 'application/json'
                        }}
                    }})
                        .then(response => response.json())
                        .catch(error => {{ return {{ error: error.toString() }}; }})
                    """,
                    
                    # Attempt 3: Fetch with different headers
                    f"""
                    fetch('{self.api_url}', {{
                        credentials: 'include',
                        mode: 'cors',
                        headers: {{
                            'Accept': '*/*',
                            'X-Requested-With': 'XMLHttpRequest'
                        }}
                    }})
                        .then(response => response.json())
                        .catch(error => {{ return {{ error: error.toString() }}; }})
                    """
                ]

                for i, fetch_code in enumerate(fetch_attempts, 1):
                    print(f"Attempting fetch method {i}...")
                    try:
                        api_response = await page.evaluate(fetch_code)
                        
                        if isinstance(api_response, list) and len(api_response) > 0:
                            print(f"Success with method {i}! Retrieved {len(api_response)} attributes")
                            return api_response
                        elif isinstance(api_response, dict):
                            if 'error' in api_response:
                                print(f"Method {i} error: {api_response['error']}")
                                if 'text' in api_response:
                                    print(f"Response text: {api_response['text']}")
                            else:
                                print(f"Method {i} returned dict: {api_response}")
                        else:
                            print(f"Method {i} returned: {type(api_response)} - {api_response}")
                            
                    except Exception as e:
                        print(f"Method {i} exception: {e}")
                    
                    await asyncio.sleep(1)

                print("All fetch methods failed")
                return []

            except Exception as e:
                print(f"Error during authentication/fetch: {e}")
                import traceback
                traceback.print_exc()
                return []
            finally:
                await browser.close()

    def update_database(self, attributes_data):
        """Update database with section attributes"""
        if not attributes_data:
            print("No attributes data to update")
            return

        print(f"Processing {len(attributes_data)} attributes...")

        # Create mapping of attribute ID to title
        attribute_mapping = {}
        for attr in attributes_data:
            attr_id = attr.get('id', '').strip()
            attr_title = attr.get('attrTitle', '').strip()
            
            if attr_id and attr_title:
                attribute_mapping[attr_id] = attr_title
                print(f"  {attr_id}: {attr_title}")

        if not attribute_mapping:
            print("No valid attribute mappings found")
            return

        # Update database
        session = get_session()
        try:
            print(f"\nUpdating section attributes in database...")
            
            # Get all unique attribute IDs currently in database
            result = session.execute(text("SELECT DISTINCT attribute_id FROM section_attributes"))
            db_attribute_ids = [row.attribute_id for row in result]
            
            updated_count = 0
            missing_count = 0
            
            for attr_id in db_attribute_ids:
                if attr_id in attribute_mapping:
                    # Update with format "attribute name - attribute code"
                    formatted_title = f"{attribute_mapping[attr_id]} - {attr_id}"
                    
                    update_query = text("""
                        UPDATE section_attributes 
                        SET attribute_title = :attr_title,
                            updated_at = NOW()
                        WHERE attribute_id = :attr_id
                    """)
                    
                    session.execute(update_query, {
                        'attr_title': formatted_title,
                        'attr_id': attr_id
                    })
                    updated_count += 1
                    print(f"  Updated {attr_id}: {formatted_title}")
                else:
                    missing_count += 1
                    print(f"  Missing from API: {attr_id}")
            
            session.commit()
            print(f"\nUpdate complete:")
            print(f"  Updated: {updated_count} attributes")
            print(f"  Missing from API: {missing_count} attributes")
            
        except Exception as e:
            session.rollback()
            print(f"Error updating database: {e}")
            raise
        finally:
            session.close()

    def verify_updates(self):
        """Verify the updates were successful"""
        session = get_session()
        try:
            print(f"\nVerifying updates...")
            
            # Check some sample updated attributes
            result = session.execute(text("""
                SELECT attribute_id, attribute_title 
                FROM section_attributes 
                WHERE attribute_title IS NOT NULL 
                  AND attribute_title LIKE '% - %'
                ORDER BY attribute_id 
                LIMIT 10
            """))
            
            print("Sample updated attributes:")
            for row in result:
                print(f"  {row.attribute_id}: {row.attribute_title}")
            
            # Count total updated vs not updated
            count_result = session.execute(text("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN attribute_title LIKE '% - %' THEN 1 END) as updated_with_format
                FROM section_attributes
            """))
            
            counts = count_result.fetchone()
            print(f"\nSummary:")
            print(f"  Total section attributes: {counts.total}")
            print(f"  Updated with proper format: {counts.updated_with_format}")
            if counts.total > 0:
                print(f"  Coverage: {(counts.updated_with_format / counts.total * 100):.1f}%")
            
        finally:
            session.close()

async def main():
    """Main function"""
    print("Texas A&M Section Attributes Fetcher (with Authentication)")
    print("=" * 60)
    
    fetcher = TAMUAttributesFetcher()
    
    # Fetch attributes using authenticated session
    attributes_data = await fetcher.authenticate_and_fetch()
    
    if not attributes_data:
        print("Failed to fetch attributes. Exiting.")
        return
    
    # Update database with proper formatting
    fetcher.update_database(attributes_data)
    
    # Verify updates
    fetcher.verify_updates()
    
    print("\nSection attributes update completed!")

if __name__ == "__main__":
    asyncio.run(main()) 