import re
from playwright.sync_api import Page, expect

def get_all_departments():
    """Get all departments from the database"""
    page = Page()
    page.goto("https://tamu.collegescheduler.com/api/terms/Fall%202025%20-%20College%20Station/subjects")
    print(page.content())
    return page.content()

def main():
    departments = get_all_departments()
    print(departments)
    
    
if __name__ == "__main__":
    main()