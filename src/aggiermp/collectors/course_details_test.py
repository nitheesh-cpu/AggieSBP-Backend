from playwright.sync_api import sync_playwright


def get_all_departments() -> str:
    """Get all departments from the database"""
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(
            "https://tamu.collegescheduler.com/api/terms/Fall%202025%20-%20College%20Station/subjects"
        )
        content = page.content()
        browser.close()
        return content


def main() -> None:
    departments = get_all_departments()
    print(departments)


if __name__ == "__main__":
    main()
