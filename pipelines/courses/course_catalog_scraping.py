# gonna try to scrape the course catalog from the https://catalog.tamu.edu/undergraduate/course-descriptions/stat/ sites and get all the
# important data

import re
from html import unescape
from urllib.parse import urljoin
from typing import List, Dict, Any
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://catalog.tamu.edu/undergraduate/course-descriptions/"


def get_all_departments() -> List[Dict[str, Any]]:
    """
    Scrape all departments and their course catalog links from the base URL.

    Returns:
        list: List of dictionaries with department information:
            [
                {
                    'id': 'AALO',
                    'title': 'Arabic & Asian Language',
                    'long_name': 'AALO - Arabic & Asian Language',
                    'url': 'https://catalog.tamu.edu/undergraduate/course-descriptions/aalo/',
                    'path': '/undergraduate/course-descriptions/aalo/'
                },
                ...
            ]
    """
    try:
        response = requests.get(BASE_URL, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        departments = []

        # Find all links that match the department pattern
        # Example: <a href="/undergraduate/course-descriptions/aalo/">AALO - Arabic &amp; Asian Language (AALO)</a>
        department_links = soup.find_all(
            "a", href=re.compile(r"/undergraduate/course-descriptions/[^/]+/$")
        )

        for link in department_links:
            href_val = link.get("href", "")
            if isinstance(href_val, list):
                href = href_val[0] if href_val else ""
            else:
                href = str(href_val)

            text = link.get_text(strip=True)

            # Skip if it's not a department link (should have format: "CODE - Name (CODE)")
            if not text or "/" not in href:
                continue

            # Parse the text: "AALO - Arabic & Asian Language (AALO)"
            # Pattern: CODE - Name (CODE)
            match = re.match(r"^([A-Z]+)\s*-\s*(.+?)\s*\([A-Z]+\)$", text)

            if match:
                code = match.group(1)
                name = match.group(2).strip()

                # Decode HTML entities (e.g., &amp; -> &) and remove zero-width spaces
                name = unescape(name).replace("\u200b", "").strip()

                # Build full URL
                full_url = urljoin(BASE_URL, href)

                # Format: id (code), title (name), long_name (code - name)
                departments.append(
                    {
                        "id": code,
                        "title": name,
                        "long_name": f"{code} - {name}",
                        "url": full_url,
                        "path": href,
                    }
                )

        # Remove duplicates based on URL (in case HTML has duplicate links)
        seen_urls = set()
        unique_departments = []
        for dept in departments:
            if dept["url"] not in seen_urls:
                seen_urls.add(dept["url"])
                unique_departments.append(dept)

        # Sort by department code for consistency
        unique_departments.sort(key=lambda x: str(x.get("id", "")))

        return unique_departments

    except requests.RequestException as e:
        print(f"Error fetching departments: {e}")
        return []
    except Exception as e:
        print(f"Error parsing departments: {e}")
        return []


def parse_course_block(course_block: Any) -> Dict[str, Any]:
    """
    Parse a single course block div and extract all course information.

    Args:
        course_block: BeautifulSoup Tag object representing a courseblock div

    Returns:
        dict: Course information with keys:
            - code: Course code (e.g., "CSCE 221")
            - name: Course name (e.g., "Data Structures and Algorithms")
            - credits: Number of credits (int)
            - lecture_hours: Lecture hours (int or None)
            - lab_hours: Lab hours (int or None)
            - description: Course description text
            - prerequisites: Prerequisites text (or None)
            - prerequisite_courses: List of prerequisite course codes (e.g., ["CSCE 120", "CSCE 121"])
            - cross_listings: List of cross-listed course codes (or empty list)
    """
    course_data: Dict[str, Any] = {
        "code": None,
        "name": None,
        "credits": None,
        "lecture_hours": None,
        "lab_hours": None,
        "other_hours": None,
        "description": None,
        "prerequisites": None,
        "prerequisite_courses": [],  # Flat list for backward compatibility
        "prerequisite_groups": [],  # List of lists: [[group1], [group2]] where group1 is OR, groups are AND
        "corequisites": None,
        "corequisite_courses": [],  # Flat list for backward compatibility
        "corequisite_groups": [],  # List of lists: [[group1], [group2]] where group1 is OR, groups are AND
        "cross_listings": [],
    }

    # Parse course title (e.g., "CSCE 221 Data Structures and Algorithms" or "CSCE 222/ECEN 222 Discrete Structures")
    title_elem = course_block.find("h2", class_="courseblocktitle")
    if title_elem:
        title_text = title_elem.get_text(strip=True)
        # Replace &nbsp; with space and normalize whitespace
        title_text = re.sub(r"\s+", " ", title_text.replace("\xa0", " "))

        # Pattern 1: Cross-listed in title (e.g., "CSCE 222/ECEN 222 Discrete Structures")
        cross_listed_match = re.match(
            r"^([A-Z]+\s+\d+[A-Z]*)/([A-Z]+\s+\d+[A-Z]*)\s+(.+)$", title_text
        )
        if cross_listed_match:
            primary_code = cross_listed_match.group(1).strip()
            cross_code = cross_listed_match.group(2).strip()
            course_name = cross_listed_match.group(3).strip()
            course_data["code"] = primary_code
            course_data["name"] = course_name
            # Add the cross-listed course to cross_listings
            if cross_code not in course_data["cross_listings"]:
                course_data["cross_listings"].append(cross_code)
        else:
            # Pattern 2: Regular course (e.g., "CSCE 221 Data Structures and Algorithms")
            # Try to match: CODE NUMBER NAME
            match = re.match(r"^([A-Z]+)\s+(\d+[A-Z]*)\s+(.+)$", title_text)
            if match:
                dept_code = match.group(1)
                course_num = match.group(2)
                course_name = match.group(3).strip()
                course_data["code"] = f"{dept_code} {course_num}"
                course_data["name"] = course_name

    # Parse course description block
    desc_elem = course_block.find("p", class_="courseblockdesc")
    if desc_elem:
        # Extract hours information
        hours_elem = desc_elem.find("span", class_="hours")
        if hours_elem:
            hours_text = hours_elem.get_text()
            # Parse credits - handle various formats:
            # - "Credit 1" or "Credits 4" -> 1 or 4
            # - "Credit 1.5" or "Credits 1.5" -> 1.5 (round to 2 for int, or store as float)
            # - "Credits 0 to 3" or "Credits 0-3" -> take maximum (3)
            # - "Credits 1 to 3" -> take maximum (3)
            # Note: HTML uses both "Credit" (singular) and "Credits" (plural)

            # Try to match credit range FIRST (e.g., "Credits 0 to 3", "Credits 1-3", "Credits 0-3")
            # This must be checked before single credit to avoid matching "Credit 0" in "Credit 0 to 3"
            # Match both singular "Credit" and plural "Credits"
            range_match = re.search(
                r"Credit(?:s)?\s+(\d+(?:\.\d+)?)\s*(?:to|-)\s*(\d+(?:\.\d+)?)",
                hours_text,
                re.IGNORECASE,
            )
            if range_match:
                # For ranges, take the maximum value
                # This handles cases like "0 to 3" (variable credit courses)
                # where students can take 0-3 credits. We store the max (3).
                max_credits = float(range_match.group(2))
                course_data["credits"] = int(round(max_credits))
                # Note: If you want to store the range differently, we could:
                # - Store as min_credits and max_credits separately
                # - Store as a string "0-3"
                # - Store the average
            else:
                # Try to match single credit value (e.g., "Credit 1" or "Credits 4" or "Credits 1.5")
                # Match both singular "Credit" and plural "Credits"
                credits_match = re.search(
                    r"Credit(?:s)?\s+(\d+(?:\.\d+)?)", hours_text, re.IGNORECASE
                )
                if credits_match:
                    credits_value = float(credits_match.group(1))
                    course_data["credits"] = int(round(credits_value))

            lecture_match = re.search(r"(\d+)\s+Lecture\s+Hours?", hours_text)
            if lecture_match:
                course_data["lecture_hours"] = int(lecture_match.group(1))

            lab_match = re.search(r"(\d+)\s+Lab\s+Hours?", hours_text)
            if lab_match:
                course_data["lab_hours"] = int(lab_match.group(1))

            other_match = re.search(
                r"(\d+)\s+Other\s+Hours?", hours_text, re.IGNORECASE
            )
            if other_match:
                course_data["other_hours"] = int(other_match.group(1))

        # Get full description text
        full_text = desc_elem.get_text(separator=" ", strip=True)

        # Extract prerequisites
        # Look for "Prerequisite:" or "Prerequisites:" in the text
        # Handle both singular and plural forms
        # Find the start of prerequisites
        prereq_start_match = re.search(
            r"Prerequisite(?:s)?:?\s*",
            full_text,
            re.IGNORECASE,
        )
        if prereq_start_match:
            prereq_start = prereq_start_match.end()
            # Find where prerequisites end - look for common stop phrases
            prereq_section = full_text[prereq_start:]

            # Find the end - stop at "also taught", "also offered", "Corequisite", "Cross Listing", or period
            stop_patterns = [
                r";\s*also\s+taught",
                r";\s*also\s+offered",
                r"\.\s*Corequisite",
                r"\.\s*Cross\s+Listing",
                r"\.\s*$",  # End of sentence
            ]

            prereq_end = len(prereq_section)
            for pattern in stop_patterns:
                match = re.search(pattern, prereq_section, re.IGNORECASE)
                if match and match.start() < prereq_end:
                    prereq_end = match.start()

            prereq_text = prereq_section[:prereq_end].strip()
            # Clean up the text - remove trailing periods, semicolons, and extra whitespace
            prereq_text = re.sub(r"[.;]\s*$", "", prereq_text).strip()
            course_data["prerequisites"] = prereq_text

            # Extract prerequisite course codes from links
            # Only get links that appear in the prerequisite section (before "Cross Listing:", "Corequisite:", "also taught", etc.)
            prereq_section_end = len(full_text)
            for stop_phrase in [
                "cross listing",
                "corequisite",
                "concurrent enrollment",
                "also taught",
            ]:
                idx = full_text.lower().find(stop_phrase)
                if idx > 0 and idx < prereq_section_end:
                    prereq_section_end = idx

            prereq_section_text = (
                full_text[:prereq_section_end] if prereq_section_end > 0 else full_text
            )

            # Parse prerequisites into groups
            # Groups are separated by semicolons (AND relationship)
            # Within groups, courses are separated by "or" (OR relationship)
            # Example: "CSCE 120 or CSCE 121 ; CSCE 222/ECEN 222" -> [[CSCE 120, CSCE 121], [CSCE 222, ECEN 222]]

            normalized_prereq_text = re.sub(
                r"\s+", " ", prereq_section_text.replace("\xa0", " ")
            )

            # Split by semicolons to get groups (AND relationship)
            # But be careful - semicolons might be in other contexts
            # Look for patterns like " ; " or " ;" before splitting
            prereq_groups_text = re.split(r"\s*;\s*", normalized_prereq_text)

            all_prereq_links = desc_elem.find_all("a", class_="bubblelink code")

            for group_text in prereq_groups_text:
                # Skip groups that are just "or concurrent enrollment" (handled separately)
                if re.search(
                    r"^or\s+concurrent\s+enrollment", group_text, re.IGNORECASE
                ):
                    continue

                # Find all course links that appear in this group
                group_courses = []
                seen_in_group = set()

                for link in all_prereq_links:
                    link_text = link.get_text(strip=True)
                    link_text = re.sub(r"\s+", " ", link_text.replace("\xa0", " "))

                    if not re.match(r"^[A-Z]+\s+\d+", link_text):
                        continue

                    # Skip the course itself
                    if link_text == course_data["code"]:
                        continue

                    # Check if this link appears in this group
                    link_in_group = False
                    if "/" in link_text:
                        parts = [c.strip() for c in link_text.split("/")]
                        link_in_group = any(part in group_text for part in parts)
                    else:
                        link_in_group = link_text in group_text

                    if link_in_group:
                        # Check if it's a standalone "Concurrent enrollment" (skip those)
                        link_context_start = max(0, group_text.find(link_text) - 30)
                        link_context_end = min(
                            len(group_text),
                            group_text.find(link_text) + len(link_text) + 30,
                        )
                        link_context = group_text[link_context_start:link_context_end]

                        has_or_concurrent = re.search(
                            r"or\s+concurrent\s+enrollment",
                            link_context,
                            re.IGNORECASE,
                        )

                        # Skip standalone "Concurrent enrollment" but keep "or concurrent enrollment"
                        if not has_or_concurrent and (
                            re.search(
                                r"concurrent\s+enrollment.*" + re.escape(link_text),
                                link_context,
                                re.IGNORECASE,
                            )
                            or re.search(
                                re.escape(link_text) + r".*concurrent\s+enrollment",
                                link_context,
                                re.IGNORECASE,
                            )
                        ):
                            continue

                        # Handle cross-listed courses (e.g., "CSCE 222/ECEN 222")
                        if "/" in link_text:
                            cross_courses = [
                                c.strip() for c in link_text.split("/") if c.strip()
                            ]
                            # Add all cross-listed variants to the group
                            for course in cross_courses:
                                if course not in seen_in_group:
                                    seen_in_group.add(course)
                                    group_courses.append(course)
                        else:
                            if link_text not in seen_in_group:
                                seen_in_group.add(link_text)
                                group_courses.append(link_text)

                # Only add non-empty groups
                if group_courses:
                    course_data["prerequisite_groups"].append(group_courses)
                    # Also add to flat list for backward compatibility
                    course_data["prerequisite_courses"].extend(group_courses)

            # Deduplicate the flat list while preserving order
            seen = set()
            unique_prereqs = []
            for course in course_data["prerequisite_courses"]:
                if course not in seen:
                    seen.add(course)
                    unique_prereqs.append(course)
            course_data["prerequisite_courses"] = unique_prereqs

            # Note: We do NOT remove corequisite courses from prerequisite courses here
            # because courses with "or concurrent enrollment" should be in BOTH lists
            # (they can be taken as either prerequisite OR corequisite)

        # Extract corequisites (concurrent enrollment)
        # Look for "Concurrent enrollment" or "Corequisite" in the text
        # This can appear as a standalone section or within prerequisites
        coreq_match = re.search(
            r"(?:Corequisite|Concurrent\s+enrollment)\s+in\s+([A-Z]+\s+\d+[A-Z]*(?:\s*/\s*[A-Z]+\s+\d+[A-Z]*)?)",
            full_text,
            re.IGNORECASE,
        )
        if coreq_match:
            # Check if this is part of "or concurrent enrollment" (which is handled separately)
            coreq_pos = full_text.lower().find(coreq_match.group(0).lower())
            before_coreq = full_text[:coreq_pos].lower() if coreq_pos > 0 else ""

            # Only extract if it's not part of "or concurrent enrollment" clause
            if not re.search(
                r"or\s+concurrent\s+enrollment", before_coreq, re.IGNORECASE
            ):
                coreq_course_text = coreq_match.group(1).strip()
                course_data["corequisites"] = (
                    f"Concurrent enrollment in {coreq_course_text}"
                )

                # Extract the corequisite course code from the match
                coreq_group = []
                if "/" in coreq_course_text:
                    cross_courses = [
                        c.strip() for c in coreq_course_text.split("/") if c.strip()
                    ]
                    for course in cross_courses:
                        if course != course_data["code"]:
                            coreq_group.append(course)
                            course_data["corequisite_courses"].append(course)
                else:
                    if coreq_course_text != course_data["code"]:
                        coreq_group.append(coreq_course_text)
                        course_data["corequisite_courses"].append(coreq_course_text)

                # Add to corequisite groups
                if coreq_group:
                    course_data["corequisite_groups"].append(coreq_group)

        # Check for "or concurrent enrollment" in prerequisites (these are prerequisite OR corequisite)
        # Use prerequisite_groups to create corresponding corequisite_groups
        if course_data["prerequisites"] and course_data["prerequisite_groups"]:
            # Get the prerequisite section text to check for "or concurrent enrollment"
            prereq_section_end = full_text.lower().find("cross listing")
            if prereq_section_end == -1:
                prereq_section_end = len(full_text)

            prereq_section_text = (
                full_text[:prereq_section_end] if prereq_section_end > 0 else full_text
            )

            # Check which prerequisite groups have "or concurrent enrollment"
            normalized_prereq_text = re.sub(
                r"\s+", " ", prereq_section_text.replace("\xa0", " ")
            )

            # Find groups that have "or concurrent enrollment"
            prereq_groups_text = re.split(r"\s*;\s*", normalized_prereq_text)

            for i, group_text in enumerate(prereq_groups_text):
                if re.search(
                    r"or\s+concurrent\s+enrollment", group_text, re.IGNORECASE
                ):
                    # This group can also be taken as corequisite
                    # Add the corresponding prerequisite group to corequisite groups
                    if i < len(course_data["prerequisite_groups"]):
                        prereq_group = course_data["prerequisite_groups"][i]
                        # Add to corequisite groups if not already there
                        if prereq_group not in course_data["corequisite_groups"]:
                            course_data["corequisite_groups"].append(prereq_group)
                        # Also add to flat corequisite_courses list
                        for course in prereq_group:
                            if course not in course_data["corequisite_courses"]:
                                course_data["corequisite_courses"].append(course)

            # Also handle the old pattern matching for backward compatibility
            if re.search(
                r"or\s+concurrent\s+enrollment",
                course_data["prerequisites"],
                re.IGNORECASE,
            ):
                # Find courses that are specifically mentioned with "or concurrent enrollment"
                # (This is a fallback if the prerequisite_groups approach didn't catch it)
                or_concurrent_pattern = re.finditer(
                    r"([A-Z]+\s+\d+[A-Z]*(?:\s*/\s*[A-Z]+\s+\d+[A-Z]*)*(?:\s+or\s+[A-Z]+\s+\d+[A-Z]*(?:\s*/\s*[A-Z]+\s+\d+[A-Z]*)?)*)\s*,\s*or\s+concurrent\s+enrollment",
                    course_data["prerequisites"],
                    re.IGNORECASE,
                )
                for match in or_concurrent_pattern:
                    course_codes_text = match.group(1).strip()
                    # Create a group for these courses (they're OR within the group)
                    coreq_group = []
                    # Handle multiple courses separated by "or" (e.g., "CSCE 222/ECEN 222 or ECEN 222/CSCE 222")
                    # Split by "or" first, then handle cross-listings
                    course_parts = re.split(
                        r"\s+or\s+", course_codes_text, flags=re.IGNORECASE
                    )
                    for part in course_parts:
                        part = part.strip()
                        # Handle cross-listed courses (e.g., "CSCE 222/ECEN 222")
                        if "/" in part:
                            cross_courses = [
                                c.strip() for c in part.split("/") if c.strip()
                            ]
                            for course in cross_courses:
                                if (
                                    course != course_data["code"]
                                    and course not in coreq_group
                                ):
                                    coreq_group.append(course)
                                    if course not in course_data["corequisite_courses"]:
                                        course_data["corequisite_courses"].append(
                                            course
                                        )
                        else:
                            if part != course_data["code"] and part not in coreq_group:
                                coreq_group.append(part)
                                if part not in course_data["corequisite_courses"]:
                                    course_data["corequisite_courses"].append(part)

                    # Add group to corequisite_groups if not already there
                    if (
                        coreq_group
                        and coreq_group not in course_data["corequisite_groups"]
                    ):
                        course_data["corequisite_groups"].append(coreq_group)

        # Deduplicate corequisite courses and set text
        if course_data["corequisite_courses"]:
            seen = set()
            unique_coreqs = []
            for course in course_data["corequisite_courses"]:
                if course not in seen:
                    seen.add(course)
                    unique_coreqs.append(course)
            course_data["corequisite_courses"] = unique_coreqs
            # Set corequisites text if not already set
            if not course_data["corequisites"]:
                course_data["corequisites"] = (
                    f"Concurrent enrollment in {', '.join(unique_coreqs)}"
                )

        # Extract cross-listings from the description
        # Look for "Cross Listing:" section - find links that appear after this text
        cross_listing_match = re.search(
            r"Cross\s+Listing:?\s*(.+?)(?:\.|<br|$)",
            full_text,
            re.IGNORECASE | re.DOTALL,
        )
        if cross_listing_match:
            cross_section_text = cross_listing_match.group(1).strip()
            # Find all links in the description
            all_links = desc_elem.find_all("a", class_="bubblelink code")
            # Find the link whose text appears in the cross-listing section
            for link in all_links:
                link_text = link.get_text(strip=True)
                link_text = re.sub(r"\s+", " ", link_text.replace("\xa0", " "))
                # Check if this link's text appears in the cross-listing section
                # Normalize both texts for comparison
                normalized_cross_text = re.sub(
                    r"\s+", " ", cross_section_text.replace("\xa0", " ")
                )
                if link_text in normalized_cross_text or any(
                    part.strip() in normalized_cross_text
                    for part in link_text.split("/")
                ):
                    # Handle cross-listed format (e.g., "ECEN 222/CSCE 222")
                    if "/" in link_text:
                        cross_courses = [
                            c.strip() for c in link_text.split("/") if c.strip()
                        ]
                        for cross_course in cross_courses:
                            # Don't add the primary course code to cross_listings
                            if (
                                cross_course != course_data["code"]
                                and cross_course not in course_data["cross_listings"]
                            ):
                                course_data["cross_listings"].append(cross_course)
                    else:
                        # Don't add the primary course code to cross_listings
                        if (
                            link_text != course_data["code"]
                            and link_text not in course_data["cross_listings"]
                        ):
                            course_data["cross_listings"].append(link_text)
                    break  # Only process the first matching link (the cross-listing one)

        # Also look for patterns like "Cross-listed with CSCE 222" or "Same as ECEN 222"
        cross_listing_patterns = [
            r"Cross-?listed\s+with\s+([A-Z]+\s+\d+[A-Z]*)",
            r"Same\s+as\s+([A-Z]+\s+\d+[A-Z]*)",
        ]
        for pattern in cross_listing_patterns:
            cross_matches = re.finditer(pattern, full_text, re.IGNORECASE)
            for match in cross_matches:
                cross_code = match.group(1).strip()
                # Don't add the primary course code to cross_listings
                if (
                    cross_code != course_data["code"]
                    and cross_code not in course_data["cross_listings"]
                ):
                    course_data["cross_listings"].append(cross_code)

        # Extract description (everything except hours and prerequisites)
        # Remove the hours span
        if hours_elem:
            hours_elem.decompose()

        # Remove prerequisite section for clean description
        desc_text = desc_elem.get_text(separator=" ", strip=True)
        # Remove prerequisite text
        desc_text = re.sub(
            r"Prerequisite:?.+?\.", "", desc_text, flags=re.IGNORECASE | re.DOTALL
        )
        # Remove corequisite text
        desc_text = re.sub(
            r"(?:Corequisite|Concurrent\s+enrollment):?.+?\.",
            "",
            desc_text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        # Remove cross-listing text
        desc_text = re.sub(
            r"Cross\s+Listing:?.+?\.", "", desc_text, flags=re.IGNORECASE | re.DOTALL
        )
        desc_text = re.sub(
            r"Cross-?listed\s+with\s+[A-Z]+\s+\d+[A-Z]*\.?",
            "",
            desc_text,
            flags=re.IGNORECASE,
        )
        desc_text = re.sub(
            r"Same\s+as\s+[A-Z]+\s+\d+[A-Z]*\.?",
            "",
            desc_text,
            flags=re.IGNORECASE,
        )
        course_data["description"] = desc_text.strip()

    return course_data


def get_courses_from_department(department_url: str) -> List[Dict[str, Any]]:
    """
    Scrape all courses from a department's course catalog page.

    Args:
        department_url: URL to the department's course catalog page

    Returns:
        list: List of course dictionaries, each containing:
            - code: Course code
            - name: Course name
            - credits: Number of credits
            - lecture_hours: Lecture hours
            - lab_hours: Lab hours
            - description: Course description
            - prerequisites: Prerequisites text
            - prerequisite_courses: List of prerequisite course codes
            - cross_listings: List of cross-listed course codes
    """
    try:
        response = requests.get(department_url, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        courses = []

        # Find all course blocks
        course_blocks = soup.find_all("div", class_="courseblock")

        for block in course_blocks:
            course_data = parse_course_block(block)
            # Only add if we successfully parsed a course code
            if course_data["code"]:
                courses.append(course_data)

        return courses

    except requests.RequestException as e:
        print(f"Error fetching courses from {department_url}: {e}")
        return []
    except Exception as e:
        print(f"Error parsing courses from {department_url}: {e}")
        return []


if __name__ == "__main__":
    import json
    import sys

    # Test with a specific department if provided as argument
    if len(sys.argv) > 1:
        dept_code = sys.argv[1].upper()
        departments = get_all_departments()
        dept = next((d for d in departments if d["code"] == dept_code), None)

        if dept:
            print(f"Scraping courses from {dept['code']} - {dept['name']}\n")
            courses = get_courses_from_department(dept["url"])
            print(f"Found {len(courses)} courses\n")

            # Show first few courses as examples
            for i, course in enumerate(courses[:3], 1):
                print(f"Course {i}:")
                print(f"  Code: {course['code']}")
                print(f"  Name: {course['name']}")
                print(f"  Credits: {course['credits']}")
                print(f"  Lecture Hours: {course['lecture_hours']}")
                print(f"  Lab Hours: {course['lab_hours']}")
                print(f"  Description: {course['description'][:100]}...")
                if course["prerequisites"]:
                    print(f"  Prerequisites: {course['prerequisites']}")
                if course["prerequisite_courses"]:
                    print(
                        f"  Prerequisite Courses: {', '.join(course['prerequisite_courses'])}"
                    )
                if course["cross_listings"]:
                    print(f"  Cross-listings: {', '.join(course['cross_listings'])}")
                print()

            # Optionally save to JSON
            if len(sys.argv) > 2 and sys.argv[2] == "--json":
                output_file = f"{dept_code}_courses.json"
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(courses, f, indent=2, ensure_ascii=False)
                print(f"Saved {len(courses)} courses to {output_file}")
        else:
            print(f"Department '{dept_code}' not found")
            print("Available departments:")
            for d in departments[:10]:
                print(f"  {d['code']} - {d['name']}")
    else:
        # Default: show departments
        departments = get_all_departments()
        print(f"Found {len(departments)} unique departments\n")
        print("First 20 departments:")
        print("-" * 80)
        for dept in departments[:20]:
            # Clean name for printing (remove any problematic unicode characters)
            clean_name = dept["name"].encode("ascii", "ignore").decode("ascii")
            print(f"{dept['code']:6} - {clean_name:50} -> {dept['url']}")

        if len(departments) > 20:
            print(f"\n... and {len(departments) - 20} more departments")
        print("\nTo scrape courses from a department, run:")
        print("  python course_catalog_scraping.py CSCE")
        print("  python course_catalog_scraping.py CSCE --json  # Save to JSON")
