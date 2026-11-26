"""Bulk review-count gate for incremental synchronization."""

from pipelines.professors.scrapers import RMPReviewCollector
from aggiermp.database.base import ProfessorDB

COUNT_QUERY = """
query TeacherCounts($count: Int!, $cursor: String, $query: TeacherSearchQuery!) {
  search: newSearch {
    teachers(query: $query, first: $count, after: $cursor) {
      edges { node { id numRatings school { id } } }
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""


def needs_review_fetch(local_count: int, remote_count: int | None) -> bool:
    return remote_count is None or remote_count != local_count


def school_listing(collector, school_id: str) -> dict[str, int | None]:
    """Every teacher id RMP lists for a school -> numRatings (None if invalid).

    Raises if any page fails or pagination is invalid, so a returned listing is
    always complete.
    """
    listing: dict[str, int | None] = {}
    cursor = None
    seen = set()
    while True:
        response = collector.session.post(
            collector.url,
            headers=collector.headers,
            cookies=collector.cookies,
            json={
                "query": COUNT_QUERY,
                "operationName": "TeacherCounts",
                "variables": {
                    "count": 100,
                    "cursor": cursor,
                    "query": {"text": "", "schoolID": school_id, "fallback": False},
                },
            },
            timeout=30,
        )
        response.raise_for_status()
        body = response.json()
        if body.get("errors"):
            raise ValueError("RMP rating-count query returned GraphQL errors")
        teachers = body["data"]["search"]["teachers"]
        for edge in teachers["edges"]:
            node = edge["node"]
            count = node.get("numRatings")
            if (node.get("school") or {}).get("id") != school_id:
                continue
            if isinstance(node.get("id"), str):
                listing[node["id"]] = (
                    count if type(count) is int and count >= 0 else None
                )
        page = teachers["pageInfo"]
        if page["hasNextPage"] is False:
            return listing
        if page["hasNextPage"] is not True:
            raise ValueError("Invalid RMP count pagination")
        cursor = page.get("endCursor")
        if not cursor or cursor in seen:
            raise ValueError("RMP rating-count pagination did not advance")
        seen.add(cursor)


def school_rating_counts(collector, school_id: str) -> dict[str, int]:
    return {
        pid: count
        for pid, count in school_listing(collector, school_id).items()
        if count is not None
    }


def fetch_school_listings(session, professor_ids) -> dict[str, dict[str, int | None]]:
    """{school_id: listing} for schools whose full listing completed."""
    if not professor_ids:
        return {}
    rows = (
        session.query(ProfessorDB.id, ProfessorDB.university_id)
        .filter(ProfessorDB.id.in_(professor_ids))
        .all()
    )
    schools = {school for _, school in rows if school}
    listings = {}
    for school_id in schools:
        collector = None
        try:
            collector = RMPReviewCollector()
            # Only publish a school's snapshot after all pages succeed.
            listings[school_id] = school_listing(collector, school_id)
        except Exception as exc:
            print(
                f"  Count check unavailable for {school_id}; checking reviews individually ({type(exc).__name__})",
                flush=True,
            )
        finally:
            if collector is not None:
                collector.session.close()
    return listings


def fetch_rating_counts(session, professor_ids) -> dict[str, int]:
    """Unknown/missing counts fall back to normal review fetching."""
    counts = {}
    for listing in fetch_school_listings(session, professor_ids).values():
        counts.update({pid: c for pid, c in listing.items() if c is not None})
    return counts


NODE_QUERY = """
query TeacherExists($id: ID!) {
  node(id: $id) { __typename ... on Teacher { id } }
}
"""

RATING_IDS_QUERY = """
query TeacherRatingIds($id: ID!, $count: Int!, $cursor: String) {
  node(id: $id) {
    ... on Teacher {
      id
      numRatings
      ratings(first: $count, after: $cursor) {
        edges { node { id } }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}
"""


def _post(collector, query, operation, variables):
    response = collector.session.post(
        collector.url,
        headers=collector.headers,
        cookies=collector.cookies,
        json={"query": query, "operationName": operation, "variables": variables},
        timeout=30,
    )
    response.raise_for_status()
    body = response.json()
    if not isinstance(body, dict) or body.get("errors"):
        raise ValueError("RMP query returned GraphQL errors or a bad body")
    data = body.get("data")
    if not isinstance(data, dict) or "node" not in data:
        raise ValueError("Unexpected RMP response shape")
    return data["node"]


def teacher_exists(collector, professor_id: str) -> bool | None:
    """True/False if RMP answers definitively, None if unknown. Never raises."""
    try:
        node = _post(collector, NODE_QUERY, "TeacherExists", {"id": professor_id})
        if node is None:
            return False
        if isinstance(node, dict) and node.get("__typename") == "Teacher":
            return True
        return None
    except Exception:
        return None


def deleted_review_ids(
    collector, professor_id: str, stored_ids: set[str]
) -> set[str] | None:
    """stored_ids no longer on RMP, or None when the listing can't be trusted."""
    try:
        rmp_ids: set[str] = set()
        seen_cursors = set()
        cursor = None
        expected = None
        while True:
            node = _post(
                collector,
                RATING_IDS_QUERY,
                "TeacherRatingIds",
                {"id": professor_id, "count": 100, "cursor": cursor},
            )
            if not isinstance(node, dict):
                return None
            total = node.get("numRatings")
            if type(total) is not int or total < 0:
                return None
            if expected is not None and total != expected:
                return None
            expected = total
            ratings = node.get("ratings")
            if not isinstance(ratings, dict):
                return None
            for edge in ratings["edges"]:
                rid = edge["node"]["id"]
                if not isinstance(rid, str):
                    return None
                rmp_ids.add(rid)
            page = ratings["pageInfo"]
            if page["hasNextPage"] is False:
                break
            if page["hasNextPage"] is not True:
                return None
            cursor = page.get("endCursor")
            if not cursor or cursor in seen_cursors:
                return None
            seen_cursors.add(cursor)
        # RMP's numRatings can lag its list by one or two (seen: 753 vs 754),
        # so a longer list is fine. A shorter one means pages are missing and
        # absent ids would be wrongly treated as deleted.
        if len(rmp_ids) < expected:
            return None
        return set(stored_ids) - rmp_ids
    except Exception:
        return None
