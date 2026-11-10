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


def school_rating_counts(collector, school_id: str) -> dict[str, int]:
    counts = {}
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
            if node.get("school", {}).get("id") != school_id:
                continue
            if isinstance(node.get("id"), str) and type(count) is int and count >= 0:
                counts[node["id"]] = count
        page = teachers["pageInfo"]
        if page["hasNextPage"] is False:
            return counts
        if page["hasNextPage"] is not True:
            raise ValueError("Invalid RMP count pagination")
        cursor = page.get("endCursor")
        if not cursor or cursor in seen:
            raise ValueError("RMP rating-count pagination did not advance")
        seen.add(cursor)


def fetch_rating_counts(session, professor_ids) -> dict[str, int]:
    """Unknown/missing counts fall back to normal review fetching."""
    if not professor_ids:
        return {}
    rows = (
        session.query(ProfessorDB.id, ProfessorDB.university_id)
        .filter(ProfessorDB.id.in_(professor_ids))
        .all()
    )
    schools = {school for _, school in rows if school}
    counts = {}
    for school_id in schools:
        collector = None
        try:
            collector = RMPReviewCollector()
            # Only publish a school's snapshot after all pages succeed.
            counts.update(school_rating_counts(collector, school_id))
        except Exception as exc:
            print(
                f"  Count check unavailable for {school_id}; checking reviews individually ({type(exc).__name__})",
                flush=True,
            )
        finally:
            if collector is not None:
                collector.session.close()
    return counts
