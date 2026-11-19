"""Read the HTML emitted by the manual instructor AI review job."""

from functools import lru_cache
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from bs4 import BeautifulSoup

AI_REVIEW_PRIORITY = {"high": 0, "possible": 1}


@lru_cache(maxsize=1)
def read_saved_ai_report(path: Path, modified_ns: int) -> dict:
    """Cache the current report until its modification time changes."""
    return parse_instructor_ai_report(path.read_text(encoding="utf-8"))


def _https_url(value: str) -> str | None:
    try:
        url = urlsplit(value)
        if url.scheme == "https" and url.hostname and not url.username:
            return value
    except ValueError:
        pass
    return None


def parse_instructor_ai_report(html: str) -> dict:
    # Extract only text and validated links. The browser never renders report HTML.
    document = BeautifulSoup(html, "html.parser")
    if not document.title or document.title.get_text(strip=True) != "Instructor link review":
        raise ValueError("The saved file is not an instructor AI review report.")

    reviews = []
    for article in document.find_all("article"):
        heading = article.find("h2")
        paragraphs = article.find_all("p", recursive=False)
        admin = article.find("a", recursive=False, href=True)
        if not heading or not paragraphs or not admin:
            raise ValueError("A recommendation in the saved AI report is incomplete.")

        confidence, _, title = heading.get_text().partition(" · ")
        confidence = confidence.lower()
        query = parse_qs(urlsplit(admin["href"]).query)
        key = query.get("q", [""])[0]
        tab = query.get("tab", [""])[0]
        if (not title or confidence not in {"high", "possible", "unclear", "unlikely"}
                or not key or tab not in {"proposed", "conflict"}):
            raise ValueError("A recommendation in the saved AI report has invalid review details.")

        rmp = heading.find("a", href=True)
        sources = []
        for item in article.select("ul > li"):
            link = item.find("a", href=True)
            if link and (url := _https_url(link["href"])):
                source_title = link.get_text(strip=True)
                sources.append({
                    "url": url,
                    "title": source_title or url,
                    "snippet": item.get_text(" ", strip=True).removeprefix(source_title).strip(),
                })

        reviews.append({
            "title": title,
            "confidence": confidence,
            "key": key,
            "tab": tab,
            "rmpUrl": _https_url(rmp["href"]) if rmp else None,
            "reason": paragraphs[0].get_text(strip=True),
            "details": [p.get_text(strip=True) for p in paragraphs[1:]],
            "sources": sources,
        })

    # The generator writes two introductory paragraphs, then any run errors.
    messages = [p.get_text(strip=True) for p in document.find_all("p", recursive=False)][2:]
    return {"reviews": reviews, "errors": messages}


def attach_ai_reviews(instructors: list[dict], reviews: list[dict]) -> None:
    """Attach saved evidence to current pending candidates for the same instructor."""
    by_key: dict[str, list[dict]] = {}
    for review in reviews:
        by_key.setdefault(review["key"], []).append(review)

    for instructor in instructors:
        candidates = instructor["candidates"]
        for review in by_key.get(instructor["key"], []):
            if rmp_url := review["rmpUrl"]:
                rmp = urlsplit(rmp_url)
                prefix = "/professor/"
                legacy_id = rmp.path.rstrip("/").removeprefix(prefix)
                if (rmp.hostname not in {"www.ratemyprofessors.com", "ratemyprofessors.com"}
                        or not rmp.path.startswith(prefix) or not legacy_id.isdecimal()):
                    continue
                matches = [c for c in candidates if str(c["rmpLegacyId"]) == legacy_id]
            else:
                # Saved reports omit the URL when a profile has no RMP legacy ID.
                _, _, label = review["title"].partition(" \u2192 ")
                matches = [c for c in candidates if label and label == (c["name"] or c["professorId"])]
            if len(matches) == 1 and matches[0]["status"] == "proposed":
                matches[0].setdefault("aiReview", {
                    field: review[field] for field in ("confidence", "reason", "sources")
                })


def instructor_ai_priority(instructor: dict) -> int:
    return min(
        (AI_REVIEW_PRIORITY.get(c["aiReview"]["confidence"], 2)
         for c in instructor["candidates"] if "aiReview" in c),
        default=2,
    )
