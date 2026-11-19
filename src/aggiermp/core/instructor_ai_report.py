"""Read the HTML emitted by the manual instructor AI review job."""

from urllib.parse import parse_qs, urlsplit

from bs4 import BeautifulSoup


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
