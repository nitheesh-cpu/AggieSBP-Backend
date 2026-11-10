"""
Parse catalog prerequisite text into AND/OR trees (courses.prereq_tree).

Order of attempts per course:
  1. Rules (prereq_parser.parse_with_rules) for simple phrasings. High confidence.
  2. TAMU AI in batches, validated against the codes in the source text.
     High confidence unless validation found recoverable issues.
  3. The scraper's legacy prerequisite_groups. Always low confidence.

Rows whose text hash (normalized text + PARSER_VERSION) is unchanged are
skipped unless --force.

Usage:
  # Apply pipelines/courses/migrations/001_prereq_trees.sql first.
  python -m pipelines.courses.parse_prereqs                 # all changed rows
  python -m pipelines.courses.parse_prereqs --dept CSCE --dry-run
  python -m pipelines.courses.parse_prereqs --no-llm        # rules + legacy only
  python -m pipelines.courses.parse_prereqs --from-json samples.json --dry-run
      # offline: list of {code, prereq, coreq, groups?}; prints trees, no DB
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

project_root = Path(__file__).resolve().parent.parent.parent
for p in (project_root, project_root / "src"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(dotenv_path=project_root / ".env")

from pipelines.courses.prereq_parser import (  # noqa: E402
    PARSER_VERSION,
    RETRYABLE_ISSUES,
    RuleParseFailed,
    codes_in_text,
    finalize,
    legacy_tree,
    normalize_code,
    normalize_text,
    parse_with_rules,
    text_hash,
    validate_tree,
)

logger = logging.getLogger("parse_prereqs")

BATCH_SIZE = 10
BATCH_MAX_TOKENS = 6000

SYSTEM_PROMPT = """You convert Texas A&M course catalog prerequisite text into JSON trees.

Node shapes:
  {"type":"and","children":[...]}   all children required
  {"type":"or","children":[...]}    any one child suffices
  {"type":"course","code":"CSCE 221","min_grade":"C" or null,"concurrent":false}
  {"type":"classification","levels":["junior","senior"]}   levels from: freshman, sophomore, junior, senior, graduate
  {"type":"approval","by":"instructor"|"department"|"advisor"|"other"}
  {"type":"condition","kind":"major"|"admission"|"placement"|"equivalent"|"credits"|"gpa"|"other","text":"short original phrase"}

Rules:
- Use ONLY course codes that appear in the given text, written "DEPT 123". Never add courses.
- Semicolons usually separate clauses that are ALL required (and).
- "A, B, or C" is or. "A, B, and C" is and. When a clause mixes "and" and "or" with commas, follow the most natural English reading.
- "Grade of C or better in X" sets min_grade "C" on the courses in that clause only.
- "X or concurrent enrollment" / "X or registration therein" means X may be taken the same term: set "concurrent": true on X (or every course in that clause). It is NOT a separate corequisite.
- "Concurrent enrollment in X" alone means X must be taken before or in the same term: a course node with "concurrent": true.
- Cross-listed "CSCE 350/ECEN 350" means either one: an "or" of both.
- "or equivalent" adds {"type":"condition","kind":"equivalent","text":"or equivalent"} to that clause's or.
- "or approval of instructor" / "approval of department head" are approval nodes, joined with or/and as the text says.
- Majors, program admission, placement exams, credit-hour counts ("6 credits of 300-level SPAN"), GPA minimums, and anything else non-course are condition nodes. "300-level" or "400-level" is never a course code.
- "Senior classification in industrial engineering" is a classification node AND a major condition.
- Courses that are only "suggested", "recommended", or "preferred" are not requirements: leave them out.
- The coreq field lists courses the catalog says need concurrent enrollment. Output a coreq tree ONLY for courses that must be taken in the same term (or earlier). If the prereq text already allows that course "or concurrent enrollment", or only suggests it, coreq is null.
- Use null for an empty tree.

Return a JSON object: {"results":[{"code":"<course code>","prereq":<tree or null>,"coreq":<tree or null>}]} with one result per input course, in order.

Example input:
[{"code":"CSCE 411","prereq":"Grade of C or better in CSCE 221 and CSCE 222; junior or senior classification or approval of instructor","coreq":null}]
Example output:
{"results":[{"code":"CSCE 411","prereq":{"type":"and","children":[{"type":"course","code":"CSCE 221","min_grade":"C","concurrent":false},{"type":"course","code":"CSCE 222","min_grade":"C","concurrent":false},{"type":"or","children":[{"type":"classification","levels":["junior","senior"]},{"type":"approval","by":"instructor"}]}]},"coreq":null}]}
"""


@dataclass
class CourseRow:
    id: Optional[str]
    code: str
    prereq: Optional[str]
    coreq: Optional[str]
    groups: Optional[str] = None
    old_hash: Optional[str] = None


@dataclass
class ParseResult:
    row: CourseRow
    prereq_tree: Optional[Dict[str, Any]]
    coreq_tree: Optional[Dict[str, Any]]
    source: str
    confidence: str
    issues: List[str] = field(default_factory=list)
    model: Optional[str] = None

    def meta(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "confidence": self.confidence,
            "issues": self.issues,
            "hash": text_hash(self.row.prereq, self.row.coreq),
            "parser_version": PARSER_VERSION,
            "model": self.model,
            "parsed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def try_rules(row: CourseRow) -> Optional[ParseResult]:
    try:
        p, c = parse_with_rules(row.prereq, row.coreq, normalize_code(row.code))
    except RuleParseFailed:
        return None
    return ParseResult(row, p, c, "rules", "high")


def fallback(row: CourseRow, issue: str) -> ParseResult:
    text = normalize_text(row.prereq)
    tree = legacy_tree(row.groups, text, normalize_code(row.code))
    if tree is not None:
        # Same clean-up as other sources (drops suggested courses, merges
        # cross-listings); issues stay just the fallback reason.
        tree = finalize(tree, None, prereq_text=text, coreq_text="", self_code=normalize_code(row.code))[0]
    if tree is None and text:
        # Keep *something* so the planner shows "check the catalog" instead
        # of treating the course as having no prerequisites.
        tree = {"type": "condition", "kind": "other", "text": text}
    return ParseResult(row, tree, None, "legacy", "low", [issue])


RETRY_HINTS = {
    "and_in_or_clause": "you joined courses with AND inside a clause whose only connector is 'or'",
    "or_in_and_clause": "you joined items with OR inside a clause whose only connector is 'and'",
    "and_of_crosslisted": "you required both halves of a cross-listed pair; 'A/B' means either one",
    "llm_rejected": "you used course codes that are not in the text; '300-level' and credit counts are credits conditions",
}


def _llm_input(rows: List[CourseRow], hints: Optional[Dict[str, str]] = None) -> str:
    items = []
    for r in rows:
        item: Dict[str, Any] = {
            "code": normalize_code(r.code) or r.code,
            "prereq": normalize_text(r.prereq) or None,
            "coreq": normalize_text(r.coreq) or None,
        }
        hint = (hints or {}).get(normalize_code(r.code) or "")
        if hint:
            item["note"] = f"A previous attempt was wrong: {hint}. Re-read each clause's connectors."
        items.append(item)
    return json.dumps(items, ensure_ascii=False)


def _retry_hint(result: "ParseResult") -> Optional[str]:
    for issue in result.issues:
        key, _, detail = issue.partition(":")
        if key == "missing_codes":
            return f"you left out {detail}, which the text requires"
        if key in RETRYABLE_ISSUES or key == "llm_rejected":
            return RETRY_HINTS[key]
    return None


def _score(result: "ParseResult") -> tuple:
    """Lower is better: prefer LLM over legacy, then fewer issues."""
    return (result.source == "legacy", len(result.issues))


def _validate_llm_result(row: CourseRow, item: Dict[str, Any], model: str) -> ParseResult:
    self_code = normalize_code(row.code)
    p_text = normalize_text(row.prereq)
    c_text = normalize_text(row.coreq)
    prereq_tree, _ = validate_tree(
        item.get("prereq"), source_text=p_text, self_code=self_code
    )
    # The coreq text is generated by the scraper and its courses usually also
    # appear in the prereq text, so allow codes from either.
    coreq_tree, _ = validate_tree(
        item.get("coreq"),
        source_text=c_text,
        self_code=self_code,
        extra_allowed=codes_in_text(p_text),
    )
    prereq_tree, coreq_tree, issues = finalize(
        prereq_tree, coreq_tree, prereq_text=p_text, coreq_text=c_text, self_code=self_code
    )
    return ParseResult(
        row, prereq_tree, coreq_tree, "llm", "low" if issues else "high", issues, model
    )


def parse_batch_llm(client: Any, rows: List[CourseRow]) -> List[ParseResult]:
    """Parse a batch, then retry once (with a hint) the courses the lint or
    validation flagged as structurally misread."""
    first = _request_llm(client, rows)
    retry_rows = [r.row for r in first if _retry_hint(r)]
    if not retry_rows:
        return first
    hints = {normalize_code(r.row.code) or "": h for r in first if (h := _retry_hint(r))}
    logger.info("retrying %d courses with hints", len(retry_rows))
    second = {normalize_code(r.row.code): r for r in _request_llm(client, retry_rows, hints)}
    out = []
    for r in first:
        again = second.get(normalize_code(r.row.code))
        out.append(again if again is not None and _score(again) < _score(r) else r)
    return out


def _request_llm(
    client: Any, rows: List[CourseRow], hints: Optional[Dict[str, str]] = None
) -> List[ParseResult]:
    """One request per batch. Truncation splits the batch; other failures fall back."""
    from pipelines.professors.hierarchical_summarization.ai_clients import (
        TamuAIError,
        TamuAIQuotaExceeded,
        TamuAITruncated,
    )

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": _llm_input(rows, hints)},
    ]
    try:
        body = client._post(
            messages,
            max_tokens=BATCH_MAX_TOKENS,
            response_format={"type": "json_object"},
        )
    except TamuAITruncated:
        if len(rows) == 1:
            return [fallback(rows[0], "llm_truncated")]
        mid = len(rows) // 2
        return _request_llm(client, rows[:mid], hints) + _request_llm(client, rows[mid:], hints)
    except TamuAIQuotaExceeded:
        raise
    except TamuAIError as exc:
        logger.warning("LLM batch failed (%d courses): %s", len(rows), exc)
        return [fallback(r, "llm_error") for r in rows]

    by_code: Dict[str, Dict[str, Any]] = {}
    for item in body.get("results") or []:
        if isinstance(item, dict):
            code = normalize_code(str(item.get("code", "")))
            if code:
                by_code[code] = item

    out: List[ParseResult] = []
    for row in rows:
        item = by_code.get(normalize_code(row.code) or "")
        if item is None:
            out.append(fallback(row, "llm_missing_result"))
            continue
        try:
            out.append(_validate_llm_result(row, item, client.model))
        except ValueError as exc:
            logger.info("%s: LLM tree rejected: %s", row.code, exc)
            out.append(fallback(row, f"llm_rejected:{str(exc)[:120]}"))
    return out


def parse_rows(rows: List[CourseRow], *, use_llm: bool, workers: int) -> List[ParseResult]:
    results: List[ParseResult] = []
    pending: List[CourseRow] = []
    for row in rows:
        if not normalize_text(row.prereq) and not normalize_text(row.coreq):
            results.append(ParseResult(row, None, None, "rules", "high"))
            continue
        r = try_rules(row)
        if r:
            results.append(r)
        else:
            pending.append(row)
    logger.info("rules: %d parsed, %d need the LLM", len(results), len(pending))

    if not pending:
        return results

    client = None
    if use_llm:
        from pipelines.professors.hierarchical_summarization.ai_clients import (
            TamuAIChatClient,
        )

        client = TamuAIChatClient.from_env()
        if client is None:
            logger.warning("TAMU_AI_* not configured; using legacy groups for the rest")

    if client is None:
        return results + [fallback(r, "llm_unavailable") for r in pending]

    from pipelines.professors.hierarchical_summarization.ai_clients import (
        TamuAIQuotaExceeded,
    )

    batches = [pending[i : i + BATCH_SIZE] for i in range(0, len(pending), BATCH_SIZE)]
    done = 0
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [pool.submit(parse_batch_llm, client, b) for b in batches]
        for fut, batch in zip(futures, batches):
            try:
                results.extend(fut.result())
            except TamuAIQuotaExceeded:
                # Leave these unparsed (no write) so the next run picks them up.
                logger.error("TAMU quota reached; %d courses left for the next run", len(batch))
            done += 1
            if done % 10 == 0:
                logger.info("LLM batches: %d/%d", done, len(batches))
    return results


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------


def load_rows_from_db(session: Any, dept: Optional[str], force: bool, limit: Optional[int]) -> List[CourseRow]:
    from sqlalchemy import text

    sql = """
        SELECT id, code, prerequisites, corequisites, prerequisite_groups,
               prereq_parse->>'hash' AS old_hash
        FROM courses
        WHERE (prerequisites IS NOT NULL OR corequisites IS NOT NULL
               OR prereq_tree IS NOT NULL OR coreq_tree IS NOT NULL)
    """
    params: Dict[str, Any] = {}
    if dept:
        sql += " AND subject_id = :dept"
        params["dept"] = dept.upper()
    sql += " ORDER BY code"
    rows = [
        CourseRow(r.id, r.code, r.prerequisites, r.corequisites, r.prerequisite_groups, r.old_hash)
        for r in session.execute(text(sql), params)
    ]
    if not force:
        rows = [r for r in rows if r.old_hash != text_hash(r.prereq, r.coreq)]
    return rows[:limit] if limit else rows


def write_results(session: Any, results: List[ParseResult]) -> None:
    from sqlalchemy import text

    stmt = text(
        """
        UPDATE courses
        SET prereq_tree = CAST(:prereq_tree AS JSONB),
            coreq_tree = CAST(:coreq_tree AS JSONB),
            prereq_parse = CAST(:meta AS JSONB)
        WHERE id = :id
        """
    )
    for i in range(0, len(results), 200):
        chunk = results[i : i + 200]
        session.execute(
            stmt,
            [
                {
                    "id": r.row.id,
                    "prereq_tree": json.dumps(r.prereq_tree) if r.prereq_tree else None,
                    "coreq_tree": json.dumps(r.coreq_tree) if r.coreq_tree else None,
                    "meta": json.dumps(r.meta()),
                }
                for r in chunk
            ],
        )
        session.commit()


def apply_results_file(session: Any, path: str) -> Dict[str, int]:
    """Write a reviewed --dry-run --out file to the DB without re-parsing.

    A course is written only if its current text still hashes to the value the
    file was parsed from; changed courses are left for the next normal run.
    """
    from sqlalchemy import text

    entries = json.loads(Path(path).read_text(encoding="utf-8"))
    by_code = {e["code"]: e for e in entries}
    db_rows = session.execute(
        text(
            "SELECT id, code, prerequisites, corequisites FROM courses WHERE code = ANY(:codes)"
        ),
        {"codes": list(by_code)},
    ).fetchall()
    stats = {"written": 0, "stale": 0, "not_in_db": len(by_code) - len(db_rows)}
    results: List[ParseResult] = []
    for r in db_rows:
        e = by_code[r.code]
        if e.get("parser_version") != PARSER_VERSION or e.get("hash") != text_hash(r.prerequisites, r.corequisites):
            stats["stale"] += 1
            continue
        row = CourseRow(r.id, r.code, r.prerequisites, r.corequisites)
        results.append(
            ParseResult(row, e["prereq_tree"], e["coreq_tree"], e["source"], e["confidence"], e.get("issues") or [], e.get("model"))
        )
    write_results(session, results)
    stats["written"] = len(results)
    return stats


def load_rows_from_json(path: str) -> List[CourseRow]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return [
        CourseRow(
            None,
            d["code"],
            d.get("prereq"),
            d.get("coreq"),
            json.dumps(d["groups"]) if d.get("groups") is not None else None,
        )
        for d in data
    ]


def format_tree(n: Optional[Dict[str, Any]]) -> str:
    if n is None:
        return "-"
    t = n["type"]
    if t == "course":
        grade = f"[{n['min_grade']}]" if n.get("min_grade") else ""
        return f"{n['code']}{grade}{'~' if n.get('concurrent') else ''}"
    if t in ("and", "or"):
        return "(" + f" {t.upper()} ".join(format_tree(c) for c in n["children"]) + ")"
    if t == "classification":
        return "/".join(n["levels"])
    if t == "approval":
        return f"approval:{n['by']}"
    return f"<{n['kind']}>"


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dept", help="Only this subject (e.g. CSCE)")
    ap.add_argument("--limit", type=int, help="Max courses to parse")
    ap.add_argument("--force", action="store_true", help="Re-parse even if text is unchanged")
    ap.add_argument("--no-llm", action="store_true", help="Rules + legacy fallback only")
    ap.add_argument("--dry-run", action="store_true", help="Print trees, write nothing")
    ap.add_argument("--workers", type=int, default=4, help="Concurrent LLM batches")
    ap.add_argument("--from-json", help="Offline input file instead of the DB (implies --dry-run)")
    ap.add_argument("--out", help="With --dry-run, also write results as JSON here")
    ap.add_argument("--apply", metavar="FILE", help="Write a reviewed --out file to the DB (no parsing)")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    if args.apply:
        from aggiermp.database.base import get_session

        session = get_session()
        try:
            logger.info("apply: %s", apply_results_file(session, args.apply))
        finally:
            session.close()
        return 0

    session = None
    if args.from_json:
        rows = load_rows_from_json(args.from_json)[: args.limit or None]
        args.dry_run = True
    else:
        from aggiermp.database.base import get_session

        session = get_session()
        rows = load_rows_from_db(session, args.dept, args.force, args.limit)
    logger.info("%d courses to parse", len(rows))

    results = parse_rows(rows, use_llm=not args.no_llm, workers=args.workers)

    counts: Dict[str, int] = {}
    for r in results:
        key = f"{r.source}/{r.confidence}"
        counts[key] = counts.get(key, 0) + 1
    logger.info("results: %s", counts)

    if args.dry_run:
        # Save first: printing catalog text to a Windows console can still
        # fail, and the LLM requests behind these results aren't free.
        if args.out:
            Path(args.out).write_text(
                json.dumps(
                    [{"code": r.row.code, "prereq_tree": r.prereq_tree, "coreq_tree": r.coreq_tree, **r.meta()} for r in results],
                    indent=1,
                ),
                encoding="utf-8",
            )
            logger.info("results saved to %s", args.out)
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        for r in results:
            if r.source != "rules":
                print(f"{r.row.code} [{r.source}/{r.confidence}] {normalize_text(r.row.prereq)}")
                print(f"    prereq => {format_tree(r.prereq_tree)}")
                if r.coreq_tree or r.row.coreq:
                    print(f"    coreq  => {format_tree(r.coreq_tree)}   (text: {normalize_text(r.row.coreq)})")
                if r.issues:
                    print(f"    issues: {r.issues}")
    else:
        write_results(session, results)
        logger.info("wrote %d rows", len(results))
    if session is not None:
        session.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
