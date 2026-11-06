"""
Prerequisite text -> AND/OR tree.

Node shapes (stored as JSONB in courses.prereq_tree / courses.coreq_tree):

    {"type": "and" | "or", "children": [node, ...]}
    {"type": "course", "code": "CSCE 221", "min_grade": "C" | null,
     "concurrent": bool}            # concurrent: may be taken the same term
    {"type": "classification", "levels": ["junior", "senior"]}
    {"type": "approval", "by": "instructor" | "department" | "advisor" | "other"}
    {"type": "condition", "kind": "major" | "admission" | "placement" |
     "equivalent" | "credits" | "gpa" | "other", "text": "..."}

A missing tree (None) means no requirement. Planner code decides how to treat
approval/condition leaves; the parser only records them.

Simple catalog phrasings are parsed by `parse_with_rules`. Everything else goes
to the LLM (see parse_prereqs.py), whose output is checked by `validate_tree`
against the course codes in the source text.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Iterable, List, Literal, Optional, Set, Tuple, Union

from pydantic import BaseModel, Field, ValidationError

# Bump when rules, prompt, or validation change so reruns re-parse every row.
PARSER_VERSION = 1

CLASS_LEVELS = ("freshman", "sophomore", "junior", "senior", "graduate")
GRADES = ("A", "B", "C", "D")

# "CSCE 221", "CSCE221", "MATH 151H" -> captures dept + number.
_CODE_RE = re.compile(r"\b([A-Z]{3,4})\s*(\d{3}[A-Z]?)\b")
# One code or a cross-listed pair/chain: "CSCE 350/ECEN 350".
_CODE_GROUP_RE = re.compile(
    r"[A-Z]{3,4}\s*\d{3}[A-Z]?(?:\s*/\s*[A-Z]{3,4}\s*\d{3}[A-Z]?)*"
)


# ---------------------------------------------------------------------------
# Text and code normalization
# ---------------------------------------------------------------------------


def normalize_text(text: Optional[str]) -> str:
    """Fix scraper mojibake and spacing so codes and keywords match reliably.

    The catalog uses non-breaking spaces between dept and number; some rows
    were stored double-encoded ("\\u00c2\\xa0"), and some arrive as U+FFFD.
    """
    if not text:
        return ""
    s = text.replace("\u00c2\u00a0", " ").replace("\u00a0", " ")
    s = s.replace("\ufffd", " ").replace("\u00c2", " ")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\s+([,;.])", r"\1", s)  # "CSCE 221 ," -> "CSCE 221,"
    s = re.sub(r";\s*or\b", ", or", s, flags=re.I)  # "A; or B" continues the clause
    s = _expand_shorthand_numbers(s)
    s = _collapse_mirrored_crosslists(s)
    return s.strip()


_MIRRORED_RE = re.compile(
    r"(,\s*)?\b([A-Z]{3,4} \d{3}[A-Z]?)/([A-Z]{3,4} \d{3}[A-Z]?),? or \3/\2\b"
)


# A bare course number not followed by "-level", "credits", etc.
_BARE_NUM = r"\d{3}[A-Z]?\b(?!-|\s*(?:credits?|hours?|level|or above|or higher))"
_SHORTHAND_RE = re.compile(
    rf"\b([A-Z]{{3,4}}) (\d{{3}}[A-Z]?)((?:\s*(?:,\s*or|,\s*and|,|or|and|/)\s*{_BARE_NUM})+)"
)


def _expand_shorthand_numbers(s: str) -> str:
    """'GEOL 101 and 102' -> 'GEOL 101 and GEOL 102'; 'STAT 301, 302, or 303'
    -> 'STAT 301, STAT 302, or STAT 303'. The catalog drops the subject for
    later courses in a list."""

    def expand(m: "re.Match[str]") -> str:
        dept = m.group(1)
        tail = re.sub(rf"(?<![A-Z] )\b({_BARE_NUM})", lambda n: f"{dept} {n.group(1)}", m.group(3))
        return f"{dept} {m.group(2)}{tail}"

    return _SHORTHAND_RE.sub(expand, s)


def _collapse_mirrored_crosslists(s: str) -> str:
    """'CSCE 350/ECEN 350, or ECEN 350/CSCE 350' -> 'CSCE 350/ECEN 350'.

    The catalog lists each cross-listed course under both subjects; the
    repetition reads like two alternatives and confuses the LLM.
    """
    # In "X, A/B, or B/A" the "or" is the list's connector: keep it as
    # "X, or A/B" so the list still reads as alternatives.
    return _MIRRORED_RE.sub(
        lambda m: f"{', or ' if m.group(1) else ''}{m.group(2)}/{m.group(3)}", s
    )


def normalize_code(code: str) -> Optional[str]:
    m = _CODE_RE.search(normalize_text(code).upper())
    if not m:
        return None
    return f"{m.group(1)} {m.group(2)}"


def codes_in_text(text: str) -> List[str]:
    """Course codes in order of first appearance, de-duplicated."""
    seen: Dict[str, None] = {}
    for m in _CODE_RE.finditer(normalize_text(text)):
        seen.setdefault(f"{m.group(1)} {m.group(2)}", None)
    return list(seen)


def text_hash(prereq_text: Optional[str], coreq_text: Optional[str]) -> str:
    payload = f"v{PARSER_VERSION}\x1f{normalize_text(prereq_text)}\x1f{normalize_text(coreq_text)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


# ---------------------------------------------------------------------------
# Tree models (used to validate LLM output)
# ---------------------------------------------------------------------------


class CourseNode(BaseModel):
    type: Literal["course"]
    code: str
    min_grade: Optional[Literal["A", "B", "C", "D"]] = None
    concurrent: bool = False


class ClassificationNode(BaseModel):
    type: Literal["classification"]
    levels: List[Literal["freshman", "sophomore", "junior", "senior", "graduate"]] = Field(
        min_length=1
    )


class ApprovalNode(BaseModel):
    type: Literal["approval"]
    by: Literal["instructor", "department", "advisor", "other"] = "instructor"


class ConditionNode(BaseModel):
    type: Literal["condition"]
    kind: Literal[
        "major", "admission", "placement", "equivalent", "credits", "gpa", "other"
    ] = "other"
    text: str = ""


class GroupNode(BaseModel):
    type: Literal["and", "or"]
    children: List["Node"] = Field(min_length=1)


Node = Union[GroupNode, CourseNode, ClassificationNode, ApprovalNode, ConditionNode]
GroupNode.model_rebuild()


class TreeNode(BaseModel):
    """Wrapper so a single node can be validated with the discriminated union."""

    root: Node = Field(discriminator="type")


def _parse_node(raw: Any) -> Node:
    return TreeNode.model_validate({"root": raw}).root


# ---------------------------------------------------------------------------
# Tree helpers
# ---------------------------------------------------------------------------


def course(code: str, min_grade: Optional[str] = None, concurrent: bool = False) -> Dict[str, Any]:
    return {"type": "course", "code": code, "min_grade": min_grade, "concurrent": concurrent}


def group(op: str, children: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {"type": op, "children": children}


def tree_codes(tree: Optional[Dict[str, Any]]) -> Set[str]:
    out: Set[str] = set()

    def walk(n: Dict[str, Any]) -> None:
        if n["type"] == "course":
            out.add(n["code"])
        for c in n.get("children", []):
            walk(c)

    if tree:
        walk(tree)
    return out


def simplify(tree: Optional[Dict[str, Any]], self_code: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Normalize codes, drop self-references, flatten and de-duplicate groups."""
    if tree is None:
        return None
    t = tree["type"]
    if t == "course":
        code = normalize_code(tree.get("code", ""))
        if not code or code == self_code:
            return None
        grade = tree.get("min_grade")
        return course(code, grade if grade in GRADES else None, bool(tree.get("concurrent")))
    if t == "classification":
        levels = [lv for lv in CLASS_LEVELS if lv in set(tree.get("levels") or [])]
        return {"type": "classification", "levels": levels} if levels else None
    if t == "approval":
        return {"type": "approval", "by": tree.get("by") or "instructor"}
    if t == "condition":
        return {"type": "condition", "kind": tree.get("kind") or "other", "text": (tree.get("text") or "").strip()}
    if t in ("and", "or"):
        kids: List[Dict[str, Any]] = []
        seen: Set[str] = set()
        by_code: Dict[str, Dict[str, Any]] = {}
        for child in tree.get("children") or []:
            s = simplify(child, self_code)
            if s is None:
                continue
            parts = s["children"] if s["type"] == t else [s]
            for p in parts:
                if p["type"] == "course" and p["code"] in by_code:
                    # "A or A-concurrently" -> one node allowing both; under
                    # AND keep the stricter requirement.
                    prev = by_code[p["code"]]
                    if t == "or":
                        prev["concurrent"] = prev["concurrent"] or p["concurrent"]
                    else:
                        prev["concurrent"] = prev["concurrent"] and p["concurrent"]
                    prev["min_grade"] = _stricter_grade(prev["min_grade"], p["min_grade"])
                    continue
                key = _canonical(p)
                if key in seen:
                    continue
                seen.add(key)
                if p["type"] == "course":
                    p = dict(p)
                    by_code[p["code"]] = p
                kids.append(p)
        if not kids:
            return None
        if len(kids) == 1:
            return kids[0]
        return group(t, kids)
    raise ValueError(f"unknown node type {t!r}")


def _canonical(n: Dict[str, Any]) -> str:
    """Order-insensitive key: (A or B) == (B or A)."""
    if n["type"] in ("and", "or"):
        return n["type"] + "(" + ",".join(sorted(_canonical(c) for c in n["children"])) + ")"
    return json.dumps(n, sort_keys=True)


def _stricter_grade(a: Optional[str], b: Optional[str]) -> Optional[str]:
    if not a or not b:
        return a or b
    return min(a, b)  # "A" sorts before "C" and is the stricter grade


def _iter_courses(tree: Optional[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    if not tree:
        return
    if tree["type"] == "course":
        yield tree
    for child in tree.get("children", []):
        yield from _iter_courses(child)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

_SOFT_MENTION_RE = re.compile(r"\b(suggested|recommended|encouraged|preferred)\b", re.I)


# Codes *after* these words aren't requirements: "must be taken prior to
# AGSM 440", "Replaces CHEM 323 in previous catalogs".
_NOT_REQUIRED_AFTER_RE = re.compile(
    r"\b(?:taken (?:immediately )?prior to|semester (?:immediately )?prior to|"
    r"replaces|formerly|previous catalogs?)\b",
    re.I,
)


def _soft_mention_codes(text: str) -> Set[str]:
    """Codes the text mentions without requiring them: everything in a clause
    like 'X is suggested', and codes after 'must be taken prior to'."""
    out: Set[str] = set()
    for clause in re.split(r"[;.]", text):
        if _SOFT_MENTION_RE.search(clause):
            out.update(codes_in_text(clause))
            continue
        m = _NOT_REQUIRED_AFTER_RE.search(clause)
        if m:
            out.update(codes_in_text(clause[m.end():]))
    return out


def expand_crosslists(tree: Optional[Dict[str, Any]], text: str) -> Optional[Dict[str, Any]]:
    """Replace a lone half of an 'A/B' cross-listing with (A or B).

    Both halves are the same course, so a tree that names only one, or ANDs
    them together, is corrected deterministically rather than flagged.
    """
    if tree is None:
        return None
    # Alternatives in catalog order: "MSEN 222/MEEN 222" stays MSEN, MEEN.
    alts: Dict[str, List[str]] = {}
    for m in _CODE_GROUP_RE.finditer(normalize_text(text)):
        codes = [c for c in (normalize_code(p) for p in m.group(0).split("/")) if c]
        if len(codes) > 1:
            for code in codes:
                merged = alts.setdefault(code, [])
                merged.extend(c for c in codes if c not in merged)

    def walk(n: Dict[str, Any]) -> Dict[str, Any]:
        if n["type"] == "course" and n["code"] in alts:
            return group("or", [{**n, "code": c} for c in alts[n["code"]]])
        if n["type"] in ("and", "or"):
            return group(n["type"], [walk(c) for c in n["children"]])
        return n

    return simplify(walk(tree)) if alts else tree


_CONDITION_KINDS = {"major", "admission", "placement", "equivalent", "credits", "gpa", "other"}
_APPROVAL_BY = {"instructor", "department", "advisor", "other"}


def coerce_llm_node(n: Any) -> Any:
    """Fix harmless LLM schema slips (unknown condition kind, approval written
    as a condition, odd grade strings) so they don't cost a fallback. Shape
    errors that change meaning (non-object children, unknown node types) are
    left for validation to reject."""
    if not isinstance(n, dict):
        return n
    n = dict(n)
    t = n.get("type")
    if t in ("and", "or") and isinstance(n.get("children"), list):
        n["children"] = [coerce_llm_node(c) for c in n["children"]]
    elif t == "condition":
        kind = str(n.get("kind") or "other").lower()
        if kind in ("approval", "permission", "consent"):
            return {"type": "approval", "by": "other"}
        if kind == "classification":
            levels = [lv for lv in CLASS_LEVELS if lv in str(n.get("text", "")).lower()]
            if levels:
                return {"type": "classification", "levels": levels}
        n["kind"] = kind if kind in _CONDITION_KINDS else "other"
        n["text"] = str(n.get("text") or "")
    elif t == "approval":
        by = str(n.get("by") or "other").lower()
        n["by"] = by if by in _APPROVAL_BY else ("department" if "department" in by else "other")
    elif t == "classification" and isinstance(n.get("levels"), list):
        n["levels"] = [str(lv).lower() for lv in n["levels"] if str(lv).lower() in CLASS_LEVELS]
    elif t == "course":
        grade = str(n.get("min_grade") or "").strip().upper()[:1]
        n["min_grade"] = grade if grade in GRADES else None
        n["concurrent"] = bool(n.get("concurrent"))
    return n


def validate_tree(
    raw: Any,
    *,
    source_text: str,
    self_code: Optional[str],
    extra_allowed: Iterable[str] = (),
) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """Check an LLM tree against its source text.

    Returns (tree, issues). Raises ValueError when the tree is unusable: bad
    shape, or it names a course the source text never mentions (hallucination).
    Issues are recoverable problems that make the result low-confidence.
    """
    if raw is None:
        tree = None
    else:
        raw = coerce_llm_node(raw)
        try:
            _parse_node(raw)
        except ValidationError as exc:
            raise ValueError(f"tree failed schema validation: {exc.errors()[:2]}") from exc
        tree = simplify(raw, self_code)

    allowed = set(codes_in_text(source_text)) | set(extra_allowed)
    got = tree_codes(tree)
    invented = got - allowed
    if invented:
        raise ValueError(f"tree names courses not in text: {sorted(invented)}")

    return tree, []


def _drop_codes(tree: Optional[Dict[str, Any]], codes: Set[str]) -> Optional[Dict[str, Any]]:
    if tree is None or not codes:
        return tree
    if tree["type"] == "course":
        return None if tree["code"] in codes else tree
    if tree["type"] in ("and", "or"):
        kids = [k for k in (_drop_codes(c, codes) for c in tree["children"]) if k]
        return simplify(group(tree["type"], kids)) if kids else None
    return tree


def _segments(text: str) -> List[str]:
    return [s.strip() for s in re.split(r"\s*;\s*", text) if s.strip()]


# Phrases whose "or"/"and" don't join courses.
_NOT_CONNECTORS_RE = re.compile(
    r"\b(?:or better|or concurrent enrollment|or registration therein|or equivalent|"
    r"or approval of [a-z ]+|(?:freshman|sophomore|junior|senior)(?:,? (?:or|and) "
    r"(?:freshman|sophomore|junior|senior))+ classification)\b",
    re.I,
)


def _segment_connectors(seg: str) -> Set[str]:
    cleaned = _NOT_CONNECTORS_RE.sub(" ", seg)
    return {w for w in ("and", "or") if re.search(rf"\b{w}\b", cleaned, re.I)}


def crosslisted_pairs(text: str) -> Set[frozenset]:
    """{A, B} for every "A/B" in the text (the same course under two subjects)."""
    pairs: Set[frozenset] = set()
    for m in _CODE_GROUP_RE.finditer(normalize_text(text)):
        alts = [normalize_code(p) for p in m.group(0).split("/")]
        alts = [a for a in alts if a]
        for i, a in enumerate(alts):
            for b in alts[i + 1:]:
                pairs.add(frozenset((a, b)))
    return pairs


def lint_tree(tree: Optional[Dict[str, Any]], prereq_text: str) -> List[str]:
    """Heuristic checks of a tree against the clause wording.

    Each leaf is attributed to the ";"-clauses it could come from (a course by
    its code, a classification/approval node by that word). When two siblings
    both come from only the same clause, the group joining them must match the
    clause's connector: an AND inside an or-only clause, or an OR inside an
    and-only clause, is flagged. Also flags ANDs of cross-listed equivalents
    and long clauses that mix and/or, which the catalog phrases ambiguously.
    Findings make a parse low-confidence; they don't reject it.
    """
    issues: Set[str] = set()
    segs = _segments(prereq_text)
    seg_codes = [set(codes_in_text(s)) for s in segs]
    seg_conns = [_segment_connectors(s) for s in segs]
    pairs = crosslisted_pairs(prereq_text)

    def leaf_segments(n: Dict[str, Any]) -> Set[int]:
        if n["type"] == "course":
            return {i for i, codes in enumerate(seg_codes) if n["code"] in codes}
        if n["type"] in ("classification", "approval"):
            word = "classification" if n["type"] == "classification" else "approval"
            return {i for i, s in enumerate(segs) if word in s.lower()}
        if n["type"] in ("and", "or"):
            out: Set[int] = set()
            for c in n["children"]:
                out |= leaf_segments(c)
            return out
        return set()

    def walk(n: Dict[str, Any]) -> None:
        if n["type"] not in ("and", "or"):
            return
        kids = n["children"]
        kid_segs = [leaf_segments(c) for c in kids]
        kid_codes = [tree_codes(c) for c in kids]
        for i in range(len(kids)):
            for j in range(i + 1, len(kids)):
                if n["type"] == "and" and any(
                    frozenset((a, b)) in pairs for a in kid_codes[i] for b in kid_codes[j]
                ):
                    issues.add("and_of_crosslisted")
                if len(kid_segs[i]) != 1 or kid_segs[i] != kid_segs[j]:
                    continue
                conns = seg_conns[next(iter(kid_segs[i]))]
                if n["type"] == "and" and conns == {"or"}:
                    issues.add("and_in_or_clause")
                if n["type"] == "or" and conns == {"and"}:
                    # "or approval of ..." is stripped before reading the
                    # clause's connectors, so an approval sibling is expected.
                    if kids[i]["type"] == "approval" or kids[j]["type"] == "approval":
                        continue
                    # "A/B" pairs are joined by "/", not by the clause's "and".
                    if not any(
                        frozenset((a, b)) in pairs for a in kid_codes[i] for b in kid_codes[j]
                    ):
                        issues.add("or_in_and_clause")
        for c in kids:
            walk(c)

    if tree:
        walk(tree)
    for seg, conns in zip(segs, seg_conns):
        # Count "A/B" cross-listings once: they're one course.
        if conns == {"and", "or"} and len(_CODE_GROUP_RE.findall(seg)) >= 4:
            issues.add("ambiguous_clause")
    return sorted(issues)


# Issues worth one LLM retry (the model misread or dropped part of the text).
RETRYABLE_ISSUES = (
    "and_in_or_clause",
    "or_in_and_clause",
    "and_of_crosslisted",
    "missing_codes",
)


def _mark_concurrent(tree: Dict[str, Any], codes: Optional[Set[str]] = None) -> Dict[str, Any]:
    """Set concurrent=True on course nodes (only those in `codes`, if given)."""
    if tree["type"] == "course":
        if codes is None or tree["code"] in codes:
            return {**tree, "concurrent": True}
        return tree
    if tree["type"] in ("and", "or"):
        return group(tree["type"], [_mark_concurrent(c, codes) for c in tree["children"]])
    return tree


def finalize(
    prereq_tree: Optional[Dict[str, Any]],
    coreq_tree: Optional[Dict[str, Any]],
    *,
    prereq_text: str,
    coreq_text: str,
    self_code: Optional[str],
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], List[str]]:
    """Shared clean-up for rule and LLM trees, plus coverage/lint issues.

    The scraper writes "Concurrent enrollment in X" into the coreq text both
    for "X or concurrent enrollment" (optional) and for a required "concurrent
    enrollment in X". Either way X may be taken in the same term, so a coreq
    course that is already in the prereq tree becomes concurrent=True there
    and leaves the coreq tree. Coreq courses the text only suggests are
    dropped. Remaining coreq nodes mean "taken before or in the same term".
    """
    prereq_tree = expand_crosslists(simplify(prereq_tree, self_code), prereq_text)
    coreq_tree = expand_crosslists(simplify(coreq_tree, self_code), f"{prereq_text}; {coreq_text}")
    soft = _soft_mention_codes(prereq_text)
    # Suggested/"prior to" courses aren't requirements even if a parser kept them.
    prereq_tree = _drop_codes(prereq_tree, soft)
    in_prereq = tree_codes(prereq_tree) & tree_codes(coreq_tree)
    if in_prereq and prereq_tree:
        prereq_tree = simplify(_mark_concurrent(prereq_tree, in_prereq), self_code)
    coreq_tree = _drop_codes(coreq_tree, in_prereq | soft)
    if coreq_tree:
        coreq_tree = _mark_concurrent(coreq_tree)

    issues = lint_tree(prereq_tree, prereq_text)
    covered = tree_codes(prereq_tree) | tree_codes(coreq_tree)
    expected = (set(codes_in_text(prereq_text)) | set(codes_in_text(coreq_text))) - {self_code} - soft
    missing = expected - covered
    if missing:
        issues.append(f"missing_codes:{','.join(sorted(missing))}")
    return prereq_tree, coreq_tree, issues


# ---------------------------------------------------------------------------
# Rule-based parser for simple phrasings
# ---------------------------------------------------------------------------

_LEVEL_ALT = "|".join(CLASS_LEVELS)
_CLASSIFICATION_RE = re.compile(
    rf"^((?:{_LEVEL_ALT})(?:\s*(?:,|or|and|,\s*or|/)\s*(?:{_LEVEL_ALT}))*)\s+classification$",
    re.I,
)
_APPROVAL_RE = re.compile(
    r"^approval of (?:the )?(instructor|department head|department|advisor)$", re.I
)
_GRADE_PREFIX_RE = re.compile(r"^(?:a )?grade of ([A-D]) or better in\s+", re.I)
_GRADE_SUFFIX_RE = re.compile(r"\s+with a grade of ([A-D]) or better$", re.I)
_OR_CONCURRENT_RE = re.compile(
    r",?\s+or (?:concurrent enrollment|registration therein)$", re.I
)
_OR_APPROVAL_RE = re.compile(
    r",?\s+or (approval of (?:the )?(?:instructor|department head|department|advisor))$",
    re.I,
)
_OR_EQUIVALENT_RE = re.compile(r",?\s+or equivalent$", re.I)
_OR_PLACEMENT_RE = re.compile(r",?\s+or (placement by exam)$", re.I)
_CONCURRENT_IN_RE = re.compile(r"^(?:concurrent enrollment in|corequisites?:?)\s+", re.I)


_SUGGESTION_CLAUSE_RE = re.compile(
    r"^[^.]*?\s(?:is |are )?(?:strongly |highly )?(?:suggested|recommended|encouraged|preferred)\.?$",
    re.I,
)


class RuleParseFailed(Exception):
    """Text is not a phrasing the rules understand; send it to the LLM."""


def _approval_node(match_text: str) -> Dict[str, Any]:
    who = match_text.lower()
    by = "department" if "department" in who else "advisor" if "advisor" in who else "instructor"
    return {"type": "approval", "by": by}


_CONDITION_KEYWORDS = (
    ("placement", r"\bplacement\b"),
    ("gpa", r"\bgpa\b|grade point"),
    ("credits", r"\b(?:credits?|hours?)\b"),
    ("admission", r"\badmi(?:ssion|tted)\b|\bacceptance\b|\baccepted\b"),
    ("major", r"\bmajors?\b|\bminors?\b"),
)


def _keyword_condition(seg: str) -> Dict[str, Any]:
    """Course-free clause -> condition node, unless it needs real parsing."""
    low = seg.lower()
    # "3 credits of literature at 200-level", "6 hours of 300-level SPAN"
    is_credits = re.search(r"\b\d+\s+(?:semester\s+)?(?:credits?|hours?)\b", low)
    if re.search(
        r"classification|approval|permission|consent|concurrent|registration|"
        r"\b(?:freshman|sophomore|junior|senior|graduate)\b",
        low,
    ) or (re.search(r"\d{3}", low) and not is_credits):
        raise RuleParseFailed("course-free clause needs the LLM")
    kind = next((k for k, pat in _CONDITION_KEYWORDS if re.search(pat, low)), "other")
    return {"type": "condition", "kind": kind, "text": seg}


_ROLE_RE = re.compile(
    r"\b(?:instructor|department|head|advisor|director|dean|coordinator|chair|"
    r"office|faculty|committee|professor|delegate)\b",
    re.I,
)
_GENERAL_APPROVAL_RE = re.compile(
    r"^(?:(?:pre-?)?approval|permission|consent) (?:of|by|from) (?:the )?(.+)$|"
    r"^(instructor|department head|departmental|advisor) (?:approval|permission|consent)$",
    re.I,
)
_CLASS_IN_RE = re.compile(rf"^(.*?\bclassification) in (?:an? )?(.+)$", re.I)
# Split course-free clauses only where the next word starts a new requirement,
# so "junior or senior classification" and "majors and minors" stay whole.
_STARTERS = (
    r"(?:approval|pre-approval|permission|consent|admission|admitted|acceptance|"
    r"enrollment|freshman|sophomore|junior|senior|graduate|major|minor|instructor approval)\b"
)
_CODELESS_OR_SPLIT = re.compile(rf",?\s+or\s+(?={_STARTERS})", re.I)
_CODELESS_AND_SPLIT = re.compile(rf",?\s+and\s+(?={_STARTERS})|,\s+(?={_STARTERS})", re.I)


def _approval_list(who: str) -> Dict[str, Any]:
    """'instructor and department head' -> AND of approvals; 'X or Y' -> OR."""
    for op, splitter in (("or", r"\s+or\s+"), ("and", r",?\s+and\s+|,\s+")):
        parts = re.split(splitter, who)
        if len(parts) > 1 and all(_ROLE_RE.search(p) for p in parts):
            return simplify(group(op, [_approval_list(p) for p in parts]))
    low = who.lower()
    if "instructor" in low or "professor" in low or "faculty member" in low:
        by = "instructor"
    elif "department" in low:
        by = "department"
    elif "advisor" in low:
        by = "advisor"
    else:
        by = "other"
    return {"type": "approval", "by": by}


def _codeless_atom(seg: str) -> Dict[str, Any]:
    seg = seg.strip().rstrip(".").strip()
    m = _CLASSIFICATION_RE.match(seg)
    if m:
        levels = [lv for lv in CLASS_LEVELS if re.search(rf"\b{lv}\b", m.group(1), re.I)]
        return {"type": "classification", "levels": levels}
    m = _CLASS_IN_RE.match(seg)
    if m and _CLASSIFICATION_RE.match(m.group(1)):
        # "Senior classification in industrial engineering"
        return group("and", [
            _codeless_atom(m.group(1)),
            {"type": "condition", "kind": "major", "text": m.group(2)},
        ])
    m = _GENERAL_APPROVAL_RE.match(seg)
    if m:
        return _approval_list(m.group(1) or m.group(2))
    return _keyword_condition(seg)


_LEVEL_LIST_RE = re.compile(
    rf"\b(?:{_LEVEL_ALT})(?:\s*(?:,\s*or|,|or|and)\s*(?:{_LEVEL_ALT}))+\b(?=\s+classification)",
    re.I,
)


def _parse_codeless(seg: str) -> Dict[str, Any]:
    """Course-free clause: classification / approval / condition, joined by
    and/or only where a new requirement starts. Anything else -> LLM."""
    # "junior or senior classification" -> "junior/senior classification" so
    # the splitters below don't cut the level list apart at "or senior".
    seg = _LEVEL_LIST_RE.sub(lambda m: "/".join(re.findall(_LEVEL_ALT, m.group(0), re.I)), seg)
    try:
        return _codeless_atom(seg)
    except RuleParseFailed:
        pass
    for op, splitter in (("or", _CODELESS_OR_SPLIT), ("and", _CODELESS_AND_SPLIT)):
        parts = [p for p in splitter.split(seg) if p.strip()]
        if len(parts) > 1:
            return simplify(group(op, [_parse_codeless(p) for p in parts]))
    raise RuleParseFailed("course-free clause needs the LLM")


def _parse_course_list(seg: str, *, concurrent: bool, min_grade: Optional[str]) -> Dict[str, Any]:
    """'A, B, or C' -> or; 'A and B' -> and; a lone code -> course."""
    groups = list(_CODE_GROUP_RE.finditer(seg))
    if not groups:
        raise RuleParseFailed("no course codes")
    residue = _CODE_GROUP_RE.sub(" # ", seg)
    connectors = re.sub(r"[#\s]+", " ", residue.replace(",", " , ")).strip()
    words = {w for w in connectors.split() if w != ","}
    if words - {"or", "and"}:
        raise RuleParseFailed(f"unexpected words: {sorted(words)}")
    if words == {"or", "and"}:
        raise RuleParseFailed("mixed and/or")
    if len(groups) > 1 and not words:
        raise RuleParseFailed("comma-only list is ambiguous")

    items: List[Dict[str, Any]] = []
    for g in groups:
        alts = [normalize_code(p) for p in g.group(0).split("/")]
        nodes = [course(c, min_grade, concurrent) for c in alts if c]
        items.append(nodes[0] if len(nodes) == 1 else group("or", nodes))
    if len(items) == 1:
        return items[0]
    return group("and" if words == {"and"} else "or", items)


def _parse_segment(seg: str) -> Dict[str, Any]:
    seg = seg.strip().rstrip(".").strip()
    if not seg:
        raise RuleParseFailed("empty segment")
    if seg.lower().startswith(("or ", "and ")):
        raise RuleParseFailed("segment continues previous clause")

    m = _CLASSIFICATION_RE.match(seg)
    if m:
        levels = [lv for lv in CLASS_LEVELS if re.search(rf"\b{lv}\b", m.group(1), re.I)]
        return {"type": "classification", "levels": levels}
    m = re.match(rf"^(.*classification)\s+or\s+(approval of .*)$", seg, re.I)
    if m and _CLASSIFICATION_RE.match(m.group(1)) and _APPROVAL_RE.match(m.group(2)):
        cls = _parse_segment(m.group(1))
        return group("or", [cls, _approval_node(m.group(2))])
    m = _APPROVAL_RE.match(seg)
    if m:
        return _approval_node(m.group(1))
    m = _CONCURRENT_IN_RE.match(seg)
    if m:
        # "Concurrent enrollment in X": X taken before or in the same term.
        return _parse_course_list(seg[m.end():].strip(), concurrent=True, min_grade=None)

    if not _CODE_RE.search(seg):
        return _parse_codeless(seg)

    # Course list with optional grade and trailing alternatives.
    extras: List[Dict[str, Any]] = []
    concurrent = False
    changed = True
    while changed:
        changed = False
        m = _OR_APPROVAL_RE.search(seg)
        if m:
            extras.append(_approval_node(m.group(1)))
            seg = seg[: m.start()].strip()
            changed = True
        m = _OR_CONCURRENT_RE.search(seg)
        if m:
            concurrent = True
            seg = seg[: m.start()].strip()
            changed = True
        m = _OR_EQUIVALENT_RE.search(seg)
        if m:
            extras.append({"type": "condition", "kind": "equivalent", "text": "or equivalent"})
            seg = seg[: m.start()].strip()
            changed = True
        m = _OR_PLACEMENT_RE.search(seg)
        if m:
            extras.append({"type": "condition", "kind": "placement", "text": m.group(1)})
            seg = seg[: m.start()].strip()
            changed = True

    min_grade: Optional[str] = None
    m = _GRADE_PREFIX_RE.match(seg)
    if m:
        min_grade = m.group(1).upper()
        seg = seg[m.end():]
    m = _GRADE_SUFFIX_RE.search(seg)
    if m:
        if min_grade:
            raise RuleParseFailed("two grade clauses")
        min_grade = m.group(1).upper()
        seg = seg[: m.start()]

    node = _parse_course_list(seg.strip(), concurrent=concurrent, min_grade=min_grade)
    if extras:
        # "A and B or approval" is ambiguous; only attach alternatives to a
        # single course or an OR list.
        if node["type"] == "and":
            raise RuleParseFailed("alternative after an AND list")
        node = group("or", [node] + extras)
    return node


def parse_with_rules(
    prereq_text: Optional[str], coreq_text: Optional[str], self_code: Optional[str]
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Parse simple phrasings. Raises RuleParseFailed for anything else."""
    p = normalize_text(prereq_text)
    c = normalize_text(coreq_text)

    prereq_tree: Optional[Dict[str, Any]] = None
    if p:
        segments = []
        for s in re.split(r"\s*;\s*", p):
            if not s.strip():
                continue
            if _SOFT_MENTION_RE.search(s):
                # "PBSI 301 and PBSI 302 recommended" isn't a requirement:
                # skip it. A suggestion anywhere else in a clause -> LLM.
                if not _SUGGESTION_CLAUSE_RE.match(s):
                    raise RuleParseFailed("suggestion mixed into a requirement clause")
                continue
            segments.append(s)
        if segments:
            prereq_tree = simplify(group("and", [_parse_segment(s) for s in segments]), self_code)

    coreq_tree: Optional[Dict[str, Any]] = None
    if c:
        m = _CONCURRENT_IN_RE.match(c)
        if not m:
            raise RuleParseFailed("unrecognized coreq text")
        codes = [x for x in codes_in_text(c[m.end():]) if x != self_code]
        if codes:
            coreq_tree = group("and", [course(x, None, True) for x in codes])

    prereq_tree, coreq_tree, issues = finalize(
        prereq_tree, coreq_tree, prereq_text=p, coreq_text=c, self_code=self_code
    )
    if len(tree_codes(coreq_tree)) > 1:
        raise RuleParseFailed("multiple corequisites: and/or unknown")
    if issues:
        raise RuleParseFailed(f"rule tree has issues: {issues}")
    return prereq_tree, coreq_tree


# ---------------------------------------------------------------------------
# Legacy fallback
# ---------------------------------------------------------------------------


def legacy_tree(
    groups_json: Optional[str], prereq_text: Optional[str], self_code: Optional[str]
) -> Optional[Dict[str, Any]]:
    """AND-of-OR tree from the scraper's prerequisite_groups.

    Known to be wrong for "A and B" (stored as one OR group), so this is only
    used when neither rules nor the LLM produce a tree, and is always marked
    low-confidence. Codes the text doesn't mention are dropped.
    """
    if not groups_json:
        return None
    try:
        groups = json.loads(groups_json) if isinstance(groups_json, str) else groups_json
    except (TypeError, ValueError):
        return None
    allowed = set(codes_in_text(prereq_text or ""))
    ands: List[Dict[str, Any]] = []
    for g in groups or []:
        ors = [course(c) for c in (normalize_code(x) for x in g or []) if c and c in allowed]
        if ors:
            ands.append(group("or", ors))
    return simplify(group("and", ands), self_code) if ands else None
