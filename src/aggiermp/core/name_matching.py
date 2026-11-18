"""Utilities for matching professor names across registrar, GPA, and RMP data.

Given names match exactly, by initial, by nickname (Mike/Michael), joined
("Hee Yeon"/"Heeyeon"), or by a close spelling that keeps the last letter
(Abigail/Abigael, but not Yan/Yang or Maria/Mario). Surnames match exactly
(spaces and hyphens aside), on a whole part of a compound surname (capped
below an exact match), or by a close spelling of a long surname
(Wickliff/Wicliff, but not Li/Liu or Johnson/Johnston).
"""

from __future__ import annotations

import functools
import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher


_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "phd", "md"}
_SURNAME_PARTICLES = {
    "de", "del", "della", "der", "di", "du", "la", "le", "van", "von", "den", "ten", "ter",
    "da", "dos", "das", "do", "al", "el", "abu", "bin",
}
# Letters NFKD does not decompose to ASCII.
_TRANSLITERATION = str.maketrans({
    "ø": "o", "Ø": "O", "ß": "ss", "ł": "l", "Ł": "L", "đ": "d", "Đ": "D",
    "ı": "i", "æ": "ae", "Æ": "AE", "œ": "oe", "Œ": "OE",
})

# Formal names and their nicknames. Shared with the duplicate-merge code
# (professor_identity), which treats any two names in one line as related.
_NICKNAME_LINES = (
    "william: bill will billy willy liam",
    "robert: bob rob bobby robbie bert",
    "james: jim jimmy jamie",
    "michael: mike mikey mick",
    "christopher: chris kit",
    "christina christine kristina kristine: chris tina",
    "thomas: tom tommy",
    "richard: rick rich dick ricky",
    "joseph: joe joey",
    "daniel: dan danny",
    "david: dave davey",
    "stephen steven: steve",
    "matthew: matt",
    "andrew: andy drew",
    "anthony: tony",
    "edward: ed eddie ted ned",
    "elizabeth: liz beth betsy lizzy betty eliza",
    "katherine catherine kathryn: kate kathy katie cathy kat",
    "kathleen: kathy kate katie",
    "margaret: maggie peggy meg",
    "patricia: pat patty trish tricia",
    "patrick: pat",
    "samuel: sam sammy",
    "samantha: sam",
    "benjamin: ben benny",
    "alexander: alex al sandy xander",
    "alexandra: alex sandy lexi",
    "nicholas: nick nicky",
    "jonathan: jon",
    "john: jack johnny",
    "timothy: tim timmy",
    "kenneth: ken kenny",
    "ronald: ron ronnie",
    "donald: don donnie",
    "gregory: greg",
    "jeffrey geoffrey: jeff",
    "lawrence: larry",
    "charles: chuck charlie",
    "susan: sue susie",
    "deborah: debbie deb",
    "rebecca: becky becca",
    "jennifer: jen jenny",
    "joshua: josh",
    "zachary: zach zack",
    "frederick: fred freddy",
    "douglas: doug",
    "gerald: jerry",
    "raymond: ray",
    "phillip philip: phil",
    "jacob: jake",
    "jason: jay",
    "peter: pete",
    "sandra: sandy",
    "henry: hank harry",
    "harold: hal harry",
    "albert: al bert",
    "alan allan allen: al",
    "alfred: al alf fred",
    "theodore: ted teddy theo",
    "leonard: leo len lenny",
    "abigail: abby",
    "cynthia: cindy",
    "jessica: jess",
    "victoria: vicky tori",
    "nathaniel nathan: nate",
    "eugene: gene",
    "vincent: vince",
    "walter: walt",
    "terrence terence: terry",
    "randall: randy",
    "mitchell: mitch",
    "russell: russ",
    "stanley: stan",
    "judith: judy",
    "barbara: barb",
    "kimberly: kim",
    "pamela: pam",
    "jacqueline: jackie",
)
NICKNAME_GROUPS: tuple[tuple[frozenset[str], frozenset[str]], ...] = tuple(
    (frozenset(formal.split()), frozenset(nicknames.split()))
    for formal, nicknames in (line.split(":") for line in _NICKNAME_LINES)
)
_ALTERNATES: dict[str, set[str]] = {}
for _formal, _nicknames in NICKNAME_GROUPS:
    for _name in _formal:
        _ALTERNATES.setdefault(_name, set()).update(_nicknames)
    for _name in _nicknames:
        _ALTERNATES.setdefault(_name, set()).update(_formal)


def is_nickname(left: str, right: str) -> bool:
    """One name is a nickname of the other (Bill/William). Two formal names
    (Christina/Christine) or two nicknames (Chris/Tina) are not."""
    return right in _ALTERNATES.get(left, ())


def nickname_alternates(name: str) -> frozenset[str]:
    """Names ``name`` is a nickname or formal form of: "bill" -> william."""
    return frozenset(_ALTERNATES.get(name, ()))


def _normalize_token(value: str) -> str:
    value = value.translate(_TRANSLITERATION)
    ascii_value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z]", "", ascii_value.lower())


def _tokens(value: str) -> tuple[str, ...]:
    return tuple("".join(group) for group in _token_groups(value))


def _token_groups(value: str) -> list[tuple[str, ...]]:
    """Whitespace-separated words, each split into its hyphenated parts.

    Suffixes (Jr., III) are dropped only where they end a name, or just
    before the grade reports' trailing initial ("GATLIN III D"). Elsewhere
    they are names: "J.R. Smith", "MD Ariful Azad".
    """
    result = []
    for raw_token in re.split(r"\s+", value.strip()):
        parts = tuple(p for p in (_normalize_token(x) for x in raw_token.split("-")) if p)
        if parts:
            result.append(parts)
    end = len(result) - 1
    if end >= 1 and len("".join(result[end])) == 1:
        end -= 1
    while end >= 1 and "".join(result[end]) in _SUFFIXES:
        del result[end]
        end -= 1
    return result


def _surname_parts(groups: list[tuple[str, ...]]) -> tuple[str, ...]:
    # A hyphenated surname keeps its parts as separate tokens: the registrar's
    # "Moreira-Tamayo" is "Tamayo" on RMP, and "Dugat-Wickliff" is "Wickliff"
    # in the registrar. Joined, the parts still equal the unhyphenated form.
    return tuple(part for group in groups for part in group)


@dataclass(frozen=True)
class ParsedName:
    given: tuple[str, ...]
    surname: tuple[str, ...]

    @property
    def given_initial(self) -> str:
        return self.given[0][0] if self.given and self.given[0] else ""


def _split(groups: list[tuple[str, ...]], start: int) -> ParsedName:
    return ParsedName(
        tuple("".join(group) for group in groups[:start]), _surname_parts(groups[start:])
    )


@functools.lru_cache(maxsize=1 << 16)
def name_parses(full_name: str) -> tuple[ParsedName, ...]:
    """Plausible readings of a name, the usual one (parse_person_name) first.

    A "First [Middle] Last" name with three or more full words may also have
    a two-word surname without a hyphen: "Alejandro Castillo Ayala" is
    "Castillo" on RMP, so the surname may start one full word earlier
    (keeping at least one given name).
    """
    if not full_name or not full_name.strip():
        return (ParsedName((), ()),)

    if "," in full_name:
        surname_text, given_text = full_name.split(",", 1)
        return (ParsedName(_tokens(given_text), _surname_parts(_token_groups(surname_text))),)

    groups = _token_groups(full_name)
    parts = tuple("".join(group) for group in groups)
    if not parts:
        return (ParsedName((), ()),)
    if len(parts) == 1:
        return (ParsedName((), _surname_parts(groups)),)

    # ANEX/GPA exports use "SURNAME [SURNAME ...] INITIAL".
    if len(parts[-1]) == 1:
        return (ParsedName((parts[-1],), _surname_parts(groups[:-1])),)

    surname_start = len(parts) - 1
    # Keep particles with the surname, while preserving at least one given name.
    while surname_start > 1 and parts[surname_start - 1] in _SURNAME_PARTICLES:
        surname_start -= 1
    parses = [_split(groups, surname_start)]
    full = [
        i for i, part in enumerate(parts) if len(part) > 1 and part not in _SURNAME_PARTICLES
    ]
    earlier = [i for i in full if 1 <= i < surname_start]
    if len(full) >= 3 and earlier:
        parses.append(_split(groups, earlier[-1]))
    return tuple(parses)


def parse_person_name(full_name: str) -> ParsedName:
    """Parse common professor-name formats without discarding compound surnames.

    Supported examples include ``Last, First Middle``, ``First Middle Last``, and
    the GPA export convention ``LAST COMPOUND I`` (surname followed by an initial).
    """
    return name_parses(full_name)[0]


def _transposed(left: str, right: str) -> bool:
    """Two neighbouring letters swapped: Tanya/Tanay."""
    if len(left) != len(right):
        return False
    diffs = [i for i, (a, b) in enumerate(zip(left, right)) if a != b]
    return (
        len(diffs) == 2
        and diffs[1] == diffs[0] + 1
        and left[diffs[0]] == right[diffs[1]]
        and left[diffs[1]] == right[diffs[0]]
    )


def _given_similarity(left: str, right: str) -> float:
    if left == right:
        return 1.0
    if len(left) == 1 or len(right) == 1:
        return 1.0 if left[:1] == right[:1] else 0.0
    if is_nickname(left, right):
        return 0.9
    # A long name's first five or more letters: "Cemal" for "Cemalettin",
    # "Shankar" for "Shankararaman". Shorter starts are often other names
    # ("Jose"/"Joseph"), and so are near-equal lengths ("Marian"/"Marianne").
    short, long_ = sorted((left, right), key=len)
    if len(short) >= 5 and len(long_) - len(short) >= 3 and long_.startswith(short):
        return 0.9
    # The ending is where names become other names: Yan/Yang, Maria/Mario,
    # Daniel/Danielle, Christina/Christine.
    if left[-1] != right[-1] and not _transposed(left, right):
        return 0.0
    return SequenceMatcher(None, left, right).ratio()


_PATRONYMIC_ENDING = re.compile(r"(?:son|sen|ston)$")


def _surname_similarity(left: str, right: str) -> float:
    if left == right:
        return 1.0
    # Close spellings count only for long surnames: Li/Liu, Chen/Cheng,
    # Wang/Wong and Xu/Xue are different families.
    if left[:1] != right[:1] or min(len(left), len(right)) < 6:
        return 0.0
    # Johnson/Johnston and Peterson/Petersen are different names.
    if _PATRONYMIC_ENDING.sub("", left) == _PATRONYMIC_ENDING.sub("", right):
        return 0.0
    ratio = SequenceMatcher(None, left, right).ratio()
    return ratio if ratio >= 0.85 else 0.0


def _joined_given(name: ParsedName) -> str:
    return "".join(token for token in name.given if len(token) > 1)


def _score_parsed(a: ParsedName, b: ParsedName) -> float:
    if not a.surname or not b.surname:
        return 0.0

    a_surname = tuple(token for token in a.surname if token not in _SURNAME_PARTICLES) or a.surname
    b_surname = tuple(token for token in b.surname if token not in _SURNAME_PARTICLES) or b.surname
    # RMP may join a compound surname that the registrar writes with spaces.
    if "".join(a.surname) == "".join(b.surname) or a_surname == b_surname:
        surname_score, primary_surname_matches = 1.0, True
    else:
        if (
            len(a_surname) > 1 and len(b_surname) > 1
            and set(a_surname) - set(b_surname) and set(b_surname) - set(a_surname)
        ):
            # Two compound surnames differing in a part: "Diaz Artiles" and
            # "Diaz Beltran" are different families, unless the whole is a
            # close spelling ("Svajda-Hardy", "Savjda-Hardy").
            best = _surname_similarity("".join(a_surname), "".join(b_surname))
        else:
            best = max(
                (_surname_similarity(x, y) for x in a_surname for y in b_surname),
                default=0.0,
            )
        if best < 0.72:
            return 0.0
        # Only part of a compound surname matched ("Garcia" for "Garcia-Lopez")
        # or the spelling differs: never as good as the whole surname. The
        # primary part still ranks above the others, so "Arroyo Relion"
        # prefers "Arroyo" to an unrelated "Relion".
        surname_score = min(best, 0.78)
        primary_surname_matches = best == 1.0 and a_surname[0] == b_surname[0]

    given_score = max(
        (_given_similarity(x, y) for x in a.given for y in b.given), default=0.0
    )
    # "Po Ju", "Po-Ju" and "Poju" are one given name.
    joined = _joined_given(a)
    if joined and joined == _joined_given(b):
        given_score = 1.0

    # A matching given-name initial plus a matching surname is a strong signal.
    # Only a first-name initial counts: the middle "S." in "Monique S. Cooper"
    # must not make her match "Marcia Cooper".
    has_initial_only = any(
        len(name.given[0]) == 1 for name in (a, b) if name.given
    )
    if (
        has_initial_only
        and a.given_initial
        and b.given_initial
        and a.given_initial == b.given_initial
    ):
        given_score = max(given_score, 0.9)
    elif a.given and b.given and given_score < 0.72:
        return 0.0

    overlap_bonus = 0.08 if primary_surname_matches else 0.0
    return min(1.0, surname_score * 0.72 + given_score * 0.28 + overlap_bonus)


def professor_name_score(left: str, right: str) -> float:
    """Return a conservative 0..1 identity score for two professor names."""
    return max(
        _score_parsed(a, b) for a in name_parses(left) for b in name_parses(right)
    )


def professor_names_match(left: str, right: str, threshold: float = 0.82) -> bool:
    return professor_name_score(left, right) >= threshold


def grade_row_matches(row_name: str, registrar_name: str) -> bool:
    """Whether a grade report row ("LAST I") names this registrar instructor.

    Stricter than professor_name_score: the row's surname must be the
    instructor's whole surname or the last words of their name ("CASTILLO
    AYALA A" for Alejandro Castillo Ayala), and its initial must be their
    first given name's ("SMITH D" is not John D. Smith, "LI W" is not Wei
    Liu). A surname the grade report truncated no longer matches.
    """
    row = parse_person_name(row_name)
    if len(row.given) != 1 or len(row.given[0]) != 1 or not row.surname:
        return False
    person = parse_person_name(registrar_name)
    if not person.surname or row.given[0] != person.given_initial:
        return False
    surname = "".join(row.surname)
    if surname == "".join(person.surname):
        return True
    if "," in registrar_name:
        return False
    words = _tokens(registrar_name)
    return any("".join(words[i:]) == surname for i in range(1, len(words)))
