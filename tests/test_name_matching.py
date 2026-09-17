from aggiermp.core.name_matching import (
    parse_person_name,
    professor_name_score,
    professor_names_match,
)


def test_parses_registrar_last_first_name() -> None:
    parsed = parse_person_name("Arroyo Relion, Jesus")
    assert parsed.given == ("jesus",)
    assert parsed.surname == ("arroyo", "relion")


def test_parses_gpa_surname_then_initial() -> None:
    parsed = parse_person_name("ARROYO RELION J")
    assert parsed.given == ("j",)
    assert parsed.surname == ("arroyo", "relion")


def test_matches_stat_414_name_variants() -> None:
    assert professor_names_match("Arroyo Relion, Jesus", "Jesus Arroyo")
    assert professor_names_match("ARROYO RELION J", "Jesus Arroyo")
    assert professor_name_score("ARROYO RELION J", "Arroyo Relion, Jesus") == 1.0


def test_rejects_same_surname_with_different_first_initial() -> None:
    assert not professor_names_match("Smith, Alice", "Bob Smith")
    assert not professor_names_match("Alice Smith", "Andrew Smith")


def test_prefers_primary_part_of_compound_surname() -> None:
    correct = professor_name_score("Arroyo Relion, Jesus", "Jesus Arroyo")
    secondary_only = professor_name_score("Arroyo Relion, Jesus", "Jesus Relion")
    assert correct > secondary_only
