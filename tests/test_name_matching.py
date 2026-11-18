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


def test_de_ruiter_name_variants() -> None:
    parsed = parse_person_name("Darryl De Ruiter")
    assert parsed.given == ("darryl",)
    assert parsed.surname == ("de", "ruiter")
    for variant in ("Darryl De Ruiter", "Darryl DeRuiter", "De Ruiter, Darryl", "DE RUITER D"):
        assert professor_name_score("Darryl De Ruiter", variant) == 1.0
    assert not professor_names_match("Darryl De Ruiter", "David De Ruiter")
    assert not professor_names_match("Darryl De Ruiter", "Darryl De Smith")


def test_multiple_surname_particles_preserve_given_names() -> None:
    parsed = parse_person_name("Maria Elena de la Cruz")
    assert parsed.given == ("maria", "elena")
    assert parsed.surname == ("de", "la", "cruz")


def test_hyphenated_surname_keeps_its_parts() -> None:
    parsed = parse_person_name("Oscar Moreira-Tamayo")
    assert parsed.given == ("oscar",)
    assert parsed.surname == ("moreira", "tamayo")
    # A hyphenated given name stays one name.
    assert parse_person_name("Chiang-Yu Wang").given == ("chiangyu",)


def test_hyphenated_surname_matches_either_part() -> None:
    # Registrar "Moreira-Tamayo" is "Oscar Tamayo" on RMP and "MOREIRA-TAMAYO O"
    # in grade reports; RMP "Dugat-Wickliff" is the registrar's "Tanya V. Wickliff".
    assert professor_name_score("Oscar Moreira-Tamayo", "Oscar Tamayo") >= 0.82
    assert professor_name_score("Oscar Moreira-Tamayo", "Oscar Moreira") >= 0.82
    assert professor_name_score("Oscar Moreira-Tamayo", "MOREIRA-TAMAYO O") == 1.0
    assert professor_name_score("Tanya V. Wickliff", "Tanya Dugat-Wickliff") >= 0.82
    # A different given name still fails.
    assert professor_name_score("Maria Garcia-Lopez", "Mario Lopez") < 0.82
