from aggiermp.core.name_matching import (
    grade_row_matches,
    is_nickname,
    name_parses,
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


def test_nicknames_match_their_formal_names() -> None:
    for formal, nickname in [
        ("Michael J. McShane", "Mike McShane"), ("James W. McGill", "Jim McGill"),
        ("Christopher G. McCall", "Chris McCall"), ("Thomas Jones", "Tom Jones"),
        ("Daniel Jones", "Dan Jones"), ("Stephen Jones", "Steve Jones"),
        ("Joseph E. Kahlig", "Joe Kahlig"), ("Benjamin Jones", "Ben Jones"),
        ("Samuel Jones", "Sam Jones"), ("Jennifer Jones", "Jen Jones"),
        ("Alexander Jones", "Alex Jones"), ("Andrew Jones", "Andy Jones"),
        ("Timothy Jones", "Tim Jones"), ("Nicholas Jones", "Nick Jones"),
        ("Richard Jones", "Rick Jones"), ("Kenneth Jones", "Ken Jones"),
        ("Donald Jones", "Don Jones"), ("Ronald Jones", "Ron Jones"),
        ("David Jones", "Dave Jones"), ("Katherine Jones", "Kate Jones"),
        ("Katherine Jones", "Kathy Jones"), ("Patricia Jones", "Pat Jones"),
        ("William R. Jones", "Bill Jones"), ("Robert Jones", "Bob Jones"),
        ("Elizabeth A. Jones", "Liz Jones"), ("Edward Jones", "Ted Jones"),
        ("Margaret Jones", "Peggy Jones"), ("Anthony Jones", "Tony Jones"),
        ("Peter Jones", "Pete Jones"), ("Sandra Jones", "Sandy Jones"),
        ("John Jones", "Jack Jones"), ("Henry Jones", "Hank Jones"),
        ("Albert Jones", "Al Jones"),
    ]:
        assert professor_names_match(formal, nickname), (formal, nickname)
    # Two formal names, or two nicknames of different names, are not nicknames.
    assert not is_nickname("christina", "christine")
    assert not is_nickname("chris", "tina")
    assert is_nickname("bill", "william") and is_nickname("william", "bill")


def test_one_letter_name_changes_are_other_people() -> None:
    for left, right in [
        ("Yan Wang", "Yang Wang"), ("Jian Wang", "Jiang Wang"), ("Xin Li", "Xing Li"),
        ("Maria Lopez", "Mario Lopez"), ("Daniel Smith", "Danielle Smith"),
        ("Christina Smith", "Christine Smith"),
        ("Wei Li", "Wei Liu"), ("Yi Chen", "Yi Cheng"), ("Jian Wang", "Jian Wong"),
        ("Yu Zhang", "Yu Zhong"), ("Lei Xu", "Lei Xue"), ("Ji Huang", "Ji Hwang"),
        ("Mark Johnson", "Mark Johnston"), ("Mark Peterson", "Mark Petersen"),
    ]:
        assert not professor_names_match(left, right), (left, right)
    # Typos that keep the name: a swapped pair, a vowel, a long surname.
    assert professor_names_match("Tanya Wickliff", "Tanay Wickliff")
    assert professor_names_match("Abigail Songok", "Abigael Songok")
    assert professor_names_match("Tanya V. Wickliff", "Tanya Wicliff")


def test_two_word_surname_without_hyphen() -> None:
    parses = name_parses("Alejandro Castillo Ayala")
    assert parses[0] == parse_person_name("Alejandro Castillo Ayala")
    assert parses[0].surname == ("ayala",)
    assert parses[1].surname == ("castillo", "ayala")
    assert professor_names_match("Alejandro Castillo Ayala", "Castillo, Alejandro")
    assert professor_names_match("Alejandro Castillo Ayala", "Castillo Ayala, Alejandro")
    assert professor_names_match("Ana M. Diaz Artiles", "Diaz, Ana")
    # The surname keeps at least one given name before it.
    assert len(name_parses("J. Michael Smith")) == 1
    # Two compound surnames that differ in a part are different families.
    assert not professor_names_match("Ana Diaz Artiles", "Diaz Beltran, Ana Carolina")
    assert professor_names_match("Megan E. Svajda-Hardy", "Savjda-Hardy, Megan")


def test_more_surname_particles() -> None:
    assert parse_person_name("Ana Da Silva").surname == ("da", "silva")
    assert parse_person_name("Joao Dos Santos").surname == ("dos", "santos")
    assert parse_person_name("Asim Abu Baker").surname == ("abu", "baker")
    assert professor_name_score("Asim Abu Baker", "Abubaker, Asim") == 1.0
    assert professor_name_score("Ahmed Q. Al-Ramthan", "Ramthan, Ahmed") >= 0.82


def test_joined_given_names() -> None:
    for left, right in [
        ("Po-Ju Chen", "Chen, Po Ju"), ("Po-Ju Chen", "Chen, Poju"),
        ("Ji Yu Kim", "Kim, Jiyu"), ("Ji Yu Kim", "Kim, Ji-Yu"),
        ("Hee Yeon Tak", "Tak, Heeyeon"), ("Hye-Chung Kum", "Kum, Hye Chung"),
    ]:
        assert professor_name_score(left, right) == 1.0, (left, right)
    # Not on the first syllable alone.
    assert not professor_names_match("Po-Ju Chen", "Chen, Po")


def test_part_of_a_compound_surname_is_capped() -> None:
    garcia = professor_name_score("Juan Garcia-Lopez", "Juan Garcia")
    assert 0.82 <= garcia < 1.0
    assert professor_name_score("Juan Garcia-Lopez", "Juan Garcia-Lopez") == 1.0


def test_letters_nfkd_does_not_fold_and_suffix_positions() -> None:
    assert professor_name_score("Lars Sørensen", "Sorensen, Lars") == 1.0
    assert professor_name_score("Hans Groß", "Gross, Hans") == 1.0
    assert professor_name_score("Hatice Ceylan Koydemir", "Koydemir, Hatıce") == 1.0
    assert professor_name_score("Lukasz Nowak", "Nowak, Łukasz") == 1.0
    # Suffixes only end a name (or precede the grade reports' initial).
    assert parse_person_name("J.R. Smith").given == ("jr",)
    assert parse_person_name("MD Ariful Azad").given == ("md", "ariful")
    assert parse_person_name("Delbert Gatlin III").surname == ("gatlin",)
    assert parse_person_name("GATLIN III D") == parse_person_name("GATLIN D")
    assert parse_person_name("Gatlin Jr., Delbert").surname == ("gatlin",)


def test_grade_rows_need_the_whole_surname_and_first_initial() -> None:
    assert grade_row_matches("TAK H", "Hee Yeon Tak")
    assert grade_row_matches("MOREIRA-TAMAYO O", "Oscar Moreira-Tamayo")
    assert grade_row_matches("MOREIRA TAMAYO O", "Oscar Moreira-Tamayo")
    assert grade_row_matches("CASTILLO AYALA A", "Alejandro Castillo Ayala")
    assert grade_row_matches("DE LA CRUZ J", "Juan De La Cruz")
    assert grade_row_matches("GATLIN III D", "Delbert Gatlin III")
    assert grade_row_matches("SMITH J", "J. Michael Smith")
    assert grade_row_matches("Smith, J", "John D. Smith")
    for row, name in [
        ("SMITH D", "John D. Smith"), ("LI W", "Wei Liu"), ("CHEN Y", "Yi Cheng"),
        ("GARCIA J", "Juan Garcia-Lopez"), ("TAMAYO O", "Oscar Moreira-Tamayo"),
        ("SMITH", "John Smith"), ("SMITH JD", "John D. Smith"),
    ]:
        assert not grade_row_matches(row, name), (row, name)


def test_long_given_name_prefix_counts_like_a_nickname() -> None:
    # RMP "Cemal Pulak" is Cemalettin M. Pulak; "Shankar Chellam" is Shankararaman.
    assert professor_name_score("Cemalettin M. Pulak", "Pulak, Cemal") >= 0.82
    assert professor_name_score("Shankararaman Chellam", "Chellam, Shankar") >= 0.82
    assert professor_name_score("Jason E. Maddock", "Maddock, Jay") >= 0.82
    # Short or near-equal starts are other names.
    assert professor_name_score("Joseph Smith", "Smith, Jose") < 0.82
    assert professor_name_score("Marianne Lee", "Lee, Marian") < 0.82
