"""Pure-function tests for calculate_easiness_score (no database access)."""

import pytest

from aggiermp.api.routers.discover import calculate_easiness_score


@pytest.fixture(scope="module", autouse=True)
def seed_data():
    """Override the conftest DB seeding fixture; these tests need no database."""


def _old_formula(gpa, diff, rating):
    return (gpa / 4.0) * 0.5 + ((5 - diff) / 4.0) * 0.3 + (rating / 5.0) * 0.2


def test_all_components_present_matches_old_formula():
    assert calculate_easiness_score(3.5, 2.5, 4.0) == pytest.approx(
        _old_formula(3.5, 2.5, 4.0)
    )


def test_missing_gpa_is_renormalized():
    # Ioerger: no GPA, difficulty 3.0, rating 4.3 -> (0.15 + 0.172) / 0.5
    expected = (0.15 + 0.172) / 0.5
    assert calculate_easiness_score(None, 3.0, 4.3) == pytest.approx(expected)
    assert calculate_easiness_score(None, 3.0, 4.3) == pytest.approx(0.644)
    assert calculate_easiness_score(0, 3.0, 4.3) == pytest.approx(expected)


def test_only_gpa():
    assert calculate_easiness_score(3.2, 0, 0) == pytest.approx(3.2 / 4.0)


def test_nothing_present_is_zero():
    assert calculate_easiness_score(None, 0, 0) == 0.0
    assert calculate_easiness_score(0, 0.0, 0.0) == 0.0
