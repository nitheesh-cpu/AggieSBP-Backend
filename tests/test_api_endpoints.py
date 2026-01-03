import pytest
from fastapi.testclient import TestClient
from aggiermp.api.main import app  # Import your FastAPI app instance


@pytest.fixture(scope="module")
def client():
    """Create a test client for the FastAPI application."""
    with TestClient(app) as client:
        yield client


def test_root_endpoint(client):
    """Test the root endpoint returns 200 and welcome message."""
    response = client.get("/")
    assert response.status_code == 200
    assert "Welcome to AggieSBP API" in response.text


def test_health_check(client):
    """Test the health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "database" in data
    assert "api_version" in data


def test_database_status(client):
    """Test the database status endpoint."""
    response = client.get("/db-status")
    assert response.status_code == 200
    data = response.json()
    assert "pool_status" in data
    assert "checked_in" in data["pool_status"]


def test_get_terms(client):
    """Test getting active terms."""
    response = client.get("/terms")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    # If there are terms, verify their structure
    if len(data) > 0:
        term = data[0]
        assert "termDesc" in term
        assert "termCode" in term


def test_get_sections(client):
    """Test getting sections with pagination."""
    # Test default
    response = client.get("/sections?limit=5")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) <= 5

    if len(data) > 0:
        section = data[0]
        assert "id" in section
        assert "dept" in section
        assert "courseNumber" in section


def test_endpoint_stats(client):
    """Test get_data_stats."""
    response = client.get("/data_stats")
    assert response.status_code == 200
    data = response.json()
    assert "reviews_count" in data
    assert "courses_count" in data
    assert "professors_count" in data


def test_ucc_discovery_endpoint(client):
    """Test the UCC discovery endpoint."""
    # We need a valid term code for this to work.
    # Let's try to fetch one first, or use a known one like '202611' from previous context.
    term_code = "202611"

    # Verify the term exists or fallback
    terms_response = client.get("/terms")
    if terms_response.status_code == 200 and len(terms_response.json()) > 0:
        term_code = terms_response.json()[0]["termCode"]

    response = client.get(f"/discover/{term_code}/ucc")

    # It might return 200 or 404 (if no data for term), or 500 if DB issue
    # But for a basic test, we expect 200 if the query is valid.
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)
    if len(data) > 0:
        category_group = data[0]
        assert "category" in category_group
        assert "courses" in category_group
