import pytest
from fastapi.testclient import TestClient
from aggiermp.api.main import app  # Import your FastAPI app instance

client = TestClient(app)

def test_all_get_endpoints():
    """
    Automatically finds all GET endpoints and checks if they 
    return a 200 OK (or 401/403 if they require auth).
    """
    for route in app.routes:
        # We only test GET routes that don't require path parameters (like /user/{id})
        if "GET" in route.methods and "{" not in route.path and route.path not in ["/professors/compare", "/professor/find"]:
            response = client.get(route.path)
            
            # Assert that the page didn't crash (500 error)
            # We accept 200 (OK) or 401/403 (Forbidden/Unauthorized) 
            assert response.status_code in [200, 401, 403], f"Endpoint {route.path} failed with {response.status_code}"

def test_health_check():
    """Manually test a specific critical endpoint."""
    response = client.get("/health") # Adjust if your health path is different
    assert response.status_code == 200
    assert response.json()['status'] == "healthy"