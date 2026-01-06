from fastapi.testclient import TestClient
from aggiermp.api.main import app  # Import your FastAPI app instance
from fastapi.routing import APIRoute
from typing import cast


def test_all_get_endpoints(client: TestClient) -> None:
    """
    Automatically finds all GET endpoints and checks if they
    return a 200 OK (or 401/403 if they require auth).
    """
    for route in app.routes:
        # We only test GET routes that don't require path parameters (like /user/{id})
        # Cast to APIRoute to access methods and path safely
        if not isinstance(route, APIRoute):
            continue

        api_route = cast(APIRoute, route)
        if (
            "GET" in api_route.methods
            and "{" not in api_route.path
            and api_route.path not in ["/professors/compare", "/professor/find"]
        ):
            response = client.get(api_route.path)

            # Assert that the page didn't crash (500 error)
            # We accept 200 (OK) or 401/403 (Forbidden/Unauthorized)
            assert response.status_code in [
                200,
                401,
                403,
            ], f"Endpoint {route.path} failed with {response.status_code}"


def test_health_check(client: TestClient) -> None:
    """Manually test a specific critical endpoint."""
    response = client.get("/health")  # Adjust if your health path is different
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"
