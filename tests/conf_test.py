import sys
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

# Ensure src is in python path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

# Import the FastAPI app
# Adjust the import path based on your findings
# It looks like it's in src/aggiermp/api/main.py
from aggiermp.api.main import app


from typing import Generator


@pytest.fixture(scope="module")
def client() -> Generator[TestClient, None, None]:
    with TestClient(app) as c:
        yield c
