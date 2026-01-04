#!/usr/bin/env python3
"""
Startup script for AggieRMP API server
Runs the FastAPI application with Scalar documentation available at /docs
"""

import sys
from pathlib import Path

import uvicorn

# Add src to path for imports
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))


def main() -> None:
    """Run the FastAPI server"""
    print("Starting AggieRMP API server...")
    print("Scalar API Documentation will be available at: http://localhost:8000/docs")
    print("OpenAPI JSON schema available at: http://localhost:8000/openapi.json")
    print("FastAPI default docs (Swagger UI) available at: http://localhost:8000/redoc")

    uvicorn.run(
        "aggiermp.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )


if __name__ == "__main__":
    main()
