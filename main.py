#!/usr/bin/env python3
"""
AggieRMP Main Entry Point

This script serves as the main entry point for the AggieRMP application.
It imports and runs the actual main function from the src package.
"""

import sys
from pathlib import Path

# Add src to path for imports
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

try:
    from aggiermp.main import main
except ImportError as e:
    print(f"Error importing main module: {e}")
    print("Make sure you've installed the package in development mode:")
    print("pip install -e .")
    sys.exit(1)

if __name__ == "__main__":
    main() 