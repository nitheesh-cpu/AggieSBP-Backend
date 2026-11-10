"""
prewarm_cache.py
================
Pre-warms Redis API cache entries for core endpoints (e.g. UCC discovery, department lists)
for active terms.
"""

import asyncio
import logging
import sys
import time
from pathlib import Path
from dotenv import load_dotenv

project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
src_path = project_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

env_path = project_root / ".env"
load_dotenv(env_path)

from aggiermp.database.base import get_session
from aggiermp.api.routers.discover import discover_ucc_courses, discover_term_departments
from fastapi import Request

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("prewarm_cache")

def build_dummy_request(path: str) -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "headers": [],
        "query_string": b"",
    }
    return Request(scope)

async def prewarm_term(term_code: str):
    session = get_session()
    logger.info("Pre-warming UCC courses for term %s...", term_code)
    req_ucc = build_dummy_request(f"/discover/{term_code}/ucc")
    t0 = time.time()
    ucc_res = await discover_ucc_courses(request=req_ucc, term_code=term_code, db=session)
    t1 = time.time()
    logger.info("Fetched %d UCC categories for term %s in %.2fs", len(ucc_res), term_code, t1 - t0)

    logger.info("Pre-warming term departments for term %s...", term_code)
    req_depts = build_dummy_request(f"/discover/{term_code}/departments")
    t0 = time.time()
    dept_res = await discover_term_departments(request=req_depts, term_code=term_code, db=session)
    t1 = time.time()
    logger.info("Fetched %d departments for term %s in %.2fs", len(dept_res), term_code, t1 - t0)

async def main():
    active_terms = ["202631", "202611"]
    logger.info("Starting API Cache Pre-warming for terms: %s", active_terms)
    for term in active_terms:
        await prewarm_term(term)
    logger.info("Pre-warming complete.")

if __name__ == "__main__":
    asyncio.run(main())
