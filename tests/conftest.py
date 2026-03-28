"""
Shared pytest fixtures for M3 test suite.
"""

import os
import sys

import pytest

# Make scripts/ importable from any test file
_SCRIPTS = os.path.join(os.path.dirname(__file__), "..", "scripts")
sys.path.insert(0, os.path.abspath(_SCRIPTS))

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _ROOT)

import duckdb
from dotenv import load_dotenv

load_dotenv(os.path.join(_ROOT, ".env"))

from m3_1_prompt_builder import build_system_prompt
from config import DB_READ_ONLY, DUCKDB_MEMORY_LIMIT


# -- Fixtures -----------------------------------------------------------------

@pytest.fixture(scope="session")
def db_connection():
    """Open a read-only DuckDB session connection with memory cap; close after all tests."""
    db_path = os.path.join(_ROOT, "models", "instacart.db")
    assert os.path.exists(db_path), f"Database not found: {db_path}"
    con = duckdb.connect(db_path, read_only=DB_READ_ONLY)
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    yield con
    con.close()


@pytest.fixture(scope="session")
def schema_context():
    """Load docs/schema_context.txt; fail clearly if missing."""
    path = os.path.join(_ROOT, "docs", "schema_context.txt")
    if not os.path.exists(path):
        pytest.fail(
            f"schema_context.txt not found at {path}. "
            "Run scripts/m2_run.py to generate it."
        )
    with open(path, encoding="utf-8") as fh:
        return fh.read()


@pytest.fixture(scope="session")
def system_prompt(schema_context):
    """Build the full system prompt from the schema context."""
    return build_system_prompt(schema_context)


@pytest.fixture(scope="session")
def anthropic_client():
    """Initialise Anthropic client; skip any test that uses this if key is absent."""
    import anthropic

    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        pytest.skip("ANTHROPIC_API_KEY is not set — skipping API-dependent test")
    return anthropic.Anthropic(api_key=key)
