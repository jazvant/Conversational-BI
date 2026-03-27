"""
Unit tests for m3_3_executor.py.
Requires db_connection fixture (no API key needed).
"""

import os
import sys

import pandas as pd

_ROOT    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_SCRIPTS = os.path.join(_ROOT, "scripts")
sys.path.insert(0, _SCRIPTS)

from m3_3_executor import execute_sql, has_limit_clause, inject_limit


# -- has_limit_clause ---------------------------------------------------------

def test_has_limit_clause_true():
    assert has_limit_clause("SELECT * FROM orders LIMIT 10") is True
    assert has_limit_clause("select * from orders limit 5")  is True


def test_has_limit_clause_false():
    assert has_limit_clause("SELECT COUNT(*) FROM orders") is False


# -- inject_limit -------------------------------------------------------------

def test_inject_limit_adds_limit():
    result = inject_limit("SELECT * FROM orders")
    assert "LIMIT 1000" in result


def test_inject_limit_preserves_existing():
    result = inject_limit("SELECT * FROM orders LIMIT 5")
    assert result.count("LIMIT") == 1
    assert "LIMIT 5" in result


def test_inject_limit_skips_non_select():
    result = inject_limit("DESCRIBE orders")
    assert "LIMIT" not in result


# -- execute_sql (DB required) ------------------------------------------------

def test_execute_sql_success(db_connection):
    result = execute_sql(db_connection, "SELECT COUNT(*) FROM orders")
    assert result["success"]           is True
    assert result["row_count"]         == 1
    assert result["error"]             is None
    assert isinstance(result["data"], pd.DataFrame)


def test_execute_sql_correct_count(db_connection):
    result = execute_sql(db_connection, "SELECT COUNT(*) AS n FROM orders")
    assert result["success"] is True
    assert result["data"]["n"].iloc[0] == 3_421_083


def test_execute_sql_invalid_table(db_connection):
    result = execute_sql(db_connection, "SELECT * FROM nonexistent_xyz")
    assert result["success"]        is False
    assert isinstance(result["error"], str)
    assert result["data"]           is None


def test_execute_sql_never_raises(db_connection):
    # Must not raise regardless of how broken the SQL is
    result = execute_sql(db_connection, "this is not sql")
    assert result["success"] is False


def test_execute_sql_auto_limit(db_connection):
    result = execute_sql(db_connection, "SELECT * FROM departments")
    assert result["success"]   is True
    assert result["row_count"] <= 1000
