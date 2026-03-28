"""
Tests for M8 Output Sanitiser — error cleaning and null rate detection.
"""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts")))

import sys as _sys
_sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import GENERIC_ERROR_MESSAGE

from m8_output_sanitiser import check_null_rates, sanitise_error, sanitise_result


# -- sanitise_error -----------------------------------------------------------

def test_syntax_error_safe():
    msg = sanitise_error("Parser Error: syntax error at or near 'FORM'")
    assert "syntax error" in msg.lower()
    assert len(msg) <= 150


def test_internal_error_generic():
    msg = sanitise_error("OutOfMemoryException: block alloc failed")
    assert msg == GENERIC_ERROR_MESSAGE


def test_no_file_path_leakage():
    msg = sanitise_error("Error at /src/duckdb/catalog.cpp:42")
    assert ".cpp" not in msg
    assert "/src" not in msg


def test_catalog_exception_safe():
    msg = sanitise_error("CatalogException: Table with name xyz does not exist")
    assert "CatalogException" not in msg
    assert "table not found" in msg.lower()


def test_raw_error_never_returned():
    raw = "InternalException: /src/duckdb/storage/block_manager.cpp:42"
    msg = sanitise_error(raw)
    assert raw not in msg


# -- check_null_rates ---------------------------------------------------------

def test_null_rate_above_threshold():
    df = pd.DataFrame({
        "department": ["produce", "dairy", None, None, None],
        "rate":       [0.66, 0.65, 0.61, 0.59, 0.57],
    })
    warnings = check_null_rates(df)
    assert len(warnings) == 1
    assert "department" in warnings[0]
    assert "60%" in warnings[0]


def test_null_rate_below_threshold():
    df = pd.DataFrame({
        "name": ["a", "b", None, "d", "e", "f", "g", "h", "i", "j"],
        "val":  list(range(10)),
    })
    # 10% null — below 20% threshold
    warnings = check_null_rates(df)
    assert warnings == []


def test_all_columns_clean():
    df = pd.DataFrame({"dept": ["produce", "dairy"], "rate": [0.66, 0.65]})
    assert check_null_rates(df) == []


# -- sanitise_result ----------------------------------------------------------

def test_sanitise_result_success_no_nulls():
    df = pd.DataFrame({"dept": ["produce"], "rate": [0.66]})
    result = {
        "success":   True,
        "data":      df,
        "row_count": 1,
        "sql":       "SELECT ...",
        "error":     None,
    }
    out = sanitise_result(result)
    assert out["success"]  is True
    assert out["warnings"] == []


def test_sanitise_result_success_with_nulls():
    df = pd.DataFrame({
        "department": ["a", "b", None, None, None, None, None, None, None, None],
        "rate":       list(range(10)),
    })
    result = {
        "success":   True,
        "data":      df,
        "row_count": 10,
        "sql":       "SELECT ...",
        "error":     None,
    }
    out = sanitise_result(result)
    assert len(out["warnings"]) == 1
    assert "department" in out["warnings"][0]


def test_sanitise_result_failure_cleans_error():
    result = {
        "success":   False,
        "data":      None,
        "row_count": 0,
        "sql":       "DROP TABLE x",
        "error":     "CatalogException: Table xyz does not exist",
    }
    out = sanitise_result(result)
    assert out["success"]              is False
    assert "CatalogException" not in out["error"]


def test_sanitise_result_always_has_warnings_key():
    success_result = {
        "success":   True,
        "data":      pd.DataFrame({"x": [1]}),
        "row_count": 1,
        "sql":       "SELECT 1",
        "error":     None,
    }
    failure_result = {
        "success":   False,
        "data":      None,
        "row_count": 0,
        "sql":       "bad",
        "error":     "OutOfMemoryException: failed",
    }
    assert "warnings" in sanitise_result(success_result)
    assert "warnings" in sanitise_result(failure_result)


def test_timeout_error_is_user_friendly():
    timeout_error = (
        "Query exceeded the 15s time limit. "
        "Try a more specific filter or add a LIMIT clause."
    )
    result = {
        "success":   False,
        "data":      None,
        "row_count": 0,
        "sql":       "SELECT ...",
        "error":     timeout_error,
    }
    out = sanitise_result(result)
    # Timeout message contains "time limit" and is not the generic fallback
    assert "time limit" in out["error"].lower()
    assert out["error"] != GENERIC_ERROR_MESSAGE
