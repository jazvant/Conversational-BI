"""
M8 — Output Sanitiser
======================
Sanitises error messages and query results before they
reach the user. Strips DuckDB internals from errors and
flags high null rates in result DataFrames.
"""

import re
import logging

import pandas as pd

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    NULL_FLAG_THRESHOLD,
    GENERIC_ERROR_MESSAGE,
    SAFE_ERROR_SUBSTRINGS,
)

log = logging.getLogger(__name__)


# Normalise known DuckDB error patterns to safe display strings.
# Checked before SAFE_ERROR_SUBSTRINGS so common DuckDB phrasing maps cleanly.
_ERROR_NORMALISATION = {
    "does not exist":  "table not found",
    "no such table":   "table not found",
    "no such column":  "column not found",
    "unknown column":  "column not found",
}


# -- Error sanitisation -------------------------------------------------------

def sanitise_error(raw_error: str) -> str:
    """Return a safe user-facing error message, stripping DuckDB internals."""
    log.debug("Raw DuckDB error: %s", raw_error)

    raw_lower = raw_error.lower()

    # Check normalisation mappings first (DuckDB phrasing → canonical safe string)
    for pattern, canonical in _ERROR_NORMALISATION.items():
        if pattern in raw_lower:
            sanitised = f"Query error: {canonical}"
            log.warning("Sanitised error (normalised): %s", sanitised)
            return sanitised

    # Check safe substrings — extract and clean the relevant portion
    for safe_sub in SAFE_ERROR_SUBSTRINGS:
        if safe_sub in raw_lower:
            idx     = raw_lower.find(safe_sub)
            excerpt = raw_error[idx:idx + 120].strip()
            # Strip file paths (e.g. /src/..., C:\...) and line numbers
            excerpt = re.sub(r"[A-Za-z]:\\[^\s,]+",    "", excerpt)
            excerpt = re.sub(r"/[a-z][^\s,]*\.[a-z]+", "", excerpt)
            excerpt = re.sub(r":\d+",                   "", excerpt)
            excerpt = excerpt.strip().rstrip(",.:").strip()
            sanitised = f"Query error: {excerpt}"
            log.warning("Sanitised error: %s", sanitised)
            return sanitised

    log.warning("Non-safe error suppressed. Raw (debug only): %.80s", raw_error)
    return GENERIC_ERROR_MESSAGE


# -- Null rate checking -------------------------------------------------------

def check_null_rates(df: pd.DataFrame) -> list[str]:
    """Return warning strings for any column whose null rate exceeds the threshold."""
    warnings = []
    if df.empty:
        return warnings
    for col in df.columns:
        null_pct = df[col].isna().mean()
        if null_pct > NULL_FLAG_THRESHOLD:
            pct_display = f"{null_pct * 100:.0f}%"
            msg = (
                f"Column '{col}' is {pct_display} null — "
                "results may be incomplete."
            )
            log.warning(msg)
            warnings.append(msg)
    return warnings


# -- Result sanitisation ------------------------------------------------------

def sanitise_result(result: dict) -> dict:
    """Sanitise a result dict from execute_sql; never mutates the input."""
    new_result = dict(result)

    if not result["success"]:
        new_result["error"]    = sanitise_error(result.get("error") or "")
        new_result["warnings"] = []
        return new_result

    # Successful result — check for null rates
    warnings = check_null_rates(result["data"])
    new_result["warnings"] = warnings
    return new_result


# -- Self-test ----------------------------------------------------------------
if __name__ == "__main__":
    msg = sanitise_error("Parser Error: syntax error at or near 'FORM'")
    assert "syntax error" in msg.lower(), msg
    assert "Parser Error" not in msg, msg

    msg = sanitise_error("OutOfMemoryException: block allocation failed")
    assert msg == GENERIC_ERROR_MESSAGE, msg

    raw = "InternalException: /src/duckdb/storage/block_manager.cpp:42"
    msg = sanitise_error(raw)
    assert "/src/duckdb" not in msg, msg
    assert ".cpp" not in msg, msg

    df = pd.DataFrame({
        "department": ["produce", "dairy", None, None, None],
        "rate":       [0.66, 0.65, 0.61, 0.59, 0.57],
    })
    warnings = check_null_rates(df)
    assert len(warnings) == 1, warnings
    assert "department" in warnings[0], warnings[0]
    assert "60%" in warnings[0], warnings[0]

    df_clean = pd.DataFrame({"dept": ["produce"], "rate": [0.66]})
    result = {
        "success":   True,
        "data":      df_clean,
        "row_count": 1,
        "sql":       "SELECT ...",
        "error":     None,
    }
    out = sanitise_result(result)
    assert out["success"]  is True, out
    assert out["warnings"] == [], out
    assert "warnings" in out, out

    result_fail = {
        "success":   False,
        "data":      None,
        "row_count": 0,
        "sql":       "DROP TABLE x",
        "error":     "CatalogException: Table xyz does not exist",
    }
    out = sanitise_result(result_fail)
    assert out["success"]              is False, out
    assert "CatalogException" not in out["error"], out
    assert "warnings" in out, out

    print("M8 output sanitiser self-test OK")
