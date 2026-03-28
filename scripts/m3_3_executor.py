"""
M3.3 — SQL Executor
====================
Executes a SQL string against instacart.db.
Returns a structured result dict — never raises to caller.
Includes input validation (M7) and output sanitisation (M8).
"""

import logging
import threading

import duckdb
import pandas as pd

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    DB_PATH,
    DB_READ_ONLY,
    DUCKDB_MEMORY_LIMIT,
    QUERY_TIMEOUT_SECONDS,
)
from scripts.m7_input_validator  import validate
from scripts.m8_output_sanitiser import sanitise_result

log = logging.getLogger(__name__)

ROW_LIMIT = 1000


# -- Functions ----------------------------------------------------------------

def has_limit_clause(sql: str) -> bool:
    """Return True if sql contains a LIMIT clause (case-insensitive)."""
    return "limit" in sql.lower()


def inject_limit(sql: str, limit: int = ROW_LIMIT) -> str:
    """Append LIMIT {limit} to a SELECT query if no LIMIT clause is present."""
    stripped = sql.strip()
    if has_limit_clause(stripped):
        return sql
    # Only inject for SELECT statements; leave EXPLAIN, DESCRIBE, etc. unchanged
    if not stripped.upper().startswith("SELECT"):
        return sql
    return f"{stripped}\nLIMIT {limit}"


def execute_sql(con: duckdb.DuckDBPyConnection, sql: str) -> dict:
    """Execute SQL with input validation and output sanitisation."""

    # Step 1 — validate before touching DB
    validation = validate(sql)
    if not validation.allowed:
        return sanitise_result({
            "success":   False,
            "data":      None,
            "row_count": 0,
            "sql":       sql,
            "error":     validation.reason,
        })

    # Step 2 — inject LIMIT if missing
    sql = inject_limit(sql)

    # Step 3 — execute with timeout
    result_container: dict = {}
    error_container:  dict = {}

    def run_query() -> None:
        try:
            df = con.execute(sql).fetchdf()
            result_container["df"] = df
        except Exception as exc:
            error_container["error"] = str(exc)

    thread = threading.Thread(target=run_query, daemon=True)
    thread.start()
    thread.join(timeout=QUERY_TIMEOUT_SECONDS)

    if thread.is_alive():
        log.error("Query timed out after %ds: %.80s", QUERY_TIMEOUT_SECONDS, sql)
        raw_result = {
            "success":   False,
            "data":      None,
            "row_count": 0,
            "sql":       sql,
            "error":     (
                f"Query exceeded the {QUERY_TIMEOUT_SECONDS}s time limit. "
                "Try a more specific filter or add a LIMIT clause."
            ),
        }
        return sanitise_result(raw_result)

    if "error" in error_container:
        raw_result = {
            "success":   False,
            "data":      None,
            "row_count": 0,
            "sql":       sql,
            "error":     error_container["error"],
        }
        return sanitise_result(raw_result)

    df = result_container["df"]
    raw_result = {
        "success":   True,
        "data":      df,
        "row_count": len(df),
        "sql":       sql,
        "error":     None,
    }
    return sanitise_result(raw_result)


# -- Self-test ----------------------------------------------------------------
if __name__ == "__main__":
    assert has_limit_clause("SELECT * FROM orders LIMIT 10") is True
    assert has_limit_clause("SELECT * FROM orders limit 10") is True
    assert has_limit_clause("SELECT COUNT(*) FROM orders")   is False
    print("has_limit_clause assertions passed.")

    injected = inject_limit("SELECT * FROM orders")
    assert "LIMIT 1000" in injected
    print(f"inject_limit result: {repr(injected)}")

    unchanged = inject_limit("SELECT * FROM orders LIMIT 5")
    assert "LIMIT 5" in unchanged
    assert unchanged.count("LIMIT") == 1
    print("inject_limit assertions passed.")

    con = duckdb.connect(DB_PATH, read_only=DB_READ_ONLY)
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")

    result = execute_sql(con, "SELECT COUNT(*) FROM orders")
    assert result["success"]   is True
    assert result["row_count"] == 1
    assert result["error"]     is None
    assert isinstance(result["data"], pd.DataFrame)
    assert "warnings" in result
    print(f"execute_sql (good query) row_count={result['row_count']}  value={result['data'].iloc[0,0]:,}")

    bad = execute_sql(con, "SELECT * FROM fake_table_xyz")
    assert bad["success"]          is False
    assert isinstance(bad["error"], str)
    assert "warnings" in bad
    print(f"execute_sql (bad query) error={bad['error'][:60]}")

    blocked = execute_sql(con, "DROP TABLE orders")
    assert blocked["success"] is False
    assert "DROP" in blocked["error"] or "permitted" in blocked["error"]
    print(f"execute_sql (blocked DDL) error={blocked['error']}")

    con.close()
    print("\nM3.3 OK")
