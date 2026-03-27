"""
M3.3 — SQL Executor
====================
Executes a SQL string against instacart.db.
Returns a structured result dict — never raises to caller.
"""

import os

import duckdb
import pandas as pd

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# -- Constants ----------------------------------------------------------------
DB_PATH   = os.path.join(_ROOT, "models", "instacart.db")
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
    """Execute sql, returning a result dict with success, data, row_count, and error."""
    safe_sql = inject_limit(sql)
    try:
        df = con.execute(safe_sql).df()
        return {
            "success":   True,
            "data":      df,
            "row_count": len(df),
            "sql":       safe_sql,
            "error":     None,
        }
    except Exception as exc:
        return {
            "success":   False,
            "data":      None,
            "row_count": 0,
            "sql":       safe_sql,
            "error":     str(exc),
        }


# -- Self-test ----------------------------------------------------------------
if __name__ == "__main__":
    assert has_limit_clause("SELECT * FROM orders LIMIT 10") == True
    assert has_limit_clause("SELECT * FROM orders limit 10") == True
    assert has_limit_clause("SELECT COUNT(*) FROM orders")   == False
    print("has_limit_clause assertions passed.")

    injected = inject_limit("SELECT * FROM orders")
    assert "LIMIT 1000" in injected
    print(f"inject_limit result: {repr(injected)}")

    unchanged = inject_limit("SELECT * FROM orders LIMIT 5")
    assert "LIMIT 5" in unchanged
    assert unchanged.count("LIMIT") == 1
    print("inject_limit assertions passed.")

    con = duckdb.connect(DB_PATH, read_only=True)

    result = execute_sql(con, "SELECT COUNT(*) FROM orders")
    assert result["success"]   == True
    assert result["row_count"] == 1
    assert result["error"]     is None
    assert isinstance(result["data"], pd.DataFrame)
    print(f"execute_sql (good query) row_count={result['row_count']}  value={result['data'].iloc[0,0]:,}")

    bad = execute_sql(con, "SELECT * FROM fake_table_xyz")
    assert bad["success"]          == False
    assert isinstance(bad["error"], str)
    print(f"execute_sql (bad query) error={bad['error'][:60]}")

    con.close()
    print("\nM3.3 OK")
