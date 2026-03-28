"""
M6 Summariser
=============
Generates a compact text summary of a query result DataFrame.
Uses programmatic summarisation only — zero API calls.
Clean interface so a Claude-based summariser can be swapped in later.
"""

import logging

import pandas as pd

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import SUMMARY_ROW_LIMIT

log = logging.getLogger(__name__)


# -- Helpers ------------------------------------------------------------------

def _is_numeric_col(series: pd.Series) -> bool:
    """Return True if the series has a numeric dtype."""
    return pd.api.types.is_numeric_dtype(series)


def _fmt_number(val, series: pd.Series) -> str:
    """Format a single numeric value based on the distribution of its column."""
    all_vals = series.dropna()
    if _is_numeric_col(series) and pd.api.types.is_integer_dtype(series):
        return f"{int(val):,}"
    # Float
    fval = float(val)
    if len(all_vals) > 0 and all_vals.between(0, 1).all():
        return f"{fval * 100:.1f}%"
    if len(all_vals) > 0 and (all_vals > 1_000).all():
        return f"{int(fval):,}"
    return f"{fval:.2f}"


# -- Sub-summarisers ----------------------------------------------------------

def summarise_scalar(df: pd.DataFrame, question: str) -> str:
    """Format a single-value result (COUNT, AVG, SUM queries)."""
    val    = df.iloc[0, 0]
    series = df.iloc[:, 0]

    if _is_numeric_col(series) and pd.api.types.is_integer_dtype(series):
        formatted = f"{int(val):,}"
    else:
        fval = float(val)
        if 0.0 < fval < 1.0:
            formatted = f"{fval * 100:.1f}%"
        else:
            formatted = f"{fval:.2f}"

    return f"Result: {formatted}."


def summarise_single_row(df: pd.DataFrame) -> str:
    """Format a single row with multiple columns as a key-value listing."""
    row    = df.iloc[0]
    parts  = [f"{col}: {row[col]}" for col in df.columns]
    return "Single result — " + ", ".join(parts)


def summarise_two_col(df: pd.DataFrame, question: str) -> str:
    """Format the top-N rows of a two-column (label + metric) result."""
    # Identify label and metric columns
    nums    = [c for c in df.columns if _is_numeric_col(df[c])]
    labels  = [c for c in df.columns if c not in nums]

    if not nums or not labels:
        return summarise_generic(df, question)

    label_col  = labels[0]
    metric_col = nums[0]
    metric_ser = df[metric_col]
    subset     = df.head(SUMMARY_ROW_LIMIT)
    total      = len(df)

    lines = [f"Top {len(subset)} results for {label_col} by {metric_col}:"]
    for i, (_, row) in enumerate(subset.iterrows(), start=1):
        val_str = _fmt_number(row[metric_col], metric_ser)
        lines.append(f" {i}. {row[label_col]}: {val_str}")

    lines.append(f"({total} total rows returned)")
    return "\n".join(lines)


def summarise_generic(df: pd.DataFrame, question: str) -> str:
    """Fallback summary for complex multi-column results."""
    row_count = len(df)
    col_count = len(df.columns)
    col_list  = list(df.columns)

    if len(col_list) > 8:
        col_display = ", ".join(col_list[:8]) + ", ..."
    else:
        col_display = ", ".join(col_list)

    first_row = ", ".join(f"{c}={df.iloc[0][c]}" for c in df.columns)

    return (
        f"{row_count} rows \u00d7 {col_count} columns returned.\n"
        f"Columns: {col_display}\n"
        f"First row: {first_row}"
    )


# -- Entry point --------------------------------------------------------------

def summarise_result(df: pd.DataFrame, question: str, sql: str) -> str:
    """Route to the appropriate sub-summariser based on result shape."""
    try:
        if len(df) == 0:
            return "Query returned no results."
        if len(df) == 1 and len(df.columns) == 1:
            return summarise_scalar(df, question)
        if len(df) == 1 and len(df.columns) > 1:
            return summarise_single_row(df)
        if len(df) > 1 and len(df.columns) == 2:
            return summarise_two_col(df, question)
        return summarise_generic(df, question)
    except Exception as exc:
        log.warning("summarise_result failed: %s", exc)
        return ""


# -- Self-test ----------------------------------------------------------------
if __name__ == "__main__":
    # Scalar integer
    df = pd.DataFrame({"count": [3_421_083]})
    r  = summarise_result(df, "How many orders?", "SELECT COUNT(*)")
    assert "3,421,083" in r, r

    # Scalar percentage
    df = pd.DataFrame({"reorder_rate": [0.658]})
    r  = summarise_result(df, "What is reorder rate?", "SELECT AVG...")
    assert "65.8%" in r, r

    # Scalar float
    df = pd.DataFrame({"avg_days": [17.5]})
    r  = summarise_result(df, "q", "sql")
    assert "17.50" in r or "17.5" in r, r

    # No rows
    df = pd.DataFrame({"department": [], "count": []})
    r  = summarise_result(df, "anything", "SELECT...")
    assert r == "Query returned no results.", r

    # Two-col percentage
    df = pd.DataFrame({
        "department":  ["produce", "dairy eggs", "beverages"],
        "reorder_rate": [0.662, 0.658, 0.613],
    })
    r = summarise_result(df, "Top departments?", "SELECT...")
    assert "produce"  in r, r
    assert "66.2%"    in r, r
    assert "1."       in r, r

    print("All m6_summariser assertions passed.")
