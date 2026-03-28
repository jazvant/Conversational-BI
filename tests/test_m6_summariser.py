"""
Tests for M6 Summariser — programmatic result summarisation.
"""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts")))

from m6_summariser import (
    summarise_generic,
    summarise_result,
    summarise_scalar,
    summarise_single_row,
    summarise_two_col,
)


# -- summarise_scalar ---------------------------------------------------------

def test_scalar_integer():
    df = pd.DataFrame({"count": [3_421_083]})
    r  = summarise_scalar(df, "How many orders?")
    assert "3,421,083" in r


def test_scalar_percentage():
    df = pd.DataFrame({"reorder_rate": [0.658]})
    r  = summarise_scalar(df, "What is the reorder rate?")
    assert "65.8%" in r


def test_scalar_float():
    df = pd.DataFrame({"avg_days": [17.5]})
    r  = summarise_scalar(df, "Average days since prior order?")
    assert "17.50" in r or "17.5" in r


# -- summarise_single_row -----------------------------------------------------

def test_single_row_contains_all_columns():
    df = pd.DataFrame({"product_name": ["Banana"], "count": [12345]})
    r  = summarise_single_row(df)
    assert "product_name" in r
    assert "count" in r
    assert "Banana" in r


# -- summarise_two_col --------------------------------------------------------

def test_two_col_top_items():
    df = pd.DataFrame({
        "department":   ["produce", "dairy eggs", "beverages"],
        "total_orders": [1_500_000, 1_200_000, 900_000],
    })
    r = summarise_two_col(df, "Orders by department?")
    assert "produce"    in r
    assert "1,500,000"  in r
    assert "1."         in r


def test_two_col_percentage_values():
    df = pd.DataFrame({
        "department":   ["produce", "dairy eggs", "beverages"],
        "reorder_rate": [0.662, 0.658, 0.613],
    })
    r = summarise_two_col(df, "Reorder rate by department?")
    assert "66.2%" in r
    assert "produce" in r


# -- summarise_generic --------------------------------------------------------

def test_generic_multi_column():
    df = pd.DataFrame({
        "user_id":     [1, 2, 3],
        "order_count": [5, 3, 10],
        "avg_basket":  [7.2, 4.1, 9.8],
    })
    r = summarise_generic(df, "User stats?")
    assert "3 rows" in r
    assert "3 columns" in r
    assert "user_id" in r


# -- summarise_result routing -------------------------------------------------

def test_empty_result():
    df = pd.DataFrame({"department": [], "count": []})
    r  = summarise_result(df, "anything", "SELECT ...")
    assert r == "Query returned no results."


def test_routes_scalar():
    df = pd.DataFrame({"n": [42]})
    r  = summarise_result(df, "Count?", "SELECT COUNT(*)")
    assert "42" in r


def test_never_raises_on_bad_df():
    """summarise_result must not propagate any exception."""
    df = pd.DataFrame()   # 0 rows, 0 columns — edge case
    r  = summarise_result(df, "edge case", "SELECT 1")
    assert isinstance(r, str)
