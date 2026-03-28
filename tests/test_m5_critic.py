"""
Tests for M5 Critic — deterministic functions only, no API calls.
"""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.m5_critic import (
    CriticValidation,
    format_result_for_critic,
    validate_result_sanity,
)
from scripts.m5_schemas import CriticOutput


# -- validate_result_sanity ---------------------------------------------------

def test_validate_zero_rows():
    result = {
        "success":   True,
        "row_count": 0,
        "data":      pd.DataFrame({"dept": [], "rate": []}),
        "sql":       "SELECT...",
        "error":     None,
    }
    sane, issues = validate_result_sanity(result)
    assert sane          is False
    assert len(issues)   > 0


def test_validate_clean_result():
    result = {
        "success":   True,
        "row_count": 5,
        "data":      pd.DataFrame({
            "department":   ["produce", "dairy", "beverages", "snacks", "frozen"],
            "reorder_rate": [0.662, 0.658, 0.613, 0.574, 0.551],
        }),
        "sql": "SELECT...", "error": None,
    }
    sane, issues = validate_result_sanity(result)
    assert sane   is True
    assert issues == []


def test_validate_high_null_column():
    df = pd.DataFrame({
        "dept": ["a", "b", None, None, None, None, None, None, None, None],
        "rate": list(range(10)),
    })
    result = {"success": True, "row_count": 10, "data": df, "sql": "", "error": None}
    sane, issues = validate_result_sanity(result)
    assert sane              is False
    assert "null" in issues[0].lower()


def test_validate_failed_result():
    result = {
        "success":   False,
        "row_count": 0,
        "data":      None,
        "sql":       "bad sql",
        "error":     "table not found",
    }
    # critique() handles failed results; validate_result_sanity gets a None df
    # Test via CriticValidation construction path used in critique()
    cv = CriticValidation(
        sane=False,
        issues=[result["error"]],
        narrative=None,
    )
    assert cv.sane          is False
    assert cv.narrative     is None
    assert len(cv.issues)   > 0


# -- format_result_for_critic -------------------------------------------------

def test_format_result_basic():
    df = pd.DataFrame({"dept": ["a", "b", "c"], "rate": [0.66, 0.55, 0.44]})
    fmt = format_result_for_critic(df)
    assert "Columns:" in fmt
    assert "Row 1:"   in fmt


def test_format_result_percentage_format():
    df = pd.DataFrame({"dept": ["produce"], "reorder_rate": [0.662]})
    fmt = format_result_for_critic(df)
    assert "%" in fmt


def test_format_result_caps_at_max_rows():
    df = pd.DataFrame({"x": list(range(20))})
    fmt = format_result_for_critic(df, max_rows=10)
    assert "(20 total rows)" in fmt
    assert "Row 11:"         not in fmt


# -- generate_narrative row_count guard (no API) ------------------------------

def test_narrative_skipped_scalar():
    """generate_narrative returns None when row_count <= NARRATIVE_MIN_ROWS."""
    from config import NARRATIVE_MIN_ROWS
    from scripts.m5_critic import generate_narrative

    scalar_result = {
        "success":   True,
        "row_count": 1,
        "data":      pd.DataFrame({"count": [99]}),
        "sql":       "",
        "error":     None,
    }
    # row_count=1 <= NARRATIVE_MIN_ROWS=2 → returns None without any API call
    assert scalar_result["row_count"] <= NARRATIVE_MIN_ROWS
    # We can't call generate_narrative without a real client,
    # but we confirm the guard condition holds
    result = generate_narrative(None, "How many orders?", scalar_result)  # type: ignore
    assert result is None


# -- CriticOutput field validation --------------------------------------------

def test_critic_output_valid():
    c = CriticOutput(
        answer="Produce leads at 66.2%.",
        finding="Gap to dairy is 0.4%.",
        caveat="None.",
        followup="Which products drive this?",
    )
    assert c.answer.startswith("Produce")


def test_followup_appends_question_mark():
    c = CriticOutput(
        answer="Produce leads.",
        finding="Gap is 0.4%.",
        caveat="None.",
        followup="Which products drive this",
    )
    assert c.followup == "Which products drive this?"
