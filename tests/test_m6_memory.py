"""
Tests for M6 Memory — conversation history with result summaries.
"""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts")))

from m6_memory import (
    _format_assistant_turn,
    add_turn,
    build_messages,
    get_context_summary,
    get_window_size,
    is_multistep,
)


# -- Helpers ------------------------------------------------------------------

def _success_result(df: pd.DataFrame) -> dict:
    return {"success": True, "data": df, "row_count": len(df), "error": None}


def _failure_result(error: str = "syntax error") -> dict:
    return {"success": False, "data": None, "row_count": 0, "error": error}


_SIMPLE_DF = pd.DataFrame({"count": [99]})
_TWO_COL_DF = pd.DataFrame({
    "department":   ["produce", "dairy eggs"],
    "total_orders": [1_500_000, 1_200_000],
})


# -- is_multistep -------------------------------------------------------------

def test_multistep_detected_compare():
    assert is_multistep("compare dairy to produce") is True


def test_multistep_detected_trend():
    assert is_multistep("show me the trend over time") is True


def test_multistep_not_detected_simple():
    assert is_multistep("how many orders on Sunday?") is False


# -- get_window_size ----------------------------------------------------------

def test_window_expanded_for_multistep():
    size = get_window_size("first show produce then compare with dairy")
    assert size >= 5


def test_window_default_for_simple():
    size = get_window_size("top 10 products")
    assert size >= 1
    assert size <= 5


# -- add_turn -----------------------------------------------------------------

def test_add_turn_appends_entry():
    memory = []
    memory = add_turn(memory, "How many orders?", "SELECT COUNT(*)", _success_result(_SIMPLE_DF))
    assert len(memory) == 1
    assert memory[0]["question"] == "How many orders?"
    assert memory[0]["success"]  is True


def test_add_turn_summary_populated_on_success():
    memory = []
    memory = add_turn(memory, "q", "sql", _success_result(_SIMPLE_DF))
    assert memory[0]["summary"] != ""


def test_add_turn_summary_on_failure():
    memory = []
    memory = add_turn(memory, "q", "bad sql", _failure_result("Catalog Error: table not found"))
    assert "failed" in memory[0]["summary"].lower()


def test_add_turn_multiple():
    memory = []
    for i in range(3):
        memory = add_turn(memory, f"question {i}", f"sql {i}", _success_result(_SIMPLE_DF))
    assert len(memory) == 3


# -- _format_assistant_turn ---------------------------------------------------

def test_format_assistant_turn_contains_sql_and_result():
    turn = {"sql": "SELECT 1", "summary": "Result: 1."}
    fmt  = _format_assistant_turn(turn)
    assert "SELECT 1"   in fmt
    assert "[Result:"   in fmt
    assert "Result: 1." in fmt


# -- build_messages -----------------------------------------------------------

def test_build_messages_empty_memory():
    msgs = build_messages("How many orders?", [])
    assert len(msgs) == 1
    assert msgs[0]["role"]    == "user"
    assert msgs[0]["content"] == "How many orders?"


def test_build_messages_structure():
    memory = []
    memory = add_turn(memory, "Top products?", "SELECT ...", _success_result(_TWO_COL_DF))
    msgs = build_messages("Now filter by dairy?", memory)
    assert msgs[0]["role"] == "user"
    assert msgs[1]["role"] == "assistant"
    assert msgs[-1]["content"] == "Now filter by dairy?"


def test_build_messages_result_embedded_in_assistant():
    memory = []
    memory = add_turn(memory, "Top depts?", "SELECT dept ...", _success_result(_TWO_COL_DF))
    msgs = build_messages("Sort differently", memory)
    assistant_content = msgs[1]["content"]
    assert "[Result:" in assistant_content


# -- get_context_summary ------------------------------------------------------

def test_context_summary_empty():
    assert get_context_summary([]) == "No history yet."


def test_context_summary_with_turns():
    memory = []
    memory = add_turn(memory, "How many orders?", "SELECT COUNT(*)", _success_result(_SIMPLE_DF))
    summary = get_context_summary(memory)
    assert "1 turns" in summary
    assert "How many orders?" in summary or "How many orders" in summary
