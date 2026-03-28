"""
Tests for M5 Planner — helper functions and schema only, no API calls.
"""

import os
import sys

import pytest
from pydantic import ValidationError

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.m5_planner import (
    PlannerDecision,
    build_planner_message,
    is_cannot_answer,
    is_conversational,
    is_data_query,
    is_multistep,
)
from scripts.m5_schemas import PlannerOutput


# -- Helper predicates --------------------------------------------------------

def test_is_data_query():
    d = PlannerDecision("data_query", "r", [])
    assert is_data_query(d)     is True
    assert is_conversational(d) is False
    assert is_multistep(d)      is False
    assert is_cannot_answer(d)  is False


def test_is_conversational():
    d = PlannerDecision("conversational", "r", [])
    assert is_conversational(d) is True
    assert is_data_query(d)     is False


def test_is_multistep():
    d = PlannerDecision("multistep", "r", ["q1"])
    assert is_multistep(d) is True
    assert len(d.subqueries) == 1


def test_is_cannot_answer():
    d = PlannerDecision("cannot_answer", "r", [])
    assert is_cannot_answer(d) is True
    assert is_data_query(d)    is False


# -- build_planner_message ----------------------------------------------------

def test_build_planner_message_empty_memory():
    result = build_planner_message("How many orders?", [])
    assert "Current question: How many orders?" in result
    assert "Recent context" not in result


def test_build_planner_message_with_memory():
    memory = [
        {"question": "Top departments?", "summary": "produce 66.2%",
         "sql": "", "success": True}
    ]
    result = build_planner_message("Why is produce highest?", memory)
    assert "Top departments?"          in result
    assert "produce 66.2%"             in result
    assert "Current question: Why is produce highest?" in result


def test_build_planner_message_caps_at_two_turns():
    memory = [
        {"question": f"q{i}", "summary": f"s{i}", "sql": "", "success": True}
        for i in range(5)
    ]
    result = build_planner_message("latest?", memory)
    assert result.count("User asked:") <= 2


# -- PlannerOutput schema (used by classify_intent internally) ----------------

def test_planner_schema_valid():
    p = PlannerOutput(intent="data_query", reason="wants data", subqueries=[])
    assert p.intent == "data_query"


def test_planner_schema_invalid_intent():
    with pytest.raises(ValidationError):
        PlannerOutput(intent="bad_intent", reason="r", subqueries=[])  # type: ignore
