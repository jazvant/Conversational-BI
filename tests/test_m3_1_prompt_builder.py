"""
Unit tests for m3_1_prompt_builder.py.
No API key or DB connection required.
"""

import os
import sys

import pytest

_ROOT    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_SCRIPTS = os.path.join(_ROOT, "scripts")
sys.path.insert(0, _SCRIPTS)

from m3_1_prompt_builder import (
    SCHEMA_CONTEXT_PATH,
    build_system_prompt,
    build_user_message,
    load_schema_context,
)

_FOUR_ITEM_HISTORY = [
    {"role": "user",      "content": "Top 10 products?"},
    {"role": "assistant", "content": "SELECT product_name..."},
    {"role": "user",      "content": "Now just dairy?"},
    {"role": "assistant", "content": "SELECT product_name WHERE dept='dairy'..."},
]

_TEN_ITEM_HISTORY = [
    {"role": "user",      "content": f"Question {i}"}
    if i % 2 == 0
    else {"role": "assistant", "content": f"SELECT {i}"}
    for i in range(10)
]


# -- load_schema_context ------------------------------------------------------

def test_load_schema_context_success():
    schema = load_schema_context(SCHEMA_CONTEXT_PATH)
    assert "order_details" in schema
    assert "0=Saturday"    in schema


def test_load_schema_context_missing_file():
    with pytest.raises(FileNotFoundError):
        load_schema_context("/nonexistent/path/schema_context.txt")


# -- build_system_prompt ------------------------------------------------------

def test_build_system_prompt_contains_required_rules(schema_context):
    system = build_system_prompt(schema_context)
    assert "CANNOT_ANSWER"     in system
    assert "No markdown fences" in system
    assert "LIMIT 1000"         in system
    assert "DuckDB"             in system


# -- build_user_message -------------------------------------------------------

def test_build_user_message_no_history():
    msgs = build_user_message("test question")
    assert len(msgs) == 1
    assert msgs[0]["role"]    == "user"
    assert msgs[0]["content"] == "test question"


def test_build_user_message_with_history():
    msgs = build_user_message("follow up", _FOUR_ITEM_HISTORY)
    assert msgs[-1]["content"] == "follow up"
    assert len(msgs) <= 7


def test_build_user_message_history_cap():
    msgs = build_user_message("current question", _TEN_ITEM_HISTORY)
    assert len(msgs) <= 7
    assert msgs[-1]["content"] == "current question"
    assert msgs[-1]["role"]    == "user"


def test_build_user_message_empty_history():
    msgs = build_user_message("question", [])
    assert len(msgs) == 1
    assert msgs[0]["content"] == "question"
