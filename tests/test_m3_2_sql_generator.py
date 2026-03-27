"""
Unit tests for m3_2_sql_generator.py.
Tests strip_markdown_fences and is_cannot_answer without API calls.
"""

import os
import sys

_ROOT    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_SCRIPTS = os.path.join(_ROOT, "scripts")
sys.path.insert(0, _SCRIPTS)

from m3_2_sql_generator import is_cannot_answer, strip_markdown_fences


# -- strip_markdown_fences ----------------------------------------------------

def test_strip_fences_with_sql_tag():
    result = strip_markdown_fences("```sql\nSELECT 1\n```")
    assert result == "SELECT 1"


def test_strip_fences_without_language_tag():
    result = strip_markdown_fences("```\nSELECT 1\n```")
    assert result == "SELECT 1"


def test_strip_fences_no_fences():
    sql    = "SELECT COUNT(*) FROM orders"
    result = strip_markdown_fences(sql)
    assert result == sql


def test_strip_fences_multiline_sql():
    raw    = "```sql\nSELECT\n  COUNT(*)\nFROM orders\n```"
    result = strip_markdown_fences(raw)
    assert "SELECT"     in result
    assert "FROM orders" in result
    assert "```"        not in result


# -- is_cannot_answer ---------------------------------------------------------

def test_is_cannot_answer_true():
    assert is_cannot_answer("CANNOT_ANSWER") is True


def test_is_cannot_answer_false_select():
    assert is_cannot_answer("SELECT 1") is False


def test_is_cannot_answer_false_empty():
    assert is_cannot_answer("") is False


def test_is_cannot_answer_strips_whitespace():
    assert is_cannot_answer("  CANNOT_ANSWER  ") is True
