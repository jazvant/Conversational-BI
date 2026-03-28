"""
Tests for M5 Conversational — context building only, no API calls.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.m5_conversational import build_conversational_context


# -- build_conversational_context ---------------------------------------------

def test_context_with_memory():
    memory = [
        {
            "question": "Top departments by reorder rate?",
            "summary":  "1. produce 66.2%, 2. dairy 65.8%",
            "sql":      "SELECT...",
            "success":  True,
        }
    ]
    context = build_conversational_context(
        "Why is produce ranked highest?", memory
    )
    assert "produce"                       in context
    assert "66.2%"                         in context
    assert "Why is produce ranked highest?" in context


def test_context_empty_memory():
    context = build_conversational_context("Why?", [])
    assert "no prior" in context.lower()


def test_context_caps_at_three_turns():
    memory = [
        {"question": f"q{i}", "summary": f"s{i}", "sql": "", "success": True}
        for i in range(5)
    ]
    context = build_conversational_context("current?", memory)
    assert context.count("User asked:") <= 3


def test_context_includes_current_question():
    memory = [
        {"question": "first?", "summary": "result", "sql": "", "success": True}
    ]
    context = build_conversational_context("follow up?", memory)
    assert "follow up?" in context


def test_context_empty_memory_suggests_data_question():
    context = build_conversational_context("Explain this", [])
    # Should mention needing a data question first
    assert "data" in context.lower() or "question" in context.lower()
