"""
M6 Memory
=========
Manages conversation history with result summaries.
Replaces the simple list-of-dicts history with a richer structure
that includes result summaries alongside SQL for semantic follow-ups.
Implements dynamic history window expansion for multi-step reasoning.
"""

import logging
import re
import sys
import os

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import MAX_HISTORY_TURNS, MAX_HISTORY_TURNS_EXT, MULTISTEP_KEYWORDS
from scripts.m6_summariser import summarise_result

log = logging.getLogger(__name__)


# -- Multi-step detection -----------------------------------------------------

def is_multistep(question: str) -> bool:
    """Return True if the question contains any multi-step trigger keyword."""
    q_lower = question.lower()
    return any(kw in q_lower for kw in MULTISTEP_KEYWORDS)


def get_window_size(question: str) -> int:
    """Return the number of prior turns to include based on question complexity."""
    if is_multistep(question):
        log.info("Multi-step question detected — expanding history window to %d turns.",
                 MAX_HISTORY_TURNS_EXT)
        return MAX_HISTORY_TURNS_EXT
    return MAX_HISTORY_TURNS


# -- Memory management --------------------------------------------------------

def add_turn(
    memory: list,
    question: str,
    sql: str,
    result: dict,
) -> list:
    """Append a completed turn to the memory store; return updated list."""
    if result["success"]:
        summary = summarise_result(result["data"], question, sql)
    else:
        error_preview = str(result.get("error", "unknown error"))[:100]
        summary = f"Query failed: {error_preview}"

    memory.append({
        "question": question,
        "sql":      sql,
        "summary":  summary,
        "success":  result["success"],
    })
    return memory


def _format_assistant_turn(turn: dict) -> str:
    """Format a memory turn into the assistant message content for the LLM."""
    return f"{turn['sql']}\n\n[Result: {turn['summary']}]"


def build_messages(question: str, memory: list) -> list:
    """Convert memory into Claude API messages list with the current question appended."""
    window_size = get_window_size(question)
    recent      = memory[-window_size:] if memory else []

    messages = []
    for turn in recent:
        messages.append({"role": "user",      "content": turn["question"]})
        messages.append({"role": "assistant", "content": _format_assistant_turn(turn)})

    messages.append({"role": "user", "content": question})
    return messages


# -- Sidebar helper -----------------------------------------------------------

def get_context_summary(memory: list) -> str:
    """Return a short session description for display in the Streamlit sidebar."""
    if not memory:
        return "No history yet."
    last_q = memory[-1]["question"][:60]
    return f"{len(memory)} turns in memory. Last question: {last_q}..."


# -- Self-test ----------------------------------------------------------------
if __name__ == "__main__":
    memory = []

    result1 = {
        "success":   True,
        "data":      pd.DataFrame({
            "department":   ["produce", "dairy eggs"],
            "reorder_rate": [0.662, 0.658],
        }),
        "row_count": 2,
        "sql":       "SELECT d.department, AVG(od.reordered) AS reorder_rate ...",
        "error":     None,
    }
    memory = add_turn(memory, "Top departments by reorder rate?", result1["sql"], result1)
    assert len(memory) == 1
    assert "produce" in memory[0]["summary"]
    assert "66.2%"   in memory[0]["summary"]
    print("add_turn assertions passed.")

    msgs = build_messages("Why is produce highest?", memory)
    assert msgs[0]["role"]    == "user"
    assert msgs[-1]["content"] == "Why is produce highest?"
    assert "[Result:"          in msgs[1]["content"]
    assert "produce"           in msgs[1]["content"]
    print("build_messages assertions passed.")

    assert is_multistep("first show X then compare with Y") is True
    assert is_multistep("how many orders on Sunday?")       is False
    print("is_multistep assertions passed.")

    assert get_window_size("compare this to last week") == MAX_HISTORY_TURNS_EXT
    assert get_window_size("how many orders?")          == MAX_HISTORY_TURNS
    print("get_window_size assertions passed.")

    assert get_context_summary([])     == "No history yet."
    assert "1 turns"                   in get_context_summary(memory)
    print("get_context_summary assertions passed.")

    print("\nM6 memory OK")
