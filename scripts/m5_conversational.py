"""
M5 Conversational
==================
Answers questions classified as conversational.
Reads from memory summaries — no SQL, no structured output.
"""

import logging

import anthropic

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CRITIC_MAX_TOKENS, MODEL

log = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "You are a data analyst assistant. Answer the user's "
    "question conversationally using the provided context. "
    "Be concise and specific. Use actual numbers where "
    "available. Maximum 150 words."
)

_FALLBACK = (
    "I couldn't retrieve an answer from the conversation history. "
    "Please try asking a new data question."
)


# -- Functions ----------------------------------------------------------------

def build_conversational_context(question: str, memory: list[dict]) -> str:
    """Build context string from last 3 memory turns plus the current question."""
    if not memory:
        return (
            f"The user asked: '{question}'\n"
            "There is no prior conversation history. "
            "Let them know you need a data question first "
            "before you can provide analysis."
        )

    recent = memory[-3:]
    lines  = []
    for turn in recent:
        lines.append(f"User asked: {turn['question']}")
        lines.append(f"Result: {turn.get('summary', '')}")

    lines.append(f"Current question: {question}")
    lines.append(
        "Answer using the context above. Use specific "
        "numbers from result summaries where relevant. "
        "If the question cannot be answered from the "
        "history, say so clearly and suggest what data "
        "question to ask instead."
    )
    return "\n".join(lines)


def answer_from_memory(
    client: anthropic.Anthropic,
    question: str,
    memory: list[dict],
) -> str:
    """Call Claude to answer a conversational question from memory context."""
    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=CRITIC_MAX_TOKENS,
            system=_SYSTEM_PROMPT,
            messages=[
                {
                    "role":    "user",
                    "content": build_conversational_context(question, memory),
                }
            ],
        )
        return response.content[0].text.strip()
    except Exception as exc:
        log.error("answer_from_memory failed: %s", exc)
        return _FALLBACK


# -- Self-test (no API) -------------------------------------------------------
if __name__ == "__main__":
    context = build_conversational_context(
        "Why is produce ranked highest?",
        [
            {
                "question": "Top departments?",
                "sql":      "SELECT...",
                "summary":  "1. produce: 66.2%, 2. dairy: 65.8%",
                "success":  True,
            }
        ],
    )
    assert "produce"                    in context
    assert "66.2%"                      in context
    assert "Why is produce ranked highest?" in context

    empty = build_conversational_context("Why?", [])
    assert "no prior" in empty.lower()

    # 5-turn memory — only last 3 should appear
    big_memory = [
        {"question": f"q{i}", "summary": f"s{i}", "sql": "", "success": True}
        for i in range(5)
    ]
    ctx = build_conversational_context("current?", big_memory)
    assert ctx.count("User asked:") <= 3

    print("m5_conversational self-test OK")
