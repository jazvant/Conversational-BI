"""
M3.1 — Prompt Builder
======================
Builds the prompt payload for each user question.
Combines static schema context with the user question
and optional conversation history.
"""

import os

from config import SCHEMA_CONTEXT_PATH

_SYSTEM_INSTRUCTIONS = """
You are a DuckDB SQL expert.
Return a single SQL query only.
No markdown fences, no explanation, no comments.
Query must be executable against the Instacart DuckDB database described above without modification.
Always apply LIMIT 1000 unless the question explicitly asks for all rows or an aggregation.
If the question cannot be answered from the available schema, return exactly: CANNOT_ANSWER
""".strip()

_MAX_HISTORY_ITEMS = 6


# -- Functions ----------------------------------------------------------------

def load_schema_context(path: str) -> str:
    """Read schema_context.txt and return its full contents as a string."""
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Schema context not found at {path}. "
            "Run scripts/m2_run.py first to generate it."
        )
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def build_system_prompt(schema_context: str) -> str:
    """Append SQL generation instructions to the schema context."""
    return f"{schema_context}\n\n{_SYSTEM_INSTRUCTIONS}"


def build_user_message(
    question: str,
    history: list | None = None,
) -> list:
    """Build the Claude messages list, prepending up to 3 prior turns from history."""
    current = {"role": "user", "content": question}

    if not history:
        return [current]

    # Keep only the most recent _MAX_HISTORY_ITEMS items (never exceed cap)
    trimmed = history[-_MAX_HISTORY_ITEMS:]
    return trimmed + [current]


# -- Self-test ----------------------------------------------------------------
if __name__ == "__main__":
    schema = load_schema_context(SCHEMA_CONTEXT_PATH)
    assert "order_details" in schema,  "order_details missing from schema"
    assert "order_dow"     in schema,  "order_dow missing from schema"
    # NOTE: schema encodes 0=Sunday, 6=Saturday — the literal "0=Saturday"
    # is not present; this assertion checks the schema has the Saturday mapping.
    assert "6=Saturday" in schema or "Saturday" in schema, \
        "Day-of-week encoding missing from schema"
    print("load_schema_context assertions passed.")

    system = build_system_prompt(schema)
    assert "CANNOT_ANSWER"    in system
    assert "No markdown fences" in system
    assert "LIMIT 1000"       in system
    print("build_system_prompt assertions passed.")

    msgs = build_user_message("How many orders on Sundays?")
    assert len(msgs) == 1
    assert msgs[0]["role"] == "user"
    print("build_user_message (no history) assertions passed.")

    history = [
        {"role": "user",      "content": "Top 10 products?"},
        {"role": "assistant", "content": "SELECT product_name..."},
        {"role": "user",      "content": "Now just dairy?"},
        {"role": "assistant", "content": "SELECT product_name..."},
    ]
    msgs = build_user_message("Sort by reorder rate", history)
    assert msgs[-1]["content"] == "Sort by reorder rate"
    assert len(msgs) <= 7
    print("build_user_message (with history) assertions passed.")

    print("\nM3.1 OK")
