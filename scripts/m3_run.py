"""
M3 — Instacart BI Agent REPL
==============================
Interactive plain-English to SQL interface for the Instacart dataset.

Run with:
    .venv/Scripts/python scripts/m3_run.py
"""

import logging
import os
import sys

import anthropic
import duckdb
from dotenv import load_dotenv

# Ensure scripts/ is importable when invoked from the project root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from m3_1_prompt_builder import build_system_prompt, load_schema_context
from m3_4_error_recovery import attempt_with_retry
from m6_memory import add_turn, build_messages

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
load_dotenv(os.path.join(_ROOT, ".env"))

from config import DB_PATH, SCHEMA_CONTEXT_PATH  # noqa: E402

# -- Constants ----------------------------------------------------------------
_SEP = "-" * 60
_BANNER = """\
============================================
 Instacart BI Agent -- M3 ready
 Type a question in plain English.
 Type 'exit' or 'quit' to stop.
============================================"""


# -- Main REPL ----------------------------------------------------------------

def main() -> None:
    """Start the interactive Conversational BI REPL."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    # 1 & 2. Load schema and build system prompt
    try:
        schema_context = load_schema_context(SCHEMA_CONTEXT_PATH)
    except FileNotFoundError as exc:
        print(f"Error: {exc}")
        sys.exit(1)
    system_prompt = build_system_prompt(schema_context)

    # 3. Connect to database
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}")
        print("Run scripts/build_database.py first.")
        sys.exit(1)
    con = duckdb.connect(DB_PATH, read_only=True)

    # 4. Initialise Anthropic client
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print(
            "Error: ANTHROPIC_API_KEY is not set.\n"
            "Add it to .env or export it in your shell."
        )
        sys.exit(1)
    client = anthropic.Anthropic(api_key=api_key)

    # 5. Conversation memory
    memory: list = []

    # 6. Startup banner
    print(_BANNER)

    # 7. REPL loop
    while True:
        # a. Prompt
        print("> ", end="", flush=True)

        # b. Read input
        try:
            question = input().strip()
        except (EOFError, KeyboardInterrupt):
            break

        # c. Exit commands
        if question.lower() in ("exit", "quit"):
            break

        # d. Empty input
        if not question:
            continue

        # e. Generate and execute SQL with error recovery
        result = attempt_with_retry(
            client,
            con,
            system_prompt,
            question,
            memory,
        )

        # f-i. Print result
        print(_SEP)
        print(f"SQL:\n{result['sql']}")
        print(_SEP)

        if result["success"]:
            print(result["data"].to_string(index=False))
            print(f"({result['row_count']} rows)")
        else:
            print(f"Error: {result['error']}")

        print(_SEP)

        # j. Add turn to memory
        memory = add_turn(memory, question, result["sql"], result)

    # 8. Goodbye
    con.close()
    print("Goodbye.")


if __name__ == "__main__":
    main()
