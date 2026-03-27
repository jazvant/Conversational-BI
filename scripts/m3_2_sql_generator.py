"""
M3.2 — SQL Generator
=====================
Calls the Claude API and returns a clean SQL string.
Single responsibility: API call and response extraction only.
No execution. No validation.
"""

import anthropic

# -- Constants ----------------------------------------------------------------
MODEL      = "claude-sonnet-4-6"
MAX_TOKENS = 600

_CANNOT_ANSWER = "CANNOT_ANSWER"


# -- Functions ----------------------------------------------------------------

def strip_markdown_fences(text: str) -> str:
    """Remove opening and closing markdown code fences from text if present."""
    stripped = text.strip()
    if not stripped.startswith("```"):
        return stripped

    lines = stripped.splitlines()
    # Drop the opening fence line (e.g. ```sql or ```)
    inner = lines[1:]
    # Drop the closing fence line if present
    if inner and inner[-1].strip() == "```":
        inner = inner[:-1]

    return "\n".join(inner).strip()


def generate_sql(
    client: anthropic.Anthropic,
    system_prompt: str,
    messages: list,
) -> str:
    """Send messages to Claude and return a clean SQL string."""
    response = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=system_prompt,
        messages=messages,
    )
    raw = response.content[0].text
    cleaned = strip_markdown_fences(raw)

    if cleaned.strip() == _CANNOT_ANSWER:
        return _CANNOT_ANSWER

    return cleaned


def is_cannot_answer(sql: str) -> bool:
    """Return True if sql is the sentinel CANNOT_ANSWER string."""
    return sql.strip() == _CANNOT_ANSWER


# -- Self-test ----------------------------------------------------------------
if __name__ == "__main__":
    assert strip_markdown_fences("```sql\nSELECT 1\n```") == "SELECT 1"
    assert strip_markdown_fences("```\nSELECT 1\n```")    == "SELECT 1"
    assert strip_markdown_fences("SELECT 1")               == "SELECT 1"
    assert strip_markdown_fences("  SELECT 1  ")           == "SELECT 1"
    assert strip_markdown_fences("```sql\nSELECT\n  1\n```") == "SELECT\n  1"
    print("strip_markdown_fences assertions passed.")

    assert is_cannot_answer("CANNOT_ANSWER")      == True
    assert is_cannot_answer("  CANNOT_ANSWER  ")  == True
    assert is_cannot_answer("SELECT 1")           == False
    assert is_cannot_answer("")                   == False
    print("is_cannot_answer assertions passed.")

    print("\nM3.2 OK")
