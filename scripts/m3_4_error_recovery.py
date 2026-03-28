"""
M3.4 — Error Recovery
======================
On SQL execution failure, feeds the error back to Claude and asks it to
correct the SQL. Hard cap of 3 total attempts (1 original + 2 retries).
"""

import logging

from m3_2_sql_generator  import generate_sql, is_cannot_answer
from m6_memory           import build_messages
from m3_3_executor       import execute_sql

log = logging.getLogger(__name__)

# -- Constants ----------------------------------------------------------------
MAX_RETRIES = 3

_CANNOT_ANSWER_RESULT = {
    "success":   False,
    "data":      None,
    "row_count": 0,
    "sql":       "CANNOT_ANSWER",
    "error":     "Question cannot be answered from available schema",
}


# -- Functions ----------------------------------------------------------------

def build_retry_message(
    original_question: str,
    failed_sql: str,
    error_message: str,
) -> str:
    """Build a retry prompt containing the original question, failed SQL, and error."""
    return (
        f"The following plain English question was asked:\n"
        f"{original_question}\n\n"
        f"The SQL below was generated but failed to execute:\n"
        f"```sql\n{failed_sql}\n```\n\n"
        f"DuckDB returned this error:\n"
        f"{error_message}\n\n"
        f"Return corrected DuckDB SQL only. No explanation."
    )


def attempt_with_retry(
    client,
    con,
    system_prompt: str,
    question: str,
    history: list | None = None,
) -> dict:
    """Generate SQL and execute it, retrying on failure up to MAX_RETRIES times."""
    sql    = None
    result = None

    for attempt in range(1, MAX_RETRIES + 1):
        log.info("Attempt %d/%d for question: %s", attempt, MAX_RETRIES, question[:50])

        if attempt == 1:
            messages = build_messages(question, history or [])
        else:
            # Feed the previous error back to Claude for correction
            retry_content = build_retry_message(question, sql, result["error"])
            messages = [{"role": "user", "content": retry_content}]

        sql = generate_sql(client, system_prompt, messages)

        if is_cannot_answer(sql):
            return _CANNOT_ANSWER_RESULT

        result = execute_sql(con, sql)

        if result["success"]:
            return result

        if attempt < MAX_RETRIES:
            log.warning(
                "Attempt %d failed: %s. Retrying...",
                attempt, result["error"][:100],
            )

    log.error("All 3 attempts exhausted for: %s", question[:50])
    return result


# -- Self-test ----------------------------------------------------------------
if __name__ == "__main__":
    msg = build_retry_message(
        original_question="How many orders on Sundays?",
        failed_sql="SELECT COUNT(*) FROM orderz WHERE order_dow = 0",
        error_message='Table "orderz" does not exist.',
    )
    assert "How many orders on Sundays?" in msg
    assert "SELECT COUNT(*) FROM orderz" in msg
    assert 'Table "orderz" does not exist.' in msg
    assert "Return corrected DuckDB SQL only. No explanation." in msg
    print("build_retry_message assertions passed.")
    print("\nM3.4 OK (live retry test requires API key — run via m3_run.py)")
