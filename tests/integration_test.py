"""
M3 Integration Test
====================
Full end-to-end pipeline test against the real DuckDB and Claude API.
Runs as a plain Python script — does NOT use pytest.

Run with:
    python tests/integration_test.py
"""

import os
import sys

# Make scripts/ importable
_ROOT    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_SCRIPTS = os.path.join(_ROOT, "scripts")
sys.path.insert(0, _SCRIPTS)

import anthropic
import duckdb
from dotenv import load_dotenv

load_dotenv(os.path.join(_ROOT, ".env"))

from m3_1_prompt_builder import build_system_prompt, load_schema_context
from m3_4_error_recovery import attempt_with_retry

# -- Constants ----------------------------------------------------------------
DB_PATH             = os.path.join(_ROOT, "models", "instacart.db")
SCHEMA_CONTEXT_PATH = os.path.join(_ROOT, "docs",   "schema_context.txt")
MODEL               = "claude-sonnet-4-6"

# -- Test cases ---------------------------------------------------------------
INTEGRATION_TESTS = [
    {
        "name":           "order_count",
        "question":       "How many total orders are in the database?",
        "expect_success": True,
        "sql_contains":   ["COUNT", "orders"],
        "result_checks":  [
            # orders table = 3,421,083 (all eval_sets incl. test)
            # order_details = 3,346,083 (prior+train only — test orders have no products)
            # Accept either valid count
            lambda df: df.iloc[0, 0] in (3_421_083, 3_346_083),
        ],
    },
    {
        "name":           "sunday_orders",
        "question":       "How many orders were placed on Sundays?",
        "expect_success": True,
        "sql_contains":   ["order_dow", "1"],
        "result_checks":  [
            lambda df: df.iloc[0, 0] > 100_000,
        ],
    },
    {
        "name":           "avg_days_between_orders",
        "question":       "What is the average days between orders?",
        "expect_success": True,
        "sql_contains":   ["days_since_prior_order", "IS NOT NULL", "AVG"],
        "result_checks":  [
            lambda df: 5.0 < float(df.iloc[0, 0]) < 30.0,
        ],
    },
    {
        "name":           "top_products",
        "question":       "What are the top 5 most purchased products?",
        "expect_success": True,
        "sql_contains":   ["product_name", "COUNT", "LIMIT"],
        "result_checks":  [
            lambda df: len(df) <= 5,
            lambda df: (
                "product_name" in df.columns.str.lower().tolist()
                or df.shape[1] >= 2
            ),
        ],
    },
    {
        "name":           "department_reorder",
        "question":       "Which department has the highest reorder rate?",
        "expect_success": True,
        "sql_contains":   ["department", "reordered"],
        "result_checks":  [
            lambda df: len(df) >= 1,
            lambda df: df.shape[1] >= 2,
        ],
    },
    {
        "name":           "cannot_answer",
        "question":       "What is the weather in New York today?",
        "expect_success": False,
        "sql_contains":   [],
        "result_checks":  [],
    },
]


# -- Functions ----------------------------------------------------------------

def run_single_test(client, con, system_prompt: str, test_case: dict) -> dict:
    """Run one test case through the full M3 pipeline; return pass/fail dict."""
    name           = test_case["name"]
    question       = test_case["question"]
    expect_success = test_case["expect_success"]
    sql_checks     = test_case["sql_contains"]
    result_checks  = test_case["result_checks"]
    failures       = []

    result = attempt_with_retry(client, con, system_prompt, question)
    sql    = result.get("sql", "")

    # Check success expectation
    if result["success"] != expect_success:
        failures.append(
            f"Expected success={expect_success}, got {result['success']}"
            + (f" (error: {result['error']})" if result.get("error") else "")
        )

    # Check SQL contains required tokens
    sql_lower = sql.lower()
    for token in sql_checks:
        if token.lower() not in sql_lower:
            failures.append(f"SQL missing: {token!r}")

    # Run result_checks only when the pipeline succeeded as expected
    if expect_success and result["success"]:
        df = result["data"]
        for i, check_fn in enumerate(result_checks):
            try:
                if not check_fn(df):
                    failures.append(f"result_check[{i}] returned False")
            except Exception as exc:
                failures.append(f"result_check[{i}] raised {exc}")

    return {
        "name":     name,
        "passed":   len(failures) == 0,
        "sql":      sql,
        "failures": failures,
    }


def main() -> None:
    """Orchestrate integration tests; exit 0 on all-pass, 1 on any failure."""
    # 1. Check API key
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print(
            "Error: ANTHROPIC_API_KEY is not set.\n"
            "Add it to .env or export it in your shell."
        )
        sys.exit(1)

    # 2. Load schema context and build system prompt
    schema_context = load_schema_context(SCHEMA_CONTEXT_PATH)
    system_prompt  = build_system_prompt(schema_context)

    # 3. Connect to database
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}")
        print("Run scripts/build_database.py first.")
        sys.exit(1)
    con = duckdb.connect(DB_PATH, read_only=True)

    # 4. Initialise Anthropic client
    client = anthropic.Anthropic(api_key=api_key)

    # 5. Run tests
    outcomes = []
    for tc in INTEGRATION_TESTS:
        print(f"  Running: {tc['name']} ...", flush=True)
        outcome = run_single_test(client, con, system_prompt, tc)
        outcomes.append(outcome)

    con.close()

    # 6. Results table
    name_w = max(len(o["name"]) for o in outcomes)
    sep    = "-" * (name_w + 36)
    print(f"\n{sep}")
    print(f"{'Test name':<{name_w}} | {'Result':^7} | Failures")
    print(sep)
    for o in outcomes:
        label    = "PASS" if o["passed"] else "FAIL"
        failures = "; ".join(o["failures"])
        print(f"{o['name']:<{name_w}} | {label:^7} | {failures}")
    print(sep)

    # 7. Summary
    passed = sum(1 for o in outcomes if o["passed"])
    failed = len(outcomes) - passed
    print(f"\nTotal : {len(outcomes)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    # 8. Exit code
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
