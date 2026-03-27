"""
M2.4 — Prompt Validation
=========================
Sends each benchmark question to Claude with schema_context.txt as the
system prompt, checks that the generated SQL contains expected tokens,
and executes every query against instacart.db to confirm it is runnable.

Run with:
    .venv/Scripts/python scripts/m2_4_validate_prompt.py
"""

import logging
import os
import sys

import anthropic
import duckdb
from dotenv import load_dotenv

# Load .env from the project root (handles files without a trailing newline)
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# -- Constants ----------------------------------------------------------------
SCHEMA_CONTEXT_PATH = os.path.join(_ROOT, "docs",   "schema_context.txt")
DB_PATH             = os.path.join(_ROOT, "models", "instacart.db")
MODEL               = "claude-sonnet-4-6"
MAX_TOKENS          = 400

# -- Benchmark suite ----------------------------------------------------------
BENCHMARK_QUESTIONS = [
    {
        "category": "single_table",
        "question": "How many orders were placed on Sundays?",
        "checks":   ["order_dow", "= 1"],
    },
    {
        "category": "single_table",
        "question": "What is the most popular hour of day for orders?",
        "checks":   ["order_hour_of_day", "order_details", "COUNT"],
    },
    {
        "category": "null_handling",
        "question": "What is the average number of days between orders?",
        "checks":   ["days_since_prior_order", "IS NOT NULL", "AVG"],
    },
    {
        "category": "two_table_join",
        "question": "What are the top 10 most frequently purchased products?",
        "checks":   ["order_details", "JOIN products", "product_name",
                     "COUNT", "LIMIT 10"],
    },
    {
        "category": "three_table_join",
        "question": "Which department has the highest reorder rate?",
        "checks":   ["order_details", "JOIN products", "JOIN departments",
                     "reordered", "department"],
    },
    {
        "category": "three_table_join",
        "question": "What are the top 5 aisles by average basket size?",
        "checks":   ["order_details", "JOIN products", "JOIN aisles",
                     "aisle", "COUNT"],
    },
    {
        "category": "eval_set_awareness",
        "question": "How many unique products appear in the training set?",
        "checks":   ["eval_set", "train", "product_id", "COUNT"],
    },
    {
        "category": "dow_encoding",
        "question": (
            "Which day of the week has the most orders? "
            "Return the day name not the number."
        ),
        "checks":   ["order_dow", "Saturday"],
    },
    {
        "category": "reorder_encoding",
        "question": "What percentage of product purchases are reorders?",
        "checks":   ["reordered", "order_details"],
    },
    {
        "category": "large_table_safety",
        "question": "Show me all columns from order_details.",
        "checks":   ["LIMIT"],
    },
]

# Instruction appended to the system prompt on every call
_SQL_INSTRUCTION = (
    "Return a single DuckDB SQL query only. "
    "No markdown fences. No explanation. No comments."
)


# -- Functions ----------------------------------------------------------------

def load_schema_context(path: str) -> str:
    """Read schema_context.txt and return its contents as a string."""
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"schema_context.txt not found at {path}. "
            "Run scripts/m2_run.py first to generate it."
        )
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def generate_sql(client, schema_context: str, question: str) -> str:
    """Send question to Claude with schema_context as system prompt; return raw SQL."""
    system_prompt = f"{schema_context}\n\n{_SQL_INSTRUCTION}"
    response = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=system_prompt,
        messages=[{"role": "user", "content": question}],
    )
    raw = response.content[0].text.strip()

    # Strip markdown fences if the model emits them despite the instruction
    if raw.startswith("```"):
        lines = raw.splitlines()
        # Drop opening fence (and optional language tag) and closing fence
        inner = [
            ln for ln in lines
            if not ln.strip().startswith("```")
        ]
        raw = "\n".join(inner).strip()

    return raw


def check_sql(sql: str, checks: list) -> tuple:
    """Return (all_passed, failed_checks) by testing each check against sql."""
    sql_lower = sql.lower()
    failed = [c for c in checks if c.lower() not in sql_lower]
    return (len(failed) == 0, failed)


def run_sql_against_db(con, sql: str) -> tuple:
    """Execute sql against instacart.db; return (True, '') or (False, error_msg)."""
    try:
        con.execute(sql)
        return (True, "")
    except Exception as exc:
        return (False, str(exc))


def validate_all(client, con, schema_context: str, benchmark: list) -> dict:
    """Run the full validate pipeline for every benchmark question."""
    results = []

    for item in benchmark:
        category = item["category"]
        question = item["question"]
        checks   = item["checks"]

        log.info("Processing [%s]: %s", category, question[:70])

        sql           = generate_sql(client, schema_context, question)
        checks_passed, failed_checks = check_sql(sql, checks)
        db_ok, db_err = run_sql_against_db(con, sql)

        if not checks_passed:
            log.warning(
                "[%s] Check failures — missing: %s",
                category, ", ".join(failed_checks)
            )
        if not db_ok:
            log.error("[%s] DB execution failed: %s", category, db_err[:120])

        results.append({
            "category":      category,
            "question":      question,
            "sql":           sql,
            "checks_passed": checks_passed,
            "db_executed":   db_ok,
            "failed_checks": failed_checks,
            "db_error":      db_err,
        })

    passed = sum(1 for r in results if r["checks_passed"] and r["db_executed"])
    return {
        "total":   len(results),
        "passed":  passed,
        "failed":  len(results) - passed,
        "results": results,
    }


# -- Main ---------------------------------------------------------------------

def main() -> None:
    """Entrypoint: load context, run benchmark, print results, exit on failure."""
    # 1. Load schema context
    schema_context = load_schema_context(SCHEMA_CONTEXT_PATH)

    # 2. Connect to DB (read-only — we only want to validate queries, not mutate)
    if not os.path.exists(DB_PATH):
        log.error("Database not found at %s — run build_database.py first.", DB_PATH)
        sys.exit(1)
    con = duckdb.connect(DB_PATH, read_only=True)

    # 3. Initialise Anthropic client
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        log.error(
            "ANTHROPIC_API_KEY is not set. "
            "Add it to .env or export it in your shell."
        )
        sys.exit(1)
    client = anthropic.Anthropic(api_key=api_key)

    # 4. Run benchmark
    outcome = validate_all(client, con, schema_context, BENCHMARK_QUESTIONS)
    con.close()

    results = outcome["results"]

    # 5. Print results table
    cat_w  = max(len(r["category"]) for r in results)
    sep    = "-" * (cat_w + 42)
    header = f"{'Category':<{cat_w}} | {'Checks':^6} | {'DB Run':^6} | Failed checks"

    print(f"\n{sep}")
    print(header)
    print(sep)

    for r in results:
        chk_label = "PASS" if r["checks_passed"] else "FAIL"
        db_label  = "PASS" if r["db_executed"]   else "FAIL"
        failed    = ", ".join(r["failed_checks"])
        db_note   = r["db_error"][:35] if not r["db_executed"] else ""
        tail      = failed or db_note or ""
        print(f"{r['category']:<{cat_w}} | {chk_label:^6} | {db_label:^6} | {tail}")

    print(sep)

    # 6. Summary
    checks_ok = sum(1 for r in results if r["checks_passed"])
    db_ok     = sum(1 for r in results if r["db_executed"])
    total     = outcome["total"]

    print(f"\nTotal questions : {total}")
    print(f"Checks passed   : {checks_ok} / {total}")
    print(f"DB executed     : {db_ok} / {total}")
    print()

    # 7. Outcome
    if outcome["passed"] >= 9:
        print("M2.4 PASSED — schema_context.txt is ready for M3")
    else:
        print("M2.4 FAILED — fix the following in m2_3_prompt_builder.py:")
        for r in results:
            if not r["checks_passed"] or not r["db_executed"]:
                short_q = r["question"][:70]
                print(f"\n  [{r['category']}] {short_q}")
                if r["failed_checks"]:
                    print(f"    Missing checks : {', '.join(r['failed_checks'])}")
                if not r["db_executed"]:
                    print(f"    DB error       : {r['db_error'][:100]}")
        sys.exit(1)


if __name__ == "__main__":
    main()
