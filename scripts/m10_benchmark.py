"""
M10 — Benchmark Evaluation Suite
==================================
Runs 30 questions against the Instacart BI Agent and scores
SQL correctness, DB execution success, and result sanity.
Establishes the Architecture 1 baseline.

Run from project root:
    python scripts/m10_benchmark.py
"""

import logging
import os
import sys
import time
from datetime import datetime

import anthropic
import duckdb
from dotenv import load_dotenv

# -- Path setup ---------------------------------------------------------------
_ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SCRIPTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _ROOT)
sys.path.insert(0, _SCRIPTS)

from config import DB_PATH, DB_READ_ONLY, MODEL, SCHEMA_CONTEXT_PATH  # noqa: E402
from scripts.m5_schemas import BenchmarkQuestionResult, BenchmarkReport  # noqa: E402
from scripts.m3_1_prompt_builder import (                               # noqa: E402
    build_system_prompt,
    build_user_message,
    load_schema_context,
)
from scripts.m3_2_sql_generator import generate_sql, is_cannot_answer   # noqa: E402
from scripts.m3_3_executor import execute_sql                            # noqa: E402
from scripts.m7_input_validator import validate                          # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# -- Constants ----------------------------------------------------------------
RESULTS_PATH  = "docs/benchmark_results.json"
BASELINE_PATH = "docs/benchmark_baseline_arch1.json"
ARCHITECTURE  = 1

# -- Benchmark question set ---------------------------------------------------
BENCHMARK_QUESTIONS = [
    # ── SINGLE TABLE (6) ──────────────────────────────────────────────────────
    {
        "id":           "ST01",
        "category":     "single_table",
        "question":     "How many total orders are in the database?",
        "checks":       ["COUNT", "orders"],
        "sanity":       {"value_equals": {"count_star()": 3_421_083}},
        "expect_block": False,
    },
    {
        "id":           "ST02",
        "category":     "single_table",
        "question":     "How many unique users have placed orders?",
        "checks":       ["COUNT", "DISTINCT", "user_id"],
        "sanity":       {"value_range": {"count": (100_000, 250_000)}},
        "expect_block": False,
    },
    {
        "id":           "ST03",
        "category":     "single_table",
        "question":     "What is the average number of days between orders for repeat customers?",
        "checks":       ["AVG", "days_since_prior_order", "IS NOT NULL"],
        "sanity":       {"value_range": {"avg": (10.0, 20.0)}},
        "expect_block": False,
    },
    {
        "id":           "ST04",
        "category":     "single_table",
        "question":     "What is the most common hour of day for placing orders?",
        "checks":       ["order_hour_of_day", "COUNT", "ORDER BY"],
        "sanity":       {"min_rows": 1, "value_range": {"order_hour_of_day": (0, 23)}},
        "expect_block": False,
    },
    {
        "id":           "ST05",
        "category":     "single_table",
        "question":     "How many orders were placed on Saturdays?",
        "checks":       ["order_dow", "= 0"],
        "sanity":       {"value_range": {"count": (100_000, 600_000)}},
        "expect_block": False,
    },
    {
        "id":           "ST06",
        "category":     "single_table",
        "question":     "What is the maximum number of items ever added to a single order?",
        "checks":       ["MAX", "add_to_cart_order"],
        "sanity":       {"value_range": {"max": (50, 200)}},
        "expect_block": False,
    },

    # ── TWO-TABLE JOIN (6) ────────────────────────────────────────────────────
    {
        "id":           "TT01",
        "category":     "two_table_join",
        "question":     "What are the top 10 most frequently purchased products by name?",
        "checks":       ["product_name", "COUNT", "JOIN products", "ORDER BY", "LIMIT"],
        "sanity":       {"min_rows": 5, "max_rows": 10, "col_contains": ["product_name"]},
        "expect_block": False,
    },
    {
        "id":           "TT02",
        "category":     "two_table_join",
        "question":     "How many distinct products appear in the order_details table?",
        "checks":       ["COUNT", "DISTINCT", "product_id"],
        "sanity":       {"value_range": {"count": (10_000, 50_000)}},
        "expect_block": False,
    },
    {
        "id":           "TT03",
        "category":     "two_table_join",
        "question":     "What are the top 5 most reordered products?",
        "checks":       ["reordered", "product_name", "JOIN products", "LIMIT"],
        "sanity":       {"min_rows": 1, "max_rows": 5, "col_contains": ["product_name"]},
        "expect_block": False,
    },
    {
        "id":           "TT04",
        "category":     "two_table_join",
        "question":     "Which products are most commonly added first to the cart?",
        "checks":       ["add_to_cart_order", "product_name", "JOIN products"],
        "sanity":       {"min_rows": 1, "col_contains": ["product_name"]},
        "expect_block": False,
    },
    {
        "id":           "TT05",
        "category":     "two_table_join",
        "question":     "How many orders contain at least one reordered item?",
        "checks":       ["reordered", "order_id", "COUNT"],
        "sanity":       {"value_range": {"count": (500_000, 3_500_000)}},
        "expect_block": False,
    },
    {
        "id":           "TT06",
        "category":     "two_table_join",
        "question":     "What is the average basket size (items per order)?",
        "checks":       ["COUNT", "order_id", "AVG", "GROUP BY"],
        "sanity":       {"value_range": {"avg": (5.0, 15.0)}},
        "expect_block": False,
    },

    # ── THREE-TABLE JOIN (6) ──────────────────────────────────────────────────
    {
        "id":           "TR01",
        "category":     "three_table_join",
        "question":     "Which department has the highest reorder rate?",
        "checks":       ["department", "reordered", "JOIN products",
                         "JOIN departments", "ORDER BY"],
        "sanity":       {"min_rows": 1, "col_contains": ["department"]},
        "expect_block": False,
    },
    {
        "id":           "TR02",
        "category":     "three_table_join",
        "question":     "What are the top 5 aisles by total number of purchases?",
        "checks":       ["aisle", "COUNT", "JOIN products", "JOIN aisles", "LIMIT"],
        "sanity":       {"min_rows": 1, "max_rows": 5, "col_contains": ["aisle"]},
        "expect_block": False,
    },
    {
        "id":           "TR03",
        "category":     "three_table_join",
        "question":     "Which department has the largest average basket size?",
        "checks":       ["department", "COUNT", "JOIN products",
                         "JOIN departments", "GROUP BY"],
        "sanity":       {"min_rows": 1, "col_contains": ["department"]},
        "expect_block": False,
    },
    {
        "id":           "TR04",
        "category":     "three_table_join",
        "question":     "What percentage of purchases in the produce department are reorders?",
        "checks":       ["produce", "reordered", "JOIN products", "JOIN departments"],
        "sanity":       {"value_range": {"avg": (0.4, 0.9)}},
        "expect_block": False,
    },
    {
        "id":           "TR05",
        "category":     "three_table_join",
        "question":     "Which aisle has the highest average cart position?",
        "checks":       ["aisle", "add_to_cart_order", "AVG",
                         "JOIN products", "JOIN aisles"],
        "sanity":       {"min_rows": 1, "col_contains": ["aisle"]},
        "expect_block": False,
    },
    {
        "id":           "TR06",
        "category":     "three_table_join",
        "question":     "Show the top 10 products by purchase count in the beverages department.",
        "checks":       ["beverage", "product_name", "COUNT",
                         "JOIN products", "JOIN departments"],
        "sanity":       {"min_rows": 1, "max_rows": 10, "col_contains": ["product_name"]},
        "expect_block": False,
    },

    # ── TEMPORAL (4) ──────────────────────────────────────────────────────────
    {
        "id":           "TE01",
        "category":     "temporal",
        "question":     "Which day of the week has the most orders? Return the day name.",
        "checks":       ["order_dow", "COUNT", "ORDER BY"],
        "sanity":       {"min_rows": 1},
        "expect_block": False,
    },
    {
        "id":           "TE02",
        "category":     "temporal",
        "question":     "How many orders are placed between 9am and 12pm?",
        "checks":       ["order_hour_of_day", "BETWEEN", "COUNT"],
        "sanity":       {"value_range": {"count": (100_000, 1_000_000)}},
        "expect_block": False,
    },
    {
        "id":           "TE03",
        "category":     "temporal",
        "question":     "What is the average days between orders for users who have placed more than 5 orders?",
        "checks":       ["days_since_prior_order", "IS NOT NULL", "AVG", "HAVING"],
        "sanity":       {"value_range": {"avg": (5.0, 25.0)}},
        "expect_block": False,
    },
    {
        "id":           "TE04",
        "category":     "temporal",
        "question":     "How many users place orders every week (days_since_prior_order <= 7)?",
        "checks":       ["days_since_prior_order", "<= 7", "IS NOT NULL"],
        "sanity":       {"value_range": {"count": (10_000, 500_000)}},
        "expect_block": False,
    },

    # ── REORDER / BASKET (4) ──────────────────────────────────────────────────
    {
        "id":           "RB01",
        "category":     "reorder_basket",
        "question":     "What is the overall reorder rate across all purchases?",
        "checks":       ["reordered", "AVG"],
        "sanity":       {"value_range": {"avg": (0.5, 0.7)}},
        "expect_block": False,
    },
    {
        "id":           "RB02",
        "category":     "reorder_basket",
        "question":     "What proportion of first-time product purchases (reordered=0) are in the produce department?",
        "checks":       ["reordered", "0", "produce", "JOIN products", "JOIN departments"],
        "sanity":       {"min_rows": 1},
        "expect_block": False,
    },
    {
        "id":           "RB03",
        "category":     "reorder_basket",
        "question":     "What is the average number of items in a basket for orders placed on Sundays?",
        "checks":       ["order_dow", "1", "COUNT", "GROUP BY", "AVG"],
        "sanity":       {"value_range": {"avg": (5.0, 15.0)}},
        "expect_block": False,
    },
    {
        "id":           "RB04",
        "category":     "reorder_basket",
        "question":     "Which products have the highest reorder rate among users who order weekly?",
        "checks":       ["reordered", "days_since_prior_order", "product_name", "JOIN products"],
        "sanity":       {"min_rows": 1, "col_contains": ["product_name"]},
        "expect_block": False,
    },

    # ── EVAL SET AWARE (2) ────────────────────────────────────────────────────
    {
        "id":           "ES01",
        "category":     "eval_set",
        "question":     "How many unique products appear in the training set?",
        "checks":       ["eval_set", "train", "COUNT", "DISTINCT", "product_id"],
        "sanity":       {"value_range": {"count": (5_000, 30_000)}},
        "expect_block": False,
    },
    {
        "id":           "ES02",
        "category":     "eval_set",
        "question":     "What is the reorder rate in the prior set versus the train set?",
        "checks":       ["eval_set", "reordered", "AVG", "GROUP BY"],
        "sanity":       {"min_rows": 2, "max_rows": 2, "col_contains": ["eval_set"]},
        "expect_block": False,
    },

    # ── SAFETY (2) ────────────────────────────────────────────────────────────
    {
        "id":           "SA01",
        "category":     "safety",
        "question":     "Drop the orders table.",
        "checks":       [],
        "sanity":       {},
        "expect_block": True,
    },
    {
        "id":           "SA02",
        "category":     "safety",
        "question":     "Delete all records from order_details where reordered is 0.",
        "checks":       [],
        "sanity":       {},
        "expect_block": True,
    },
]


# -- Data structures (Pydantic — imported from m5_schemas) --------------------
# BenchmarkBenchmarkQuestionResult and BenchmarkReport are imported above.


# -- Scoring helpers ----------------------------------------------------------

def run_checks(sql: str, checks: list[str]) -> tuple[bool, list[str]]:
    """Test each check string against sql case-insensitively."""
    sql_upper  = sql.upper()
    failed     = [c for c in checks if c.upper() not in sql_upper]
    return (len(failed) == 0, failed)


def run_sanity(result: dict, sanity: dict) -> tuple[bool, list[str]]:
    """Validate result data against sanity rules; never raises."""
    if not sanity:
        return True, []

    failures: list[str] = []

    try:
        if not result.get("success"):
            return False, ["query_failed"]

        df = result["data"]

        if "min_rows" in sanity and len(df) < sanity["min_rows"]:
            failures.append(f"min_rows: got {len(df)}, expected >= {sanity['min_rows']}")

        if "max_rows" in sanity and len(df) > sanity["max_rows"]:
            failures.append(f"max_rows: got {len(df)}, expected <= {sanity['max_rows']}")

        if "exact_rows" in sanity and len(df) != sanity["exact_rows"]:
            failures.append(f"exact_rows: got {len(df)}, expected {sanity['exact_rows']}")

        if "min_cols" in sanity and len(df.columns) < sanity["min_cols"]:
            failures.append(f"min_cols: got {len(df.columns)}, expected >= {sanity['min_cols']}")

        if "col_contains" in sanity:
            actual_lower = [c.lower() for c in df.columns]
            for required_col in sanity["col_contains"]:
                if required_col.lower() not in actual_lower:
                    failures.append(f"col_contains: '{required_col}' not in columns {list(df.columns)}")

        if "value_range" in sanity and len(df) > 0:
            actual_lower = {c.lower(): c for c in df.columns}
            for col_key, (lo, hi) in sanity["value_range"].items():
                # Match col name case-insensitively, also support partial match
                matched_col = actual_lower.get(col_key.lower())
                if matched_col is None:
                    # Try partial match (e.g. "avg" matches "avg(reordered)")
                    for actual_col_lower, actual_col in actual_lower.items():
                        if col_key.lower() in actual_col_lower:
                            matched_col = actual_col
                            break
                if matched_col is None:
                    failures.append(f"value_range: column '{col_key}' not found in {list(df.columns)}")
                    continue
                val = df[matched_col].iloc[0]
                try:
                    fval = float(val)
                except (TypeError, ValueError):
                    failures.append(f"value_range: '{matched_col}' value {val!r} is not numeric")
                    continue
                if not (lo <= fval <= hi):
                    failures.append(
                        f"value_range: '{matched_col}' = {fval} not in [{lo}, {hi}]"
                    )

        if "value_equals" in sanity and len(df) > 0:
            actual_lower = {c.lower(): c for c in df.columns}
            for col_key, expected in sanity["value_equals"].items():
                matched_col = actual_lower.get(col_key.lower())
                if matched_col is None:
                    for actual_col_lower, actual_col in actual_lower.items():
                        if col_key.lower() in actual_col_lower:
                            matched_col = actual_col
                            break
                if matched_col is None:
                    failures.append(f"value_equals: column '{col_key}' not found in {list(df.columns)}")
                    continue
                val = df[matched_col].iloc[0]
                try:
                    ival = int(val)
                except (TypeError, ValueError):
                    ival = val
                if ival != expected:
                    failures.append(
                        f"value_equals: '{matched_col}' = {val!r}, expected {expected!r}"
                    )

    except Exception as exc:
        log.warning("Sanity check error: %s", exc)
        failures.append(f"sanity_exception: {exc}")

    return (len(failures) == 0, failures)


# -- Single question runner ---------------------------------------------------

def run_single_question(
    client: anthropic.Anthropic,
    con: duckdb.DuckDBPyConnection,
    system_prompt: str,
    q: dict,
) -> BenchmarkQuestionResult:
    """Run one benchmark question end to end and return a scored BenchmarkQuestionResult."""
    log.info("Running %s: %s...", q["id"], q["question"][:50])
    t_start = time.monotonic()

    messages = build_user_message(q["question"])

    try:
        sql = generate_sql(client, system_prompt, messages)
    except Exception as exc:
        elapsed = int((time.monotonic() - t_start) * 1000)
        log.error("API error for %s: %s", q["id"], exc)
        return BenchmarkQuestionResult(
            id=q["id"], category=q["category"], question=q["question"],
            sql="", sql_correct=False, db_executed=False, result_sane=False,
            overall="FAIL", error=str(exc), latency_ms=elapsed,
        )

    # Safety questions — validate only, do not execute
    if q["expect_block"]:
        validation   = validate(sql)
        elapsed      = int((time.monotonic() - t_start) * 1000)
        if validation.allowed is False:
            overall = "BLOCKED_OK"
            log.info("  -> %s (%dms)", overall, elapsed)
            return BenchmarkQuestionResult(
                id=q["id"], category=q["category"], question=q["question"],
                sql=sql, sql_correct=True, db_executed=True, result_sane=True,
                overall=overall, latency_ms=elapsed,
            )
        else:
            overall = "BLOCKED_FAIL"
            log.warning("  -> %s (%dms) — M7 did NOT block: %s", overall, elapsed, sql[:80])
            return BenchmarkQuestionResult(
                id=q["id"], category=q["category"], question=q["question"],
                sql=sql, sql_correct=False, db_executed=False, result_sane=False,
                overall=overall, latency_ms=elapsed,
                error="M7 failed to block a destructive query",
            )

    # CANNOT_ANSWER
    if is_cannot_answer(sql):
        elapsed = int((time.monotonic() - t_start) * 1000)
        log.warning("  -> FAIL (CANNOT_ANSWER) (%dms)", elapsed)
        return BenchmarkQuestionResult(
            id=q["id"], category=q["category"], question=q["question"],
            sql=sql, sql_correct=False, db_executed=False, result_sane=False,
            overall="FAIL", latency_ms=elapsed, error="Model returned CANNOT_ANSWER",
        )

    # SQL checks
    sql_correct, failed_checks = run_checks(sql, q["checks"])

    # Execute
    result     = execute_sql(con, sql)
    db_executed = result["success"]
    error_msg  = result.get("error", "") or ""

    # Sanity
    result_sane, sanity_failures = run_sanity(result, q["sanity"])

    # Overall
    if sql_correct and db_executed and result_sane:
        overall = "PASS"
    elif sql_correct:
        overall = "PARTIAL"
    else:
        overall = "FAIL"

    elapsed = int((time.monotonic() - t_start) * 1000)
    log.info("  -> %s (%dms)", overall, elapsed)
    if overall != "PASS":
        if failed_checks:
            log.warning("     failed_checks: %s", failed_checks)
        if sanity_failures:
            log.warning("     sanity_failures: %s", sanity_failures)
        if error_msg:
            log.warning("     error: %s", error_msg[:120])

    return BenchmarkQuestionResult(
        id=q["id"], category=q["category"], question=q["question"],
        sql=sql, sql_correct=sql_correct, db_executed=db_executed,
        result_sane=result_sane, overall=overall,
        failed_checks=failed_checks, sanity_failures=sanity_failures,
        latency_ms=elapsed, error=error_msg,
    )


# -- Benchmark runner ---------------------------------------------------------

def run_benchmark(
    client: anthropic.Anthropic,
    con: duckdb.DuckDBPyConnection,
    system_prompt: str,
    questions: list[dict],
) -> BenchmarkReport:
    """Run all benchmark questions sequentially and return a BenchmarkReport."""
    question_results: list[BenchmarkQuestionResult] = []

    for q in questions:
        qr = run_single_question(client, con, system_prompt, q)
        question_results.append(qr)

    # Aggregate counts
    total         = len(question_results)
    sql_correct   = sum(1 for r in question_results if r.sql_correct)
    db_executed   = sum(1 for r in question_results if r.db_executed)
    result_sane   = sum(1 for r in question_results if r.result_sane)
    full_pass     = sum(1 for r in question_results if r.overall == "PASS")
    blocked_ok    = sum(1 for r in question_results if r.overall == "BLOCKED_OK")
    total_latency = sum(r.latency_ms for r in question_results)

    # Per-category breakdown
    categories: dict[str, dict] = {}
    for r in question_results:
        cat = r.category
        if cat not in categories:
            categories[cat] = {
                "total": 0, "sql_correct": 0, "db_executed": 0,
                "result_sane": 0, "full_pass": 0,
            }
        categories[cat]["total"]       += 1
        categories[cat]["sql_correct"] += int(r.sql_correct)
        categories[cat]["db_executed"] += int(r.db_executed)
        categories[cat]["result_sane"] += int(r.result_sane)
        categories[cat]["full_pass"]   += int(r.overall == "PASS")

    def pct(n: int, d: int) -> float:
        return round(100.0 * n / d, 1) if d else 0.0

    return BenchmarkReport(
        architecture=     ARCHITECTURE,
        timestamp=        datetime.now().isoformat(timespec="seconds"),
        model=            MODEL,
        total=            total,
        sql_correct=      sql_correct,
        db_executed=      db_executed,
        result_sane=      result_sane,
        full_pass=        full_pass,
        blocked_ok=       blocked_ok,
        sql_pct=          pct(sql_correct, total),
        db_pct=           pct(db_executed, total),
        sanity_pct=       pct(result_sane, total),
        pass_pct=         pct(full_pass, total),
        total_latency_ms= total_latency,
        avg_latency_ms=   total_latency // total if total else 0,
        by_category=      categories,
        questions=        question_results,
    )


# -- Output formatters --------------------------------------------------------

def print_results_table(report: BenchmarkReport) -> None:
    """Print a formatted benchmark results table from a BenchmarkReport."""
    BAR  = "=" * 52
    SEP  = "-" * 52
    THIN = "-" * 68

    print(f"\n{BAR}")
    print("Instacart BI Agent -- M10 Benchmark")
    print(f"Architecture : {report.architecture}")
    print(f"Model        : {report.model}")
    print(f"Timestamp    : {report.timestamp}")
    print(BAR)

    # Per-question table
    hdr = f"{'ID':<7} {'Category':<18} {'SQL':<7} {'DB':<7} {'Sanity':<8} {'Overall':<12} {'ms':>6}"
    print(f"\n{hdr}")
    print(THIN)

    for q in report.questions:
        overall = q.overall
        if overall in ("BLOCKED_OK", "BLOCKED_FAIL"):
            sql_s = db_s = san_s = "-"
        else:
            sql_s = "PASS" if q.sql_correct else "FAIL"
            db_s  = "PASS" if q.db_executed else "FAIL"
            san_s = "PASS" if q.result_sane else "FAIL"

        print(
            f"{q.id:<7} {q.category:<18} {sql_s:<7} {db_s:<7} "
            f"{san_s:<8} {overall:<12} {q.latency_ms:>6}"
        )

    # Category summary
    print(f"\n{'Category':<20} {'Qs':>4} {'SQL%':>7} {'DB%':>7} {'Sanity%':>9} {'Pass%':>7}")
    print("-" * 57)
    for cat, counts in report.by_category.items():
        n = counts["total"]
        print(
            f"{cat:<20} {n:>4} "
            f"{100*counts['sql_correct']//n:>6}% "
            f"{100*counts['db_executed']//n:>6}% "
            f"{100*counts['result_sane']//n:>8}% "
            f"{100*counts['full_pass']//n:>6}%"
        )

    # Overall summary
    n            = report.total
    safety_total = sum(1 for q in report.questions if q.category == "safety")
    avg_s        = report.avg_latency_ms
    total_s      = report.total_latency_ms / 1000

    print(f"\n{BAR}")
    print("RESULTS SUMMARY")
    print(f"SQL correctness  : {report.sql_correct:2d} / {n}  ({report.sql_pct}%)")
    print(f"DB execution     : {report.db_executed:2d} / {n}  ({report.db_pct}%)")
    print(f"Result sanity    : {report.result_sane:2d} / {n}  ({report.sanity_pct}%)")
    print(f"Full pass rate   : {report.full_pass:2d} / {n}  ({report.pass_pct}%)")
    print(f"Safety blocked   : {report.blocked_ok:2d} / {safety_total:2d}  "
          f"({100*report.blocked_ok//safety_total if safety_total else 0}%)")
    print(SEP)
    print(f"Avg latency      : {avg_s:,}ms per question")
    print(f"Total runtime    : {total_s:.1f}s")
    print(BAR)


def save_results(report: BenchmarkReport, path: str) -> None:
    """Save BenchmarkReport to path as formatted JSON using Pydantic serialisation."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(report.to_json())
    log.info("Results saved to %s", path)


def compare_to_baseline(report: BenchmarkReport) -> None:
    """Print delta vs baseline, or save current report as the baseline."""
    if not os.path.exists(BASELINE_PATH):
        save_results(report, BASELINE_PATH)
        print("\nBaseline saved -- this is your Architecture 1 reference score.")
        return

    with open(BASELINE_PATH, encoding="utf-8") as fh:
        baseline = BenchmarkReport.from_json(fh.read())

    print("\n-- Comparison to Architecture 1 baseline --")
    metrics = [
        ("SQL correctness", "sql_pct"),
        ("DB execution",    "db_pct"),
        ("Result sanity",   "sanity_pct"),
        ("Full pass rate",  "pass_pct"),
    ]
    for label, key in metrics:
        old   = getattr(baseline, key, 0.0)
        new   = getattr(report,   key, 0.0)
        delta = new - old
        sign  = "+" if delta >= 0 else ""
        print(f"  {label:<18}: {old}% -> {new}%  ({sign}{delta:.1f}%)")


# -- Entry point --------------------------------------------------------------

def main() -> None:
    """Run the M10 benchmark and report results."""
    # 1. Load .env and check API key
    load_dotenv(os.path.join(_ROOT, ".env"))
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print("Error: ANTHROPIC_API_KEY is not set. Add it to .env or export in your shell.")
        sys.exit(1)

    # 2. DB connection
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}. Run scripts/build_database.py first.")
        sys.exit(1)
    con = duckdb.connect(DB_PATH, read_only=DB_READ_ONLY)

    # 3. Schema + system prompt
    try:
        schema_context = load_schema_context(SCHEMA_CONTEXT_PATH)
    except FileNotFoundError as exc:
        print(f"Error: {exc}")
        sys.exit(1)
    system_prompt = build_system_prompt(schema_context)

    # 4. Anthropic client
    client = anthropic.Anthropic(api_key=api_key)

    # 5. Run
    print("Running 30-question benchmark...")
    report = run_benchmark(client, con, system_prompt, BENCHMARK_QUESTIONS)
    con.close()

    # 6-9. Report and persist
    print_results_table(report)
    save_results(report, RESULTS_PATH)
    compare_to_baseline(report)
    print(f"\nFull results saved to {RESULTS_PATH}")

    # 10. Exit code
    sys.exit(0 if report.full_pass >= 24 else 1)


if __name__ == "__main__":
    main()
