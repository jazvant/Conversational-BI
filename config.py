"""
config.py — Single source of truth for all dataset-specific configuration.
Swapping datasets = swapping this file.
All paths are relative to the project root (run everything from there).
"""

import sys as _sys
import os as _os
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

# -- Paths --------------------------------------------------------------------
DB_PATH             = "models/instacart.db"
SCHEMA_CONTEXT_PATH = "docs/schema_context.txt"
METADATA_PATH       = "docs/schema_metadata.txt"

# -- Model settings -----------------------------------------------------------
MODEL      = "claude-sonnet-4-6"
MAX_TOKENS = 600

# -- Tables -------------------------------------------------------------------
TABLES = [
    "orders",
    "order_details",
    "order_products_prior",
    "order_products_train",
    "products",
    "aisles",
    "departments",
]

# -- Foreign key relationships ------------------------------------------------
FK_RELATIONSHIPS = {
    "order_products_prior": {
        "order_id":   ("orders",   "order_id"),
        "product_id": ("products", "product_id"),
    },
    "order_products_train": {
        "order_id":   ("orders",   "order_id"),
        "product_id": ("products", "product_id"),
    },
    "order_details": {
        "order_id":   ("orders",   "order_id"),
        "product_id": ("products", "product_id"),
    },
    "products": {
        "aisle_id":      ("aisles",       "aisle_id"),
        "department_id": ("departments",  "department_id"),
    },
}

# -- Table size hints ---------------------------------------------------------
TABLE_SIZE_HINTS = {
    "orders":               "MEDIUM — 3.4M rows",
    "order_products_prior": "LARGE — 32M rows, avoid SELECT *",
    "order_products_train": "MEDIUM — 1.4M rows",
    "order_details":        "LARGE — 33.8M rows, primary query surface",
    "products":             "SMALL — 50K rows",
    "aisles":               "TINY — 134 rows",
    "departments":          "TINY — 21 rows",
}

# -- Semantic notes injected into schema prompt -------------------------------
SEMANTIC_NOTES = {
    "order_dow": (
        "0=Saturday, 1=Sunday, 2=Monday, 3=Tuesday, "
        "4=Wednesday, 5=Thursday, 6=Friday"
    ),
    "reordered":              "1=reordered, 0=first time purchase",
    "eval_set":               "prior, train, test",
    "days_since_prior_order": "NULL for a user's first ever order",
}

# -- Query rules injected into schema prompt ----------------------------------
QUERY_RULES = [
    "Use order_details as the primary fact table for all product-level queries.",
    "Never query order_products_prior or order_products_train directly "
    "unless analysing the prior/train split.",
    "Always write explicit JOINs to reach product_name, aisle, or department "
    "— there is no pre-joined flat view.",
    "Add WHERE days_since_prior_order IS NOT NULL for any query aggregating "
    "or filtering on that column.",
    "Never use SELECT * on order_details, order_products_prior, or "
    "order_products_train.",
    "order_dow encoding: 0=Saturday, 1=Sunday, 2=Monday, 3=Tuesday, "
    "4=Wednesday, 5=Thursday, 6=Friday.",
    "reordered is INTEGER: 1=reordered, 0=first time purchase.",
    "eval_set values: prior, train, test. test orders have no rows in "
    "order_products tables.",
]

# -- Memory settings ----------------------------------------------------------
MAX_HISTORY_TURNS     = 3   # default rolling window (3 turns = 6 messages)
MAX_HISTORY_TURNS_EXT = 5   # extended window for multi-step patterns
SUMMARY_ROW_LIMIT     = 10  # max rows included in result summary

# -- Multi-step trigger keywords — expand history window when detected --------
MULTISTEP_KEYWORDS = [
    "compare", "versus", "vs", "difference between",
    "trend", "over time", "step by step", "first", "then",
    "next", "after that", "following", "subsequently",
]

# ── Security settings ──────────────────────────────────────

# DuckDB connection mode
DB_READ_ONLY = True   # always True in production

# Memory cap for DuckDB (applies per connection)
DUCKDB_MEMORY_LIMIT = "2GB"

# Query execution timeout in seconds
QUERY_TIMEOUT_SECONDS = 15

# Blocked SQL statement types — caught before DB execution
# Read-only connection handles writes, but this gives early
# rejection with a clear user-facing message
BLOCKED_DDL = {
    "DROP", "CREATE", "ALTER", "TRUNCATE",
}
BLOCKED_DML = {
    "DELETE", "INSERT", "UPDATE", "MERGE", "REPLACE",
}
BLOCKED_FILE_OPS = {
    "COPY", "EXPORT", "IMPORT",
}

# All blocked keywords combined
BLOCKED_KEYWORDS: set[str] = (
    BLOCKED_DDL | BLOCKED_DML | BLOCKED_FILE_OPS
)

# NaN threshold — flag result if any column exceeds this
# null percentage
NULL_FLAG_THRESHOLD = 0.20   # 20%

# Generic error shown to users — never expose internals
GENERIC_ERROR_MESSAGE = (
    "The query could not be completed. "
    "Please try rephrasing your question."
)

# Error substrings that are safe to show users
# (descriptive without leaking schema internals)
SAFE_ERROR_SUBSTRINGS = [
    "syntax error",
    "column not found",
    "table not found",
    "ambiguous column",
    "division by zero",
    "type mismatch",
    "time limit",
]

# ── Architecture settings ──────────────────────────────────
from scripts.m5_schemas import PlannerOutput, CriticOutput  # noqa: E402, F401

ARCHITECTURE = 2

INTENT_DATA_QUERY     = "data_query"
INTENT_CONVERSATIONAL = "conversational"
INTENT_MULTISTEP      = "multistep"
INTENT_CANNOT_ANSWER  = "cannot_answer"

CRITIC_MAX_TOKENS  = 400
NARRATIVE_MIN_ROWS = 2

PLANNER_SYSTEM_PROMPT = """
You are the query planner for a conversational BI agent
built on the Instacart grocery dataset.

Classify the user's intent. Use the recent conversation
context to determine whether the current question is a
follow-up or a new query.

Classification rules:
- data_query:     user wants data from the database.
                  Most analytical questions fall here.
- conversational: user wants explanation or analysis of
                  a prior result. Triggered by: why,
                  explain, what does that mean, tell me
                  more, is that good, how does that compare.
- multistep:      user asks for two or more distinct data
                  queries. Triggered by: first X then Y,
                  compare A with B, show X and also Y.
- cannot_answer:  question has nothing to do with grocery
                  purchasing data.
"""

CRITIC_SYSTEM_PROMPT = """
You are the insight analyst for a conversational BI agent.
You will be given a user question and the data result.
Provide a concise structured insight.

Rules:
- Use actual numbers from the data — never be vague.
- Keep each field to 1-2 sentences maximum.
- answer must directly respond to the question asked.
- finding must include a specific number or comparison.
- caveat must say exactly 'None.' if there are no issues.
- followup must be a complete question ending with '?'.
- Total response must be under 120 words across all fields.
"""
