"""
M7 — Input Validator
=====================
Validates SQL strings before they reach DuckDB.
Catches destructive statements, file operations, and
suspiciously structured queries.
Never modifies the SQL — only accepts or rejects it.
"""

import re
import logging
from dataclasses import dataclass

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import BLOCKED_KEYWORDS, QUERY_TIMEOUT_SECONDS  # noqa: F401

log = logging.getLogger(__name__)


# -- Data structures ----------------------------------------------------------

@dataclass
class ValidationResult:
    """Result of a SQL validation check."""
    allowed: bool
    reason:  str


# -- Helpers ------------------------------------------------------------------

def extract_leading_keyword(sql: str) -> str:
    """Extract the first meaningful SQL keyword, stripping leading comments."""
    # Remove /* ... */ block comments
    cleaned = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    # Remove -- line comments
    cleaned = re.sub(r"--[^\n]*", " ", cleaned)
    # Strip whitespace and grab first word
    stripped = cleaned.strip()
    if not stripped:
        return ""
    return stripped.split()[0].upper()


def is_blocked_keyword(sql: str) -> bool:
    """Return True if the leading SQL keyword is in BLOCKED_KEYWORDS."""
    return extract_leading_keyword(sql) in BLOCKED_KEYWORDS


def contains_inline_blocked_keyword(sql: str) -> bool:
    """Return True if any blocked keyword appears as a standalone word anywhere in the SQL."""
    sql_upper = sql.upper()
    for keyword in BLOCKED_KEYWORDS:
        pattern = r"\b" + re.escape(keyword) + r"\b"
        if re.search(pattern, sql_upper):
            log.warning("Inline blocked keyword detected: %s", keyword)
            return True
    return False


def is_suspicious_structure(sql: str) -> bool:
    """Return True if SQL contains multiple statements, inline comments, or stacked UNIONs."""
    stripped = sql.strip()

    # Multiple statements: semicolon not at the very end
    without_trailing = stripped.rstrip(";").rstrip()
    if ";" in without_trailing:
        log.warning("Suspicious structure: multiple statements detected")
        return True

    # Comment-based obfuscation: /* anywhere, or -- after the first line
    if "/*" in sql:
        log.warning("Suspicious structure: block comment detected")
        return True
    lines = sql.splitlines()
    if len(lines) > 1:
        for line in lines[1:]:
            if "--" in line:
                log.warning("Suspicious structure: inline comment on non-first line")
                return True

    # Stacked UNION chains (more than 3 UNION keywords)
    union_count = len(re.findall(r"\bUNION\b", sql, flags=re.IGNORECASE))
    if union_count > 3:
        log.warning("Suspicious structure: %d UNION keywords detected", union_count)
        return True

    return False


def validate(sql: str) -> ValidationResult:
    """Master validation function — runs all safety checks in order."""
    # 1. Empty or whitespace-only SQL
    if not sql or not sql.strip():
        log.warning("Validation blocked: empty query")
        return ValidationResult(allowed=False, reason="Empty query received.")

    # 2. Blocked leading keyword
    if is_blocked_keyword(sql):
        keyword = extract_leading_keyword(sql)
        reason  = f"Statement type '{keyword}' is not permitted."
        log.warning("Validation blocked: %s", reason)
        return ValidationResult(allowed=False, reason=reason)

    # 3. Inline blocked keyword
    if contains_inline_blocked_keyword(sql):
        reason = "Query contains a prohibited operation."
        log.warning("Validation blocked: %s", reason)
        return ValidationResult(allowed=False, reason=reason)

    # 4. Suspicious structure
    if is_suspicious_structure(sql):
        reason = "Query structure is not permitted."
        log.warning("Validation blocked: %s", reason)
        return ValidationResult(allowed=False, reason=reason)

    # 5. All checks passed
    log.info("Validation passed for SQL: %.80s", sql.strip())
    return ValidationResult(allowed=True, reason="")


# -- Self-test ----------------------------------------------------------------
if __name__ == "__main__":
    r = validate("SELECT COUNT(*) FROM orders")
    assert r.allowed is True and r.reason == "", r

    r = validate("DROP TABLE orders")
    assert r.allowed is False and "DROP" in r.reason, r

    r = validate("DELETE FROM orders WHERE 1=1")
    assert r.allowed is False, r

    r = validate("SELECT * FROM orders; DROP TABLE orders")
    assert r.allowed is False, r

    r = validate("COPY orders TO '/tmp/out.csv'")
    assert r.allowed is False, r

    r = validate("SELECT 1; SELECT 2")
    assert r.allowed is False, r

    r = validate("-- get count\nSELECT COUNT(*) FROM orders")
    assert r.allowed is True, r

    r = validate("drop table orders")
    assert r.allowed is False, r

    r = validate("")
    assert r.allowed is False, r

    print("M7 input validator self-test OK")
