"""
Tests for M7 Input Validator — SQL safety checks.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts")))

from m7_input_validator import validate


# -- Allowed queries ----------------------------------------------------------

def test_select_allowed():
    r = validate("SELECT COUNT(*) FROM orders")
    assert r.allowed is True
    assert r.reason == ""


def test_leading_comment_stripped():
    r = validate("-- get count\nSELECT COUNT(*) FROM orders")
    assert r.allowed is True


# -- Blocked DDL --------------------------------------------------------------

def test_drop_blocked():
    r = validate("DROP TABLE orders")
    assert r.allowed is False
    assert "DROP" in r.reason


def test_truncate_blocked():
    r = validate("TRUNCATE TABLE orders")
    assert r.allowed is False


# -- Blocked DML --------------------------------------------------------------

def test_delete_blocked():
    r = validate("DELETE FROM orders WHERE 1=1")
    assert r.allowed is False


def test_insert_blocked():
    r = validate("INSERT INTO orders VALUES (1,2,3)")
    assert r.allowed is False


# -- Blocked file ops ---------------------------------------------------------

def test_copy_blocked():
    r = validate("COPY orders TO '/tmp/out.csv'")
    assert r.allowed is False


# -- Inline blocked keyword ---------------------------------------------------

def test_inline_drop_blocked():
    r = validate("SELECT 1; DROP TABLE orders")
    assert r.allowed is False


# -- Empty SQL ----------------------------------------------------------------

def test_empty_sql_blocked():
    assert validate("").allowed is False
    assert validate("   ").allowed is False


# -- Suspicious structure -----------------------------------------------------

def test_block_comment_suspicious():
    r = validate("SELECT /* DROP */ COUNT(*) FROM orders")
    assert r.allowed is False


def test_multiple_statements_blocked():
    r = validate("SELECT 1; SELECT 2")
    assert r.allowed is False


def test_stacked_unions_blocked():
    sql = (
        "SELECT 1 UNION SELECT 2 UNION SELECT 3 "
        "UNION SELECT 4 UNION SELECT 5"
    )
    r = validate(sql)
    assert r.allowed is False


# -- Case insensitivity -------------------------------------------------------

def test_case_insensitive_drop():
    r = validate("drop table orders")
    assert r.allowed is False


def test_case_insensitive_delete():
    r = validate("Delete from orders")
    assert r.allowed is False
