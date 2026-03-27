"""
Unit tests for m3_4_error_recovery.py.
Tests build_retry_message without API calls.
attempt_with_retry is covered in integration_test.py.
"""

import os
import sys

_ROOT    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_SCRIPTS = os.path.join(_ROOT, "scripts")
sys.path.insert(0, _SCRIPTS)

from m3_4_error_recovery import build_retry_message

_Q   = "How many orders?"
_SQL = "SELECT bad"
_ERR = "syntax error"


def _msg():
    return build_retry_message(_Q, _SQL, _ERR)


# -- build_retry_message ------------------------------------------------------

def test_build_retry_message_contains_question():
    assert _Q in _msg()


def test_build_retry_message_contains_failed_sql():
    assert _SQL in _msg()


def test_build_retry_message_contains_error():
    assert _ERR in _msg()


def test_build_retry_message_contains_instruction():
    msg = _msg()
    assert "corrected" in msg.lower()
    assert "SQL" in msg
