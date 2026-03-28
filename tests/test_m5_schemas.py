"""
Tests for M5 Schemas — pure Pydantic validation, no API calls.
"""

import os
import sys

import pytest
from pydantic import ValidationError

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.m5_schemas import (
    BenchmarkQuestionResult,
    BenchmarkReport,
    CriticOutput,
    PlannerOutput,
)


# -- PlannerOutput ------------------------------------------------------------

def test_planner_output_valid_intents():
    for intent in ("data_query", "conversational", "multistep", "cannot_answer"):
        p = PlannerOutput(intent=intent, reason="r", subqueries=[])
        assert p.intent == intent


def test_planner_output_invalid_intent():
    with pytest.raises(ValidationError):
        PlannerOutput(intent="invalid", reason="r", subqueries=[])  # type: ignore


def test_planner_output_subqueries_cleared_for_non_multistep():
    p = PlannerOutput(intent="data_query", reason="r", subqueries=["q1", "q2"])
    assert p.subqueries == []


def test_planner_output_subqueries_kept_for_multistep():
    p = PlannerOutput(intent="multistep", reason="r", subqueries=["q1", "q2"])
    assert p.subqueries == ["q1", "q2"]


# -- CriticOutput -------------------------------------------------------------

def test_critic_output_followup_gets_question_mark():
    c = CriticOutput(
        answer="Produce leads at 66.2%.",
        finding="Gap to dairy is 0.4%.",
        caveat="None.",
        followup="Which products drive this",
    )
    assert c.followup.endswith("?")


def test_critic_output_followup_preserved_with_question_mark():
    c = CriticOutput(
        answer="Produce leads.",
        finding="Gap is 0.4%.",
        caveat="None.",
        followup="What comes next?",
    )
    assert c.followup == "What comes next?"


def test_critic_output_empty_answer_rejected():
    with pytest.raises(ValidationError):
        CriticOutput(answer="", finding="f", caveat="c", followup="q?")


def test_critic_output_empty_finding_rejected():
    with pytest.raises(ValidationError):
        CriticOutput(answer="a", finding="", caveat="c", followup="q?")


# -- BenchmarkQuestionResult --------------------------------------------------

def test_benchmark_result_frozen():
    r = BenchmarkQuestionResult(
        id="ST01", category="single_table", question="q",
        sql="SELECT 1", sql_correct=True, db_executed=True,
        result_sane=True, overall="PASS", latency_ms=1000,
    )
    with pytest.raises(Exception):
        r.sql_correct = False  # type: ignore


def test_benchmark_result_latency_non_negative():
    with pytest.raises(ValidationError):
        BenchmarkQuestionResult(
            id="ST01", category="single_table", question="q",
            sql="SELECT 1", sql_correct=True, db_executed=True,
            result_sane=True, overall="PASS", latency_ms=-1,
        )


# -- BenchmarkReport ----------------------------------------------------------

def test_benchmark_report_round_trip():
    report = BenchmarkReport(
        architecture=1, timestamp="2026-01-01T00:00:00",
        model="claude-sonnet-4-6", total=30,
        sql_correct=28, db_executed=27, result_sane=25,
        full_pass=24, blocked_ok=2,
        sql_pct=93.3, db_pct=90.0, sanity_pct=83.3,
        pass_pct=80.0, total_latency_ms=55000,
        avg_latency_ms=1833, by_category={}, questions=[],
    )
    json_str = report.to_json()
    restored = BenchmarkReport.from_json(json_str)
    assert restored.architecture == 1
    assert restored.pass_pct     == 80.0
    assert restored.sql_correct  == 28
    assert restored.total        == 30
