"""
Instacart BI Agent — Streamlit Application
==========================================
Conversational interface for natural-language queries
against the Instacart Market Basket Analysis dataset.

Run with:
    streamlit run app.py
"""

import logging
import os
import sys

import anthropic
import duckdb
import streamlit as st
from dotenv import load_dotenv

# Make scripts/ importable as both a package (scripts.X) and directly (X)
_ROOT    = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS = os.path.join(_ROOT, "scripts")
sys.path.insert(0, _SCRIPTS)
sys.path.insert(0, _ROOT)

load_dotenv(os.path.join(_ROOT, ".env"))

from scripts.m3_1_prompt_builder import (  # noqa: E402
    build_system_prompt,
    load_schema_context,
)
from scripts.m3_4_error_recovery import attempt_with_retry   # noqa: E402
from scripts.m4_chart_selector import detect_chart_type      # noqa: E402
from scripts.m4_renderer import render_chart                  # noqa: E402
from scripts.m5_critic import CriticValidation, critique      # noqa: E402
from scripts.m5_conversational import answer_from_memory      # noqa: E402
from scripts.m5_planner import (                              # noqa: E402
    PlannerDecision,
    classify_intent,
    is_cannot_answer,
    is_conversational,
    is_data_query,
    is_multistep,
)
from scripts.m5_schemas import CriticOutput                   # noqa: E402
from scripts.m6_memory import (                               # noqa: E402
    add_turn,
    build_messages,
    get_context_summary,
)
from config import (                                          # noqa: E402
    ARCHITECTURE,
    DB_PATH,
    INTENT_CANNOT_ANSWER,
    INTENT_CONVERSATIONAL,
    INTENT_DATA_QUERY,
    INTENT_MULTISTEP,
    SCHEMA_CONTEXT_PATH,
)

logging.basicConfig(level=logging.WARNING)

# -- Page config --------------------------------------------------------------
st.set_page_config(
    page_title="Instacart BI Agent",
    page_icon="📊",
    layout="wide",
)

# -- Session state initialisation (runs once per browser session) -------------
if "con" not in st.session_state:
    # API key check
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        st.error(
            "**ANTHROPIC_API_KEY is not set.**  \n"
            "Add it to your `.env` file or export it in your shell, "
            "then restart the app."
        )
        st.stop()

    # Database connection
    if not os.path.exists(DB_PATH):
        st.error(
            f"**Could not connect to instacart.db** — file not found at `{DB_PATH}`.  \n"
            "Run `python scripts/build_database.py` first."
        )
        st.stop()

    try:
        st.session_state.con = duckdb.connect(DB_PATH, read_only=True)
    except Exception as exc:
        st.error(f"**Could not connect to instacart.db:** {exc}")
        st.stop()

    # Schema and system prompt
    try:
        schema_context = load_schema_context(SCHEMA_CONTEXT_PATH)
    except FileNotFoundError as exc:
        st.error(f"**Schema context missing:** {exc}")
        st.stop()

    st.session_state.system_prompt    = build_system_prompt(schema_context)
    st.session_state.client           = anthropic.Anthropic(api_key=api_key)
    st.session_state.memory           = []
    st.session_state.results          = []
    st.session_state.last_narrative   = None
    st.session_state.last_validation  = None
    st.session_state.last_decision    = None

# Convenience aliases
con           = st.session_state.con
system_prompt = st.session_state.system_prompt
client        = st.session_state.client

# -- Layout -------------------------------------------------------------------
col_left, col_right = st.columns([2, 3])

# ── Left column: chat ─────────────────────────────────────────────────────────
with col_left:
    st.title("📊 Instacart BI Agent")
    st.subheader("Ask a question about customer purchasing behaviour")

    question = st.chat_input("Ask a question...")

    if question:
        if ARCHITECTURE == 2:
            # -- Planner: classify intent ------------------------------------
            with st.spinner("Understanding your question..."):
                decision = classify_intent(
                    client, question, st.session_state.memory
                )
            st.session_state.last_decision = decision

            if is_cannot_answer(decision):
                result = {
                    "success":   False,
                    "data":      None,
                    "row_count": 0,
                    "sql":       "",
                    "error":     (
                        "This question is outside the scope "
                        "of the Instacart dataset."
                    ),
                    "warnings":  [],
                }
                validation = None

            elif is_conversational(decision):
                with st.spinner("Thinking..."):
                    answer = answer_from_memory(
                        client, question, st.session_state.memory
                    )
                result = {
                    "success":               True,
                    "data":                  None,
                    "row_count":             0,
                    "sql":                   "",
                    "error":                 None,
                    "warnings":              [],
                    "conversational_answer": answer,
                }
                validation = None

            elif is_multistep(decision):
                results_list = []
                for subq in decision.subqueries:
                    with st.spinner(f"Running: {subq[:60]}..."):
                        r = attempt_with_retry(
                            client, con, system_prompt,
                            subq, st.session_state.memory,
                        )
                        results_list.append(r)
                        if r.get("sql"):
                            st.session_state.memory = add_turn(
                                st.session_state.memory,
                                subq, r["sql"], r,
                            )
                result = next(
                    (r for r in reversed(results_list) if r["success"]),
                    results_list[-1],
                )
                with st.spinner("Generating insight..."):
                    validation = critique(client, question, result)

            else:  # data_query
                with st.spinner("Querying database..."):
                    result = attempt_with_retry(
                        client, con, system_prompt,
                        question, st.session_state.memory,
                    )
                if result["success"]:
                    with st.spinner("Generating insight..."):
                        validation = critique(client, question, result)
                else:
                    validation = None

        else:  # ARCHITECTURE == 1
            result     = attempt_with_retry(
                client, con, system_prompt,
                question, st.session_state.memory,
            )
            validation = None
            st.session_state.last_decision = None

        # Store validation + narrative
        st.session_state.last_validation = validation
        st.session_state.last_narrative  = (
            validation.narrative
            if validation and validation.narrative
            else None
        )

        # Update memory (skip pure conversational)
        if result.get("sql"):
            st.session_state.memory = add_turn(
                st.session_state.memory, question, result["sql"], result
            )

        # Store result for right column
        st.session_state.results.append(result)

    # Conversation history display (derived from memory + results)
    _intent_badge = {
        INTENT_CONVERSATIONAL: "💬 ",
        INTENT_MULTISTEP:      "🔀 ",
        INTENT_CANNOT_ANSWER:  "❌ ",
        INTENT_DATA_QUERY:     "",
    }
    for i, turn in enumerate(st.session_state.memory):
        with st.chat_message("user"):
            st.write(turn["question"])
        with st.chat_message("assistant"):
            st.code(turn["sql"], language="sql")

# ── Right column: chart + data + insight ─────────────────────────────────────
with col_right:
    if not st.session_state.results:
        st.info("Your results will appear here.")
    else:
        result     = st.session_state.results[-1]
        narrative  = st.session_state.get("last_narrative")
        validation = st.session_state.get("last_validation")

        # Conversational answer (no chart)
        if result.get("conversational_answer"):
            with st.container(border=True):
                st.markdown("#### Response")
                st.markdown(result["conversational_answer"])

        elif result["success"] and result.get("data") is not None:
            df = result["data"]

            # Auto chart detection
            recommended, alternatives = detect_chart_type(df)

            chart_choice = st.selectbox(
                "Chart type",
                options=alternatives,
                index=alternatives.index(recommended),
            )

            # Derive title from last question
            title = st.session_state.memory[-1]["question"] if st.session_state.memory else ""

            fig = render_chart(df, chart_choice, title)
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("Data")
            st.dataframe(df, use_container_width=True, hide_index=True)

            st.caption(f"{result['row_count']} rows returned")
            st.download_button(
                label="Download CSV",
                data=df.to_csv(index=False),
                file_name="query_result.csv",
                mime="text/csv",
            )

            # Data quality warnings (M8)
            if result.get("warnings"):
                for w in result["warnings"]:
                    st.warning(w)

            # Insight panel (Architecture 2)
            if narrative and isinstance(narrative, CriticOutput):
                with st.container(border=True):
                    st.markdown("#### Insight")
                    st.markdown(f"**Answer:** {narrative.answer}")
                    st.markdown(f"**Finding:** {narrative.finding}")
                    if narrative.caveat != "None.":
                        st.warning(f"**Note:** {narrative.caveat}")
                    st.markdown("---")
                    st.caption(f"Suggested follow-up: {narrative.followup}")

        else:
            st.error(f"Could not generate valid SQL: {result['error']}")
            if result.get("sql"):
                st.code(result["sql"], language="sql")

        # Validation issues
        if validation and validation.issues:
            for issue in validation.issues:
                st.warning(issue)

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("Session info")
st.sidebar.metric("Questions asked",    len(st.session_state.results))
st.sidebar.metric(
    "Successful queries",
    sum(r["success"] for r in st.session_state.results),
)
st.sidebar.button(
    "Clear history",
    on_click=lambda: st.session_state.clear(),
)
st.sidebar.text(get_context_summary(st.session_state.memory))

st.sidebar.markdown("---")
arch_label = (
    "2 — Planner + Critic"
    if ARCHITECTURE == 2
    else "1 — Direct SQL"
)
st.sidebar.markdown(f"**Architecture:** {arch_label}")

decision = st.session_state.get("last_decision")
if decision:
    st.sidebar.markdown(f"Last intent: `{decision.intent}`")
    st.sidebar.caption(decision.reason)
