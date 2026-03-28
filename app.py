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
from scripts.m3_4_error_recovery import attempt_with_retry  # noqa: E402
from scripts.m4_chart_selector import detect_chart_type     # noqa: E402
from scripts.m4_renderer import render_chart                 # noqa: E402

logging.basicConfig(level=logging.WARNING)

# -- Constants ----------------------------------------------------------------
DB_PATH             = os.path.join(_ROOT, "models", "instacart.db")
SCHEMA_CONTEXT_PATH = os.path.join(_ROOT, "docs",   "schema_context.txt")
_MAX_HISTORY        = 6
_SEP = "─" * 50

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

    st.session_state.system_prompt = build_system_prompt(schema_context)
    st.session_state.client        = anthropic.Anthropic(api_key=api_key)
    st.session_state.history       = []
    st.session_state.results       = []

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
        # 1. Append user message
        st.session_state.history.append({"role": "user", "content": question})

        # 2-3. Generate SQL and run query
        with st.spinner("Generating SQL and running query..."):
            result = attempt_with_retry(
                client,
                con,
                system_prompt,
                question,
                st.session_state.history[:-1],  # history before current turn
            )

        # 4. Append assistant message (the SQL)
        st.session_state.history.append(
            {"role": "assistant", "content": result["sql"]}
        )

        # 5. Store full result
        st.session_state.results.append(result)

        # 6. Trim history
        st.session_state.history = st.session_state.history[-_MAX_HISTORY:]

    # Conversation history display
    for msg in st.session_state.history:
        if msg["role"] == "user":
            with st.chat_message("user"):
                st.write(msg["content"])
        else:
            with st.chat_message("assistant"):
                st.code(msg["content"], language="sql")

# ── Right column: chart + data ────────────────────────────────────────────────
with col_right:
    if not st.session_state.results:
        st.info("Your results will appear here.")
    else:
        result = st.session_state.results[-1]

        if result["success"]:
            df = result["data"]

            # Auto chart detection
            recommended, alternatives = detect_chart_type(df)

            # User-overrideable chart type selector
            chart_choice = st.selectbox(
                "Chart type",
                options=alternatives,
                index=alternatives.index(recommended),
            )

            # Derive title from the last user question
            title = ""
            history = st.session_state.history
            for msg in reversed(history):
                if msg["role"] == "user":
                    title = msg["content"]
                    break

            # Render and display chart
            fig = render_chart(df, chart_choice, title)
            st.plotly_chart(fig, use_container_width=True)

            # Data table below chart
            st.subheader("Data")
            st.dataframe(df, use_container_width=True, hide_index=True)

            # Row count + CSV download
            st.caption(f"{result['row_count']} rows returned")
            st.download_button(
                label="Download CSV",
                data=df.to_csv(index=False),
                file_name="query_result.csv",
                mime="text/csv",
            )

        else:
            st.error(f"Could not generate valid SQL: {result['error']}")
            st.code(result["sql"], language="sql")

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("Session info")
st.sidebar.metric(
    "Questions asked",
    len(st.session_state.results),
)
st.sidebar.metric(
    "Successful queries",
    sum(r["success"] for r in st.session_state.results),
)
st.sidebar.button(
    "Clear history",
    on_click=lambda: st.session_state.clear(),
)
