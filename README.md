# Conversational BI Agent
### Instacart Market Basket Analysis

A natural language BI agent that accepts plain English questions about grocery purchasing behaviour, generates and executes DuckDB SQL, renders results as interactive charts, and explains findings in plain English — all from a single Streamlit interface.

---

## What it does

Type a question. Get a chart, a data table, and a plain English insight explaining what the result means and what to look at next.

```
"Which department has the highest reorder rate?"
→ Bar chart ranked by reorder rate
→ Insight: "Produce leads at 66.2%, notably ahead of dairy eggs
   at 65.8%. The top 3 departments all exceed 60% reorder rate."
→ Follow-up suggestion: "Which produce products drive this rate?"
```

Follow-up questions use the prior result for context:

```
"Why is produce ranked highest?"
→ Conversational answer drawn from memory — no new SQL needed
```

Destructive requests are blocked before they reach the database:

```
"Delete all orders from the database"
→ "This request involves a write operation which is not
   permitted in this BI tool."
```

---

## Dataset

**Instacart Market Basket Analysis** — 3.4 million grocery orders from 200,000+ users.

| Table | Rows | Description |
|---|---|---|
| `orders` | 3.4M | Order headers — user, timing, sequence |
| `order_products_prior` | 32.4M | Products in all prior orders |
| `order_products_train` | 1.4M | Products in each user's most recent order |
| `order_details` | 33.8M | Unified view — prior + train combined |
| `products` | 50K | Product catalogue with aisle and department |
| `aisles` | 134 | Aisle names |
| `departments` | 21 | Department names |

Source: [Kaggle — Instacart Market Basket Analysis](https://www.kaggle.com/datasets/psparks/instacart-market-basket-analysis)

---

## Architecture

The system runs in two modes controlled by a single flag in `config.py`:

**Architecture 1 — Direct SQL** (baseline)
```
User question → Schema context → LLM → SQL → DuckDB → Chart + Table
```

**Architecture 2 — Planner + Critic** (default)
```
User question → Intent classification → Route:
  data_query     → SQL pipeline → Critic insight
  conversational → Memory-based answer
  multistep      → Sequential SQL runs → Critic insight
  cannot_answer  → Graceful rejection
```

Switch architectures without any code change:
```python
# config.py
ARCHITECTURE = 1   # direct SQL
ARCHITECTURE = 2   # planner + critic (default)
```

---

## Benchmark results

Evaluated against 30 questions spanning single-table lookups through three-table joins, temporal queries, reorder analysis, and safety checks.

| Metric | Score |
|---|---|
| SQL correctness | 27 / 30 (90.0%) |
| DB execution | 26 / 30 (86.7%) |
| Result sanity | 18 / 30 (60.0%) |
| Full pass rate | 17 / 30 (56.7%) |
| Avg latency | 2,705ms per question |

Category breakdown:

| Category | SQL% | DB% | Pass% |
|---|---|---|---|
| Single table | 83% | 66% | 33% |
| Two-table join | 100% | 100% | 66% |
| Three-table join | 100% | 100% | 83% |
| Temporal | 100% | 100% | 50% |
| Reorder / basket | 100% | 100% | 75% |
| Eval set | 100% | 100% | 50% |

Note: the result sanity score (60%) reflects conservative expected value ranges in the benchmark harness rather than incorrect query results. SQL correctness at 90% and DB execution at 87% better reflect real-world query accuracy. Safety blocking (0/2) is a known gap — the read-only DuckDB connection prevents any writes at the driver level regardless.

---

## Setup

### Prerequisites

- Python 3.11 or later
- Anthropic API key
- Instacart dataset CSVs (download from Kaggle link above)

### Installation

```bash
git clone <repo-url>
cd "Conversational BI"
pip install -r requirements.txt
```

### Environment

Create a `.env` file in the project root:

```
ANTHROPIC_API_KEY=your_key_here
```

Or set it directly:

```bash
# Windows
set ANTHROPIC_API_KEY=your_key_here

# macOS / Linux
export ANTHROPIC_API_KEY=your_key_here
```

### Data setup

Place the Instacart CSV files in the `data/` folder:

```
data/
├── orders.csv
├── order_products__prior.csv
├── order_products__train.csv
├── products.csv
├── aisles.csv
└── departments.csv
```

### Build the database

```bash
python scripts/build_database.py
```

This creates `models/instacart.db` (approximately 2.7 GB). Runs once — subsequent starts load from the existing database.

### Generate schema context

```bash
python scripts/m2_run.py
```

This creates `docs/schema_context.txt` — the LLM's reference document for the database structure.

### Run the app

```bash
streamlit run app.py
```

Open `http://localhost:8501` in your browser.

---

## Running tests

```bash
# Unit tests — no API key required
pytest tests/ -v -k "not integration"

# Integration tests — requires API key and database
python tests/integration_test.py

# Full benchmark — 30 questions, requires API key
python scripts/m10_benchmark.py
```

---

## Project structure

```
Conversational BI/
├── data/                          # raw CSVs — not committed
├── models/
│   └── instacart.db               # persistent DuckDB — not committed
├── scripts/
│   ├── build_database.py          # M1 — data ingestion
│   ├── validate.py                # M1 — validation
│   ├── m2_1_table_description.py  # M2 — schema extraction
│   ├── m2_2_join_relationships.py # M2 — FK graph
│   ├── m2_3_prompt_builder.py     # M2 — prompt assembly
│   ├── m2_4_validate_prompt.py    # M2 — prompt validation
│   ├── m2_run.py                  # M2 — entrypoint
│   ├── m3_1_prompt_builder.py     # M3 — question prompt
│   ├── m3_2_sql_generator.py      # M3 — LLM SQL generation
│   ├── m3_3_executor.py           # M3 — DuckDB execution
│   ├── m3_4_error_recovery.py     # M3 — retry logic
│   ├── m3_run.py                  # M3 — terminal REPL
│   ├── m4_chart_selector.py       # M4 — auto chart type
│   ├── m4_renderer.py             # M4 — Plotly rendering
│   ├── m5_schemas.py              # M5 — Pydantic schemas
│   ├── m5_planner.py              # M5 — intent classifier
│   ├── m5_conversational.py       # M5 — memory-based answers
│   ├── m5_critic.py               # M5 — result validation + narrative
│   ├── m6_memory.py               # M6 — conversation history
│   ├── m6_summariser.py           # M6 — result summarisation
│   ├── m7_input_validator.py      # M7 — SQL safety checks
│   ├── m8_output_sanitiser.py     # M8 — error sanitisation
│   └── m10_benchmark.py           # M10 — evaluation suite
├── tests/
│   ├── conftest.py
│   ├── test_m3_*.py
│   ├── test_m5_*.py
│   ├── test_m6_*.py
│   ├── test_m7_*.py
│   ├── test_m8_*.py
│   └── integration_test.py
├── docs/
│   ├── schema_metadata.txt        # table structure reference
│   ├── schema_context.txt         # LLM system prompt
│   └── benchmark_results.json     # M10 output
├── config.py                      # all configuration
├── app.py                         # Streamlit application
└── requirements.txt
```

---

## Example questions to try

**Simple aggregations**
- How many total orders are in the database?
- What is the most popular hour of day for placing orders?
- How many unique users have placed orders?

**Product analysis**
- What are the top 10 most frequently purchased products?
- Which products are most commonly added first to the cart?
- What are the top 5 most reordered products?

**Department and aisle**
- Which department has the highest reorder rate?
- What are the top 5 aisles by total purchases?
- Show me the top 10 products in the beverages department.

**Temporal patterns**
- Which day of the week has the most orders?
- How many orders are placed between 9am and 12pm?
- What is the average days between orders for repeat customers?

**Follow-up questions (try after any result)**
- Why is that department ranked highest?
- What does that tell us about customer behaviour?
- Can you explain that result?

---

## Dependencies

| Package | Version | Purpose |
|---|---|---|
| `duckdb` | ≥1.2.0 | Query engine |
| `pandas` | ≥2.2.0 | DataFrame handling |
| `anthropic` | ≥0.86.0 | Claude API |
| `streamlit` | ≥1.40.0 | Web UI |
| `plotly` | ≥5.24.0 | Chart rendering |
| `pydantic` | ≥2.0 | Structured outputs |
| `pyarrow` | ≥19.0.0 | Parquet / columnar support |
| `python-dotenv` | ≥1.0.0 | Environment variable loading |

---

## Known limitations

- No pricing or revenue data in the dataset — cost analysis is not possible
- No absolute timestamps — only relative gaps between orders (`days_since_prior_order`)
- No geographic data — regional analysis is not possible
- Safety intent blocking is partially implemented — the read-only DuckDB connection prevents writes at the driver level, but user-facing blocking messages for write-intent questions are a known gap
- The M10 benchmark routes directly to the SQL pipeline, bypassing the Planner — Architecture 1 and Architecture 2 benchmark scores are therefore identical
