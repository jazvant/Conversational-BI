"""
M4 Renderer
============
Takes a DataFrame and a chart-type string and returns a Plotly figure.
Single responsibility: figure creation only.
Never calls Streamlit directly.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# -- Colour palette -----------------------------------------------------------
_PALETTE   = px.colors.qualitative.Bold
_HEADER_BG = "#1f4e79"
_HEADER_FG = "#ffffff"
_ROW_ODD   = "#1e2a3a"
_ROW_EVEN  = "#16202e"
_CELL_FG   = "#e8edf2"

# -- Internal column helpers --------------------------------------------------
_TEMPORAL_KW = {"dow", "hour", "day", "week", "month", "number", "sequence"}
_METRIC_KW   = {"count", "cnt", "avg", "mean", "sum", "total", "rate",
                 "pct", "percent", "score", "ratio", "size"}


def _is_numeric(series: pd.Series) -> bool:
    """Return True if the series has a numeric dtype."""
    return pd.api.types.is_numeric_dtype(series)


def _is_temporal_name(col: str) -> bool:
    """Return True if the column name contains a temporal keyword."""
    col_lower = col.lower()
    return any(kw in col_lower for kw in _TEMPORAL_KW)


def _categorical_cols(df: pd.DataFrame) -> list:
    """Return column names that are object/string dtype (categorical)."""
    return [c for c in df.columns if df[c].dtype == object or
            str(df[c].dtype).startswith("string")]


def _numeric_cols(df: pd.DataFrame) -> list:
    """Return column names that have numeric dtype."""
    return [c for c in df.columns if _is_numeric(df[c])]


def _first_temporal_or_categorical(df: pd.DataFrame) -> str | None:
    """Return the first temporal-named column, then first categorical, or None."""
    for col in df.columns:
        if _is_temporal_name(col):
            return col
    cats = _categorical_cols(df)
    return cats[0] if cats else None


# -- Render functions ---------------------------------------------------------
def apply_theme(fig: go.Figure) -> go.Figure:
    """Applies transparent background and light text for Streamlit dark theme."""
    fig.update_layout(
        paper_bgcolor = "rgba(0,0,0,0)",
        plot_bgcolor  = "rgba(0,0,0,0)",
        font          = dict(color="#FAFAFA"),
        title_font    = dict(color="#FAFAFA"),
        legend        = dict(
            bgcolor   = "rgba(0,0,0,0)",
            font      = dict(color="#FAFAFA")
        ),
        xaxis = dict(
            color      = "#FAFAFA",
            gridcolor  = "rgba(250,250,250,0.1)",
            linecolor  = "rgba(250,250,250,0.2)",
        ),
        yaxis = dict(
            color      = "#FAFAFA",
            gridcolor  = "rgba(250,250,250,0.1)",
            linecolor  = "rgba(250,250,250,0.2)",
        ),
    )
    return fig

def render_bar(df: pd.DataFrame, title: str = "") -> go.Figure:
    """Render a vertical or horizontal bar chart (horizontal when > 10 categories)."""
    cats = _categorical_cols(df)
    nums = _numeric_cols(df)

    x_col = cats[0] if cats else df.columns[0]
    y_col = nums[0] if nums else (df.columns[1] if len(df.columns) > 1 else df.columns[0])
    n_cats = df[x_col].nunique()

    if n_cats > 10:
        fig = px.bar(
            df, x=y_col, y=x_col, orientation="h",
            title=title, color_discrete_sequence=_PALETTE,
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
    else:
        fig = px.bar(
            df, x=x_col, y=y_col,
            title=title, color_discrete_sequence=_PALETTE,
        )

    fig.update_layout(
        plot_bgcolor="white",
        paper_bgcolor="white",
        font={"size": 13},
    )
    return apply_theme(fig)


def render_line(df: pd.DataFrame, title: str = "") -> go.Figure:
    """Render a line chart with markers; all numeric columns become separate lines."""
    x_col  = _first_temporal_or_categorical(df)
    nums   = _numeric_cols(df)
    y_cols = [c for c in nums if c != x_col] or nums

    if x_col is None:
        x_col = df.columns[0]

    fig = px.line(
        df, x=x_col, y=y_cols,
        title=title, markers=True,
        color_discrete_sequence=_PALETTE,
    )
    fig.update_layout(
        plot_bgcolor="white",
        paper_bgcolor="white",
        font={"size": 13},
    )
    return apply_theme(fig)


def render_pie(df: pd.DataFrame, title: str = "") -> go.Figure:
    """Render a pie chart showing percentage and absolute value on hover."""
    cats  = _categorical_cols(df)
    nums  = _numeric_cols(df)
    names = cats[0] if cats else df.columns[0]
    vals  = nums[0] if nums else df.columns[1]

    fig = px.pie(
        df, names=names, values=vals,
        title=title,
        color_discrete_sequence=_PALETTE,
        hover_data={vals: True},
    )
    fig.update_traces(textinfo="percent+label")
    return apply_theme(fig)


def render_scatter(df: pd.DataFrame, title: str = "") -> go.Figure:
    """Render a scatter plot; third categorical column used for colour grouping."""
    nums = _numeric_cols(df)
    cats = _categorical_cols(df)
    x_col  = nums[0] if len(nums) > 0 else df.columns[0]
    y_col  = nums[1] if len(nums) > 1 else df.columns[1]
    color  = cats[0] if cats else None

    fig = px.scatter(
        df, x=x_col, y=y_col, color=color,
        title=title, color_discrete_sequence=_PALETTE,
    )
    fig.update_layout(
        plot_bgcolor="white",
        paper_bgcolor="white",
        font={"size": 13},
    )
    return apply_theme(fig)




def render_histogram(df: pd.DataFrame, title: str = "") -> go.Figure:
    """Render a histogram of the first numeric column with 30 auto-bins."""
    nums  = _numeric_cols(df)
    x_col = nums[0] if nums else df.columns[0]

    fig = px.histogram(
        df, x=x_col, nbins=30,
        title=title,
        color_discrete_sequence=_PALETTE,
    )
    fig.update_layout(
        plot_bgcolor="white",
        paper_bgcolor="white",
        yaxis_title="Count",
        font={"size": 13},
        bargap=0.05,
    )
    return apply_theme(fig)


def render_heatmap(df: pd.DataFrame, title: str = "") -> go.Figure:
    """Render a heatmap: first categorical → y, second categorical → x, numeric → colour."""
    cats = _categorical_cols(df)
    nums = _numeric_cols(df)

    y_col     = cats[0] if len(cats) > 0 else df.columns[0]
    x_col     = cats[1] if len(cats) > 1 else (df.columns[1] if len(df.columns) > 1 else df.columns[0])
    value_col = nums[0] if nums else df.columns[-1]

    # Pivot for heatmap grid
    try:
        pivot = df.pivot_table(index=y_col, columns=x_col, values=value_col, aggfunc="sum")
        fig = px.imshow(
            pivot,
            title=title,
            color_continuous_scale="Blues",
            aspect="auto",
        )
    except Exception:
        # Fallback to go.Heatmap if pivot fails
        fig = go.Figure(go.Heatmap(
            z=df[value_col],
            x=df[x_col],
            y=df[y_col],
            colorscale="Blues",
        ))
        fig.update_layout(title=title)

    fig.update_layout(font={"size": 13})
    return apply_theme(fig)


def render_area(df: pd.DataFrame, title: str = "") -> go.Figure:
    """Render a filled area chart (line chart with area fill)."""
    x_col  = _first_temporal_or_categorical(df)
    nums   = _numeric_cols(df)
    y_cols = [c for c in nums if c != x_col] or nums

    if x_col is None:
        x_col = df.columns[0]

    fig = px.area(
        df, x=x_col, y=y_cols,
        title=title,
        color_discrete_sequence=_PALETTE,
    )
    fig.update_layout(
        plot_bgcolor="white",
        paper_bgcolor="white",
        font={"size": 13},
    )
    return apply_theme(fig)


def render_table(df: pd.DataFrame, title: str = "") -> go.Figure:
    """Render a styled Plotly table with alternating row colours."""
    n_rows   = len(df)
    n_cols   = len(df.columns)

    fill_colors = [_ROW_ODD if i % 2 == 0 else _ROW_EVEN for i in range(n_rows)]

    # Font colour must be a list matching fill_color length
    # when fill_color is a list — Plotly ignores scalar font.color in that case
    cell_font_colors = [_CELL_FG] * n_rows

    fig = go.Figure(go.Table(
        header=dict(
            values     = [f"<b>{c}</b>" for c in df.columns],
            fill_color = _HEADER_BG,
            font       = dict(color=_HEADER_FG, size=13),
            align      = "left",
            height     = 32,
        ),
        cells=dict(
            values     = [df[c].tolist() for c in df.columns],
            fill_color = [fill_colors] * n_cols,
            font       = dict(
                color  = [cell_font_colors] * n_cols,  # ← list, not scalar
                size   = 12
            ),
            align      = "left",
            height     = 28,
        ),
    ))

    fig.update_layout(
        paper_bgcolor = "rgba(0,0,0,0)",
        margin        = dict(l=0, r=0, t=40 if title else 10, b=0),
    )

    if title:
        fig.update_layout(
            title=dict(text=title, font=dict(color=_HEADER_FG))
        )

    return fig


# -- Dispatcher ---------------------------------------------------------------

_RENDER_MAP = {
    "BAR":       render_bar,
    "LINE":      render_line,
    "PIE":       render_pie,
    "SCATTER":   render_scatter,
    "HISTOGRAM": render_histogram,
    "HEATMAP":   render_heatmap,
    "AREA":      render_area,
    "TABLE":     render_table,
}


def render_chart(df: pd.DataFrame, chart_type: str, title: str = "") -> go.Figure:
    """Dispatch to the correct render function; falls back to render_table."""
    fn = _render_map.get(chart_type.upper(), render_table)
    return fn(df, title)


# Fix the reference
_render_map = _RENDER_MAP


# -- Self-test ----------------------------------------------------------------
if __name__ == "__main__":
    import pandas as pd

    df = pd.DataFrame({
        "department": ["produce", "dairy", "beverages", "frozen", "snacks"],
        "orders":     [12000, 8500, 7200, 5300, 9800],
    })

    for ct in ["BAR", "PIE", "TABLE", "HISTOGRAM", "LINE"]:
        test_df = df.copy()
        if ct == "HISTOGRAM":
            test_df = pd.DataFrame({"days": [1.0, 7.0, 14.0, 21.0, 28.0]})
        if ct == "LINE":
            test_df = pd.DataFrame({"order_number": [1,2,3,4,5], "avg_days":[0,7,8,9,10]})
        fig = render_chart(test_df, ct, title=f"Test {ct}")
        assert fig is not None
        print(f"  render_chart({ct}) OK — {type(fig).__name__}")

    print("\nM4 renderer OK")
