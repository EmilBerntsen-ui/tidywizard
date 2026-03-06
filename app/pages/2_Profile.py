"""Profile: data quality dashboard with actionable insights."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import streamlit as st

from core.profile import profile_dataframe

st.title("Profile")
st.markdown("Understand your data and spot issues before cleaning.")

if "df_raw" not in st.session_state:
    st.warning("No dataset loaded yet. Go to **Upload** first.")
    st.stop()

if "pipeline_spec" not in st.session_state:
    st.session_state["pipeline_spec"] = {"version": 1, "steps": []}

df = st.session_state["df_raw"]
spec = st.session_state["pipeline_spec"]
steps = spec.get("steps") or []
profile = profile_dataframe(df)


def _add_step_and_go_clean(name: str, params: dict) -> None:
    """Add a pipeline step and navigate to the Clean page."""
    steps.append({"name": name, "params": params})
    st.session_state["pipeline_spec"] = {"version": 1, "steps": steps}
    st.switch_page("pages/3_Clean.py")


# ── Overview metrics ────────────────────────────────────────────────────

c1, c2, c3, c4 = st.columns(4)
c1.metric("Rows", f"{profile['n_rows']:,}")
c2.metric("Columns", profile["n_cols"])
n_missing_cells = sum(
    round(c["pct_missing"] / 100 * profile["n_rows"]) for c in profile["columns"]
)
total_cells = profile["n_rows"] * profile["n_cols"]
pct_complete = round(100 * (1 - n_missing_cells / total_cells), 1) if total_cells > 0 else 100
c3.metric("Completeness", f"{pct_complete}%")
c4.metric("Duplicate rows", profile["n_duplicates"])

# ── Column summary table ───────────────────────────────────────────────

st.subheader("Column overview")

summary_rows = []
for col_info in profile["columns"]:
    flags = []
    if col_info["pct_missing"] > 50:
        flags.append("mostly empty")
    elif col_info["pct_missing"] > 0:
        flags.append("has gaps")
    if col_info.get("is_constant"):
        flags.append("constant")
    if col_info.get("has_whitespace_in_name"):
        flags.append("whitespace in name")
    if col_info["n_unique"] == profile["n_rows"] and profile["n_rows"] > 1:
        flags.append("all unique")

    summary_rows.append({
        "Column": col_info["name"],
        "Type": col_info["dtype"],
        "Missing %": col_info["pct_missing"],
        "Unique": col_info["n_unique"],
        "Flags": ", ".join(flags) if flags else "",
    })

summary_df = pd.DataFrame(summary_rows)
st.dataframe(
    summary_df.style.applymap(
        lambda v: "background-color: #ffdddd" if v > 50 else
                  ("background-color: #fff3cd" if v > 0 else ""),
        subset=["Missing %"],
    ),
    use_container_width=True,
    hide_index=True,
)

# ── Data quality issues ────────────────────────────────────────────────

st.subheader("Issues found")

issues_found = False

# Whitespace in column names
whitespace_cols = [c["name"] for c in profile["columns"] if c.get("has_whitespace_in_name")]
if whitespace_cols:
    issues_found = True
    col_msg, col_btn = st.columns([5, 1])
    with col_msg:
        st.warning(f"**Whitespace in column names:** {', '.join(whitespace_cols)}")
    with col_btn:
        if st.button("Fix", key="fix_whitespace"):
            _add_step_and_go_clean("strip_whitespace", {"strip_headers": True, "columns": None})

# Constant columns
constant_cols = [c["name"] for c in profile["columns"] if c.get("is_constant")]
if constant_cols:
    issues_found = True
    col_msg, col_btn = st.columns([5, 1])
    with col_msg:
        st.warning(f"**Constant columns** (all values identical, no information): {', '.join(constant_cols)}")
    with col_btn:
        if st.button("Drop", key="fix_constant"):
            _add_step_and_go_clean("drop_columns", {"columns": constant_cols})

# Duplicate rows
if profile["n_duplicates"] > 0:
    issues_found = True
    col_msg, col_btn = st.columns([5, 1])
    with col_msg:
        st.warning(f"**{profile['n_duplicates']} duplicate rows** found.")
    with col_btn:
        if st.button("Deduplicate", key="fix_dupes"):
            _add_step_and_go_clean("deduplicate", {"keep": "first"})

# Columns with high missing %
high_missing = [c for c in profile["columns"] if c["pct_missing"] > 50]
if high_missing:
    issues_found = True
    names = [c["name"] for c in high_missing]
    col_msg, col_btn = st.columns([5, 1])
    with col_msg:
        st.warning(
            f"**Mostly empty columns** (>50% missing): {', '.join(names)}. "
            "These may not be useful."
        )
    with col_btn:
        if st.button("Drop", key="fix_high_missing"):
            _add_step_and_go_clean("drop_columns", {"columns": names})

# Columns with some missing values
some_missing = [c for c in profile["columns"] if 0 < c["pct_missing"] <= 50]
if some_missing:
    issues_found = True
    names = [f"{c['name']} ({c['pct_missing']}%)" for c in some_missing]
    col_msg, col_btn = st.columns([5, 1])
    with col_msg:
        st.info(f"**Columns with gaps:** {', '.join(names)}")
    with col_btn:
        if st.button("Impute", key="fix_some_missing"):
            _add_step_and_go_clean("impute", {
                "numeric": {"strategy": "median", "fill_value": None},
                "categorical": {"strategy": "mode", "fill_value": None},
            })

if not issues_found:
    st.success("No issues detected. Your data looks clean!")

# ── Missing values chart ───────────────────────────────────────────────

cols_with_missing = [c for c in profile["columns"] if c["pct_missing"] > 0]
if cols_with_missing:
    st.subheader("Missing values by column")
    chart_df = pd.DataFrame({
        "Column": [c["name"] for c in cols_with_missing],
        "Missing %": [c["pct_missing"] for c in cols_with_missing],
    }).set_index("Column")
    st.bar_chart(chart_df, horizontal=True)

# ── Numeric distributions ──────────────────────────────────────────────

numeric_cols = [c for c in profile["columns"] if "min" in c]
if numeric_cols:
    st.subheader("Numeric columns")
    for col_info in numeric_cols:
        with st.expander(f"**{col_info['name']}** — range: {col_info['min']:.4g} to {col_info['max']:.4g}, mean: {col_info['mean']:.4g}"):
            series = df[col_info["name"]].dropna()
            if len(series) > 0:
                st.bar_chart(series.value_counts(bins=min(30, len(series))).sort_index())

# ── Duplicate rows inspector ───────────────────────────────────────────

if profile["n_duplicates"] > 0:
    st.subheader("Duplicate rows")
    dup_mask = df.duplicated(keep=False)
    dup_df = df[dup_mask].sort_values(list(df.columns))
    n_show = min(100, len(dup_df))
    st.caption(f"Showing {n_show} of {len(dup_df)} duplicate rows (all copies).")
    st.dataframe(dup_df.head(n_show), use_container_width=True)

# ── Browse full dataset ────────────────────────────────────────────────

with st.expander("Browse full dataset"):
    st.dataframe(df, use_container_width=True, height=400)
