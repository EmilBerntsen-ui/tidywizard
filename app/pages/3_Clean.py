"""Clean: interactive pipeline builder (drop, impute, dropna, deduplicate)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st

st.title("🧹 Clean")
st.markdown("Add cleaning steps. They are appended to the pipeline and applied in order at export.")

if "df_raw" not in st.session_state:
    st.warning("No dataset in session. Upload a file in **Upload** first.")
    st.stop()

if "pipeline_spec" not in st.session_state:
    st.session_state["pipeline_spec"] = {"version": 1, "steps": []}

df = st.session_state["df_raw"]
spec = st.session_state["pipeline_spec"]
steps = spec.get("steps") or []

# --- Drop columns ---
st.subheader("Drop columns")
to_drop = st.multiselect("Columns to drop (optional)", options=list(df.columns), key="clean_drop")
if st.button("Add: drop columns") and to_drop:
    steps.append({"name": "drop_columns", "params": {"columns": to_drop}})
    st.session_state["pipeline_spec"] = {"version": 1, "steps": steps}
    st.rerun()

# --- Impute ---
st.subheader("Impute missing values")
n_strat = st.selectbox("Numeric strategy", ["median", "mean", "constant"], key="impute_num_strat")
n_const = st.text_input("Numeric constant (if strategy=constant)", value="0", key="impute_num_const")
c_strat = st.selectbox("Categorical strategy", ["mode", "constant"], key="impute_cat_strat")
c_const = st.text_input("Categorical constant (if strategy=constant)", value="unknown", key="impute_cat_const")

def _parse_const(s: str, for_numeric: bool):
    s = s.strip()
    if for_numeric:
        try:
            return float(s) if "." in s else int(s)
        except ValueError:
            return 0
    return s if s else "unknown"

if st.button("Add: impute"):
    n_fv = _parse_const(n_const, True) if n_strat == "constant" else None
    c_fv = c_const.strip() or "unknown" if c_strat == "constant" else None
    steps.append({
        "name": "impute",
        "params": {
            "numeric": {"strategy": n_strat, "fill_value": n_fv},
            "categorical": {"strategy": c_strat, "fill_value": c_fv},
        },
    })
    st.session_state["pipeline_spec"] = {"version": 1, "steps": steps}
    st.rerun()

# --- Drop NA rows ---
st.subheader("Drop rows with missing values")
dropna_how = st.radio("Drop if missing in", ["any column", "all columns"], key="dropna_how")
if st.button("Add: drop NA rows"):
    how = "any" if "any" in dropna_how else "all"
    steps.append({"name": "dropna_rows", "params": {"how": how}})
    st.session_state["pipeline_spec"] = {"version": 1, "steps": steps}
    st.rerun()

# --- Deduplicate ---
st.subheader("Deduplicate rows")
dedup_keep = st.radio("Keep", ["first", "last", "none (drop all dupes)"], key="dedup_keep")
if st.button("Add: deduplicate"):
    k = "first" if "first" in dedup_keep else ("last" if "last" in dedup_keep else False)
    steps.append({"name": "deduplicate", "params": {"keep": k}})
    st.session_state["pipeline_spec"] = {"version": 1, "steps": steps}
    st.rerun()

# --- Pipeline summary ---
st.subheader("Pipeline")
if steps:
    for i, s in enumerate(steps):
        st.code(f"{i+1}. {s['name']} — {s.get('params', {})}")
    if st.button("Clear pipeline"):
        st.session_state["pipeline_spec"] = {"version": 1, "steps": []}
        st.rerun()
else:
    st.info("No steps yet. Add steps above.")
