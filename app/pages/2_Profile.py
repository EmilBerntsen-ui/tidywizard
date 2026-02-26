"""Profile: dtypes, missing, unique, top values, duplicates, numeric stats."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st

from core.profile import profile_dataframe


@st.cache_data(ttl=3600)
def _cached_profile(cache_key: str, _df) -> dict:
    """Cache profile by key; _df is not hashed (Streamlit convention)."""
    return profile_dataframe(_df)


st.title("Profile")

if "df_raw" not in st.session_state:
    st.warning("No dataset in session. Upload a file in **Upload** first.")
    st.stop()

df = st.session_state["df_raw"]
cache_key = f"profile_{id(df)}_{df.shape[0]}_{df.shape[1]}_{hash(tuple(df.columns))}"
profile = _cached_profile(cache_key, df)

st.metric("Duplicate rows", profile["n_duplicates"])

# Warnings
whitespace_cols = [c["name"] for c in profile["columns"] if c.get("has_whitespace_in_name")]
if whitespace_cols:
    st.warning(
        f"Column names with leading/trailing whitespace: {whitespace_cols}. "
        "Consider adding a **Strip whitespace** step."
    )

constant_cols = [c["name"] for c in profile["columns"] if c.get("is_constant")]
if constant_cols:
    st.warning(f"Constant columns (all values identical): {constant_cols}")

st.write("")

for col in profile["columns"]:
    label = f"**{col['name']}** — {col['dtype']}"
    if col.get("is_constant"):
        label += "  (constant)"
    if col.get("has_whitespace_in_name"):
        label += "  (whitespace in name)"

    with st.expander(label):
        st.write("- % missing:", col["pct_missing"])
        st.write("- # unique:", col["n_unique"])
        if "min" in col:
            st.write(f"- min: {col['min']}, max: {col['max']}, mean: {col['mean']}, std: {col['std']}")
        if col["top_values"]:
            st.write("Top values:")
            for val, cnt in col["top_values"]:
                st.write(f"  - `{val}`: {cnt}")
