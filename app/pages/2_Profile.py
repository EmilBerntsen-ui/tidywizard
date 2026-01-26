"""Profile: dtypes, missing, unique, top values, duplicates."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st

from core.profile import profile_dataframe

st.title("📊 Profile")

if "df_raw" not in st.session_state:
    st.warning("No dataset in session. Upload a file in **Upload** first.")
    st.stop()

df = st.session_state["df_raw"]
profile = profile_dataframe(df)

st.metric("Duplicate rows", profile["n_duplicates"])
st.write("")

for col in profile["columns"]:
    with st.expander(f"**{col['name']}** — {col['dtype']}"):
        st.write("- % missing:", col["pct_missing"])
        st.write("- # unique:", col["n_unique"])
        if col["top_values"]:
            st.write("Top values:")
            for val, cnt in col["top_values"]:
                st.write(f"  - `{val}`: {cnt}")
