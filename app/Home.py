"""
TidyWizard — Multi-page Streamlit app for data cleaning.
"""

import sys
from pathlib import Path

# Ensure project root is on path for `import core`
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import streamlit as st

st.set_page_config(page_title="TidyWizard", page_icon="🧹", layout="wide")

st.title("🧹 TidyWizard")
st.markdown("**Just your friendly neighborhood spiderman, helping you clean your data until it's as neat as a button! (and exporting a reproducible pipeline).**")

st.markdown("""
1. **Upload** — Load a CSV, Excel, or instrument-specific file.
2. **Profile** — Inspect dtypes, missing values, uniques, and duplicates.
3. **Clean** — Build a cleaning pipeline step by step.
4. **Export** — Download the cleaned data, `pipeline.yaml`, and `pipeline.py`.

Use the sidebar to move through the steps.
""")

if "df_raw" not in st.session_state:
    st.info("👈 Start by uploading a dataset in **Upload**.")
else:
    n, p = st.session_state["df_raw"].shape
    st.success(f"Dataset loaded: **{n}** rows × **{p}** columns. You can **Profile**, **Clean**, or **Export**.")
