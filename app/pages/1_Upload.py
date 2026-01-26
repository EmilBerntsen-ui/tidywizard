"""Upload CSV or Excel and store in session state."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st

from core.io import load_csv, load_excel

st.title("📤 Upload")
st.markdown("Upload a CSV or Excel (.xlsx) file. The dataframe is stored as `df_raw` in session state.")

uploaded = st.file_uploader("Choose a file", type=["csv", "xlsx"], help="CSV or XLSX only.")

if uploaded is not None:
    try:
        ext = (uploaded.name or "").lower()
        if ext.endswith(".xlsx"):
            df = load_excel(uploaded)
        else:
            df = load_csv(uploaded)
        st.session_state["df_raw"] = df
        st.session_state["uploaded_name"] = uploaded.name or "data.csv"
        n, p = df.shape
        st.success(f"Loaded **{n}** rows × **{p}** columns from `{uploaded.name}`.")
        st.dataframe(df.head(20), use_container_width=True)
    except ValueError as e:
        st.error(str(e))
    except Exception as e:
        st.error(f"Unexpected error: {e}")

if "df_raw" in st.session_state:
    df = st.session_state["df_raw"]
    with st.expander("Shape and preview"):
        st.write("Shape:", df.shape)
        st.dataframe(df.head(10), use_container_width=True)
