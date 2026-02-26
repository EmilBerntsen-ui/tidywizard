"""Instrument-specific file loading: alternative entry point to the generic Upload page."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import streamlit as st
from core.instruments.prometheus_panta import load_melting_scan

_INSTRUMENTS = {"Prometheus Panta (nanoDSF)": "prometheus_panta"}

st.title("Instruments")
st.markdown(
    "Load files from a specific laboratory instrument. After loading, continue to Profile → Clean → Export in the sidebar."
)
instrument = st.selectbox("Select instrument", list(_INSTRUMENTS.keys()))

if _INSTRUMENTS[instrument] == "prometheus_panta":
    st.markdown(
        "**Prometheus Panta** — NanoTemper nanoDSF. "
        "Upload the melting scan CSV (semicolon-separated, from NanoTemper software)."
    )
    uploaded_scan = st.file_uploader(
        "Melting scan CSV (required)",
        type=["csv", "txt"],
        key="panta_scan",
        help="The semicolon-separated raw data file exported from the Prometheus Panta software.",
    )
    uploaded_meta = st.file_uploader(
        "Data table TSV (optional — not yet processed)", type=["tsv", "txt", "csv"], key="panta_meta"
    )
    if uploaded_meta is not None:
        st.info("Data table TSV received. Metadata merge is not yet implemented.")
    if st.button("Parse", disabled=uploaded_scan is None):
        if uploaded_scan is None:
            st.error("No file uploaded.")
            st.stop()
        try:
            df = load_melting_scan(uploaded_scan)
            st.session_state["df_raw"] = df
            st.session_state["uploaded_name"] = uploaded_scan.name or "panta_melting_scan.csv"
            st.session_state["pipeline_spec"] = {"version": 1, "steps": []}
            n, p = df.shape
            st.success(
                f"Parsed **{n}** rows × **{p}** columns from `{uploaded_scan.name}`."
            )
            st.dataframe(df.head(20), use_container_width=True)
            st.info("Continue to **Profile** or **Clean** in the sidebar.")
        except ValueError as e:
            st.error(str(e))
        except Exception as e:
            st.error(f"Unexpected error: {e}")
if "df_raw" in st.session_state:
    df = st.session_state["df_raw"]
    with st.expander("Current dataset preview"):
        st.write("Shape:", df.shape)
        st.dataframe(df.head(10), use_container_width=True)
