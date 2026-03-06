"""Upload CSV, Excel, or instrument-specific files and store in session state."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st

from core.io import get_excel_sheet_names, load_csv, load_excel
from core.instruments.prometheus_panta import load_melting_scan

st.title("Upload")

tab_generic, tab_instrument = st.tabs(["CSV / Excel", "Instrument format"])

# ── Generic file upload ──────────────────────────────────────────────────

with tab_generic:
    st.markdown("Upload a CSV, TSV, or Excel (.xlsx) file.")
    uploaded = st.file_uploader(
        "Choose a file", type=["csv", "tsv", "txt", "xlsx"], help="CSV, TSV, TXT, or XLSX."
    )

    if uploaded is not None:
        ext = (uploaded.name or "").lower()
        is_excel = ext.endswith(".xlsx")

        if not is_excel:
            with st.expander("Advanced options (delimiter, encoding, etc.)"):
                _SEP_OPTIONS = {",": ",", "Tab (\\t)": "\t", ";": ";", "|": "|"}
                default_sep = 1 if ext.endswith(".tsv") else 0
                sep_label = st.selectbox(
                    "Delimiter", list(_SEP_OPTIONS.keys()), index=default_sep, key="csv_sep"
                )
                sep = _SEP_OPTIONS[sep_label]
                skip_rows = st.number_input(
                    "Skip rows at top (metadata lines before the header)",
                    min_value=0, value=0, step=1, key="csv_skip",
                )
                header_row = st.number_input(
                    "Header row (0-indexed, after skip)", min_value=0, value=0, step=1, key="csv_header"
                )
                encoding = st.selectbox(
                    "Encoding", ["utf-8", "latin-1", "cp1252", "utf-16", "ascii"], key="csv_enc"
                )

        selected_sheet: int | str = 0
        if is_excel:
            try:
                sheet_names = get_excel_sheet_names(uploaded)
                uploaded.seek(0)
                if len(sheet_names) > 1:
                    selected_sheet = st.selectbox("Select sheet", sheet_names, key="xlsx_sheet")
            except Exception:
                pass

        try:
            if is_excel:
                df = load_excel(uploaded, sheet_name=selected_sheet)
            else:
                df = load_csv(
                    uploaded,
                    encoding=encoding,
                    sep=sep,
                    skiprows=skip_rows if skip_rows > 0 else None,
                    header=header_row,
                )
            st.session_state["df_raw"] = df
            st.session_state["uploaded_name"] = uploaded.name or "data.csv"
            st.session_state["pipeline_spec"] = {"version": 1, "steps": []}
            n, p = df.shape
            st.success(f"Loaded **{n}** rows x **{p}** columns from `{uploaded.name}`.")
            st.dataframe(df.head(20), use_container_width=True)
        except ValueError as e:
            st.error(str(e))
        except Exception as e:
            st.error(f"Unexpected error: {e}")

# ── Instrument-specific upload ───────────────────────────────────────────

_INSTRUMENTS = {"Prometheus Panta (nanoDSF)": "prometheus_panta"}

with tab_instrument:
    st.markdown(
        "Load files from a laboratory instrument. "
        "The raw instrument output is converted to a tidy table."
    )
    instrument = st.selectbox("Select instrument", list(_INSTRUMENTS.keys()))

    if _INSTRUMENTS[instrument] == "prometheus_panta":
        st.markdown(
            "**Prometheus Panta** — NanoTemper nanoDSF thermal shift assay. "
            "Upload the semicolon-separated melting scan CSV exported from the Prometheus software."
        )
        uploaded_scan = st.file_uploader(
            "Melting scan CSV", type=["csv", "txt"], key="panta_scan",
            help="The semicolon-separated raw data file from the Prometheus Panta software.",
        )
        uploaded_meta = st.file_uploader(
            "Data table TSV (optional — not yet processed)",
            type=["tsv", "txt", "csv"], key="panta_meta",
        )
        if uploaded_meta is not None:
            st.info("Data table TSV received. Metadata merge is not yet implemented.")
        if st.button("Parse instrument file", disabled=uploaded_scan is None):
            if uploaded_scan is None:
                st.error("No file uploaded.")
                st.stop()
            try:
                df = load_melting_scan(uploaded_scan)
                st.session_state["df_raw"] = df
                st.session_state["uploaded_name"] = uploaded_scan.name or "panta_melting_scan.csv"
                st.session_state["pipeline_spec"] = {"version": 1, "steps": []}
                n, p = df.shape
                st.success(f"Parsed **{n}** rows x **{p}** columns from `{uploaded_scan.name}`.")
                st.dataframe(df.head(20), use_container_width=True)
            except ValueError as e:
                st.error(str(e))
            except Exception as e:
                st.error(f"Unexpected error: {e}")

# ── Current dataset preview ──────────────────────────────────────────────

if "df_raw" in st.session_state:
    df = st.session_state["df_raw"]
    with st.expander("Current dataset"):
        st.write("Shape:", df.shape)
        st.dataframe(df.head(10), use_container_width=True)
