"""Upload CSV or Excel and store in session state."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st

from core.io import get_excel_sheet_names, load_csv, load_excel

st.title("Upload")
st.markdown("Upload a CSV or Excel (.xlsx) file. The dataframe is stored as `df_raw` in session state.")

uploaded = st.file_uploader("Choose a file", type=["csv", "tsv", "txt", "xlsx"], help="CSV, TSV, TXT, or XLSX.")

if uploaded is not None:
    ext = (uploaded.name or "").lower()
    is_excel = ext.endswith(".xlsx")

    # --- CSV/TSV advanced options ---
    if not is_excel:
        with st.expander("CSV / TSV options", expanded=False):
            _SEP_OPTIONS = {",": ",", "Tab (\\t)": "\t", ";": ";", "|": "|"}
            default_sep = 1 if ext.endswith(".tsv") else 0
            sep_label = st.selectbox(
                "Delimiter", list(_SEP_OPTIONS.keys()), index=default_sep, key="csv_sep"
            )
            sep = _SEP_OPTIONS[sep_label]
            skip_rows = st.number_input(
                "Skip rows at top (metadata lines)", min_value=0, value=0, step=1, key="csv_skip"
            )
            header_row = st.number_input(
                "Header row (0-indexed, after skip)", min_value=0, value=0, step=1, key="csv_header"
            )
            encoding = st.selectbox(
                "Encoding", ["utf-8", "latin-1", "cp1252", "utf-16", "ascii"], key="csv_enc"
            )

    # --- Excel sheet selection ---
    selected_sheet: int | str = 0
    if is_excel:
        try:
            sheet_names = get_excel_sheet_names(uploaded)
            uploaded.seek(0)
            if len(sheet_names) > 1:
                selected_sheet = st.selectbox("Select sheet", sheet_names, key="xlsx_sheet")
        except Exception:
            pass  # fall back to default sheet 0

    # --- Load ---
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

if "df_raw" in st.session_state:
    df = st.session_state["df_raw"]
    with st.expander("Shape and preview"):
        st.write("Shape:", df.shape)
        st.dataframe(df.head(10), use_container_width=True)
