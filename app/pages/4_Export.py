"""Export: cleaned CSV, pipeline.yaml, pipeline.py, and reproducibility info."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st

from core.pipeline import apply_pipeline
from core.export import pipeline_to_yaml, pipeline_to_python

st.title("📥 Export")

if "df_raw" not in st.session_state:
    st.warning("No dataset in session. Upload a file in **Upload** first.")
    st.stop()

df_raw = st.session_state["df_raw"]
spec = st.session_state.get("pipeline_spec") or {"version": 1, "steps": []}
steps = spec.get("steps") or []

try:
    df_clean = apply_pipeline(df_raw, spec)
except ValueError as e:
    st.error(f"Pipeline error: {e}")
    st.stop()

st.success(f"Pipeline applied: **{len(df_raw)}** → **{len(df_clean)}** rows, **{len(df_clean.columns)}** columns.")

# Download cleaned CSV
base = (st.session_state.get("uploaded_name") or "data").rsplit(".", 1)[0]
csv_name = f"{base}_cleaned.csv"
st.download_button(
    "Download cleaned CSV",
    data=df_clean.to_csv(index=False).encode("utf-8"),
    file_name=csv_name,
    mime="text/csv",
    key="dl_csv",
)

# Download pipeline.yaml
yaml_str = pipeline_to_yaml(spec)
st.download_button(
    "Download pipeline.yaml",
    data=yaml_str.encode("utf-8"),
    file_name="pipeline.yaml",
    mime="text/yaml",
    key="dl_yaml",
)

# Download pipeline.py
input_path = "input.csv"
output_path = f"{base}_cleaned.csv"
py_str = pipeline_to_python(spec, input_path=input_path, output_path=output_path)
st.download_button(
    "Download pipeline.py",
    data=py_str.encode("utf-8"),
    file_name="pipeline.py",
    mime="text/x-python",
    key="dl_py",
)

st.subheader("Reproducibility")
try:
    import pandas as pd
    import streamlit as _st
    import yaml as _yaml
    v_pd = getattr(pd, "__version__", "?")
    v_st = getattr(_st, "__version__", "?")
    v_yml = getattr(_yaml, "__version__", "?")
except Exception:
    v_pd = v_st = v_yml = "?"
st.text(f"pandas {v_pd} | streamlit {v_st} | pyyaml {v_yml}")

st.caption(
    "Run from project root: put input CSV as input.csv, run `python pipeline.py`. "
    "Or edit INPUT_PATH/OUTPUT_PATH in pipeline.py."
)
