"""Export: cleaned CSV, pipeline.yaml, pipeline.py, and reproducibility info."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st

from core.export import pipeline_to_python, pipeline_to_yaml
from core.pipeline import apply_pipeline


@st.cache_data(ttl=3600)
def _cached_apply_pipeline(cache_key: str, _df, _spec: dict):
    """Cache apply_pipeline by key; _df and _spec are not hashed."""
    return apply_pipeline(_df, _spec)


@st.cache_data(ttl=3600)
def _cached_yaml(spec_key: str, _spec: dict) -> str:
    """Cache pipeline_to_yaml by spec key."""
    return pipeline_to_yaml(_spec)


@st.cache_data(ttl=3600)
def _cached_python(cache_key: str, _spec: dict, _input_path: str, _output_path: str) -> str:
    """Cache pipeline_to_python by key."""
    return pipeline_to_python(_spec, input_path=_input_path, output_path=_output_path)


st.title("📥 Export")

if "df_raw" not in st.session_state:
    st.warning("No dataset in session. Upload a file in **Upload** first.")
    st.stop()

df_raw = st.session_state["df_raw"]
spec = st.session_state.get("pipeline_spec") or {"version": 1, "steps": []}
steps = spec.get("steps") or []

pipeline_cache_key = (
    f"pipeline_{id(df_raw)}_{df_raw.shape[0]}_{df_raw.shape[1]}_{hash(json.dumps(spec, sort_keys=True))}"
)
try:
    df_clean = _cached_apply_pipeline(pipeline_cache_key, df_raw, spec)
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
spec_key = str(hash(json.dumps(spec, sort_keys=True)))
yaml_str = _cached_yaml(spec_key, spec)
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
py_cache_key = f"py_{hash(json.dumps(spec, sort_keys=True))}_{input_path}_{output_path}"
py_str = _cached_python(py_cache_key, spec, input_path, output_path)
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
    import yaml as _yaml  # type: ignore[import-untyped]
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
