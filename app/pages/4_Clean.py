"""Clean: interactive pipeline builder with all transformation steps."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st

st.title("Clean")
st.markdown("Add cleaning steps. They are appended to the pipeline and applied in order at export.")

if "df_raw" not in st.session_state:
    st.warning("No dataset in session. Upload a file in **Upload** first.")
    st.stop()

if "pipeline_spec" not in st.session_state:
    st.session_state["pipeline_spec"] = {"version": 1, "steps": []}

df = st.session_state["df_raw"]
spec = st.session_state["pipeline_spec"]
steps = spec.get("steps") or []


def _add_step(name: str, params: dict) -> None:
    steps.append({"name": name, "params": params})
    st.session_state["pipeline_spec"] = {"version": 1, "steps": steps}
    st.rerun()


# ── Strip whitespace ──────────────────────────────────────────────────────

with st.expander("Strip whitespace"):
    st.caption("Remove leading/trailing spaces from column names and cell values.")
    strip_headers = st.checkbox("Strip column names", value=True, key="strip_headers")
    strip_cells = st.checkbox("Strip cell values", value=True, key="strip_cells")
    if st.button("Add: strip whitespace"):
        _add_step("strip_whitespace", {"strip_headers": strip_headers, "columns": None if strip_cells else []})

# ── Rename columns ────────────────────────────────────────────────────────

with st.expander("Rename columns"):
    st.caption("Give columns new names. Select which columns to rename.")
    cols_to_rename = st.multiselect("Columns to rename", list(df.columns), key="rename_select")
    rename_mapping: dict[str, str] = {}
    for col in cols_to_rename:
        new_name = st.text_input(f"New name for '{col}'", value=col, key=f"rename_{col}")
        if new_name and new_name != col:
            rename_mapping[col] = new_name
    if st.button("Add: rename columns") and rename_mapping:
        _add_step("rename_columns", {"mapping": rename_mapping})

# ── Replace values ────────────────────────────────────────────────────────

with st.expander("Replace values"):
    st.caption("Replace specific cell values. Useful for instrument sentinel values.")
    use_preset = st.checkbox(
        "Use instrument preset (BDL, <LOD, N/D, ---, . → NA)", value=False, key="replace_preset"
    )
    custom_old = st.text_input("Value to find", key="replace_old")
    custom_new = st.text_input("Replace with (leave empty for NA/missing)", key="replace_new")
    replace_cols = st.multiselect(
        "Apply to columns (leave empty for all)", list(df.columns), key="replace_cols"
    )
    if st.button("Add: replace values"):
        mapping: dict = {}
        if use_preset:
            for sentinel in ["BDL", "<LOD", "N/D", "---", ".", "N/A", "#N/A", "n/a"]:
                mapping[sentinel] = None
        if custom_old.strip():
            mapping[custom_old.strip()] = custom_new.strip() if custom_new.strip() else None
        if mapping:
            params: dict = {"mapping": mapping}
            if replace_cols:
                params["columns"] = replace_cols
            _add_step("replace_values", params)
        else:
            st.warning("No replacements specified.")

# ── Filter rows ───────────────────────────────────────────────────────────

with st.expander("Filter rows"):
    st.caption("Keep only rows matching a condition.")
    filter_col = st.selectbox("Column", list(df.columns), key="filter_col")
    filter_op = st.selectbox(
        "Operator",
        ["eq", "ne", "gt", "lt", "ge", "le", "contains", "not_contains", "isin", "notin"],
        format_func=lambda x: {
            "eq": "equals", "ne": "not equals", "gt": ">", "lt": "<",
            "ge": ">=", "le": "<=", "contains": "contains", "not_contains": "not contains",
            "isin": "is in list", "notin": "not in list",
        }.get(x, x),
        key="filter_op",
    )
    filter_val_str = st.text_input(
        "Value (for isin/notin: comma-separated)", key="filter_val"
    )
    if st.button("Add: filter rows") and filter_val_str.strip():
        raw = filter_val_str.strip()
        val: list[str] | float | int | str
        if filter_op in ("isin", "notin"):
            val = [v.strip() for v in raw.split(",") if v.strip()]
        elif filter_op in ("gt", "lt", "ge", "le"):
            try:
                val = float(raw) if "." in raw else int(raw)
            except ValueError:
                val = raw
        else:
            val = raw
        _add_step("filter_rows", {"column": filter_col, "op": filter_op, "value": val})

# ── Melt (wide → long) ───────────────────────────────────────────────────

with st.expander("Melt (wide to long)"):
    st.caption(
        "Unpivot wide-format data into tidy long format. "
        "Select ID columns to keep fixed; remaining columns become rows."
    )
    all_cols = list(df.columns)
    melt_id_vars = st.multiselect(
        "ID columns (keep as-is)",
        options=all_cols,
        key="melt_id_vars",
        help="Columns that identify each observation (e.g. Temperature, Time).",
    )
    remaining_cols = [c for c in all_cols if c not in melt_id_vars]
    melt_value_vars = st.multiselect(
        "Value columns to melt (leave empty = all remaining)",
        options=remaining_cols,
        key="melt_value_vars",
    )
    melt_var_name = st.text_input("Variable column name", value="variable", key="melt_var_name")
    melt_value_name = st.text_input("Value column name", value="value", key="melt_value_name")
    if st.button("Add: melt") and melt_id_vars:
        _add_step("melt", {
            "id_vars": melt_id_vars,
            "value_vars": melt_value_vars if melt_value_vars else None,
            "var_name": melt_var_name or "variable",
            "value_name": melt_value_name or "value",
        })

# ── Drop columns ──────────────────────────────────────────────────────────

with st.expander("Drop columns"):
    to_drop = st.multiselect("Columns to drop", options=list(df.columns), key="clean_drop")
    if st.button("Add: drop columns") and to_drop:
        _add_step("drop_columns", {"columns": to_drop})

# ── Impute missing values ─────────────────────────────────────────────────

with st.expander("Impute missing values"):
    n_strat = st.selectbox("Numeric strategy", ["median", "mean", "constant"], key="impute_num_strat")
    n_const = st.text_input("Numeric constant (if strategy=constant)", value="0", key="impute_num_const")
    c_strat = st.selectbox("Categorical strategy", ["mode", "constant"], key="impute_cat_strat")
    c_const = st.text_input(
        "Categorical constant (if strategy=constant)", value="unknown", key="impute_cat_const"
    )

    def _parse_const(s: str, for_numeric: bool):
        s = s.strip()
        if for_numeric:
            try:
                return float(s) if "." in s else int(s)
            except ValueError:
                return None
        return s if s else None

    if st.button("Add: impute"):
        n_fv = _parse_const(n_const, True) if n_strat == "constant" else None
        c_fv = c_const.strip() or None if c_strat == "constant" else None
        if n_strat == "constant" and n_fv is None:
            st.error(f"Invalid numeric constant: '{n_const}'. Enter a valid number.")
        elif c_strat == "constant" and c_fv is None:
            st.error(f"Invalid categorical constant: '{c_const}'. Enter a non-empty string.")
        else:
            _add_step("impute", {
                "numeric": {"strategy": n_strat, "fill_value": n_fv},
                "categorical": {"strategy": c_strat, "fill_value": c_fv},
            })

# ── Drop NA rows ──────────────────────────────────────────────────────────

with st.expander("Drop rows with missing values"):
    dropna_how = st.radio("Drop if missing in", ["any column", "all columns"], key="dropna_how")
    if st.button("Add: drop NA rows"):
        how = "any" if "any" in dropna_how else "all"
        _add_step("dropna_rows", {"how": how})

# ── Deduplicate ───────────────────────────────────────────────────────────

with st.expander("Deduplicate rows"):
    dedup_keep = st.radio("Keep", ["first", "last", "none (drop all dupes)"], key="dedup_keep")
    if st.button("Add: deduplicate"):
        k: str | bool = (
            "first" if "first" in dedup_keep else ("last" if "last" in dedup_keep else False)
        )
        _add_step("deduplicate", {"keep": k})

# ── Pipeline summary ──────────────────────────────────────────────────────

st.subheader("Pipeline")
if steps:
    for i, s in enumerate(steps):
        col1, col2 = st.columns([6, 1])
        with col1:
            st.code(f"{i + 1}. {s['name']}  {s.get('params', {})}")
        with col2:
            if st.button("X", key=f"del_step_{i}"):
                steps.pop(i)
                st.session_state["pipeline_spec"] = {"version": 1, "steps": steps}
                st.rerun()

    col_clear, _ = st.columns([1, 5])
    with col_clear:
        if st.button("Clear all steps"):
            st.session_state["pipeline_spec"] = {"version": 1, "steps": []}
            st.rerun()

    # ── Live preview ──
    with st.expander("Preview pipeline result", expanded=False):
        try:
            from core.pipeline import apply_pipeline

            preview_df = apply_pipeline(df, spec)
            st.write(
                f"**{len(df)}** rows -> **{len(preview_df)}** rows, "
                f"**{len(preview_df.columns)}** columns"
            )
            st.dataframe(preview_df.head(20), use_container_width=True)
        except ValueError as e:
            st.error(f"Pipeline error: {e}")
else:
    st.info("No steps yet. Add steps above.")
