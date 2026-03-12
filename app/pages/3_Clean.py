"""Clean: interactive pipeline builder with all transformation steps."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st

from core.profile import profile_dataframe

st.title("Clean")
st.markdown("Build your cleaning pipeline step by step. Each step is applied in order when you export.")

if "df_raw" not in st.session_state:
    st.warning("No dataset loaded yet. Go to **Upload** first.")
    st.stop()

if "pipeline_spec" not in st.session_state:
    st.session_state["pipeline_spec"] = {"version": 1, "steps": []}

df = st.session_state["df_raw"]
spec = st.session_state["pipeline_spec"]
steps = spec.get("steps") or []
columns = list(df.columns)


def _add_step(name: str, params: dict) -> None:
    steps.append({"name": name, "params": params})
    st.session_state["pipeline_spec"] = {"version": 1, "steps": steps}
    st.rerun()


def _col_exists(col: str) -> bool:
    return col in columns


# ── Suggested steps (based on data) ─────────────────────────────────────

_profile = profile_dataframe(df)
_suggestions = []
_whitespace_cols = [c["name"] for c in _profile["columns"] if c.get("has_whitespace_in_name")]
if _whitespace_cols:
    _suggestions.append("**Strip whitespace** — some column names have extra spaces")
if _profile["n_duplicates"] > 0:
    _suggestions.append(f"**Deduplicate** — {_profile['n_duplicates']} duplicate rows found")
_missing_cols = [c["name"] for c in _profile["columns"] if c["pct_missing"] > 0]
if _missing_cols:
    _suggestions.append(f"**Impute** or **Drop missing rows** — {len(_missing_cols)} columns have missing values")

if _suggestions:
    st.info("**Suggested steps based on your data:**\n\n" + "\n\n".join(f"- {s}" for s in _suggestions))


# ── Strip whitespace ────────────────────────────────────────────────────

with st.expander("Strip whitespace"):
    st.caption("Remove extra spaces from the start and end of column names and cell values.")
    strip_headers = st.checkbox("Strip column names", value=True, key="strip_headers")
    strip_cells = st.checkbox("Strip cell values", value=True, key="strip_cells")
    replace_spaces = st.checkbox(
        "Replace spaces in values with '_'",
        value=False, key="strip_replace_spaces",
        help="e.g. 'Sodium acetate' → 'Sodium_acetate'",
    )
    if st.button("Add step", key="btn_strip"):
        _add_step("strip_whitespace", {
            "strip_headers": strip_headers,
            "columns": None if strip_cells else [],
            "replace_spaces": replace_spaces,
        })

# ── Rename columns ──────────────────────────────────────────────────────

with st.expander("Rename columns"):
    st.caption("Give columns new names.")
    cols_to_rename = st.multiselect("Select columns to rename", columns, key="rename_select")
    rename_mapping: dict[str, str] = {}
    for col in cols_to_rename:
        new_name = st.text_input(f"New name for '{col}'", value=col, key=f"rename_{col}")
        if new_name and new_name != col:
            rename_mapping[col] = new_name
    if st.button("Add step", key="btn_rename"):
        if not rename_mapping:
            st.warning("No columns were renamed. Change at least one name.")
        else:
            _add_step("rename_columns", {"mapping": rename_mapping})

# ── Replace values ──────────────────────────────────────────────────────

with st.expander("Replace values"):
    st.caption(
        "Replace specific values in your data. "
        "Useful for cleaning up placeholder text like 'N/A', 'BDL', or instrument codes."
    )
    use_preset = st.checkbox(
        "Replace common placeholders with missing (BDL, <LOD, N/D, ---, .)",
        value=False, key="replace_preset",
    )
    custom_old = st.text_input("Value to find", key="replace_old")
    custom_new = st.text_input("Replace with (leave empty to mark as missing)", key="replace_new")
    replace_cols = st.multiselect(
        "Apply to specific columns (leave empty for all)", columns, key="replace_cols"
    )
    if st.button("Add step", key="btn_replace"):
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
            st.warning("Nothing to replace. Check the preset box or enter a value to find.")

# ── Filter rows ─────────────────────────────────────────────────────────

with st.expander("Filter rows"):
    st.caption("Keep only rows that match a condition. Rows that don't match are removed.")
    filter_col = st.selectbox("Column", columns, key="filter_col")
    filter_op = st.selectbox(
        "Condition",
        ["eq", "ne", "gt", "lt", "ge", "le", "contains", "not_contains", "isin", "notin"],
        format_func=lambda x: {
            "eq": "equals (=)", "ne": "not equals (!=)",
            "gt": "greater than (>)", "lt": "less than (<)",
            "ge": "greater or equal (>=)", "le": "less or equal (<=)",
            "contains": "text contains", "not_contains": "text does not contain",
            "isin": "is one of (comma-separated list)", "notin": "is not one of (comma-separated list)",
        }.get(x, x),
        key="filter_op",
    )

    if filter_op in ("isin", "notin"):
        filter_val_str = st.text_input(
            "Values (comma-separated)", key="filter_val",
            help="Enter values separated by commas, e.g.: red, blue, green",
        )
    else:
        filter_val_str = st.text_input("Value", key="filter_val")

    if st.button("Add step", key="btn_filter"):
        if not filter_val_str.strip():
            st.warning("Enter a value to filter on.")
        elif not _col_exists(filter_col):
            st.error(f"Column '{filter_col}' not found in the dataset.")
        else:
            raw = filter_val_str.strip()
            val: list[str] | float | int | str
            if filter_op in ("isin", "notin"):
                val = [v.strip() for v in raw.split(",") if v.strip()]
            elif filter_op in ("gt", "lt", "ge", "le"):
                try:
                    val = float(raw) if "." in raw else int(raw)
                except ValueError:
                    st.error(f"'{raw}' is not a valid number for this comparison.")
                    st.stop()
            else:
                val = raw
            _add_step("filter_rows", {"column": filter_col, "op": filter_op, "value": val})

# ── Reshape: wide to long ───────────────────────────────────────────────

with st.expander("Reshape: wide to long (melt)"):
    st.caption(
        "Convert wide-format data into long format. "
        "Pick the columns that identify each row (like Sample, Time). "
        "The remaining columns are stacked into two new columns: one for the name, one for the value."
    )
    st.markdown(
        "*Example: columns `Sample, Gene_A, Gene_B` become `Sample, Gene, Expression` "
        "with one row per gene per sample.*"
    )
    all_cols = columns
    melt_id_vars = st.multiselect(
        "Identifier columns (keep as-is)",
        options=all_cols, key="melt_id_vars",
        help="Columns that stay fixed, e.g. Sample, Temperature, Time.",
    )
    remaining_cols = [c for c in all_cols if c not in melt_id_vars]
    melt_value_vars = st.multiselect(
        "Columns to stack (leave empty = all remaining)",
        options=remaining_cols, key="melt_value_vars",
    )
    melt_var_name = st.text_input("Name for the new 'category' column", value="variable", key="melt_var_name")
    melt_value_name = st.text_input("Name for the new 'value' column", value="value", key="melt_value_name")
    if st.button("Add step", key="btn_melt"):
        if not melt_id_vars:
            st.warning("Select at least one identifier column.")
        else:
            _add_step("melt", {
                "id_vars": melt_id_vars,
                "value_vars": melt_value_vars if melt_value_vars else None,
                "var_name": melt_var_name or "variable",
                "value_name": melt_value_name or "value",
            })

# ── Drop columns ────────────────────────────────────────────────────────

with st.expander("Drop columns"):
    st.caption("Remove columns you don't need.")
    to_drop = st.multiselect("Columns to remove", options=columns, key="clean_drop")
    if st.button("Add step", key="btn_drop"):
        if not to_drop:
            st.warning("Select at least one column to drop.")
        else:
            _add_step("drop_columns", {"columns": to_drop})

# ── Impute missing values ───────────────────────────────────────────────

with st.expander("Fill in missing values (impute)"):
    st.caption(
        "Automatically fill empty cells. "
        "Numeric columns and text columns are handled separately."
    )
    n_strat = st.selectbox(
        "For number columns, fill with",
        ["median", "mean", "constant"],
        format_func=lambda x: {"median": "Median (middle value)", "mean": "Mean (average)", "constant": "A fixed number"}.get(x, x),
        key="impute_num_strat",
    )
    n_const = st.text_input("Fixed number (only if 'fixed number' selected)", value="0", key="impute_num_const")
    c_strat = st.selectbox(
        "For text columns, fill with",
        ["mode", "constant"],
        format_func=lambda x: {"mode": "Most common value", "constant": "A fixed word"}.get(x, x),
        key="impute_cat_strat",
    )
    c_const = st.text_input(
        "Fixed word (only if 'fixed word' selected)", value="unknown", key="impute_cat_const"
    )

    def _parse_const(s: str, for_numeric: bool):
        s = s.strip()
        if for_numeric:
            try:
                return float(s) if "." in s else int(s)
            except ValueError:
                return None
        return s if s else None

    if st.button("Add step", key="btn_impute"):
        n_fv = _parse_const(n_const, True) if n_strat == "constant" else None
        c_fv = c_const.strip() or None if c_strat == "constant" else None
        if n_strat == "constant" and n_fv is None:
            st.error(f"'{n_const}' is not a valid number.")
        elif c_strat == "constant" and c_fv is None:
            st.error("Enter a non-empty word for text columns.")
        else:
            _add_step("impute", {
                "numeric": {"strategy": n_strat, "fill_value": n_fv},
                "categorical": {"strategy": c_strat, "fill_value": c_fv},
            })

# ── Drop rows with missing values ───────────────────────────────────────

with st.expander("Drop rows with missing values"):
    st.caption("Remove rows that have empty cells.")
    dropna_how = st.radio(
        "When to drop a row",
        ["any", "all"],
        format_func=lambda x: {
            "any": "If any column is empty (strict — removes rows with even one missing value)",
            "all": "Only if every column is empty (lenient — keeps partially filled rows)",
        }.get(x, x),
        key="dropna_how",
    )
    if st.button("Add step", key="btn_dropna"):
        _add_step("dropna_rows", {"how": dropna_how})

# ── Deduplicate ─────────────────────────────────────────────────────────

with st.expander("Remove duplicate rows"):
    st.caption("Remove rows that are exact copies of another row.")
    dedup_keep = st.radio(
        "Which copy to keep",
        ["first", "last", "none"],
        format_func=lambda x: {
            "first": "Keep the first occurrence",
            "last": "Keep the last occurrence",
            "none": "Remove all copies (drop every duplicate)",
        }.get(x, x),
        key="dedup_keep",
    )
    if st.button("Add step", key="btn_dedup"):
        k: str | bool = dedup_keep if dedup_keep != "none" else False
        _add_step("deduplicate", {"keep": k})

# ── Pipeline summary ────────────────────────────────────────────────────

st.divider()
st.subheader("Your pipeline")

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

    with st.expander("Preview result", expanded=False):
        try:
            from core.pipeline import apply_pipeline

            with st.spinner("Applying pipeline..."):
                preview_df = apply_pipeline(df, spec)
            st.write(
                f"**{len(df)}** rows → **{len(preview_df)}** rows, "
                f"**{len(preview_df.columns)}** columns"
            )
            st.dataframe(preview_df.head(20), use_container_width=True)
        except ValueError as e:
            st.error(f"Pipeline error: {e}")
else:
    st.info("No steps added yet. Use the sections above to build your pipeline.")
