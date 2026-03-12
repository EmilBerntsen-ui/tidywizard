"""Pipeline step functions: apply(df, params) -> df with strict param validation."""

from __future__ import annotations

import re
from typing import Any, Callable

import pandas as pd


# --- drop_columns ---


def _validate_drop_columns_params(params: dict[str, Any]) -> None:
    if not isinstance(params, dict):
        raise ValueError("drop_columns params must be a dict.")
    cols = params.get("columns")
    if cols is None:
        raise ValueError("drop_columns requires 'columns' in params.")
    if not isinstance(cols, (list, tuple)):
        raise ValueError("drop_columns 'columns' must be a list or tuple of column names.")
    for c in cols:
        if not isinstance(c, str):
            raise ValueError(f"drop_columns 'columns' must contain strings, got: {type(c).__name__}")


def apply_drop_columns(df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
    """Drop the given columns. Missing columns are ignored."""
    _validate_drop_columns_params(params)
    cols = [c for c in params["columns"] if c in df.columns]
    if not cols:
        return df.copy()
    return df.drop(columns=cols)


# --- impute ---


def _is_numeric_series(s: pd.Series) -> bool:
    return pd.api.types.is_numeric_dtype(s) and s.dtype.kind in "iufc"


def _validate_impute_params(params: dict[str, Any]) -> None:
    if not isinstance(params, dict):
        raise ValueError("impute params must be a dict.")
    for key in ("numeric", "categorical"):
        if key not in params:
            raise ValueError(f"impute requires '{key}' in params.")
    for key in ("numeric", "categorical"):
        p = params[key]
        if not isinstance(p, dict):
            raise ValueError(f"impute '{key}' must be a dict.")
        strat = p.get("strategy")
        if strat not in ("mean", "median", "constant", "mode", None):
            raise ValueError(
                f"impute '{key}.strategy' must be one of mean, median, constant, mode; got {strat!r}"
            )
        if strat == "constant" and "fill_value" not in p:
            raise ValueError(f"impute '{key}': strategy 'constant' requires 'fill_value'.")


def apply_impute(df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
    """
    Impute missing values: numeric (mean/median/constant) and categorical (mode/constant).
    """
    _validate_impute_params(params)
    out = df.copy()

    num_cfg = params.get("numeric") or {}
    cat_cfg = params.get("categorical") or {}

    for col in out.columns:
        s = out[col]
        if s.isna().sum() == 0:
            continue
        if _is_numeric_series(s):
            cfg = num_cfg
            strategy = cfg.get("strategy") or "median"
            fv = cfg.get("fill_value")
        else:
            cfg = cat_cfg
            strategy = cfg.get("strategy") or "mode"
            fv = cfg.get("fill_value")

        if strategy == "constant":
            if fv is None or pd.isna(fv):
                raise ValueError(f"impute: 'constant' strategy requires a non-null fill_value for {col}.")
            out[col] = s.fillna(fv)
            continue

        if strategy == "mean":
            if not _is_numeric_series(s):
                raise ValueError(f"impute: 'mean' only for numeric columns; {col} is {s.dtype}.")
            out[col] = s.fillna(s.mean())
        elif strategy == "median":
            if not _is_numeric_series(s):
                raise ValueError(f"impute: 'median' only for numeric columns; {col} is {s.dtype}.")
            out[col] = s.fillna(s.median())
        elif strategy == "mode":
            mode_vals = s.mode()
            fill = mode_vals.iloc[0] if len(mode_vals) > 0 else (fv if fv is not None else "unknown")
            # Use where to avoid FutureWarning from fillna downcasting object dtypes
            out[col] = s.where(s.notna(), fill)
        else:
            # no-op / skip
            pass

    return out


# --- dropna_rows ---


def _validate_dropna_rows_params(params: dict[str, Any]) -> None:
    if not isinstance(params, dict):
        raise ValueError("dropna_rows params must be a dict.")
    how = params.get("how", "any")
    if how not in ("any", "all"):
        raise ValueError(f"dropna_rows 'how' must be 'any' or 'all'; got {how!r}")


def apply_dropna_rows(df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
    """Drop rows with missing values. how: 'any' or 'all'."""
    _validate_dropna_rows_params(params)
    how = params.get("how", "any")
    return df.dropna(how=how)


# --- deduplicate ---


def _validate_deduplicate_params(params: dict[str, Any]) -> None:
    if not isinstance(params, dict):
        raise ValueError("deduplicate params must be a dict.")
    keep = params.get("keep", "first")
    if keep not in ("first", "last", False):
        raise ValueError(f"deduplicate 'keep' must be 'first', 'last', or False; got {keep!r}")


def apply_deduplicate(df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
    """Remove duplicate rows. keep: 'first', 'last', or False (drop all dupes)."""
    _validate_deduplicate_params(params)
    keep = params.get("keep", "first")
    return df.drop_duplicates(keep=keep)


# --- melt (wide-to-long) ---


def _validate_melt_params(params: dict[str, Any]) -> None:
    if not isinstance(params, dict):
        raise ValueError("melt params must be a dict.")
    id_vars = params.get("id_vars")
    if id_vars is None:
        raise ValueError("melt requires 'id_vars' in params.")
    if not isinstance(id_vars, (list, tuple)):
        raise ValueError("melt 'id_vars' must be a list or tuple of column names.")
    for c in id_vars:
        if not isinstance(c, str):
            raise ValueError(f"melt 'id_vars' must contain strings, got: {type(c).__name__}")
    value_vars = params.get("value_vars")
    if value_vars is not None:
        if not isinstance(value_vars, (list, tuple)):
            raise ValueError("melt 'value_vars' must be a list, tuple, or null.")
        for c in value_vars:
            if not isinstance(c, str):
                raise ValueError(f"melt 'value_vars' must contain strings, got: {type(c).__name__}")
    var_name = params.get("var_name", "variable")
    if not isinstance(var_name, str):
        raise ValueError("melt 'var_name' must be a string.")
    value_name = params.get("value_name", "value")
    if not isinstance(value_name, str):
        raise ValueError("melt 'value_name' must be a string.")


def apply_melt(df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
    """Unpivot (melt) from wide to long format."""
    _validate_melt_params(params)
    id_vars = params["id_vars"]
    value_vars = params.get("value_vars")
    var_name = params.get("var_name", "variable")
    value_name = params.get("value_name", "value")

    missing_ids = [c for c in id_vars if c not in df.columns]
    if missing_ids:
        raise ValueError(f"melt: id_vars columns not found: {missing_ids}")
    if value_vars is not None:
        missing_vals = [c for c in value_vars if c not in df.columns]
        if missing_vals:
            raise ValueError(f"melt: value_vars columns not found: {missing_vals}")

    return pd.melt(
        df,
        id_vars=id_vars,
        value_vars=value_vars if value_vars else None,
        var_name=var_name,
        value_name=value_name,
    )


# --- rename_columns ---


def _validate_rename_columns_params(params: dict[str, Any]) -> None:
    if not isinstance(params, dict):
        raise ValueError("rename_columns params must be a dict.")
    mapping = params.get("mapping")
    if mapping is None:
        raise ValueError("rename_columns requires 'mapping' in params.")
    if not isinstance(mapping, dict):
        raise ValueError("rename_columns 'mapping' must be a dict of {old_name: new_name}.")
    for k, v in mapping.items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise ValueError("rename_columns mapping keys and values must be strings.")


def apply_rename_columns(df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
    """Rename columns using a mapping. Missing columns are ignored."""
    _validate_rename_columns_params(params)
    mapping = {k: v for k, v in params["mapping"].items() if k in df.columns}
    return df.rename(columns=mapping)


# --- filter_rows ---


_FILTER_OPS = ("eq", "ne", "gt", "lt", "ge", "le", "contains", "not_contains", "isin", "notin")


def _validate_filter_rows_params(params: dict[str, Any]) -> None:
    if not isinstance(params, dict):
        raise ValueError("filter_rows params must be a dict.")
    column = params.get("column")
    if not isinstance(column, str):
        raise ValueError("filter_rows requires 'column' (string) in params.")
    op = params.get("op")
    if op not in _FILTER_OPS:
        raise ValueError(
            f"filter_rows 'op' must be one of {', '.join(_FILTER_OPS)}; got {op!r}"
        )
    if "value" not in params:
        raise ValueError("filter_rows requires 'value' in params.")


def apply_filter_rows(df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
    """Keep rows matching a condition on a single column."""
    _validate_filter_rows_params(params)
    col = params["column"]
    if col not in df.columns:
        raise ValueError(f"filter_rows: column '{col}' not found.")
    op = params["op"]
    val = params["value"]
    s = df[col]
    if op == "eq":
        mask = s == val
    elif op == "ne":
        mask = s != val
    elif op == "gt":
        mask = s > val
    elif op == "lt":
        mask = s < val
    elif op == "ge":
        mask = s >= val
    elif op == "le":
        mask = s <= val
    elif op == "contains":
        mask = s.astype(str).str.contains(str(val), na=False)
    elif op == "not_contains":
        mask = ~s.astype(str).str.contains(str(val), na=False)
    elif op == "isin":
        mask = s.isin(val if isinstance(val, list) else [val])
    elif op == "notin":
        mask = ~s.isin(val if isinstance(val, list) else [val])
    else:
        mask = pd.Series(True, index=df.index)
    return df.loc[mask].reset_index(drop=True)


# --- replace_values ---


def _validate_replace_values_params(params: dict[str, Any]) -> None:
    if not isinstance(params, dict):
        raise ValueError("replace_values params must be a dict.")
    mapping = params.get("mapping")
    if mapping is None or not isinstance(mapping, dict):
        raise ValueError("replace_values requires 'mapping' dict of {old_value: new_value}.")


def apply_replace_values(df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
    """Replace specific cell values. Optionally scoped to certain columns."""
    _validate_replace_values_params(params)
    mapping = params["mapping"]
    columns = params.get("columns")
    out = df.copy()
    if columns:
        for col in columns:
            if col in out.columns:
                out[col] = out[col].replace(mapping)
    else:
        out = out.replace(mapping)
    return out


# --- strip_whitespace ---


def _validate_strip_whitespace_params(params: dict[str, Any]) -> None:
    if not isinstance(params, dict):
        raise ValueError("strip_whitespace params must be a dict.")


def apply_strip_whitespace(df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
    """Strip leading/trailing whitespace from column names and/or cell values.

    Optionally replaces all internal spaces in both headers and cell values with '_'.
    """
    _validate_strip_whitespace_params(params)
    out = df.copy()
    if params.get("strip_headers", True):
        out.columns = [c.strip() if isinstance(c, str) else c for c in out.columns]
    if params.get("replace_spaces"):
        out.columns = [c.replace(" ", "_") if isinstance(c, str) else c for c in out.columns]
    columns = params.get("columns")
    target_cols = columns if columns else [c for c in out.columns if out[c].dtype == object]
    for col in target_cols:
        if col in out.columns and out[col].dtype == object:
            out[col] = out[col].str.strip()
            if params.get("replace_spaces"):
                out[col] = out[col].str.replace(" ", "_", regex=False)
    return out


# --- normalise_text ---


def _validate_normalise_text_params(params: dict[str, Any]) -> None:
    if not isinstance(params, dict):
        raise ValueError("normalise_text params must be a dict.")


def apply_normalise_text(df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
    """Lowercase and/or remove special characters from column names and/or cell values.

    Special characters are anything that is not a letter, digit, space, or underscore.
    """
    _validate_normalise_text_params(params)
    out = df.copy()

    if params.get("lowercase_headers"):
        out.columns = [c.lower() if isinstance(c, str) else c for c in out.columns]
    if params.get("remove_special_headers"):
        out.columns = [
            re.sub(r"[^a-zA-Z0-9\s_]", "", c) if isinstance(c, str) else c
            for c in out.columns
        ]

    str_cols = [c for c in out.columns if out[c].dtype == object]
    if params.get("lowercase_values"):
        for col in str_cols:
            out[col] = out[col].str.lower()
    if params.get("remove_special_values"):
        for col in str_cols:
            out[col] = out[col].str.replace(r"[^a-zA-Z0-9\s_]", "", regex=True)

    return out


# --- Registry ---


STEP_REGISTRY: dict[str, Callable[[pd.DataFrame, dict[str, Any]], pd.DataFrame]] = {
    "drop_columns": apply_drop_columns,
    "impute": apply_impute,
    "dropna_rows": apply_dropna_rows,
    "deduplicate": apply_deduplicate,
    "melt": apply_melt,
    "rename_columns": apply_rename_columns,
    "filter_rows": apply_filter_rows,
    "replace_values": apply_replace_values,
    "strip_whitespace": apply_strip_whitespace,
    "normalise_text": apply_normalise_text,
}
