"""Pipeline step functions: apply(df, params) -> df with strict param validation."""

from __future__ import annotations

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
            if fv is None and pd.isna(fv):
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


# --- Registry ---


STEP_REGISTRY: dict[str, Callable[[pd.DataFrame, dict[str, Any]], pd.DataFrame]] = {
    "drop_columns": apply_drop_columns,
    "impute": apply_impute,
    "dropna_rows": apply_dropna_rows,
    "deduplicate": apply_deduplicate,
}
