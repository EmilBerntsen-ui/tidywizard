"""DataFrame profiling: dtypes, missing, unique, top values, duplicates, numeric stats."""

from __future__ import annotations

from typing import Any

import pandas as pd


def profile_dataframe(df: pd.DataFrame) -> dict[str, Any]:
    """
    Build a profile of the DataFrame for the UI.

    Returns:
        dict with:
          - columns: list of column info dicts
          - n_rows, n_cols
          - n_duplicates: number of duplicate rows
    """
    if df is None or not isinstance(df, pd.DataFrame):
        return {"columns": [], "n_rows": 0, "n_cols": 0, "n_duplicates": 0}

    n_rows, n_cols = df.shape
    n_duplicates = int(df.duplicated().sum())

    columns: list[dict[str, Any]] = []
    for col in df.columns:
        s = df[col]
        dtype = str(s.dtype)
        n_missing = s.isna().sum()
        pct_missing = round(100.0 * n_missing / len(s), 2) if len(s) > 0 else 0.0
        n_unique = int(s.nunique())

        top_values: list[tuple[Any, int]] = []
        if n_unique > 0 and n_unique <= 100 and s.dtype == object:
            vc = s.dropna().value_counts()
            for val, cnt in vc.head(5).items():
                top_values.append((val, int(cnt)))

        info: dict[str, Any] = {
            "name": col,
            "dtype": dtype,
            "pct_missing": pct_missing,
            "n_unique": n_unique,
            "top_values": top_values,
        }

        # Numeric statistics
        if pd.api.types.is_numeric_dtype(s) and s.notna().any():
            info["min"] = float(s.min())
            info["max"] = float(s.max())
            info["mean"] = round(float(s.mean()), 4)
            info["std"] = round(float(s.std()), 4) if len(s) > 1 else 0.0

        # Constant column detection
        info["is_constant"] = bool(s.dropna().nunique() <= 1) and s.notna().any()

        # Whitespace in column name
        info["has_whitespace_in_name"] = isinstance(col, str) and col != col.strip()

        columns.append(info)

    return {
        "columns": columns,
        "n_rows": n_rows,
        "n_cols": n_cols,
        "n_duplicates": n_duplicates,
    }
