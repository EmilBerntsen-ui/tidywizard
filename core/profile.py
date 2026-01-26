"""DataFrame profiling: dtypes, missing, unique, top values, duplicates."""

from __future__ import annotations

from typing import Any

import pandas as pd


def profile_dataframe(df: pd.DataFrame) -> dict[str, Any]:
    """
    Build a profile of the DataFrame for the UI.

    Returns:
        dict with:
          - columns: list of {name, dtype, pct_missing, n_unique, top_values}
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

        columns.append(
            {
                "name": col,
                "dtype": dtype,
                "pct_missing": pct_missing,
                "n_unique": n_unique,
                "top_values": top_values,
            }
        )

    return {
        "columns": columns,
        "n_rows": n_rows,
        "n_cols": n_cols,
        "n_duplicates": n_duplicates,
    }
