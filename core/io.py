"""Load CSV and Excel files with consistent dtype handling."""

from __future__ import annotations

import io
from typing import Any

import pandas as pd


def load_csv(
    source: str | bytes | io.BytesIO,
    *,
    encoding: str = "utf-8",
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Load a CSV file into a DataFrame with good defaults.

    Args:
        source: File path, bytes, or BytesIO.
        encoding: Text encoding (default utf-8).
        **kwargs: Passed to pandas.read_csv.

    Returns:
        DataFrame with consistent handling (object for mixed types).

    Raises:
        ValueError: If source is empty or parsing fails with a clear message.
    """
    if source is None:
        raise ValueError("CSV source is empty.")
    if isinstance(source, bytes) and len(source) == 0:
        raise ValueError("CSV source is empty.")
    if isinstance(source, io.BytesIO) and len(source.getvalue()) == 0:
        raise ValueError("CSV source is empty.")

    # pandas.read_csv expects path or file-like; wrap bytes in BytesIO.
    if isinstance(source, bytes):
        source = io.BytesIO(source)

    default_kwargs: dict[str, Any] = {
        "encoding": encoding,
        "dtype_backend": "numpy_nullable",
        "na_values": ["", "NA", "N/A", "null", "None", "#N/A", "nan"],
        "keep_default_na": True,
    }
    opts = {**default_kwargs, **kwargs}

    try:
        df = pd.read_csv(source, **opts)
    except Exception as e:
        raise ValueError(f"Failed to load CSV: {e}") from e

    if df.empty and len(df.columns) == 0:
        raise ValueError("CSV parsed to an empty DataFrame with no columns.")

    return _normalize_dtypes(df)


def load_excel(
    source: str | bytes | io.BytesIO,
    *,
    sheet_name: int | str = 0,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Load an Excel (.xlsx) file into a DataFrame.

    Uses openpyxl engine. First sheet by default.

    Args:
        source: File path, bytes, or BytesIO.
        sheet_name: Sheet to read (0 or name).
        **kwargs: Passed to pandas.read_excel.

    Returns:
        DataFrame with consistent dtypes.

    Raises:
        ValueError: If source is empty or parsing fails.
    """
    if source is None:
        raise ValueError("Excel source is empty.")
    if isinstance(source, bytes) and len(source) == 0:
        raise ValueError("Excel source is empty.")
    if isinstance(source, io.BytesIO) and len(source.getvalue()) == 0:
        raise ValueError("Excel source is empty.")

    default_kwargs: dict[str, Any] = {
        "sheet_name": sheet_name,
        "engine": "openpyxl",
        "dtype_backend": "numpy_nullable",
    }
    opts = {**default_kwargs, **kwargs}

    try:
        df = pd.read_excel(source, **opts)
    except Exception as e:
        raise ValueError(f"Failed to load Excel: {e}") from e

    if df.empty and len(df.columns) == 0:
        raise ValueError("Excel sheet parsed to an empty DataFrame with no columns.")

    return _normalize_dtypes(df)


def get_excel_sheet_names(source: str | bytes | io.BytesIO) -> list[str]:
    """Return list of sheet names from an Excel file."""
    if isinstance(source, bytes):
        source = io.BytesIO(source)
    import openpyxl

    wb = openpyxl.load_workbook(source, read_only=True, data_only=True)
    names = wb.sheetnames
    wb.close()
    return names


def _normalize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure mixed-type columns become object; avoid categorical for consistency."""
    out = df.copy()
    for c in out.columns:
        if out[c].dtype.name == "category":
            out[c] = out[c].astype(object)
        # Leave numeric and datetime as-is; object stays object
    return out
