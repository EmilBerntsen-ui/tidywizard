"""Parse Prometheus Panta (nanoDSF) instrument files into tidy long format."""

from __future__ import annotations

import io
import re
from typing import Optional

import pandas as pd


_CAP_PATTERN = re.compile(r'[Cc]ap(?:illary)?\.?\s*(\d+)')


def _classify_measurement_type(col_lower: str) -> Optional[str]:
    """Classify a lowercased column name into a measurement type by keyword."""
    if "temp" in col_lower:
        return "temperature"
    if "ratio" in col_lower:
        return "ratio"
    if "turbid" in col_lower or "scatter" in col_lower:
        return "turbidity"
    if "radius" in col_lower or " rh " in col_lower or col_lower.strip().endswith(" rh"):
        return "cumulant_radius"
    return None


def _parse_column(col_name: str) -> tuple[int, str]:
    """
    Extract (capillary_number, measurement_type) from a Prometheus Panta column name.

    Raises ValueError if the capillary number or measurement type cannot be determined.
    Column name variants from different firmware versions are handled via keyword
    matching and a permissive capillary number regex.
    """
    cap_match = _CAP_PATTERN.search(col_name)
    if cap_match is None:
        raise ValueError(
            f"Cannot extract capillary number from column: {col_name!r}. "
            f"Expected a name containing 'Cap.N', 'Cap N', or 'Capillary N'."
        )
    capillary = int(cap_match.group(1))

    mtype = _classify_measurement_type(col_name.lower())
    if mtype is None:
        raise ValueError(
            f"Unrecognised column type: {col_name!r}. "
            f"Expected column name containing one of: 'Temp', 'Ratio', "
            f"'Turbid'/'Scatter', 'Radius'/'Rh'."
        )
    return capillary, mtype


def _pair_columns(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Convert paired (temperature, measurement) columns to long-format rows.

    Columns must arrive as alternating pairs: temperature column first, then
    measurement column, for the same capillary. Raises ValueError if the count
    is odd, a pair is in the wrong order, or capillary numbers don't match.
    """
    cols = list(df_raw.columns)
    if len(cols) % 2 != 0:
        raise ValueError(
            f"Expected an even number of columns (temperature+measurement pairs), "
            f"got {len(cols)}."
        )

    frames = []
    for i in range(0, len(cols), 2):
        temp_col = cols[i]
        meas_col = cols[i + 1]

        temp_cap, temp_type = _parse_column(temp_col)
        meas_cap, meas_type = _parse_column(meas_col)

        if temp_type != "temperature":
            raise ValueError(
                f"Column at index {i} should be a temperature column, got: {temp_col!r}"
            )
        if meas_type == "temperature":
            raise ValueError(
                f"Column at index {i + 1} should be a measurement column, got: {meas_col!r}"
            )
        if temp_cap != meas_cap:
            raise ValueError(
                f"Capillary mismatch at columns {i} and {i + 1}: "
                f"temperature column is Cap.{temp_cap} "
                f"but measurement column is Cap.{meas_cap}."
            )

        mini = pd.DataFrame({
            "capillary": temp_cap,
            "measurement_type": meas_type,
            "temperature": pd.to_numeric(df_raw[temp_col], errors="coerce"),
            "value": pd.to_numeric(df_raw[meas_col], errors="coerce"),
        })
        frames.append(mini)

    return pd.concat(frames, ignore_index=True)


def load_melting_scan(source) -> pd.DataFrame:
    """
    Load a Prometheus Panta melting scan CSV into tidy long format.

    The file must be semicolon-separated with cp1252 encoding (standard Windows
    output from NanoTemper software). Columns arrive in alternating pairs of
    (temperature, measurement) for each capillary.

    Args:
        source: file path string, bytes, or BytesIO object.

    Returns:
        DataFrame with columns:
            capillary (int)           — 1-indexed capillary number
            measurement_type (str)    — 'ratio', 'turbidity', or 'cumulant_radius'
            temperature (float)       — temperature in degrees Celsius
            value (float)             — raw measurement value

    Raises:
        ValueError: if source is empty, columns are unrecognised,
                    or a capillary number mismatch is detected.
    """
    if source is None:
        raise ValueError("Melting scan source is empty.")
    if isinstance(source, bytes):
        if len(source) == 0:
            raise ValueError("Melting scan source is empty.")
        source = io.BytesIO(source)
    if isinstance(source, io.BytesIO) and len(source.getvalue()) == 0:
        raise ValueError("Melting scan source is empty.")

    try:
        df_raw = pd.read_csv(
            source,
            sep=";",
            encoding="cp1252",
            dtype=str,
            skip_blank_lines=True,
        )
    except Exception as e:
        raise ValueError(f"Failed to read melting scan CSV: {e}") from e

    if df_raw.empty or len(df_raw.columns) == 0:
        raise ValueError("Melting scan CSV parsed to an empty DataFrame.")

    out = _pair_columns(df_raw)
    out["capillary"] = out["capillary"].astype(int)
    return out.reset_index(drop=True)


# ── Data table (Excel, multi-row merged header) ───────────────────────────

_PANTA_DATA_TABLE_N_HEADER_ROWS = 3


def _read_excel_any_engine(source, **kwargs) -> pd.DataFrame:
    """Try xlrd (for .xls) first, fall back to openpyxl (for .xlsx)."""
    pos = source.tell() if isinstance(source, io.BytesIO) else None
    try:
        import xlrd  # noqa: F401
        return pd.read_excel(source, engine="xlrd", **kwargs)
    except ImportError:
        pass
    except Exception:
        if pos is not None:
            source.seek(pos)
    try:
        return pd.read_excel(source, engine="openpyxl", **kwargs)
    except Exception as e:
        raise ValueError(f"Failed to read Excel data table: {e}") from e


def _flatten_headers(header_rows: pd.DataFrame) -> list[str]:
    """
    Flatten multi-row merged-cell headers into a single list of column names.

    Excel merged cells appear as a value in the first cell and NaN in all others.
    We forward-fill all section/group rows (every row except the last) to un-merge
    them, then combine non-empty parts from each level with '_'.

    The leaf row (last header row) is NOT forward-filled — it contains one explicit
    name per column, and carrying it forward would corrupt unrelated adjacent columns.

    Example:
        Row 0 (ffilled): General  General  General  ...  Ratio  Ratio  Ratio
        Row 1 (ffilled): NaN      NaN      NaN      ...  IP#1   IP#1   Slope
        Row 2 (as-is):   Exclude  Cap      datafile ...  ø      sigma  ø
        Result:          Exclude  General_Cap  General_datafile  ...  Ratio_IP#1_ø
    """
    n_rows = header_rows.shape[0]
    filled = header_rows.copy().astype(object)

    for i in range(n_rows - 1):
        filled.iloc[i] = filled.iloc[i].ffill()

    flat_cols = []
    for col_idx in range(filled.shape[1]):
        parts = []
        for row_idx in range(n_rows):
            val = filled.iloc[row_idx, col_idx]
            if pd.notna(val) and str(val).strip():
                parts.append(str(val).strip())
        # Remove consecutive duplicates (edge case: same word in group + leaf)
        deduped: list[str] = []
        for p in parts:
            if not deduped or p != deduped[-1]:
                deduped.append(p)
        flat_cols.append("_".join(deduped) if deduped else f"col_{col_idx}")

    # Ensure uniqueness: append .1, .2, … for duplicate names
    seen: dict[str, int] = {}
    unique_cols: list[str] = []
    for name in flat_cols:
        if name in seen:
            seen[name] += 1
            unique_cols.append(f"{name}.{seen[name]}")
        else:
            seen[name] = 0
            unique_cols.append(name)

    return unique_cols


def load_data_table(source, n_header_rows: int = _PANTA_DATA_TABLE_N_HEADER_ROWS) -> pd.DataFrame:
    """
    Load a Prometheus Panta data table Excel file into a flat DataFrame.

    The data table uses a multi-row merged-cell header (section > subsection > column).
    This function flattens the header into single-level column names and returns
    all data rows below the header.

    Supports both .xls (legacy binary) and .xlsx (Open XML) formats.

    Args:
        source: file path string, bytes, or BytesIO object.
        n_header_rows: number of header rows to collapse (default: 3).

    Returns:
        DataFrame with flat column names derived from the merged header hierarchy.

    Raises:
        ValueError: if source is empty, unreadable, or has fewer rows than
                    n_header_rows.
    """
    if source is None:
        raise ValueError("Data table source is empty.")
    if isinstance(source, bytes):
        if len(source) == 0:
            raise ValueError("Data table source is empty.")
        source = io.BytesIO(source)
    if isinstance(source, io.BytesIO) and len(source.getvalue()) == 0:
        raise ValueError("Data table source is empty.")

    raw = _read_excel_any_engine(source, header=None, dtype=str)

    if raw.empty or len(raw.columns) == 0:
        raise ValueError("Data table parsed to an empty DataFrame.")
    if raw.shape[0] < n_header_rows:
        raise ValueError(
            f"Data table has only {raw.shape[0]} row(s); "
            f"expected at least {n_header_rows} header rows."
        )

    header_df = raw.iloc[:n_header_rows].reset_index(drop=True)
    data_df = raw.iloc[n_header_rows:].copy()

    flat_cols = _flatten_headers(header_df)
    data_df.columns = flat_cols

    viscosity_col = next(
        (c for c in data_df.columns if c.lower() == "viscosity_components"), None
    )
    if viscosity_col is not None:
        data_df = _split_text_number_unit(data_df, viscosity_col)

    solvent_col = next(
        (c for c in data_df.columns if c.lower() == "viscosity_solvent"), None
    )
    if solvent_col is not None:
        data_df = data_df.drop(columns=[solvent_col])

    return data_df.reset_index(drop=True)


def _split_text_number_unit(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Split a column whose values look like 'Sodium acetate 25 mM' into two columns:
        {col}_name        → 'Sodium acetate'  (str)
        {col}_value_{unit} → 25.0             (float, unit taken from the data)

    The unit is read from the first matching row and must be consistent across
    all non-null rows. Raises ValueError if units differ between rows.

    Rows that don't match the expected pattern (text, number, unit) are left as
    NaN in both output columns.
    """
    _PATTERN = re.compile(r'^(.+?)\s+(\d+(?:\.\d+)?)\s*(\S+)\s*$')

    names: list = []
    values: list = []
    units: list[Optional[str]] = []

    for raw in df[col]:
        if pd.isna(raw) or str(raw).strip() == "":
            names.append(pd.NA)
            values.append(pd.NA)
            units.append(None)
            continue
        m = _PATTERN.match(str(raw).strip())
        if m:
            names.append(m.group(1).rstrip(":,; "))
            values.append(float(m.group(2)))
            units.append(m.group(3))
        else:
            names.append(raw)
            values.append(pd.NA)
            units.append(None)

    non_null_units = [u for u in units if u is not None]
    if not non_null_units:
        value_col = f"{col}_value"
    else:
        unique_units = set(non_null_units)
        if len(unique_units) > 1:
            raise ValueError(
                f"Column {col!r} has mixed units across rows: {unique_units}. "
                "All rows must use the same unit."
            )
        value_col = f"{col}_value_{non_null_units[0]}"

    insert_at = df.columns.get_loc(col)
    df = df.drop(columns=[col])
    df.insert(insert_at, f"{col}_name", names)
    df.insert(insert_at + 1, value_col, values)
    return df
