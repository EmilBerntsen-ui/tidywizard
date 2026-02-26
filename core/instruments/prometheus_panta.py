"""Parse Prometheus Panta (nanoDSF) melting scan CSV into tidy long format."""

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


def load_melting_scan(source):
    raise NotImplementedError
