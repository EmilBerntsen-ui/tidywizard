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


def _pair_columns(columns):
    raise NotImplementedError


def load_melting_scan(source):
    raise NotImplementedError
