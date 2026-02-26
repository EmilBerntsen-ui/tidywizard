"""Tests for the Prometheus Panta melting scan parser."""

from __future__ import annotations

import io

import pandas as pd
import pytest

from core.instruments.prometheus_panta import (
    _classify_measurement_type,
    _parse_column,
    _pair_columns,
    load_melting_scan,
)


# ── _classify_measurement_type ───────────────────────────────────────────


def test_classify_temp() -> None:
    assert _classify_measurement_type("temperatur for cap.1 (\u00b0c)") == "temperature"


def test_classify_ratio() -> None:
    assert _classify_measurement_type("ratio 350 nm / 330 nm for cap.1") == "ratio"


def test_classify_turbidity_turbid() -> None:
    assert _classify_measurement_type("turbidity for cap.1") == "turbidity"


def test_classify_turbidity_scatter() -> None:
    assert _classify_measurement_type("scattering intensity for cap.1") == "turbidity"


def test_classify_cumulant_radius() -> None:
    assert _classify_measurement_type("cumulant radius for cap.1 (nm)") == "cumulant_radius"


def test_classify_unknown_returns_none() -> None:
    assert _classify_measurement_type("unknown signal for cap.1") is None


# ── _parse_column ─────────────────────────────────────────────────────────


def test_parse_column_temperature() -> None:
    cap, mtype = _parse_column("Temperatur for Cap.1 (\u00b0C)")
    assert cap == 1
    assert mtype == "temperature"


def test_parse_column_ratio() -> None:
    cap, mtype = _parse_column("Ratio 350 nm / 330 nm for Cap.3")
    assert cap == 3
    assert mtype == "ratio"


def test_parse_column_turbidity() -> None:
    cap, mtype = _parse_column("Turbidity for Cap.12")
    assert cap == 12
    assert mtype == "turbidity"


def test_parse_column_cumulant_radius() -> None:
    cap, mtype = _parse_column("Cumulant Radius for Cap.2 (nm)")
    assert cap == 2
    assert mtype == "cumulant_radius"


def test_parse_column_variant_capillary_label() -> None:
    """Different firmware versions use different capillary label formats."""
    cap, _ = _parse_column("Ratio 350/330 for Capillary 5")
    assert cap == 5
    cap, _ = _parse_column("Turbidity for Cap 7")
    assert cap == 7


def test_parse_column_no_capillary_raises() -> None:
    with pytest.raises(ValueError, match="capillary number"):
        _parse_column("Ratio 350 nm / 330 nm (no capillary here)")


def test_parse_column_unknown_type_raises() -> None:
    with pytest.raises(ValueError, match="Unrecognised column type"):
        _parse_column("Mystery Signal for Cap.1")


# ── _pair_columns ─────────────────────────────────────────────────────────


def _make_raw_df(n_capillaries: int = 1, n_rows: int = 2) -> pd.DataFrame:
    """Build a minimal raw DataFrame mimicking what pd.read_csv returns."""
    data: dict[str, list] = {}
    for cap in range(1, n_capillaries + 1):
        data[f"Temperatur for Cap.{cap} (C)"] = [f"{25.0 + i * 0.1:.1f}" for i in range(n_rows)]
        data[f"Ratio 350 nm / 330 nm for Cap.{cap}"] = [f"{1.2 + i * 0.01:.3f}" for i in range(n_rows)]
        data[f"Temperatur for Cap.{cap} (C)_t2"] = [f"{25.0 + i * 0.1:.1f}" for i in range(n_rows)]
        data[f"Turbidity for Cap.{cap}"] = [f"{0.002 + i * 0.001:.3f}" for i in range(n_rows)]
        data[f"Temperatur for Cap.{cap} (C)_t3"] = [f"{25.0 + i * 0.1:.1f}" for i in range(n_rows)]
        data[f"Cumulant Radius for Cap.{cap} (nm)"] = [f"{4.5 + i * 0.01:.2f}" for i in range(n_rows)]
    return pd.DataFrame(data)


def test_pair_columns_schema() -> None:
    df_raw = _make_raw_df(n_capillaries=1, n_rows=2)
    out = _pair_columns(df_raw)
    assert list(out.columns) == ["capillary", "measurement_type", "temperature", "value"]


def test_pair_columns_row_count() -> None:
    # 1 capillary × 3 measurement types × 2 rows = 6 rows
    df_raw = _make_raw_df(n_capillaries=1, n_rows=2)
    out = _pair_columns(df_raw)
    assert len(out) == 6


def test_pair_columns_measurement_types() -> None:
    df_raw = _make_raw_df(n_capillaries=1, n_rows=2)
    out = _pair_columns(df_raw)
    assert set(out["measurement_type"].unique()) == {"ratio", "turbidity", "cumulant_radius"}


def test_pair_columns_odd_column_count_raises() -> None:
    df_raw = pd.DataFrame({"Temperatur for Cap.1 (C)": ["25.0"]})
    with pytest.raises(ValueError, match="even number of columns"):
        _pair_columns(df_raw)
