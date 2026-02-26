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
