# Prometheus Panta Parser + Development Agents Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a Prometheus Panta nanoDSF melting scan parser that produces tidy long-format output, wire it into a new Streamlit page, and create three Claude Code development agents to accelerate future work.

**Architecture:** A new `core/instruments/prometheus_panta.py` module exposes `load_melting_scan()` which reads the semicolon-separated cp1252-encoded CSV, classifies columns by keyword matching, pairs temperature+measurement columns, and concatenates into a `(capillary, measurement_type, temperature, value)` long-format DataFrame. A new `5_Instruments.py` Streamlit page provides the UI entry point and stores results in `st.session_state["df_raw"]` so the existing Profile → Clean → Export flow works unchanged.

**Tech Stack:** Python 3.10+, pandas 2.2+, pytest, Streamlit 1.36+, re (stdlib)

---

## Task 1: Create module scaffolding

**Files:**
- Create: `core/instruments/__init__.py`
- Create: `tests/instruments/__init__.py`

**Step 1: Create the directories and empty init files**

```bash
mkdir -p core/instruments tests/instruments
touch core/instruments/__init__.py tests/instruments/__init__.py
```

**Step 2: Verify structure**

```bash
ls core/instruments/ tests/instruments/
```
Expected: both directories contain `__init__.py`.

**Step 3: Commit**

```bash
git add core/instruments/__init__.py tests/instruments/__init__.py
git commit -m "feat: scaffold core/instruments and tests/instruments modules"
```

---

## Task 2: TDD — `_classify_measurement_type`

**Files:**
- Create: `core/instruments/prometheus_panta.py`
- Create: `tests/instruments/test_prometheus_panta.py`

**Step 1: Create the parser file with only what the test needs**

Create `core/instruments/prometheus_panta.py`:

```python
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
```

**Step 2: Write the failing tests**

Create `tests/instruments/test_prometheus_panta.py`:

```python
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
```

**Step 3: Run tests — expect the classify tests to pass, others to fail on import**

```bash
.venv/bin/python -m pytest tests/instruments/test_prometheus_panta.py::test_classify_temp tests/instruments/test_prometheus_panta.py::test_classify_ratio tests/instruments/test_prometheus_panta.py::test_classify_turbidity_turbid tests/instruments/test_prometheus_panta.py::test_classify_turbidity_scatter tests/instruments/test_prometheus_panta.py::test_classify_cumulant_radius tests/instruments/test_prometheus_panta.py::test_classify_unknown_returns_none -v
```
Expected: 6 PASSED.

**Step 4: Commit**

```bash
git add core/instruments/prometheus_panta.py tests/instruments/test_prometheus_panta.py
git commit -m "feat: add _classify_measurement_type with tests"
```

---

## Task 3: TDD — `_parse_column`

**Files:**
- Modify: `core/instruments/prometheus_panta.py`
- Modify: `tests/instruments/test_prometheus_panta.py`

**Step 1: Add failing tests to `test_prometheus_panta.py`**

Append after the existing `_classify_measurement_type` tests:

```python
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
```

**Step 2: Run — expect ImportError or NameError for `_parse_column`**

```bash
.venv/bin/python -m pytest tests/instruments/test_prometheus_panta.py::test_parse_column_temperature -v
```
Expected: ERROR — `cannot import name '_parse_column'`.

**Step 3: Implement `_parse_column` in `prometheus_panta.py`**

Add after `_classify_measurement_type`:

```python
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
```

**Step 4: Run — expect all `_parse_column` tests to pass**

```bash
.venv/bin/python -m pytest tests/instruments/test_prometheus_panta.py -k "parse_column" -v
```
Expected: 7 PASSED.

**Step 5: Commit**

```bash
git add core/instruments/prometheus_panta.py tests/instruments/test_prometheus_panta.py
git commit -m "feat: add _parse_column with column variant and error tests"
```

---

## Task 4: TDD — `_pair_columns`

**Files:**
- Modify: `core/instruments/prometheus_panta.py`
- Modify: `tests/instruments/test_prometheus_panta.py`

**Step 1: Add failing tests**

Append to the test file:

```python
# ── _pair_columns ─────────────────────────────────────────────────────────


def _make_raw_df(n_capillaries: int = 1, n_rows: int = 2) -> pd.DataFrame:
    """Build a minimal raw DataFrame mimicking what pd.read_csv returns."""
    cols = {}
    for cap in range(1, n_capillaries + 1):
        for row_i in range(n_rows):
            pass  # built below
    data: dict[str, list] = {}
    for cap in range(1, n_capillaries + 1):
        data[f"Temperatur for Cap.{cap} (C)"] = [f"{25.0 + i * 0.1:.1f}" for i in range(n_rows)]
        data[f"Ratio 350 nm / 330 nm for Cap.{cap}"] = [f"{1.2 + i * 0.01:.3f}" for i in range(n_rows)]
        data[f"Temperatur for Cap.{cap} (C)_2"] = [f"{25.0 + i * 0.1:.1f}" for i in range(n_rows)]
        data[f"Turbidity for Cap.{cap}"] = [f"{0.002 + i * 0.001:.3f}" for i in range(n_rows)]
        data[f"Temperatur for Cap.{cap} (C)_3"] = [f"{25.0 + i * 0.1:.1f}" for i in range(n_rows)]
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
```

**Step 2: Run — expect import error for `_pair_columns`**

```bash
.venv/bin/python -m pytest tests/instruments/test_prometheus_panta.py -k "pair_columns" -v
```
Expected: ERROR — `cannot import name '_pair_columns'`.

**Step 3: Implement `_pair_columns` in `prometheus_panta.py`**

Add after `_parse_column`:

```python
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
```

**Step 4: Run — expect all `_pair_columns` tests to pass**

```bash
.venv/bin/python -m pytest tests/instruments/test_prometheus_panta.py -k "pair_columns" -v
```
Expected: 4 PASSED.

**Step 5: Commit**

```bash
git add core/instruments/prometheus_panta.py tests/instruments/test_prometheus_panta.py
git commit -m "feat: add _pair_columns with pairing and error tests"
```

---

## Task 5: TDD — `load_melting_scan` (golden test)

**Files:**
- Modify: `core/instruments/prometheus_panta.py`
- Modify: `tests/instruments/test_prometheus_panta.py`

**Step 1: Add the synthetic data helper and golden tests**

Append to `test_prometheus_panta.py`:

```python
# ── Synthetic CSV builder ─────────────────────────────────────────────────


def _make_csv(n_capillaries: int = 2, n_rows: int = 3) -> bytes:
    """
    Build a minimal synthetic Prometheus Panta melting scan CSV (cp1252 encoded).
    Column names use the degree symbol (\xb0) to test encoding handling.
    """
    header_parts = []
    for cap in range(1, n_capillaries + 1):
        header_parts += [
            f"Temperatur for Cap.{cap} (\xb0C)",
            f"Ratio 350 nm / 330 nm for Cap.{cap}",
            f"Temperatur for Cap.{cap} (\xb0C)",
            f"Turbidity for Cap.{cap}",
            f"Temperatur for Cap.{cap} (\xb0C)",
            f"Cumulant Radius for Cap.{cap} (nm)",
        ]

    rows = [";".join(header_parts)]
    for row_i in range(n_rows):
        temp = 25.0 + row_i * 0.1
        row_parts = []
        for cap in range(1, n_capillaries + 1):
            row_parts += [
                f"{temp:.1f}", f"{1.2 + cap * 0.01 + row_i * 0.001:.3f}",
                f"{temp:.1f}", f"{0.002 + row_i * 0.001:.3f}",
                f"{temp:.1f}", f"{4.5 + cap * 0.1 + row_i * 0.01:.2f}",
            ]
        rows.append(";".join(row_parts))

    return "\n".join(rows).encode("cp1252")


# ── load_melting_scan ─────────────────────────────────────────────────────


def test_load_schema() -> None:
    df = load_melting_scan(_make_csv())
    assert list(df.columns) == ["capillary", "measurement_type", "temperature", "value"]
    assert pd.api.types.is_integer_dtype(df["capillary"])
    assert df["measurement_type"].dtype == object
    assert pd.api.types.is_float_dtype(df["temperature"])
    assert pd.api.types.is_float_dtype(df["value"])


def test_load_row_count() -> None:
    # 2 capillaries × 3 measurement types × 3 rows = 18 rows
    df = load_melting_scan(_make_csv(n_capillaries=2, n_rows=3))
    assert len(df) == 18


def test_load_capillaries_present() -> None:
    df = load_melting_scan(_make_csv(n_capillaries=2))
    assert set(df["capillary"].unique()) == {1, 2}


def test_load_measurement_types_present() -> None:
    df = load_melting_scan(_make_csv())
    assert set(df["measurement_type"].unique()) == {"ratio", "turbidity", "cumulant_radius"}


def test_load_no_duplicate_index() -> None:
    df = load_melting_scan(_make_csv())
    dupes = df.duplicated(subset=["capillary", "measurement_type", "temperature"])
    assert not dupes.any(), "Duplicate (capillary, measurement_type, temperature) rows found"


def test_load_no_nulls_in_output() -> None:
    df = load_melting_scan(_make_csv())
    assert not df.isnull().any().any()


def test_load_deterministic() -> None:
    data = _make_csv()
    a = load_melting_scan(data)
    b = load_melting_scan(data)
    pd.testing.assert_frame_equal(a, b)


def test_load_accepts_bytes() -> None:
    df = load_melting_scan(_make_csv())
    assert len(df) > 0


def test_load_accepts_bytesio() -> None:
    df = load_melting_scan(io.BytesIO(_make_csv()))
    assert len(df) > 0


def test_load_empty_bytes_raises() -> None:
    with pytest.raises(ValueError, match="empty"):
        load_melting_scan(b"")


def test_load_none_raises() -> None:
    with pytest.raises(ValueError, match="empty"):
        load_melting_scan(None)
```

**Step 2: Run — expect import error for `load_melting_scan`**

```bash
.venv/bin/python -m pytest tests/instruments/test_prometheus_panta.py::test_load_schema -v
```
Expected: ERROR — `cannot import name 'load_melting_scan'`.

**Step 3: Implement `load_melting_scan` in `prometheus_panta.py`**

Append to the end of the file:

```python
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
```

**Step 4: Update `core/instruments/__init__.py`**

```python
from core.instruments.prometheus_panta import load_melting_scan

__all__ = ["load_melting_scan"]
```

**Step 5: Run all instrument tests**

```bash
.venv/bin/python -m pytest tests/instruments/ -v
```
Expected: all tests PASSED.

**Step 6: Run full test suite**

```bash
make qa
```
Expected: ruff + mypy + pytest all pass.

**Step 7: Commit**

```bash
git add core/instruments/prometheus_panta.py core/instruments/__init__.py tests/instruments/test_prometheus_panta.py
git commit -m "feat: implement load_melting_scan with golden tests (Prometheus Panta nanoDSF)"
```

---

## Task 6: Streamlit page — `5_Instruments.py`

**Files:**
- Create: `app/pages/5_Instruments.py`

No unit tests for the UI layer — the underlying parser is already tested. Manual smoke-test steps are included.

**Step 1: Create the page**

```python
"""Instrument-specific file loading: alternative entry point to the generic Upload page."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st

from core.instruments.prometheus_panta import load_melting_scan

_INSTRUMENTS = {
    "Prometheus Panta (nanoDSF)": "prometheus_panta",
}

st.title("Instruments")
st.markdown(
    "Load files from a specific laboratory instrument. "
    "After loading, continue to **Profile → Clean → Export** in the sidebar."
)

instrument = st.selectbox("Select instrument", list(_INSTRUMENTS.keys()))

if _INSTRUMENTS[instrument] == "prometheus_panta":
    st.markdown(
        "**Prometheus Panta** — NanoTemper nanoDSF. "
        "Upload the melting scan CSV (semicolon-separated, from NanoTemper software)."
    )

    uploaded_scan = st.file_uploader(
        "Melting scan CSV (required)",
        type=["csv", "txt"],
        key="panta_scan",
        help="The semicolon-separated raw data file exported from the Prometheus Panta software.",
    )
    uploaded_meta = st.file_uploader(
        "Data table TSV (optional — not yet processed)",
        type=["tsv", "txt", "csv"],
        key="panta_meta",
        help="Metadata file. Merge with raw data will be added in a future update.",
    )

    if uploaded_meta is not None:
        st.info("Data table TSV received. Metadata merge is not yet implemented.")

    if st.button("Parse", disabled=uploaded_scan is None):
        try:
            df = load_melting_scan(uploaded_scan)
            st.session_state["df_raw"] = df
            st.session_state["uploaded_name"] = uploaded_scan.name or "panta_melting_scan.csv"
            st.session_state["pipeline_spec"] = {"version": 1, "steps": []}
            n, p = df.shape
            st.success(f"Parsed **{n}** rows \u00d7 **{p}** columns from `{uploaded_scan.name}`.")
            st.dataframe(df.head(20), use_container_width=True)
            st.info("Continue to **Profile** or **Clean** in the sidebar.")
        except ValueError as e:
            st.error(str(e))
        except Exception as e:
            st.error(f"Unexpected error: {e}")

if "df_raw" in st.session_state:
    df = st.session_state["df_raw"]
    with st.expander("Current dataset preview"):
        st.write("Shape:", df.shape)
        st.dataframe(df.head(10), use_container_width=True)
```

**Step 2: Smoke-test — start the app and verify the page appears**

```bash
make run
```

Open http://localhost:8501 in a browser. Verify:
- "Instruments" appears in the sidebar
- Selecting "Prometheus Panta" shows two file uploaders
- Parse button is disabled when no file is uploaded
- A valid Prometheus Panta CSV produces a preview table with columns `capillary`, `measurement_type`, `temperature`, `value`
- An invalid file shows a readable error message (not a stack trace)

**Step 3: Commit**

```bash
git add app/pages/5_Instruments.py
git commit -m "feat: add Instruments page with Prometheus Panta entry point"
```

---

## Task 7: Create `panta-parser-dev` agent

**Files:**
- Create: `.claude/agents/panta-parser-dev.md`

**Step 1: Create the agents directory**

```bash
mkdir -p .claude/agents
```

**Step 2: Create the agent file**

```bash
cat > .claude/agents/panta-parser-dev.md << 'EOF'
---
name: panta-parser-dev
description: Use when developing, debugging, or testing the Prometheus Panta melting scan parser in core/instruments/prometheus_panta.py. Generates synthetic test data, validates output schema, debugs column name keyword patterns, and checks cp1252 encoding handling.
tools: [Read, Write, Edit, Bash, Glob, Grep]
---

You are a specialist in the NanoTemper Prometheus Panta nanoDSF instrument file format and the TidyWizard parser for it.

## What you know

**Prometheus Panta melting scan CSV:**
- Encoding: cp1252 (Windows). The degree symbol ° is byte 0xB0 in cp1252 — this appears as `Å*C` if the file is mistakenly read as UTF-8.
- Separator: semicolon (`;`)
- Column structure: alternating pairs of (temperature_column, measurement_column) per capillary. Per capillary: (Temp, Ratio), (Temp, Turbidity), (Temp, CumulantRadius)
- Capillary number regex: `[Cc]ap(?:illary)?\.?\s*(\d+)` — handles Cap.1, Cap 1, Capillary 1
- Measurement type classification by keyword (case-insensitive): "Temp"→temperature, "Ratio"→ratio, "Turbid"/"Scatter"→turbidity, "Radius"/"Rh"→cumulant_radius

**Expected tidy output schema:**
- capillary (int): 1-indexed capillary number
- measurement_type (str): one of "ratio", "turbidity", "cumulant_radius"
- temperature (float): temperature in °C
- value (float): measurement value

**Scientific value ranges (nanoDSF):**
- temperature: 20–95°C typical scan range
- ratio (350/330 nm): 0.5–2.5 for most proteins; ~1.0 at native state, rises at Tm
- turbidity: near 0.0 at low temperatures, rises sharply at aggregation onset (Tagg)
- cumulant_radius: 1–20 nm for monomeric antibodies; spikes at aggregation

## Your workflow

1. Always read `core/instruments/prometheus_panta.py` and `tests/instruments/test_prometheus_panta.py` before suggesting changes.
2. When generating synthetic CSV for testing: use cp1252-encoded bytes, semicolon separators, realistic column name patterns, and physically plausible values.
3. When debugging a column name variant: show the keyword match step-by-step and suggest regex adjustments if needed.
4. When validating output: check schema, duplicate index triplets, null counts, and value ranges.
EOF
```

**Step 3: Verify the file was created**

```bash
cat .claude/agents/panta-parser-dev.md
```

**Step 4: Commit**

```bash
git add .claude/agents/panta-parser-dev.md
git commit -m "feat: add panta-parser-dev Claude Code agent"
```

---

## Task 8: Create `step-scaffolder` agent

**Files:**
- Create: `.claude/agents/step-scaffolder.md`

**Step 1: Create the agent file**

```bash
cat > .claude/agents/step-scaffolder.md << 'EOF'
---
name: step-scaffolder
description: Use when adding a new pipeline step to core/steps.py. Reads existing step conventions, scaffolds the full implementation (validation function, apply function, registry entry), generates matching unit tests following test_steps.py patterns, and updates core/__init__.py exports.
tools: [Read, Write, Edit, Glob, Grep]
---

You are a specialist in the TidyWizard pipeline step conventions.

## The pattern you must follow

Every step in `core/steps.py` has exactly three parts:

**1. Validation function** (`_validate_X_params`):
- Takes `params: dict[str, Any]`
- Raises `ValueError` with clear messages for every invalid input
- Checks: params is a dict, required keys exist, values are correct types/values

**2. Apply function** (`apply_X`):
- Signature: `apply_X(df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame`
- Calls `_validate_X_params(params)` first
- Returns a modified copy of df (never mutates in place)
- Silently ignores missing columns (e.g. drop_columns skips columns not in df)

**3. Registry entry** in `STEP_REGISTRY` dict at the bottom of the file.

## Your workflow

1. Read `core/steps.py` (especially the last registered step) to confirm current conventions.
2. Read `tests/test_steps.py` to understand the test pattern.
3. Scaffold the new step with all three parts.
4. Generate unit tests covering: happy path, edge cases (empty list, missing column), invalid params (missing required key, wrong type, invalid value).
5. Update `core/__init__.py` if the new step's apply function should be exported.

## Constraints

- DRY: do not duplicate validation logic already in another step
- YAGNI: only add params the user actually needs, no optional extras
- Keep error messages human-readable — they are shown directly in the Streamlit UI
EOF
```

**Step 2: Commit**

```bash
git add .claude/agents/step-scaffolder.md
git commit -m "feat: add step-scaffolder Claude Code agent"
```

---

## Task 9: Create `data-shape-verifier` agent

**Files:**
- Create: `.claude/agents/data-shape-verifier.md`

**Step 1: Create the agent file**

```bash
cat > .claude/agents/data-shape-verifier.md << 'EOF'
---
name: data-shape-verifier
description: Use after implementing any data transformation to verify the output is correctly shaped, scientifically plausible, and ready for golden tests. Checks schema, duplicate index combinations, null counts, value ranges, and writes assert statements for pytest.
tools: [Read, Bash, Glob]
---

You are a specialist in tidy data validation for biotechnology instrument output.

## What you check

**Schema checks:**
- Column names match expected schema exactly (case-sensitive)
- dtypes are correct: int for identifiers, float for measurements, str/object for categories
- No unexpected extra columns

**Index integrity:**
- No duplicate rows on the natural key (e.g. `capillary + measurement_type + temperature` for Prometheus Panta)
- Run: `df.duplicated(subset=[...]).sum()` — must be 0

**Null checks:**
- Count nulls per column: `df.isnull().sum()`
- Flag any nulls in columns that should never be null (identifiers, temperature)

**Value range checks for Prometheus Panta (nanoDSF):**
- temperature: 20–95°C
- ratio (350/330 nm): 0.5–2.5
- turbidity: 0.0–1.0 at low temperatures
- cumulant_radius: 1–50 nm

## Your output

For each check, write a ready-to-paste `assert` statement for pytest:

```python
assert list(df.columns) == ["capillary", "measurement_type", "temperature", "value"]
assert pd.api.types.is_integer_dtype(df["capillary"])
assert not df.duplicated(subset=["capillary", "measurement_type", "temperature"]).any()
assert df["temperature"].between(20, 95).all()
assert df["value"][df["measurement_type"] == "ratio"].between(0.5, 2.5).all()
```

Always explain WHY each assertion matters scientifically, not just what it checks.
EOF
```

**Step 2: Commit**

```bash
git add .claude/agents/data-shape-verifier.md
git commit -m "feat: add data-shape-verifier Claude Code agent"
```

---

## Task 10: Final verification

**Step 1: Run full QA suite**

```bash
make qa
```
Expected: ruff + mypy + pytest all pass with no errors.

**Step 2: Verify agent files are in place**

```bash
ls .claude/agents/
```
Expected: `panta-parser-dev.md`, `step-scaffolder.md`, `data-shape-verifier.md`

**Step 3: Verify git log**

```bash
git log --oneline -10
```
Expected: clean commit history, one commit per task.

**Step 4: Commit any outstanding changes**

```bash
git status
```
If clean: done. If not: add and commit remaining files.
