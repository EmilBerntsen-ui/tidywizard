# Design: Prometheus Panta Parser + Development Agents

**Date:** 2026-02-26
**Project:** TidyWizard
**Context:** Antibody development company; first instrument to support is the NanoTemper Prometheus Panta (nanoDSF).

---

## Problem

The Prometheus Panta outputs two files per experiment:

1. **Melting scan CSV** — semicolon-separated, Windows cp1252 encoded. Contains one column per capillary per measurement type, giving ~96 columns total. Column names encode both capillary number and measurement type (e.g. `Ratio 350 nm / 330 nm for Cap.1`). The temperature axis is repeated as a separate column for each measurement type per capillary.

2. **Data table TSV** — tab-separated metadata file with per-capillary sample information (`Exclude`, `Merge Set`, `Capillary`, `Data file`, etc.).

Neither file is in tidy format. The raw file requires header parsing and reshaping before it can be analysed.

---

## Desired Output

One row per `(capillary, measurement_type, temperature_point)`:

| capillary | measurement_type | temperature | value |
|-----------|-----------------|-------------|-------|
| 1 | ratio | 25.0 | 1.234 |
| 1 | turbidity | 25.0 | 0.002 |
| 1 | cumulant_radius | 25.0 | 4.56 |
| 2 | ratio | 25.0 | 1.198 |
| ... | ... | ... | ... |

The metadata file (data table TSV) is out of scope for this iteration and will be handled separately.

---

## Architecture

### File structure

```
core/
  instruments/
    __init__.py
    prometheus_panta.py      ← new: public load_melting_scan()

app/pages/
  5_Instruments.py           ← new: alternative entry point for instrument files

tests/
  instruments/
    __init__.py
    test_prometheus_panta.py ← new: golden tests with synthetic data
```

**Nothing in the existing codebase changes.** `5_Instruments.py` is a parallel entry point that stores parsed output in `st.session_state["df_raw"]` — the same key as `1_Upload.py` — so users continue through the existing Profile → Clean → Export flow unchanged.

---

## Prometheus Panta Parser (`core/instruments/prometheus_panta.py`)

### Public API

```python
def load_melting_scan(source: str | bytes | io.BytesIO) -> pd.DataFrame:
    """
    Load a Prometheus Panta melting scan CSV into tidy long format.

    Returns DataFrame with columns:
        capillary (int), measurement_type (str), temperature (float), value (float)

    Raises ValueError for empty files, unrecognised column names,
    or mismatched capillary numbers within a column pair.
    """
```

### Four internal steps

**1. Load raw file**
- Encoding: `cp1252` (handles Windows degree symbol that appears as `Å*C` under UTF-8)
- Separator: `;`
- All columns read as `str` initially — no type coercion at load time

**2. Parse headers (keyword-based, permissive)**

Column names are classified by keyword matching, not literal regex, to handle firmware version variations:

| Keywords (case-insensitive) | Classified as |
|-----------------------------|---------------|
| contains `"Temp"` | `temperature` |
| contains `"Ratio"` | `ratio` |
| contains `"Turbid"` or `"Scatter"` | `turbidity` |
| contains `"Radius"` or `"Rh"` | `cumulant_radius` |

Capillary number extracted with permissive pattern: `r'[Cc]ap(?:illary)?\.?\s*(\d+)'` — matches `Cap.1`, `Cap. 1`, `Cap 1`, `Capillary 1`.

Any column that matches none of the above keywords raises `ValueError` with the offending column name printed — new firmware variants surface as explicit errors, not silent data corruption.

**3. Pair temperature + measurement columns**

Columns arrive in positional pairs: `(Temp, Measurement)` repeating. For capillary 1 this is:
- `(Temp_ratio, Ratio)`, `(Temp_turbidity, Turbidity)`, `(Temp_radius, CumulantRadius)`

Each pair is converted to a mini DataFrame with schema `(capillary, measurement_type, temperature, value)`. Mismatched capillary numbers between the two columns in a pair raise `ValueError`.

**4. Concatenate**

`pd.concat` of all mini DataFrames. Cast `temperature` and `value` to `float`, `capillary` to `int`.

---

## Streamlit Integration (`app/pages/5_Instruments.py`)

Three-panel layout:

1. **Instrument dropdown** — starts with `"Prometheus Panta (nanoDSF)"`. Adding new instruments later requires only extending this dropdown.

2. **File uploaders** — two uploaders for Prometheus Panta: melting scan CSV (required) and data table TSV (optional, out of scope this iteration).

3. **Parse + preview** — on clicking "Parse": calls `load_melting_scan()`, stores result in `st.session_state["df_raw"]`, shows row count and 20-row preview. `ValueError` messages are shown via `st.error()`. On success, user is directed to continue via the sidebar.

---

## Development Agents

### Agent 1: `panta-parser-dev`

**Purpose:** Accelerate development of `core/instruments/prometheus_panta.py`.

**Knows:**
- Prometheus Panta column naming conventions and known variations
- cp1252 encoding behaviour
- Pair-wise column structure
- Expected tidy output schema: `(capillary, measurement_type, temperature, value)`

**Tasks:**
- Generate synthetic Prometheus Panta CSV content for tests (realistic column names, semicolon-separated, cp1252-safe)
- Validate parser output: correct dtypes, no duplicate `(capillary, measurement_type, temperature)` triplets, plausible value ranges
- Debug keyword patterns when new column name variants are discovered

---

### Agent 2: `step-scaffolder`

**Purpose:** Add new pipeline steps to `core/steps.py` following existing conventions.

**Knows:**
- The `_validate_X_params` + `apply_X` + `STEP_REGISTRY` pattern
- `test_steps.py` test conventions
- `core/__init__.py` export pattern

**Tasks:**
- Read existing steps before writing
- Scaffold: validation function, apply function, registry entry
- Generate matching unit tests
- Update `core/__init__.py` exports

---

### Agent 3: `data-shape-verifier`

**Purpose:** Verify that any transformation produces correct, scientifically sensible tidy output.

**Knows:**
- Tidy data principles
- Prometheus Panta value norms: temperature 20–95°C, ratio 0.5–2.5, turbidity near zero at low temperatures

**Tasks:**
- Check output schema: column names, dtypes, nulls
- Verify no duplicate index combinations
- Flag scientifically implausible values
- Write `assert` statements for golden tests

---

## Key Decisions

| Decision | Rationale |
|----------|-----------|
| Keyword-based header classification | Firmware versions vary column names; strict regex breaks silently |
| cp1252 encoding | Handles Windows degree symbol misread as `Å*C` under UTF-8 |
| Parallel entry point (`5_Instruments.py`) | Keeps existing generic flow intact; no risk to current users |
| Metadata file out of scope (iteration 1) | User confirmed raw file is the priority; merge can be added later |
| Three specialised agents | Each maps to one hard problem; extensible to future instruments |
