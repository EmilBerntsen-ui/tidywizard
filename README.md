# TidyWizard

A local hobby tool to clean your data and export a **reproducible pipeline** as YAML and Python.

- **Upload** CSV or Excel (.xlsx)
- **Profile** dtypes, missing %, unique counts, top values, duplicates
- **Clean** interactively: drop columns, impute (numeric: mean/median/constant; categorical: mode/constant), drop NA rows, deduplicate
- **Export** cleaned CSV, `pipeline.yaml`, and `pipeline.py` that reapplies the pipeline

## Install

```bash
python -m venv .venv
source .venv/bin/activate   # or: .venv\Scripts\activate on Windows
pip install -e .
```

## Run

```bash
make run
```

This runs `PYTHONPATH=. streamlit run app/Home.py`. Open the URL (usually http://localhost:8501) and go through **Upload → Profile → Clean → Export**. You can use `sample_data/messy_people.csv` to try it.

## QA

```bash
make qa
```

Runs:

- `ruff check .`
- `mypy .`
- `pytest -q`

## Pipeline spec (YAML)

Example `pipeline.yaml`:

```yaml
version: 1
steps:
  - name: drop_columns
    params: { columns: ["colA", "colB"] }
  - name: impute
    params:
      numeric: { strategy: "median", fill_value: null }
      categorical: { strategy: "mode", fill_value: null }
  - name: dropna_rows
    params: { how: "any" }
  - name: deduplicate
    params: { keep: "first" }
```

## Export

- **Cleaned CSV** — result of applying the pipeline to the uploaded data.
- **pipeline.yaml** — the pipeline spec above.
- **pipeline.py** — Python script that:
  - defines `PIPELINE_SPEC` (same as the YAML),
  - loads a CSV from `INPUT_PATH`,
  - runs `apply_pipeline` from `core.pipeline`,
  - saves to `OUTPUT_PATH`.

Run from the project root (so `core` is importable, or install with `pip install -e .`):

```bash
# Put your raw CSV as input.csv, then:
python pipeline.py
```

Edit `INPUT_PATH` and `OUTPUT_PATH` in `pipeline.py` if needed.

## Reproducibility

The Export page shows versions of `pandas`, `streamlit`, and `pyyaml` used in the app. The generated `pipeline.py` depends on `pandas`, `pyyaml`, and the local `core` package.
