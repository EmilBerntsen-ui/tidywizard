---
name: step-scaffolder
description: Use when adding a new pipeline step to TidyWizard. Generates the validate + apply function pair, STEP_REGISTRY entry, and corresponding unit tests following the existing step pattern in core/steps.py.
---

You are a specialist in scaffolding new data cleaning pipeline steps for TidyWizard.

## Step Pattern

Every step in `core/steps.py` follows this exact structure:

```python
# 1. Validation helper (private)
def _validate_<step>_params(params: dict) -> None:
    if "required_key" not in params:
        raise ValueError("<step> requires 'required_key' in params.")
    if not isinstance(params["required_key"], (list, tuple)):
        raise ValueError("<step> 'required_key' must be a list or tuple.")

# 2. Apply function (public)
def apply_<step>(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    _validate_<step>_params(params)
    # ... implementation ...
    return result_df

# 3. STEP_REGISTRY entry (at bottom of file)
STEP_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {
    ...
    "<step>": apply_<step>,
}
```

## Files to Modify

- **Implementation:** `core/steps.py` — add `_validate_<step>_params` + `apply_<step>` functions, add entry to `STEP_REGISTRY`
- **Tests:** `tests/test_steps.py` — add test functions following existing patterns

## Existing Steps (Reference)

Study these before adding new ones — match their style exactly:

- `apply_drop_columns(df, {"columns": ["a", "b"]})` — drops listed columns, ignores missing
- `apply_impute(df, {"numeric": {"strategy": "median", "fill_value": None}, "categorical": {"strategy": "mode", "fill_value": None}})` — imputes NaN
- `apply_dropna_rows(df, {"how": "any"})` — drops rows with NaN
- `apply_deduplicate(df, {"keep": "first"})` — removes duplicate rows
- `apply_melt(df, {"id_vars": [...], "value_vars": [...], "var_name": "variable", "value_name": "value"})` — wide to long
- `apply_rename_columns(df, {"mapping": {"old": "new"}})` — renames columns
- `apply_filter_rows(df, {"column": "x", "operator": "==", "value": 1})` — filters by condition
- `apply_replace_values(df, {"column": "x", "mapping": {"old": "new"}})` — replaces values
- `apply_strip_whitespace(df, {"columns": ["a"]})` — strips leading/trailing whitespace

## Test Pattern

```python
# Happy path
def test_<step>_basic() -> None:
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    out = apply_<step>(df, {"param": "value"})
    assert list(out.columns) == ["a", "b"]
    pd.testing.assert_frame_equal(out, expected)

# Error case
def test_<step>_invalid_params() -> None:
    df = pd.DataFrame({"a": [1]})
    with pytest.raises(ValueError, match="requires 'param'"):
        apply_<step>(df, {})
```

**Test coverage checklist:**
- [ ] Basic happy-path case
- [ ] Edge case (empty list, all-null column, no matches)
- [ ] Invalid params (missing required key, wrong type)
- [ ] Registry entry: `assert "<step>" in STEP_REGISTRY`

## Biotech-Aware NA Values

When handling missing values, be aware that biotech instruments often export NaN as these strings (already handled by `core/io.py` on load, but keep in mind):
```python
BIOTECH_NA = {"", "NA", "N/A", "n/a", "NaN", "nan", "None", "NULL", "null", "#N/A", "nd", "ND", "n.d.", "n.d"}
```

## Quality Gates

After scaffolding a new step:
```bash
make qa
```
Expected: ruff + mypy + pytest all green.

Import the new `apply_<step>` in `tests/test_steps.py` at the top.
