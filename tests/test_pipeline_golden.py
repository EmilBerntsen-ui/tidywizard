"""Golden test: sample input -> pipeline -> expected output schema and key values."""

import pandas as pd
import pytest

from core.pipeline import apply_pipeline


# Deterministic sample input
def _sample_df() -> pd.DataFrame:
    return pd.DataFrame({
        "name": ["Alice", "Bob", "Alice", "Carol"],
        "age": [30, pd.NA, 30, 25],
        "city": ["Oslo", "Berlin", "Oslo", "Paris"],
        "value": [100, 200, 100, 300],
    })


def _golden_pipeline() -> dict:
    return {
        "version": 1,
        "steps": [
            {"name": "drop_columns", "params": {"columns": ["value"]}},
            {"name": "impute", "params": {
                "numeric": {"strategy": "median", "fill_value": None},
                "categorical": {"strategy": "mode", "fill_value": None},
            }},
            {"name": "dropna_rows", "params": {"how": "any"}},
            {"name": "deduplicate", "params": {"keep": "first"}},
        ],
    }


def test_golden_schema() -> None:
    df = _sample_df()
    spec = _golden_pipeline()
    out = apply_pipeline(df, spec)
    # After drop value: name, age, city
    assert list(out.columns) == ["name", "age", "city"]
    # After impute: age 30, (30+25)/2=27.5 for Bob, 30, 25. No more NA in age.
    # After dropna: all rows have no NA (impute filled age).
    # After dedup: (Alice,30,Oslo) and (Alice,30,Oslo) -> one; (Bob,27.5,Berlin); (Carol,25,Paris)
    assert len(out) == 3


def test_golden_values() -> None:
    df = _sample_df()
    spec = _golden_pipeline()
    out = apply_pipeline(df, spec)
    names = out["name"].tolist()
    ages = out["age"].tolist()
    cities = out["city"].tolist()
    assert "Alice" in names
    assert "Bob" in names
    assert "Carol" in names
    # Bob's age was imputed to median of [30, 25] = 27.5 (Bob was NA, Carol 25, so median of non-na is 27.5)
    # Actually: age had [30, NA, 30, 25]. Median of [30,30,25] = 30. So Bob gets 30.
    idx_bob = names.index("Bob")
    assert ages[idx_bob] == 30
    assert cities[idx_bob] == "Berlin"


def test_golden_deterministic() -> None:
    df = _sample_df()
    spec = _golden_pipeline()
    a = apply_pipeline(df, spec)
    b = apply_pipeline(df, spec)
    pd.testing.assert_frame_equal(a, b)


def test_pipeline_empty_steps() -> None:
    df = pd.DataFrame({"a": [1, 2]})
    out = apply_pipeline(df, {"version": 1, "steps": []})
    pd.testing.assert_frame_equal(out, df)


def test_pipeline_invalid_spec() -> None:
    df = pd.DataFrame({"a": [1]})
    with pytest.raises(ValueError, match="'steps'"):
        apply_pipeline(df, {"version": 1})
    with pytest.raises(ValueError, match="must be a list"):
        apply_pipeline(df, {"version": 1, "steps": "x"})
    with pytest.raises(ValueError, match="Unknown step"):
        apply_pipeline(df, {"version": 1, "steps": [{"name": "no_such_step", "params": {}}]})
