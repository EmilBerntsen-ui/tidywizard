"""Unit tests for pipeline steps: drop_columns, impute, dropna_rows, deduplicate."""

import numpy as np
import pandas as pd
import pytest

from core.steps import (
    apply_drop_columns,
    apply_impute,
    apply_dropna_rows,
    apply_deduplicate,
    STEP_REGISTRY,
)


# --- drop_columns ---


def test_drop_columns_basic() -> None:
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
    out = apply_drop_columns(df, {"columns": ["b"]})
    assert list(out.columns) == ["a", "c"]
    pd.testing.assert_frame_equal(out, df[["a", "c"]])


def test_drop_columns_missing_ignored() -> None:
    df = pd.DataFrame({"a": [1], "b": [2]})
    out = apply_drop_columns(df, {"columns": ["b", "x", "y"]})
    assert list(out.columns) == ["a"]


def test_drop_columns_empty_list() -> None:
    df = pd.DataFrame({"a": [1]})
    out = apply_drop_columns(df, {"columns": []})
    pd.testing.assert_frame_equal(out, df)


def test_drop_columns_with_spaces_and_special_chars() -> None:
    df = pd.DataFrame({"col A": [1], "col-b": [2], "col.b": [3]})
    out = apply_drop_columns(df, {"columns": ["col A", "col-b"]})
    assert list(out.columns) == ["col.b"]


def test_drop_columns_invalid_params() -> None:
    df = pd.DataFrame({"a": [1]})
    with pytest.raises(ValueError, match="requires 'columns'"):
        apply_drop_columns(df, {})
    with pytest.raises(ValueError, match="must be a list or tuple"):
        apply_drop_columns(df, {"columns": "a"})


# --- impute ---


def test_impute_numeric_median() -> None:
    df = pd.DataFrame({"x": [1.0, np.nan, 3.0]})
    out = apply_impute(df, {
        "numeric": {"strategy": "median", "fill_value": None},
        "categorical": {"strategy": "mode", "fill_value": None},
    })
    assert out["x"].iloc[1] == 2.0


def test_impute_numeric_mean() -> None:
    df = pd.DataFrame({"x": [1.0, 3.0, np.nan]})
    out = apply_impute(df, {
        "numeric": {"strategy": "mean", "fill_value": None},
        "categorical": {"strategy": "mode", "fill_value": None},
    })
    assert out["x"].iloc[2] == 2.0


def test_impute_numeric_constant() -> None:
    df = pd.DataFrame({"x": [1.0, np.nan, 3.0]})
    out = apply_impute(df, {
        "numeric": {"strategy": "constant", "fill_value": -1},
        "categorical": {"strategy": "mode", "fill_value": None},
    })
    assert out["x"].iloc[1] == -1


def test_impute_categorical_mode() -> None:
    df = pd.DataFrame({"c": ["a", np.nan, "a", "b"]}, dtype=object)
    out = apply_impute(df, {
        "numeric": {"strategy": "median", "fill_value": None},
        "categorical": {"strategy": "mode", "fill_value": None},
    })
    assert out["c"].iloc[1] == "a"


def test_impute_categorical_constant() -> None:
    df = pd.DataFrame({"c": ["a", np.nan, "b"]}, dtype=object)
    out = apply_impute(df, {
        "numeric": {"strategy": "median", "fill_value": None},
        "categorical": {"strategy": "constant", "fill_value": "MISSING"},
    })
    assert out["c"].iloc[1] == "MISSING"


def test_impute_all_null_column() -> None:
    df = pd.DataFrame({"x": [np.nan, np.nan, np.nan]})
    out = apply_impute(df, {
        "numeric": {"strategy": "constant", "fill_value": 0},
        "categorical": {"strategy": "mode", "fill_value": None},
    })
    assert out["x"].tolist() == [0.0, 0.0, 0.0]


def test_impute_mixed_type_object_column() -> None:
    # Column that is object (mixed or string) gets categorical treatment
    df = pd.DataFrame({"o": ["x", np.nan, "y"]}, dtype=object)
    out = apply_impute(df, {
        "numeric": {"strategy": "median", "fill_value": None},
        "categorical": {"strategy": "constant", "fill_value": "?"},
    })
    assert out["o"].iloc[1] == "?"


def test_impute_invalid_params() -> None:
    df = pd.DataFrame({"x": [1.0]})
    with pytest.raises(ValueError, match="requires 'numeric'"):
        apply_impute(df, {"categorical": {"strategy": "mode", "fill_value": None}})


# --- dropna_rows ---


def test_dropna_rows_any() -> None:
    df = pd.DataFrame({"a": [1, np.nan, 3], "b": [4, 5, np.nan]})
    out = apply_dropna_rows(df, {"how": "any"})
    assert len(out) == 1
    assert out.iloc[0]["a"] == 1 and out.iloc[0]["b"] == 4


def test_dropna_rows_all() -> None:
    df = pd.DataFrame({"a": [1, np.nan, 3], "b": [4, np.nan, 6]})
    out = apply_dropna_rows(df, {"how": "all"})
    assert len(out) == 3


def test_dropna_rows_default_how() -> None:
    df = pd.DataFrame({"a": [1, np.nan]})
    out = apply_dropna_rows(df, {})
    assert len(out) == 1


def test_dropna_rows_invalid_how() -> None:
    df = pd.DataFrame({"a": [1]})
    with pytest.raises(ValueError, match="'any' or 'all'"):
        apply_dropna_rows(df, {"how": "invalid"})


# --- deduplicate ---


def test_deduplicate_keep_first() -> None:
    df = pd.DataFrame({"a": [1, 1, 2], "b": [10, 10, 20]})
    out = apply_deduplicate(df, {"keep": "first"})
    assert len(out) == 2
    assert out["a"].tolist() == [1, 2]


def test_deduplicate_keep_last() -> None:
    df = pd.DataFrame({"a": [1, 1, 2], "b": [10, 11, 20]})
    out = apply_deduplicate(df, {"keep": "last"})
    assert len(out) == 2
    assert out["b"].tolist() == [11, 20]


def test_deduplicate_keep_false() -> None:
    df = pd.DataFrame({"a": [1, 1, 2], "b": [10, 10, 20]})
    out = apply_deduplicate(df, {"keep": False})
    assert len(out) == 1
    assert out["a"].iloc[0] == 2


def test_deduplicate_invalid_keep() -> None:
    df = pd.DataFrame({"a": [1]})
    with pytest.raises(ValueError, match="'first', 'last', or False"):
        apply_deduplicate(df, {"keep": "invalid"})


# --- registry ---


def test_step_registry_has_all_steps() -> None:
    assert "drop_columns" in STEP_REGISTRY
    assert "impute" in STEP_REGISTRY
    assert "dropna_rows" in STEP_REGISTRY
    assert "deduplicate" in STEP_REGISTRY
