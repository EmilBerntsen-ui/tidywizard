"""Apply a pipeline spec (list of steps) to a DataFrame."""

from __future__ import annotations

from typing import Any

import pandas as pd

from core.steps import STEP_REGISTRY


def apply_pipeline(df: pd.DataFrame, pipeline_spec: dict[str, Any]) -> pd.DataFrame:
    """
    Apply each step in pipeline_spec["steps"] in order.

    Args:
        df: Input DataFrame.
        pipeline_spec: Must have "steps" (list of {name, params}).

    Returns:
        Transformed DataFrame.

    Raises:
        ValueError: If spec is invalid or a step fails.
    """
    if not isinstance(pipeline_spec, dict):
        raise ValueError("pipeline_spec must be a dict.")
    steps = pipeline_spec.get("steps")
    if steps is None:
        raise ValueError("pipeline_spec must contain 'steps'.")
    if not isinstance(steps, list):
        raise ValueError("pipeline_spec 'steps' must be a list.")

    out = df.copy()
    for i, entry in enumerate(steps):
        if not isinstance(entry, dict):
            raise ValueError(f"Step {i} must be a dict, got {type(entry).__name__}.")
        name = entry.get("name")
        if not name or not isinstance(name, str):
            raise ValueError(f"Step {i} must have a string 'name'.")
        params = entry.get("params")
        if params is None:
            params = {}
        if not isinstance(params, dict):
            raise ValueError(f"Step {i} 'params' must be a dict.")
        fn = STEP_REGISTRY.get(name)
        if fn is None:
            raise ValueError(f"Unknown step name: {name}. Known: {list(STEP_REGISTRY.keys())}.")
        out = fn(out, params)
    return out
