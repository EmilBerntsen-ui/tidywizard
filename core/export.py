"""Export pipeline as YAML and as standalone executable Python code."""

from __future__ import annotations

from typing import Any

import yaml  # type: ignore[import-untyped]


def _spec_to_python_literal(obj: Any) -> str:
    """Convert pipeline spec to a valid Python literal string (repr for basic types)."""
    if obj is None:
        return "None"
    if isinstance(obj, bool):
        return "True" if obj else "False"
    if isinstance(obj, (int, float)):
        return repr(obj)
    if isinstance(obj, str):
        return repr(obj)
    if isinstance(obj, (list, tuple)):
        parts = ", ".join(_spec_to_python_literal(x) for x in obj)
        return "[" + parts + "]"
    if isinstance(obj, dict):
        parts = ", ".join(f"{repr(k)}: {_spec_to_python_literal(v)}" for k, v in obj.items())
        return "{" + parts + "}"
    return repr(obj)


def pipeline_to_yaml(pipeline_spec: dict[str, Any]) -> str:
    """Serialize pipeline_spec to a YAML string."""
    if not isinstance(pipeline_spec, dict):
        raise ValueError("pipeline_spec must be a dict.")
    # Ensure version if missing
    out = dict(pipeline_spec)
    if "version" not in out:
        out["version"] = 1
    return yaml.dump(out, default_flow_style=False, sort_keys=False, allow_unicode=True)


def _step_to_python(step: dict[str, Any]) -> list[str]:
    """Generate Python code lines for a single pipeline step using only pandas."""
    name = step["name"]
    p = step.get("params", {})

    if name == "drop_columns":
        cols = _spec_to_python_literal(p["columns"])
        return [f"df = df.drop(columns=[c for c in {cols} if c in df.columns])"]

    if name == "melt":
        parts = [f"id_vars={_spec_to_python_literal(p['id_vars'])}"]
        if p.get("value_vars"):
            parts.append(f"value_vars={_spec_to_python_literal(p['value_vars'])}")
        parts.append(f"var_name={_spec_to_python_literal(p.get('var_name', 'variable'))}")
        parts.append(f"value_name={_spec_to_python_literal(p.get('value_name', 'value'))}")
        return [f"df = pd.melt(df, {', '.join(parts)})"]

    if name == "rename_columns":
        mapping = _spec_to_python_literal(p["mapping"])
        return [f"df = df.rename(columns={mapping})"]

    if name == "filter_rows":
        col = repr(p["column"])
        op = p["op"]
        val = _spec_to_python_literal(p["value"])
        if op == "eq":
            return [f"df = df[df[{col}] == {val}].reset_index(drop=True)"]
        if op == "ne":
            return [f"df = df[df[{col}] != {val}].reset_index(drop=True)"]
        if op == "gt":
            return [f"df = df[df[{col}] > {val}].reset_index(drop=True)"]
        if op == "lt":
            return [f"df = df[df[{col}] < {val}].reset_index(drop=True)"]
        if op == "ge":
            return [f"df = df[df[{col}] >= {val}].reset_index(drop=True)"]
        if op == "le":
            return [f"df = df[df[{col}] <= {val}].reset_index(drop=True)"]
        if op == "contains":
            return [f"df = df[df[{col}].astype(str).str.contains({val}, na=False)].reset_index(drop=True)"]
        if op == "not_contains":
            return [f"df = df[~df[{col}].astype(str).str.contains({val}, na=False)].reset_index(drop=True)"]
        if op == "isin":
            return [f"df = df[df[{col}].isin({val})].reset_index(drop=True)"]
        if op == "notin":
            return [f"df = df[~df[{col}].isin({val})].reset_index(drop=True)"]
        return [f"# filter_rows: unsupported op {op!r}"]

    if name == "replace_values":
        mapping = _spec_to_python_literal(p["mapping"])
        cols = p.get("columns")
        if cols:
            lines = []
            for col in cols:
                lines.append(f"df[{repr(col)}] = df[{repr(col)}].replace({mapping})")
            return lines
        return [f"df = df.replace({mapping})"]

    if name == "strip_whitespace":
        lines = []
        if p.get("strip_headers", True):
            lines.append("df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]")
        cols = p.get("columns")
        if cols is None or cols:
            if cols:
                for col in cols:
                    lines.append(
                        f"if df[{repr(col)}].dtype == object: df[{repr(col)}] = df[{repr(col)}].str.strip()"
                    )
            else:
                lines.append(
                    "for _c in df.select_dtypes(include='object').columns: df[_c] = df[_c].str.strip()"
                )
        return lines

    if name == "impute":
        lines = []
        num_cfg = p.get("numeric", {})
        cat_cfg = p.get("categorical", {})
        n_strat = num_cfg.get("strategy") or "median"
        c_strat = cat_cfg.get("strategy") or "mode"

        lines.append("for _c in df.columns:")
        lines.append("    _s = df[_c]")
        lines.append("    if _s.isna().sum() == 0: continue")
        lines.append("    if pd.api.types.is_numeric_dtype(_s) and _s.dtype.kind in 'iufc':")
        if n_strat == "mean":
            lines.append("        df[_c] = _s.fillna(_s.mean())")
        elif n_strat == "constant":
            fv = _spec_to_python_literal(num_cfg.get("fill_value"))
            lines.append(f"        df[_c] = _s.fillna({fv})")
        else:  # median
            lines.append("        df[_c] = _s.fillna(_s.median())")
        lines.append("    else:")
        if c_strat == "constant":
            fv = _spec_to_python_literal(cat_cfg.get("fill_value"))
            lines.append(f"        df[_c] = _s.where(_s.notna(), {fv})")
        else:  # mode
            lines.append("        _m = _s.mode()")
            lines.append("        _fill = _m.iloc[0] if len(_m) > 0 else 'unknown'")
            lines.append("        df[_c] = _s.where(_s.notna(), _fill)")
        return lines

    if name == "dropna_rows":
        how = repr(p.get("how", "any"))
        return [f"df = df.dropna(how={how})"]

    if name == "deduplicate":
        keep = _spec_to_python_literal(p.get("keep", "first"))
        return [f"df = df.drop_duplicates(keep={keep})"]

    return [f"# Unknown step: {name}"]


def pipeline_to_python(
    pipeline_spec: dict[str, Any],
    *,
    input_path: str = "input.csv",
    output_path: str = "output.csv",
) -> str:
    """
    Generate standalone Python code that loads a CSV, applies the pipeline, and saves.

    The generated script uses only pandas — no TidyWizard imports required.
    """
    if not isinstance(pipeline_spec, dict):
        raise ValueError("pipeline_spec must be a dict.")

    step_lines: list[str] = []
    for i, step in enumerate(pipeline_spec.get("steps", [])):
        step_lines.append(f"    # Step {i + 1}: {step['name']}")
        for line in _step_to_python(step):
            step_lines.append(f"    {line}")
        step_lines.append("")

    body = "\n".join(step_lines)

    spec_literal = _spec_to_python_literal(pipeline_spec)

    return f'''"""
Reproducible pipeline generated by TidyWizard.
Run: python pipeline.py
"""

import pandas as pd

INPUT_PATH = "{input_path}"
OUTPUT_PATH = "{output_path}"
PIPELINE_SPEC = {spec_literal}


def main() -> None:
    df = pd.read_csv(INPUT_PATH, dtype_backend="numpy_nullable")
    print(f"Loaded {{len(df)}} rows x {{len(df.columns)}} columns")

{body}\
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved {{len(df)}} rows to {{OUTPUT_PATH}}")


if __name__ == "__main__":
    main()
'''
