"""Tests for pipeline export: YAML and pipeline.py."""

from typing import Any

from core.export import pipeline_to_yaml, pipeline_to_python


def test_pipeline_to_yaml_roundtrip() -> None:
    import yaml  # type: ignore[import-untyped]

    spec = {"version": 1, "steps": [{"name": "drop_columns", "params": {"columns": ["a"]}}]}
    s = pipeline_to_yaml(spec)
    loaded = yaml.safe_load(s)
    assert loaded["version"] == 1
    assert loaded["steps"][0]["name"] == "drop_columns"
    assert loaded["steps"][0]["params"]["columns"] == ["a"]


def test_pipeline_to_yaml_adds_version() -> None:
    spec: dict[str, Any] = {"steps": []}
    s = pipeline_to_yaml(spec)
    assert "version" in s


def test_pipeline_to_python_compiles() -> None:
    spec = {"version": 1, "steps": [{"name": "deduplicate", "params": {"keep": "first"}}]}
    code = pipeline_to_python(spec)
    compile(code, "pipeline.py", "exec")


def test_pipeline_to_python_spec_embedded_matches() -> None:
    spec = {"version": 1, "steps": [{"name": "dropna_rows", "params": {"how": "any"}}]}
    code = pipeline_to_python(spec, input_path="in.csv", output_path="out.csv")
    # Exec in a namespace; set __name__ so "if __name__ == '__main__'" does not run main().
    ns: dict = {"__name__": "__test__"}
    exec(compile(code, "pipeline.py", "exec"), ns)
    assert "PIPELINE_SPEC" in ns
    assert ns["PIPELINE_SPEC"] == spec
    assert ns["INPUT_PATH"] == "in.csv"
    assert ns["OUTPUT_PATH"] == "out.csv"
