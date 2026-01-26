"""Tests for core.io: load_csv, load_excel."""

import io
from pathlib import Path

import pytest

from core.io import load_csv, load_excel


def test_load_csv_from_string_path(tmp_path: Path) -> None:
    p = tmp_path / "a.csv"
    p.write_text("a,b\n1,2\n3,4", encoding="utf-8")
    df = load_csv(str(p))
    assert list(df.columns) == ["a", "b"]
    assert len(df) == 2


def test_load_csv_from_bytes() -> None:
    raw = b"x,y\n1,2"
    df = load_csv(raw)
    assert list(df.columns) == ["x", "y"]
    assert df["x"].iloc[0] == 1


def test_load_csv_from_bytesio() -> None:
    bio = io.BytesIO(b"p,q\n10,20")
    df = load_csv(bio)
    assert list(df.columns) == ["p", "q"]


def test_load_csv_empty_fails() -> None:
    with pytest.raises(ValueError, match="empty"):
        load_csv(b"")
    with pytest.raises(ValueError, match="empty"):
        load_csv(io.BytesIO(b""))


def test_load_csv_none_fails() -> None:
    with pytest.raises(ValueError, match="empty"):
        load_csv(None)  # type: ignore[arg-type]


def test_load_csv_column_with_spaces() -> None:
    raw = b"col A,col-B\n1,2"
    df = load_csv(raw)
    assert "col A" in df.columns
    assert "col-B" in df.columns


def test_load_csv_sample_data() -> None:
    p = Path(__file__).resolve().parents[1] / "sample_data" / "messy_people.csv"
    df = load_csv(str(p))
    assert "name" in df.columns
    assert "first name" in df.columns
    assert len(df) >= 1

