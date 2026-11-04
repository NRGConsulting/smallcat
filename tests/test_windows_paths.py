"""SC-022: Windows paths give valid URIs and the constructor has no side effects."""

# ruff: noqa: S108

from pathlib import Path

import pandas as pd
import pytest

from smallcat.datasets.csv_dataset import CSVDataset
from smallcat.path_utils import (
    local_path_from_uri,
    norm_join_uri,
    normalize_file_uri,
    to_file_uri,
)


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/var/data", "file:///var/data"),
        (r"C:\Users\me\data", "file:///C:/Users/me/data"),
        ("C:/Users/me/data", "file:///C:/Users/me/data"),
    ],
)
def test_to_file_uri(path: str, expected: str) -> None:
    assert to_file_uri(path) == expected


def test_normalize_file_uri_fixes_drive_letter_uris() -> None:
    assert normalize_file_uri(r"file://C:\d\x") == "file:///C:/d/x"
    assert normalize_file_uri("file:///C:/d") == "file:///C:/d"
    assert normalize_file_uri("file:///tmp/d") == "file:///tmp/d"
    assert normalize_file_uri("gs://b/p") == "gs://b/p"


def test_norm_join_uri_does_not_mix_separators() -> None:
    assert norm_join_uri(r"C:\data\x", "a/b.csv") == "C:/data/x/a/b.csv"
    assert norm_join_uri("file:///C:/data", "a.csv") == "file:///C:/data/a.csv"


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("file:///C:/d/a.csv", "C:/d/a.csv"),
        ("file:///tmp/a.csv", "/tmp/a.csv"),
        ("data/a.csv", "data/a.csv"),
        ("gs://b/a.csv", None),
        ("s3://b/a.csv", None),
    ],
)
def test_local_path_from_uri(uri: str, expected: str | None) -> None:
    assert local_path_from_uri(uri) == expected


def test_constructor_does_not_create_directories(tmp_path: Path) -> None:
    base = tmp_path / "does" / "not" / "exist"
    CSVDataset(conn={"conn_type": "fs", "extra": {"base_path": str(base)}})
    assert not base.exists()


def test_save_creates_nested_directories(tmp_path: Path) -> None:
    base = tmp_path / "a" / "b"
    ds = CSVDataset(conn={"conn_type": "fs", "extra": {"base_path": str(base)}})
    ds.save_pandas("c/out.csv", pd.DataFrame({"x": [1, 2]}))
    assert (base / "c" / "out.csv").exists()
    assert ds.load_pandas("c/out.csv")["x"].tolist() == [1, 2]
