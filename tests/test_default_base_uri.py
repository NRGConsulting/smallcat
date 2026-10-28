import pandas as pd
import pytest

from smallcat.datasets.csv_dataset import CSVDataset
from smallcat.path_utils import norm_join_uri


def test_fs_without_extras_has_no_base_uri():
    ds = CSVDataset(conn={"conn_type": "fs"})
    assert ds.base_uri is None


@pytest.mark.parametrize(
    ("rel", "expected"),
    [("data/a.csv", "data/a.csv"), ("/data/a.csv", "data/a.csv")],
)
def test_fs_without_extras_resolves_relative_to_cwd(rel, expected):
    ds = CSVDataset(conn={"conn_type": "fs"})
    assert ds._full_uri(rel) == expected


def test_fs_without_extras_writes_in_current_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    ds = CSVDataset(conn={"conn_type": "fs"})
    ds.save_pandas("out.csv", pd.DataFrame({"a": [1, 2]}))
    assert (tmp_path / "out.csv").exists()


def test_norm_join_uri_with_empty_file_base_is_relative():
    assert norm_join_uri("file://", "data/a.csv") == "data/a.csv"
    assert norm_join_uri("file://", "/data/a.csv") != "file:/data/a.csv"


@pytest.mark.parametrize(
    ("base", "rel", "expected"),
    [
        ("file:///tmp/data/", "/t/p.parquet", "file:///tmp/data/t/p.parquet"),
        ("file:///", "tmp/a.csv", "file:///tmp/a.csv"),
        ("gs://bucket/prefix", "a/b.parquet", "gs://bucket/prefix/a/b.parquet"),
        ("/var/data", "logs/x", "/var/data/logs/x"),
    ],
)
def test_norm_join_uri_unchanged_for_real_bases(base, rel, expected):
    assert norm_join_uri(base, rel) == expected
