import pandas as pd
import pytest

from smallcat.datasets.csv_dataset import CSVDataset
from smallcat.path_utils import norm_join_uri, to_relative_posix_path

ESCAPING = [
    "../x",
    "../../etc/passwd",
    "/../x",
    "a/../../x",
    "a/b/../../../x",
    "..",
    r"a\..\..\x",
]


@pytest.mark.parametrize("base", ["/var/data", "gs://bucket/prefix", "file:///srv/data"])
@pytest.mark.parametrize("rel", ESCAPING)
def test_norm_join_uri_rejects_paths_escaping_the_base(base, rel):
    with pytest.raises(ValueError, match="escapes the base"):
        norm_join_uri(base, rel)


@pytest.mark.parametrize("rel", ESCAPING)
def test_to_relative_posix_path_rejects_escaping_paths(rel):
    with pytest.raises(ValueError, match="escapes the base"):
        to_relative_posix_path(rel)


@pytest.mark.parametrize(
    ("base", "rel", "expected"),
    [
        ("/var/data", "a/../b.csv", "/var/data/b.csv"),
        ("/var/data", "./a//b.csv", "/var/data/a/b.csv"),
        ("/var/data", "a/b/../c.csv", "/var/data/a/c.csv"),
        ("gs://bucket/prefix", "data/**/*.parquet", "gs://bucket/prefix/data/**/*.parquet"),
        ("gs://bucket/prefix", "/t/p.parquet", "gs://bucket/prefix/t/p.parquet"),
        ("file:///srv/data", "dir/", "file:///srv/data/dir/"),
    ],
)
def test_norm_join_uri_normalizes_safe_paths(base, rel, expected):
    assert norm_join_uri(base, rel) == expected


def test_to_relative_posix_path_normalizes_safe_paths():
    assert to_relative_posix_path("/foo/./bar//baz") == "foo/bar/baz"
    assert to_relative_posix_path("a/../b") == "b"


def test_dataset_cannot_write_outside_its_base(tmp_path):
    base = tmp_path / "base"
    base.mkdir()
    ds = CSVDataset(conn={"conn_type": "fs", "extra": {"base_uri": str(base)}})
    with pytest.raises(ValueError, match="escapes the base"):
        ds.save_pandas("../outside.csv", pd.DataFrame({"a": [1]}))
    assert not (tmp_path / "outside.csv").exists()
