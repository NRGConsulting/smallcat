import os

import pyarrow as pa
import pytest

from smallcat.datasets.delta_table_dataset import DeltaTableDataset

DATABRICKS_CONN = {
    "conn_type": "databricks",
    "host": "https://example.cloud.databricks.com",
    "password": "super-secret-token",
    "extra": {},
}
ENV_KEYS = ("DATABRICKS_WORKSPACE_URL", "DATABRICKS_ACCESS_TOKEN")


@pytest.fixture
def dataset():
    return DeltaTableDataset(conn=DATABRICKS_CONN)


def test_databricks_save_fails_instead_of_silently_skipping(dataset):
    with pytest.raises(NotImplementedError, match="Databricks"):
        dataset.save_arrow_table("t", pa.table({"a": [1]}))


def test_databricks_load_is_not_supported(dataset):
    with pytest.raises(NotImplementedError, match="Databricks"):
        dataset.load_arrow_table("t")


def test_databricks_credentials_never_reach_os_environ(dataset, monkeypatch):
    for key in ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(NotImplementedError):
        dataset.save_arrow_table("t", pa.table({"a": [1]}))
    with pytest.raises(NotImplementedError):
        dataset.load_arrow_table("t")
    assert not any(key in os.environ for key in ENV_KEYS)
