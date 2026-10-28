from unittest.mock import patch

import pyarrow as pa
import pytest

from smallcat.datasets import delta_table_dataset as mod
from smallcat.datasets.delta_table_dataset import (
    DeltaTableDataset,
    DeltaTableSaveOptions,
)


@pytest.fixture
def table():
    return pa.table({"id": [1, 2, 3]})


def _dataset(local_conn, **save_options):
    opts = DeltaTableSaveOptions(**save_options) if save_options else None
    return DeltaTableDataset(conn=local_conn, save_options=opts)


def test_write_is_attempted_once_and_type_error_propagates(local_conn, table):
    ds = _dataset(local_conn, mode="append")
    with (
        patch.object(mod, "write_deltalake", side_effect=TypeError("boom")) as write,
        pytest.raises(TypeError, match="boom"),
    ):
        ds.save_arrow_table("t", table)
    assert write.call_count == 1


def test_engine_not_passed_when_unsupported(local_conn, table):
    ds = _dataset(local_conn, mode="append")
    with (
        patch.object(mod, "_WRITE_DELTALAKE_HAS_ENGINE", new=False),
        patch.object(mod, "write_deltalake") as write,
    ):
        ds.save_arrow_table("t", table)
    assert "engine" not in write.call_args.kwargs


@pytest.mark.parametrize(
    ("save_options", "engine"),
    [({}, "pyarrow"), ({"schema_mode": "merge"}, "rust")],
)
def test_engine_passed_when_supported(local_conn, table, save_options, engine):
    ds = _dataset(local_conn, **save_options)
    with (
        patch.object(mod, "_WRITE_DELTALAKE_HAS_ENGINE", new=True),
        patch.object(mod, "write_deltalake") as write,
    ):
        ds.save_arrow_table("t", table)
    assert write.call_count == 1
    assert write.call_args.kwargs["engine"] == engine


def test_append_writes_rows_exactly_once(local_conn, table):
    ds = _dataset(local_conn, mode="append")
    ds.save_arrow_table("t", table)
    assert ds.load_arrow_table("t").num_rows == 3
