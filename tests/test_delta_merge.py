"""Delta `merge` (upsert by key) and `update` by predicate."""

from datetime import date

import polars as pl
import pyarrow as pa
import pytest
from pydantic import ValidationError

from smallcat.datasets.delta_table_dataset import (
    DeltaTableDataset,
    DeltaTableSaveOptions,
)

PATH = "customers"


def _ds(conn, **options):
    return DeltaTableDataset(conn=conn, save_options=DeltaTableSaveOptions(**options))


def _merge(conn, rows, **extra):
    options = {"mode": "merge", "merge_keys": ["id"], **extra}
    _ds(conn, **options).save_arrow_table(PATH, pa.table(rows))


def _read(conn) -> pl.DataFrame:
    return DeltaTableDataset(conn=conn).load_polars(PATH).sort("id")


@pytest.fixture
def seeded(local_conn):
    _merge(local_conn, {"id": [1, 2], "name": ["a", "b"], "city": ["x", "y"]})
    return local_conn


# ---------- merge ----------


def test_merge_creates_the_table_when_it_does_not_exist(local_conn):
    _merge(local_conn, {"id": [1, 2], "name": ["a", "b"]})
    assert _read(local_conn).to_dict(as_series=False) == {
        "id": [1, 2],
        "name": ["a", "b"],
    }


def test_merge_inserts_new_rows_and_updates_existing_ones(seeded):
    _merge(seeded, {"id": [2, 3], "name": ["B", "c"], "city": ["Y", "z"]})
    assert _read(seeded).to_dict(as_series=False) == {
        "id": [1, 2, 3],
        "name": ["a", "B", "c"],
        "city": ["x", "Y", "z"],
    }


def test_merge_never_deletes_rows(seeded):
    _merge(seeded, {"id": [9], "name": ["n"], "city": ["c"]})
    assert _read(seeded)["id"].to_list() == [1, 2, 9]


def test_merge_without_changes_keeps_the_data(seeded):
    _merge(seeded, {"id": [1, 2], "name": ["a", "b"], "city": ["x", "y"]})
    assert _read(seeded).height == 2
    assert _read(seeded)["name"].to_list() == ["a", "b"]


def test_merge_update_columns_limits_what_is_overwritten(seeded):
    _merge(
        seeded,
        {"id": [2, 3], "name": ["B", "c"], "city": ["Y", "z"]},
        update_columns=["name"],
    )
    assert _read(seeded).to_dict(as_series=False) == {
        "id": [1, 2, 3],
        "name": ["a", "B", "c"],
        "city": ["x", "y", "z"],  # existing row keeps its city; new row is whole
    }


def test_merge_with_a_composite_key(local_conn):
    options = {"mode": "merge", "merge_keys": ["id", "k"]}
    ds = _ds(local_conn, **options)
    ds.save_arrow_table(PATH, pa.table({"id": [1, 1], "k": ["a", "b"], "v": [1, 2]}))
    ds.save_arrow_table(PATH, pa.table({"id": [1, 2], "k": ["b", "a"], "v": [20, 3]}))
    out = DeltaTableDataset(conn=local_conn).load_polars(PATH).sort("id", "k")
    assert out.to_dict(as_series=False) == {
        "id": [1, 1, 2],
        "k": ["a", "b", "a"],
        "v": [1, 20, 3],
    }


def test_merge_with_only_key_columns_just_inserts(local_conn):
    _merge(local_conn, {"id": [1]})
    _merge(local_conn, {"id": [1, 2]})
    assert _read(local_conn)["id"].to_list() == [1, 2]


# ---------- merge: errors, always before writing ----------


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ({"id": [1, 1], "name": ["a", "b"]}, "several rows"),
        ({"id": [1, None], "name": ["a", "b"]}, "null"),
        ({"other": [1], "name": ["a"]}, "not columns of the data"),
    ],
)
def test_bad_source_is_rejected_and_nothing_is_written(local_conn, rows, message):
    ds = _ds(local_conn, mode="merge", merge_keys=["id"])
    with pytest.raises(ValueError, match=message):
        ds.save_arrow_table(PATH, pa.table(rows))
    assert not (local_conn_path(local_conn) / PATH).exists()


def local_conn_path(conn):
    from pathlib import Path

    return Path(conn["extra"]["base_path"])


def test_bad_source_leaves_an_existing_table_untouched(seeded):
    with pytest.raises(ValueError, match="several rows"):
        _merge(seeded, {"id": [3, 3], "name": ["c", "d"], "city": ["z", "z"]})
    assert _read(seeded)["id"].to_list() == [1, 2]


def test_merge_with_a_column_missing_in_the_table_is_rejected(seeded):
    with pytest.raises(ValueError, match="not in the Delta table"):
        _merge(seeded, {"id": [3], "name": ["c"], "city": ["z"], "extra": [1]})


def test_update_columns_missing_in_the_data_is_rejected(seeded):
    with pytest.raises(ValueError, match="update_columns"):
        _merge(seeded, {"id": [3], "name": ["c"]}, update_columns=["city"])


def test_merge_on_databricks_is_not_supported():
    ds = DeltaTableDataset(
        conn={"conn_type": "databricks", "host": "h", "password": "p", "extra": {}},
        save_options=DeltaTableSaveOptions(mode="merge", merge_keys=["id"]),
    )
    with pytest.raises(NotImplementedError, match="Databricks"):
        ds.save_arrow_table(PATH, pa.table({"id": [1]}))


def test_column_names_that_look_like_sql_are_quoted(local_conn):
    weird = 'id" OR 1=1 --'
    _merge(local_conn, {weird: [1, 2], "v": [1, 2]}, merge_keys=[weird])
    _merge(local_conn, {weird: [2, 3], "v": [20, 30]}, merge_keys=[weird])
    out = DeltaTableDataset(conn=local_conn).load_polars(PATH).sort(weird)
    assert out["v"].to_list() == [1, 20, 30]


# ---------- options ----------


@pytest.mark.parametrize(
    "options",
    [
        {"mode": "merge"},
        {"mode": "merge", "merge_keys": []},
        {"mode": "merge", "merge_keys": ["id", "id"]},
        {"mode": "merge", "merge_keys": ["id"], "update_columns": ["id"]},
        {"mode": "merge", "merge_keys": ["id"], "schema_mode": "merge"},
        {"mode": "append", "merge_keys": ["id"]},
        {"mode": "overwrite", "update_columns": ["a"]},
    ],
)
def test_invalid_merge_options_are_rejected(options):
    with pytest.raises(ValidationError):
        DeltaTableSaveOptions(**options)


def test_merge_options_without_a_mode_do_not_write(local_conn):
    ds = _ds(local_conn, merge_keys=["id"])
    with pytest.raises(ValueError, match="only valid with mode 'merge'"):
        ds.save_arrow_table(PATH, pa.table({"id": [1]}))


def test_merge_options_are_not_passed_to_write_deltalake(local_conn):
    # `update_columns` alone is allowed (no mode) and must not reach delta-rs
    ds = _ds(local_conn, mode="append")
    ds.save_arrow_table(PATH, pa.table({"id": [1]}))
    assert _read(local_conn).height == 1


# ---------- update ----------


def test_update_changes_the_rows_of_the_predicate(seeded):
    ds = DeltaTableDataset(conn=seeded)
    metrics = ds.update(PATH, "id = 2", {"name": "it's", "city": None})
    assert metrics["num_updated_rows"] == 1
    out = _read(seeded)
    assert out["name"].to_list() == ["a", "it's"]
    assert out["city"].to_list() == ["x", None]


def test_update_with_a_date_and_numbers(local_conn):
    ds = DeltaTableDataset(conn=local_conn)
    ds.save_arrow_table(
        PATH,
        pa.table(
            {
                "id": [1, 2],
                "n": [1.5, 2.5],
                "d": [date(2020, 1, 1), date(2020, 1, 2)],
            },
        ),
    )
    ds.update(PATH, "id = 1", {"n": 9.0, "d": date(2021, 5, 6)})
    out = _read(local_conn)
    assert out["n"].to_list() == [9.0, 2.5]
    assert out["d"].to_list() == [date(2021, 5, 6), date(2020, 1, 2)]


@pytest.mark.parametrize("predicate", ["", "   ", None])
def test_update_requires_a_predicate(seeded, predicate):
    with pytest.raises(ValueError, match="predicate"):
        DeltaTableDataset(conn=seeded).update(PATH, predicate, {"name": "x"})


@pytest.mark.parametrize(
    "predicate",
    [
        "id = 1; DROP TABLE t",
        "id IN (SELECT 1)",
        "1) UNION SELECT * FROM data WHERE (1",
    ],
)
def test_update_rejects_unsafe_predicates(seeded, predicate):
    with pytest.raises(ValueError, match="WHERE"):
        DeltaTableDataset(conn=seeded).update(PATH, predicate, {"name": "x"})
    assert _read(seeded)["name"].to_list() == ["a", "b"]


def test_update_values_cannot_inject_sql(seeded):
    evil = "x' WHERE 1=1 --"
    DeltaTableDataset(conn=seeded).update(PATH, "id = 1", {"name": evil})
    assert _read(seeded)["name"].to_list() == [evil, "b"]


def test_update_rejects_unknown_columns_and_bad_values(seeded):
    ds = DeltaTableDataset(conn=seeded)
    with pytest.raises(ValueError, match="not in the Delta table"):
        ds.update(PATH, "id = 1", {"nope": 1})
    with pytest.raises(ValueError, match="at least one value"):
        ds.update(PATH, "id = 1", {})
    with pytest.raises(TypeError, match="unsupported"):
        ds.update(PATH, "id = 1", {"name": object()})
    with pytest.raises(ValueError, match="NaN"):
        ds.update(PATH, "id = 1", {"name": float("nan")})


def test_update_on_databricks_is_not_supported():
    ds = DeltaTableDataset(
        conn={"conn_type": "databricks", "host": "h", "password": "p", "extra": {}},
    )
    with pytest.raises(NotImplementedError, match="Databricks"):
        ds.update(PATH, "id = 1", {"a": 1})


# ---------- catalog ----------


@pytest.mark.filterwarnings("ignore:Inline connections are deprecated")
def test_catalog_save_polars_merges_with_save_options(local_conn):
    from smallcat.catalog import Catalog

    catalog = Catalog.from_dict(
        {
            "entries": {
                "customers": {
                    "connection": local_conn,
                    "file_format": "delta_table",
                    "location": PATH,
                    "save_options": {
                        "mode": "merge",
                        "merge_keys": ["id"],
                        "update_columns": ["name"],
                    },
                },
            },
        },
    )
    catalog.save_polars("customers", pl.DataFrame({"id": [1, 2], "name": ["a", "b"]}))
    catalog.save_polars("customers", pl.DataFrame({"id": [2, 3], "name": ["B", "c"]}))
    out = catalog.load_polars("customers").sort("id")
    assert out.to_dict(as_series=False) == {"id": [1, 2, 3], "name": ["a", "B", "c"]}
