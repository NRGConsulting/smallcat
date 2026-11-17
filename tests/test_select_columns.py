"""SC-043: selected columns are quoted, so reserved and non-ASCII names work."""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from smallcat.connections_config import Connections
from smallcat.datasets.csv_dataset import CSVDataset
from smallcat.datasets.delta_table_dataset import DeltaTableDataset
from smallcat.datasets.excel_dataset import ExcelDataset
from smallcat.datasets.parquet_dataset import ParquetDataset
from smallcat.models import Model, ModelRegistry, use_registry

TRICKY = ["order", "group", "año", "Mixed Case", "a.b"]


@pytest.fixture
def conn(tmp_path):
    return {"conn_type": "fs", "extra": {"base_path": str(tmp_path)}}


@pytest.fixture
def frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "order": ["x", "y", "z"],
            "group": [10, 20, 30],
            "año": [2001, 2002, 2003],
            "Mixed Case": [1.5, 2.5, 3.5],
            "a.b": [True, False, True],
        }
    )


@pytest.mark.parametrize(
    ("dataset_cls", "path"),
    [
        (CSVDataset, "t.csv"),
        (ParquetDataset, "t.parquet"),
        (DeltaTableDataset, "t_delta"),
    ],
)
@pytest.mark.parametrize("where", [None, "id >= 2"])
def test_tricky_column_names_can_be_selected(conn, frame, dataset_cls, path, where):
    dataset_cls(conn).save_polars(path, frame)

    out = dataset_cls(conn).load_polars(path, where=where, columns=TRICKY)

    expected = frame.filter(pl.col("id") >= 2) if where else frame
    assert out.columns == TRICKY
    assert out.to_dicts() == expected.select(TRICKY).to_dicts()


def test_csv_scan_fallback_accepts_tricky_names(conn, frame):
    CSVDataset(conn).save_polars("t.csv", frame)

    out = CSVDataset(conn).scan_polars("t.csv", columns=["order", "año"]).collect()

    assert out.columns == ["order", "año"]


def test_excel_quotes_the_selected_columns():
    fake = MagicMock()
    fake.__enter__.return_value = fake
    with patch.object(ExcelDataset, "_duckdb_conn", return_value=fake):
        ds = ExcelDataset(conn={"conn_type": "fs", "extra": {}})
        ds.load_arrow_record_batch_reader(
            "in.xlsx", where="id = 1", columns=["order", "año"]
        )

    query = " ".join(fake.sql.return_value.query.call_args.args[1].split())
    assert query.startswith('SELECT "order", "año" FROM data')


def test_build_query_quotes_the_columns_and_keeps_star_for_none(conn):
    ds = CSVDataset(conn)

    def sql(columns):
        return " ".join(ds._build_query("data", columns, None).split())

    assert sql(["order"]) == 'SELECT "order" FROM data'
    assert sql(None) == "SELECT * FROM data"
    assert sql([]) == "SELECT * FROM data"


@pytest.mark.parametrize(
    "bad",
    ["name;drop", 'a"b', "", "a\nb", "a\x00b", 'x" FROM other --'],
)
def test_unsafe_column_names_are_still_refused(conn, bad):
    with pytest.raises(ValueError, match="Unsafe column name"):
        CSVDataset(conn)._build_query("data", ["id", bad], None)


# ---------- the case from the issue, through the model layer ----------


class Reserved(Model):
    id: int
    order: str
    año: int

    class Meta:
        location = "reserved.parquet"
        file_format = "parquet"


@pytest.fixture
def registry(tmp_path) -> ModelRegistry:
    reg = ModelRegistry()
    reg.load_connections(
        Connections.from_dict(
            {
                "connections": {
                    "local": {"conn_type": "fs", "extra": {"base_path": str(tmp_path)}},
                },
            },
        ),
    )
    reg.register(Reserved, connection="local")
    with use_registry(reg):
        yield reg


@pytest.fixture
def saved(registry):
    Reserved.objects.save(
        pl.DataFrame({"id": [1, 2], "order": ["x", "y"], "año": [2001, 2002]}),
        mode="overwrite",
    )


def test_where_with_select_of_a_reserved_name(saved):
    out = Reserved.objects.where(id=1).select("order").all()
    assert out.to_dicts() == [{"order": "x"}]


def test_where_with_select_of_a_non_ascii_name(saved):
    out = Reserved.objects.where("id = 2").select("año").all()
    assert out.to_dicts() == [{"año": 2002}]
