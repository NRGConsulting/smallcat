from unittest.mock import MagicMock

import pytest

from smallcat.datasets.base_dataset import sql_string_literal
from smallcat.datasets.csv_dataset import CSVDataset


@pytest.fixture
def dataset(local_conn):
    return CSVDataset(conn=local_conn)


@pytest.mark.parametrize(
    "where",
    [
        "amount > 10",
        "name = 'a--b'",
        "name = 'a;b'",
        "name LIKE '%/*%'",
        "amount > 10 AND name IN ('x', 'y')",
    ],
)
def test_where_accepts_valid_predicates(dataset, where):
    assert dataset._validated_where(where) == where


@pytest.mark.parametrize(
    "where",
    [
        "amount > 10; drop table data",
        "amount > 10 -- comment",
        "amount > 10 /* comment",
        "id IN (SELECT 1)",
        "name = (SELECT content FROM read_text('/etc/passwd'))",
        "EXISTS (SELECT 1)",
        "1) UNION SELECT * FROM other WHERE (1",
        "1) LIMIT 1 OFFSET (1",
        "1) GROUP BY name HAVING (1",
        "amount >",
    ],
)
def test_where_rejects_unsafe_or_invalid_predicates(dataset, where):
    with pytest.raises(ValueError, match="WHERE"):
        dataset._validated_where(where)


def test_build_query_wraps_where_in_parentheses(dataset):
    query = dataset._build_query("data", None, "a = 1 OR b = 2")
    assert query.endswith("WHERE (a = 1 OR b = 2)")


def test_sql_string_literal_escapes_quotes():
    assert sql_string_literal("it's") == "'it''s'"
    assert sql_string_literal("a\b") == "'a\b'"


def test_gcp_secret_escapes_credentials(local_conn):
    ds = CSVDataset(conn=local_conn)
    ds.conn = MagicMock(login="id'x", password="p'; DROP SECRET s; --")
    con = MagicMock()
    ds._configure_gcp_secret(con)
    sql = con.execute.call_args.args[0]
    assert "KEY_ID 'id''x'" in sql
    assert "SECRET 'p''; DROP SECRET s; --'" in sql
