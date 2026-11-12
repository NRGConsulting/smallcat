"""The runnable example in the docs keeps working and prints what it documents."""

import os
import subprocess
import sys
from pathlib import Path

EXAMPLES = Path(__file__).parent.parent / "docs" / "examples"
EXAMPLE = EXAMPLES / "models_local.py"
MODELS_QUERYING = EXAMPLES / "models_querying.py"
QUERYING_MODELS = EXAMPLES / "querying_models.py"
QUERYING_CATALOG = EXAMPLES / "querying_catalog.py"
CONNECTIONS_CSV = EXAMPLES / "connections_csv.py"
CONNECTIONS_GCS = EXAMPLES / "connections_gcs.py"
CONNECTIONS_S3 = EXAMPLES / "connections_s3.py"
CONNECTIONS_CATALOG = EXAMPLES / "connections_catalog.py"
CONNECTIONS_ORM = EXAMPLES / "connections_orm.py"
WRITING_CATALOG = EXAMPLES / "writing_catalog.py"
WRITING_MODELS = EXAMPLES / "writing_models.py"


def test_the_models_example_runs_and_the_guard_rails_refuse():
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-W", "ignore", str(EXAMPLE)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert "-- events (appended twice, then merged)" in out
    assert "│ 2   ┆ logout   │" in out  # merge updated id 2
    assert "│ 3   ┆ purchase │" in out  # and inserted id 3
    assert "[wrong types] ModelSchemaValidationError" in out
    assert "[append, which the model does not allow] ModelWriteNotAllowedError" in out
    assert "[write a read-only model] ModelWriteNotAllowedError" in out
    assert "inside use_registry the data is separate: 0 rows" in out
    assert "outside it is untouched: 3 rows" in out


def test_the_models_querying_example_filters_as_documented():
    out = _run(MODELS_QUERYING)
    assert 'SELECTION: col("id") > 1' in out  # the filter reached the scan
    assert "PROJECT 2/4 COLUMNS" in out
    assert "Customer(id=1, name='Ada'" in out  # as_models
    assert "rows : 2" in out
    assert "again: 1 (recent is unchanged)" in out  # the first query was not modified
    assert "[filter + where] ModelQueryError" in out


def _run(script: Path) -> str:
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-W", "ignore", str(script)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def test_the_querying_models_page_prints_what_it_documents():
    out = _run(QUERYING_MODELS)
    # Polars, SQL and lookups, each with the rows the page lists
    assert "amount > 100        : [1, 3, 5]" in out
    assert "positional : [3, 6]" in out
    assert "sql=       : [3, 6]" in out
    assert "ne          : [1, 2, 4, 5]" in out
    assert "range       : [1, 2, 6]" in out
    assert "isnull True : [1, 3, 5]" in out
    assert "contains    : [1, 3, 4, 5, 6]" in out
    assert "date        : [3, 4, 5, 6]" in out
    assert "datetime    : [1, 2]" in out
    # values are data
    assert "a quote           : []" in out
    assert "a literal percent : [6]" in out
    # sql= and lookups together; and the three spellings agree
    assert "[3] (the OR stays inside its own parentheses)" in out
    assert "[5] [5] [5]" in out
    # the filter reached the Parquet scan
    assert 'SELECTION: col("amount") > 100.0' in out
    # what is refused, as the page shows it
    assert "did you mean 'customer'?" in out
    assert "'gt' is not available for 'customer', a str field" in out
    assert "does not take '100' (str)" in out
    assert "None is not allowed in the values; use status__isnull" in out
    assert "expects exactly 2 values, got 1" in out
    assert "where() was already used" in out
    assert out.count("ModelQueryError") == 10


def test_the_querying_catalog_page_prints_what_it_documents():
    out = _run(QUERYING_CATALOG)
    for key in ("orders_csv", "orders_parquet", "orders_delta"):
        assert f"{key:15} [3, 5]" in out  # the same rows on every format
    assert "customer IN ('ada', 'alan')        [1, 4, 5]" in out
    assert "coupon LIKE '%!%%' ESCAPE '!'      [6]" in out
    assert "amount BETWEEN 35.5 AND 120        [1, 2, 6]" in out
    assert "Table 3 rows" in out
    assert 'SELECTION: col("amount") > 100.0' in out  # pushed into the scan
    assert "1040.0" in out
    assert out.count("ValueError: Unsafe or invalid WHERE clause") == 3
    assert "KeyError:" in out


def test_the_csv_connection_page_prints_what_it_documents():
    out = _run(CONNECTIONS_CSV)
    assert "repr            Connections(local:fs)" in out  # names and types only
    assert "'local' in      True" in out
    assert "conn_type       fs" in out
    assert "local.extra.base_pth: Extra inputs are not permitted" in out


def test_the_catalog_connection_page_prints_what_it_documents():
    out = _run(CONNECTIONS_CATALOG)
    assert "load            [(1, 'ada'), (2, 'linus'), (3, 'grace')]" in out
    assert "where + columns [('linus',), ('grace',)]" in out
    assert "entry 'sales' uses 'warehouse' (available: lake)" in out


def test_the_orm_connection_page_prints_what_it_documents():
    out = _run(CONNECTIONS_ORM)
    assert "filter          [(2, 'linus'), (3, 'grace')]" in out
    assert "archive         [(9, 'alan')]" in out
    assert "lake, untouched [(1, 'ada'), (2, 'linus'), (3, 'grace')]" in out
    assert "Connection 'warehouse' is not loaded (available: lake, archive)" in out


def test_the_gcs_connection_page_prints_what_it_documents():
    out = _run(CONNECTIONS_GCS)
    assert (
        "Connections(gcs_hmac:google_cloud_platform, gcs_sa:google_cloud_platform)"
    ) in out
    assert "password        False" in out  # the secret is not in the repr
    for line in (
        "GCS HMAC keys need both `login` (key id) and `password` (secret).",
        "Provide only one service account source, found `keyfile`, `key_path`.",
    ):
        assert line in out


def test_the_s3_connection_page_prints_what_it_documents():
    out = _run(CONNECTIONS_S3)
    assert "Connections(s3_chain:aws, s3_profile:aws, s3_keys:aws, s3_minio:aws)" in out
    assert "password        False" in out  # the secret is not in the repr
    for line in (
        "AWS static keys need both `login` (access key id) and `password`.",
        "`profile_name` cannot be combined with static keys",
        "`aws_session_token` needs static keys (login/password).",
    ):
        assert line in out


def test_the_writing_catalog_page_prints_what_it_documents():
    out = _run(WRITING_CATALOG)
    assert "people_csv      [(2, 'linus', 'Helsinki')]" in out
    assert (
        "append          [(1, 'ada', 'Madrid'), (2, 'linus', 'Oslo'), (3, 'grace', 'Boston')]"
        in out
    )
    assert (
        "ignore          [(1, 'ada', 'Madrid'), (2, 'linus', 'Oslo'), (3, 'grace', 'Boston')]"
        in out
    )
    assert (
        "(2, 'linus', 'Helsinki'), (3, 'grace', 'Boston'), (4, 'alan', 'London')]"
        in out
    )
    assert "(1, 'ada', 'Seville')" in out  # update_columns kept the name
    assert "first run       2 rows" in out
    assert "second run      2 rows" in out  # ignore: the second save did nothing
    assert "rows updated    1" in out


def test_the_writing_models_page_prints_what_it_documents():
    out = _run(WRITING_MODELS)
    assert "(9, 'grace', 'Austin')" in out  # merged by name
    assert "rows updated    1" in out
    assert "overwrite       [(2, 'linus', 'Oslo')]" in out
    for line in (
        "no mode         TypeError",
        "append a file   ModelUnsupportedWriteModeError",
        "merge a file    ModelUnsupportedWriteModeError",
        "keys, no merge  ModelQueryError",
        "key not a field ModelQueryError",
        "wrong columns   ModelSchemaValidationError",
        "does not allow 'overwrite' (allowed: ['merge'])",
    ):
        assert line in out
