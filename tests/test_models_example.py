"""The runnable example in the docs keeps working and prints what it documents."""

import os
import subprocess
import sys
from pathlib import Path

EXAMPLES = Path(__file__).parent.parent / "docs" / "examples"
EXAMPLE = EXAMPLES / "models_local.py"
QUERY_EXAMPLE = EXAMPLES / "models_query.py"
QUERYING_MODELS = EXAMPLES / "querying_models.py"
QUERYING_CATALOG = EXAMPLES / "querying_catalog.py"


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
    assert "SELECTION: col(\"id\") > 1" in out  # the filter reached the scan
    assert "[wrong types] ModelSchemaValidationError" in out
    assert "[append, which the model does not allow] ModelWriteNotAllowedError" in out
    assert "[write a read-only model] ModelWriteNotAllowedError" in out
    assert "[filter + where] ModelQueryError" in out
    assert "inside use_registry the data is separate: 0 rows" in out
    assert "outside it is untouched: 3 rows" in out


def test_the_query_example_runs_and_filters_as_documented():
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-W", "ignore", str(QUERY_EXAMPLE)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert out.startswith("Connections(lake:fs)")
    assert 'SELECTION: col("amount") > 100.0' in out  # pushed into the scan
    assert "rows : 2" in out
    assert "again: 1" in out  # the original query was not modified
    assert out.count("ModelQueryError") == 4
    # revenue per country: US 980 + 60, then UK, ES and FI (the 15.0 order is under 20)
    assert "1040.0" in out
    assert "240.0" in out


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
