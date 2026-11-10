"""The runnable example in the docs keeps working and prints what it documents."""

import os
import subprocess
import sys
from pathlib import Path

EXAMPLES = Path(__file__).parent.parent / "docs" / "examples"
EXAMPLE = EXAMPLES / "models_local.py"
QUERY_EXAMPLE = EXAMPLES / "models_query.py"


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
    assert "-- events (appended twice, then merged)" in out
    assert "│ 2   ┆ logout   │" in out  # merge updated id 2
    assert "│ 3   ┆ purchase │" in out  # and inserted id 3
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
