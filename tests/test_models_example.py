"""The runnable example in the docs keeps working and prints what it documents."""

import os
import subprocess
import sys
from pathlib import Path

EXAMPLE = Path(__file__).parent.parent / "docs" / "examples" / "models_local.py"


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
