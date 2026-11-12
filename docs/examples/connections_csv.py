# ruff: noqa: D103, INP001, T201
"""Define a connection to a folder of CSV files and load it.

Run it with ``python docs/examples/connections_csv.py``. It writes its
connections file under a temporary directory and touches nothing else.
"""

import tempfile
from pathlib import Path

from smallcat import Connections
from smallcat.connections_config import ConnectionsValidationError

workdir = Path(tempfile.mkdtemp(prefix="smallcat-csv-"))


def title(text: str) -> None:
    print(f"\n=== {text}")


# 1. Define it in a file -------------------------------------------------------
(workdir / "connections.yaml").write_text(
    f"""
connections:
  local:
    conn_type: fs
    extra:
      base_path: {(workdir / "data").as_posix()}
""",
)

# 2. Load it: it is validated here ---------------------------------------------
conns = Connections.from_yaml(workdir / "connections.yaml")

title("what is loaded")
print("repr           ", conns)
print("names          ", conns.names())
print("'local' in     ", "local" in conns)

title("the definition, as a dataset receives it")
local = conns.get("local")
print("conn_type      ", local["conn_type"])  # type: ignore[index]
print("base_path      ", Path(local["extra"]["base_path"]).name)  # type: ignore[index]

# 3. A wrong definition fails when it is loaded --------------------------------
title("a typo is reported, not ignored")
try:
    Connections.from_dict(
        {"connections": {"local": {"conn_type": "fs", "extra": {"base_pth": "/data"}}}},
    )
except ConnectionsValidationError as e:
    print(e)
