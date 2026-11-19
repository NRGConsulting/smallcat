# ruff: noqa: D101, D103, D106, INP001, T201
"""Load a connection and use it from a model.

Run it with ``python docs/examples/connections_orm.py``. It writes its
connections file and its data under a temporary directory.
"""

import tempfile
from pathlib import Path

import polars as pl

from smallcat import Connections, Model, ModelFileFormat, ModelWriteMode, registry

workdir = Path(tempfile.mkdtemp(prefix="smallcat-conn-orm-"))


def title(text: str) -> None:
    print(f"\n=== {text}")


# 1. The connections: defined once, in a file ----------------------------------
(workdir / "connections.yaml").write_text(
    f"""
connections:
  lake:
    conn_type: fs
    extra:
      base_path: {(workdir / "lake").as_posix()}
  archive:
    conn_type: fs
    extra:
      base_path: {(workdir / "archive").as_posix()}
""",
)
conns = Connections.from_yaml(workdir / "connections.yaml")
print(conns)


# 2. The model never names a connection ----------------------------------------
class Person(Model):
    id: int
    name: str

    class Meta:
        location = "people.csv"
        file_format = ModelFileFormat.CSV


# 3. The registry loads the connections and says which one the model uses ------
registry.load_connections(conns)
registry.register(Person, connection="lake")

# 4. Basic use -----------------------------------------------------------------
title("save and read")
df = pl.DataFrame({"id": [1, 2, 3], "name": ["ada", "linus", "grace"]})
Person.objects.save(df, mode=ModelWriteMode.OVERWRITE)
print("all            ", Person.objects.all().rows())
print("filter         ", Person.objects.filter(pl.col("id") > 1).all().rows())

# 5. Other storage, same model: only the registration changes ------------------
title("the same model on other storage")
registry.register(Person, connection="archive", replace=True)
Person.objects.save(pl.DataFrame({"id": [9], "name": ["alan"]}), mode="overwrite")
print("archive        ", Person.objects.all().rows())
registry.register(Person, connection="lake", replace=True)
print("lake, untouched", Person.objects.all().rows())

# 6. A name that was never loaded ----------------------------------------------
title("a connection that is not loaded")
try:
    registry.register(Person, connection="warehouse", replace=True)
except Exception as e:  # noqa: BLE001
    print(type(e).__name__, "-", e)
