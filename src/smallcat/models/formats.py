"""Which field types each file format can store and read back.

A model declares its fields with Python types, and every type has a Polars
equivalent (see :mod:`smallcat.models.schema`). That does not mean every file
format can hold it. `time` and `timedelta` are the two that cannot, in some
formats or in all of them:

============== ======= ============ ======= =======
Field type     Parquet Delta table  CSV     Excel
============== ======= ============ ======= =======
`time`         yes     no           yes     no
`timedelta`    no      no           no      no
============== ======= ============ ======= =======

Writing them either fails (Delta Lake has no such types) or writes a file that
cannot be read back through smallcat (DuckDB stores a duration in Parquet as an
`INTERVAL`, which Polars refuses; a CSV or an Excel sheet gives it back as a
time or as text). Rather than let that surface when the job runs, a model that
declares one of those combinations is refused when it is defined.

The one exception: Parquet written by another tool can hold a duration that
Polars reads. A **read-only** model (`allowed_write_modes = []`) may declare a
`timedelta` over such a file. (`where()` still cannot read it: DuckDB gives it
back as a number.)

Everything here was checked by writing and reading each type in each format.
"""

import polars as pl

from smallcat.models.enums import ModelFileFormat as Fmt
from smallcat.models.lookups import kind_of
from smallcat.models.schema import ModelSchema

_DURATION_REASONS = {
    Fmt.PARQUET: "DuckDB writes it as an INTERVAL, which Polars cannot read back",
    Fmt.DELTA_TABLE: "Delta Lake has no duration type",
    Fmt.CSV: "a CSV has no duration type: it is read back as a time of day",
    Fmt.EXCEL: "an Excel sheet has no duration type: it is read back as text",
}
_TIME_REASONS = {
    Fmt.DELTA_TABLE: "Delta Lake has no time-of-day type",
    Fmt.EXCEL: "an Excel sheet has no time-of-day type: it is read back as text",
}

UNSUPPORTED: dict[str, dict[Fmt, str]] = {
    "duration": _DURATION_REASONS,
    "time": _TIME_REASONS,
}
"""For each kind of field, the formats that cannot hold it and why."""

_NAMES = {"duration": "timedelta", "time": "time"}
_ALTERNATIVES = {
    "duration": (
        "Store it as a number (seconds as a `float`, or microseconds as an `int`) "
        "and convert it in your code"
    ),
    "time": (
        "Store it as a `datetime`, as a `str` such as '08:30:00', or as an `int` "
        "(seconds since midnight)"
    ),
}
_READ_ONLY_OK = {("duration", Fmt.PARQUET)}


def unsupported_fields(
    schema: ModelSchema,
    file_format: Fmt,
    *,
    writable: bool,
) -> list[str]:
    """Describe the fields whose type the file format cannot hold.

    Args:
        schema: The model's schema.
        file_format: The format the model's data is stored in.
        writable: Whether the model allows any write mode. A read-only model may
            declare a `timedelta` over a Parquet file written by another tool.

    Returns:
        One message per field that cannot be stored, empty if all can.
    """
    problems = []
    for name, spec in schema.fields.items():
        dtype = spec.dtype
        nested = False
        while isinstance(dtype, pl.List):
            inner = dtype.inner
            dtype = inner if isinstance(inner, pl.DataType) else inner()
            nested = True
        kind = kind_of(dtype)
        reason = UNSUPPORTED.get(kind, {}).get(file_format)
        if reason is None or (not writable and (kind, file_format) in _READ_ONLY_OK):
            continue
        shown = f"a list of {_NAMES[kind]}" if nested else f"a {_NAMES[kind]}"
        advice = _ALTERNATIVES[kind]
        if (kind, file_format) in _READ_ONLY_OK:
            advice += (
                ", or make the model read-only (`allowed_write_modes = []`) if "
                "the file is written by another tool"
            )
        problems.append(
            f"{schema.model_name}.{name} is {shown}, which cannot be stored in "
            f"{file_format.value}: {reason}. {advice}.",
        )
    return problems
