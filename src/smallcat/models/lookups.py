"""Django-style lookups for `where()`: ``job_id__lte=2`` and friends.

A lookup is written as ``<field>`` or ``<field>__<operator>`` and a value:

    Customer.objects.where(job_id=42)
    Customer.objects.where(job_id__lte=2, country="ES")
    Customer.objects.where(status__in=["new", "open"], deleted_at__isnull=True)

Lookups are resolved when the query is built, against the fields of the model:
an unknown field, an operator the field's type does not offer or a value of the
wrong type raises :class:`ModelQueryError` right where you write it. Nothing is
defined per model: the fields and their types are read from the model itself.

They are rendered to an SQL predicate (DuckDB). Names come from the model and
values are typed literals that this module quotes, so nothing the caller passes
is ever interpreted as SQL.

Operators:

* `exact`: equal. It is the default when no operator is given. Takes one value,
  or `None` (the same as `isnull=True`).
* `ne`: not equal. One value, or `None`.
* `lt`, `lte`, `gt`, `gte`: less than, less than or equal, greater than, greater
  than or equal. One value.
* `in`: equal to any of the values. A list, tuple or set; an empty one matches
  nothing.
* `range`: between two values, both included. A pair `(low, high)`.
* `isnull`: `True` for is null, `False` for is not null. A `bool`.
* `contains`, `startswith`, `endswith`: text matches. A `str`; `%` and `_` in it
  mean themselves.

Which operators a field accepts depends on its type (see :data:`OPERATORS`).
"""

import datetime as dt
import math
from collections.abc import Callable, Mapping
from difflib import get_close_matches
from typing import Any

import polars as pl

from smallcat.datasets.base_dataset import sql_string_literal
from smallcat.models.errors import ModelQueryError
from smallcat.models.schema import FieldSpec, ModelSchema

_ORDERED = ("exact", "ne", "lt", "lte", "gt", "gte", "in", "range", "isnull")
_TEXT = ("exact", "ne", "in", "contains", "startswith", "endswith", "isnull")

OPERATORS: dict[str, tuple[str, ...]] = {
    "int": _ORDERED,
    "float": _ORDERED,
    "date": _ORDERED,
    "datetime": _ORDERED,
    "time": _ORDERED,
    "duration": _ORDERED,
    "str": _TEXT,
    "bool": ("exact", "ne", "isnull"),
    "other": ("isnull",),
}
"""The operators each kind of field accepts. `other` is lists and bytes."""

_COMPARISON_SQL = {
    "exact": "=",
    "ne": "<>",
    "lt": "<",
    "lte": "<=",
    "gt": ">",
    "gte": ">=",
}
_LIKE_ESCAPE = "!"


def kind_of(dtype: pl.DataType) -> str:
    """Return the kind of a field, which decides its operators and its values.

    Args:
        dtype: The canonical Polars type of the field.

    Returns:
        One of the keys of :data:`OPERATORS`.
    """
    if dtype.is_integer():
        return "int"
    if dtype.is_float():
        return "float"
    simple = {
        pl.String: "str",
        pl.Boolean: "bool",
        pl.Date: "date",
        pl.Time: "time",
    }
    for polars_type, kind in simple.items():
        if dtype == polars_type:
            return kind
    if isinstance(dtype, pl.Datetime):
        return "datetime"
    if isinstance(dtype, pl.Duration):
        return "duration"
    return "other"


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _bad_value(label: str, kind: str, value: object) -> ModelQueryError:
    msg = f"{label}: a {kind} field does not take {value!r} ({type(value).__name__})"
    return ModelQueryError(msg)


def _int(label: str, value: object) -> str:
    if type(value) is not int:
        raise _bad_value(label, "int", value)
    return str(value)


def _float(label: str, value: object) -> str:
    if type(value) not in (int, float) or not math.isfinite(value):  # type: ignore[arg-type]
        raise _bad_value(label, "float", value)
    return repr(float(value))  # type: ignore[arg-type]


def _str(label: str, value: object) -> str:
    if not isinstance(value, str):
        raise _bad_value(label, "str", value)
    return _escape_colons(sql_string_literal(value))


def _escape_colons(literal: str) -> str:
    """Protect ``:name`` in text, which SQLAlchemy would take for a bind parameter."""
    return literal.replace(":", "\\:")


def _bool(label: str, value: object) -> str:
    if not isinstance(value, bool):
        raise _bad_value(label, "bool", value)
    return "TRUE" if value else "FALSE"


def _date(label: str, value: object) -> str:
    if type(value) is not dt.date:
        raise _bad_value(label, "date", value)
    return f"DATE '{value.isoformat()}'"


def _datetime(label: str, value: object) -> str:
    if not isinstance(value, dt.datetime):
        raise _bad_value(label, "datetime", value)
    if value.tzinfo is not None:
        msg = f"{label}: the field is a datetime without time zone; got {value!r}"
        raise ModelQueryError(msg)
    return f"TIMESTAMP '{value.isoformat(sep=' ')}'"


def _time(label: str, value: object) -> str:
    if not isinstance(value, dt.time) or value.tzinfo is not None:
        raise _bad_value(label, "time", value)
    return f"TIME '{value.isoformat()}'"


def _duration(label: str, value: object) -> str:
    if not isinstance(value, dt.timedelta):
        raise _bad_value(label, "duration", value)
    return f"to_microseconds({value // dt.timedelta(microseconds=1)})"


_RENDER: dict[str, Callable[[str, object], str]] = {
    "int": _int,
    "float": _float,
    "str": _str,
    "bool": _bool,
    "date": _date,
    "datetime": _datetime,
    "time": _time,
    "duration": _duration,
}


def _resolve(
    model_name: str,
    fields: Mapping[str, FieldSpec],
    key: str,
) -> tuple[FieldSpec, str]:
    """Split a lookup name into its field and operator, or explain why not."""
    if key in fields:
        spec = fields[key]
        kind = kind_of(spec.dtype)
        if "exact" not in OPERATORS[kind]:
            msg = (
                f"where(): {key!r} is a {kind} field, which cannot be compared "
                f"(available: {key}__{', '.join(OPERATORS[kind])})"
            )
            raise ModelQueryError(msg)
        return spec, "exact"
    name, sep, operator = key.rpartition("__")
    if sep and name in fields:
        spec = fields[name]
        allowed = OPERATORS[kind_of(spec.dtype)]
        if operator not in {op for ops in OPERATORS.values() for op in ops}:
            msg = (
                f"where(): '{operator}' is not a lookup operator (in '{key}'); "
                f"{name!r} accepts: {', '.join(allowed)}"
            )
            raise ModelQueryError(msg)
        if operator not in allowed:
            msg = (
                f"where(): '{operator}' is not available for {name!r}, a "
                f"{kind_of(spec.dtype)} field (available: {', '.join(allowed)})"
            )
            raise ModelQueryError(msg)
        return spec, operator
    close = get_close_matches(name if sep else key, list(fields), n=1)
    hint = f"; did you mean {close[0]!r}?" if close else ""
    msg = (
        f"where(): {key!r} is not a field of {model_name}{hint} "
        f"(fields: {', '.join(fields)})"
    )
    raise ModelQueryError(msg)


def _like(operator: str, value: str) -> str:
    escaped = (
        value.replace(_LIKE_ESCAPE, _LIKE_ESCAPE * 2)
        .replace("%", _LIKE_ESCAPE + "%")
        .replace("_", _LIKE_ESCAPE + "_")
    )
    pattern = {
        "contains": f"%{escaped}%",
        "startswith": f"{escaped}%",
        "endswith": f"%{escaped}",
    }[operator]
    return f"LIKE {_escape_colons(sql_string_literal(pattern))} ESCAPE '{_LIKE_ESCAPE}'"


def _sequence(label: str, value: object, size: int | None) -> list[Any]:
    if not isinstance(value, list | tuple | set | frozenset):
        msg = f"{label}: expects a list, tuple or set, got {type(value).__name__}"
        if size == 2:  # noqa: PLR2004
            msg = f"{label}: expects a pair (low, high), got {type(value).__name__}"
        raise ModelQueryError(msg)
    unordered = isinstance(value, set | frozenset)
    items = sorted(value, key=repr) if unordered else list(value)
    if size is not None and len(items) != size:
        msg = f"{label}: expects exactly {size} values, got {len(items)}"
        raise ModelQueryError(msg)
    if any(item is None for item in items):
        field = label.rpartition("__")[0]
        msg = f"{label}: None is not allowed in the values; use {field}__isnull"
        raise ModelQueryError(msg)
    return items


def _several(
    operator: str,
    column: str,
    render: Callable[[str, object], str],
    value: object,
    key: str,
) -> str:
    """Render `in` and `range`, which take several values."""
    if operator == "range":
        low, high = _sequence(key, value, 2)
        return f"{column} BETWEEN {render(key, low)} AND {render(key, high)}"
    items = _sequence(key, value, None)
    if not items:
        return "FALSE"
    return f"{column} IN ({', '.join(render(key, item) for item in items)})"


def _condition(spec: FieldSpec, operator: str, value: object, key: str) -> str:
    """Render one lookup as an SQL condition."""
    column = _quote_identifier(spec.name)
    kind = kind_of(spec.dtype)

    if operator == "isnull":
        if not isinstance(value, bool):
            raise _bad_value(key, "bool", value)
        return f"{column} IS {'' if value else 'NOT '}NULL"

    render = _RENDER[kind]  # `other` fields only reach here with isnull, handled above
    if kind == "time":
        # Polars stores a time as TIME_NS in Parquet and DuckDB will not compare
        # that with a TIME literal; casting the column works for every format.
        column = f"CAST({column} AS TIME)"
    if operator in {"in", "range"}:
        return _several(operator, column, render, value, key)
    if operator in {"contains", "startswith", "endswith"}:
        if not isinstance(value, str):
            raise _bad_value(key, "str", value)
        return f"{column} {_like(operator, value)}"
    if value is None and operator in {"exact", "ne"}:
        return f"{column} IS {'NOT ' if operator == 'ne' else ''}NULL"
    return f"{column} {_COMPARISON_SQL[operator]} {render(key, value)}"


def lookups_to_sql(schema: ModelSchema, lookups: Mapping[str, object]) -> str:
    """Render lookups as one SQL predicate, joined with AND.

    Args:
        schema: The schema of the model the lookups refer to.
        lookups: Lookup names and their values, e.g. ``{"job_id__lte": 2}``.

    Returns:
        The SQL predicate (DuckDB).

    Raises:
        ModelQueryError: If there are no lookups, a name is not a field of the
            model, an operator is not available for the field's type, or a
            value does not suit the field.
    """
    if not lookups:
        msg = "where() needs lookups such as job_id__lte=2, or sql='...'"
        raise ModelQueryError(msg)
    parts = []
    for key, value in lookups.items():
        spec, operator = _resolve(schema.model_name, schema.fields, key)
        parts.append(_condition(spec, operator, value, key))
    return " AND ".join(parts)
