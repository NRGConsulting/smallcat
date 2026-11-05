"""Polars schema derived from a model's fields, and DataFrame validation.

The schema answers two questions about a DataFrame: does it have the columns
the model declares, and are their types the declared ones. How strictly the
second question is answered is set by :class:`ModelValidationMode`:

* ``STRICT``: a column must be of the declared *kind* (any integer width for
  ``int``, any time unit for ``datetime``...). Nothing is converted.
* ``COERCE``: columns are cast to the declared type. A value that cannot be cast
  without loss is an error, so a malformed value is never silently changed.

Missing columns, nulls in a field that is not optional, and unexpected columns
(unless the model sets ``extra="ignore"`` or ``"allow"``) are errors in both.
Messages never contain values from the data.
"""

import datetime as dt
import types
import typing
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, get_args, get_origin

import polars as pl

from smallcat.models.enums import ModelValidationMode
from smallcat.models.errors import ModelSchemaValidationError, SchemaIssue

ExtraPolicy = Literal["forbid", "ignore", "allow"]
DType = pl.DataType | pl.datatypes.DataTypeClass

_SIMPLE_TYPES: dict[type, pl.DataType] = {
    int: pl.Int64(),
    float: pl.Float64(),
    str: pl.String(),
    bool: pl.Boolean(),
    bytes: pl.Binary(),
    dt.datetime: pl.Datetime("us"),
    dt.date: pl.Date(),
    dt.time: pl.Time(),
    dt.timedelta: pl.Duration("us"),
}

SUPPORTED_TYPES_HINT = (
    "int, float, str, bool, bytes, datetime, date, time, timedelta, "
    "Literal of str or int, list of those, and any of them as `X | None`"
)


class UnsupportedFieldTypeError(TypeError):
    """A field annotation has no Polars equivalent."""


@dataclass(frozen=True)
class FieldSpec:
    """The expected Polars type of one model field.

    Attributes:
        name: Field (and column) name.
        dtype: The canonical Polars type the field maps to.
        nullable: Whether `None` is allowed (the field is `X | None`).
    """

    name: str
    dtype: pl.DataType
    nullable: bool


@dataclass(frozen=True)
class ModelSchema:
    """The expected columns of a model.

    Attributes:
        model_name: Name of the model class, for messages.
        fields: The fields, in declaration order.
        extra: What to do with columns the model does not declare.
    """

    model_name: str
    fields: dict[str, FieldSpec]
    extra: ExtraPolicy

    def polars_schema(self) -> dict[str, pl.DataType]:
        """Return the canonical Polars type of every field."""
        return {name: spec.dtype for name, spec in self.fields.items()}


def dtype_of(annotation: object) -> tuple[pl.DataType, bool]:
    """Map a Python annotation to its canonical Polars type.

    Args:
        annotation: A field annotation.

    Returns:
        The Polars type and whether the annotation admits `None`.

    Raises:
        UnsupportedFieldTypeError: If the annotation has no Polars equivalent.
    """
    origin = get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        options = get_args(annotation)
        present = [a for a in options if a is not type(None)]
        if len(present) != 1:
            msg = f"unions of several types are not supported ({annotation})"
            raise UnsupportedFieldTypeError(msg)
        dtype, _ = dtype_of(present[0])
        return dtype, True
    if origin is Literal:
        kinds = {type(v) for v in get_args(annotation)}
        if kinds == {str}:
            return pl.String(), False
        if kinds == {int}:
            return pl.Int64(), False
        msg = f"Literal values must be all str or all int ({annotation})"
        raise UnsupportedFieldTypeError(msg)
    if origin is list:
        (inner,) = get_args(annotation) or (None,)
        if inner is None:
            msg = "list needs an item type, e.g. list[int]"
            raise UnsupportedFieldTypeError(msg)
        return pl.List(dtype_of(inner)[0]), False
    if isinstance(annotation, type) and annotation in _SIMPLE_TYPES:
        return _SIMPLE_TYPES[annotation], False
    name = getattr(annotation, "__name__", str(annotation))
    msg = f"type {name!r} is not supported (supported: {SUPPORTED_TYPES_HINT})"
    raise UnsupportedFieldTypeError(msg)


def _is_compatible(actual: DType, expected: DType) -> bool:
    """Return whether `actual` is the kind of type that `expected` declares."""
    if actual == pl.Null:
        return True  # an all-null column carries no type; nulls are checked apart
    if expected.is_integer():
        return actual.is_integer()
    if expected.is_float():
        return actual.is_float()
    if isinstance(expected, pl.List):
        return isinstance(actual, pl.List) and _is_compatible(
            actual.inner,
            expected.inner,
        )
    return actual.base_type() == expected.base_type()


@dataclass
class _Plan:
    """What a schema comparison found and what to do about it."""

    issues: list[SchemaIssue]
    casts: dict[str, pl.DataType]
    select: list[str]


def _analyze(
    actual: pl.Schema,
    spec: ModelSchema,
    mode: ModelValidationMode,
    selected: Sequence[str] | None,
) -> _Plan:
    """Compare a schema with the model: missing, mismatched and extra columns."""
    issues: list[SchemaIssue] = []
    casts: dict[str, pl.DataType] = {}
    wanted = list(selected) if selected else list(spec.fields)

    for name in wanted:
        expected = spec.fields[name].dtype
        if name not in actual:
            issues.append(SchemaIssue(name, "column is missing"))
        elif actual[name] != expected:
            if mode is ModelValidationMode.COERCE:
                casts[name] = expected
            elif not _is_compatible(actual[name], expected):
                issues.append(
                    SchemaIssue(name, f"expected {expected}, got {actual[name]}"),
                )

    select = list(wanted)
    if not selected:
        extras = [c for c in actual.names() if c not in spec.fields]
        if spec.extra == "forbid":
            issues.extend(SchemaIssue(c, "unexpected column") for c in extras)
        elif spec.extra == "allow":
            select.extend(extras)
    return _Plan(issues, casts, select)


def _null_issues(
    df: pl.DataFrame,
    spec: ModelSchema,
    selected: Sequence[str] | None,
) -> list[SchemaIssue]:
    """Report nulls in the columns of fields that are not optional."""
    issues = []
    for name in selected or spec.fields:
        if name in df.columns and not spec.fields[name].nullable:
            nulls = df[name].null_count()
            if nulls:
                issues.append(
                    SchemaIssue(name, f"{nulls} null value(s) in a required field"),
                )
    return issues


def _cast_column(
    df: pl.DataFrame,
    name: str,
    dtype: pl.DataType,
) -> tuple[pl.Series | None, SchemaIssue | None]:
    """Cast a column, or describe why it cannot be cast without loss."""
    column = df[name]
    casted = column.cast(dtype, strict=False)
    failed = int((column.is_not_null() & casted.is_null()).sum())
    if failed:
        return None, SchemaIssue(name, f"{failed} value(s) cannot be cast to {dtype}")
    if column.dtype.is_float() and dtype.is_integer():
        lossy = int((casted.cast(pl.Float64) != column).fill_null(value=False).sum())
        if lossy:
            return None, SchemaIssue(
                name,
                f"{lossy} value(s) would lose their decimals as {dtype}",
            )
    return casted, None


def validate_frame(
    df: pl.DataFrame,
    spec: ModelSchema,
    mode: ModelValidationMode,
    selected: Sequence[str] | None = None,
) -> pl.DataFrame:
    """Validate a DataFrame against a model, coercing it if the mode says so.

    Args:
        df: The data.
        spec: The model's schema.
        mode: How strictly types must match.
        selected: Only these columns are required and checked. `None` means
            every field of the model.

    Returns:
        The DataFrame with its columns in the model's order (cast to the
        declared types in `COERCE` mode, without unexpected columns unless the
        model allows them).

    Raises:
        ModelSchemaValidationError: If it does not match. Every mismatch found
            is reported.
    """
    plan = _analyze(df.schema, spec, mode, selected)
    issues = list(plan.issues)
    out = df
    for name, dtype in plan.casts.items():
        casted, issue = _cast_column(out, name, dtype)
        if issue:
            issues.append(issue)
        elif casted is not None:
            out = out.with_columns(casted.alias(name))
    issues.extend(_null_issues(out, spec, selected))
    if issues:
        raise ModelSchemaValidationError(spec.model_name, issues)
    return out.select(plan.select)


def check_nulls(
    df: pl.DataFrame,
    spec: ModelSchema,
    selected: Sequence[str] | None = None,
) -> None:
    """Raise if a field that is not optional has nulls.

    Lazy plans cannot check this before they run, so it is checked on the
    collected result.

    Raises:
        ModelSchemaValidationError: If there are nulls in a required field.
    """
    issues = _null_issues(df, spec, selected)
    if issues:
        raise ModelSchemaValidationError(spec.model_name, issues)


@dataclass(frozen=True)
class LazyPlan:
    """How to prepare a LazyFrame so that it matches a model.

    Attributes:
        frame: The frame with its columns cast (`COERCE` mode).
        select: The columns to keep, in the model's order.
    """

    frame: pl.LazyFrame
    select: list[str]


def prepare_lazy(
    lf: pl.LazyFrame,
    spec: ModelSchema,
    mode: ModelValidationMode,
    selected: Sequence[str] | None = None,
) -> LazyPlan:
    """Check a LazyFrame's schema against a model and add the casts it needs.

    Only the schema is known before running, so values are not inspected here:
    a cast that fails raises when the plan is collected.

    Raises:
        ModelSchemaValidationError: If a column is missing, unexpected or, in
            `STRICT` mode, of the wrong type.
    """
    plan = _analyze(lf.collect_schema(), spec, mode, selected)
    if plan.issues:
        raise ModelSchemaValidationError(spec.model_name, plan.issues)
    if plan.casts:
        lf = lf.with_columns(
            *(pl.col(name).cast(dtype) for name, dtype in plan.casts.items()),
        )
    return LazyPlan(lf, plan.select)
