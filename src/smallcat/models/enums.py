"""Enums that configure how a model reads and writes."""

import contextlib
from enum import StrEnum


class ModelWriteMode(StrEnum):
    """What a write does with the data that already exists.

    Attributes:
        APPEND: Add the rows to the existing data. Only formats that can append
            support it (Delta tables).
        OVERWRITE: Replace the existing data entirely.
        MERGE: Insert the rows whose key is new and update the ones whose key
            already exists, by `merge_keys`. Nothing is deleted. Only Delta
            tables support it.
    """

    APPEND = "append"
    OVERWRITE = "overwrite"
    MERGE = "merge"


class ModelFileFormat(StrEnum):
    """The file format a model's data is stored in.

    Attributes:
        CSV: Comma-separated values.
        EXCEL: An `.xlsx` workbook.
        PARQUET: Parquet, a single file or a directory.
        DELTA_TABLE: A Delta Lake table.
    """

    CSV = "csv"
    EXCEL = "excel"
    PARQUET = "parquet"
    DELTA_TABLE = "delta_table"


class ModelValidationMode(StrEnum):
    """How strictly data must match the types declared by a model.

    Attributes:
        STRICT: A column whose type is not the declared kind is an error.
        COERCE: Columns are cast to the declared types; it is an error only if
            a value cannot be cast without loss.
    """

    STRICT = "strict"
    COERCE = "coerce"


def parse_enum[E: StrEnum](enum: type[E], value: object, what: str) -> E:
    """Convert `value` to a member of `enum`, with a message that lists them.

    Args:
        enum: The enum class.
        value: A member or its string value.
        what: What is being parsed, for the message.

    Returns:
        The enum member.

    Raises:
        ValueError: If `value` is not a member of `enum`.
    """
    if isinstance(value, str):
        with contextlib.suppress(ValueError):
            return enum(value)
    valid = ", ".join(repr(m.value) for m in enum)
    msg = f"{value!r} is not a valid {what}; expected one of: {valid}"
    raise ValueError(msg)
