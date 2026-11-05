"""The `Meta` class of a model, parsed and validated.

`Meta` describes *what the data is*: where it lives, its format and how it is
read, written and validated. It deliberately has no connection: that is given
when the model is registered, so the same model can run against different
storage in each environment.

    class Customer(Model):
        id: int
        name: str

        class Meta:
            location = "silver/customers"
            file_format = "parquet"
            allowed_write_modes = [ModelWriteMode.OVERWRITE]   # default: all
            validation_mode = ModelValidationMode.STRICT       # default
            load_options = {"hive_partitioning": True}
            save_options = {}

`load_options` and `save_options` are the format options that datasets already
accept in a catalog (`sep`, `header`, `partition_by`, ...) and are validated
against the same schemas.
"""

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError

from smallcat.catalog import CatalogEntry
from smallcat.models.enums import ModelValidationMode, ModelWriteMode, parse_enum
from smallcat.models.errors import ModelDefinitionError

FileFormat = Literal["csv", "excel", "parquet", "delta_table"]

META_ATTRIBUTES = (
    "location",
    "file_format",
    "allowed_write_modes",
    "validation_mode",
    "load_options",
    "save_options",
)

_ENTRY_ADAPTER: TypeAdapter[Any] = TypeAdapter(
    Annotated[CatalogEntry, Field(discriminator="file_format")],
)


class ModelMeta(BaseModel):
    """The validated `Meta` of a model.

    Attributes:
        location: Path of the data, relative to the connection's base.
        file_format: `csv`, `excel`, `parquet` or `delta_table`.
        allowed_write_modes: The write modes the model permits. Empty means the
            model is read-only. By default every mode is allowed.
        validation_mode: How strictly data must match the declared types.
        load_options: Format options applied when reading.
        save_options: Format options applied when writing.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    location: str = Field(min_length=1)
    file_format: FileFormat
    allowed_write_modes: frozenset[ModelWriteMode]
    validation_mode: ModelValidationMode = ModelValidationMode.STRICT
    load_options: dict[str, Any] | None = None
    save_options: dict[str, Any] | None = None


def _describe(prefix: str, error: ValidationError) -> list[str]:
    """Describe a validation error without echoing the input values."""
    lines = []
    for err in error.errors(
        include_input=False,
        include_url=False,
        include_context=False,
    ):
        parts = [str(p) for p in err["loc"]]
        if parts and parts[0] in ("csv", "excel", "parquet", "delta_table"):
            parts.pop(0)  # the format tag of the entry union
        lines.append(f"{'.'.join([prefix, *parts])}: {err['msg']}")
    return lines


def _parse_enums(values: dict[str, Any], problems: list[str]) -> None:
    """Convert the enum-valued attributes in place, reporting what is invalid."""
    modes = values.get("allowed_write_modes")
    try:
        values["allowed_write_modes"] = frozenset(
            parse_enum(ModelWriteMode, m, "write mode")
            for m in (ModelWriteMode if modes is None else modes)
        )
    except (ValueError, TypeError) as e:
        problems.append(f"Meta.allowed_write_modes: {e}")
        values["allowed_write_modes"] = frozenset()
    if "validation_mode" in values:
        try:
            values["validation_mode"] = parse_enum(
                ModelValidationMode,
                values["validation_mode"],
                "validation mode",
            )
        except ValueError as e:
            problems.append(f"Meta.validation_mode: {e}")
            del values["validation_mode"]


def parse_meta(model_name: str, meta_class: type) -> ModelMeta:
    """Read and validate the `Meta` class of a model.

    Args:
        model_name: Name of the model class, for messages.
        meta_class: The `Meta` class.

    Returns:
        The validated meta.

    Raises:
        ModelDefinitionError: If `Meta` has unknown attributes, lacks
            `location` or `file_format`, has an invalid value or invalid
            format options. All problems are reported together.
    """
    values = {
        k: getattr(meta_class, k) for k in dir(meta_class) if not k.startswith("_")
    }
    problems: list[str] = []

    unknown = sorted(set(values) - set(META_ATTRIBUTES))
    if unknown:
        problems.append(
            f"unknown Meta attribute(s) {unknown}; valid ones: {list(META_ATTRIBUTES)}",
        )
    problems.extend(
        f"Meta.{required} is required"
        for required in ("location", "file_format")
        if required not in values
    )
    _parse_enums(values, problems)

    meta = None
    if "location" in values and "file_format" in values:
        try:
            meta = ModelMeta(**{k: values[k] for k in META_ATTRIBUTES if k in values})
        except ValidationError as e:
            problems.extend(_describe("Meta", e))
    if meta is not None:
        problems.extend(_check_options(meta))
    if problems or meta is None:
        details = "\n  - ".join(problems)
        msg = f"Invalid Meta in {model_name}:\n  - {details}"
        raise ModelDefinitionError(msg)
    return meta


def _check_options(meta: ModelMeta) -> list[str]:
    """Validate the format options against the schemas the catalog uses."""
    problems = []
    if meta.file_format == "delta_table" and "mode" in (meta.save_options or {}):
        problems.append(
            "Meta.save_options.mode is not allowed: the write mode is chosen on "
            "each save (`objects.save(df, mode=...)`) and limited by "
            "`allowed_write_modes`",
        )
    try:
        _ENTRY_ADAPTER.validate_python(entry_dict(meta, "meta-check"))
    except ValidationError as e:
        problems.extend(_describe("Meta", e))
    return problems


def entry_dict(
    meta: ModelMeta,
    connection: str,
    save_options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the catalog-entry dictionary that a model stands for.

    Args:
        meta: The model's meta.
        connection: Name of a loaded connection.
        save_options: Replaces the meta's save options when given.

    Returns:
        A dictionary a :class:`~smallcat.catalog.CatalogEntry` accepts.
    """
    return {
        "file_format": meta.file_format,
        "connection": connection,
        "location": meta.location,
        "load_options": meta.load_options,
        "save_options": meta.save_options if save_options is None else save_options,
    }


def build_entry(
    meta: ModelMeta,
    connection: str,
    save_options: dict[str, Any] | None = None,
) -> Any:  # noqa: ANN401
    """Build the catalog entry for a model (see :func:`entry_dict`)."""
    return _ENTRY_ADAPTER.validate_python(entry_dict(meta, connection, save_options))


def unsupported_reason(meta: ModelMeta, mode: ModelWriteMode) -> str | None:
    """Explain why the model's format cannot write with `mode`, if it cannot.

    Args:
        meta: The model's meta.
        mode: The requested write mode.

    Returns:
        The reason, or `None` if the format supports the mode.
    """
    if meta.file_format == "delta_table":
        return None
    if mode is ModelWriteMode.APPEND:
        return f"{meta.file_format} files cannot be appended to; only 'overwrite'"
    if meta.file_format == "parquet" and (meta.save_options or {}).get("partition_by"):
        return (
            "overwriting a partitioned Parquet dataset only adds files and keeps "
            "the old ones, so it would not replace the data; write without "
            "`partition_by`, or use a Delta table"
        )
    return None
