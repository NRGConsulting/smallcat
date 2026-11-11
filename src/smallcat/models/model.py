"""The `Model` base class.

A model is a Pydantic class whose fields describe the columns of a dataset and
whose `Meta` says where that dataset lives and how it is stored:

    class Customer(Model):
        id: int
        name: str
        email: str | None = None

        class Meta:
            location = "silver/customers"
            file_format = "parquet"

Reading and writing go through `Customer.objects`; see
:mod:`smallcat.models.query`. Which connection a model uses is decided when it
is registered (see :mod:`smallcat.models.registry`), not here.

Field values are validated by Pydantic whenever an instance is built. DataFrames
are validated for columns, types and nulls only (see
:mod:`smallcat.models.schema`); `Meta.validation_mode` sets how strictly. Column
names are the field names: aliases are not used.
"""

from typing import Any, ClassVar

from pydantic import BaseModel

from smallcat.models.errors import ModelDefinitionError
from smallcat.models.formats import unsupported_fields
from smallcat.models.meta import ModelMeta, parse_meta
from smallcat.models.query import ManagerDescriptor
from smallcat.models.schema import (
    ExtraPolicy,
    FieldSpec,
    ModelSchema,
    UnsupportedFieldTypeError,
    dtype_of,
)


def _extra_policy(model: "type[Model]") -> ExtraPolicy:
    """Read how the model treats columns it does not declare."""
    extra = model.model_config.get("extra")
    return extra if extra in ("ignore", "allow") else "forbid"  # type: ignore[return-value]


def _build_schema(model: "type[Model]") -> ModelSchema:
    """Derive the expected Polars schema from the model's fields."""
    fields: dict[str, FieldSpec] = {}
    problems = []
    for name, info in model.model_fields.items():
        try:
            dtype, nullable = dtype_of(info.annotation)
        except UnsupportedFieldTypeError as e:
            problems.append(f"{name}: {e}")
            continue
        fields[name] = FieldSpec(name, dtype, nullable)
    if problems:
        details = "\n  - ".join(problems)
        msg = f"{model.__name__} has fields that cannot be stored:\n  - {details}"
        raise ModelDefinitionError(msg)
    return ModelSchema(model.__name__, fields, _extra_policy(model))


class Model(BaseModel):
    """Base class of the models.

    A subclass with a `Meta` is a model whose data can be read and written. A
    subclass without one is abstract (a base for other models).
    """

    objects: ClassVar[Any] = ManagerDescriptor()
    """The manager that reads and writes the model's data. Class access only."""

    __smallcat_meta__: ClassVar[ModelMeta | None] = None
    __smallcat_schema__: ClassVar[ModelSchema | None] = None

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:  # noqa: ANN401
        """Validate `Meta` and derive the schema when a model is defined."""
        super().__pydantic_init_subclass__(**kwargs)
        meta_class = getattr(cls, "Meta", None)
        if meta_class is None:
            return
        meta = parse_meta(cls.__name__, meta_class)
        schema = _build_schema(cls)
        problems = unsupported_fields(
            schema,
            meta.file_format,
            writable=bool(meta.allowed_write_modes),
        )
        if problems:
            details = "\n  - ".join(problems)
            msg = (
                f"{cls.__name__} has fields its file format cannot hold:"
                f"\n  - {details}"
            )
            raise ModelDefinitionError(msg)
        cls.__smallcat_meta__ = meta
        cls.__smallcat_schema__ = schema
