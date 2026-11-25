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
:mod:`smallcat.models.query`. An instance writes itself with `.save()` (a merge
by `Meta.merge_keys` by default, so Delta only). Which connection a model uses
is decided when it is registered (see :mod:`smallcat.models.registry`), not
here.

Field values are validated by Pydantic whenever an instance is built. DataFrames
are validated for columns, types and nulls only (see
:mod:`smallcat.models.schema`); `Meta.validation_mode` sets how strictly. Column
names are the field names: aliases are not used.

A `NaN` given to a field that admits `None` (`X | None`) is stored as `None`:
that is how pandas hands over empty cells. Set `nan_as_none = False` on a model
to keep the `NaN`. A field that does not admit `None` is never changed.
"""

import math
import types
import typing
from collections.abc import Sequence
from typing import Any, ClassVar

from pydantic import BaseModel, model_validator

from smallcat.models.enums import ModelWriteMode
from smallcat.models.errors import ModelDefinitionError
from smallcat.models.formats import unsupported_fields
from smallcat.models.meta import ModelMeta, parse_meta
from smallcat.models.query import ManagerDescriptor, ModelManager
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


def _admits_none(annotation: object) -> bool:
    """Return whether an annotation is `X | None` (or `Optional[X]`)."""
    if typing.get_origin(annotation) in (typing.Union, types.UnionType):
        return type(None) in typing.get_args(annotation)
    return False


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

    objects: ClassVar[ManagerDescriptor] = ManagerDescriptor()
    """The manager that reads and writes the model's data. Class access only.

    `objects` is reserved: a subclass can only replace it with a
    :class:`ModelManager` (see there to add methods of your own).
    """

    nan_as_none: ClassVar[bool] = True
    """Store a `NaN` given to an `X | None` field as `None` (default `True`)."""

    __smallcat_meta__: ClassVar[ModelMeta | None] = None
    __smallcat_schema__: ClassVar[ModelSchema | None] = None

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:  # noqa: ANN401
        """Validate `Meta` and derive the schema when a model is defined."""
        super().__pydantic_init_subclass__(**kwargs)
        declared = cls.__dict__.get("objects", None)
        if "objects" in cls.model_fields or (
            declared is not None
            and not isinstance(declared, ModelManager | ManagerDescriptor)
        ):
            msg = (
                f"'objects' is a reserved name on models: in {cls.__name__} it "
                "must be a ModelManager instance (e.g. objects = "
                f"CustomerManager()), got {type(declared).__name__}"
            )
            raise ModelDefinitionError(msg)
        if "save" in cls.model_fields:
            msg = (
                f"'save' is a reserved name on models: {cls.__name__} cannot "
                "have a field called 'save' (it is the method that writes the "
                "instance)"
            )
            raise ModelDefinitionError(msg)
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
                f"{cls.__name__} has fields its file format cannot hold:\n  - {details}"
            )
            raise ModelDefinitionError(msg)
        cls.__smallcat_meta__ = meta
        cls.__smallcat_schema__ = schema

    def save(
        self,
        *,
        mode: ModelWriteMode | str = ModelWriteMode.MERGE,
        merge_keys: Sequence[str] | None = None,
    ) -> None:
        """Validate this instance and write it as one row.

        Shortcut for ``type(self).objects.save(self, mode=..., merge_keys=...)``.
        By default it merges: the row is inserted if its key is new and updates
        the row with the same key otherwise, by ``Meta.merge_keys`` (or
        ``merge_keys``). Every column is written, so a field left at its default
        overwrites the stored value.

        Only Delta tables can merge: on any other format the default mode
        raises :class:`ModelUnsupportedWriteModeError` and nothing is written.
        Pass ``mode="overwrite"`` explicitly to replace the whole dataset with
        this row.

        Each call is one write (one Delta commit). To save many rows use
        ``Model.objects.save([...], mode=...)``.

        Args:
            mode: A :class:`ModelWriteMode` or its value. Default ``merge``.
            merge_keys: Fields that identify the row, with ``mode="merge"``.
                By default ``Meta.merge_keys``.

        Raises:
            ValueError: If `mode` is not a :class:`ModelWriteMode`.
            ModelNotRegisteredError: If the model is not registered.
            ModelQueryError: If ``merge`` has no keys (neither in the call nor in
                ``Meta``), a key is not a field of the model, or `merge_keys`
                is given with a mode other than ``merge``.
            ModelWriteNotAllowedError: If the model does not allow the mode.
            ModelUnsupportedWriteModeError: If the file format cannot write
                with the mode (``merge`` on CSV, Parquet or Excel).
            ModelSchemaValidationError: If the row does not match the model.
        """
        type(self).objects.save(self, mode=mode, merge_keys=merge_keys)

    @model_validator(mode="before")
    @classmethod
    def _nan_to_none(cls, data: object) -> object:
        """Turn `NaN` into `None` in the fields that admit `None`.

        Empty cells come from pandas as `NaN`. Fields that do not admit `None`
        keep their value: `NaN` is a valid `float`.
        """
        if not cls.nan_as_none or not isinstance(data, dict):
            return data
        fixed: dict[Any, Any] | None = None
        for name, info in cls.model_fields.items():
            value = data.get(name)
            if (
                isinstance(value, float)
                and math.isnan(value)
                and _admits_none(info.annotation)
            ):
                if fixed is None:
                    fixed = dict(data)
                fixed[name] = None
        return data if fixed is None else fixed
