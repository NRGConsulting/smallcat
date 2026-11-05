"""Models with a small ORM over the catalog's datasets.

A :class:`Model` declares the columns of a dataset and, in `Meta`, where and how
it is stored. A :class:`ModelRegistry` binds models to connections that were
loaded beforehand, and `Model.objects` reads and writes Polars DataFrames
validated against the model.

Example:
  >>> class Customer(Model):
  ...     id: int
  ...     name: str
  ...
  ...     class Meta:
  ...         location = "silver/customers"
  ...         file_format = "parquet"
  >>> registry.load_connections(Connections.from_yaml("connections.yaml"))
  >>> registry.register(Customer, connection="datalake_gcs")
  >>> Customer.objects.filter(pl.col("id") > 10).all()
  >>> Customer.objects.save(df, mode=ModelWriteMode.OVERWRITE)
"""

from smallcat.models.enums import ModelFileFormat, ModelValidationMode, ModelWriteMode
from smallcat.models.errors import (
    ModelDefinitionError,
    ModelError,
    ModelNotRegisteredError,
    ModelQueryError,
    ModelRegistrationError,
    ModelSchemaValidationError,
    ModelUnsupportedWriteModeError,
    ModelWriteNotAllowedError,
    SchemaIssue,
)
from smallcat.models.model import Model
from smallcat.models.query import ModelManager, ModelQuery
from smallcat.models.registry import (
    ModelRegistry,
    Registration,
    current_registry,
    default_registry,
    use_registry,
)

__all__ = [
    "Model",
    "ModelDefinitionError",
    "ModelError",
    "ModelFileFormat",
    "ModelManager",
    "ModelNotRegisteredError",
    "ModelQuery",
    "ModelQueryError",
    "ModelRegistrationError",
    "ModelRegistry",
    "ModelSchemaValidationError",
    "ModelUnsupportedWriteModeError",
    "ModelValidationMode",
    "ModelWriteMode",
    "ModelWriteNotAllowedError",
    "Registration",
    "SchemaIssue",
    "current_registry",
    "default_registry",
    "use_registry",
]
