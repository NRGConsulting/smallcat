"""Errors raised by models, their registry and their queries."""

from dataclasses import dataclass


class ModelError(Exception):
    """Base class of the errors raised by the models layer."""


class ModelDefinitionError(ModelError, TypeError):
    """A model class is defined incorrectly (its `Meta` or its field types)."""


class ModelRegistrationError(ModelError, ValueError):
    """A model cannot be registered, or is registered twice."""


class ModelNotRegisteredError(ModelError, LookupError):
    """A model is used before being registered in the current registry."""


class ModelQueryError(ModelError, ValueError):
    """A query is built with an invalid or contradictory combination of calls."""


class ModelWriteNotAllowedError(ModelError, PermissionError):
    """The model does not allow writing with the requested mode."""


class ModelUnsupportedWriteModeError(ModelError, NotImplementedError):
    """The file format cannot write with the requested mode."""


@dataclass(frozen=True)
class SchemaIssue:
    """One mismatch between a DataFrame and the schema of a model.

    Attributes:
        column: The column concerned.
        problem: What is wrong. It never contains values from the data.
    """

    column: str
    problem: str


class ModelSchemaValidationError(ModelError, ValueError):
    """A DataFrame does not match the schema declared by a model.

    Attributes:
        model_name: Name of the model class.
        issues: Every mismatch found, so they can be fixed in one go.
    """

    def __init__(self, model_name: str, issues: list[SchemaIssue]) -> None:
        """Build the error from the list of mismatches."""
        self.model_name = model_name
        self.issues = issues
        lines = "\n".join(f"  - {i.column}: {i.problem}" for i in issues)
        super().__init__(f"{model_name} does not match its schema:\n{lines}")
