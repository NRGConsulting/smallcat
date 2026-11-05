"""Registry that binds models to connections.

A model says what the data is; the registry says where it is read from and
written to, by naming a connection that was loaded beforehand. Keeping the two
apart lets each environment (dev, test, prod) register the same models against
its own connections, and lets it take permissions away.

    smallcat.registry.load_connections(Connections.from_yaml("connections.yaml"))
    smallcat.registry.register(Customer, connection="datalake_gcs")

    Customer.objects.all()

`Customer.objects` uses the *current* registry, which is
:data:`default_registry` unless :func:`use_registry` has replaced it, as tests
do to run against a registry of their own.
"""

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from smallcat.connections_config import Connections
from smallcat.models.enums import ModelWriteMode, parse_enum
from smallcat.models.errors import ModelNotRegisteredError, ModelRegistrationError
from smallcat.models.meta import ModelMeta, build_entry

if TYPE_CHECKING:
    from smallcat.models.model import Model


@dataclass(frozen=True)
class Registration:
    """A model bound to a connection.

    Attributes:
        model: The model class.
        connection: Name of the connection it reads from and writes to.
        allowed_write_modes: The write modes in force for this registration:
            never more than the model itself permits.
    """

    model: "type[Model]"
    connection: str
    allowed_write_modes: frozenset[ModelWriteMode]


class ModelRegistry:
    """Registers models against loaded connections."""

    def __init__(self, connections: Connections | None = None) -> None:
        """Create a registry.

        Args:
            connections: Connections to start with. More can be added with
                :meth:`load_connections`.
        """
        self._connections = Connections()
        if connections is not None:
            self._connections.update(connections)
        self._registrations: dict[type, Registration] = {}

    @property
    def connections(self) -> Connections:
        """The connections loaded in this registry."""
        return self._connections

    def load_connections(self, connections: Connections) -> None:
        """Add connections that models can then be registered against.

        Args:
            connections: The connections, already validated by their loader.

        Raises:
            DuplicateConnectionError: If a name is already loaded.
        """
        self._connections.update(connections)

    def register(
        self,
        model: "type[Model]",
        *,
        connection: str,
        allowed_write_modes: Iterable[ModelWriteMode | str] | None = None,
        replace: bool = False,
    ) -> Registration:
        """Register a model against a loaded connection.

        Args:
            model: The model class.
            connection: Name of a connection already loaded in this registry.
            allowed_write_modes: Restricts the write modes for this
                registration. It can only narrow what the model's `Meta`
                allows; `[]` makes it read-only here. By default the model's
                own modes apply.
            replace: Allow replacing an existing registration of the model.

        Returns:
            The registration.

        Raises:
            ModelRegistrationError: If the model has no `Meta`, is already
                registered, or `allowed_write_modes` asks for a mode the model
                does not allow.
            ConnectionNotLoadedError: If the connection is not loaded.
        """
        meta: ModelMeta | None = getattr(model, "__smallcat_meta__", None)
        if meta is None:
            msg = f"{model.__name__} has no Meta: it cannot be registered"
            raise ModelRegistrationError(msg)
        self._connections.require(connection)
        if model in self._registrations and not replace:
            msg = (
                f"{model.__name__} is already registered "
                "(pass replace=True to replace it)"
            )
            raise ModelRegistrationError(msg)

        allowed = meta.allowed_write_modes
        if allowed_write_modes is not None:
            requested = frozenset(
                parse_enum(ModelWriteMode, m, "write mode") for m in allowed_write_modes
            )
            widened = sorted(m.value for m in requested - meta.allowed_write_modes)
            if widened:
                msg = (
                    f"{model.__name__}.Meta does not allow {widened}: registering "
                    "can only restrict write modes, not add them"
                )
                raise ModelRegistrationError(msg)
            allowed = requested

        registration = Registration(model, connection, allowed)
        self._registrations[model] = registration
        return registration

    def unregister(self, model: "type[Model]") -> None:
        """Remove a model's registration, if it has one."""
        self._registrations.pop(model, None)

    def get(self, model: "type[Model]") -> Registration:
        """Return a model's registration.

        Raises:
            ModelNotRegisteredError: If the model is not registered here.
        """
        try:
            return self._registrations[model]
        except KeyError:
            msg = (
                f"{model.__name__} is not registered. Register it first: "
                f"registry.register({model.__name__}, connection='<name>')"
            )
            raise ModelNotRegisteredError(msg) from None

    def entry_for(
        self,
        model: "type[Model]",
        write_mode: ModelWriteMode | None = None,
    ) -> Any:  # noqa: ANN401
        """Build the catalog entry that reads or writes a registered model.

        Args:
            model: The model class.
            write_mode: The mode of the write about to happen. Delta tables take
                it as their `mode`; other formats need nothing.

        Returns:
            A catalog entry bound to this registry's connections.
        """
        registration = self.get(model)
        meta: ModelMeta = model.__smallcat_meta__  # type: ignore[assignment]
        save_options = None
        if write_mode is not None and meta.file_format == "delta_table":
            save_options = {**(meta.save_options or {}), "mode": write_mode.value}
        entry = build_entry(meta, registration.connection, save_options)
        entry.bind_connections(self._connections)
        return entry

    def __contains__(self, model: object) -> bool:
        """Return whether a model is registered here."""
        return model in self._registrations

    def __iter__(self) -> Iterator["type[Model]"]:
        """Iterate over the registered models."""
        return iter(self._registrations)

    def __len__(self) -> int:
        """Return the number of registered models."""
        return len(self._registrations)


default_registry = ModelRegistry()
"""The registry used when none has been set with :func:`use_registry`."""

_current: ContextVar[ModelRegistry] = ContextVar(
    "smallcat_current_registry",
    default=default_registry,
)


def current_registry() -> ModelRegistry:
    """Return the registry that `Model.objects` is using right now."""
    return _current.get()


@contextmanager
def use_registry(registry: ModelRegistry) -> Iterator[ModelRegistry]:
    """Make `Model.objects` use `registry` inside the `with` block.

    The previous registry is restored on exit, even if the block raises. It is
    kept per context, so threads and async tasks do not affect each other.

    Args:
        registry: The registry to use.

    Yields:
        The same registry.
    """
    token = _current.set(registry)
    try:
        yield registry
    finally:
        _current.reset(token)
