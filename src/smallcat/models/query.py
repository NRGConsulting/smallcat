"""Reading and writing a model's data: the manager and the query it builds.

`Model.objects` returns a :class:`ModelManager`. Its filtering methods return a
:class:`ModelQuery`, which is immutable and reads nothing until it is run:

    Customer.objects.filter(pl.col("id") > 10).select("id", "name").all()
    Customer.objects.filter(pl.col("id") > 10).lazy().group_by("name").len()
    Customer.objects.where("id > 10").all(as_models=True)
    Customer.objects.where(id__gt=10, country="ES").all()

There are two ways to filter and they cannot be combined in one query:
`filter()` takes Polars expressions and `where()` takes an SQL predicate
(DuckDB), written by hand (`sql=`) or as Django-style lookups (`id__gt=10`).
`select()` combines with either.
"""

import copy
import warnings
from collections.abc import Sequence
from dataclasses import dataclass, replace
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    ForwardRef,
    Generic,
    Literal,
    NoReturn,
    Self,
    get_args,
    get_origin,
    overload,
)

import polars as pl
from typing_extensions import TypeVar

from smallcat.models.enums import ModelValidationMode, ModelWriteMode, parse_enum
from smallcat.models.errors import (
    ModelDefinitionError,
    ModelQueryError,
    ModelUnsupportedWriteModeError,
    ModelWriteNotAllowedError,
)
from smallcat.models.lookups import lookups_to_sql
from smallcat.models.meta import ModelMeta, unsupported_reason
from smallcat.models.registry import current_registry
from smallcat.models.schema import (
    ModelSchema,
    check_nulls,
    prepare_lazy,
    validate_frame,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from smallcat.models.model import Model

_MIXED = (
    "filter() takes Polars expressions and where() takes an SQL predicate: "
    "a query can use only one of them"
)


@dataclass(frozen=True)
class _Context:
    """Everything needed to run a query for a registered model."""

    meta: ModelMeta
    spec: ModelSchema
    entry: Any

    @property
    def mode(self) -> ModelValidationMode:
        return self.meta.validation_mode


def _context(model: "type[Model]") -> _Context:
    registry = current_registry()
    meta: ModelMeta = model.__smallcat_meta__  # type: ignore[assignment]
    spec: ModelSchema = model.__smallcat_schema__  # type: ignore[assignment]
    return _Context(meta, spec, registry.entry_for(model))


@dataclass(frozen=True)
class ModelQuery[M: "Model"]:
    """A description of what to read from a model, run when asked for a result.

    Attributes:
        model: The model class.
        filters: Polars predicates, combined with AND.
        where_sql: An SQL predicate (DuckDB), alternative to `filters`.
        columns: The columns to read, or `None` for all of them.
    """

    model: "type[M]"
    filters: tuple[pl.Expr, ...] = ()
    where_sql: str | None = None
    columns: tuple[str, ...] | None = None

    # ---------- Building ----------

    def filter(self, *predicates: pl.Expr) -> Self:
        """Keep the rows that satisfy every Polars predicate.

        Args:
            *predicates: Polars expressions, e.g. `pl.col("id") > 10`.

        Returns:
            A new query.

        Raises:
            ModelQueryError: If no predicate is given, one is not a Polars
                expression, or the query already uses `where()`.
        """
        if not predicates:
            msg = "filter() needs at least one Polars expression"
            raise ModelQueryError(msg)
        for predicate in predicates:
            if not isinstance(predicate, pl.Expr):
                msg = (
                    "filter() takes Polars expressions such as pl.col('id') > 10, "
                    f"not {type(predicate).__name__}; use where() for an SQL string"
                )
                raise ModelQueryError(msg)
        if self.where_sql is not None:
            raise ModelQueryError(_MIXED)
        return replace(self, filters=(*self.filters, *predicates))

    def where(self, sql: str | None = None, **lookups: Any) -> Self:  # noqa: ANN401
        """Keep the rows that satisfy an SQL predicate and/or lookups.

        Lookups are Django-style: ``field=value`` or ``field__operator=value``
        (see :mod:`smallcat.models.lookups` for the operators). They are checked
        against the model's fields and types right here. Several conditions are
        joined with AND, and so are `sql` and the lookups when both are given.

        Args:
            sql: An SQL predicate (DuckDB), e.g. `"id > 10 OR name = 'a'"`. It
                can also be passed as the first positional argument. To filter
                a field that is itself called `sql`, write `sql__exact=...`.
            **lookups: The lookups, e.g. `job_id__lte=2, country="ES"`.

        Returns:
            A new query.

        Raises:
            ModelQueryError: If there is neither an SQL predicate nor lookups,
                the predicate is empty, a lookup is not valid for the model,
                `where()` was already used, or the query already uses
                `filter()`.
        """
        if sql is None and not lookups:
            msg = "where() takes an SQL predicate (sql='...') or lookups (id__gt=1)"
            raise ModelQueryError(msg)
        if sql is not None and (not isinstance(sql, str) or not sql.strip()):
            msg = "where() takes a non-empty SQL predicate string"
            raise ModelQueryError(msg)
        if self.filters:
            raise ModelQueryError(_MIXED)
        if self.where_sql is not None:
            msg = (
                "where() was already used: put every condition in one call "
                "(joined with AND)"
            )
            raise ModelQueryError(msg)
        if not lookups:
            return replace(self, where_sql=sql)
        spec: ModelSchema = self.model.__smallcat_schema__  # type: ignore[assignment]
        rendered = lookups_to_sql(spec, lookups)
        combined = rendered if sql is None else f"({sql}) AND ({rendered})"
        return replace(self, where_sql=combined)

    def select(self, *names: str) -> Self:
        """Read only some of the model's fields.

        Args:
            *names: Field names.

        Returns:
            A new query.

        Raises:
            ModelQueryError: If a name is not a field of the model, is repeated,
                or `select()` was already used.
        """
        if not names:
            msg = "select() needs at least one field name"
            raise ModelQueryError(msg)
        if self.columns is not None:
            msg = "select() was already used"
            raise ModelQueryError(msg)
        unknown = [n for n in names if n not in self.model.model_fields]
        if unknown:
            msg = (
                f"select(): {unknown} are not fields of {self.model.__name__} "
                f"(fields: {list(self.model.model_fields)})"
            )
            raise ModelQueryError(msg)
        if len(set(names)) != len(names):
            msg = "select(): a field is repeated"
            raise ModelQueryError(msg)
        return replace(self, columns=tuple(names))

    # ---------- Running ----------

    def lazy(self) -> pl.LazyFrame:
        """Return the query as a Polars LazyFrame.

        Parquet and Delta are scanned natively, so filters and columns are
        pushed down to the storage. Other formats are read in full. A query
        with `where()` runs its SQL now and wraps the result.

        Only the schema (columns and types) is checked here, since no data has
        been read. Two checks need the data and are *not* made on a frame you
        collect yourself: nulls in fields that are not optional (use `all()`
        for that), and in `COERCE` mode a value that cannot be cast, which then
        fails with a Polars error when the frame is collected.

        Returns:
            The LazyFrame, validated against the model's schema.
        """
        if self.where_sql is not None:
            return self.all().lazy()
        return self._plan(_context(self.model))[0]

    @overload
    def all(self, *, as_models: Literal[False] = False) -> pl.DataFrame: ...

    @overload
    def all(self, *, as_models: Literal[True]) -> "list[M]": ...

    @overload
    def all(self, *, as_models: bool) -> "pl.DataFrame | list[M]": ...

    def all(self, *, as_models: bool = False) -> "pl.DataFrame | list[M]":
        """Run the query and return a DataFrame, or model instances.

        Args:
            as_models: If true, return one instance of the model per row
                instead of a DataFrame. Each row is validated by Pydantic, so
                field constraints apply.

        Returns:
            The rows as a DataFrame with the model's columns in its order, or
            as a list of instances when `as_models` is true.

        Raises:
            ModelQueryError: If `as_models` is true and the query uses
                `select()`, since instances need every required field.
            ModelSchemaValidationError: If the data does not match the model.
        """
        if not as_models:
            return self._frame()
        if self.columns is not None:
            msg = "as_models() needs every field: remove select()"
            raise ModelQueryError(msg)
        rows = self._frame().iter_rows(named=True)
        return [self.model.model_validate(row) for row in rows]

    def _frame(self) -> pl.DataFrame:
        """Run the query and return the validated DataFrame."""
        ctx = _context(self.model)
        selected = list(self.columns) if self.columns else None
        if self.where_sql is not None:
            df = ctx.entry.load_polars(where=self.where_sql, columns=selected)
            return validate_frame(df, ctx.spec, ctx.mode, selected)

        plan, raw = self._plan(ctx)
        try:
            df = plan.collect()
        except (pl.exceptions.InvalidOperationError, pl.exceptions.ComputeError):
            if ctx.mode is ModelValidationMode.COERCE:
                # Find out which column and how many values, from the raw data.
                raw_df = (raw.select(selected) if selected else raw).collect()
                validate_frame(raw_df, ctx.spec, ctx.mode, selected)
            raise
        check_nulls(df, ctx.spec, selected)
        return df

    def as_models(self) -> "list[M]":
        """Run the query and return instances of the model.

        Shortcut for ``all(as_models=True)``.

        Returns:
            One instance per row.

        Raises:
            ModelQueryError: If the query uses `select()`, since instances
                need every required field.
        """
        return self.all(as_models=True)

    def _plan(self, ctx: _Context) -> tuple[pl.LazyFrame, pl.LazyFrame]:
        """Build the lazy plan and return it with the unfiltered scan."""
        selected = list(self.columns) if self.columns else None
        raw = ctx.entry.scan_polars()
        prepared = prepare_lazy(raw, ctx.spec, ctx.mode, selected)
        frame = prepared.frame
        if self.filters:
            frame = frame.filter(*self.filters)
        return frame.select(prepared.select), raw


def _plain(value: object) -> Any:  # noqa: ANN401
    """Replace the `Enum` members in a dumped value by their values."""
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_plain(v) for v in value]
    return value


def _to_frame(model: "type[Model]", data: object) -> pl.DataFrame:
    """Turn what `save` receives into a DataFrame."""
    if isinstance(data, pl.DataFrame):
        return data
    if isinstance(data, pl.LazyFrame):
        msg = "save() takes a DataFrame: call .collect() on the LazyFrame first"
        raise TypeError(msg)
    items = [data] if isinstance(data, model) else data
    try:
        items = list(items)  # type: ignore[call-overload]
    except TypeError:
        msg = (
            f"save() takes a polars DataFrame, a {model.__name__} or a list of "
            f"{model.__name__}, not {type(data).__name__}"
        )
        raise TypeError(msg) from None
    wrong = sorted({type(i).__name__ for i in items if not isinstance(i, model)})
    if wrong:
        msg = f"save() expected {model.__name__} instances, found {wrong}"
        raise TypeError(msg)
    spec: ModelSchema = model.__smallcat_schema__  # type: ignore[assignment]
    rows = [_plain(i.model_dump()) for i in items]
    return pl.DataFrame(rows, schema=spec.polars_schema())


def _merge_keys_for(
    model: "type[Model]",
    meta: ModelMeta,
    spec: ModelSchema,
    mode: ModelWriteMode,
    call_keys: Sequence[str] | None,
) -> tuple[str, ...] | None:
    """Resolve the keys of a write: those of the call, else those of `Meta`."""
    if mode is not ModelWriteMode.MERGE:
        if call_keys is not None:
            msg = (
                f"merge_keys only applies to mode='merge', not '{mode.value}': "
                "it would be ignored"
            )
            raise ModelQueryError(msg)
        return None
    if isinstance(call_keys, str):
        msg = "merge_keys must be a list of field names, not a string"
        raise ModelQueryError(msg)
    keys = tuple(call_keys) if call_keys is not None else meta.merge_keys
    if not keys:
        msg = (
            f"{model.__name__} has no merge keys: pass "
            "save(..., mode='merge', merge_keys=[...]) or set Meta.merge_keys"
        )
        raise ModelQueryError(msg)
    if len(set(keys)) != len(keys):
        msg = f"merge_keys has repeated fields: {list(keys)}"
        raise ModelQueryError(msg)
    unknown = [k for k in keys if k not in spec.fields]
    if unknown:
        msg = (
            f"merge_keys {unknown} are not fields of {model.__name__} "
            f"(fields: {list(spec.fields)})"
        )
        raise ModelQueryError(msg)
    return keys


def _declared_models(manager_cls: type) -> list[Any]:
    """The model arguments written as `ModelManager[...]` in a manager's bases."""
    found: list[Any] = []
    for klass in manager_cls.__mro__:
        for base in getattr(klass, "__orig_bases__", ()):
            origin = get_origin(base)
            if isinstance(origin, type) and issubclass(origin, ModelManager):
                found.extend(a for a in get_args(base) if not isinstance(a, TypeVar))
    return found


M = TypeVar("M", bound="Model", default="Model")


class ModelManager(Generic[M]):
    """The entry point to a model's data: `Model.objects`.

    Subclass it to add methods of your own and declare it on the model:

        class CustomerManager(ModelManager["Customer"]):
            def get_by_id(self, id: int) -> "Customer | None":
                rows = self.where(id=id).all(as_models=True)
                return rows[0] if rows else None

        class Customer(Model):
            objects = CustomerManager()

    The manager is bound to the model that declares it (and to its subclasses)
    when it is accessed. The ``["Customer"]`` is optional: it only types the
    results (``list[Customer]`` instead of ``list[Model]``) and, when given,
    must name the model that uses the manager.
    """

    def __init__(self, model: "type[M] | None" = None) -> None:
        """Create a manager, bound to `model` or free to be declared on one."""
        self._model = model

    @property
    def model(self) -> "type[M]":
        """The model this manager reads and writes."""
        if self._model is None:
            msg = (
                f"{type(self).__name__} is not bound to a model: declare it as "
                "'objects' in a Model subclass"
            )
            raise ModelDefinitionError(msg)
        return self._model

    def __set_name__(self, owner: type, name: str) -> None:
        """Check, when declared, that the manager is typed for this model."""
        if name != "objects":
            msg = (
                f"A manager must be declared as 'objects', not '{name}' "
                f"(in {owner.__name__})"
            )
            raise ModelDefinitionError(msg)
        for arg in _declared_models(type(self)):
            if isinstance(arg, type):
                ok = issubclass(owner, arg)
                declared = arg.__name__
            else:
                declared = arg.__forward_arg__ if isinstance(arg, ForwardRef) else arg
                short = str(declared).rsplit(".", 1)[-1]
                ok = any(k.__name__ == short for k in owner.__mro__)
            if not ok:
                self._mismatch(owner, str(declared))

    def _mismatch(self, owner: type, declared: str) -> NoReturn:
        msg = (
            f"{type(self).__name__} is typed for '{declared}' but is used in "
            f"'{owner.__name__}' (ModelManager[\"{declared}\"] must match the "
            "model that declares it)"
        )
        raise ModelDefinitionError(msg)

    @overload
    def __get__(self, instance: None, owner: type) -> Self: ...

    @overload
    def __get__(self, instance: object, owner: type) -> NoReturn: ...

    def __get__(self, instance: object, owner: type) -> Self:
        """Return the manager bound to `owner`; instances do not have one."""
        if instance is not None:
            msg = "'objects' is only available on the model class, not on instances"
            raise AttributeError(msg)
        if self._model is owner:
            return self
        bound = copy.copy(self)
        bound._model = owner  # noqa: SLF001
        return bound

    def _query(self) -> ModelQuery[M]:
        return ModelQuery(self.model)

    @overload
    def all(self, *, as_models: Literal[False] = False) -> pl.DataFrame: ...

    @overload
    def all(self, *, as_models: Literal[True]) -> list[M]: ...

    @overload
    def all(self, *, as_models: bool) -> pl.DataFrame | list[M]: ...

    def all(self, *, as_models: bool = False) -> pl.DataFrame | list[M]:
        """Read every row. See :meth:`ModelQuery.all`."""
        return self._query().all(as_models=as_models)

    def filter(self, *predicates: pl.Expr) -> ModelQuery[M]:
        """Start a query with Polars predicates. See :meth:`ModelQuery.filter`."""
        return self._query().filter(*predicates)

    def where(self, sql: str | None = None, **lookups: Any) -> ModelQuery[M]:  # noqa: ANN401
        """Start a query with SQL and/or lookups. See :meth:`ModelQuery.where`."""
        return self._query().where(sql, **lookups)

    def select(self, *names: str) -> ModelQuery[M]:
        """Start a query on some fields. See :meth:`ModelQuery.select`."""
        return self._query().select(*names)

    def lazy(self) -> pl.LazyFrame:
        """Read lazily. See :meth:`ModelQuery.lazy`."""
        return self._query().lazy()

    def as_models(self) -> list[M]:
        """Read every row as model instances. See :meth:`ModelQuery.as_models`."""
        return self._query().all(as_models=True)

    def create_table(self) -> bool:
        """Create the model's dataset, empty, if it does not exist yet.

        Reading a model whose table or file has not been written yet fails.
        This creates it with no rows and the model's schema (a Delta table, or
        a CSV, Parquet or Excel file). Nothing is done if it already exists.

        A model that allows no write mode (read-only) is not created: a
        warning is emitted instead of an error.

        Returns:
            True if it was created; False if it already existed or the model
            is read-only.
        """
        model = self.model
        registry = current_registry()
        if not registry.get(model).allowed_write_modes:
            msg = f"{model.__name__} is read-only: its dataset was not created"
            warnings.warn(msg, UserWarning, stacklevel=2)
            return False
        spec: ModelSchema = model.__smallcat_schema__  # type: ignore[assignment]
        entry = registry.entry_for(model)
        if entry.exists():
            return False
        schema = pl.DataFrame(schema=spec.polars_schema()).to_arrow().schema
        entry.create_empty(schema)
        return True

    def save(
        self,
        data: "pl.DataFrame | M | Iterable[M]",
        *,
        mode: ModelWriteMode | str,
        merge_keys: Sequence[str] | None = None,
    ) -> None:
        """Validate data against the model and write it.

        The mode is required because it decides what happens to the data that
        already exists:

        * ``overwrite`` replaces the dataset entirely.
        * ``append`` adds the rows (only Delta tables can).
        * ``merge`` inserts the rows with a new key and updates the ones whose
          key exists, by ``merge_keys`` (only Delta tables can). It never
          deletes rows. The keys of the call replace those of ``Meta``.

        Args:
            data: A DataFrame, an instance of the model or a list of them.
            mode: A :class:`ModelWriteMode` or its value.
            merge_keys: Fields that identify a row, with ``mode="merge"``.
                By default ``Meta.merge_keys``.

        Raises:
            ValueError: If `mode` is not a :class:`ModelWriteMode`.
            ModelQueryError: If `merge_keys` is given with a mode other than
                ``merge``, if ``merge`` has no keys (neither in the call nor in
                ``Meta``) or a key is not a field of the model.
            ModelWriteNotAllowedError: If the model (or its registration) does
                not allow this mode; read-only models allow none.
            ModelUnsupportedWriteModeError: If the file format cannot write
                with this mode.
            ModelSchemaValidationError: If the data does not match the model.
            TypeError: If `data` is not a DataFrame or model instances.
        """
        write_mode = parse_enum(ModelWriteMode, mode, "write mode")
        model = self.model
        registry = current_registry()
        registration = registry.get(model)
        meta: ModelMeta = model.__smallcat_meta__  # type: ignore[assignment]
        spec: ModelSchema = model.__smallcat_schema__  # type: ignore[assignment]

        allowed = registration.allowed_write_modes
        if write_mode not in allowed:
            if not allowed:
                msg = f"{model.__name__} is read-only: it allows no write mode"
            else:
                msg = (
                    f"{model.__name__} does not allow '{write_mode.value}' "
                    f"(allowed: {sorted(m.value for m in allowed)})"
                )
            raise ModelWriteNotAllowedError(msg)
        reason = unsupported_reason(meta, write_mode)
        if reason:
            msg = (
                f"{model.__name__} cannot be saved with '{write_mode.value}': {reason}"
            )
            raise ModelUnsupportedWriteModeError(msg)

        keys = _merge_keys_for(model, meta, spec, write_mode, merge_keys)
        frame = validate_frame(_to_frame(model, data), spec, meta.validation_mode)
        registry.entry_for(model, write_mode, keys).save_polars(frame)


class ManagerDescriptor:
    """Gives each model class an `objects` manager (and only the class)."""

    @overload
    def __get__[M: "Model"](
        self,
        instance: None,
        owner: "type[M]",
    ) -> ModelManager[M]: ...

    @overload
    def __get__(self, instance: object, owner: "type[Model]") -> NoReturn: ...

    def __get__(self, instance: object, owner: "type[Model]") -> ModelManager[Any]:
        """Return the manager of `owner`; instances do not have one."""
        if instance is not None:
            msg = "'objects' is only available on the model class, not on instances"
            raise AttributeError(msg)
        return ModelManager(owner)
