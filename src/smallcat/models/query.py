"""Reading and writing a model's data: the manager and the query it builds.

`Model.objects` returns a :class:`ModelManager`. Its filtering methods return a
:class:`ModelQuery`, which is immutable and reads nothing until it is run:

    Customer.objects.filter(pl.col("id") > 10).select("id", "name").all()
    Customer.objects.filter(pl.col("id") > 10).lazy().group_by("name").len()
    Customer.objects.where("id > 10").as_models()
    Customer.objects.where(id__gt=10, country="ES").all()

There are two ways to filter and they cannot be combined in one query:
`filter()` takes Polars expressions and `where()` takes an SQL predicate
(DuckDB), written by hand (`sql=`) or as Django-style lookups (`id__gt=10`).
`select()` combines with either.
"""

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Self

import polars as pl

from smallcat.models.enums import ModelValidationMode, ModelWriteMode, parse_enum
from smallcat.models.errors import (
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

    def all(self) -> pl.DataFrame:
        """Run the query and return a DataFrame.

        Returns:
            The rows, with the model's columns in its order.

        Raises:
            ModelSchemaValidationError: If the data does not match the model.
        """
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

        Each row is validated by Pydantic, so field constraints apply.

        Returns:
            One instance per row.

        Raises:
            ModelQueryError: If the query uses `select()`, since instances
                need every required field.
        """
        if self.columns is not None:
            msg = "as_models() needs every field: remove select()"
            raise ModelQueryError(msg)
        return [
            self.model.model_validate(row) for row in self.all().iter_rows(named=True)
        ]

    def _plan(self, ctx: _Context) -> tuple[pl.LazyFrame, pl.LazyFrame]:
        """Build the lazy plan and return it with the unfiltered scan."""
        selected = list(self.columns) if self.columns else None
        raw = ctx.entry.scan_polars()
        prepared = prepare_lazy(raw, ctx.spec, ctx.mode, selected)
        frame = prepared.frame
        if self.filters:
            frame = frame.filter(*self.filters)
        return frame.select(prepared.select), raw


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
    return pl.DataFrame([i.model_dump() for i in items], schema=spec.polars_schema())


class ModelManager[M: "Model"]:
    """The entry point to a model's data: `Model.objects`."""

    def __init__(self, model: "type[M]") -> None:
        """Create the manager of a model."""
        self.model = model

    def _query(self) -> ModelQuery[M]:
        return ModelQuery(self.model)

    def all(self) -> pl.DataFrame:
        """Read every row. See :meth:`ModelQuery.all`."""
        return self._query().all()

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
        return self._query().as_models()

    def save(
        self,
        data: "pl.DataFrame | M | Iterable[M]",
        *,
        mode: ModelWriteMode | str,
    ) -> None:
        """Validate data against the model and write it.

        The mode is required because it decides what happens to the data that
        already exists:

        * ``overwrite`` replaces the dataset entirely.
        * ``append`` adds the rows (only Delta tables can).

        There is no upsert.

        Args:
            data: A DataFrame, an instance of the model or a list of them.
            mode: A :class:`ModelWriteMode` or its value.

        Raises:
            ValueError: If `mode` is not a :class:`ModelWriteMode`.
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

        frame = validate_frame(_to_frame(model, data), spec, meta.validation_mode)
        registry.entry_for(model, write_mode).save_polars(frame)


class ManagerDescriptor:
    """Gives each model class an `objects` manager (and only the class)."""

    def __get__(self, instance: object, owner: "type[Model]") -> ModelManager[Any]:
        """Return the manager of `owner`; instances do not have one."""
        if instance is not None:
            msg = "'objects' is only available on the model class, not on instances"
            raise AttributeError(msg)
        return ModelManager(owner)
