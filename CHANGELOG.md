# Changelog

All notable changes to `smallcat-chonk` are listed here. The version is taken
from the git tag (`vX.Y.Z`).

## 0.6.4 - 2026-11-24

### Added

- `Model.objects.create_table()` creates the model's dataset, empty and with the
  model's schema, if it does not exist yet: a Delta table, or a CSV, Parquet or
  Excel file. It returns `True` if it created it and `False` if it already
  existed. A model that allows no write mode is not created: a `UserWarning` is
  emitted and it returns `False`. Datasets and catalog entries gain `exists()`
  and `create_empty()`. An empty CSV or Excel file has no types, so those models
  need `validation_mode = "coerce"` to be read.

## 0.6.3 - 2026-11-23

### Added

- A model can declare its own manager with methods of its own, such as
  `get_by_id`: subclass `ModelManager` and assign it to `objects`
  (`objects = CustomerManager()`). `ModelManager["Customer"]` types the results
  as `list[Customer]` (`list[Model]` without it) and must name the model that
  declares the manager. `objects` is now a reserved name: anything that is not a
  `ModelManager` raises `ModelDefinitionError`.
- `ManagerDescriptor` and `UnsupportedFieldTypeError` are exported from
  `smallcat.models`; `Catalog` and the connection errors from `smallcat`; the
  `*LoadOptions` and `*SaveOptions` classes from `smallcat.datasets`. IDE
  auto-import now finds them.

## 0.6.2 - 2026-11-23

### Added

- `all(as_models=True)` on `Model.objects` and on every query
  (`where(...)`, `filter(...)`, `select(...)`) returns a list of model
  instances instead of a DataFrame. `as_models` is keyword-only and `False` by
  default, so existing code is unchanged. It still raises `ModelQueryError`
  when combined with `select()`, since instances need every required field.
  `as_models()` stays as a shortcut for `all(as_models=True)`.

### Changed

- `Model.objects` is now typed, so IDEs and type checkers complete its methods
  and infer the result: `Customer.objects.where(id=1)` is a
  `ModelQuery[Customer]`, `all()` is a `pl.DataFrame` and
  `all(as_models=True)` is a `list[Customer]`.

## 0.6.1 - 2026-11-19

### Added

- Model fields can be annotated with an `Enum` (`StrEnum`, `IntEnum` or an
  `Enum` whose values are all `str` or all `int`). They are stored as their
  values and read back as members.
