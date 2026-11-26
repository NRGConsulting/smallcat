# Changelog

All notable changes to `smallcat-chonk` are listed here. The version is taken
from the git tag (`vX.Y.Z`).

## 0.6.7 - 2026-11-26

### Fixed

- Reading a Delta table with a `where` on a text column no longer fails with
  `Function 'equal' has no kernel matching input types (string, string_view)`.
  This happened when the table's Parquet files hold `string_view` columns (for
  example, files written by Polars with a recent Arrow compatibility level) and
  affected `Model.objects.where(...)` and `load_polars(where=...)`. When
  pyarrow cannot evaluate the pushed-down filter, smallcat now reads the data
  cast to the table schema and lets DuckDB filter it, without pushdown.

## 0.6.6 - 2026-11-25

### Added

- A model instance saves itself with `instance.save()`. By default it merges the
  row by `Meta.merge_keys` (inserted if its key is new, updated otherwise), so
  `Customer(id=1, name="Ana").save()` needs no `objects.save(..., mode="merge")`.
  It also takes `mode=` and `merge_keys=`. Only Delta tables can merge: on CSV,
  Parquet or Excel the default raises `ModelUnsupportedWriteModeError` and
  nothing is written; ask for `mode="overwrite"` to replace the file with that
  row. Every column is written, so a field left at its default overwrites the
  stored value. Each call is one write: to save many rows use
  `Model.objects.save([...], mode=...)`.

### Changed

- `save` is now a reserved name on models: a field called `save` raises
  `ModelDefinitionError` when the model is defined.
- Docstrings corrected across the library: missing `Raises` sections added, S3
  listed among the Delta backends, and the descriptions of `base_uri`, the GCS
  secret and the CSV, Excel and Parquet options fixed.

## 0.6.5 - 2026-11-24

### Added

- A model stores a `NaN` as `None` in the fields that admit `None` (`T | None`),
  so the empty cells that pandas hands over (an Excel sheet with blanks, for
  instance) need no validator of your own. A field that does not admit `None`
  keeps its `NaN`, which is a valid `float`. Set `nan_as_none = False` on a
  model to keep the `NaN` in its nullable fields too; subclasses inherit it.
  DataFrames are not affected, and `all(as_models=True)` builds instances, so
  there a `NaN` becomes `None`.

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
