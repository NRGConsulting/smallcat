# Changelog

All notable changes to `smallcat-chonk` are listed here. The version is taken
from the git tag (`vX.Y.Z`).

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
