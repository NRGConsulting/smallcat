# Catalog

A catalog maps dataset names to entries, each one naming a
[connection](connections.md). For how to write one, see the
[catalog reference](../catalog-reference.md), and for how to use it,
[Querying a catalog](../querying-catalog.md) and
[Writing with a catalog](../writing-catalog.md). `Catalog.get_dataset` returns
one of the [datasets](datasets.md).

::: smallcat.catalog.Catalog
    options:
      show_root_heading: true
      members: true

## Entries

Every entry type shares the fields and methods of the base class.

::: smallcat.catalog.EntryBase
    options:
      heading_level: 3
      show_root_heading: true
      members: true
::: smallcat.catalog.CSVEntry
    options:
      heading_level: 3
      show_root_heading: true
      members: true
      inherited_members:
        - save_polars
        - scan_polars
        - load_polars

::: smallcat.catalog.ExcelEntry
    options:
      heading_level: 3
      show_root_heading: true
      members: true
      inherited_members:
        - save_polars
        - scan_polars
        - load_polars

::: smallcat.catalog.ParquetEntry
    options:
      heading_level: 3
      show_root_heading: true
      members: true
      inherited_members:
        - save_polars
        - scan_polars
        - load_polars

::: smallcat.catalog.DeltaTableEntry
    options:
      heading_level: 3
      show_root_heading: true
      members: true
      inherited_members:
        - save_polars
        - scan_polars
        - load_polars
