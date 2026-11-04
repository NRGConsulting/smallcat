# Catalog
::: smallcat.catalog.Catalog
    options:
      show_root_heading: true
      members: true

## Entries
::: smallcat.catalog.CSVEntry
    options:
      heading_level: 3
      show_root_heading: true
      members: true
      inherited_members:
        - save_polars
        - load_polars

::: smallcat.catalog.ExcelEntry
    options:
      heading_level: 3
      show_root_heading: true
      members: true
      inherited_members:
        - save_polars
        - load_polars

::: smallcat.catalog.ParquetEntry
    options:
      heading_level: 3
      show_root_heading: true
      members: true
      inherited_members:
        - save_polars
        - load_polars

::: smallcat.catalog.DeltaTableEntry
    options:
      heading_level: 3
      show_root_heading: true
      members: true
      inherited_members:
        - save_polars
        - load_polars
