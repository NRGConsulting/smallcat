# Datasets
::: smallcat.datasets
    options:
      show_root_heading: false
      members: false
      show_source: false

## CSV
::: smallcat.datasets.csv_dataset
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false
      show_source: false

::: smallcat.datasets.csv_dataset.CSVDataset
    options:
      heading_level: 3
      show_root_heading: true
      members: true
      inherited_members:
        - from_conn_id
        - load_polars
        - save_polars
        - scan_polars

### Options
::: smallcat.datasets.csv_dataset.CSVLoadOptions
    options:
      heading_level: 4
      show_root_heading: true
      members: true

::: smallcat.datasets.csv_dataset.CSVSaveOptions
    options:
      heading_level: 4
      show_root_heading: true
      members: true

## Excel
::: smallcat.datasets.excel_dataset
    options:
      show_root_toc_entry: false
      show_root_heading: false
      members: false
      show_source: false

::: smallcat.datasets.excel_dataset.ExcelDataset
    options:
      heading_level: 3
      show_root_heading: true
      members: true
      inherited_members:
        - from_conn_id
        - load_polars
        - save_polars
        - scan_polars

### Options
::: smallcat.datasets.excel_dataset.ExcelLoadOptions
    options:
      heading_level: 4
      show_root_heading: true
      members: true

::: smallcat.datasets.excel_dataset.ExcelSaveOptions
    options:
      heading_level: 4
      show_root_heading: true
      members: true

## Parquet
::: smallcat.datasets.parquet_dataset
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false
      show_source: false

::: smallcat.datasets.parquet_dataset.ParquetDataset
    options:
      heading_level: 3
      show_root_heading: true
      members: true
      inherited_members:
        - from_conn_id
        - load_polars
        - save_polars
        - scan_polars

### Options
::: smallcat.datasets.parquet_dataset.ParquetLoadOptions
    options:
      heading_level: 4
      show_root_heading: true
      members: true

::: smallcat.datasets.parquet_dataset.ParquetSaveOptions
    options:
      heading_level: 4
      show_root_heading: true
      members: true

## Delta Table
::: smallcat.datasets.delta_table_dataset
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false
      show_source: false

::: smallcat.datasets.delta_table_dataset.DeltaTableDataset
    options:
      heading_level: 3
      show_root_heading: true
      members: true
      inherited_members:
        - from_conn_id
        - load_polars
        - save_polars
        - scan_polars

### Options
::: smallcat.datasets.delta_table_dataset.DeltaTableLoadOptions
    options:
      heading_level: 4
      show_root_heading: true
      members: true

::: smallcat.datasets.delta_table_dataset.DeltaTableSaveOptions
    options:
      heading_level: 4
      show_root_heading: true
      members: true
