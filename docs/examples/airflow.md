# Smallcat Airflow
Use Smallcat with Airflow. In this example we'll work locally by reading connections and variables from the Local Filesystem Backend. In production we could deploy the same DAG but the variables and connections would be defined in the database or a Secret Backend

## Setup
### Install smallcat

Airflow is an optional dependency of smallcat, available as the `airflow` extra.

```sh
pip install "smallcat[airflow]"
```

or

```sh
uv add "smallcat[airflow]"
```

### Point Airflow to local secrets files

Configure the Local Filesystem Secrets Backend:

```sh
AIRFLOW__SECRETS__BACKENDS=airflow.secrets.local_filesystem.LocalFilesystemBackend
AIRFLOW__SECRETS__BACKEND_KWARGS='{"connections_file_path":"/path/to/connections.yaml","variables_file_path":"/path/to/variables.yaml"}'
```

Replace the paths with real locations.

### Define a connection (local filesystem)

`connections.yaml`:

```yaml
local_filesystem:
  conn_type: fs
  extra:
    base_path: /tmp
```

### Define a sample catalog variable

`variables.yaml`:

```yaml
my_catalog:
  entries:
    # Raw daily weather data
    foo:
      file_format: csv
      connection: local_filesystem
      location: foo.csv
      save_options:
        header: true
      load_options:
        header: true

    # Transformed output we’ll write
    foo_processed:
      file_format: csv
      connection: local_filesystem
      location: foo_processed.csv
      save_options:
        header: true
      load_options:
        header: true
```


This declares two datasets under `/tmp`:

- `foo.csv` (input)
- `foo_processed.csv` (output).


## Usage

### Example DAG

The example pulls the Seattle weather CSV, stores it as foo, then creates a filtered/augmented dataset foo_processed:

- parse dates
- keep 2012-01-01 to 2012-03-31
- keep only rainy/drizzly days
- add temp_range = temp_max - temp_min

```python
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
import pandas as pd

from smallcat.catalog import Catalog


SOURCE_URL = "https://raw.githubusercontent.com/vega/vega-datasets/master/data/seattle-weather.csv"

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """This would usually be a function that you import and you have tested in notebooks"""
    return (
        df
        .assign(date=lambda d: pd.to_datetime(d["date"]))
        .loc[lambda d: d["date"].between("2012-01-01", "2012-03-31")
                      & d["weather"].isin(["rain", "drizzle"])]
        .assign(temp_range=lambda d: d["temp_max"] - d["temp_min"])
        .loc[:, ["date", "precipitation", "temp_max", "temp_min", "weather", "temp_range"]]
        .copy()
    )

with DAG(
    dag_id="example_smallcat_weather",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False,
) as dag:
    @task
    def get_input_data():
        """We can use the catalog to save input data.
        Only used so we have some data to work with.
        This would usually be a Data Engineering process"""
        catalog = Catalog.from_airflow_variable("my_catalog")
        raw_df = pd.read_csv(SOURCE_URL)
        catalog.save_pandas("foo", raw_df)

    @task
    def ds_pipeline():
        catalog = Catalog.from_airflow_variable("my_catalog")

        df = catalog.load_pandas("foo", where="event_date >= '2024-01-01'")
        out = transform(df)     # Function should be pure, only IO happens in the pipeline with the catalog
        catalog.save_pandas("foo_processed", out)

    get_input_data()
    ds_pipeline()


if __name__ == "__main__":
    dag.test()
```

After the run, check `/tmp/foo_processed.csv` for the transformed result.
