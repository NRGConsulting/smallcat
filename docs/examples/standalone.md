# smallcat-chonk standalone
Use smallcat-chonk without any dependencies.

## Setup
### Install smallcat-chonk

```sh
pip install smallcat-chonk
```

or

```sh
uv add smallcat-chonk
```

### Create the connections
Create a file named `connections.yaml`. Connections live apart from the catalog
and are referred to by name.

```yaml
connections:
  tmp:
    conn_type: fs
    extra:
      base_path: /tmp/
```

### Create an example catalog
Create a file named `catalog.yaml` with the following contents.

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/NRGConsulting/smallcat/refs/heads/main/schemas/catalog.schema.json
entries:
  foo:
    file_format: csv
    connection: tmp
    location: foo.csv
    save_options:
      header: true
    load_options:
      header: true

```

The `# yaml-language-server` line gives you intellisense in VS code, so it will validate and auto-complete your catalog.

## Usage

### Create main.py

Create a python file with the following contents:

```python
import polars as pl
from smallcat import Connections
from smallcat.catalog import Catalog


def get_example_dataset() -> pl.DataFrame:
    url = "https://raw.githubusercontent.com/vega/vega-datasets/master/data/seattle-weather.csv"
    return pl.read_csv(url, try_parse_dates=True)


def main():
    print("Hello from smallcat-local-example!")

    catalog = Catalog.from_yaml('catalog.yaml', connections=Connections.from_yaml('connections.yaml'))
    catalog.save_polars('foo', df=get_example_dataset())

    df = catalog.load_polars('foo', where="precipitation > 0")
    print(df.head())


if __name__ == "__main__":
    main()
```

### Run the file

```sh
uv run main.py
```

Output:

```
Hello from smallcat-local-example!
        date  precipitation  temp_max  temp_min  wind  weather
0 2012-01-01            0.0      12.8       5.0   4.7  drizzle
1 2012-01-02           10.9      10.6       2.8   4.5     rain
2 2012-01-03            0.8      11.7       7.2   2.3     rain
3 2012-01-04           20.3      12.2       5.6   4.7     rain
4 2012-01-05            1.3       8.9       2.8   6.1     rain
```
