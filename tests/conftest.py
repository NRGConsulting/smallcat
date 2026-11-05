import polars as pl
import pytest


@pytest.fixture
def local_conn(tmp_path):
    conn = {"conn_type": "fs", "extra": {"base_path": str(tmp_path)}}
    return conn


@pytest.fixture
def example_df():
    return pl.DataFrame(
        {
            "id": [1.0, 2.0, 3.0],
            "name": ["Alice", "Bob", "Cara"],
            "amount": [12.5, 7.0, 19.99],
        },
    )


@pytest.fixture
def another_example_df():
    return pl.DataFrame(
        {
            "id": [4.0, 5.0, 6.0],
            "name": ["John", "Chris", "Wendy"],
            "amount": [2.5, 17.0, 39.99],
        },
    )
