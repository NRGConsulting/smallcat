"""`Model.objects` keeps the model type, so IDEs and mypy can complete it."""

from typing import assert_type

import polars as pl
import pytest

from smallcat.models import Model
from smallcat.models.query import ModelManager, ModelQuery


class Customer(Model):
    id: int
    name: str

    class Meta:
        location = "silver/customers"
        file_format = "parquet"


def _typing_checks() -> None:
    """Never run: mypy checks the types of these lines."""
    assert_type(Customer.objects, ModelManager[Customer])
    assert_type(Customer.objects.where(id=1), ModelQuery[Customer])
    assert_type(Customer.objects.filter(pl.col("id") > 1), ModelQuery[Customer])
    assert_type(Customer.objects.as_models(), list[Customer])
    assert_type(Customer.objects.all(), pl.DataFrame)
    assert_type(Customer.objects.all(as_models=False), pl.DataFrame)
    assert_type(Customer.objects.all(as_models=True), list[Customer])
    assert_type(Customer.objects.where(id=1).all(), pl.DataFrame)
    assert_type(Customer.objects.where(id=1).all(as_models=True), list[Customer])


def test_objects_is_a_manager_of_the_model() -> None:
    assert isinstance(Customer.objects, ModelManager)
    assert Customer.objects.model is Customer


def test_objects_is_not_available_on_instances() -> None:
    with pytest.raises(AttributeError, match="only available on the model class"):
        _ = Customer(id=1, name="a").objects  # type: ignore[misc]
