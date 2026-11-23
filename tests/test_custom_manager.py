"""Custom managers: `objects = CustomerManager()` with methods of their own."""

from typing import assert_type

import pytest

from smallcat.models import Model, ModelDefinitionError, ModelManager
from smallcat.models.query import ManagerDescriptor


class CustomerManager(ModelManager["Customer"]):
    def get_by_id(self, id: int) -> "Customer | None":
        rows = self.where(id=id).all(as_models=True)
        return rows[0] if rows else None


class Customer(Model):
    id: int
    name: str
    objects = CustomerManager()

    class Meta:
        location = "silver/customers"
        file_format = "parquet"


class UntypedManager(ModelManager):
    def first_name(self) -> str:
        return self.model.__name__


class Plain(Model):
    id: int
    objects = UntypedManager()

    class Meta:
        location = "silver/plain"
        file_format = "parquet"


def _typing_checks() -> None:
    """Never run: mypy checks the types of these lines."""
    assert_type(Customer.objects, CustomerManager)
    assert_type(Customer.objects.get_by_id(1), Customer | None)
    assert_type(Customer.objects.all(as_models=True), list[Customer])
    assert_type(Plain.objects, UntypedManager)
    assert_type(Plain.objects.first_name(), str)
    assert_type(Plain.objects.all(as_models=True), list[Model])


def test_typed_manager_is_bound_and_inherited() -> None:
    assert isinstance(Customer.objects, CustomerManager)
    assert Customer.objects.model is Customer

    class VipCustomer(Customer):
        pass

    assert VipCustomer.objects.model is VipCustomer


def test_custom_manager_is_bound_to_the_model() -> None:
    manager = Plain.objects
    assert isinstance(manager, UntypedManager)
    assert manager.model is Plain
    assert manager.first_name() == "Plain"


def test_default_manager_is_unchanged() -> None:
    class Other(Model):
        id: int

    assert Other.__dict__.get("objects") is None
    assert type(Other.objects) is ModelManager
    assert isinstance(Model.__dict__["objects"], ManagerDescriptor)


def test_subclass_inherits_the_manager_bound_to_itself() -> None:
    class Child(Plain):
        pass

    assert isinstance(Child.objects, UntypedManager)
    assert Child.objects.model is Child
    assert Plain.objects.model is Plain


def test_not_available_on_instances() -> None:
    with pytest.raises(AttributeError, match="only available on the model class"):
        _ = Plain(id=1).objects  # type: ignore[misc]


def test_manager_typed_for_another_model_is_rejected() -> None:
    with pytest.raises(ModelDefinitionError, match=r"typed for 'Customer'.*'Order'"):

        class Order(Model):
            objects = CustomerManager()


def test_manager_typed_with_a_class_is_checked() -> None:
    class ByClass(ModelManager[Customer]):
        pass

    with pytest.raises(ModelDefinitionError, match="typed for 'Customer'"):

        class Order(Model):
            objects = ByClass()


def test_misspelled_reference_is_rejected() -> None:
    class Bad(ModelManager["Custmer"]):  # type: ignore[type-var,name-defined]
        pass

    with pytest.raises(ModelDefinitionError, match="typed for 'Custmer'"):

        class Thing(Model):
            objects = Bad()


def test_unbound_manager_has_no_model() -> None:
    with pytest.raises(ModelDefinitionError, match="not bound to a model"):
        _ = UntypedManager().model


@pytest.mark.parametrize("value", [5, CustomerManager, "x"])
def test_objects_must_be_a_manager(value: object) -> None:
    with pytest.raises(ModelDefinitionError, match="'objects' is a reserved name"):
        type("Bad", (Model,), {"objects": value, "__module__": __name__})


@pytest.mark.filterwarnings("ignore:Field name")
def test_objects_cannot_be_a_field() -> None:
    with pytest.raises(ModelDefinitionError, match="'objects' is a reserved name"):

        class Bad(Model):
            objects: int  # type: ignore[misc,assignment]
