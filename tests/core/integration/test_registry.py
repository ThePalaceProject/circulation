from unittest.mock import MagicMock

import pytest

from core.integration.goals import Goals
from core.integration.registry import IntegrationRegistry, IntegrationRegistryException


@pytest.fixture
def mock_goal() -> Goals:
    return MagicMock(spec=Goals.PATRON_AUTH_GOAL)


@pytest.fixture
def registry(mock_goal: Goals) -> IntegrationRegistry:
    return IntegrationRegistry(mock_goal)


def test_registry_constructor(registry: IntegrationRegistry, mock_goal: Goals):
    """Test that the constructor sets up the registry correctly."""
    assert registry._lookup == {}
    assert registry._reverse_lookup == {}
    assert registry.goal == mock_goal


def test_registry_register_no_args(registry: IntegrationRegistry):
    """Test that register() works with no arguments."""
    registry.register(object)
    assert registry.get("object") == object
    assert registry.get_protocol(object) == "object"
    assert registry.get_protocols(object) == ["object"]
    assert registry["object"] == object
    assert "object" in registry
    assert len(registry) == 1


def test_registry_register_raises_value_error_if_name_already_registered(
    registry: IntegrationRegistry,
):
    """Test that register() raises a IntegrationRegistryException if the name is already registered."""
    registry.register(object)

    # can register same object again
    registry.register(object)

    # registering a different object with the same name raises an error
    with pytest.raises(IntegrationRegistryException):
        registry.register(list, canonical="object")


def test_registry_register_aliases(registry: IntegrationRegistry):
    """Test that register() works with aliases."""
    registry.register(object, aliases=["test2", "test3"])
    assert registry.get("object") == object
    assert registry.get("test2") == object
    assert registry.get("test3") == object
    assert registry.get_protocol(object) == "object"
    assert registry.get_protocols(object) == ["object", "test2", "test3"]
    assert len(registry) == 1
    assert registry.integrations == {object}


def test_registry_register_canonical(registry: IntegrationRegistry):
    """Test that register() works with a canonical name."""
    registry.register(object, canonical="test")
    assert registry.get("test") == object
    assert registry.get("object") == object
    assert registry.get_protocol(object) == "test"
    assert len(registry) == 1


def test_registry_register_multiple_classes(registry: IntegrationRegistry):
    """Test that register() works with multiple classes."""
    registry.register(object)
    registry.register(list)
    registry.register(dict, canonical="Dict", aliases=["test1", "test2"])

    assert registry.get("object") == object
    assert registry.get("list") == list
    assert registry.get("dict") == dict
    assert registry.get("Dict") == dict
    assert registry.get("test1") == dict
    assert registry.get("test2") == dict
    assert registry.get_protocol(object) == "object"
    assert registry.get_protocol(list) == "list"
    assert registry.get_protocol(dict) == "Dict"
    assert len(registry) == 3


def test_registry_get_returns_default_if_name_not_registered(
    registry: IntegrationRegistry,
):
    # default is none
    assert registry.get("test_class") is None

    # default is not none
    assert registry.get("test_class", "default") == "default"

    # __get__ throws KeyError
    with pytest.raises(KeyError):
        _ = registry["test_class"]


def test_registry_get_protocol_returns_default_if_integration_not_registered(
    registry: IntegrationRegistry,
):
    # default is none
    assert registry.get_protocol(object) is None

    # default is not none
    assert registry.get_protocol(object, "default") == "default"
