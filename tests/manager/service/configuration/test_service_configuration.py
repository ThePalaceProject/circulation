from __future__ import annotations

import json
from functools import partial
from typing import TYPE_CHECKING, Self

import pytest
from pydantic import Field, ValidationError, field_validator, model_validator
from pydantic_settings import SettingsConfigDict
from pyfakefs.fake_filesystem import FakeFilesystem

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)

if TYPE_CHECKING:
    from pytest import MonkeyPatch


class MockServiceConfiguration(ServiceConfiguration):
    @field_validator("string_with_default")
    @classmethod
    def cannot_be_xyz(cls, v: str) -> str:
        if v == "xyz":
            raise ValueError("Must not be xyz!")
        return v

    @model_validator(mode="after")
    def strings_not_same(self) -> Self:
        if self.string_with_default == self.string_without_default:
            raise ValueError("strings must not be the same")
        return self

    string_with_default: str = "default"
    string_without_default: str
    int_type: int = 12
    list_type: list[int] = [1, 2, 3]
    aliased_field: int = Field(1, alias="this_is_an_alias")

    model_config = SettingsConfigDict(env_prefix="MOCK_")


class ServiceConfigurationFixture:
    def __init__(self, type: str, monkeypatch: MonkeyPatch, fs: FakeFilesystem):
        self.type = type
        self.monkeypatch = monkeypatch
        self.fs = fs

        # Make sure the .env file is empty
        self.env_file = fs.create_file(".env", contents="")
        self.env_file_vars: dict[str, str] = {}

        # Make sure the environment is empty
        self.reset(
            ["MOCK_STRING_WITHOUT_DEFAULT", "MOCK_INT_TYPE", "MOCK_STRING_WITH_DEFAULT"]
        )

        self.mock_config = partial(
            MockServiceConfiguration,
            string_without_default="string",
        )

    def reset(self, keys: list[str]):
        for key in keys:
            self.monkeypatch.delenv(key, raising=False)
            if key in self.env_file_vars:
                del self.env_file_vars[key]
        self._update_dot_env()

    def set(self, key: str, value: str):
        if self.type == "env":
            self.set_env(key, value)
        elif self.type == "dot_env":
            self.set_dot_env(key, value)
        else:
            raise ValueError(f"Unknown type: {self.type}")

    def set_env(self, key: str, value: str):
        self.monkeypatch.setenv(key, value)

    def _update_dot_env(self):
        self.env_file.set_contents(
            "\n".join([f"{key}={value}" for key, value in self.env_file_vars.items()])
        )

    def set_dot_env(self, key: str, value: str):
        self.env_file_vars[key] = value
        self._update_dot_env()

    def set_defaults(self):
        self.set("MOCK_STRING_WITHOUT_DEFAULT", "string")


@pytest.fixture(params=["env", "dot_env"])
def service_configuration_fixture(
    request: pytest.FixtureRequest, monkeypatch: MonkeyPatch, fs: FakeFilesystem
):
    if request.param not in ["env", "dot_env"]:
        raise ValueError(f"Unknown param: {request.param}")

    return ServiceConfigurationFixture(request.param, monkeypatch, fs)


class TestServiceConfiguration:
    def test_set(self, service_configuration_fixture: ServiceConfigurationFixture):
        service_configuration_fixture.set("MOCK_STRING_WITHOUT_DEFAULT", "string")
        service_configuration_fixture.set("MOCK_INT_TYPE", "42")

        config = MockServiceConfiguration()

        assert config.string_with_default == "default"
        assert config.string_without_default == "string"
        assert config.int_type == 42

    def test_override_default(
        self, service_configuration_fixture: ServiceConfigurationFixture
    ):
        service_configuration_fixture.set("MOCK_STRING_WITHOUT_DEFAULT", "string")
        service_configuration_fixture.set("MOCK_INT_TYPE", "42")
        # Note the spaces around the value, these should be stripped
        service_configuration_fixture.set("MOCK_STRING_WITH_DEFAULT", "  not default  ")

        config = MockServiceConfiguration()

        assert config.string_with_default == "not default"
        assert config.string_without_default == "string"
        assert config.int_type == 42

    def test_encoding(self, service_configuration_fixture: ServiceConfigurationFixture):
        service_configuration_fixture.set("MOCK_STRING_WITHOUT_DEFAULT", "🎉")
        config = MockServiceConfiguration()
        assert config.string_without_default == "🎉"

    def test_exception_missing(
        self, service_configuration_fixture: ServiceConfigurationFixture
    ):
        with pytest.raises(CannotLoadConfiguration) as exc_info:
            MockServiceConfiguration()

        assert "MOCK_STRING_WITHOUT_DEFAULT:  Field required" in str(exc_info.value)

    def test_exception_validation(
        self, service_configuration_fixture: ServiceConfigurationFixture
    ):
        service_configuration_fixture.set("MOCK_INT_TYPE", "this is not an int")

        with pytest.raises(CannotLoadConfiguration) as exc_info:
            MockServiceConfiguration()

        assert (
            "MOCK_INT_TYPE:  Input should be a valid integer, unable to parse string as an integer"
            in str(exc_info.value)
        )

    def test_exception_mutation(
        self, service_configuration_fixture: ServiceConfigurationFixture
    ):
        config = service_configuration_fixture.mock_config()

        with pytest.raises(ValidationError):
            # Ignore the type error, since it tells us this is immutable,
            # and we are testing that behavior at runtime.
            config.string_with_default = "new value"  # type: ignore[misc]

    def test_with_alias(
        self, service_configuration_fixture: ServiceConfigurationFixture
    ):
        service_configuration_fixture.set("THIS_IS_AN_ALIAS", "12")
        config = service_configuration_fixture.mock_config()
        assert config.aliased_field == 12

    def test_with_alias_error(
        self, service_configuration_fixture: ServiceConfigurationFixture
    ):
        service_configuration_fixture.set("THIS_IS_AN_ALIAS", "value")

        with pytest.raises(
            CannotLoadConfiguration,
            match="THIS_IS_AN_ALIAS:  Input should be a valid integer",
        ) as exc_info:
            service_configuration_fixture.mock_config()

    def test_with_model_validator(
        self, service_configuration_fixture: ServiceConfigurationFixture
    ):
        service_configuration_fixture.set("MOCK_STRING_WITH_DEFAULT", "abc")
        service_configuration_fixture.set("MOCK_STRING_WITHOUT_DEFAULT", "abc")

        with pytest.raises(
            CannotLoadConfiguration,
            match="Error loading settings from environment:\n *Value error, strings must not be the same",
        ) as exc_info:
            MockServiceConfiguration()

    def test_with_field_validator(
        self, service_configuration_fixture: ServiceConfigurationFixture
    ):
        service_configuration_fixture.set("MOCK_STRING_WITH_DEFAULT", "xyz")

        with pytest.raises(
            CannotLoadConfiguration,
            match="Error loading settings from environment:\n *MOCK_STRING_WITH_DEFAULT:  Value error, Must not be xyz!",
        ) as exc_info:
            service_configuration_fixture.mock_config()

    def test_error_with_list_validation(
        self, service_configuration_fixture: ServiceConfigurationFixture
    ):
        service_configuration_fixture.set("MOCK_LIST_TYPE", json.dumps([1, 2, "foo"]))

        with pytest.raises(
            CannotLoadConfiguration,
            match="Error loading settings from environment:\n *MOCK_LIST_TYPE__2:  Input should be a valid integer",
        ) as exc_info:
            service_configuration_fixture.mock_config()
