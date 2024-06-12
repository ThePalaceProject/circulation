from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pyfakefs.fake_filesystem import FakeFilesystem

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.service.configuration.limited_env_override import (
    ServiceConfigurationWithLimitedEnvOverride,
)
from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)
from palace.manager.service.logging.configuration import LogLevel

if TYPE_CHECKING:
    from pytest import MonkeyPatch


class MockServiceConfiguration(ServiceConfiguration):
    string_with_default: str = "default"
    string_without_default: str
    int_type: int = 12

    class Config:
        env_prefix = "MOCK_"


class ServiceConfigurationFixture:
    def __init__(self, type: str, monkeypatch: MonkeyPatch, fs: FakeFilesystem):
        self.type = type
        self.monkeypatch = monkeypatch
        self.fs = fs

        # Make sure the environment is empty
        self.reset(
            ["MOCK_STRING_WITHOUT_DEFAULT", "MOCK_INT_TYPE", "MOCK_STRING_WITH_DEFAULT"]
        )

        # Make sure the .env file is empty
        self.env_file = fs.create_file(".env", contents="")

    def reset(self, keys: list[str]):
        for key in keys:
            self.monkeypatch.delenv(key, raising=False)

    def set(self, key: str, value: str):
        if self.type == "env":
            self.set_env(key, value)
        elif self.type == "dot_env":
            self.set_dot_env(key, value)
        else:
            raise ValueError(f"Unknown type: {self.type}")

    def set_env(self, key: str, value: str):
        self.monkeypatch.setenv(key, value)

    def set_dot_env(self, key: str, value: str):
        existing = self.env_file.contents or ""
        self.env_file.set_contents("\n".join([existing, f"{key}={value}"]))


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

        assert "MOCK_STRING_WITHOUT_DEFAULT:  field required" in str(exc_info.value)

    def test_exception_validation(
        self, service_configuration_fixture: ServiceConfigurationFixture
    ):
        service_configuration_fixture.set("MOCK_INT_TYPE", "this is not an int")

        with pytest.raises(CannotLoadConfiguration) as exc_info:
            MockServiceConfiguration()

        assert "MOCK_INT_TYPE:  value is not a valid integer" in str(exc_info.value)

    def test_exception_mutation(
        self, service_configuration_fixture: ServiceConfigurationFixture
    ):
        service_configuration_fixture.set("MOCK_STRING_WITHOUT_DEFAULT", "string")
        config = MockServiceConfiguration()

        with pytest.raises(TypeError):
            # Ignore the type error, since it tells us this is immutable,
            # and we are testing that behavior at runtime.
            config.string_with_default = "new value"  # type: ignore[misc]


class TestServiceConfigurationWithLimitedEnvOverride:
    def test_unknown_field(self):
        class MockConfiguration1(ServiceConfigurationWithLimitedEnvOverride):
            existing_field: bool = True

            class Config:
                env_prefix = "MOCK_"
                environment_override_error_fields = {"non_existing_field"}

        with pytest.raises(CannotLoadConfiguration) as exc_info:
            MockConfiguration1()
        assert "The following are not the name of an existing field" in str(
            exc_info.value
        )
        assert "non_existing_field" in str(exc_info.value)

        class MockConfiguration2(ServiceConfigurationWithLimitedEnvOverride):
            existing_field: bool = True

            class Config:
                env_prefix = "MOCK_"
                environment_override_warning_fields = {"non_existing_field"}

        with pytest.raises(CannotLoadConfiguration) as exc_info:
            MockConfiguration2()
        assert "The following are not the name of an existing field" in str(
            exc_info.value
        )
        assert "non_existing_field" in str(exc_info.value)

    def test_overlapping_fields(self):
        class MockConfiguration(ServiceConfigurationWithLimitedEnvOverride):
            field: bool = True
            overlapping_field: bool = True

            class Config:
                env_prefix = "MOCK_"
                environment_override_error_fields = {"field", "overlapping_field"}
                environment_override_warning_fields = {"overlapping_field"}

        with pytest.raises(CannotLoadConfiguration) as exc_info:
            MockConfiguration()
        assert "The following field names are specified in multiple settings" in str(
            exc_info.value
        )
        assert "overlapping_field" in str(exc_info.value)

    def test_environment_override_warning_fields(
        self,
        caplog: pytest.LogCaptureFixture,
        service_configuration_fixture: ServiceConfigurationFixture,
    ):
        caplog.set_level(LogLevel.warning)

        class MockConfiguration(ServiceConfigurationWithLimitedEnvOverride):
            field1: bool = True
            warning_field2: bool = True

            class Config:
                env_prefix = "MOCK_"
                environment_override_warning_fields = {
                    "warning_field2",
                }

        service_configuration_fixture.reset(["MOCK_FIELD1", "MOCK_WARNING_FIELD2"])
        config1 = MockConfiguration()

        assert config1.field1 is True
        assert config1.warning_field2 is True
        assert (
            "Some `environment_override_warning_fields` are overridden in the environment."
            not in caplog.text
        )
        assert "warning_field2" not in caplog.text
        assert "field1" not in caplog.text

        service_configuration_fixture.set("MOCK_FIELD1", "false")
        service_configuration_fixture.set("MOCK_WARNING_FIELD2", "false")
        config2 = MockConfiguration()

        assert config2.field1 is False
        assert config2.warning_field2 is True
        assert (
            "Some `environment_override_warning_fields` are overridden in the environment."
            in caplog.text
        )
        assert "warning_field2" in caplog.text
        assert "field1" not in caplog.text

        service_configuration_fixture.reset(["MOCK_FIELD1", "MOCK_WARNING_FIELD2"])

    def test_environment_override_error_field(
        self,
        service_configuration_fixture: ServiceConfigurationFixture,
    ):
        class MockConfiguration(ServiceConfigurationWithLimitedEnvOverride):
            field1: bool = True
            error_field2: bool = True

            class Config:
                env_prefix = "MOCK_"
                environment_override_error_fields = {
                    "error_field2",
                }

        service_configuration_fixture.reset(["MOCK_FIELD1", "MOCK_ERROR_FIELD2"])
        config1 = MockConfiguration()

        assert config1.field1 is True
        assert config1.error_field2 is True

        service_configuration_fixture.set("MOCK_FIELD1", "false")
        service_configuration_fixture.set("MOCK_ERROR_FIELD2", "false")
        with pytest.raises(CannotLoadConfiguration) as exc_info:
            MockConfiguration()

        assert (
            "Some `environment_override_error_fields` are overridden in the environment."
            in str(exc_info.value)
        )
        assert "error_field2" in str(exc_info.value)
        assert "field1" not in str(exc_info.value)

        service_configuration_fixture.reset(["MOCK_FIELD1", "MOCK_ERROR_FIELD2"])
