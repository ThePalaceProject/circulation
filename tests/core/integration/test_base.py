import datetime
from enum import Enum
from functools import partial
from unittest.mock import MagicMock, Mock, patch

import pytest

from core.integration.base import integration_settings_load, integration_settings_update
from core.integration.goals import Goals
from core.integration.settings import BaseSettings
from core.model import IntegrationConfiguration
from tests.fixtures.database import DatabaseTransactionFixture


class BaseFixture:
    def __init__(self, mock_flag_modified: Mock):
        self.mock_settings_cls = MagicMock(spec=BaseSettings)
        self.mock_integration = MagicMock(spec=IntegrationConfiguration)
        self.mock_integration.settings_dict = {"test": "test", "number": 123}
        self.mock_flag_modified = mock_flag_modified

        self.load = partial(
            integration_settings_load, self.mock_settings_cls, self.mock_integration
        )
        self.update = partial(
            integration_settings_update, self.mock_settings_cls, self.mock_integration
        )


@pytest.fixture
def base_fixture():
    with patch("core.integration.base.flag_modified") as mock_flag_modified:
        yield BaseFixture(mock_flag_modified=mock_flag_modified)


def test_integration_settings_load(base_fixture: BaseFixture) -> None:
    return_value: BaseSettings = base_fixture.load()
    base_fixture.mock_settings_cls.assert_called_once_with(test="test", number=123)
    assert return_value is base_fixture.mock_settings_cls.return_value


def test_integration_settings_roundtrip(db: DatabaseTransactionFixture) -> None:
    class TestEnum(Enum):
        FOO = "foo"
        BAR = "bar"

    class TestSettings(BaseSettings):
        test: str
        number: int
        enum: TestEnum
        date: datetime.date

    # Create a settings object and save it to the database
    settings = TestSettings(
        test="test", number=123, enum=TestEnum.FOO, date=datetime.date.today()
    )
    integration = db.integration_configuration(protocol="test", goal=Goals.LICENSE_GOAL)
    integration_settings_update(TestSettings, integration, settings)
    settings_dict = integration.settings_dict.copy()

    # Expire this object in the session, so that we can be sure that the integration data
    # gets round-tripped from the database, which includes a JSON serialization step.
    db.session.flush()
    db.session.expire(integration)

    # Load the settings from the database and check that the settings_dict is different
    # due to the JSON serialization, but that once we load the settings object, it is
    # equal to the original settings object.
    assert integration.settings_dict != settings_dict
    settings_roundtripped = integration_settings_load(TestSettings, integration)
    assert settings_roundtripped == settings


def test_integration_settings_update_no_merge(base_fixture: BaseFixture) -> None:
    base_fixture.update({"test": "foo"}, merge=False)
    base_fixture.mock_settings_cls.assert_called_with(test="foo")
    base_fixture.mock_flag_modified.assert_called_once_with(
        base_fixture.mock_integration, "settings_dict"
    )


def test_integration_settings_update_merge(base_fixture: BaseFixture) -> None:
    base_fixture.update({"test": "foo"}, merge=True)
    base_fixture.mock_settings_cls.assert_called_with(test="foo", number=123)
    base_fixture.mock_flag_modified.assert_called_once_with(
        base_fixture.mock_integration, "settings_dict"
    )


def test_integration_settings_update_basesettings(base_fixture: BaseFixture) -> None:
    mock_base = MagicMock(spec=BaseSettings)
    mock_base.dict.return_value = {"test": "foo", "bool": True}

    base_fixture.update(mock_base, merge=True)
    mock_base.dict.assert_called_once_with()
    base_fixture.mock_settings_cls.assert_called_with(test="foo", number=123, bool=True)
    base_fixture.mock_flag_modified.assert_called_once_with(
        base_fixture.mock_integration, "settings_dict"
    )
