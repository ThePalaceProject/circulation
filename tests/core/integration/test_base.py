from functools import partial
from unittest.mock import MagicMock, Mock, patch

import pytest

from core.integration.base import integration_settings_load, integration_settings_update
from core.integration.settings import BaseSettings
from core.model import IntegrationConfiguration


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
    base_fixture.mock_settings_cls.construct.assert_called_once_with(
        test="test", number=123
    )
    assert return_value is base_fixture.mock_settings_cls.construct.return_value


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
