from typing import List, Optional

from werkzeug.datastructures import ImmutableMultiDict

from api.admin.form_data import ProcessFormData
from core.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
    ConfigurationFormItemType,
)


class MockSettings(BaseSettings):
    class ConfigurationForm(BaseSettings.ConfigurationForm):
        field1 = ConfigurationFormItem(
            label="Field 1",
            type=ConfigurationFormItemType.LIST,
        )
        field2 = ConfigurationFormItem(
            label="Field 2",
            type=ConfigurationFormItemType.MENU,
        )
        field3 = ConfigurationFormItem(
            label="Field 3",
        )

    field1: List[str] = []
    field2: List[str] = []
    field3: Optional[str] = None


def test_get_settings():
    data = ImmutableMultiDict(
        [
            ("field1", "value1"),
            ("field1", "value2"),
            ("field2_menu", "field 2 description"),
            ("field2_value3", "blah blah"),
            ("field2_value4", "blah blah blah"),
            ("field3", "value5"),
        ]
    )
    settings = ProcessFormData.get_settings(MockSettings, data)
    assert settings.field1 == ["value1", "value2"]
    assert settings.field2 == ["value3", "value4"]
    assert settings.field3 == "value5"
