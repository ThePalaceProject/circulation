from typing import List, Optional

from werkzeug.datastructures import ImmutableMultiDict

from api.admin.form_data import ProcessFormData
from core.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)


class MockSettings(BaseSettings):
    field1: List[str] = FormField(
        [],
        form=ConfigurationFormItem(
            label="Field 1",
            type=ConfigurationFormItemType.LIST,
        ),
    )
    field2: List[str] = FormField(
        [],
        form=ConfigurationFormItem(
            label="Field 2",
            type=ConfigurationFormItemType.MENU,
        ),
    )
    field3: Optional[str] = FormField(
        None,
        form=ConfigurationFormItem(
            label="Field 3",
        ),
    )


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
