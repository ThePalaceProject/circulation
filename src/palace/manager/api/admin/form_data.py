from __future__ import annotations

from typing import Any, TypeVar

from werkzeug.datastructures import ImmutableMultiDict

from palace.manager.integration.settings import (
    BaseSettings,
    ConfigurationFormItemType,
    FormFieldInfo,
)

T = TypeVar("T", bound=BaseSettings)


class ProcessFormData:
    @classmethod
    def _process_list(
        cls, key: str, form_data: ImmutableMultiDict[str, str]
    ) -> list[str]:
        return [v for v in form_data.getlist(key) if v != ""]

    @classmethod
    def _process_menu(
        cls, key: str, form_data: ImmutableMultiDict[str, str]
    ) -> list[str]:
        return [
            v.removeprefix(f"{key}_")
            for v in form_data.keys()
            if v.startswith(key) and v != f"{key}_menu"
        ]

    @classmethod
    def get_settings_dict(
        cls, settings_class: type[BaseSettings], form_data: ImmutableMultiDict[str, str]
    ) -> dict[str, Any]:
        """
        Process the wacky format that form data is sent by the admin interface into
        a dictionary that we can use to update the settings.
        """
        return_data: dict[str, Any] = {}
        for field_name, field_info in settings_class.model_fields.items():
            assert isinstance(
                field_info, FormFieldInfo
            ), f"Expected FormFieldInfo, got {field_info.__class__}"
            form_item = field_info.form
            if form_item.type == ConfigurationFormItemType.LIST:
                return_data[field_name] = cls._process_list(field_name, form_data)
            elif form_item.type == ConfigurationFormItemType.MENU:
                return_data[field_name] = cls._process_menu(field_name, form_data)
            else:
                data = form_data.get(field_name)
                if data is not None:
                    return_data[field_name] = data

        return return_data

    @classmethod
    def get_settings(
        cls, settings_class: type[T], form_data: ImmutableMultiDict[str, str]
    ) -> T:
        return settings_class.model_validate(
            cls.get_settings_dict(settings_class, form_data)
        )
