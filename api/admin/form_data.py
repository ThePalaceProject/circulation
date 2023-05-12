from __future__ import annotations

from typing import Any, Dict, List, Type, TypeVar

from werkzeug.datastructures import ImmutableMultiDict

from core.integration.settings import (
    BaseSettings,
    ConfigurationFormItemType,
    FormFieldInfo,
)

T = TypeVar("T", bound=BaseSettings)


class ProcessFormData:
    @classmethod
    def _remove_prefix(cls, text: str, prefix: str) -> str:
        # TODO: Remove this when we upgrade to Python 3.9
        if text.startswith(prefix):
            return text[len(prefix) :]
        return text

    @classmethod
    def _process_list(
        cls, key: str, form_data: ImmutableMultiDict[str, str]
    ) -> List[str]:
        return [v for v in form_data.getlist(key) if v != ""]

    @classmethod
    def _process_menu(
        cls, key: str, form_data: ImmutableMultiDict[str, str]
    ) -> List[str]:
        return [
            cls._remove_prefix(v, f"{key}_")
            for v in form_data.keys()
            if v.startswith(key) and v != f"{key}_menu"
        ]

    @classmethod
    def get_settings_dict(
        cls, settings_class: Type[BaseSettings], form_data: ImmutableMultiDict[str, str]
    ) -> Dict[str, Any]:
        """
        Process the wacky format that form data is sent by the admin interface into
        a dictionary that we can use to update the settings.
        """
        return_data: Dict[str, Any] = {}
        for field in settings_class.__fields__.values():
            if not isinstance(field.field_info, FormFieldInfo):
                continue
            form_item = field.field_info.form
            if form_item.type == ConfigurationFormItemType.LIST:
                return_data[field.name] = cls._process_list(field.name, form_data)
            elif form_item.type == ConfigurationFormItemType.MENU:
                return_data[field.name] = cls._process_menu(field.name, form_data)
            else:
                data = form_data.get(field.name)
                if data is not None:
                    return_data[field.name] = data

        return return_data

    @classmethod
    def get_settings(
        cls, settings_class: Type[T], form_data: ImmutableMultiDict[str, str]
    ) -> T:
        return settings_class(**cls.get_settings_dict(settings_class, form_data))
