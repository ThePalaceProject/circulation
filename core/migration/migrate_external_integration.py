import json
from collections import defaultdict
from typing import Any, TypeVar

from sqlalchemy.engine import Connection, CursorResult, Row

from core.integration.base import (
    HasIntegrationConfiguration,
    HasLibraryIntegrationConfiguration,
)
from core.integration.settings import (
    BaseSettings,
    ConfigurationFormItemType,
    FormFieldInfo,
)
from core.model import json_serializer

T = TypeVar("T", bound=BaseSettings)


def _validate_and_load_settings(
    settings_class: type[T], settings_dict: dict[str, str]
) -> T:
    aliases = {
        f.alias: f.name
        for f in settings_class.__fields__.values()
        if f.alias is not None
    }
    parsed_settings_dict = {}
    for key, setting in settings_dict.items():
        if key in aliases:
            key = aliases[key]
        field = settings_class.__fields__.get(key)
        if field is None or not isinstance(field.field_info, FormFieldInfo):
            continue
        config_item = field.field_info.form
        if (
            config_item.type == ConfigurationFormItemType.LIST
            or config_item.type == ConfigurationFormItemType.MENU
        ):
            parsed_settings_dict[key] = json.loads(setting)
        else:
            parsed_settings_dict[key] = setting
    return settings_class(**parsed_settings_dict)


def get_configuration_settings(
    connection: Connection,
    integration: Row,
) -> tuple[dict[str, str], dict[str, dict[str, str]], str]:
    settings = connection.execute(
        "select cs.library_id, cs.key, cs.value from configurationsettings cs "
        "where cs.external_integration_id = (%s)",
        (integration.id,),
    )
    settings_dict = {}
    library_settings: dict[str, dict[str, str]] = defaultdict(dict)
    self_test_results = json_serializer({})
    for setting in settings:
        if not setting.value:
            continue
        if setting.key == "self_test_results":
            self_test_results = setting.value
            continue
        if setting.library_id:
            library_settings[setting.library_id][setting.key] = setting.value
        else:
            settings_dict[setting.key] = setting.value

    return settings_dict, library_settings, self_test_results


def _migrate_external_integration(
    connection: Connection,
    name: str,
    protocol: str,
    protocol_class: type[HasIntegrationConfiguration[BaseSettings]],
    goal: str,
    settings_dict: dict[str, Any],
    self_test_results: str,
    context: dict[str, Any] | None = None,
) -> int:
    # Load and validate the settings before storing them in the database.
    settings_class = protocol_class.settings_class()
    settings_obj = _validate_and_load_settings(settings_class, settings_dict)
    integration_configuration = connection.execute(
        "insert into integration_configurations "
        "(protocol, goal, name, settings, context, self_test_results) "
        "values (%s, %s, %s, %s, %s, %s)"
        "returning id",
        (
            protocol,
            goal,
            name,
            json_serializer(settings_obj.dict()),
            json_serializer(context or {}),
            self_test_results,
        ),
    ).fetchone()
    assert integration_configuration is not None
    return integration_configuration[0]  # type: ignore[no-any-return]


def _migrate_library_settings(
    connection: Connection,
    integration_id: int,
    library_id: int,
    library_settings: dict[str, str],
    protocol_class: type[
        HasLibraryIntegrationConfiguration[BaseSettings, BaseSettings]
    ],
) -> None:
    library_settings_class = protocol_class.library_settings_class()
    library_settings_obj = _validate_and_load_settings(
        library_settings_class, library_settings
    )
    connection.execute(
        "insert into integration_library_configurations "
        "(parent_id, library_id, settings) "
        "values (%s, %s, %s)",
        (
            integration_id,
            library_id,
            json_serializer(library_settings_obj.dict()),
        ),
    )


def get_integrations(connection: Connection, goal: str) -> CursorResult:
    external_integrations = connection.execute(
        "select ei.id, ei.protocol, ei.name from externalintegrations ei "
        "where ei.goal = %s",
        goal,
    )
    return external_integrations


def get_library_for_integration(
    connection: Connection, integration_id: int
) -> CursorResult:
    external_integration_library = connection.execute(
        "select library_id from externalintegrations_libraries where externalintegration_id = %s",
        (integration_id,),
    )
    return external_integration_library
