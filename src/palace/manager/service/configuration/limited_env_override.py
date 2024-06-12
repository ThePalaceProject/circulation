import functools
from typing import Any

from pydantic.env_settings import BaseSettings, SettingsSourceCallable
from pydantic.fields import ModelField

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)
from palace.manager.util.log import LoggerMixin


class ServiceConfigurationWithLimitedEnvOverride(ServiceConfiguration, LoggerMixin):
    # Fields that can be overridden by environment variables should be specified as normal.

    # For non-overridable fields:
    # - Set `const=True` on the field, if nothing should override the default..
    # - Add the field name to one of the `environment_override_*` Config settings.

    class Config:
        # Handle environment variable overrides, depending on presence of field name in:
        #   environment_override_error_fields: report field and raise exception; or
        #   environment_override_warning_fields: report field and log warning.
        # If a field is not specified in one of these lists, an override is permitted.
        # If a field is specified in both, it is an error and an exception is raised.
        # If a field is NOT specified in one of these lists, then an override is allowed.
        # The exception, when raised, will be a `CannotLoadConfiguration`.
        environment_override_error_fields: set[str] | None = None
        environment_override_warning_fields: set[str] | None = None

        # See `pydantic` documentation on customizing sources.
        # https://docs.pydantic.dev/1.10/usage/settings/#adding-sources
        @classmethod
        def customise_sources(
            cls,
            init_settings: SettingsSourceCallable,
            env_settings: SettingsSourceCallable,
            file_secret_settings: SettingsSourceCallable,
        ) -> tuple[SettingsSourceCallable, ...]:
            # We have to wrap the environment settings source in our own function
            # so that we can report on/strip out fields that are not overridable
            # before `pydantic` sees them.
            return (
                init_settings,
                functools.partial(_restrict_environment, env_settings),
                file_secret_settings,
            )


def _env_var_for(field: ModelField) -> str:
    env_prefix = field.model_config.env_prefix or ""  # type: ignore[attr-defined]
    return (env_prefix + field.name).upper()


def _restrict_environment(
    env_settings: SettingsSourceCallable, settings: BaseSettings
) -> dict[str, Any]:
    """Limit environment variables to those not restricted by the `environment_override_*` settings.

    :param env_settings: The environment settings source function, usually indirectly from `pydantic`..
    :param settings: A pydantic model instance.
    :return: A dictionary by field alias of values from the environment.

    :raises CannotLoadConfiguration: Under the following conditions:
        - A non-existent field is specified in one of the `environment_override_*` settings.
        - A field is specified in more than one `environment_override_*` setting.
        - A field specified in `environment_override_error_fields` is overridden in the environment

    If a field is (1) specified in `environment_override_warning_fields` and (2) overridden in the
    environment, then a warning is logged and the field is NOT overridden.
    """
    config = settings.__config__
    logger = settings.log  # type: ignore[attr-defined]

    warning_fields: set[str] = config.environment_override_warning_fields or set()  # type: ignore[attr-defined]
    error_fields: set[str] = config.environment_override_error_fields or set()  # type: ignore[attr-defined]

    fields_by_name = settings.__fields__
    fields_by_alias = {field.alias: field for name, field in fields_by_name.items()}

    if nonexistent_fields := (warning_fields | error_fields) - set(fields_by_name):
        raise CannotLoadConfiguration(
            "Only existing fields may be specified in any of the `environment_override_*` "
            "settings. The following are not the name of an existing field: "
            f"{nonexistent_fields}."
        )
    if overlapping_fields := warning_fields & error_fields:
        raise CannotLoadConfiguration(
            "A field may not be specified in more than one `environment_override_*` setting. "
            "The following field names are specified in multiple settings: "
            f"{overlapping_fields}."
        )

    env_settings_by_alias = env_settings(settings)
    if not env_settings_by_alias:
        return env_settings_by_alias

    env_settings_by_name = {
        fields_by_alias[alias].name: value
        for alias, value in env_settings_by_alias.items()
        if alias in fields_by_alias
    }

    if warnings := set(env_settings_by_name) & warning_fields:
        _msg = (
            "Some `environment_override_warning_fields` are overridden in the environment. Please "
            "remove from either the environment or the `environment_override_warning_fields` setting."
            "The value(s) from the environment will be ignored."
        )
        for field in (fields_by_name[name] for name in warnings):
            _msg += f"\n  {field.name}: alias={field.alias}, env={_env_var_for(field)}"
        logger.warning(_msg)

    if errors := set(env_settings_by_name) & error_fields:
        _msg = (
            "Some `environment_override_error_fields` are overridden in the environment. Please "
            "remove from either the environment or the `environment_override_error_fields` setting."
        )
        for field in (fields_by_name[name] for name in errors):
            _msg += f"\n  {field.name}: alias={field.alias}, env={_env_var_for(field)}"
        raise CannotLoadConfiguration(_msg)

    overridable_names = set(fields_by_name) - warnings - errors
    overridable_aliases = {
        field.alias
        for name, field in fields_by_name.items()
        if name in overridable_names
    }

    return {
        alias: value
        for alias, value in env_settings_by_alias.items()
        if alias in overridable_aliases
    }
