"""Site-wide defaults for promoting a SAML integration's libraries in the web catalog."""

from __future__ import annotations

from functools import cache
from typing import Any, Final

from pydantic import (
    ValidationError,
    ValidationInfo,
    ValidatorFunctionWrapHandler,
    field_validator,
)
from pydantic_settings import SettingsConfigDict

from palace.util.log import LoggerMixin

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)

ALLOW_PROMOTION_ENV_VAR = "PALACE_SAML_ALLOW_PROMOTION"
ACCEPT_RELAY_STATE_PROMOTE_LABEL_ENV_VAR = (
    "PALACE_SAML_ACCEPT_RELAY_STATE_PROMOTE_LABEL"
)

# Built-in defaults, used when the environment does not set a usable value.
DEFAULT_ALLOW_PROMOTION: Final = False
DEFAULT_ACCEPT_RELAY_STATE_PROMOTE_LABEL: Final = False


class SAMLPromotionConfiguration(ServiceConfiguration, LoggerMixin):
    """Site-wide defaults for the SAML web catalog promotion settings.

    They apply to integrations that leave their own settings unset, and are
    loaded from environment variables. A setting is None when its environment
    variable is unset or unrecognized, so the built-in default applies.

    Environment variables:
        PALACE_SAML_ALLOW_PROMOTION: Whether the web catalog may promote an
            integration's libraries
        PALACE_SAML_ACCEPT_RELAY_STATE_PROMOTE_LABEL: Whether a promote label in
            the relay state replaces an integration's own label
    """

    model_config = SettingsConfigDict(env_prefix="PALACE_SAML_")

    allow_promotion: bool | None = None
    accept_relay_state_promote_label: bool | None = None

    @field_validator("allow_promotion", "accept_relay_state_promote_label", mode="wrap")
    @classmethod
    def ignore_unrecognized_value(
        cls, value: Any, handler: ValidatorFunctionWrapHandler, info: ValidationInfo
    ) -> bool | None:
        """Ignore a value that is not a boolean, rather than refusing to load.

        These settings have a safe built-in default, so a typo in the environment
        should not take down sign-in or the admin interface.
        """
        try:
            parsed: bool | None = handler(value)
        except ValidationError:
            env_var = f"{cls.model_config.get('env_prefix')}{info.field_name}".upper()
            cls.logger().error(
                f"{env_var} has an unrecognized value '{value}'. Expected 'true' or "
                "'false'. Integrations without their own setting will use the "
                "built-in default instead."
            )
            return None
        return parsed


@cache
def promotion_configuration() -> SAMLPromotionConfiguration:
    """Returns the site-wide promotion defaults.

    They are loaded from the environment once per process, so an unrecognized
    value is logged only once.
    """
    return SAMLPromotionConfiguration()


def resolve_promotion_setting(
    setting: bool | None, system_wide: bool | None, built_in: bool
) -> bool:
    """Resolves an integration's web catalog promotion setting.

    :param setting: The integration's own setting, or None if it is unset
    :param system_wide: The system-wide default, or None if the environment sets none
    :param built_in: The built-in default

    :return: The first of the values that is set
    """
    if setting is not None:
        return setting
    if system_wide is not None:
        return system_wide
    return built_in
