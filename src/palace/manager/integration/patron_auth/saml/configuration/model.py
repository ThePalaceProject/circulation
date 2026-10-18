import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from functools import cache
from re import Pattern
from threading import Lock
from typing import Annotated, Any, Final, NamedTuple

from annotated_types import Ge, Le
from flask_babel import LazyString, lazy_gettext as _
from frozendict import frozendict
from onelogin.saml2.settings import OneLogin_Saml2_Settings
from pydantic import PositiveInt, ValidationError, field_validator
from sqlalchemy.orm import Session

from palace.util.exceptions import BasePalaceException
from palace.util.log import LoggerMixin

from palace.manager.api.admin.problem_details import INCOMPLETE_CONFIGURATION
from palace.manager.api.authentication.base import (
    AuthProviderLibrarySettings,
    AuthProviderSettings,
)
from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.integration.patron_auth.saml.configuration.problem_details import (
    SAML_GENERIC_PARSING_ERROR,
    SAML_INCORRECT_FILTER_EXPRESSION,
    SAML_INCORRECT_METADATA,
    SAML_INCORRECT_PATRON_ID_REGULAR_EXPRESSION,
    SAML_INCORRECT_PRIVATE_KEY,
)
from palace.manager.integration.patron_auth.saml.configuration.promotion import (
    ACCEPT_RELAY_STATE_PROMOTE_LABEL_ENV_VAR,
    ALLOW_PROMOTION_ENV_VAR,
    DEFAULT_ACCEPT_RELAY_STATE_PROMOTE_LABEL,
    DEFAULT_ALLOW_PROMOTION,
    promotion_configuration,
)
from palace.manager.integration.patron_auth.saml.configuration.service_provider import (
    ACS_SELECTION_POLICY_ENV_VAR,
    METADATA_ENV_VAR,
    METADATA_FILE_ENV_VAR,
    PRIVATE_KEY_ENV_VAR,
    PRIVATE_KEY_FILE_ENV_VAR,
    SamlServiceProviderConfiguration,
)
from palace.manager.integration.patron_auth.saml.metadata.federations import incommon
from palace.manager.integration.patron_auth.saml.metadata.model import (
    DEFAULT_ACS_SELECTION_POLICY,
    SAMLACSSelectionPolicy,
    SAMLAttributeType,
    SAMLBinding,
    SAMLIdentityProviderMetadata,
    SAMLServiceProviderMetadata,
    SAMLSubjectPatronIDExtractor,
)
from palace.manager.integration.patron_auth.saml.metadata.parser import (
    SAMLMetadataParser,
    SAMLMetadataParsingError,
)
from palace.manager.integration.settings import (
    BaseSettings,
    FormFieldType,
    FormMetadata,
    FormOptionsType,
    SettingsValidationError,
)
from palace.manager.sqlalchemy.model.saml import (
    SAMLFederatedIdentityProvider,
    SAMLFederation,
)
from palace.manager.util.filter import FilterExpression, FilterExpressionError
from palace.manager.util.string_helpers import oxford_comma_join


class SAMLConfigurationError(BasePalaceException):
    """Raised in the case of any configuration errors."""


class FederatedIdentityProviderOptions:
    """Provides the options key for the federated identity provider select input."""

    def __init__(self) -> None:
        """Initialize a new instance of FederatedIdentityProviderOptions class."""
        self._mutex = Lock()
        self._last_updated_at = datetime.min
        self._options: FormOptionsType = {}

    def __call__(self, db: Session) -> FormOptionsType:
        """Get federated identity provider options."""
        with self._mutex:
            if self._needs_refresh(db):
                self._options = self._fetch(db)
        return self._options

    def _needs_refresh(self, db: Session) -> bool:
        """Check if the federated identity provider options need to be refreshed."""
        incommon_fed = (
            db.query(SAMLFederation)
            .filter(SAMLFederation.type == incommon.FEDERATION_TYPE)
            .first()
        )
        if incommon_fed is None or incommon_fed.last_updated_at is None:
            return False
        needs_refresh = incommon_fed.last_updated_at > self._last_updated_at
        if needs_refresh:
            self._last_updated_at = incommon_fed.last_updated_at
        return needs_refresh

    @staticmethod
    def _fetch(db: Session) -> FormOptionsType:
        """Fetch federated identity provider options."""
        identity_providers = (
            db.query(
                SAMLFederatedIdentityProvider.entity_id,
                SAMLFederatedIdentityProvider.display_name,
            )
            .join(SAMLFederation)
            .filter(SAMLFederation.type == incommon.FEDERATION_TYPE)
            .order_by(SAMLFederatedIdentityProvider.display_name)
            .all()
        )

        return {entity_id: label for entity_id, label in identity_providers}


def _validate_filter_expression(cls: type[BaseSettings], v: str | None) -> str | None:
    """Shared validator for SAML filter expression fields on settings models.

    :raises SettingsValidationError: if the expression is syntactically invalid.
    """
    if v is not None:
        try:
            FilterExpression(v).check_syntax()
        except FilterExpressionError as exception:
            cls.logger().warning(
                f"Validation of the filter expression failed: {exception}"
            )
            raise SettingsValidationError(
                problem_detail=SAML_INCORRECT_FILTER_EXPRESSION.detailed(
                    f"SAML filter expression has an incorrect format: {str(exception)}"
                )
            )
    return v


# Names shown in the administrative interface. Also used in the field's description, so
# that the stated default follows DEFAULT_ACS_SELECTION_POLICY rather than being restated.
ACS_SELECTION_POLICY_LABELS: Final[Mapping[SAMLACSSelectionPolicy, str]] = frozendict(
    {
        SAMLACSSelectionPolicy.FIRST_INDEX: "Lowest index",
        SAMLACSSelectionPolicy.METADATA_DEFAULT: "Metadata default",
        SAMLACSSelectionPolicy.DEFER_TO_IDP: "Defer to identity provider",
    }
)


class SAMLAuthnContextClass(StrEnum):
    """SAML authentication context class references offered in the admin interface.

    The values are class reference URIs: most come from the SAML 2.0
    authentication context specification, plus the REFEDS MFA and SFA profiles
    (https URIs), which are common in InCommon and other academic federations.
    They are stored in each integration's settings and sent to IdPs in
    authentication requests, so they must not change.
    """

    PASSWORD_PROTECTED_TRANSPORT = (
        "urn:oasis:names:tc:SAML:2.0:ac:classes:PasswordProtectedTransport"
    )
    PASSWORD = "urn:oasis:names:tc:SAML:2.0:ac:classes:Password"
    MOBILE_TWO_FACTOR_CONTRACT = (
        "urn:oasis:names:tc:SAML:2.0:ac:classes:MobileTwoFactorContract"
    )
    TIME_SYNC_TOKEN = "urn:oasis:names:tc:SAML:2.0:ac:classes:TimeSyncToken"
    TLS_CLIENT = "urn:oasis:names:tc:SAML:2.0:ac:classes:TLSClient"
    SMARTCARD = "urn:oasis:names:tc:SAML:2.0:ac:classes:Smartcard"
    KERBEROS = "urn:oasis:names:tc:SAML:2.0:ac:classes:Kerberos"
    REFEDS_MFA = "https://refeds.org/profile/mfa"
    REFEDS_SFA = "https://refeds.org/profile/sfa"
    UNSPECIFIED = "urn:oasis:names:tc:SAML:2.0:ac:classes:unspecified"


class SAMLAuthnContextComparison(StrEnum):
    """Comparison attribute values for a RequestedAuthnContext element.

    The values are defined by the SAML 2.0 Core specification and are stored in
    each integration's settings, so they must not change.
    """

    EXACT = "exact"
    MINIMUM = "minimum"
    BETTER = "better"
    MAXIMUM = "maximum"


# Names shown in the administrative interface. The technical class name is kept in
# parentheses so admins can match a selection against IdP documentation.
AUTHN_CONTEXT_CLASS_LABELS: Final[Mapping[SAMLAuthnContextClass, str]] = frozendict(
    {
        SAMLAuthnContextClass.PASSWORD_PROTECTED_TRANSPORT: (
            "Password over a secure connection (PasswordProtectedTransport)"
        ),
        SAMLAuthnContextClass.PASSWORD: "Password (Password)",
        SAMLAuthnContextClass.MOBILE_TWO_FACTOR_CONTRACT: (
            "Mobile two-factor (MobileTwoFactorContract)"
        ),
        SAMLAuthnContextClass.TIME_SYNC_TOKEN: (
            "One-time password token (TimeSyncToken)"
        ),
        SAMLAuthnContextClass.TLS_CLIENT: "TLS client certificate (TLSClient)",
        SAMLAuthnContextClass.SMARTCARD: "Smart card (Smartcard)",
        SAMLAuthnContextClass.KERBEROS: "Kerberos (Kerberos)",
        SAMLAuthnContextClass.REFEDS_MFA: "Multi-factor (REFEDS MFA)",
        SAMLAuthnContextClass.REFEDS_SFA: "Single-factor (REFEDS SFA)",
        SAMLAuthnContextClass.UNSPECIFIED: "Unspecified (unspecified)",
    }
)

AUTHN_CONTEXT_COMPARISON_LABELS: Final[Mapping[SAMLAuthnContextComparison, str]] = (
    frozendict(
        {
            SAMLAuthnContextComparison.EXACT: (
                "Exact: the authentication method must exactly match one of the "
                "requested methods"
            ),
            SAMLAuthnContextComparison.MINIMUM: (
                "Minimum: one of the requested methods, or stronger"
            ),
            SAMLAuthnContextComparison.BETTER: (
                "Better: stronger than all of the requested methods"
            ),
            SAMLAuthnContextComparison.MAXIMUM: (
                "Maximum: as strong as possible without exceeding the requested methods"
            ),
        }
    )
)


# Names shown in the administrative interface for the web catalog promotion settings.
PROMOTION_LABELS: Final[Mapping[bool, str]] = frozendict(
    {True: "Allow promotion", False: "Do not allow promotion"}
)
RELAY_STATE_PROMOTE_LABEL_LABELS: Final[Mapping[bool, str]] = frozendict(
    {True: "Accept a relay state label", False: "Ignore a relay state label"}
)


def _promotion_setting_description(
    intro: str,
    system_wide_default: Callable[[], bool | None],
    built_in_default: bool,
    labels: Mapping[bool, str],
    env_var: str,
) -> LazyString:
    """Returns the description of a web catalog promotion setting.

    The description is built when the form is shown, so it states the
    system-wide default currently in effect.

    :param intro: What the setting does
    :param system_wide_default: Returns the default set by the environment, or
        None if it sets none
    :param built_in_default: Default used when the environment sets none
    :param labels: Names of the setting's values
    :param env_var: Environment variable that sets the default
    """

    def describe() -> str:
        configured = system_wide_default()
        if configured is None:
            default = (
                f"'{labels[built_in_default]}'. The {env_var} environment variable "
                "can override it."
            )
        else:
            default = (
                f"'{labels[configured]}' (set by the {env_var} environment variable)."
            )
        return (
            f"{intro} Leave unset to use the system-wide default, which is "
            f"currently {default}"
        )

    return LazyString(describe)


def _sp_metadata_environment_source(
    sp_configuration: SamlServiceProviderConfiguration,
) -> str | None:
    """Returns the environment variable that provides the SP metadata, if any.

    The value is checked the same way the setting's validator checks it.

    :raises SettingsValidationError: If the environment's metadata is not valid
    """
    if not (metadata := sp_configuration.get_metadata()):
        return None
    SAMLWebSSOAuthSettings.validate_sp_xml_metadata_value(metadata, "environment")
    return METADATA_FILE_ENV_VAR if sp_configuration.metadata_file else METADATA_ENV_VAR


def _sp_private_key_environment_source(
    sp_configuration: SamlServiceProviderConfiguration,
) -> str | None:
    """Returns the environment variable that provides the SP private key, if any.

    The value is checked the same way the setting's validator checks it.

    :raises SettingsValidationError: If the environment's private key is not valid
    """
    if not (key := (sp_configuration.get_private_key() or "").strip()):
        return None
    SAMLWebSSOAuthSettings.validate_sp_private_key_value(key, "environment")
    return (
        PRIVATE_KEY_FILE_ENV_VAR
        if sp_configuration.private_key_file
        else PRIVATE_KEY_ENV_VAR
    )


# Errors that mean the environment's SAML Service Provider configuration is not valid.
_INVALID_SP_ENVIRONMENT_ERRORS: Final = (
    ValidationError,
    CannotLoadConfiguration,
    OSError,
    UnicodeDecodeError,
    SettingsValidationError,
)


class SPEnvironmentCheck(NamedTuple):
    """What the environment provides for one Service Provider setting."""

    # False if the environment's SAML Service Provider configuration is not valid.
    valid: bool
    # The environment variable that provides a valid value, or None if none does.
    source: str | None


@cache
def sp_environment_check(
    environment_source: Callable[[SamlServiceProviderConfiguration], str | None],
) -> SPEnvironmentCheck:
    """Checks what the environment provides for a Service Provider setting.

    The admin form uses the result to decide whether the setting is required.

    The result is cached for the life of the process. Environment variables do
    not change while the process runs. Without the cache, each form request
    would parse the metadata again and log any parsing error again.

    Limitation: the files named by PALACE_SAML_SP_METADATA_FILE and
    PALACE_SAML_SP_PRIVATE_KEY_FILE can change while the app runs. The form
    does not show such a change until the app restarts. Saving still reads the
    environment, so a save that depends on a changed file reports any problem.
    To show file changes without a restart, the cache must expire. For example,
    it could expire when a file's modification time changes, or after a set time.

    :param environment_source: Returns the environment variable that provides
        the setting's value, or None if none does
    """
    try:
        return SPEnvironmentCheck(
            valid=True,
            source=environment_source(SamlServiceProviderConfiguration()),
        )
    except _INVALID_SP_ENVIRONMENT_ERRORS:
        return SPEnvironmentCheck(valid=False, source=None)


@dataclass(frozen=True)
class _SPEnvironmentSetting:
    """A Service Provider setting whose value the environment can provide.

    The setting is required only when the environment does not provide a valid
    value. The required flag and the description use the same cached check
    (see :func:`sp_environment_check`), so they always agree.

    :param intro: What the setting is
    :param environment_source: Returns the environment variable that provides
        the setting's value, or None if no variable provides one
    :param env_vars: Environment variables that can provide the value
    """

    intro: str
    environment_source: Callable[[SamlServiceProviderConfiguration], str | None]
    env_vars: Sequence[str]

    def required(self) -> bool:
        """Whether the setting is required, given the environment.

        If the environment's configuration is not valid, the setting is required.
        """
        check = sp_environment_check(self.environment_source)
        return not check.valid or check.source is None

    def description(self) -> LazyString:
        """Returns the setting's description, built each time the form is sent."""
        return LazyString(self._describe)

    def _describe(self) -> str:
        """States whether the setting is required, and why."""
        check = sp_environment_check(self.environment_source)
        if not check.valid:
            status = (
                "Required, because the SAML Service Provider configuration in the "
                "environment is not valid."
            )
        elif source := check.source:
            status = (
                f"Optional, because the {source} environment variable provides "
                "a value. Leave empty to use the environment's value, or enter "
                "a value here to override it."
            )
        else:
            status = (
                "Required, because the environment does not provide a value. "
                f"Set {oxford_comma_join(self.env_vars, 'or')} in the environment "
                "to make this setting optional."
            )
        return f"{self.intro}<br><br>{status}"


_SP_METADATA_SETTING: Final = _SPEnvironmentSetting(
    intro=(
        "SAML metadata of the Circulation Manager's Service Provider in an XML format. "
        "MUST contain exactly one SPSSODescriptor tag with at least one "
        "AssertionConsumerService tag with Binding attribute set to "
        "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST."
    ),
    environment_source=_sp_metadata_environment_source,
    env_vars=(METADATA_ENV_VAR, METADATA_FILE_ENV_VAR),
)
_SP_PRIVATE_KEY_SETTING: Final = _SPEnvironmentSetting(
    intro="Private key used for encrypting SAML requests.",
    environment_source=_sp_private_key_environment_source,
    env_vars=(PRIVATE_KEY_ENV_VAR, PRIVATE_KEY_FILE_ENV_VAR),
)


class SAMLWebSSOAuthSettings(AuthProviderSettings, LoggerMixin):
    """SAML Web SSO Authentication settings"""

    non_federated_identity_provider_xml_metadata: Annotated[
        str | None,
        FormMetadata(
            label="IdP: XML Metadata",
            description=(
                "SAML metadata of Identity Providers in an XML format. "
                "MAY contain multiple IDPSSODescriptor tags but each of them MUST contain "
                "at least one SingleSignOnService tag with Binding attribute set to "
                "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect."
            ),
            type=FormFieldType.TEXTAREA,
            use_monospace_font=True,
        ),
    ] = None
    federated_identity_provider_entity_ids: Annotated[
        list[str] | None,
        FormMetadata(
            label="IdP: Federated IdPs",
            description=(
                "List of federated (for example, from InCommon Federation) IdPs supported by this authentication provider. "
                "Try to type the name of the IdP to find it in the list."
            ),
            type=FormFieldType.MENU,
            options=FederatedIdentityProviderOptions(),
            patron_auth_filter_context=True,
        ),
    ] = None
    service_provider_acs_selection_policy: Annotated[
        SAMLACSSelectionPolicy | None,
        FormMetadata(
            label="IdP: Expected SP ACS Endpoint",
            description=(
                "(Optional) How we choose which Assertion Consumer Service (ACS) endpoint "
                "(if any) to name in authentication requests when the Service Provider "
                "metadata declares more than one. "
                f"'{ACS_SELECTION_POLICY_LABELS[SAMLACSSelectionPolicy.FIRST_INDEX]}' "
                "deliberately ignores the isDefault attribute and selects the lowest index "
                "attribute value. Leave unset to use the site-wide default (currently "
                f"'{ACS_SELECTION_POLICY_LABELS[DEFAULT_ACS_SELECTION_POLICY]}' unless "
                f"overridden by the {ACS_SELECTION_POLICY_ENV_VAR} environment variable)."
                "<br>"
                "<br>"
                "NOTE: The selected ACS endpoint, if present, must align with the Identity "
                "Provider's configuration, so coordination with our IdP partner may be "
                "required when changing this value."
            ),
            type=FormFieldType.SELECT,
            options={
                "": "Use site-wide default",
                **{
                    policy.value: label
                    for policy, label in ACS_SELECTION_POLICY_LABELS.items()
                },
            },
        ),
    ] = None
    patron_id_attributes: Annotated[
        list[str] | None,
        FormMetadata(
            label=_("Patron ID: SAML Attributes"),
            description=_(
                "List of SAML attributes that MAY contain a unique patron ID. "
                "The attributes will be scanned sequentially in the order you chose them, "
                "and the first existing attribute will be used to extract a unique patron ID."
                "<br>"
                "NOTE: If a SAML attribute contains several values, only the first will be used."
            ),
            type=FormFieldType.MENU,
            options={attribute.name: attribute.name for attribute in SAMLAttributeType},
            format="narrow",
            patron_auth_filter_context=True,
        ),
    ] = None
    patron_id_use_name_id: Annotated[
        bool,
        FormMetadata(
            label=_("Patron ID: SAML NameID"),
            description=_(
                "Configuration setting indicating whether SAML NameID should be searched for a unique patron ID. "
                "NameID is only used as a fallback when no configured SAML attribute yields a patron ID. "
                "TRANSIENT NameIDs are never used, regardless of this setting."
            ),
            type=FormFieldType.SELECT,
            options={
                True: "Use SAML NameID",
                False: "Do NOT use SAML NameID",
            },
            patron_auth_filter_context=True,
        ),
    ] = True
    patron_id_regular_expression: Annotated[
        Pattern | None,
        FormMetadata(
            label="Patron ID: Regular Expression",
            description=(
                "Regular expression used to extract a unique patron ID from the attributes "
                "specified in <b>Patron ID: SAML Attributes</b> and/or NameID "
                "(if it's enabled in <b>Patron ID: SAML NameID</b>). "
                "<br>"
                "The expression MUST contain a named group <b>patron_id</b> used to match the patron ID. "
                "For example:"
                "<br>"
                "<pre>"
                "{the_regex_pattern}"
                "</pre>"
                "The expression will extract the <b>patron_id</b> from the first SAML attribute that matches "
                "or NameID if it matches the expression."
            ),
            use_monospace_font=True,
        ),
    ] = None
    filter_expression: Annotated[
        str | None,
        FormMetadata(
            label="Authorization Filter: Expression",
            description=(
                "A Python expression that may restrict a patron's access based on their"
                " SAML Subject and other settings."
                " When present, it must evaluate to True in order for the patron to gain access."
                " If a per-library <b>Authorization Filter: Expression</b> is also configured,"
                " both must evaluate to True."
                " The current UTC date is available as 'today_utc' (year, month, day)."
                "<br>"
                "<br>"
                "For example, if you want to limit access to only those patrons for whom the"
                ' "eduPersonEntitlement" attribute has the value "eresources", use the following expression:'
                "<br>"
                "<pre>"
                """
        "urn:mace:nyu.edu:entl:lib:eresources" == subject.attribute_statement.attributes["eduPersonEntitlement"].values[0]
        """
                "</pre>"
                "<br>"
                'If "eduPersonEntitlement" can have multiple values, you can use the following expression:'
                "<br>"
                "<pre>"
                """
        "urn:mace:nyu.edu:entl:lib:eresources" in subject.attribute_statement.attributes["eduPersonEntitlement"].values
        """
                "</pre>"
            ),
            type=FormFieldType.TEXTAREA,
            use_monospace_font=True,
        ),
    ] = None
    # Note: the key-ordering caveat in the description below is a JSONB storage
    # side effect — PostgreSQL sorts all object keys when storing/retrieving JSONB.
    extra_data: Annotated[
        dict[str, Any] | list[Any] | int | float | bool | str | None,
        FormMetadata(
            label="Authorization Filter: Extra Data",
            description=(
                "Extra data available to patron filter expressions via the"
                " 'integration.extra_data' context variable."
                " Accepts any JSON value (object, array, string, number, boolean, or null)."
                "<br>"
                "Note: Dictionary/object key order may not be preserved after saving."
            ),
            type=FormFieldType.JSON,
            patron_auth_filter_context=True,
            use_monospace_font=True,
        ),
    ] = None
    promote_libraries: Annotated[
        bool | None,
        FormMetadata(
            label="Web Catalog: Promote Libraries",
            description=_promotion_setting_description(
                "Whether the web catalog may promote the libraries that use this "
                "integration, to make them easier to find, when a patron's sign-in "
                "starts at the identity provider rather than in a Palace app or the "
                "web catalog.",
                lambda: promotion_configuration().allow_promotion,
                DEFAULT_ALLOW_PROMOTION,
                PROMOTION_LABELS,
                ALLOW_PROMOTION_ENV_VAR,
            ),
            type=FormFieldType.SELECT,
            options={"": "Use system-wide default", **PROMOTION_LABELS},
        ),
    ] = None
    promote_label_from_relay_state: Annotated[
        bool | None,
        FormMetadata(
            label="Web Catalog: Relay State Promote Label",
            description=_promotion_setting_description(
                "Whether to accept a label in the relay state (the promoteLabel "
                "query parameter of the RelayState that the identity provider "
                "sends, which some identity providers call the start URL). An "
                "accepted label replaces this integration's <b>Web Catalog: "
                "Promote Label</b>.",
                lambda: promotion_configuration().accept_relay_state_promote_label,
                DEFAULT_ACCEPT_RELAY_STATE_PROMOTE_LABEL,
                RELAY_STATE_PROMOTE_LABEL_LABELS,
                ACCEPT_RELAY_STATE_PROMOTE_LABEL_ENV_VAR,
            ),
            type=FormFieldType.SELECT,
            options={"": "Use system-wide default", **RELAY_STATE_PROMOTE_LABEL_LABELS},
        ),
    ] = None
    promote_label: Annotated[
        str | None,
        FormMetadata(
            label="Web Catalog: Promote Label",
            description=(
                "The label sent to the web catalog for the promoted libraries. Leave "
                "empty to use this integration's name. A label in the relay state "
                "replaces this one when <b>Web Catalog: Relay State Promote "
                "Label</b> accepts it."
            ),
        ),
    ] = None
    requested_authn_context_classes: Annotated[
        list[SAMLAuthnContextClass],
        FormMetadata(
            label="Authentication: Requested Methods",
            description=(
                "Authentication methods (SAML authentication context classes) that the "
                "Identity Provider will be asked to use, compared according to "
                "<b>Authentication: Requested Method Comparison</b>. "
                "If none are selected, no requirement is sent and the Identity Provider "
                "chooses how to authenticate the patron. Selecting none is the most "
                "compatible choice for IdPs that offer passwordless methods such as "
                "passkeys. By default, "
                f"'{AUTHN_CONTEXT_CLASS_LABELS[SAMLAuthnContextClass.PASSWORD_PROTECTED_TRANSPORT]}' "
                "is requested."
                '<details style="margin-top: 0.5em;">'
                '<summary style="cursor: pointer; text-decoration: underline;">'
                "More details about these authentication methods"
                "</summary>"
                "<ul>"
                "<li><b>Password over a secure connection</b>: username and password "
                "over an encrypted connection. This is the traditional login form and "
                "the default.</li>"
                "<li><b>Password</b>: username and password without requiring an "
                "encrypted connection. Rarely appropriate on its own.</li>"
                "<li><b>Mobile two-factor</b>: two-factor authentication tied to a "
                "mobile device contract.</li>"
                "<li><b>One-time password token</b>: a time-synchronized hardware or "
                "software token.</li>"
                "<li><b>TLS client certificate</b>: a certificate presented by the "
                "patron's browser or device.</li>"
                "<li><b>Smart card</b>: a physical smart card.</li>"
                "<li><b>Kerberos</b>: a Kerberos ticket.</li>"
                "<li><b>Multi-factor (REFEDS MFA)</b>: the REFEDS multi-factor "
                "profile, the common way to request MFA from InCommon and other "
                "federated IdPs. Use it with the 'Exact' comparison.</li>"
                "<li><b>Single-factor (REFEDS SFA)</b>: the REFEDS single-factor "
                "profile. Use it with the 'Exact' comparison.</li>"
                "<li><b>Unspecified</b>: requests the literal 'unspecified' class, "
                "which some IdPs assert when no particular method applies. It does "
                "NOT mean 'any method'. To let the IdP decide, select nothing "
                "instead.</li>"
                "</ul>"
                "Passkeys and other passwordless methods have no dedicated SAML "
                "class. To support them, select nothing, or ask the IdP operator "
                "which class they assert."
                "</details>"
            ),
            type=FormFieldType.MENU,
            options={
                authn_context_class.value: label
                for authn_context_class, label in AUTHN_CONTEXT_CLASS_LABELS.items()
            },
            format="narrow",
        ),
    ] = [SAMLAuthnContextClass.PASSWORD_PROTECTED_TRANSPORT]
    requested_authn_context_comparison: Annotated[
        SAMLAuthnContextComparison,
        FormMetadata(
            label="Authentication: Requested Method Comparison",
            description=(
                "How the Identity Provider should compare the authentication method it "
                "uses against the <b>Authentication: Requested Methods</b> list. The "
                "comparison applies to the list as a whole. It has no effect when no "
                "methods are selected."
                '<details style="margin-top: 0.5em;">'
                '<summary style="cursor: pointer; text-decoration: underline;">'
                "More details about the comparison values"
                "</summary>"
                "<ul>"
                "<li><b>Exact</b>: the method used must exactly match one of the "
                "requested methods.</li>"
                "<li><b>Minimum</b>: the method used must be at least as strong as "
                "one of the requested methods.</li>"
                "<li><b>Better</b>: the method used must be stronger than every "
                "requested method.</li>"
                "<li><b>Maximum</b>: the method used must be as strong as possible "
                "without being stronger than a requested method.</li>"
                "</ul>"
                "The SAML specification does not define which methods are stronger "
                "than others. Each Identity Provider decides its own ordering, so "
                "the non-Exact comparisons behave differently from IdP to IdP. The "
                "REFEDS MFA and SFA profiles are designed to be requested with the "
                "'Exact' comparison."
                "</details>"
            ),
            type=FormFieldType.SELECT,
            options={
                comparison.value: label
                for comparison, label in AUTHN_CONTEXT_COMPARISON_LABELS.items()
            },
        ),
    ] = SAMLAuthnContextComparison.EXACT
    session_lifetime: Annotated[
        PositiveInt | None,
        FormMetadata(
            label="Authentication: Session Lifetime",
            description=(
                "This configuration setting determines how long "
                "a session created by the SAML authentication provider will live in days. "
                "By default it's empty meaning that the lifetime of the Circulation Manager's session "
                "is exactly the same as the lifetime of the IdP's session. "
                "Setting this value to a specific number will override this behaviour."
                "<br>"
                "NOTE: This setting affects the session's lifetime only Circulation Manager's side. "
                "Accessing content protected by SAML will still be governed by the IdP and patrons "
                "will have to reauthenticate each time the IdP's session expires."
            ),
            patron_auth_filter_context=True,
        ),
    ] = None
    service_provider_xml_metadata: Annotated[
        str | None,
        FormMetadata(
            label="SP: XML Metadata",
            description=_SP_METADATA_SETTING.description(),
            type=FormFieldType.TEXTAREA,
            use_monospace_font=True,
            required=_SP_METADATA_SETTING.required,
        ),
    ] = None
    service_provider_private_key: Annotated[
        str | None,
        FormMetadata(
            label="SP: Private Key",
            description=_SP_PRIVATE_KEY_SETTING.description(),
            type=FormFieldType.TEXTAREA,
            use_monospace_font=True,
            required=_SP_PRIVATE_KEY_SETTING.required,
        ),
    ] = None
    service_provider_strict_mode: Annotated[
        int,
        FormMetadata(
            label="SP: Strict Mode",
            description=(
                "If strict is 1, then the Python Toolkit will reject unsigned or unencrypted messages "
                "if it expects them to be signed or encrypted. Also, it will reject the messages "
                "if the SAML standard is not strictly followed."
            ),
        ),
        Ge(0),
        Le(1),
    ] = 0
    service_provider_debug_mode: Annotated[
        int,
        FormMetadata(
            label="SP: Debug Mode",
            description="Enable debug mode (outputs errors).",
        ),
        Ge(0),
        Le(1),
    ] = 0

    @classmethod
    def validate_xml_metadata(cls, v: str, metadata_type: str):
        metadata_parser = SAMLMetadataParser()
        try:
            providers = metadata_parser.parse(v)
        except SAMLMetadataParsingError as exception:
            cls.logger().exception(
                "An unexpected exception occurred during parsing of SAML metadata"
            )
            message = (
                f"{metadata_type}'s metadata has incorrect format: {str(exception)}"
            )
            raise SettingsValidationError(
                problem_detail=SAML_INCORRECT_METADATA.detailed(message)
            ) from exception
        except Exception as exception:
            cls.logger().exception(
                "An unexpected exception occurred during parsing of SAML metadata"
            )
            message = str(exception)
            raise SettingsValidationError(
                problem_detail=SAML_GENERIC_PARSING_ERROR.detailed(message)
            ) from exception
        return providers

    @field_validator("service_provider_xml_metadata")
    @classmethod
    def validate_sp_xml_metadata(cls, v: str | None) -> str | None:
        """Ensure that either this setting or the environment provides a value.

        NOTE: The value of this setting (i.e., NOT the one from the environment)
        is always the one returned, even if is not the one that will eventually
        be used for configuration. That is because the returned value is what
        will be stored in the settings.

        The setting value has priority over that of the environment. The
        selected value must be valid (i.e., parse successfully).  The reason
        we validate the value from the environment, if it is selected, is to be
        able to report to the Admin UI user that the config is broken while
        they have an opportunity to override it with a valid value.
        """
        xml_metadata: str | None
        if v:
            value_from = "this setting"
            xml_metadata = v
        else:
            value_from = "environment"
            xml_metadata = SamlServiceProviderConfiguration().get_metadata()

        if not xml_metadata:
            raise SettingsValidationError(
                problem_detail=INCOMPLETE_CONFIGURATION.detailed(
                    "Service Provider's XML Metadata is required. "
                    "Provide it here or set PALACE_SAML_SP_METADATA or PALACE_SAML_SP_METADATA_FILE in the environment."
                )
            )

        cls.validate_sp_xml_metadata_value(xml_metadata, value_from)
        return v

    @classmethod
    def validate_sp_xml_metadata_value(cls, xml_metadata: str, value_from: str) -> None:
        """Ensure that SP metadata is valid.

        :param xml_metadata: The metadata to check, which must not be empty
        :param value_from: Where the metadata came from, for the error message
        :raises SettingsValidationError: If the metadata is not valid
        """
        providers = cls.validate_xml_metadata(xml_metadata, "Service Provider")
        if len(providers) != 1:
            raise SettingsValidationError(
                problem_detail=SAML_INCORRECT_METADATA.detailed(
                    f"Service Provider's XML metadata (from {value_from}) must contain exactly one declaration of SPSSODescriptor"
                )
            )

    @field_validator("service_provider_private_key")
    @classmethod
    def validate_sp_private_key(cls, v: str | None) -> str | None:
        """Ensure that either this setting or the environment provides a value.

        NOTE: The value of this setting (i.e., NOT the one from the environment)
        is always the one returned, even if is not the one that will eventually
        be used for configuration. That is because the returned value is what
        will be stored in the settings.

        The setting value has priority over that of the environment.
        """
        private_key: str | None
        if v:
            value_from = "this setting"
            private_key = v
        else:
            value_from = "environment"
            private_key = SamlServiceProviderConfiguration().get_private_key()

        if not private_key or not (key := private_key.strip()):
            raise SettingsValidationError(
                problem_detail=INCOMPLETE_CONFIGURATION.detailed(
                    "Service Provider's Private Key is required. "
                    "Provide it here or set PALACE_SAML_SP_PRIVATE_KEY or PALACE_SAML_SP_PRIVATE_KEY_FILE in the environment."
                )
            )

        cls.validate_sp_private_key_value(key, value_from)
        return v

    @classmethod
    def validate_sp_private_key_value(cls, key: str, value_from: str) -> None:
        """Ensure that an SP private key is in a valid format.

        :param key: The key to check, with surrounding whitespace removed
        :param value_from: Where the key came from, for the error message
        :raises SettingsValidationError: If the key is not in a valid format
        """
        # Ensure that the key has the required header and footer text. Some
        # keys encode their algorithm in their body. Other do not and, thus,
        # must hint it in their header/footer.
        if not re.fullmatch(
            r"\s*-----BEGIN.*PRIVATE KEY-----.+-----END.*PRIVATE KEY-----\s*",
            key,
            re.DOTALL,
        ):
            raise SettingsValidationError(
                problem_detail=SAML_INCORRECT_PRIVATE_KEY.detailed(
                    f"Service Provider's Private Key (from {value_from}) is not in a valid format. "
                    "The value must include the '-----BEGIN ...-----' header and '-----END ...-----' footer text."
                )
            )

    @field_validator("non_federated_identity_provider_xml_metadata")
    @classmethod
    def validate_idp_xml_metadata(cls, v: str):
        if v is not None:
            providers = cls.validate_xml_metadata(v, "Identity Provider")
            if len(providers) == 0:
                message = "Identity Provider's XML metadata must contain at least one declaration of IDPSSODescriptor"
                raise SettingsValidationError(
                    problem_detail=SAML_INCORRECT_METADATA.detailed(message)
                )
        return v

    @field_validator("filter_expression")
    @classmethod
    def validate_filter_expression(cls, v: str | None) -> str | None:
        return _validate_filter_expression(cls, v)

    @field_validator("patron_id_regular_expression")
    @classmethod
    def validate_patron_id_regular_expression(cls, v: Pattern):
        if v is not None:
            named_group = (
                SAMLSubjectPatronIDExtractor.PATRON_ID_REGULAR_EXPRESSION_NAMED_GROUP
            )
            if named_group not in v.groupindex:
                message = f"SAML patron ID regular expression does not have mandatory named group '{named_group}'"
                raise SettingsValidationError(
                    problem_detail=SAML_INCORRECT_PATRON_ID_REGULAR_EXPRESSION.detailed(
                        message
                    )
                )
        return v


class SAMLWebSSOAuthLibrarySettings(AuthProviderLibrarySettings):
    """Per-library SAML Web SSO authentication settings."""

    filter_expression: Annotated[
        str | None,
        FormMetadata(
            label="Authorization Filter: Expression",
            description=(
                "A Python expression that may restrict a patron's access to this library"
                " based on their SAML Subject and other settings."
                " When present, it must evaluate to True in order for the patron to gain"
                " access to this library."
                "<br>"
                "<br>"
                "Refer to the integration-level <b>Authorization Filter: Expression</b> setting"
                " for expression"
                " syntax and examples."
            ),
            type=FormFieldType.TEXTAREA,
            use_monospace_font=True,
        ),
    ] = None

    @field_validator("filter_expression")
    @classmethod
    def validate_filter_expression(cls, v: str | None) -> str | None:
        return _validate_filter_expression(cls, v)


class SAMLOneLoginConfiguration(LoggerMixin):
    """Converts metadata objects to the OneLogin's SAML Toolkit format"""

    DEBUG = "debug"
    STRICT = "strict"

    ENTITY_ID = "entityId"
    URL = "url"
    BINDING = "binding"
    X509_CERT = "x509cert"
    X509_CERT_MULTI = "x509certMulti"
    SIGNING = "signing"
    ENCRYPTION = "encryption"

    IDP = "idp"
    SINGLE_SIGN_ON_SERVICE = "singleSignOnService"
    SINGLE_LOGOUT_SERVICE = "singleLogoutService"

    SP = "sp"
    ASSERTION_CONSUMER_SERVICE = "assertionConsumerService"
    NAME_ID_FORMAT = "NameIDFormat"
    PRIVATE_KEY = "privateKey"

    SECURITY = "security"
    AUTHN_REQUESTS_SIGNED = "authnRequestsSigned"
    REQUESTED_AUTHN_CONTEXT = "requestedAuthnContext"
    REQUESTED_AUTHN_CONTEXT_COMPARISON = "requestedAuthnContextComparison"

    def __init__(
        self,
        configuration: SAMLWebSSOAuthSettings,
        sp_configuration: SamlServiceProviderConfiguration | None = None,
    ):
        """Initializes a new instance of SAMLOneLoginConfiguration class

        :param configuration: Configuration object containing SAML metadata
        :param sp_configuration: Optional SP configuration from environment variables
        """
        self._configuration = configuration
        self._sp_configuration = sp_configuration or SamlServiceProviderConfiguration()
        self._service_provider_loaded: SAMLServiceProviderMetadata | None = None
        self._service_provider_settings: dict[str, Any] | None = None
        self._identity_providers_loaded: None | (list[SAMLIdentityProviderMetadata]) = (
            None
        )
        self._identity_providers_settings: dict[str, dict[str, Any]] = {}
        self._metadata_parser = SAMLMetadataParser()

    def _get_federated_identity_providers(
        self, db: Session
    ) -> list[SAMLFederatedIdentityProvider]:
        """Return a list of federated IdPs corresponding to the entity IDs selected by the admin.

        :param db: Database session

        :return: List of SAMLFederatedIdP objects
        """
        if not self._configuration.federated_identity_provider_entity_ids:
            return []

        return (
            db.query(SAMLFederatedIdentityProvider)
            .filter(
                SAMLFederatedIdentityProvider.entity_id.in_(
                    self._configuration.federated_identity_provider_entity_ids
                )
            )
            .all()
        )

    def _load_identity_providers(
        self, db: Session
    ) -> list[SAMLIdentityProviderMetadata]:
        """Loads IdP settings from the library's configuration settings

        :param db: Database session

        :return: List of IdentityProviderMetadata objects

        :raise: SAMLParsingError
        """
        identity_providers = self._load_non_federated_identity_providers()

        if self._configuration.federated_identity_provider_entity_ids:
            for identity_provider_metadata in self._get_federated_identity_providers(
                db
            ):
                parsing_results = self._metadata_parser.parse(
                    identity_provider_metadata.xml_metadata
                )

                for parsing_result in parsing_results:
                    identity_providers.append(parsing_result.provider)

        return identity_providers

    def get_acs_selection_policy(self) -> SAMLACSSelectionPolicy:
        """Resolve the ACS endpoint selection policy for this integration.

        The integration's own setting wins. An integration that leaves it unset uses
        the site-wide default from the environment, and failing that the built-in
        default.

        This is the single source of truth for the policy: callers that need to know
        how requests are built must use it rather than reading the setting, which on
        its own does not account for the environment.

        :return: Policy to apply when parsing this integration's SP metadata
        """
        return (
            self._configuration.service_provider_acs_selection_policy
            or self._sp_configuration.acs_selection_policy
            or DEFAULT_ACS_SELECTION_POLICY
        )

    def _load_service_provider(self) -> SAMLServiceProviderMetadata:
        """Loads SP settings from integration configuration or environment.

        Integration settings take precedence over environment configuration.
        This allows per-integration overrides during the migration period.

        :return: SAMLServiceProviderMetadata object

        :raise: SAMLParsingError
        """
        # Try integration settings first (takes precedence)
        metadata: str | None = self._configuration.service_provider_xml_metadata
        private_key: str | None = self._configuration.service_provider_private_key

        # Fall back to environment if settings not present and log source.
        if metadata:
            self.log.debug("Using SAML SP metadata from integration settings.")
        else:
            self.log.debug("Using SAML SP metadata from environment.")
            metadata = self._sp_configuration.get_metadata()

        if private_key:
            self.log.debug("Using SAML SP private key from integration settings.")
        else:
            self.log.debug("Using SAML SP private key from environment.")
            private_key = self._sp_configuration.get_private_key()

        # Validate that we have the required metadata configuration
        if not metadata:
            raise SAMLConfigurationError(
                _(
                    "SAML SP metadata not configured. Set either in integration settings or via "
                    "PALACE_SAML_SP_METADATA_FILE/PALACE_SAML_SP_METADATA environment variables."
                )
            )

        # The ACS selection policy applies only to SP metadata, so this parse uses its
        # own parser rather than the one shared with identity provider parsing.
        service_provider_parser = SAMLMetadataParser(
            acs_selection_policy=self.get_acs_selection_policy()
        )
        parsing_results = service_provider_parser.parse(metadata)

        if not isinstance(parsing_results, list) or len(parsing_results) != 1:
            raise SAMLConfigurationError(
                _("SAML Service Provider's configuration is not correct")
            )

        parsing_result = parsing_results[0]
        service_provider = parsing_result.provider

        if not isinstance(service_provider, SAMLServiceProviderMetadata):
            raise SAMLConfigurationError(
                _("SAML Service Provider's configuration is not correct")
            )

        # Set private key if available
        if private_key:
            service_provider.private_key = private_key

        return service_provider

    def _load_non_federated_identity_providers(
        self,
    ) -> list[SAMLIdentityProviderMetadata]:
        """Parses the IdPs in the non-federated IdP metadata setting.

        :raise: SAMLMetadataParsingError
        """
        xml_metadata = self._configuration.non_federated_identity_provider_xml_metadata
        if not xml_metadata:
            return []
        return [result.provider for result in self._metadata_parser.parse(xml_metadata)]

    def get_identity_provider_entity_ids(self) -> frozenset[str]:
        """Returns the entity IDs of the IdPs this configuration accepts.

        Federated IdPs are read from the settings without loading their metadata.
        If the non-federated IdP metadata cannot be parsed, its IdPs are left out.
        """
        entity_ids = set(
            self._configuration.federated_identity_provider_entity_ids or []
        )
        try:
            non_federated = self._load_non_federated_identity_providers()
        except SAMLMetadataParsingError:
            self.log.warning(
                "Unable to parse the non-federated IdP metadata; leaving its IdPs out"
            )
        else:
            entity_ids.update(provider.entity_id for provider in non_federated)
        return frozenset(entity_ids)

    def get_identity_providers(self, db: Session) -> list[SAMLIdentityProviderMetadata]:
        """Returns identity providers

        :param db: Database session

        :return: List of IdentityProviderMetadata objects

        :raise: ConfigurationError
        """
        if self._identity_providers_loaded is None:
            self._identity_providers_loaded = self._load_identity_providers(db)

        return self._identity_providers_loaded

    def get_service_provider(self) -> SAMLServiceProviderMetadata:
        """Returns service provider

        :return: ServiceProviderMetadata object

        :raise: ConfigurationError
        """
        if self._service_provider_loaded is None:
            self._service_provider_loaded = self._load_service_provider()

        return self._service_provider_loaded

    def _get_identity_provider_settings(
        self, identity_provider: SAMLIdentityProviderMetadata
    ) -> dict[str, Any]:
        """Converts ServiceProviderMetadata object to the OneLogin's SAML Toolkit format

        :param identity_provider: IdentityProviderMetadata object

        :return: Dictionary containing service provider's settings in the OneLogin's SAML Toolkit format
        """
        onelogin_identity_provider: dict[str, Any] = {
            self.IDP: {
                self.ENTITY_ID: identity_provider.entity_id,
                self.SINGLE_SIGN_ON_SERVICE: {
                    self.URL: identity_provider.sso_service.url,
                    self.BINDING: identity_provider.sso_service.binding.value,
                },
            },
            self.SECURITY: {
                self.AUTHN_REQUESTS_SIGNED: identity_provider.want_authn_requests_signed
            },
        }

        if identity_provider.slo_service:
            onelogin_identity_provider[self.IDP][self.SINGLE_LOGOUT_SERVICE] = {
                self.URL: identity_provider.slo_service.url,
                self.BINDING: identity_provider.slo_service.binding.value,
            }

        if (
            len(identity_provider.signing_certificates) == 1
            and len(identity_provider.encryption_certificates) == 1
            and identity_provider.signing_certificates[0]
            == identity_provider.encryption_certificates[0]
        ):
            onelogin_identity_provider[self.IDP][self.X509_CERT] = (
                identity_provider.signing_certificates[0]
            )
        else:
            if len(identity_provider.signing_certificates) > 0:
                if self.X509_CERT_MULTI not in onelogin_identity_provider[self.IDP]:
                    onelogin_identity_provider[self.IDP][self.X509_CERT_MULTI] = {}

                onelogin_identity_provider[self.IDP][self.X509_CERT_MULTI][
                    self.SIGNING
                ] = identity_provider.signing_certificates
            if len(identity_provider.encryption_certificates) > 0:
                if self.X509_CERT_MULTI not in onelogin_identity_provider[self.IDP]:
                    onelogin_identity_provider[self.IDP][self.X509_CERT_MULTI] = {}

                onelogin_identity_provider[self.IDP][self.X509_CERT_MULTI][
                    self.ENCRYPTION
                ] = identity_provider.encryption_certificates

        return onelogin_identity_provider

    def _get_service_provider_settings(
        self, service_provider: SAMLServiceProviderMetadata
    ) -> dict[str, Any]:
        """Converts ServiceProviderMetadata object to the OneLogin's SAML Toolkit format

        :param service_provider: ServiceProviderMetadata object

        :return: Dictionary containing service provider's settings in the OneLogin's SAML Toolkit format
        """
        onelogin_service_provider = {
            self.SP: {
                self.ENTITY_ID: service_provider.entity_id,
                self.ASSERTION_CONSUMER_SERVICE: {
                    self.URL: service_provider.acs_service.url,
                    self.BINDING: service_provider.acs_service.binding.value,
                },
                self.NAME_ID_FORMAT: service_provider.name_id_format,
                self.X509_CERT: (
                    service_provider.certificate if service_provider.certificate else ""
                ),
                self.PRIVATE_KEY: (
                    service_provider.private_key if service_provider.private_key else ""
                ),
            },
            self.SECURITY: {
                self.AUTHN_REQUESTS_SIGNED: service_provider.authn_requests_signed
            },
        }

        return onelogin_service_provider

    @property
    def configuration(self) -> SAMLWebSSOAuthSettings:
        """Returns original configuration

        :return: Original configuration
        """
        return self._configuration

    def get_identity_provider_settings(
        self, db: Session, idp_entity_id: str
    ) -> dict[str, Any]:
        """Returns a dictionary containing identity provider's settings in a OneLogin's SAML Toolkit format

        :param db: Database session
        :param idp_entity_id: IdP's entity ID

        :return: Dictionary containing identity provider's settings in a OneLogin's SAML Toolkit format
        """
        if idp_entity_id in self._identity_providers_settings:
            return self._identity_providers_settings[idp_entity_id]

        identity_providers = [
            idp
            for idp in self.get_identity_providers(db)
            if idp.entity_id == idp_entity_id
        ]

        if not identity_providers:
            raise SAMLConfigurationError(
                _(
                    "There is no identity provider with entityID = {}".format(
                        idp_entity_id
                    )
                )
            )

        if len(identity_providers) > 1:
            raise SAMLConfigurationError(
                _(
                    "There are multiple identity providers with entityID = {}".format(
                        idp_entity_id
                    )
                )
            )

        identity_provider = self._get_identity_provider_settings(identity_providers[0])

        self._identity_providers_settings[idp_entity_id] = identity_provider

        return identity_provider

    def get_service_provider_settings(self) -> dict[str, Any]:
        """Returns a dictionary containing service provider's settings in the OneLogin's SAML Toolkit format

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :return: Dictionary containing service provider's settings in the OneLogin's SAML Toolkit format
        :rtype: Dict
        """
        if self._service_provider_settings is None:
            self._service_provider_settings = self._get_service_provider_settings(
                self.get_service_provider()
            )

        return self._service_provider_settings

    def get_settings(self, db: Session, idp_entity_id: str) -> dict[str, Any]:
        """Returns a dictionary containing SP's and IdP's settings in the OneLogin's SAML Toolkit format

        :param db: Database session
        :param idp_entity_id: IdP's entity ID

        :return: Dictionary containing SP's and IdP's settings in the OneLogin's SAML Toolkit format
        """
        onelogin_settings: dict[str, Any] = {
            self.DEBUG: self._configuration.service_provider_debug_mode,
            self.STRICT: self._configuration.service_provider_strict_mode,
        }
        identity_provider_settings = self.get_identity_provider_settings(
            db, idp_entity_id
        )
        service_provider_settings = self.get_service_provider_settings()

        onelogin_settings.update(identity_provider_settings)
        onelogin_settings.update(service_provider_settings)

        # We need to use disjunction separately because dict.update just overwrites values
        onelogin_settings[self.SECURITY][self.AUTHN_REQUESTS_SIGNED] = (
            service_provider_settings[self.SECURITY][self.AUTHN_REQUESTS_SIGNED]
            or service_provider_settings[self.SECURITY][self.AUTHN_REQUESTS_SIGNED]
        )

        # An empty selection means no RequestedAuthnContext element is sent at all
        # (the OneLogin toolkit omits it when the value is False), leaving the choice
        # of authentication method entirely to the IdP.
        requested_classes = self._configuration.requested_authn_context_classes
        onelogin_settings[self.SECURITY][self.REQUESTED_AUTHN_CONTEXT] = (
            [authn_context_class.value for authn_context_class in requested_classes]
            if requested_classes
            else False
        )
        onelogin_settings[self.SECURITY][
            self.REQUESTED_AUTHN_CONTEXT_COMPARISON
        ] = self._configuration.requested_authn_context_comparison.value

        settings = OneLogin_Saml2_Settings(onelogin_settings)

        return {
            self.DEBUG: self._configuration.service_provider_debug_mode,
            self.STRICT: self._configuration.service_provider_strict_mode,
            self.IDP: settings.get_idp_data(),
            self.SP: settings.get_sp_data(),
            self.SECURITY: settings.get_security_data(),
        }

    def get_logout_settings(
        self, db: Session, idp_entity_id: str, sp_slo_url: str
    ) -> dict[str, Any]:
        """Return settings for SAML SLO, extending base settings with SP SLO service URL.

        The SP `singleLogoutService` is not part of the standard integration
        settings (it's a runtime URL), so it must be patched in here. The IdP
        `singleLogoutService` is included automatically by
        `_get_identity_provider_settings` when the IdP's metadata contains one.

        The SP SLO binding is always set to HTTP-Redirect, which the OneLogin
        toolkit can fully validate. HTTP-Redirect is preferred even when the
        IdP also supports HTTP-POST; if the IdP supports only HTTP-POST its
        response is accepted as best-effort in `finish_logout`.

        :param db: Database session
        :param idp_entity_id: IdP's entity ID
        :param sp_slo_url: Absolute URL for the SP's SLO callback endpoint
        :return: Settings dict suitable for `OneLogin_Saml2_Auth`
        """
        logout_settings = self.get_settings(db, idp_entity_id)

        # Configure SP's SLO service only if IdP has declared an SLO service.
        # Always prefer HTTP-Redirect for the SP binding so the IdP sends its
        # LogoutResponse via GET, which the OneLogin toolkit can fully validate.
        # If the IdP only supports HTTP-POST, the response is handled as
        # best-effort in finish_logout.
        identity_providers = [
            idp
            for idp in self.get_identity_providers(db)
            if idp.entity_id == idp_entity_id
        ]
        if identity_providers and identity_providers[0].slo_service:
            logout_settings[self.SP][self.SINGLE_LOGOUT_SERVICE] = {
                self.URL: sp_slo_url,
                self.BINDING: SAMLBinding.HTTP_REDIRECT.value,
            }
        else:
            # If IdP has no SLO service, remove the default SP SLO service configuration.
            logout_settings[self.SP][self.SINGLE_LOGOUT_SERVICE] = {}

        return logout_settings
