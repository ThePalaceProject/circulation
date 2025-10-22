import re
from datetime import datetime
from re import Pattern
from threading import Lock
from typing import Annotated, Any

from flask_babel import lazy_gettext as _
from onelogin.saml2.settings import OneLogin_Saml2_Settings
from pydantic import Field, PositiveInt, field_validator
from sqlalchemy.orm import Session

from palace.manager.api.admin.problem_details import INCOMPLETE_CONFIGURATION
from palace.manager.api.authentication.base import (
    AuthProviderLibrarySettings,
    AuthProviderSettings,
)
from palace.manager.core.exceptions import BasePalaceException
from palace.manager.integration.patron_auth.saml.configuration.problem_details import (
    SAML_GENERIC_PARSING_ERROR,
    SAML_INCORRECT_FILTRATION_EXPRESSION,
    SAML_INCORRECT_METADATA,
    SAML_INCORRECT_PATRON_ID_REGULAR_EXPRESSION,
    SAML_INCORRECT_PRIVATE_KEY,
)
from palace.manager.integration.patron_auth.saml.configuration.service_provider import (
    SamlServiceProviderConfiguration,
)
from palace.manager.integration.patron_auth.saml.metadata.federations import incommon
from palace.manager.integration.patron_auth.saml.metadata.filter import (
    SAMLSubjectFilter,
    SAMLSubjectFilterError,
)
from palace.manager.integration.patron_auth.saml.metadata.model import (
    SAMLAttributeType,
    SAMLIdentityProviderMetadata,
    SAMLServiceProviderMetadata,
    SAMLSubjectPatronIDExtractor,
)
from palace.manager.integration.patron_auth.saml.metadata.parser import (
    SAMLMetadataParser,
    SAMLMetadataParsingError,
)
from palace.manager.integration.patron_auth.saml.python_expression_dsl.evaluator import (
    DSLEvaluationVisitor,
    DSLEvaluator,
)
from palace.manager.integration.patron_auth.saml.python_expression_dsl.parser import (
    DSLParser,
)
from palace.manager.integration.settings import (
    ConfigurationFormItem,
    ConfigurationFormItemType,
    ConfigurationFormOptionsType,
    SettingsValidationError,
)
from palace.manager.sqlalchemy.model.saml import (
    SAMLFederatedIdentityProvider,
    SAMLFederation,
)
from palace.manager.util.log import LoggerMixin


class SAMLConfigurationError(BasePalaceException):
    """Raised in the case of any configuration errors."""


class FederatedIdentityProviderOptions:
    """Provides the options key for the federated identity provider select input."""

    def __init__(self) -> None:
        """Initialize a new instance of FederatedIdentityProviderOptions class."""
        self._mutex = Lock()
        self._last_updated_at = datetime.min
        self._options: ConfigurationFormOptionsType = {}

    def __call__(self, db: Session) -> ConfigurationFormOptionsType:
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
    def _fetch(db: Session) -> ConfigurationFormOptionsType:
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


class SAMLWebSSOAuthSettings(AuthProviderSettings, LoggerMixin):
    """SAML Web SSO Authentication settings"""

    service_provider_xml_metadata: Annotated[
        str | None,
        ConfigurationFormItem(
            label="Service Provider's XML Metadata",
            description=(
                "SAML metadata of the Circulation Manager's Service Provider in an XML format. "
                "MUST contain exactly one SPSSODescriptor tag with at least one "
                "AssertionConsumerService tag with Binding attribute set to "
                "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST. "
                "Leave empty to use environment configuration (PALACE_SAML_SP_METADATA or PALACE_SAML_SP_METADATA_FILE)."
            ),
        ),
    ] = None
    service_provider_private_key: Annotated[
        str | None,
        ConfigurationFormItem(
            label="Service Provider's Private Key",
            description="Private key used for encrypting SAML requests. "
            "Leave empty to use environment configuration (PALACE_SAML_SP_PRIVATE_KEY or PALACE_SAML_SP_PRIVATE_KEY_FILE).",
            type=ConfigurationFormItemType.TEXTAREA,
        ),
    ] = None
    federated_identity_provider_entity_ids: Annotated[
        list[str] | None,
        ConfigurationFormItem(
            label="List of Federated IdPs",
            description=(
                "List of federated (for example, from InCommon Federation) IdPs supported by this authentication provider. "
                "Try to type the name of the IdP to find it in the list."
            ),
            options=FederatedIdentityProviderOptions(),
        ),
    ] = None
    patron_id_use_name_id: Annotated[
        bool,
        ConfigurationFormItem(
            label=_("Patron ID: SAML NameID"),
            description=_(
                "Configuration setting indicating whether SAML NameID should be searched for a unique patron ID. "
                "If NameID found, it will supersede any SAML attributes selected in the next section."
            ),
            type=ConfigurationFormItemType.SELECT,
            options={
                True: "Use SAML NameID",
                False: "Do NOT use SAML NameID",
            },
        ),
    ] = True
    patron_id_attributes: Annotated[
        list[str] | None,
        ConfigurationFormItem(
            label=_("Patron ID: SAML Attributes"),
            description=_(
                "List of SAML attributes that MAY contain a unique patron ID. "
                "The attributes will be scanned sequentially in the order you chose them, "
                "and the first existing attribute will be used to extract a unique patron ID."
                "<br>"
                "NOTE: If a SAML attribute contains several values, only the first will be used."
            ),
            type=ConfigurationFormItemType.MENU,
            options={attribute.name: attribute.name for attribute in SAMLAttributeType},
            format="narrow",
        ),
    ] = None
    patron_id_regular_expression: Annotated[
        Pattern | None,
        ConfigurationFormItem(
            label="Patron ID: Regular expression",
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
        ),
    ] = None
    non_federated_identity_provider_xml_metadata: Annotated[
        str | None,
        ConfigurationFormItem(
            label="Identity Provider's XML metadata",
            description=(
                "SAML metadata of Identity Providers in an XML format. "
                "MAY contain multiple IDPSSODescriptor tags but each of them MUST contain "
                "at least one SingleSignOnService tag with Binding attribute set to "
                "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect."
            ),
            type=ConfigurationFormItemType.TEXTAREA,
        ),
    ] = None
    session_lifetime: Annotated[
        PositiveInt | None,
        ConfigurationFormItem(
            label="Session Lifetime",
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
        ),
    ] = None
    filter_expression: Annotated[
        str | None,
        ConfigurationFormItem(
            label="Filter Expression",
            description=(
                "Python expression used for filtering out patrons by their SAML attributes."
                "<br>"
                "<br>"
                'For example, if you want to authenticate using SAML only patrons having "eresources" '
                'as the value of their "eduPersonEntitlement" then you need to use the following expression:'
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
            type=ConfigurationFormItemType.TEXTAREA,
        ),
    ] = None
    service_provider_strict_mode: Annotated[
        int,
        ConfigurationFormItem(
            label="Service Provider's Strict Mode",
            description=(
                "If strict is 1, then the Python Toolkit will reject unsigned or unencrypted messages "
                "if it expects them to be signed or encrypted. Also, it will reject the messages "
                "if the SAML standard is not strictly followed."
            ),
        ),
    ] = Field(default=0, ge=0, le=1)
    service_provider_debug_mode: Annotated[
        int,
        ConfigurationFormItem(
            label="Service Provider's Debug Mode",
            description="Enable debug mode (outputs errors).",
        ),
    ] = Field(default=0, ge=0, le=1)

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

        providers = cls.validate_xml_metadata(xml_metadata, "Service Provider")
        if len(providers) != 1:
            raise SettingsValidationError(
                problem_detail=SAML_INCORRECT_METADATA.detailed(
                    f"Service Provider's XML metadata (from {value_from}) must contain exactly one declaration of SPSSODescriptor"
                )
            )

        return v

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

        return v

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
    def validate_filter_expression(cls, v: str):
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)
        subject_filter = SAMLSubjectFilter(evaluator)
        if v is not None:
            try:
                subject_filter.validate(v)
            except SAMLSubjectFilterError as exception:
                cls.logger().exception("Validation of the filtration expression failed")
                message = f"SAML filtration expression has an incorrect format: {str(exception)}"
                raise SettingsValidationError(
                    problem_detail=SAML_INCORRECT_FILTRATION_EXPRESSION.detailed(
                        message
                    )
                )
        return v

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


class SAMLWebSSOAuthLibrarySettings(AuthProviderLibrarySettings): ...


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

    SP = "sp"
    ASSERTION_CONSUMER_SERVICE = "assertionConsumerService"
    NAME_ID_FORMAT = "NameIDFormat"
    PRIVATE_KEY = "privateKey"

    SECURITY = "security"
    AUTHN_REQUESTS_SIGNED = "authnRequestsSigned"

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
        identity_providers = []

        if self._configuration.non_federated_identity_provider_xml_metadata:
            parsing_results = self._metadata_parser.parse(
                self._configuration.non_federated_identity_provider_xml_metadata
            )
            identity_providers = [
                parsing_result.provider for parsing_result in parsing_results
            ]

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

        parsing_results = self._metadata_parser.parse(metadata)

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
        onelogin_identity_provider = {
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

        settings = OneLogin_Saml2_Settings(onelogin_settings)

        return {
            self.DEBUG: self._configuration.service_provider_debug_mode,
            self.STRICT: self._configuration.service_provider_strict_mode,
            self.IDP: settings.get_idp_data(),
            self.SP: settings.get_sp_data(),
            self.SECURITY: settings.get_security_data(),
        }
