from collections.abc import Sequence

from flask import url_for
from flask_babel import lazy_gettext as _
from sqlalchemy.orm import Session
from werkzeug.datastructures import Authorization

from palace.manager.api.authentication.base import PatronData, PatronLookupNotSupported
from palace.manager.api.authenticator import BaseSAMLAuthenticationProvider
from palace.manager.integration.patron_auth.saml.auth import (
    SAMLAuthenticationManagerFactory,
)
from palace.manager.integration.patron_auth.saml.configuration.model import (
    SAMLWebSSOAuthLibrarySettings,
    SAMLWebSSOAuthSettings,
)
from palace.manager.integration.patron_auth.saml.credential import SAMLCredentialManager
from palace.manager.integration.patron_auth.saml.metadata.model import (
    SAMLLocalizedMetadataItem,
    SAMLSubject,
    SAMLSubjectPatronIDExtractor,
)
from palace.manager.opds.palace_authentication import (
    LocalizedLogoUrl,
    LocalizedString,
    PalaceAuthentication,
    PalaceAuthenticationLink,
)
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.sqlalchemy.model.credential import Credential
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.problem_detail import (
    ProblemDetail as pd,
    ProblemDetailException,
)

SAML_CANNOT_DETERMINE_PATRON = pd(
    "http://palaceproject.io/terms/problem/auth/unrecoverable/saml/cannot-identify-patron",
    status_code=401,
    title=_("Unable to identify patron."),
    detail=_(
        "Unable to determine patron from authentication response. "
        "This may indicate a service configuration issue."
    ),
)

SAML_TOKEN_EXPIRED = pd(
    "http://palaceproject.io/terms/problem/auth/recoverable/saml/session-expired",
    status_code=401,
    title=_("SAML session expired."),
    detail=_(
        "Your SAML session has expired. Please reauthenticate via your institution's identity provider."
    ),
)


class SAMLWebSSOAuthenticationProvider(
    BaseSAMLAuthenticationProvider[
        SAMLWebSSOAuthSettings, SAMLWebSSOAuthLibrarySettings
    ]
):
    """SAML authentication provider implementing Web Browser SSO profile using the following bindings:
    - HTTP-Redirect Binding for requests
    - HTTP-POST Binding for responses
    """

    def __init__(
        self,
        library_id: int,
        integration_id: int,
        settings: SAMLWebSSOAuthSettings,
        library_settings: SAMLWebSSOAuthLibrarySettings,
        analytics: Analytics | None = None,
    ):
        """Initializes a new instance of SAMLAuthenticationProvider class"""
        super().__init__(
            library_id, integration_id, settings, library_settings, analytics
        )

        self._authentication_manager_factory = SAMLAuthenticationManagerFactory()
        self._credential_manager = SAMLCredentialManager()

        self._patron_id_use_name_id = settings.patron_id_use_name_id
        self._patron_id_attributes = settings.patron_id_attributes
        self._patron_id_regular_expression = settings.patron_id_regular_expression
        self._settings = settings

    @classmethod
    def label(cls) -> str:
        return "SAML 2.0 Web SSO"

    @classmethod
    def description(cls) -> str:
        return (
            "SAML 2.0 authentication provider implementing the Web SSO profile using the following bindings: "
            "HTTP-Redirect for requests and HTTP-POST for responses."
        )

    @property
    def identifies_individuals(self):
        return True

    @classmethod
    def settings_class(cls) -> type[SAMLWebSSOAuthSettings]:
        return SAMLWebSSOAuthSettings

    @classmethod
    def library_settings_class(cls) -> type[SAMLWebSSOAuthLibrarySettings]:
        return SAMLWebSSOAuthLibrarySettings

    def get_credential_from_header(self, auth: Authorization) -> str | None:
        # We cannot extract the credential from the header, so we just return None
        return None

    def _authentication_flow_document(self, db: Session) -> PalaceAuthentication:
        """Create an Authentication Flow object for use in an Authentication for OPDS document.

        :param db: Database session
        :return: :class:`PalaceAuthentication` model
        """
        configuration = self.get_authentication_manager().configuration

        links: list[PalaceAuthenticationLink] = []
        for index, identity_provider in enumerate(
            configuration.get_identity_providers(db)
        ):
            links.append(
                PalaceAuthenticationLink(
                    rel="authenticate",
                    href=self._create_authenticate_url(db, identity_provider.entity_id),
                    display_names=self._to_localized_strings(
                        self._get_idp_display_names(index + 1, identity_provider)
                    ),
                    descriptions=self._to_localized_strings(
                        identity_provider.ui_info.descriptions
                    ),
                    information_urls=self._to_localized_strings(
                        identity_provider.ui_info.information_urls
                    ),
                    privacy_statement_urls=self._to_localized_strings(
                        identity_provider.ui_info.privacy_statement_urls
                    ),
                    logo_urls=self._to_localized_logo_urls(
                        identity_provider.ui_info.logo_urls
                    ),
                )
            )

        return PalaceAuthentication(
            type=self.flow_type,
            description=self.label(),
            links=links,
        )

    @staticmethod
    def _get_idp_display_names(identity_provider_index, identity_provider):
        """Returns a list of IdP's display names:
        - first, it checks UIInfo.display_names
        - secondly, it checks Organization.organization_display_names
        - thirdly, it generates a new name using SAMLConfiguration.IDP_DISPLAY_NAME_TEMPLATE

        :param identity_provider_index: Index of the current IdP
        :type identity_provider_index: int

        :param identity_provider: IdentityProviderMetadata object
        :type identity_provider: IdentityProviderMetadata

        :return: List of IdP's display names
        :rtype: List[LocalizableMetadataItem]
        """
        if identity_provider.ui_info.display_names:
            return identity_provider.ui_info.display_names
        elif identity_provider.organization.organization_display_names:
            return identity_provider.organization.organization_display_names
        else:
            display_name = f"Identity Provider #{identity_provider_index}"
            return [SAMLLocalizedMetadataItem(display_name, language="en")]

    def _create_authenticate_url(self, db, idp_entity_id):
        """Returns an authentication link used by clients to authenticate patrons

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param idp_entity_id: Entity ID of the IdP which will be used for authentication
        :type idp_entity_id: string

        :return: URL for authentication using the chosen IdP
        :rtype: string
        """

        library = self.library(db)

        return url_for(
            "saml_authenticate",
            _external=True,
            library_short_name=library.short_name,
            provider=self.label(),
            idp_entity_id=idp_entity_id,
        )

    @staticmethod
    def _to_localized_strings(
        items: Sequence[SAMLLocalizedMetadataItem] | None,
    ) -> list[LocalizedString]:
        """Convert SAML localized metadata items to :class:`LocalizedString` models.

        :param items: SAML localized metadata items
        :return: List of :class:`LocalizedString` models
        """
        if not items:
            return []
        return [
            LocalizedString(value=item.value, language=item.language or "")
            for item in items
        ]

    @staticmethod
    def _to_localized_logo_urls(
        items: Sequence[SAMLLocalizedMetadataItem] | None,
    ) -> list[LocalizedLogoUrl]:
        """Convert SAML localized metadata items to :class:`LocalizedLogoUrl` models.

        :param items: SAML localized metadata items
        :return: List of :class:`LocalizedLogoUrl` models
        """
        if not items:
            return []
        return [
            LocalizedLogoUrl(value=item.value, language=item.language or "")
            for item in items
        ]

    def _run_self_tests(self, _db):
        pass

    def authenticated_patron(self, db: Session, token: dict | str):
        """Go from a token to an authenticated Patron.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param token: The provider token extracted from the Authorization
            header. This is _not_ the bearer token found in
            the Authorization header; it's the provider-specific token
            embedded in that token.
        :type token: Dict

        :return: A Patron, if one can be authenticated. None, if the
            credentials do not authenticate any particular patron. A
            ProblemDetail if an error occurs.
        :rtype: Union[Patron, ProblemDetail]
        """
        credential = self._credential_manager.lookup_saml_token_by_value(
            db, token, self.library_id
        )
        if credential:
            return credential.patron

        # This token wasn't in our database, or was expired. The
        # patron will have to log in through the SAML provider again
        # to get a new token.
        return SAML_TOKEN_EXPIRED

    def get_authentication_manager(self):
        """Returns SAML authentication manager used by this provider

        :return: SAML authentication manager used by this provider
        :rtype: SAMLAuthenticationManager
        """
        authentication_manager = self._authentication_manager_factory.create(
            self._settings
        )

        return authentication_manager

    def remote_patron_lookup_from_saml_subject(
        self, subject: SAMLSubject
    ) -> PatronData:
        """Creates a PatronData object based on Subject object containing SAML Subject with AttributeStatement

        :param subject: Subject object containing SAML Subject and AttributeStatement

        :return: PatronData object containing information about the authenticated SAML subject or
            ProblemDetail object in the case of any errors

        :raises: ProblemDetailException, if there's a problem.
        """
        if not isinstance(subject, SAMLSubject):
            raise ProblemDetailException(problem_detail=SAML_CANNOT_DETERMINE_PATRON)

        extractor = SAMLSubjectPatronIDExtractor(
            self._patron_id_use_name_id,
            self._patron_id_attributes,
            self._patron_id_regular_expression,
        )
        extracted_id = extractor.extract(subject)

        if extracted_id is None:
            raise ProblemDetailException(problem_detail=SAML_CANNOT_DETERMINE_PATRON)

        return PatronData(
            permanent_id=extracted_id,
            authorization_identifier=extracted_id,
            external_type="A",
            complete=True,
        )

    def remote_patron_lookup(
        self, patron_or_patrondata: PatronData | Patron
    ) -> PatronData | None:
        """Look up patron information from PatronData or Patron object.

        SAML authentication requires the full SSO flow, so we cannot perform
        a fresh lookup using only an authorization identifier. However, for
        admin operations like reset_adobe_id, we can work with the patron
        information we already have.

        :param patron_or_patrondata: PatronData or Patron object
        :return: PatronData object if the input contains sufficient information, None otherwise
        """
        raise PatronLookupNotSupported()

    def saml_callback(
        self, db: Session, subject: SAMLSubject
    ) -> tuple[Credential, Patron, PatronData]:
        """Verifies the SAML subject, generates a Bearer token in the case of successful authentication and returns it

        :param db: Database session
        :param subject: Subject object containing SAML Subject and AttributeStatement
        :return: A 3-tuple (Credential, Patron, PatronData). The Credential
            contains the access token provided by the SAML provider. The
            Patron object represents the authenticated Patron, and the
            PatronData object includes information about the patron
            obtained from the OAuth provider which cannot be stored in the
            circulation manager's database, but which should be passed on
            to the client.
        """
        patron_data = self.remote_patron_lookup_from_saml_subject(subject)

        # Convert the PatronData into a Patron object
        patron, is_new = patron_data.get_or_create_patron(
            db, self.library_id, self.analytics
        )

        # Create a credential for the Patron
        credential = self._credential_manager.create_saml_token(
            db, patron, subject, self._settings.session_lifetime
        )

        return credential, patron, patron_data
