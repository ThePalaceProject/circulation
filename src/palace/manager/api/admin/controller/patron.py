from __future__ import annotations

from typing import Any

import flask
from flask import Response
from flask_babel import lazy_gettext as _

from palace.manager.api.admin.controller.base import AdminPermissionsControllerMixin
from palace.manager.api.admin.controller.util import required_library_from_request
from palace.manager.api.admin.model.patron_debug import (
    AuthMethodInfo,
    AuthMethodsResponse,
    PatronDebugResponse,
)
from palace.manager.api.admin.problem_details import MISSING_INTEGRATION, NO_SUCH_PATRON
from palace.manager.api.adobe_vendor_id import AuthdataUtility
from palace.manager.api.authentication.base import (
    CannotCreateLocalPatron,
    PatronAuthResult,
    PatronData,
    PatronLookupNotSupported,
)
from palace.manager.api.authentication.basic import (
    BasicAuthProviderSettings,
    Keyboards,
)
from palace.manager.api.authentication.patron_debug import HasPatronDebug
from palace.manager.api.authenticator import LibraryAuthenticator
from palace.manager.api.controller.circulation_manager import (
    CirculationManagerController,
)
from palace.manager.core.problem_details import INVALID_INPUT
from palace.manager.integration.goals import Goals
from palace.manager.service.integration_registry.patron_auth import PatronAuthRegistry
from palace.manager.sqlalchemy.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from palace.manager.util.problem_detail import ProblemDetail


class PatronController(CirculationManagerController, AdminPermissionsControllerMixin):
    def _load_patron_data(
        self, authenticator: LibraryAuthenticator | None = None
    ) -> PatronData | ProblemDetail:
        """Extract a patron identifier from an incoming form submission,
        and ask the library's LibraryAuthenticator to turn it into a
        PatronData by doing a remote lookup in the ILS.

        :param authenticator: A LibraryAuthenticator. This is for mocking
        during tests; it's not necessary to provide it normally.
        """
        library = required_library_from_request(flask.request)
        self.require_librarian(library)

        identifier = flask.request.form.get("identifier")
        if not identifier:
            return NO_SUCH_PATRON.detailed(_("Please enter a patron identifier"))

        if not authenticator:
            authenticator = LibraryAuthenticator.from_config(self._db, library)

        patron_data = PatronData(authorization_identifier=identifier)
        patron_lookup_providers = list(authenticator.unique_patron_lookup_providers)

        if not patron_lookup_providers:
            return NO_SUCH_PATRON.detailed(
                _("This library has no authentication providers, so it has no patrons.")
            )

        for provider in patron_lookup_providers:
            try:
                if remote_patron_data := provider.remote_patron_lookup(patron_data):
                    return remote_patron_data
            except PatronLookupNotSupported:
                # This provider doesn't support remote lookup, try local lookup
                if local_patron := provider.local_patron_lookup(self._db, identifier):
                    # Convert the local Patron to PatronData for consistency
                    return PatronData(
                        permanent_id=local_patron.external_identifier,
                        authorization_identifier=local_patron.authorization_identifier,
                        username=local_patron.username,
                        external_type=local_patron.external_type,
                        fines=local_patron.fines,
                        block_reason=local_patron.block_reason,
                        authorization_expires=local_patron.authorization_expires,
                        complete=True,
                    )

        # If we get here, none of the providers succeeded.
        return NO_SUCH_PATRON.detailed(
            _(
                "No patron with identifier %(patron_identifier)s was found at your library",
                patron_identifier=identifier,
            ),
        )

    def lookup_patron(
        self, authenticator: LibraryAuthenticator | None = None
    ) -> dict[str, Any] | ProblemDetail:
        """Look up personal information about a patron via the ILS.

        :param authenticator: A LibraryAuthenticator. This is for mocking
            during tests; it's not necessary to provide it normally.
        """
        patron_data: PatronData | ProblemDetail = self._load_patron_data(authenticator)
        if isinstance(patron_data, ProblemDetail):
            return patron_data
        return patron_data.to_dict

    def reset_adobe_id(
        self, authenticator: LibraryAuthenticator | None = None
    ) -> Response | ProblemDetail:
        """Delete all Credentials for a patron that are relevant
        to the patron's Adobe Account ID.

        :param authenticator: A LibraryAuthenticator. This is for mocking
            during tests; it's not necessary to provide it normal
        """
        library = required_library_from_request(flask.request)
        patron_data = self._load_patron_data(authenticator)
        if isinstance(patron_data, ProblemDetail):
            return patron_data
        # Turn the Identifier into a Patron object.
        try:
            patron, is_new = patron_data.get_or_create_patron(self._db, library.id)
        except CannotCreateLocalPatron:
            return NO_SUCH_PATRON.detailed(
                _(
                    "Could not create local patron object for %(patron_identifier)s",
                    patron_identifier=patron_data.authorization_identifier,
                )
            )

        # Wipe the Patron's 'identifier for Adobe ID purposes'.
        for credential in AuthdataUtility.adobe_relevant_credentials(patron):
            self._db.delete(credential)
        if patron.username:
            identifier = patron.username
        else:
            identifier = f"with identifier {patron.authorization_identifier}"
        return Response(
            str(
                _(
                    "Adobe ID for patron %(name_or_auth_id)s has been reset.",
                    name_or_auth_id=identifier,
                )
            ),
            200,
        )

    def get_auth_methods(self) -> dict[str, Any] | ProblemDetail:
        """Return the list of authentication methods for the library,
        including whether each supports patron debug authentication.
        """
        library = required_library_from_request(flask.request)
        self.require_librarian(library)

        registry: PatronAuthRegistry = (
            self.manager.services.integration_registry.patron_auth()
        )

        # Find all patron auth integrations configured for this library
        library_configs = (
            self._db.query(IntegrationLibraryConfiguration)
            .join(IntegrationConfiguration)
            .filter(
                IntegrationLibraryConfiguration.library_id == library.id,
                IntegrationConfiguration.goal == Goals.PATRON_AUTH_GOAL,
            )
            .all()
        )

        methods: list[AuthMethodInfo] = []
        for lib_config in library_configs:
            integration = lib_config.parent
            protocol_class = registry.get(integration.protocol, None)
            if protocol_class is None:
                continue

            supports_debug = issubclass(protocol_class, HasPatronDebug)

            # Try to extract labels from settings
            identifier_label = "Username"
            password_label = "Password"
            supports_password = True
            settings = protocol_class.settings_load(integration)
            if isinstance(settings, BasicAuthProviderSettings):
                identifier_label = settings.identifier_label
                password_label = settings.password_label
                supports_password = settings.password_keyboard != Keyboards.NULL

            methods.append(
                AuthMethodInfo(
                    id=integration.id,
                    name=integration.name or integration.protocol,
                    protocol=integration.protocol,
                    supports_debug=supports_debug,
                    supports_password=supports_password,
                    identifier_label=identifier_label,
                    password_label=password_label,
                )
            )

        return AuthMethodsResponse(auth_methods=methods).api_dict()

    def debug_auth(self) -> dict[str, Any] | ProblemDetail:
        """Run patron debug authentication against a specific integration."""
        library = required_library_from_request(flask.request)
        self.require_librarian(library)

        integration_id = flask.request.form.get("integration_id")
        username = flask.request.form.get("username")
        password = flask.request.form.get("password")

        if not integration_id or not username:
            return INVALID_INPUT.detailed(
                _("An integration ID and username are required.")
            )

        try:
            parsed_integration_id = int(integration_id)
        except ValueError:
            return INVALID_INPUT.detailed(
                _(
                    "Invalid integration ID: %(integration_id)s",
                    integration_id=integration_id,
                )
            )

        # Find the integration and its library configuration
        integration = (
            self._db.query(IntegrationConfiguration)
            .filter(
                IntegrationConfiguration.id == parsed_integration_id,
                IntegrationConfiguration.goal == Goals.PATRON_AUTH_GOAL,
            )
            .one_or_none()
        )

        if integration is None:
            return MISSING_INTEGRATION

        lib_config = integration.for_library(library)
        if lib_config is None:
            return MISSING_INTEGRATION.detailed(
                _("This integration is not configured for this library.")
            )

        # Load the provider class and instantiate it
        registry: PatronAuthRegistry = (
            self.manager.services.integration_registry.patron_auth()
        )
        protocol_class = registry.get(integration.protocol, None)
        if protocol_class is None:
            return MISSING_INTEGRATION.detailed(
                _("Unknown protocol: %(protocol)s", protocol=integration.protocol)
            )

        if not issubclass(protocol_class, HasPatronDebug):
            return INVALID_INPUT.detailed(
                _("This authentication method does not support debug authentication.")
            )

        settings = protocol_class.settings_load(integration)
        library_settings = protocol_class.library_settings_load(lib_config)
        provider = protocol_class(
            library_id=library.id,
            integration_id=integration.id,
            settings=settings,
            library_settings=library_settings,
        )

        try:
            results = provider.patron_debug(username, password)
        except Exception as e:
            self.log.exception("patron_debug failed")
            results = [
                PatronAuthResult(
                    label="Unexpected Error",
                    success=False,
                    details=f"{type(e).__name__}: {e}",
                )
            ]
        return PatronDebugResponse(results=results).api_dict()
