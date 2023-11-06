from __future__ import annotations

import base64
import os
import urllib.parse

import flask

from api.admin.exceptions import AdminNotAuthorized
from api.admin.password_admin_authentication_provider import (
    PasswordAdminAuthenticationProvider,
)
from api.admin.problem_details import (
    ADMIN_AUTH_MECHANISM_NOT_CONFIGURED,
    ADMIN_AUTH_NOT_CONFIGURED,
    INVALID_ADMIN_CREDENTIALS,
    INVALID_CSRF_TOKEN,
)
from api.config import Configuration
from core.model import (
    Admin,
    AdminRole,
    ConfigurationSetting,
    Library,
    get_one,
    get_one_or_create,
)


class AdminController:
    def __init__(self, manager):
        self.manager = manager
        self._db = self.manager._db

    @property
    def admin_auth_providers(self):
        if Admin.with_password(self._db).count() != 0:
            return [PasswordAdminAuthenticationProvider()]

        return []

    def admin_auth_provider(self, type):
        # Return an auth provider with the given type.
        # If no auth provider has this type, return None.
        for provider in self.admin_auth_providers:
            if provider.NAME == type:
                return provider
        return None

    def authenticated_admin_from_request(self):
        """Returns an authenticated admin or a problem detail."""
        if not self.admin_auth_providers:
            return ADMIN_AUTH_NOT_CONFIGURED

        email = flask.session.get("admin_email")
        type = flask.session.get("auth_type")

        if email and type:
            admin = get_one(self._db, Admin, email=email)
            auth = self.admin_auth_provider(type)
            if not auth:
                return ADMIN_AUTH_MECHANISM_NOT_CONFIGURED
            if admin:
                flask.request.admin = admin
                return admin
        flask.request.admin = None
        return INVALID_ADMIN_CREDENTIALS

    def authenticated_admin(self, admin_details):
        """Creates or updates an admin with the given details"""

        admin, is_new = get_one_or_create(self._db, Admin, email=admin_details["email"])

        if is_new and admin_details.get("roles"):
            for role in admin_details.get("roles"):
                if role.get("role") in AdminRole.ROLES:
                    library = Library.lookup(self._db, role.get("library"))
                    if role.get("library") and not library:
                        self.log.warn(
                            "%s authentication provider specified an unknown library for a new admin: %s"
                            % (admin_details.get("type"), role.get("library"))
                        )
                    else:
                        admin.add_role(role.get("role"), library)
                else:
                    self.log.warn(
                        "%s authentication provider specified an unknown role for a new admin: %s"
                        % (admin_details.get("type"), role.get("role"))
                    )

        # Set up the admin's flask session.
        flask.session["admin_email"] = admin_details.get("email")
        flask.session["auth_type"] = admin_details.get("type")

        # A permanent session expires after a fixed time, rather than
        # when the user closes the browser.
        flask.session.permanent = True

        # If this is the first time an admin has been authenticated,
        # make sure there is a value set for the sitewide BASE_URL_KEY
        # setting. If it's not set, set it to the hostname of the
        # current request. This assumes the first authenticated admin
        # is accessing the admin interface through the hostname they
        # want to be used for the site itself.
        base_url = ConfigurationSetting.sitewide(self._db, Configuration.BASE_URL_KEY)
        if not base_url.value:
            base_url.value = urllib.parse.urljoin(flask.request.url, "/")

        return admin

    def check_csrf_token(self):
        """Verifies that the CSRF token in the form data or X-CSRF-Token header
        matches the one in the session cookie.
        """
        cookie_token = self.get_csrf_token()
        header_token = flask.request.headers.get("X-CSRF-Token")
        if not cookie_token or cookie_token != header_token:
            return INVALID_CSRF_TOKEN
        return cookie_token

    def get_csrf_token(self):
        """Returns the CSRF token for the current session."""
        return flask.request.cookies.get("csrf_token")

    def generate_csrf_token(self):
        """Generate a random CSRF token."""
        return base64.b64encode(os.urandom(24)).decode("utf-8")


class AdminPermissionsControllerMixin:
    """Mixin that provides methods for verifying an admin's roles."""

    def require_system_admin(self):
        admin = getattr(flask.request, "admin", None)
        if not admin or not admin.is_system_admin():
            raise AdminNotAuthorized()

    def require_sitewide_library_manager(self):
        admin = getattr(flask.request, "admin", None)
        if not admin or not admin.is_sitewide_library_manager():
            raise AdminNotAuthorized()

    def require_library_manager(self, library):
        admin = getattr(flask.request, "admin", None)
        if not admin or not admin.is_library_manager(library):
            raise AdminNotAuthorized()

    def require_librarian(self, library):
        admin = getattr(flask.request, "admin", None)
        if not admin or not admin.is_librarian(library):
            raise AdminNotAuthorized()
