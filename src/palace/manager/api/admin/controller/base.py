from __future__ import annotations

import base64
import os

import flask

from palace.manager.api.admin.exceptions import AdminNotAuthorized
from palace.manager.api.admin.password_admin_authentication_provider import (
    PasswordAdminAuthenticationProvider,
)
from palace.manager.api.admin.problem_details import (
    ADMIN_AUTH_MECHANISM_NOT_CONFIGURED,
    ADMIN_AUTH_NOT_CONFIGURED,
    INVALID_ADMIN_CREDENTIALS,
    INVALID_CSRF_TOKEN,
)
from palace.manager.api.admin.util.flask import get_request_admin
from palace.manager.sqlalchemy.model.admin import Admin, AdminRole
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.util import get_one, get_one_or_create
from palace.manager.util.log import LoggerMixin
from palace.manager.util.problem_detail import ProblemDetail


class AdminController(LoggerMixin):
    def __init__(self, manager):
        self.manager = manager
        self._db = self.manager._db
        self.send_email = self.manager.services.email.send_email

    @property
    def admin_auth_providers(self):
        if Admin.with_password(self._db).count() != 0:
            return [PasswordAdminAuthenticationProvider(self.send_email)]

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
        setattr(flask.request, "admin", None)
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
                setattr(flask.request, "admin", admin)
                return admin
        return INVALID_ADMIN_CREDENTIALS

    def authenticated_admin(self, admin_details) -> Admin:
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

        return admin

    def check_csrf_token(self) -> str | ProblemDetail:
        """Verifies that the CSRF token in the form data or X-CSRF-Token header
        matches the one in the session cookie.
        """
        cookie_token = self.get_csrf_token()
        header_token = flask.request.headers.get("X-CSRF-Token")
        if not cookie_token or cookie_token != header_token:
            return INVALID_CSRF_TOKEN
        return cookie_token

    @staticmethod
    def get_csrf_token() -> str | None:
        """Returns the CSRF token for the current session."""
        return flask.request.cookies.get("csrf_token")

    @staticmethod
    def generate_csrf_token() -> str:
        """Generate a random CSRF token."""
        return base64.b64encode(os.urandom(24)).decode("utf-8")

    @staticmethod
    def validate_csrf_token(token: str) -> bool:
        """Validate that a CSRF token has the expected format.

        :param token: The token to validate
        :return: True if the token is valid, False otherwise
        """
        if not token or not isinstance(token, str):
            return False

        # Verify it's valid base64
        try:
            decoded = base64.b64decode(token, validate=True)
            # Should decode to exactly 24 bytes
            return len(decoded) == 24
        except Exception:
            return False


class AdminPermissionsControllerMixin:
    """Mixin that provides methods for verifying an admin's roles."""

    def require_system_admin(self) -> None:
        admin = get_request_admin(default=None)
        if not admin or not admin.is_system_admin():
            raise AdminNotAuthorized()

    def require_sitewide_library_manager(self) -> None:
        admin = get_request_admin(default=None)
        if not admin or not admin.is_sitewide_library_manager():
            raise AdminNotAuthorized()

    def require_library_manager(self, library: Library) -> None:
        admin = get_request_admin(default=None)
        if not admin or not admin.is_library_manager(library):
            raise AdminNotAuthorized()

    def require_librarian(self, library: Library) -> None:
        admin = get_request_admin(default=None)
        if not admin or not admin.is_librarian(library):
            raise AdminNotAuthorized()
