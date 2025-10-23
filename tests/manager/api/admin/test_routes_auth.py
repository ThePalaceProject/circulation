"""Tests for the requires_auth decorator in admin routes."""

import base64

from flask import Response

from palace.manager.api.admin.problem_details import INVALID_ADMIN_CREDENTIALS
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture


class TestRequiresAuthDecorator:
    """Tests for the requires_auth decorator that supports Bearer token authentication."""

    def test_bearer_token_authentication_success(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test successful authentication with valid Bearer token."""
        # Create an admin user with password
        admin = db.admin(email="admin@example.com", password="password123")

        # Create Bearer token: base64(email:password)
        credentials = "admin@example.com:password123"
        token = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

        with flask_app_fixture.test_request_context(
            "/admin/import-libraries",
            method="POST",
            headers={"Authorization": f"Bearer {token}"},
            json={"libraries": []},
        ):
            # The decorator should authenticate the user
            from palace.manager.api.admin import routes

            response = routes.import_libraries()

            # Should get a successful response (200 or 207), not 401
            assert isinstance(response, Response)
            assert response.status_code in [200, 207]
            assert response.json is not None
            assert response.json["result"] == "success"

    def test_bearer_token_authentication_invalid_credentials(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test that invalid credentials return 401."""
        # Create an admin user
        admin = db.admin(email="admin@example.com", password="password123")

        # Use wrong password
        credentials = "admin@example.com:wrongpassword"
        token = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

        with flask_app_fixture.test_request_context(
            "/admin/import-libraries",
            method="POST",
            headers={"Authorization": f"Bearer {token}"},
            json={"libraries": []},
        ):
            from palace.manager.api.admin import routes

            response = routes.import_libraries()

            assert isinstance(response, Response)
            assert response.status_code == INVALID_ADMIN_CREDENTIALS.status_code

    def test_bearer_token_authentication_missing_header(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test that missing Authorization header returns 401."""
        with flask_app_fixture.test_request_context(
            "/admin/import-libraries",
            method="POST",
            json={"libraries": []},
        ):
            from palace.manager.api.admin import routes

            response = routes.import_libraries()

            assert isinstance(response, Response)
            assert response.status_code == INVALID_ADMIN_CREDENTIALS.status_code

    def test_bearer_token_authentication_invalid_format(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test that invalid Authorization header format returns 401."""
        # Not a Bearer token
        with flask_app_fixture.test_request_context(
            "/admin/import-libraries",
            method="POST",
            headers={"Authorization": "Basic sometoken"},
            json={"libraries": []},
        ):
            from palace.manager.api.admin import routes

            response = routes.import_libraries()

            assert isinstance(response, Response)
            assert response.status_code == INVALID_ADMIN_CREDENTIALS.status_code

    def test_bearer_token_authentication_invalid_base64(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test that invalid base64 encoding returns 401."""
        with flask_app_fixture.test_request_context(
            "/admin/import-libraries",
            method="POST",
            headers={"Authorization": "Bearer not-valid-base64!!!"},
            json={"libraries": []},
        ):
            from palace.manager.api.admin import routes

            response = routes.import_libraries()

            assert isinstance(response, Response)
            assert response.status_code == INVALID_ADMIN_CREDENTIALS.status_code

    def test_bearer_token_authentication_missing_colon(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test that credentials without colon separator returns 401."""
        # Encode credentials without colon
        credentials = "adminexample.com"
        token = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

        with flask_app_fixture.test_request_context(
            "/admin/import-libraries",
            method="POST",
            headers={"Authorization": f"Bearer {token}"},
            json={"libraries": []},
        ):
            from palace.manager.api.admin import routes

            response = routes.import_libraries()

            assert isinstance(response, Response)
            assert response.status_code == INVALID_ADMIN_CREDENTIALS.status_code

    def test_bearer_token_authentication_nonexistent_user(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test that nonexistent user returns 401."""
        # Valid format but user doesn't exist
        credentials = "nonexistent@example.com:password123"
        token = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

        with flask_app_fixture.test_request_context(
            "/admin/import-libraries",
            method="POST",
            headers={"Authorization": f"Bearer {token}"},
            json={"libraries": []},
        ):
            from palace.manager.api.admin import routes

            response = routes.import_libraries()

            assert isinstance(response, Response)
            assert response.status_code == INVALID_ADMIN_CREDENTIALS.status_code

    def test_session_authentication_still_works(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test that session-based authentication still works."""
        # Create an admin user
        admin = db.admin(email="admin@example.com", password="password123")

        with flask_app_fixture.test_request_context_system_admin(
            "/admin/import-libraries",
            method="POST",
            json={"libraries": []},
        ):
            from palace.manager.api.admin import routes

            response = routes.import_libraries()

            # Should get a successful response
            assert isinstance(response, Response)
            assert response.status_code in [200, 207]

    def test_bearer_token_sets_session(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test that Bearer token authentication sets session for subsequent requests."""
        # Create an admin user
        admin = db.admin(email="admin@example.com", password="password123")

        # Create Bearer token
        credentials = "admin@example.com:password123"
        token = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

        with flask_app_fixture.test_request_context(
            "/admin/import-libraries",
            method="POST",
            headers={"Authorization": f"Bearer {token}"},
            json={"libraries": []},
        ):
            import flask

            from palace.manager.api.admin import routes

            response = routes.import_libraries()

            # Check that session was set
            assert flask.session.get("admin_email") == "admin@example.com"
            assert (
                flask.session.get("auth_type") == "PasswordAdminAuthenticationProvider"
            )

    def test_bearer_token_with_password_containing_colon(
        self,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test that passwords containing colons are handled correctly."""
        # Create an admin user with password containing colon
        admin = db.admin(email="admin@example.com", password="pass:word:123")

        # Create Bearer token with password containing colons
        credentials = "admin@example.com:pass:word:123"
        token = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

        with flask_app_fixture.test_request_context(
            "/admin/import-libraries",
            method="POST",
            headers={"Authorization": f"Bearer {token}"},
            json={"libraries": []},
        ):
            from palace.manager.api.admin import routes

            response = routes.import_libraries()

            # Should successfully authenticate
            assert isinstance(response, Response)
            assert response.status_code in [200, 207]
