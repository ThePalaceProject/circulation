"""Tests for admin route decorators, particularly the requires_auth decorator."""

import base64
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import flask
import pytest

from palace.manager.api.admin.routes import requires_auth
from palace.manager.sqlalchemy.model.admin import Admin
from palace.manager.util.problem_detail import ProblemDetail
from tests.fixtures.database import DatabaseTransactionFixture


class TestRequiresAuthDecorator:
    """Tests for the @requires_auth decorator."""

    @pytest.fixture
    def mock_flask_app(self):
        """Create a mock Flask app for testing."""
        app = flask.Flask(__name__)
        app.config["TESTING"] = True
        app.config["SECRET_KEY"] = "test-secret-key"
        return app

    @pytest.fixture
    def mock_manager(self, db: DatabaseTransactionFixture):
        """Create a mock manager with database session."""
        manager = MagicMock()
        manager._db = db.session
        return manager

    @pytest.fixture
    def test_admin(self, db: DatabaseTransactionFixture):
        """Create a test admin user."""
        admin = Admin(email="test@example.com")
        admin.password = "test_password"
        db.session.add(admin)
        db.session.commit()
        return admin

    @pytest.fixture
    def test_endpoint(self):
        """Create a standard test endpoint decorated with @requires_auth."""

        @requires_auth
        def endpoint():
            return {"success": True}

        return endpoint

    @pytest.fixture
    def auth_context(self, mock_flask_app, mock_manager):
        """Provide a context manager for making authenticated requests."""

        @contextmanager
        def _auth_context(headers=None, mock_authenticate=None, admin=None):
            """
            Context manager that sets up authentication mocking.

            Args:
                headers: Optional dict of HTTP headers
                mock_authenticate: Optional Admin instance to return from authenticate()
                admin: Optional Admin instance to use for is_system_admin check
            """
            with mock_flask_app.test_request_context(headers=headers or {}):
                with patch("palace.manager.api.admin.routes.app") as mock_app:
                    mock_app.manager = mock_manager

                    if mock_authenticate is not None and admin is not None:
                        with patch.object(
                            Admin, "authenticate"
                        ) as auth_mock, patch.object(
                            admin, "is_system_admin"
                        ) as is_system_admin:
                            auth_mock.return_value = mock_authenticate
                            is_system_admin.return_value = True
                            yield
                    elif mock_authenticate is not None:
                        with patch.object(Admin, "authenticate") as auth_mock:
                            auth_mock.return_value = mock_authenticate
                            yield
                    else:
                        yield

        return _auth_context

    @staticmethod
    def encode_credentials(email: str, password: str) -> str:
        """Helper to encode credentials as base64."""
        credentials = f"{email}:{password}".encode()
        return base64.b64encode(credentials).decode("utf-8")

    @staticmethod
    def assert_invalid_credentials(result: ProblemDetail):
        """Helper to assert that result is an invalid credentials error."""
        assert isinstance(result, ProblemDetail)
        assert result.status_code == 401
        assert "admin-credentials-invalid" in result.uri

    def test_requires_auth_no_authorization_header(self, test_endpoint, auth_context):
        """Test that missing Authorization header returns invalid credentials error."""
        with auth_context():
            result = test_endpoint()
        self.assert_invalid_credentials(result)

    def test_requires_auth_invalid_authorization_format(
        self, test_endpoint, auth_context
    ):
        """Test that invalid Authorization header format returns error."""
        with auth_context(headers={"Authorization": "InvalidFormat abc123"}):
            result = test_endpoint()
        self.assert_invalid_credentials(result)

    def test_requires_auth_invalid_bearer_token_format(
        self, test_endpoint, auth_context
    ):
        """Test that malformed Bearer token returns error."""
        with auth_context(headers={"Authorization": "Bearer not_valid_base64!@#$%"}):
            result = test_endpoint()
        self.assert_invalid_credentials(result)

    def test_requires_auth_missing_colon_in_credentials(
        self, test_endpoint, auth_context
    ):
        """Test that credentials without colon separator return error."""
        invalid_credentials = base64.b64encode(b"emailwithoutcolon").decode("utf-8")
        with auth_context(headers={"Authorization": f"Bearer {invalid_credentials}"}):
            result = test_endpoint()
        self.assert_invalid_credentials(result)

    def test_requires_auth_invalid_credentials(self, test_endpoint, auth_context):
        """Test that invalid username/password returns error."""
        credentials = self.encode_credentials("wrong@example.com", "wrongpassword")
        with auth_context(headers={"Authorization": f"Bearer {credentials}"}):
            result = test_endpoint()
        self.assert_invalid_credentials(result)

    def test_requires_auth_valid_credentials(
        self, test_endpoint, auth_context, test_admin
    ):
        """Test successful authentication with valid credentials."""
        credentials = self.encode_credentials("user@email.com", "password")
        headers = {"Authorization": f"Bearer {credentials}"}

        with auth_context(
            headers=headers, mock_authenticate=test_admin, admin=test_admin
        ):
            result = test_endpoint()

        assert result == {"success": True}

    def test_requires_auth_admin_not_authorized(
        self, test_endpoint, auth_context, test_admin
    ):
        """Test that non-system admin returns 403 error."""
        credentials = self.encode_credentials("user@email.com", "password")
        headers = {"Authorization": f"Bearer {credentials}"}

        with auth_context(headers=headers, mock_authenticate=test_admin):
            with patch.object(test_admin, "is_system_admin", return_value=False):
                result = test_endpoint()

        assert isinstance(result, ProblemDetail)
        assert result.status_code == 403
        assert "admin-not-authorized" in result.uri

    def test_requires_auth_with_empty_password(self, test_endpoint, auth_context):
        """Test that empty password in credentials returns error."""
        credentials = base64.b64encode(b"test@example.com:").decode("utf-8")
        with auth_context(headers={"Authorization": f"Bearer {credentials}"}):
            result = test_endpoint()
        self.assert_invalid_credentials(result)

    def test_requires_auth_with_empty_email(self, test_endpoint, auth_context):
        """Test that empty email in credentials returns error."""
        credentials = base64.b64encode(b":password").decode("utf-8")
        with auth_context(headers={"Authorization": f"Bearer {credentials}"}):
            result = test_endpoint()
        self.assert_invalid_credentials(result)
