"""Tests for admin route decorators, particularly the requires_auth decorator."""

import base64
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import flask
import pytest

from palace.manager.api.admin.routes import requires_auth
from palace.manager.util.problem_detail import ProblemDetail
from tests.fixtures.database import DatabaseTransactionFixture


class TestRequiresAuthDecorator:
    """Tests for the @requires_auth decorator."""

    ADMIN_EMAIL = "test@example.com"
    ADMIN_PASSWORD = "password"

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
        return db.admin(email=self.ADMIN_EMAIL, password=self.ADMIN_PASSWORD)

    @pytest.fixture
    def test_system_admin(self, db: DatabaseTransactionFixture):
        """Create a test admin user."""
        return db.admin(
            email=self.ADMIN_EMAIL, password=self.ADMIN_PASSWORD, is_system_admin=True
        )

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
        def _auth_context(headers=None):
            """Context manager that sets up authentication mocking.

            :param headers: Optional dict of HTTP headers
            """
            with mock_flask_app.test_request_context(headers=headers or {}):
                with patch("palace.manager.api.admin.routes.app") as mock_app:
                    mock_app.manager = mock_manager
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
        self, test_endpoint, auth_context, test_system_admin
    ):
        """Test successful authentication with valid credentials."""
        credentials = self.encode_credentials(self.ADMIN_EMAIL, self.ADMIN_PASSWORD)
        headers = {"Authorization": f"Bearer {credentials}"}

        with auth_context(headers=headers):
            result = test_endpoint()

        assert result == {"success": True}

    def test_requires_auth_admin_not_authorized(
        self, test_endpoint, auth_context, test_admin
    ):
        """Test that non-system admin returns 403 error."""
        credentials = self.encode_credentials(self.ADMIN_EMAIL, self.ADMIN_PASSWORD)
        headers = {"Authorization": f"Bearer {credentials}"}

        with auth_context(headers=headers):
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
