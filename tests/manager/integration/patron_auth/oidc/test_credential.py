"""Unit tests for OIDC credential manager."""

from __future__ import annotations

import datetime
import json
from unittest.mock import Mock

import pytest

from palace.manager.integration.patron_auth.oidc.auth import OIDCRefreshTokenError
from palace.manager.integration.patron_auth.oidc.credential import OIDCCredentialManager
from palace.manager.sqlalchemy.model.credential import Credential
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


@pytest.fixture
def manager():
    """Create credential manager instance."""
    return OIDCCredentialManager()


@pytest.fixture
def mock_patron(db: DatabaseTransactionFixture):
    """Create a mock patron."""
    library = db.default_library()
    patron = db.patron()
    patron.library_id = library.id
    db.session.flush()
    return patron


@pytest.fixture
def sample_id_token_claims():
    """Sample ID token claims."""
    return {
        "iss": "https://oidc.test.example.com",
        "sub": "test-user-123",
        "aud": "test-client-id",
        "exp": int((utc_now() + datetime.timedelta(hours=1)).timestamp()),
        "iat": int(utc_now().timestamp()),
        "email": "test@example.com",
    }


class TestOIDCCredentialManager:
    """Tests for OIDCCredentialManager."""

    def test_create_token_value_with_refresh_token(
        self, manager, sample_id_token_claims
    ):
        """Test creating token value with all fields."""
        token_value = manager._create_token_value(
            sample_id_token_claims,
            "test-access-token",
            "test-refresh-token",
        )

        token_data = json.loads(token_value)
        assert token_data["id_token_claims"] == sample_id_token_claims
        assert token_data["access_token"] == "test-access-token"
        assert token_data["refresh_token"] == "test-refresh-token"

    def test_create_token_value_without_refresh_token(
        self, manager, sample_id_token_claims
    ):
        """Test creating token value without refresh token."""
        token_value = manager._create_token_value(
            sample_id_token_claims,
            "test-access-token",
        )

        token_data = json.loads(token_value)
        assert token_data["id_token_claims"] == sample_id_token_claims
        assert token_data["access_token"] == "test-access-token"
        assert "refresh_token" not in token_data

    def test_extract_token_data_success(
        self,
        manager,
        db: DatabaseTransactionFixture,
        mock_patron,
        sample_id_token_claims,
    ):
        """Test successful token data extraction."""
        data_source = DataSource.lookup(
            db.session, manager.TOKEN_DATA_SOURCE_NAME, autocreate=True
        )

        token_value = manager._create_token_value(
            sample_id_token_claims,
            "test-access-token",
            "test-refresh-token",
        )

        credential = Credential(
            patron=mock_patron,
            data_source=data_source,
            type=manager.TOKEN_TYPE,
            credential=token_value,
        )

        extracted = manager.extract_token_data(credential)

        assert extracted["id_token_claims"] == sample_id_token_claims
        assert extracted["access_token"] == "test-access-token"
        assert extracted["refresh_token"] == "test-refresh-token"

    def test_extract_token_data_invalid_json(
        self, manager, db: DatabaseTransactionFixture, mock_patron
    ):
        """Test token data extraction with invalid JSON."""
        data_source = DataSource.lookup(
            db.session, manager.TOKEN_DATA_SOURCE_NAME, autocreate=True
        )

        credential = Credential(
            patron=mock_patron,
            data_source=data_source,
            type=manager.TOKEN_TYPE,
            credential="invalid json {",
        )

        with pytest.raises(ValueError) as exc_info:
            manager.extract_token_data(credential)
        assert "Invalid OIDC token format" in str(exc_info.value)

    def test_extract_token_data_missing_id_token_claims(
        self, manager, db: DatabaseTransactionFixture, mock_patron
    ):
        """Test token data extraction with missing id_token_claims."""
        data_source = DataSource.lookup(
            db.session, manager.TOKEN_DATA_SOURCE_NAME, autocreate=True
        )

        credential = Credential(
            patron=mock_patron,
            data_source=data_source,
            type=manager.TOKEN_TYPE,
            credential=json.dumps({"access_token": "test-access-token"}),
        )

        with pytest.raises(ValueError) as exc_info:
            manager.extract_token_data(credential)
        assert "missing id_token_claims" in str(exc_info.value)

    def test_extract_token_data_missing_access_token(
        self,
        manager,
        db: DatabaseTransactionFixture,
        mock_patron,
        sample_id_token_claims,
    ):
        """Test token data extraction with missing access_token."""
        data_source = DataSource.lookup(
            db.session, manager.TOKEN_DATA_SOURCE_NAME, autocreate=True
        )

        credential = Credential(
            patron=mock_patron,
            data_source=data_source,
            type=manager.TOKEN_TYPE,
            credential=json.dumps({"id_token_claims": sample_id_token_claims}),
        )

        with pytest.raises(ValueError) as exc_info:
            manager.extract_token_data(credential)
        assert "missing access_token" in str(exc_info.value)

    def test_create_oidc_token_with_session_lifetime_override(
        self,
        manager,
        db: DatabaseTransactionFixture,
        mock_patron,
        sample_id_token_claims,
    ):
        """Test creating OIDC token with session lifetime override."""
        session_lifetime_days = 30

        credential = manager.create_oidc_token(
            db.session,
            mock_patron,
            sample_id_token_claims,
            "test-access-token",
            "test-refresh-token",
            expires_in=3600,
            session_lifetime_days=session_lifetime_days,
        )

        assert credential.patron == mock_patron
        assert credential.type == manager.TOKEN_TYPE
        assert credential.data_source.name == manager.TOKEN_DATA_SOURCE_NAME

        expected_expiry = utc_now() + datetime.timedelta(days=session_lifetime_days)
        assert credential.expires is not None
        assert abs((credential.expires - expected_expiry).total_seconds()) < 5

        token_data = manager.extract_token_data(credential)
        assert token_data["id_token_claims"] == sample_id_token_claims
        assert token_data["access_token"] == "test-access-token"
        assert token_data["refresh_token"] == "test-refresh-token"

    def test_create_oidc_token_with_provider_expiry(
        self,
        manager,
        db: DatabaseTransactionFixture,
        mock_patron,
        sample_id_token_claims,
    ):
        """Test creating OIDC token with provider expiry."""
        expires_in_seconds = 3600

        credential = manager.create_oidc_token(
            db.session,
            mock_patron,
            sample_id_token_claims,
            "test-access-token",
            expires_in=expires_in_seconds,
        )

        expected_expiry = utc_now() + datetime.timedelta(seconds=expires_in_seconds)
        assert credential.expires is not None
        assert abs((credential.expires - expected_expiry).total_seconds()) < 5

    def test_create_oidc_token_with_default_expiry(
        self,
        manager,
        db: DatabaseTransactionFixture,
        mock_patron,
        sample_id_token_claims,
    ):
        """Test creating OIDC token with default 24-hour expiry."""
        credential = manager.create_oidc_token(
            db.session,
            mock_patron,
            sample_id_token_claims,
            "test-access-token",
        )

        expected_expiry = utc_now() + datetime.timedelta(hours=24)
        assert credential.expires is not None
        assert abs((credential.expires - expected_expiry).total_seconds()) < 5

    def test_create_oidc_token_without_refresh_token(
        self,
        manager,
        db: DatabaseTransactionFixture,
        mock_patron,
        sample_id_token_claims,
    ):
        """Test creating OIDC token without refresh token."""
        credential = manager.create_oidc_token(
            db.session,
            mock_patron,
            sample_id_token_claims,
            "test-access-token",
            refresh_token=None,
            expires_in=3600,
        )

        token_data = manager.extract_token_data(credential)
        assert "refresh_token" not in token_data

    def test_lookup_oidc_token_by_patron_found(
        self,
        manager,
        db: DatabaseTransactionFixture,
        mock_patron,
        sample_id_token_claims,
    ):
        """Test looking up OIDC token by patron when it exists."""
        created_credential = manager.create_oidc_token(
            db.session,
            mock_patron,
            sample_id_token_claims,
            "test-access-token",
            "test-refresh-token",
        )
        db.session.commit()

        found_credential = manager.lookup_oidc_token_by_patron(db.session, mock_patron)

        assert found_credential is not None
        assert found_credential.id == created_credential.id
        assert found_credential.patron == mock_patron

    def test_lookup_oidc_token_by_patron_not_found(
        self, manager, db: DatabaseTransactionFixture, mock_patron
    ):
        """Test looking up OIDC token by patron when it doesn't exist."""
        found_credential = manager.lookup_oidc_token_by_patron(db.session, mock_patron)

        assert found_credential is None

    def test_lookup_oidc_token_by_value_found(
        self,
        manager,
        db: DatabaseTransactionFixture,
        mock_patron,
        sample_id_token_claims,
    ):
        """Test looking up OIDC token by value when it exists."""
        credential = manager.create_oidc_token(
            db.session,
            mock_patron,
            sample_id_token_claims,
            "test-access-token",
            "test-refresh-token",
        )
        db.session.commit()

        token_value = credential.credential
        library_id = mock_patron.library_id

        found_credential = manager.lookup_oidc_token_by_value(
            db.session, token_value, library_id
        )

        assert found_credential is not None
        assert found_credential.id == credential.id

    def test_lookup_oidc_token_by_value_wrong_library(
        self,
        manager,
        db: DatabaseTransactionFixture,
        mock_patron,
        sample_id_token_claims,
    ):
        """Test looking up OIDC token with wrong library ID."""
        credential = manager.create_oidc_token(
            db.session,
            mock_patron,
            sample_id_token_claims,
            "test-access-token",
            "test-refresh-token",
        )
        db.session.commit()

        token_value = credential.credential
        wrong_library_id = 999999

        found_credential = manager.lookup_oidc_token_by_value(
            db.session, token_value, wrong_library_id
        )

        assert found_credential is None

    def test_lookup_oidc_token_by_value_not_found(
        self, manager, db: DatabaseTransactionFixture, mock_patron
    ):
        """Test looking up OIDC token by value when it doesn't exist."""
        library_id = mock_patron.library_id

        found_credential = manager.lookup_oidc_token_by_value(
            db.session, "nonexistent-token", library_id
        )

        assert found_credential is None

    def test_refresh_token_if_needed_not_expired(
        self,
        manager,
        db: DatabaseTransactionFixture,
        mock_patron,
        sample_id_token_claims,
    ):
        """Test refresh_token_if_needed when token is still valid."""
        expires_in_seconds = 3600

        credential = manager.create_oidc_token(
            db.session,
            mock_patron,
            sample_id_token_claims,
            "test-access-token",
            "test-refresh-token",
            expires_in=expires_in_seconds,
        )
        db.session.commit()

        original_expiry = credential.expires
        original_token = credential.credential

        mock_auth_manager = Mock()

        refreshed_credential = manager.refresh_token_if_needed(
            db.session, credential, mock_auth_manager, buffer_seconds=300
        )

        assert refreshed_credential == credential
        assert refreshed_credential.expires == original_expiry
        assert refreshed_credential.credential == original_token
        mock_auth_manager.refresh_access_token.assert_not_called()

    def test_refresh_token_if_needed_expired(
        self,
        manager,
        db: DatabaseTransactionFixture,
        mock_patron,
        sample_id_token_claims,
    ):
        """Test refresh_token_if_needed when token is expired."""
        credential = manager.create_oidc_token(
            db.session,
            mock_patron,
            sample_id_token_claims,
            "old-access-token",
            "old-refresh-token",
            expires_in=100,
        )

        credential.expires = utc_now() - datetime.timedelta(seconds=10)
        db.session.commit()

        mock_auth_manager = Mock()
        new_tokens = {
            "access_token": "new-access-token",
            "refresh_token": "new-refresh-token",
            "expires_in": 3600,
        }
        mock_auth_manager.refresh_access_token.return_value = new_tokens

        refreshed_credential = manager.refresh_token_if_needed(
            db.session, credential, mock_auth_manager
        )

        mock_auth_manager.refresh_access_token.assert_called_once_with(
            "old-refresh-token"
        )

        token_data = manager.extract_token_data(refreshed_credential)
        assert token_data["access_token"] == "new-access-token"
        assert token_data["refresh_token"] == "new-refresh-token"

        expected_expiry = utc_now() + datetime.timedelta(seconds=3600)
        assert refreshed_credential.expires is not None
        assert abs((refreshed_credential.expires - expected_expiry).total_seconds()) < 5

    def test_refresh_token_if_needed_expiring_soon(
        self,
        manager,
        db: DatabaseTransactionFixture,
        mock_patron,
        sample_id_token_claims,
    ):
        """Test refresh_token_if_needed when token is expiring within buffer period."""
        credential = manager.create_oidc_token(
            db.session,
            mock_patron,
            sample_id_token_claims,
            "old-access-token",
            "old-refresh-token",
            expires_in=200,
        )

        credential.expires = utc_now() + datetime.timedelta(seconds=200)
        db.session.commit()

        mock_auth_manager = Mock()
        new_tokens = {
            "access_token": "new-access-token",
            "expires_in": 3600,
        }
        mock_auth_manager.refresh_access_token.return_value = new_tokens

        buffer_seconds = 300

        refreshed_credential = manager.refresh_token_if_needed(
            db.session, credential, mock_auth_manager, buffer_seconds=buffer_seconds
        )

        mock_auth_manager.refresh_access_token.assert_called_once()

        token_data = manager.extract_token_data(refreshed_credential)
        assert token_data["access_token"] == "new-access-token"
        assert token_data["refresh_token"] == "old-refresh-token"

    def test_refresh_token_if_needed_no_refresh_token(
        self,
        manager,
        db: DatabaseTransactionFixture,
        mock_patron,
        sample_id_token_claims,
    ):
        """Test refresh_token_if_needed when credential has no refresh token."""
        credential = manager.create_oidc_token(
            db.session,
            mock_patron,
            sample_id_token_claims,
            "test-access-token",
            refresh_token=None,
            expires_in=100,
        )

        credential.expires = utc_now() - datetime.timedelta(seconds=10)
        db.session.commit()

        mock_auth_manager = Mock()

        with pytest.raises(OIDCRefreshTokenError) as exc_info:
            manager.refresh_token_if_needed(db.session, credential, mock_auth_manager)

        assert "no refresh token" in str(exc_info.value)
        mock_auth_manager.refresh_access_token.assert_not_called()

    def test_refresh_token_if_needed_refresh_fails(
        self,
        manager,
        db: DatabaseTransactionFixture,
        mock_patron,
        sample_id_token_claims,
    ):
        """Test refresh_token_if_needed when refresh operation fails."""
        credential = manager.create_oidc_token(
            db.session,
            mock_patron,
            sample_id_token_claims,
            "old-access-token",
            "old-refresh-token",
            expires_in=100,
        )

        credential.expires = utc_now() - datetime.timedelta(seconds=10)
        db.session.commit()

        mock_auth_manager = Mock()
        mock_auth_manager.refresh_access_token.side_effect = OIDCRefreshTokenError(
            "Refresh failed"
        )

        with pytest.raises(OIDCRefreshTokenError):
            manager.refresh_token_if_needed(db.session, credential, mock_auth_manager)

    def test_refresh_token_if_needed_with_new_id_token(
        self,
        manager,
        db: DatabaseTransactionFixture,
        mock_patron,
        sample_id_token_claims,
    ):
        """Test refresh_token_if_needed when refresh response includes new ID token."""
        credential = manager.create_oidc_token(
            db.session,
            mock_patron,
            sample_id_token_claims,
            "old-access-token",
            "old-refresh-token",
            expires_in=100,
        )

        credential.expires = utc_now() - datetime.timedelta(seconds=10)
        db.session.commit()

        new_id_token_claims = sample_id_token_claims.copy()
        new_id_token_claims["updated"] = True

        mock_auth_manager = Mock()
        new_tokens = {
            "access_token": "new-access-token",
            "id_token": "new-id-token-jwt",
            "expires_in": 3600,
        }
        mock_auth_manager.refresh_access_token.return_value = new_tokens
        mock_auth_manager.validate_id_token.return_value = new_id_token_claims

        refreshed_credential = manager.refresh_token_if_needed(
            db.session, credential, mock_auth_manager
        )

        mock_auth_manager.validate_id_token.assert_called_once_with(
            "new-id-token-jwt", nonce=None
        )

        token_data = manager.extract_token_data(refreshed_credential)
        assert token_data["id_token_claims"]["updated"] is True

    def test_refresh_token_if_needed_id_token_validation_fails(
        self,
        manager,
        db: DatabaseTransactionFixture,
        mock_patron,
        sample_id_token_claims,
    ):
        """Test refresh_token_if_needed when new ID token validation fails."""
        credential = manager.create_oidc_token(
            db.session,
            mock_patron,
            sample_id_token_claims,
            "old-access-token",
            "old-refresh-token",
            expires_in=100,
        )

        credential.expires = utc_now() - datetime.timedelta(seconds=10)
        db.session.commit()

        mock_auth_manager = Mock()
        new_tokens = {
            "access_token": "new-access-token",
            "id_token": "invalid-id-token",
            "expires_in": 3600,
        }
        mock_auth_manager.refresh_access_token.return_value = new_tokens
        mock_auth_manager.validate_id_token.side_effect = Exception("Invalid ID token")

        refreshed_credential = manager.refresh_token_if_needed(
            db.session, credential, mock_auth_manager
        )

        token_data = manager.extract_token_data(refreshed_credential)
        assert token_data["id_token_claims"] == sample_id_token_claims
        assert token_data["access_token"] == "new-access-token"

    def test_refresh_token_if_needed_no_expiry_in_response(
        self,
        manager,
        db: DatabaseTransactionFixture,
        mock_patron,
        sample_id_token_claims,
    ):
        """Test refresh_token_if_needed when refresh response has no expiry."""
        credential = manager.create_oidc_token(
            db.session,
            mock_patron,
            sample_id_token_claims,
            "old-access-token",
            "old-refresh-token",
            expires_in=100,
        )

        original_expiry = credential.expires
        credential.expires = utc_now() - datetime.timedelta(seconds=10)
        db.session.commit()

        mock_auth_manager = Mock()
        new_tokens = {
            "access_token": "new-access-token",
        }
        mock_auth_manager.refresh_access_token.return_value = new_tokens

        refreshed_credential = manager.refresh_token_if_needed(
            db.session, credential, mock_auth_manager
        )

        token_data = manager.extract_token_data(refreshed_credential)
        assert token_data["access_token"] == "new-access-token"

    def test_refresh_token_if_needed_no_expiry_on_credential(
        self,
        manager,
        db: DatabaseTransactionFixture,
        mock_patron,
        sample_id_token_claims,
    ):
        """Test refresh_token_if_needed when credential has no expiry set."""
        credential = manager.create_oidc_token(
            db.session,
            mock_patron,
            sample_id_token_claims,
            "test-access-token",
            "test-refresh-token",
            expires_in=3600,
        )

        credential.expires = None
        db.session.commit()

        mock_auth_manager = Mock()

        refreshed_credential = manager.refresh_token_if_needed(
            db.session, credential, mock_auth_manager
        )

        assert refreshed_credential == credential
        mock_auth_manager.refresh_access_token.assert_not_called()


class TestOIDCCredentialManagerLogout:
    """Tests for logout-related credential operations."""

    def test_lookup_patron_by_identifier(self, db, manager):
        patron = db.patron()
        patron.authorization_identifier = "user123@example.com"
        db.session.commit()

        found_patron = manager.lookup_patron_by_identifier(
            db.session, "user123@example.com"
        )

        assert found_patron is not None
        assert found_patron.id == patron.id

    def test_lookup_patron_by_identifier_not_found(self, db, manager):
        found_patron = manager.lookup_patron_by_identifier(
            db.session, "nonexistent@example.com"
        )

        assert found_patron is None

    def test_invalidate_credential(self, db, manager):
        from palace.manager.util.datetime_helpers import utc_now

        patron = db.patron()
        credential = manager.create_oidc_token(
            db.session,
            patron,
            {"sub": "user123", "email": "user@example.com"},
            "access-token",
            "refresh-token",
            3600,
        )

        future_time = utc_now() + datetime.timedelta(hours=1)
        assert credential.expires > utc_now()

        manager.invalidate_credential(db.session, credential.id)

        db.session.refresh(credential)
        assert credential.expires <= utc_now()

    def test_invalidate_credential_not_found(self, db, manager):
        manager.invalidate_credential(db.session, 999999)

    def test_invalidate_patron_credentials(self, db, manager):
        from palace.manager.util.datetime_helpers import utc_now

        patron = db.patron()

        # Create OIDC credential for patron
        credential = manager.create_oidc_token(
            db.session,
            patron,
            {"sub": "user123"},
            "access-token-1",
            "refresh-token-1",
            3600,
        )

        # Verify credential is not yet expired
        assert credential.expires > utc_now()

        # Invalidate all credentials for patron
        count = manager.invalidate_patron_credentials(db.session, patron.id)

        # Should invalidate exactly one credential (each patron has one OIDC credential)
        assert count == 1

        db.session.refresh(credential)
        assert credential.expires <= utc_now()

    def test_invalidate_patron_credentials_no_credentials(self, db, manager):
        patron = db.patron()

        count = manager.invalidate_patron_credentials(db.session, patron.id)

        assert count == 0
