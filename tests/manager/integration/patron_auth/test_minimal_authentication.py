"""Tests for MinimalAuthenticationProvider."""

from __future__ import annotations

from functools import partial

import pytest

from palace.manager.api.authentication.base import PatronData
from palace.manager.api.authentication.basic import (
    BasicAuthProviderLibrarySettings,
    BasicAuthProviderSettings,
)
from palace.manager.integration.patron_auth.minimal_authentication import (
    MinimalAuthenticationProvider,
)
from tests.fixtures.database import DatabaseTransactionFixture


class MinimalAuthFixture:
    """Fixture for testing MinimalAuthenticationProvider."""

    def __init__(self) -> None:
        self.library_id = 1
        self.integration_id = 1
        self.settings = BasicAuthProviderSettings(test_identifier="test_user")
        self.library_settings = BasicAuthProviderLibrarySettings()
        self.provider = partial(
            MinimalAuthenticationProvider,
            library_id=self.library_id,
            integration_id=self.integration_id,
            settings=self.settings,
            library_settings=self.library_settings,
        )


@pytest.fixture
def minimal_auth_fixture() -> MinimalAuthFixture:
    return MinimalAuthFixture()


class TestMinimalAuthenticationProvider:
    """Tests for MinimalAuthenticationProvider."""

    def test_label_and_description(self):
        """Test provider label and description."""
        assert (
            MinimalAuthenticationProvider.label() == "Minimal Authentication Provider"
        )
        assert (
            "no op remote authentication" in MinimalAuthenticationProvider.description()
        )

    @pytest.mark.parametrize(
        "username, password, expect_success",
        [
            pytest.param(
                "user123", "password", True, id="valid_username_with_password"
            ),
            pytest.param("user123", None, True, id="valid_username_no_password"),
            pytest.param("user123", "", True, id="valid_username_empty_password"),
            pytest.param(None, "password", False, id="no_username_returns_none"),
            pytest.param("", "password", False, id="empty_username_returns_none"),
        ],
    )
    def test_remote_authenticate(
        self,
        minimal_auth_fixture: MinimalAuthFixture,
        username: str | None,
        password: str | None,
        expect_success: bool,
    ):
        """Test remote_authenticate accepts any username and ignores password."""
        provider = minimal_auth_fixture.provider()

        result = provider.remote_authenticate(username, password)

        if expect_success:
            assert isinstance(result, PatronData)
            assert result.authorization_identifier is not None
            assert result.permanent_id is not None
            assert result.username is not None
            assert result.personal_name is not None
        else:
            assert result is None

    @pytest.mark.parametrize(
        "authorization_identifier, expected_username, expected_permanent_id, expected_personal_name",
        [
            pytest.param(
                "user123",
                "user123_username",
                "user123_id",
                "PersonalNameuser123",
                id="identifier_without_username_suffix",
            ),
            pytest.param(
                "user456_username",
                "user456_username",
                "user456_id",
                "PersonalNameuser456",
                id="identifier_with_username_suffix",
            ),
            pytest.param(
                "abc",
                "abc_username",
                "abc_id",
                "PersonalNameabc",
                id="short_identifier",
            ),
        ],
    )
    def test_generate_patrondata(
        self,
        authorization_identifier: str,
        expected_username: str,
        expected_permanent_id: str,
        expected_personal_name: str,
    ):
        """Test generate_patrondata creates correct PatronData based on authorization_identifier."""
        patrondata = MinimalAuthenticationProvider.generate_patrondata(
            authorization_identifier
        )

        assert isinstance(patrondata, PatronData)
        assert patrondata.authorization_identifier == authorization_identifier.replace(
            "_username", ""
        )
        assert patrondata.username == expected_username
        assert patrondata.permanent_id == expected_permanent_id
        assert patrondata.personal_name == expected_personal_name
        assert patrondata.authorization_expires is None
        assert patrondata.fines is None

    @pytest.mark.parametrize(
        "authorization_identifier, expect_success",
        [
            pytest.param(
                "user123",
                True,
                id="patrondata_with_valid_identifier",
            ),
            pytest.param(
                "test_username",
                True,
                id="patrondata_with_username_suffix",
            ),
            pytest.param(
                None,
                False,
                id="patrondata_with_none_identifier_returns_none",
            ),
            pytest.param(
                "",
                False,
                id="patrondata_with_empty_identifier_returns_none",
            ),
        ],
    )
    def test_remote_patron_lookup(
        self,
        minimal_auth_fixture: MinimalAuthFixture,
        authorization_identifier: str | None,
        expect_success: bool,
    ):
        """Test remote_patron_lookup returns PatronData for valid inputs."""
        provider = minimal_auth_fixture.provider()

        # Create input based on type
        patron_input = PatronData(authorization_identifier=authorization_identifier)
        result = provider.remote_patron_lookup(patron_input)

        if expect_success:
            assert isinstance(result, PatronData)
            assert result.authorization_identifier is not None
            assert result.permanent_id is not None
            assert result.username is not None
            assert result.personal_name is not None
        else:
            assert result is None

    def test_remote_patron_lookup_with_patron_object(
        self, minimal_auth_fixture: MinimalAuthFixture, db: DatabaseTransactionFixture
    ):
        """Test remote_patron_lookup works with Patron objects."""
        provider = minimal_auth_fixture.provider()

        # Create a patron object
        patron = db.patron()
        patron.authorization_identifier = "patron123"

        result = provider.remote_patron_lookup(patron)

        assert isinstance(result, PatronData)
        assert result.authorization_identifier == "patron123"
        assert result.permanent_id == "patron123_id"
        assert result.username == "patron123_username"
        assert result.personal_name == "PersonalNamepatron123"

    def test_remote_patron_lookup_preserves_identifier_format(
        self, minimal_auth_fixture: MinimalAuthFixture
    ):
        """Test that remote_patron_lookup correctly handles identifiers with and without _username suffix."""
        provider = minimal_auth_fixture.provider()

        # Test with identifier ending in _username
        patrondata_with_suffix = PatronData(authorization_identifier="test_username")
        result = provider.remote_patron_lookup(patrondata_with_suffix)
        assert result.authorization_identifier == "test"
        assert result.username == "test_username"
        assert result.permanent_id == "test_id"
        assert result.personal_name == "PersonalNametest"

        # Test with identifier NOT ending in _username
        patrondata_without_suffix = PatronData(authorization_identifier="test")
        result = provider.remote_patron_lookup(patrondata_without_suffix)
        assert result.authorization_identifier == "test"
        assert result.username == "test_username"
        assert result.permanent_id == "test_id"
        assert result.personal_name == "PersonalNametest"

    def test_remote_authenticate_creates_consistent_patrondata(
        self, minimal_auth_fixture: MinimalAuthFixture
    ):
        """Test that remote_authenticate creates PatronData consistent with generate_patrondata."""
        provider = minimal_auth_fixture.provider()

        username = "testuser"
        result = provider.remote_authenticate(username, "any_password")

        # Should match what generate_patrondata produces
        expected = MinimalAuthenticationProvider.generate_patrondata(username)

        assert result.authorization_identifier == expected.authorization_identifier
        assert result.username == expected.username
        assert result.permanent_id == expected.permanent_id
        assert result.personal_name == expected.personal_name
