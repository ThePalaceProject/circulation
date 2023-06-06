from functools import partial
from typing import Callable

import pytest

from api.authentication.base import PatronData
from api.authentication.basic import BasicAuthProviderLibrarySettings, Keyboards
from api.simple_authentication import SimpleAuthenticationProvider, SimpleAuthSettings
from tests.fixtures.database import DatabaseTransactionFixture


@pytest.fixture
def mock_library_id() -> int:
    return 20


@pytest.fixture
def mock_integration_id() -> int:
    return 20


@pytest.fixture
def create_library_settings() -> Callable[..., BasicAuthProviderLibrarySettings]:
    return partial(BasicAuthProviderLibrarySettings)


@pytest.fixture
def create_settings() -> Callable[..., SimpleAuthSettings]:
    return partial(
        SimpleAuthSettings,
        test_identifier="barcode",
        test_password="pass",
    )


@pytest.fixture
def create_provider(
    mock_library_id: int,
    mock_integration_id: int,
    create_settings: Callable[..., SimpleAuthSettings],
    create_library_settings: Callable[..., BasicAuthProviderLibrarySettings],
) -> Callable[..., SimpleAuthenticationProvider]:
    return partial(
        SimpleAuthenticationProvider,
        library_id=mock_library_id,
        integration_id=mock_integration_id,
        settings=create_settings(),
        library_settings=create_library_settings(),
    )


class TestSimpleAuth:
    def test_simple(
        self,
        create_settings: Callable[..., SimpleAuthSettings],
        create_provider: Callable[..., SimpleAuthenticationProvider],
    ):
        settings = create_settings()
        provider = create_provider(settings=settings)

        assert provider.remote_authenticate("user", "wrongpass") is None
        assert provider.remote_authenticate("user", None) is None
        assert provider.remote_authenticate(None, "pass") is None
        user = provider.remote_authenticate("barcode", "pass")
        assert isinstance(user, PatronData)
        assert "barcode" == user.authorization_identifier
        assert "barcode_id" == user.permanent_id
        assert "barcode_username" == user.username
        assert user.neighborhood is None

    def test_neighborhood(
        self,
        create_settings: Callable[..., SimpleAuthSettings],
        create_provider: Callable[..., SimpleAuthenticationProvider],
    ):
        settings = create_settings(
            neighborhood="neighborhood",
        )
        provider = create_provider(settings=settings)

        # User can also authenticate by their 'username'
        user = provider.remote_authenticate("barcode_username", "pass")
        assert isinstance(user, PatronData)
        assert "barcode" == user.authorization_identifier
        assert "neighborhood" == user.neighborhood

    def test_no_password_authentication(
        self,
        create_settings: Callable[..., SimpleAuthSettings],
        create_provider: Callable[..., SimpleAuthenticationProvider],
    ):
        """The SimpleAuthenticationProvider can be made even
        simpler by having it authenticate solely based on username.
        """
        settings = create_settings(
            password_keyboard=Keyboards.NULL,
        )
        provider = create_provider(settings=settings)

        # If you don't provide a password, you're in.
        user = provider.remote_authenticate("barcode", None)
        assert isinstance(user, PatronData)

        user2 = provider.remote_authenticate("barcode", "")
        assert isinstance(user2, PatronData)
        assert user2.authorization_identifier == user.authorization_identifier

        # If you provide any password, you're out.
        assert provider.remote_authenticate("barcode", "pass") is None

    def test_additional_identifiers(
        self,
        create_settings: Callable[..., SimpleAuthSettings],
        create_provider: Callable[..., SimpleAuthenticationProvider],
    ):
        settings = create_settings(
            additional_test_identifiers=["a", "b", "c"],
        )
        provider = create_provider(settings=settings)

        assert provider.remote_authenticate("a", None) is None
        assert provider.remote_authenticate(None, "pass") is None

        user = provider.remote_authenticate("a", "pass")
        assert isinstance(user, PatronData)
        assert "a" == user.authorization_identifier
        assert "a_id" == user.permanent_id
        assert "a_username" == user.username

        user2 = provider.remote_authenticate("b", "pass")
        assert isinstance(user2, PatronData)
        assert "b" == user2.authorization_identifier
        assert "b_id" == user2.permanent_id
        assert "b_username" == user2.username

        # Users can also authenticate by their 'username'
        user3 = provider.remote_authenticate("a_username", "pass")
        assert isinstance(user3, PatronData)
        assert "a" == user3.authorization_identifier

        user4 = provider.remote_authenticate("b_username", "pass")
        assert isinstance(user4, PatronData)
        assert "b" == user4.authorization_identifier

        # The main user can still authenticate too.
        user5 = provider.remote_authenticate("barcode", "pass")
        assert isinstance(user5, PatronData)
        assert "barcode" == user5.authorization_identifier

    def test_generate_patrondata(self):
        m = SimpleAuthenticationProvider.generate_patrondata

        # Pass in numeric barcode as identifier
        result = m("1234")
        assert result.permanent_id == "1234_id"
        assert result.authorization_identifier == "1234"
        assert result.personal_name == "PersonalName1234"
        assert result.username == "1234_username"
        assert result.neighborhood == None

        # Pass in username as identifier
        result = m("1234_username")
        assert result.permanent_id == "1234_id"
        assert result.authorization_identifier == "1234"
        assert result.personal_name == "PersonalName1234"
        assert result.username == "1234_username"
        assert result.neighborhood == None

        # Pass in a neighborhood.
        result = m("1234", "Echo Park")
        assert result.neighborhood == "Echo Park"

    def test_remote_patron_lookup(
        self,
        create_settings: Callable[..., SimpleAuthSettings],
        create_provider: Callable[..., SimpleAuthenticationProvider],
        db: DatabaseTransactionFixture,
    ):
        settings = create_settings(password_keyboard=Keyboards.NULL)
        provider = create_provider(settings=settings)

        patron_data = PatronData(authorization_identifier="barcode")
        patron = db.patron()
        patron.authorization_identifier = "barcode"

        # Returns None if nothing is passed in
        assert provider.remote_patron_lookup(None) is None  # type: ignore[arg-type]

        # Returns a patron if a patron is passed in and something is found
        result = provider.remote_patron_lookup(patron)
        assert isinstance(result, PatronData)
        assert result.permanent_id == "barcode_id"

        # Returns None if no patron is found
        patron.authorization_identifier = "wrong barcode"
        result = provider.remote_patron_lookup(patron)
        assert result is None

        # Returns a patron if a PatronData object is passed in and something is found
        result = provider.remote_patron_lookup(patron_data)
        assert isinstance(result, PatronData)
        assert result.permanent_id == "barcode_id"
