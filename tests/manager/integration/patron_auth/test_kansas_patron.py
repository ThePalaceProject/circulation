from collections.abc import Callable
from functools import partial

import pytest
from lxml import etree

from palace.manager.api.authentication.base import PatronData, PatronLookupNotSupported
from palace.manager.api.authentication.basic import BasicAuthProviderLibrarySettings
from palace.manager.integration.patron_auth.kansas_patron import (
    KansasAuthenticationAPI,
    KansasAuthSettings,
)
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import FilesFixture


class KansasPatronFilesFixture(FilesFixture):
    """A fixture providing access to Kansas patron files."""

    def __init__(self):
        super().__init__("kansas_patron")


@pytest.fixture()
def kansas_files_fixture() -> KansasPatronFilesFixture:
    """A fixture providing access to Kansas patron files."""
    return KansasPatronFilesFixture()


class MockResponse:
    def __init__(self, content):
        self.status_code = 200
        self.content = content


class MockAPI(KansasAuthenticationAPI):
    queue: list[bytes]

    def __init__(
        self,
        files: KansasPatronFilesFixture,
        library_id,
        integration_id,
        settings,
        library_settings,
        analytics=None,
    ):
        super().__init__(
            library_id, integration_id, settings, library_settings, analytics
        )
        self.queue = []
        self.files = files

    def sample_data(self, filename):
        return self.files.sample_data(filename)

    def enqueue(self, filename):
        data = self.sample_data(filename)
        self.queue.append(data)

    def post_request(self, data):
        response = self.queue[0]
        self.queue = self.queue[1:]
        return MockResponse(response)


@pytest.fixture
def mock_library_id() -> int:
    return 20


@pytest.fixture
def mock_integration_id() -> int:
    return 20


@pytest.fixture
def create_settings() -> Callable[..., KansasAuthSettings]:
    return partial(
        KansasAuthSettings,
        url="http://url.com/",
        test_identifier="barcode",
    )


@pytest.fixture
def create_provider(
    mock_library_id: int,
    mock_integration_id: int,
    create_settings: Callable[..., KansasAuthSettings],
    kansas_files_fixture: KansasPatronFilesFixture,
) -> Callable[..., MockAPI]:
    return partial(
        MockAPI,
        library_id=mock_library_id,
        integration_id=mock_integration_id,
        settings=create_settings(),
        library_settings=BasicAuthProviderLibrarySettings(),
        files=kansas_files_fixture,
    )


class TestKansasPatronAPI:
    def test_request(self, kansas_files_fixture: KansasPatronFilesFixture):
        request = KansasAuthenticationAPI.create_authorize_request("12345", "6666")
        mock_request = kansas_files_fixture.sample_data("authorize_request.xml")
        parser = etree.XMLParser(remove_blank_text=True)
        mock_request = etree.tostring(etree.fromstring(mock_request, parser=parser))
        assert request == mock_request

    def test_parse_response(self, create_provider: Callable[..., MockAPI]):
        provider = create_provider()
        response = provider.sample_data("authorization_response_good.xml")
        authorized, patron_name, library_identifier = provider.parse_authorize_response(
            response
        )
        assert authorized is True
        assert patron_name == "Montgomery Burns"
        assert library_identifier == "-2"

        response = provider.sample_data("authorization_response_bad.xml")
        authorized, patron_name, library_identifier = provider.parse_authorize_response(
            response
        )
        assert authorized is False
        assert patron_name == "Jay Gee"
        assert library_identifier == "12"

        response = provider.sample_data("authorization_response_no_status.xml")
        authorized, patron_name, library_identifier = provider.parse_authorize_response(
            response
        )
        assert authorized is False
        assert patron_name == "Simpson"
        assert library_identifier == "test"

        response = provider.sample_data("authorization_response_no_id.xml")
        authorized, patron_name, library_identifier = provider.parse_authorize_response(
            response
        )
        assert authorized is True
        assert patron_name == "Gee"
        assert library_identifier is None

        response = provider.sample_data("authorization_response_empty_tag.xml")
        authorized, patron_name, library_identifier = provider.parse_authorize_response(
            response
        )
        assert authorized is False
        assert patron_name is None
        assert library_identifier == "0"

    def test_remote_authenticate(self, create_provider: Callable[..., MockAPI]):
        provider = create_provider()
        provider.enqueue("authorization_response_good.xml")
        patrondata = provider.remote_authenticate("1234", "4321")
        assert isinstance(patrondata, PatronData)
        assert patrondata.authorization_identifier == "1234"
        assert patrondata.permanent_id == "1234"
        assert patrondata.library_identifier == "-2"
        assert patrondata.personal_name == "Montgomery Burns"

        provider.enqueue("authorization_response_bad.xml")
        patrondata = provider.remote_authenticate("1234", "4321")
        assert patrondata is None

        provider.enqueue("authorization_response_no_status.xml")
        patrondata = provider.remote_authenticate("1234", "4321")
        assert patrondata is None

        provider.enqueue("authorization_response_no_id.xml")
        patrondata = provider.remote_authenticate("1234", "4321")
        assert isinstance(patrondata, PatronData)
        assert patrondata.authorization_identifier == "1234"
        assert patrondata.permanent_id == "1234"
        assert patrondata.library_identifier == None
        assert patrondata.personal_name == "Gee"

        provider.enqueue("authorization_response_empty_tag.xml")
        patrondata = provider.remote_authenticate("1234", "4321")
        assert patrondata is None

        provider.enqueue("authorization_response_malformed.xml")
        patrondata = provider.remote_authenticate("1234", "4321")
        assert patrondata is None

    def test_remote_patron_lookup(
        self, create_provider: Callable[..., MockAPI], db: DatabaseTransactionFixture
    ):
        """Test that remote_patron_lookup raises PatronLookupNotSupported.

        Kansas auth gives very little data about the patron and cannot perform
        arbitrary patron lookups.
        """
        provider = create_provider()

        # Test with PatronData
        input_patrondata = PatronData()
        with pytest.raises(PatronLookupNotSupported):
            provider.remote_patron_lookup(input_patrondata)

        # Test with Patron object
        with pytest.raises(PatronLookupNotSupported):
            provider.remote_patron_lookup(db.patron())
