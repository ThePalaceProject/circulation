from typing import List

import pytest
from lxml import etree

from api.kansas_patron import KansasAuthenticationAPI
from core.model import ExternalIntegration

from ..fixtures.api_kansas_files import KansasPatronFilesFixture
from ..fixtures.database import DatabaseTransactionFixture


class MockResponse:
    def __init__(self, content):
        self.status_code = 200
        self.content = content


class MockAPI(KansasAuthenticationAPI):
    queue: List[bytes]

    def __init__(
        self,
        api_kansas_files_fixture: KansasPatronFilesFixture,
        library_id,
        integration,
        analytics=None,
        base_url=None,
    ):
        super().__init__(library_id, integration, analytics, base_url)
        self.queue = []
        self.api_kansas_files_fixture = api_kansas_files_fixture

    def sample_data(self, filename):
        return self.api_kansas_files_fixture.sample_data(filename)

    def enqueue(self, filename):
        data = self.sample_data(filename)
        self.queue.append(data)

    def post_request(self, data):
        response = self.queue[0]
        self.queue = self.queue[1:]
        return MockResponse(response)


class KansasPatronFixture:
    db: DatabaseTransactionFixture
    api_kansas_files_fixture: KansasPatronFilesFixture
    api: MockAPI
    integration: ExternalIntegration

    def __init__(
        self,
        db: DatabaseTransactionFixture,
        api_kansas_files_fixture: KansasPatronFilesFixture,
    ):
        self.db = db
        self.integration = db.external_integration(ExternalIntegration.PATRON_AUTH_GOAL)
        self.api = MockAPI(
            api_kansas_files_fixture,
            db.default_library(),
            self.integration,
            base_url="http://test.com",
        )


@pytest.fixture(scope="function")
def kansas_patron_fixture(
    db: DatabaseTransactionFixture, api_kansas_files_fixture: KansasPatronFilesFixture
) -> KansasPatronFixture:
    return KansasPatronFixture(db, api_kansas_files_fixture)


class TestKansasPatronAPI:
    def test_request(self, kansas_patron_fixture: KansasPatronFixture):
        request = KansasAuthenticationAPI.create_authorize_request("12345", "6666")
        mock_request = kansas_patron_fixture.api.sample_data("authorize_request.xml")
        parser = etree.XMLParser(remove_blank_text=True)
        mock_request = etree.tostring(etree.fromstring(mock_request, parser=parser))
        assert request == mock_request

    def test_parse_response(self, kansas_patron_fixture: KansasPatronFixture):
        api = kansas_patron_fixture.api
        response = api.sample_data("authorization_response_good.xml")
        authorized, patron_name, library_identifier = api.parse_authorize_response(
            response
        )
        assert authorized == True
        assert patron_name == "Montgomery Burns"
        assert library_identifier == "-2"

        response = api.sample_data("authorization_response_bad.xml")
        authorized, patron_name, library_identifier = api.parse_authorize_response(
            response
        )
        assert authorized == False
        assert patron_name == "Jay Gee"
        assert library_identifier == "12"

        response = api.sample_data("authorization_response_no_status.xml")
        authorized, patron_name, library_identifier = api.parse_authorize_response(
            response
        )
        assert authorized == False
        assert patron_name == "Simpson"
        assert library_identifier == "test"

        response = api.sample_data("authorization_response_no_id.xml")
        authorized, patron_name, library_identifier = api.parse_authorize_response(
            response
        )
        assert authorized == True
        assert patron_name == "Gee"
        assert library_identifier == None

        response = api.sample_data("authorization_response_empty_tag.xml")
        authorized, patron_name, library_identifier = api.parse_authorize_response(
            response
        )
        assert authorized == False
        assert patron_name == None
        assert library_identifier == "0"

    def test_remote_authenticate(self, kansas_patron_fixture: KansasPatronFixture):
        api = kansas_patron_fixture.api
        api.enqueue("authorization_response_good.xml")
        patrondata = api.remote_authenticate("1234", "4321")
        assert patrondata.authorization_identifier == "1234"
        assert patrondata.permanent_id == "1234"
        assert patrondata.library_identifier == "-2"
        assert patrondata.personal_name == "Montgomery Burns"

        api.enqueue("authorization_response_bad.xml")
        patrondata = api.remote_authenticate("1234", "4321")
        assert patrondata == False

        api.enqueue("authorization_response_no_status.xml")
        patrondata = api.remote_authenticate("1234", "4321")
        assert patrondata == False

        api.enqueue("authorization_response_no_id.xml")
        patrondata = api.remote_authenticate("1234", "4321")
        assert patrondata.authorization_identifier == "1234"
        assert patrondata.permanent_id == "1234"
        assert patrondata.library_identifier == None
        assert patrondata.personal_name == "Gee"

        api.enqueue("authorization_response_empty_tag.xml")
        patrondata = api.remote_authenticate("1234", "4321")
        assert patrondata == False

        api.enqueue("authorization_response_malformed.xml")
        patrondata = api.remote_authenticate("1234", "4321")
        assert patrondata == False
