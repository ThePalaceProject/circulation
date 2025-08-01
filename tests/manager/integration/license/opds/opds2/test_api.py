from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from requests import Response

from palace.manager.api.circulation.exceptions import CannotFulfill
from palace.manager.api.circulation.fulfillment import Fulfillment, RedirectFulfillment
from palace.manager.celery.tasks import opds2 as opds2_celery
from palace.manager.integration.license.opds.opds2 import api
from palace.manager.integration.license.opds.opds2.api import OPDS2API
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from tests.fixtures.database import DatabaseTransactionFixture


class Opds2ApiFixture:
    def __init__(self, db: DatabaseTransactionFixture, mock_http: MagicMock):
        self.patron = db.patron()
        self.collection: Collection = db.collection(
            protocol=OPDS2API,
            settings=db.opds_settings(
                external_account_id="http://opds2.example.org/feed",
                data_source="test",
            ),
        )
        self.collection.integration_configuration.context = {
            OPDS2API.TOKEN_AUTH_CONFIG_KEY: "http://example.org/token?userName={patron_id}"
        }

        self.mock_response = MagicMock(spec=Response)
        self.mock_response.status_code = 200
        self.mock_response.text = "plaintext-auth-token"

        self.mock_http = mock_http
        self.mock_http.get_with_timeout.return_value = self.mock_response

        self.data_source = DataSource.lookup(db.session, "test", autocreate=True)

        self.pool = MagicMock(spec=LicensePool)
        self.mechanism = MagicMock(spec=LicensePoolDeliveryMechanism)
        self.pool.available_delivery_mechanisms = [self.mechanism]
        self.pool.data_source = self.data_source
        self.mechanism.resource.representation.public_url = (
            "http://example.org/11234/fulfill?authToken={authentication_token}"
        )

        self.api = OPDS2API(db.session, self.collection)

    def fulfill(self) -> Fulfillment:
        return self.api.fulfill(self.patron, "", self.pool, self.mechanism)


@pytest.fixture
def opds2_api_fixture(
    db: DatabaseTransactionFixture,
) -> Generator[Opds2ApiFixture, None, None]:
    with patch.object(api, "HTTP") as mock_http:
        fixture = Opds2ApiFixture(db, mock_http)
        yield fixture


class TestOpds2Api:
    def test_token_fulfill(self, opds2_api_fixture: Opds2ApiFixture):
        fulfillment = opds2_api_fixture.fulfill()
        assert isinstance(fulfillment, RedirectFulfillment)

        patron_id = opds2_api_fixture.patron.identifier_to_remote_service(
            opds2_api_fixture.data_source
        )

        assert opds2_api_fixture.mock_http.get_with_timeout.call_count == 1
        assert (
            opds2_api_fixture.mock_http.get_with_timeout.call_args[0][0]
            == f"http://example.org/token?userName={patron_id}"
        )

        assert (
            fulfillment.content_link
            == "http://example.org/11234/fulfill?authToken=plaintext-auth-token"
        )

    def test_token_fulfill_alternate_template(self, opds2_api_fixture: Opds2ApiFixture):
        # Alternative templating
        opds2_api_fixture.mechanism.resource.representation.public_url = (
            "http://example.org/11234/fulfill{?authentication_token}"
        )
        fulfillment = opds2_api_fixture.fulfill()
        assert isinstance(fulfillment, RedirectFulfillment)

        assert (
            fulfillment.content_link
            == "http://example.org/11234/fulfill?authentication_token=plaintext-auth-token"
        )

    def test_token_fulfill_400_response(self, opds2_api_fixture: Opds2ApiFixture):
        # non-200 response
        opds2_api_fixture.mock_response.status_code = 400
        with pytest.raises(CannotFulfill):
            opds2_api_fixture.fulfill()

    def test_token_fulfill_no_template(self, opds2_api_fixture: Opds2ApiFixture):
        # No templating in the url
        opds2_api_fixture.mechanism.resource.representation.public_url = (
            "http://example.org/11234/fulfill"
        )
        fulfillment = opds2_api_fixture.fulfill()
        assert isinstance(fulfillment, RedirectFulfillment)
        assert (
            fulfillment.content_link
            == opds2_api_fixture.mechanism.resource.representation.public_url
        )

    def test_token_fulfill_no_endpoint_config(self, opds2_api_fixture: Opds2ApiFixture):
        # No token endpoint config
        opds2_api_fixture.api.token_auth_configuration = None
        mock = MagicMock()
        opds2_api_fixture.api.fulfill_token_auth = mock
        opds2_api_fixture.fulfill()
        # we never call the token auth function
        assert mock.call_count == 0

    def test_get_authentication_token(self, opds2_api_fixture: Opds2ApiFixture):
        token = OPDS2API.get_authentication_token(
            opds2_api_fixture.patron, opds2_api_fixture.data_source, ""
        )

        assert token == "plaintext-auth-token"
        assert opds2_api_fixture.mock_http.get_with_timeout.call_count == 1

    def test_get_authentication_token_400_response(
        self, opds2_api_fixture: Opds2ApiFixture
    ):
        opds2_api_fixture.mock_response.status_code = 400
        with pytest.raises(CannotFulfill):
            OPDS2API.get_authentication_token(
                opds2_api_fixture.patron, opds2_api_fixture.data_source, ""
            )

    def test_get_authentication_token_bad_response(
        self, opds2_api_fixture: Opds2ApiFixture
    ):
        opds2_api_fixture.mock_response.text = None
        with pytest.raises(CannotFulfill):
            OPDS2API.get_authentication_token(
                opds2_api_fixture.patron, opds2_api_fixture.data_source, ""
            )

    def test_import_task(self) -> None:
        collection_id = MagicMock()
        force = MagicMock()
        with patch.object(opds2_celery, "import_collection") as mock_import:
            result = OPDS2API.import_task(collection_id, force)

        mock_import.s.assert_called_once_with(collection_id, force=force)
        assert result == mock_import.s.return_value
