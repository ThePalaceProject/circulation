import io
import json
from unittest.mock import patch
from urllib.parse import quote

import pytest
from requests import Response

from api.app import app
from api.circulation import FulfillmentInfo
from api.circulation_exceptions import CannotFulfill
from api.opds2 import (
    OPDS2API,
    OPDS2NavigationsAnnotator,
    OPDS2PublicationsAnnotator,
    OPDS2TokenAuthenticationFulfillmentProcessor,
)
from core.lane import Facets, Pagination
from core.model.collection import Collection
from core.model.configuration import ConfigurationSetting, ExternalIntegration
from core.model.resource import Hyperlink
from core.problem_details import INVALID_CREDENTIALS
from core.testing import DatabaseTest
from tests.api.test_controller import CirculationControllerTest


class TestOPDS2FeedController(CirculationControllerTest):
    def setup_method(self):
        super().setup_method()
        self.annotator = OPDS2PublicationsAnnotator(
            "https://example.org/opds2",
            Facets.default(self._default_library),
            Pagination.default(),
            self._default_library,
        )
        self.controller = self.manager.opds2_feeds

    def test_publications_feed(self):
        with self.request_context_with_library("/"):
            response = self.controller.publications()
            assert response.status_code == 200
            feed = json.loads(response.data)
            assert "metadata" in feed
            assert "links" in feed
            assert "publications" in feed


class TestOPDS2PublicationAnnotator(DatabaseTest):
    def setup_method(self):
        super().setup_method()
        self.annotator = OPDS2PublicationsAnnotator(
            "https://example.org/opds2",
            Facets.default(self._default_library),
            Pagination.default(),
            self._default_library,
        )

    def test_loan_link(self):
        work = self._work()
        idn = work.presentation_edition.primary_identifier
        with app.test_request_context("/"):
            link = self.annotator.loan_link(work.presentation_edition)
            assert Hyperlink.BORROW == link["rel"]
            assert (
                quote(
                    f"/{self._default_library.short_name}/works/{idn.type}/{idn.identifier}/borrow"
                )
                == link["href"]
            )

    def test_self_link(self):
        work = self._work()
        idn = work.presentation_edition.primary_identifier
        with app.test_request_context("/"):
            link = self.annotator.self_link(work.presentation_edition)
            assert link["rel"] == "self"
            assert (
                quote(
                    f"/{self._default_library.short_name}/works/{idn.type}/{idn.identifier}"
                )
                == link["href"]
            )


class TestOPDS2NavigationAnnotator(DatabaseTest):
    def setup_method(self):
        super().setup_method()
        self.annotator = OPDS2NavigationsAnnotator(
            "/",
            Facets.default(self._default_library),
            Pagination.default(),
            self._default_library,
            title="Navigation",
        )

    def test_navigation(self):
        with app.test_request_context("/"):
            navigation = self.annotator.navigation_collection()
        assert len(navigation) == 1
        assert (
            navigation[0]["href"]
            == f"/{self._default_library.short_name}/opds2/publications"
        )


class TestOPDS2API(DatabaseTest):
    @patch("api.opds2.HTTP")
    def test_get_authentication_token(self, mock_http):
        resp = Response()
        resp.status_code = 200
        resp.raw = io.BytesIO(b"plaintext-auth-token")
        mock_http.get_with_timeout.return_value = resp
        token = OPDS2API.get_authentication_token(
            self._patron(), "http://example.org/token"
        )

        assert token == "plaintext-auth-token"
        assert mock_http.get_with_timeout.call_count == 1

    @patch("api.opds2.HTTP")
    def test_get_authentication_token_errors(self, mock_http):
        resp = Response()
        resp.status_code = 400
        mock_http.get_with_timeout.return_value = resp

        token = OPDS2API.get_authentication_token(
            self._patron(), "http://example.org/token"
        )

        assert token == INVALID_CREDENTIALS


class TestOPDS2TokenAuthenticationFulfillmentProcessor(DatabaseTest):
    @patch("api.opds2.HTTP")
    def test_fulfill(self, mock_http):
        patron = self._patron()
        patron.username = "username"
        collection: Collection = self._collection(
            protocol=ExternalIntegration.OPDS2_IMPORT
        )
        work = self._work(with_license_pool=True, collection=collection)
        integration: ExternalIntegration = collection.create_external_integration(
            ExternalIntegration.OPDS2_IMPORT
        )
        setting: ConfigurationSetting = ConfigurationSetting.for_externalintegration(
            ExternalIntegration.TOKEN_AUTH, integration
        )
        setting.value = "http://example.org/token?userName={patron_id}"

        ff_info = FulfillmentInfo(
            collection,
            "datasource",
            "proquest",
            "11234",
            "http://example.org/11234/fulfill?authToken={authentication_token}",
            None,
            None,
            None,
        )

        resp = Response()
        resp.status_code = 200
        resp.raw = io.BytesIO(b"plaintext-auth-token")
        mock_http.get_with_timeout.return_value = resp

        processor = OPDS2TokenAuthenticationFulfillmentProcessor(collection)

        ff_info = processor.fulfill(patron, None, work.license_pools[0], None, ff_info)

        assert mock_http.get_with_timeout.call_count == 1
        assert (
            mock_http.get_with_timeout.call_args[0][0]
            == "http://example.org/token?userName=username"
        )

        assert (
            ff_info.content_link
            == "http://example.org/11234/fulfill?authToken=plaintext-auth-token"
        )
        assert ff_info.content_link_redirect == True

        ## Test error case
        # Reset the content link
        ff_info.content_link = (
            "http://example.org/11234/fulfill?authToken={authentication_token}"
        )
        # non-200 response
        resp = Response()
        resp.status_code = 400
        mock_http.reset_mock()
        mock_http.get_with_timeout.return_value = resp
        with pytest.raises(CannotFulfill):
            processor.fulfill(patron, None, work.license_pools[0], None, ff_info)

        ## Pass through cases
        # No templating in the url
        ff_info.content_link = (
            "http://example.org/11234/fulfill?authToken=authentication_token"
        )
        ff_info.content_link_redirect = False
        ff_info = processor.fulfill(patron, None, work.license_pools[0], None, ff_info)
        assert ff_info.content_link_redirect == False

        # No token endpoint config
        ff_info.content_link = (
            "http://example.org/11234/fulfill?authToken={authentication_token}"
        )
        setting.value = None
        ff_info = processor.fulfill(patron, None, work.license_pools[0], None, ff_info)
        assert ff_info.content_link_redirect == False
