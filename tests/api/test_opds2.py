import io
import json
from unittest.mock import patch
from urllib.parse import quote

import pytest
from requests import Response
from webpub_manifest_parser.opds2 import OPDS2FeedParserFactory

from api.app import app
from api.circulation import FulfillmentInfo
from api.circulation_exceptions import CannotFulfill
from api.controller import CirculationManager
from api.opds2 import (
    OPDS2NavigationsAnnotator,
    OPDS2PublicationsAnnotator,
    TokenAuthenticationFulfillmentProcessor,
)
from core.lane import Facets, Pagination
from core.model.collection import Collection
from core.model.configuration import ExternalIntegration
from core.model.datasource import DataSource
from core.model.patron import Loan
from core.model.resource import Hyperlink
from core.opds2_import import OPDS2Importer, RWPMManifestParser
from core.problem_details import INVALID_CREDENTIALS
from tests.fixtures.api_controller import (
    CirculationControllerFixture,
    ControllerFixture,
)
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.opds2_files import OPDS2FilesFixture


class OPDS2FeedControllerFixture:
    def __init__(self, circulation_fixture: CirculationControllerFixture):
        self.db = circulation_fixture.db
        self.circulation_fixture = circulation_fixture
        self.annotator = OPDS2PublicationsAnnotator(
            "https://example.org/opds2",
            Facets.default(self.db.default_library()),
            Pagination.default(),
            self.db.default_library(),
        )
        self.controller = self.circulation_fixture.manager.opds2_feeds


@pytest.fixture(scope="function")
def opds2_feed_controller(
    circulation_fixture: CirculationControllerFixture,
) -> OPDS2FeedControllerFixture:
    return OPDS2FeedControllerFixture(circulation_fixture)


class TestOPDS2FeedController:
    def test_publications_feed(self, opds2_feed_controller: OPDS2FeedControllerFixture):
        circ = opds2_feed_controller.circulation_fixture
        with circ.request_context_with_library("/"):
            response = opds2_feed_controller.controller.publications()
            assert response.status_code == 200
            feed = json.loads(response.data)
            assert "metadata" in feed
            assert "links" in feed
            assert "publications" in feed


class OPDS2PublicationAnnotatorFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.annotator = OPDS2PublicationsAnnotator(
            "https://example.org/opds2",
            Facets.default(db.default_library()),
            Pagination.default(),
            db.default_library(),
        )


@pytest.fixture(scope="function")
def opds2_publication_annotator(
    db: DatabaseTransactionFixture,
) -> OPDS2PublicationAnnotatorFixture:
    return OPDS2PublicationAnnotatorFixture(db)


class TestOPDS2PublicationAnnotator:
    def test_loan_link(
        self, opds2_publication_annotator: OPDS2PublicationAnnotatorFixture
    ):
        work = opds2_publication_annotator.db.work()
        idn = work.presentation_edition.primary_identifier
        with app.test_request_context("/"):
            link = opds2_publication_annotator.annotator.loan_link(
                work.presentation_edition
            )
            assert Hyperlink.BORROW == link["rel"]
            assert (
                quote(
                    f"/{opds2_publication_annotator.db.default_library().short_name}/works/{idn.type}/{idn.identifier}/borrow"
                )
                == link["href"]
            )

    def test_self_link(
        self, opds2_publication_annotator: OPDS2PublicationAnnotatorFixture
    ):
        work = opds2_publication_annotator.db.work()
        idn = work.presentation_edition.primary_identifier
        with app.test_request_context("/"):
            link = opds2_publication_annotator.annotator.self_link(
                work.presentation_edition
            )
            assert link["rel"] == "self"
            assert (
                quote(
                    f"/{opds2_publication_annotator.db.default_library().short_name}/works/{idn.type}/{idn.identifier}"
                )
                == link["href"]
            )


class OPDS2NavigationAnnotatorFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.annotator = OPDS2NavigationsAnnotator(
            "/",
            Facets.default(db.default_library()),
            Pagination.default(),
            db.default_library(),
            title="Navigation",
        )


@pytest.fixture(scope="function")
def opds2_navigation_annotator(
    db: DatabaseTransactionFixture,
) -> OPDS2NavigationAnnotatorFixture:
    return OPDS2NavigationAnnotatorFixture(db)


class TestOPDS2NavigationAnnotator:
    def test_navigation(
        self, opds2_navigation_annotator: OPDS2NavigationAnnotatorFixture
    ):
        with app.test_request_context("/"):
            navigation = opds2_navigation_annotator.annotator.navigation_collection()
        assert len(navigation) == 1
        assert (
            navigation[0]["href"]
            == f"/{opds2_navigation_annotator.db.default_library().short_name}/opds2/publications"
        )


class TestTokenAuthenticationFulfillmentProcessor:
    @patch("api.opds2.HTTP")
    def test_fulfill(self, mock_http, db: DatabaseTransactionFixture):
        patron = db.patron()
        patron.username = "username"
        collection: Collection = db.collection(
            protocol=ExternalIntegration.OPDS2_IMPORT
        )
        work = db.work(with_license_pool=True, collection=collection)
        collection.integration_configuration[
            ExternalIntegration.TOKEN_AUTH
        ] = "http://example.org/token?userName={patron_id}"

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

        processor = TokenAuthenticationFulfillmentProcessor(collection)
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

        # Alternative templating
        ff_info.content_link = "http://example.org/11234/fulfill{?authentication_token}"
        ff_info = processor.fulfill(patron, None, work.license_pools[0], None, ff_info)

        assert (
            ff_info.content_link
            == "http://example.org/11234/fulfill?authentication_token=plaintext-auth-token"
        )

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
        collection.integration_configuration[ExternalIntegration.TOKEN_AUTH] = None
        ff_info = processor.fulfill(patron, None, work.license_pools[0], None, ff_info)
        assert ff_info.content_link_redirect == False

    @patch("api.opds2.HTTP")
    def test_get_authentication_token(self, mock_http, db: DatabaseTransactionFixture):
        resp = Response()
        resp.status_code = 200
        resp.raw = io.BytesIO(b"plaintext-auth-token")
        mock_http.get_with_timeout.return_value = resp
        patron = db.patron()
        patron.username = "test"
        token = TokenAuthenticationFulfillmentProcessor.get_authentication_token(
            patron, "http://example.org/token"
        )

        assert token == "plaintext-auth-token"
        assert mock_http.get_with_timeout.call_count == 1

    @patch("api.opds2.HTTP")
    def test_get_authentication_token_errors(
        self, mock_http, db: DatabaseTransactionFixture
    ):
        resp = Response()
        resp.status_code = 400
        mock_http.get_with_timeout.return_value = resp

        token = TokenAuthenticationFulfillmentProcessor.get_authentication_token(
            db.patron(), "http://example.org/token"
        )

        assert token == INVALID_CREDENTIALS


class TestOPDS2WithTokens:
    def test_opds2_with_authentication_tokens(
        self,
        controller_fixture: ControllerFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ):
        """Test the end to end workflow from importing the feed to a fulfill"""
        collection = controller_fixture.db.collection(
            protocol=ExternalIntegration.OPDS2_IMPORT,
            data_source_name=DataSource.PROQUEST,
        )
        controller_fixture.db.default_library().collections.append(collection)
        # Import the test feed first
        importer: OPDS2Importer = OPDS2Importer(
            controller_fixture.db.session,
            collection,
            RWPMManifestParser(OPDS2FeedParserFactory()),
        )
        with opds2_files_fixture.sample_fd("auth_token_feed.json") as fp:
            editions, pools, works, failures = importer.import_from_feed(fp.read())

        work = works[0]
        identifier = work.presentation_edition.primary_identifier

        manager = CirculationManager(controller_fixture.db.session)
        patron = controller_fixture.db.patron()

        # Borrow the book from the library
        with controller_fixture.request_context_with_library("/") as ctx:
            ctx.request.patron = patron
            manager.loans.borrow(identifier.type, identifier.identifier)

        loans = controller_fixture.db.session.query(Loan).filter(Loan.patron == patron)  # type: ignore
        assert loans.count() == 1

        loan = loans.first()
        mechanism_id = loan.license_pool.delivery_mechanisms[0].delivery_mechanism.id
        manager.loans.authenticated_patron_from_request = lambda: patron

        # Fulfill (Download) the book, should redirect to an authenticated URL
        with controller_fixture.request_context_with_library("/") as ctx, patch.object(
            TokenAuthenticationFulfillmentProcessor, "get_authentication_token"
        ) as mock_auth:
            ctx.request.patron = patron
            mock_auth.return_value = "plaintext-token"
            response = manager.loans.fulfill(loan.license_pool.id, mechanism_id)

        assert response.status_code == 302
        assert "authToken=plaintext-token" in response.location
