import json
from urllib.parse import quote

from api.app import app
from api.opds2 import OPDS2NavigationsAnnotator, OPDS2PublicationsAnnotator
from core.lane import Facets, Pagination
from core.model.resource import Hyperlink
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
