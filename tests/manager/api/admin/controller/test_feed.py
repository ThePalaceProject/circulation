import feedparser
import pytest

from palace.manager.api.admin.exceptions import AdminNotAuthorized
from palace.manager.core.classifier import genres
from palace.manager.sqlalchemy.model.admin import AdminRole
from tests.mocks.search import ExternalSearchIndexFake


class TestFeedController:
    def test_suppressed(self, admin_librarian_fixture):
        library = admin_librarian_fixture.ctrl.library

        suppressed_work = admin_librarian_fixture.ctrl.db.work(
            with_open_access_download=True
        )
        suppressed_work.suppressed_for.append(library)
        unsuppressed_work = admin_librarian_fixture.ctrl.db.work()

        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
            response = (
                admin_librarian_fixture.manager.admin_feed_controller.suppressed()
            )
            feed = feedparser.parse(response.get_data(as_text=True))
            entries = feed["entries"]
            assert 1 == len(entries)
            assert suppressed_work.title == entries[0]["title"]

        admin_librarian_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, admin_librarian_fixture.ctrl.db.default_library()
        )
        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                admin_librarian_fixture.manager.admin_feed_controller.suppressed,
            )

    def test_genres(self, admin_librarian_fixture):
        with admin_librarian_fixture.ctrl.app.test_request_context("/"):
            response = admin_librarian_fixture.manager.admin_feed_controller.genres()

            for name in genres:
                top = "Fiction" if genres[name].is_fiction else "Nonfiction"
                assert response[top][name] == dict(
                    {
                        "name": name,
                        "parents": [parent.name for parent in genres[name].parents],
                        "subgenres": [
                            subgenre.name for subgenre in genres[name].subgenres
                        ],
                    }
                )

    def test_suppressed_search_without_query_returns_opensearch_document(
        self, admin_librarian_fixture
    ):
        """Without a query parameter, returns an OpenSearch description document."""
        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
            response = (
                admin_librarian_fixture.manager.admin_feed_controller.suppressed_search()
            )

            assert response.status_code == 200
            assert (
                response.headers["Content-Type"]
                == "application/opensearchdescription+xml"
            )

            # Verify it's a valid OpenSearch document
            data = response.get_data(as_text=True)
            assert "OpenSearchDescription" in data
            assert "Search Hidden Books" in data

    def test_suppressed_search_with_query_returns_feed(self, admin_librarian_fixture):
        """With a query parameter, returns an OPDS feed of search results."""
        library = admin_librarian_fixture.ctrl.library

        # Create a suppressed work
        suppressed_work = admin_librarian_fixture.ctrl.db.work(
            title="Searchable Suppressed Book",
            with_open_access_download=True,
        )
        suppressed_work.suppressed_for.append(library)

        # Mock the search engine to return our work
        search_engine = admin_librarian_fixture.ctrl.controller.search_engine
        assert isinstance(search_engine, ExternalSearchIndexFake)
        search_engine.mock_query_works([suppressed_work])

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/?q=Searchable"
        ):
            response = (
                admin_librarian_fixture.manager.admin_feed_controller.suppressed_search()
            )

            assert response.status_code == 200

            # Parse the feed and verify the suppressed work is in the results
            feed = feedparser.parse(response.get_data(as_text=True))
            entries = feed["entries"]
            assert len(entries) == 1
            assert entries[0]["title"] == "Searchable Suppressed Book"

    def test_suppressed_search_requires_librarian_role(self, admin_librarian_fixture):
        """Search requires librarian authorization."""
        # Remove the librarian role
        admin_librarian_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, admin_librarian_fixture.ctrl.db.default_library()
        )

        with admin_librarian_fixture.request_context_with_library_and_admin("/?q=test"):
            pytest.raises(
                AdminNotAuthorized,
                admin_librarian_fixture.manager.admin_feed_controller.suppressed_search,
            )

    def test_suppressed_search_empty_results(self, admin_librarian_fixture):
        """Search with no matching works returns empty feed."""
        # Mock the search engine to return no results
        search_engine = admin_librarian_fixture.ctrl.controller.search_engine
        assert isinstance(search_engine, ExternalSearchIndexFake)
        search_engine.mock_query_works([])

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/?q=nonexistent"
        ):
            response = (
                admin_librarian_fixture.manager.admin_feed_controller.suppressed_search()
            )

            assert response.status_code == 200
            feed = feedparser.parse(response.get_data(as_text=True))
            assert len(feed["entries"]) == 0
