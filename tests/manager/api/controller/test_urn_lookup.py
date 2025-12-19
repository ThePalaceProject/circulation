import feedparser

from palace.manager.core.classifier import Classifier
from palace.manager.sqlalchemy.constants import LinkRelations
from palace.manager.sqlalchemy.model.classification import Genre
from palace.manager.util.opds_writer import OPDSFeed
from tests.fixtures.api_controller import ControllerFixture


class TestURNLookupController:
    """Test that a client can look up data on specific works."""

    def test_work_lookup(self, controller_fixture: ControllerFixture):
        work = controller_fixture.db.work(with_open_access_download=True)
        [pool] = work.license_pools
        urn = pool.identifier.urn
        with controller_fixture.request_context_with_library("/?urn=%s" % urn):
            route_name = "work"

            # Look up a work.
            response = controller_fixture.manager.urn_lookup.work_lookup(route_name)

            # We got an OPDS feed.
            assert 200 == response.status_code
            assert response.headers["Content-Type"].startswith(
                OPDSFeed.ACQUISITION_FEED_TYPE
            )

            # Parse it.
            feed = feedparser.parse(response.data)

            # The route name we passed into work_lookup shows up in
            # the feed-level link with rel="self".
            [self_link] = feed["feed"]["links"]
            assert "/" + route_name in self_link["href"]

            # The work we looked up has an OPDS entry.
            [entry] = feed["entries"]
            assert work.title == entry["title"]

            # The OPDS feed includes an open-access acquisition link
            # -- something that only gets inserted by the
            # CirculationManagerAnnotator.
            [link] = entry.links
            assert LinkRelations.OPEN_ACCESS_DOWNLOAD == link["rel"]

    def test_work_lookup_filtered_by_audience(
        self, controller_fixture: ControllerFixture
    ):
        """Test that URN lookup excludes works filtered by audience."""
        library = controller_fixture.db.default_library()
        work = controller_fixture.db.work(with_open_access_download=True)
        work.audience = Classifier.AUDIENCE_ADULT
        [pool] = work.license_pools
        urn = pool.identifier.urn

        # Set up audience filtering
        library.settings_dict["filtered_audiences"] = ["Adult"]
        if hasattr(library, "_settings"):
            delattr(library, "_settings")

        with controller_fixture.request_context_with_library("/?urn=%s" % urn):
            response = controller_fixture.manager.urn_lookup.work_lookup("work")

        # We get a feed, but with no entries (work was filtered)
        assert 200 == response.status_code
        feed = feedparser.parse(response.data)
        assert 0 == len(feed["entries"])

    def test_work_lookup_filtered_by_genre(self, controller_fixture: ControllerFixture):
        """Test that URN lookup excludes works filtered by genre."""
        library = controller_fixture.db.default_library()
        work = controller_fixture.db.work(with_open_access_download=True)
        [pool] = work.license_pools
        urn = pool.identifier.urn

        # Add a genre to the work
        romance_genre, _ = Genre.lookup(controller_fixture.db.session, "Romance")
        work.genres = [romance_genre]

        # Set up genre filtering
        library.settings_dict["filtered_genres"] = ["Romance"]
        if hasattr(library, "_settings"):
            delattr(library, "_settings")

        with controller_fixture.request_context_with_library("/?urn=%s" % urn):
            response = controller_fixture.manager.urn_lookup.work_lookup("work")

        # We get a feed, but with no entries (work was filtered)
        assert 200 == response.status_code
        feed = feedparser.parse(response.data)
        assert 0 == len(feed["entries"])

    def test_work_lookup_not_filtered_when_settings_dont_match(
        self, controller_fixture: ControllerFixture
    ):
        """Test that URN lookup works normally when work doesn't match filters."""
        library = controller_fixture.db.default_library()
        work = controller_fixture.db.work(with_open_access_download=True)
        work.audience = Classifier.AUDIENCE_ADULT
        [pool] = work.license_pools
        urn = pool.identifier.urn

        # Set up filtering for a different audience
        library.settings_dict["filtered_audiences"] = ["Young Adult"]
        if hasattr(library, "_settings"):
            delattr(library, "_settings")

        with controller_fixture.request_context_with_library("/?urn=%s" % urn):
            response = controller_fixture.manager.urn_lookup.work_lookup("work")

        # Work doesn't match filter, should return normally
        assert 200 == response.status_code
        feed = feedparser.parse(response.data)
        assert 1 == len(feed["entries"])
        assert work.title == feed["entries"][0]["title"]

    def test_work_lookup_multiple_urns_with_filtering(
        self, controller_fixture: ControllerFixture
    ):
        """Test that URN lookup correctly filters some works while returning others."""
        library = controller_fixture.db.default_library()

        # Create two works with different audiences
        adult_work = controller_fixture.db.work(
            title="Adult Book", with_open_access_download=True
        )
        adult_work.audience = Classifier.AUDIENCE_ADULT
        [adult_pool] = adult_work.license_pools
        adult_urn = adult_pool.identifier.urn

        ya_work = controller_fixture.db.work(
            title="YA Book", with_open_access_download=True
        )
        ya_work.audience = Classifier.AUDIENCE_YOUNG_ADULT
        [ya_pool] = ya_work.license_pools
        ya_urn = ya_pool.identifier.urn

        # Filter Adult audience
        library.settings_dict["filtered_audiences"] = ["Adult"]
        if hasattr(library, "_settings"):
            delattr(library, "_settings")

        # Request both works
        with controller_fixture.request_context_with_library(
            f"/?urn={adult_urn}&urn={ya_urn}"
        ):
            response = controller_fixture.manager.urn_lookup.work_lookup("work")

        # Only the YA work should be returned
        assert 200 == response.status_code
        feed = feedparser.parse(response.data)
        assert 1 == len(feed["entries"])
        assert ya_work.title == feed["entries"][0]["title"]
