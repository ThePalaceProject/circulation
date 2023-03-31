import feedparser

from core.model import LinkRelations
from core.util.opds_writer import OPDSFeed
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
            assert OPDSFeed.ACQUISITION_FEED_TYPE == response.headers["Content-Type"]

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
