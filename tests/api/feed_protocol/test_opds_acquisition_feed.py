import datetime
import logging
from collections import defaultdict
from typing import Any, Callable, Generator, List, Type

import pytest
from lxml import etree
from sqlalchemy.orm import Session

from core.entrypoint import (
    AudiobooksEntryPoint,
    EbooksEntryPoint,
    EntryPoint,
    EverythingEntryPoint,
    MediumEntryPoint,
)
from core.external_search import MockExternalSearchIndex
from core.facets import FacetConstants
from core.feed_protocol.acquisition import LookupAcquisitionFeed, OPDSAcquisitionFeed
from core.feed_protocol.annotator.base import Annotator
from core.feed_protocol.annotator.circulation import AcquisitionHelper
from core.feed_protocol.annotator.verbose import VerboseAnnotator
from core.feed_protocol.navigation import NavigationFeed
from core.feed_protocol.types import FeedData, WorkEntry
from core.lane import Facets, FeaturedFacets, Lane, Pagination, SearchFacets, WorkList
from core.model import DeliveryMechanism, Representation
from core.model.constants import LinkRelations
from core.opds import MockUnfulfillableAnnotator
from core.util.datetime_helpers import utc_now
from core.util.flask_util import OPDSFeedResponse
from core.util.opds_writer import OPDSFeed, OPDSMessage
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.search import ExternalSearchPatchFixture


class MockAnnotator(Annotator):
    def __init__(self):
        self.lanes_by_work = defaultdict(list)

    @classmethod
    def lane_url(cls, lane):
        if lane and lane.has_visible_children:
            return cls.groups_url(lane)
        elif lane:
            return cls.feed_url(lane)
        else:
            return ""

    @classmethod
    def feed_url(cls, lane, facets=None, pagination=None):
        if isinstance(lane, Lane):
            base = "http://%s/" % lane.url_name
        else:
            base = "http://%s/" % lane.display_name
        sep = "?"
        if facets:
            base += sep + facets.query_string
            sep = "&"
        if pagination:
            base += sep + pagination.query_string
        return base

    @classmethod
    def search_url(cls, lane, query, pagination, facets=None):
        if isinstance(lane, Lane):
            base = "http://%s/" % lane.url_name
        else:
            base = "http://%s/" % lane.display_name
        sep = "?"
        if pagination:
            base += sep + pagination.query_string
            sep = "&"
        if facets:
            facet_query_string = facets.query_string
            if facet_query_string:
                base += sep + facet_query_string
        return base

    @classmethod
    def groups_url(cls, lane, facets=None):
        if lane and isinstance(lane, Lane):
            identifier = lane.id
        else:
            identifier = ""
        if facets:
            facet_string = "?" + facets.query_string
        else:
            facet_string = ""

        return f"http://groups/{identifier}{facet_string}"

    @classmethod
    def default_lane_url(cls):
        return cls.groups_url(None)

    @classmethod
    def facet_url(cls, facets):
        return "http://facet/" + "&".join(
            [f"{k}={v}" for k, v in sorted(facets.items())]
        )

    @classmethod
    def navigation_url(cls, lane):
        if lane and isinstance(lane, Lane):
            identifier = lane.id
        else:
            identifier = ""
        return "http://navigation/%s" % identifier

    @classmethod
    def top_level_title(cls):
        return "Test Top Level Title"


class TestOPDSAcquisitionFeed:
    def test_page(
        self,
        db,
        external_search_patch_fixture: ExternalSearchPatchFixture,
    ):
        session = db.session

        # Verify that AcquisitionFeed.page() returns an appropriate OPDSFeedResponse

        wl = WorkList()
        wl.initialize(db.default_library())
        private = object()
        response = OPDSAcquisitionFeed.page(
            session,
            "feed title",
            "url",
            wl,
            Annotator(),
            None,
            None,
            None,
        ).as_response(max_age=10, private=private)

        # The result is an OPDSFeedResponse. The 'private' argument,
        # unused by page(), was passed along into the constructor.
        assert isinstance(response, OPDSFeedResponse)
        assert 10 == response.max_age
        assert private == response.private

        assert "<title>feed title</title>" in str(response)

    def test_as_response(self, db: DatabaseTransactionFixture):
        session = db.session

        # Verify the ability to convert an AcquisitionFeed object to an
        # OPDSFeedResponse containing the feed.
        feed = OPDSAcquisitionFeed("feed title", "http://url/", [], Annotator())
        feed.generate_feed()

        # Some other piece of code set expectations for how this feed should
        # be cached.
        response = feed.as_response(max_age=101, private=False)
        assert 200 == response.status_code

        # We get an OPDSFeedResponse containing the feed in its
        # entity-body.
        assert isinstance(response, OPDSFeedResponse)
        assert "<title>feed title</title>" in str(response)

        # The caching expectations are respected.
        assert 101 == response.max_age
        assert False == response.private

    def test_as_error_response(self, db: DatabaseTransactionFixture):
        session = db.session

        # Verify the ability to convert an AcquisitionFeed object to an
        # OPDSFeedResponse that is to be treated as an error message.
        feed = OPDSAcquisitionFeed("feed title", "http://url/", [], Annotator())
        feed.generate_feed()

        # Some other piece of code set expectations for how this feed should
        # be cached.
        kwargs = dict(max_age=101, private=False)

        # But we know that something has gone wrong and the feed is
        # being served as an error message.
        response = feed.as_error_response(**kwargs)
        assert isinstance(response, OPDSFeedResponse)

        # The content of the feed is unchanged.
        assert 200 == response.status_code
        assert "<title>feed title</title>" in str(response)

        # But the max_age and private settings have been overridden.
        assert 0 == response.max_age
        assert True == response.private

    def test_add_entrypoint_links(self):
        """Verify that add_entrypoint_links calls _entrypoint_link
        on every EntryPoint passed in.
        """

        class Mock:
            attrs = dict(href="the response")

            def __init__(self):
                self.calls = []

            def __call__(self, *args):
                self.calls.append(args)
                return self.attrs

        mock = Mock()
        old_entrypoint_link = OPDSAcquisitionFeed._entrypoint_link
        OPDSAcquisitionFeed._entrypoint_link = mock

        feed = FeedData()
        entrypoints = [AudiobooksEntryPoint, EbooksEntryPoint]
        url_generator = object()
        OPDSAcquisitionFeed.add_entrypoint_links(
            feed, url_generator, entrypoints, EbooksEntryPoint, "Some entry points"
        )

        # Two different calls were made to the mock method.
        c1, c2 = mock.calls

        # The first entry point is not selected.
        assert c1 == (
            url_generator,
            AudiobooksEntryPoint,
            EbooksEntryPoint,
            True,
            "Some entry points",
        )
        # The second one is selected.
        assert c2 == (
            url_generator,
            EbooksEntryPoint,
            EbooksEntryPoint,
            False,
            "Some entry points",
        )

        # Two identical <link> tags were added to the <feed> tag, one
        # for each call to the mock method.
        l1, l2 = feed.links
        for l in l1, l2:
            assert mock.attrs == l.link_attribs()
        OPDSAcquisitionFeed._entrypoint_link = old_entrypoint_link

        # If there is only one facet in the facet group, no links are
        # added.
        feed = FeedData()
        mock.calls = []
        entrypoints = [EbooksEntryPoint]
        OPDSAcquisitionFeed.add_entrypoint_links(
            feed, url_generator, entrypoints, EbooksEntryPoint, "Some entry points"
        )
        assert [] == mock.calls

    def test_entrypoint_link(self):
        """Test the _entrypoint_link method's ability to create
        attributes for <link> tags.
        """
        m = OPDSAcquisitionFeed._entrypoint_link

        def g(entrypoint):
            """A mock URL generator."""
            return "%s" % (entrypoint.INTERNAL_NAME)

        # If the entry point is not registered, None is returned.
        assert None == m(g, object(), object(), True, "group")

        # Now make a real set of link attributes.
        l = m(g, AudiobooksEntryPoint, AudiobooksEntryPoint, False, "Grupe")

        # The link is identified as belonging to an entry point-type
        # facet group.
        assert l["rel"] == LinkRelations.FACET_REL
        assert l["facetGroupType"] == FacetConstants.ENTRY_POINT_REL
        assert "Grupe" == l["facetGroup"]

        # This facet is the active one in the group.
        assert "true" == l["activeFacet"]

        # The URL generator was invoked to create the href.
        assert l["href"] == g(AudiobooksEntryPoint)

        # The facet title identifies it as a way to look at audiobooks.
        assert EntryPoint.DISPLAY_TITLES[AudiobooksEntryPoint] == l["title"]

        # Now try some variants.

        # Here, the entry point is the default one.
        l = m(g, AudiobooksEntryPoint, AudiobooksEntryPoint, True, "Grupe")

        # This may affect the URL generated for the facet link.
        assert l["href"] == g(AudiobooksEntryPoint)

        # Here, the entry point for which we're generating the link is
        # not the selected one -- EbooksEntryPoint is.
        l = m(g, AudiobooksEntryPoint, EbooksEntryPoint, True, "Grupe")

        # This means the 'activeFacet' attribute is not present.
        assert "activeFacet" not in l

    def test_license_tags_no_loan_or_hold(self, db: DatabaseTransactionFixture):
        edition, pool = db.edition(with_license_pool=True)
        tags = AcquisitionHelper.license_tags(pool, None, None)
        assert dict(status="available") == tags["availability"].dict(exclude_none=True)
        assert dict(total="0") == tags["holds"].dict(exclude_none=True)
        assert dict(total="1", available="1") == tags["copies"].dict(exclude_none=True)

    def test_license_tags_hold_position(self, db: DatabaseTransactionFixture):
        # When a book is placed on hold, it typically takes a while
        # for the LicensePool to be updated with the new number of
        # holds. This test verifies the normal and exceptional
        # behavior used to generate the opds:holds tag in different
        # scenarios.
        edition, pool = db.edition(with_license_pool=True)
        patron = db.patron()

        # If the patron's hold position is less than the total number
        # of holds+reserves, that total is used as opds:total.
        pool.patrons_in_hold_queue = 3
        hold, is_new = pool.on_hold_to(patron, position=1)

        tags = AcquisitionHelper.license_tags(pool, None, hold)
        assert "1" == tags["holds"].position
        assert "3" == tags["holds"].total

        # If the patron's hold position is missing, we assume they
        # are last in the list.
        hold.position = None
        tags = AcquisitionHelper.license_tags(pool, None, hold)
        assert "3" == tags["holds"].position
        assert "3" == tags["holds"].total

        # If the patron's current hold position is greater than the
        # total recorded number of holds+reserves, their position will
        # be used as the value of opds:total.
        hold.position = 5
        tags = AcquisitionHelper.license_tags(pool, None, hold)
        assert "5" == tags["holds"].position
        assert "5" == tags["holds"].total

        # A patron earlier in the holds queue may see a different
        # total number of holds, but that's fine -- it doesn't matter
        # very much to that person the precise number of people behind
        # them in the queue.
        hold.position = 4
        tags = AcquisitionHelper.license_tags(pool, None, hold)
        assert "4" == tags["holds"].position
        assert "4" == tags["holds"].total

        # If the patron's hold position is zero (because the book is
        # reserved to them), we do not represent them as having a hold
        # position (so no opds:position), but they still count towards
        # opds:total in the case where the LicensePool's information
        # is out of date.
        hold.position = 0
        pool.patrons_in_hold_queue = 0
        tags = AcquisitionHelper.license_tags(pool, None, hold)
        assert "position" not in tags["holds"]
        assert "1" == tags["holds"].total

    def test_license_tags_show_unlimited_access_books(
        self, db: DatabaseTransactionFixture
    ):
        # Arrange
        edition, pool = db.edition(with_license_pool=True)
        pool.open_access = False
        pool.self_hosted = False
        pool.unlimited_access = True

        # Act
        tags = AcquisitionHelper.license_tags(pool, None, None)

        # Assert
        assert 1 == len(tags.keys())

        tag = tags["availability"]

        assert ("status" in tag.dict()) == True
        assert "available" == tag.status
        assert ("holds" in tag.dict()) == False
        assert ("copies" in tag.dict()) == False

    def test_unlimited_access_pool_loan(self, db: DatabaseTransactionFixture):
        patron = db.patron()
        work = db.work(unlimited_access=True, with_license_pool=True)
        pool = work.active_license_pool()
        loan, _ = pool.loan_to(patron)
        tags = AcquisitionHelper.license_tags(pool, loan, None)

        tag = tags["availability"]
        assert "since" in tag.dict()
        assert "until" not in tag.dict()

    def test_license_tags_show_self_hosted_books(self, db: DatabaseTransactionFixture):
        # Arrange
        edition, pool = db.edition(with_license_pool=True)
        pool.self_hosted = True
        pool.open_access = False
        pool.licenses_available = 0
        pool.licenses_owned = 0

        # Act
        tags = AcquisitionHelper.license_tags(pool, None, None)

        # Assert
        assert 1 == len(tags.keys())
        assert "status" in tags["availability"].dict()
        assert "available" == tags["availability"].status

    def test_single_entry(self, db: DatabaseTransactionFixture):
        session = db.session

        # Here's a Work with two LicensePools.
        work = db.work(with_open_access_download=True)
        original_pool = work.license_pools[0]
        edition, new_pool = db.edition(
            with_license_pool=True, with_open_access_download=True
        )
        work.license_pools.append(new_pool)

        # The presentation edition of the Work is associated with
        # the first LicensePool added to it.
        assert work.presentation_edition == original_pool.presentation_edition

        # This is the edition used when we create an <entry> tag for
        # this Work.
        private = object()
        entry = OPDSAcquisitionFeed.single_entry(
            work,
            Annotator(),
        )
        assert isinstance(entry, WorkEntry)
        assert entry.computed is not None
        assert entry.computed.title is not None

        assert new_pool.presentation_edition.title != entry.computed.title.text
        assert original_pool.presentation_edition.title == entry.computed.title.text

        # If the edition was issued before 1980, no datetime formatting error
        # is raised.
        work.simple_opds_entry = work.verbose_opds_entry = None
        five_hundred_years = datetime.timedelta(days=(500 * 365))
        work.presentation_edition.issued = utc_now() - five_hundred_years

        entry = OPDSAcquisitionFeed.single_entry(work, Annotator())
        assert isinstance(entry, WorkEntry)
        assert entry.computed is not None
        assert entry.computed.issued is not None

        expected = str(work.presentation_edition.issued.date())
        assert expected == entry.computed.issued.text

    # We are currently not using the cached entries
    @pytest.mark.skip
    def test_entry_cache_adds_missing_drm_namespace(
        self, db: DatabaseTransactionFixture
    ):
        work = db.work(with_open_access_download=True)

        # This work's OPDS entry was created with a namespace map
        # that did not include the drm: namespace.
        work.simple_opds_entry = "<entry><foo>bar</foo></entry>"

        # But now the annotator is set up to insert a tag with that
        # namespace.
        class AddDRMTagAnnotator(MockAnnotator):
            @classmethod
            def annotate_work_entry(
                cls, work, license_pool, edition, identifier, feed, entry
            ):
                drm_link = OPDSFeed.makeelement("{%s}licensor" % OPDSFeed.DRM_NS)
                entry.extend([drm_link])

        # The entry is retrieved from cache and the appropriate
        # namespace inserted.
        entry = OPDSAcquisitionFeed.single_entry(work, AddDRMTagAnnotator)  # type: ignore[arg-type]
        assert isinstance(entry, WorkEntry)
        assert (
            '<entry xmlns:drm="http://librarysimplified.org/terms/drm"><foo>bar</foo><drm:licensor/></entry>'
            == str(entry)
        )

    def test_error_when_work_has_no_identifier(self, db: DatabaseTransactionFixture):
        session = db.session

        # We cannot create an OPDS entry for a Work that cannot be associated
        # with an Identifier.
        work = db.work(title="Hello, World!", with_license_pool=True)
        work.license_pools[0].identifier = None
        work.presentation_edition.primary_identifier = None
        entry = OPDSAcquisitionFeed.single_entry(work, Annotator())
        assert entry == None

    def test_error_when_work_has_no_licensepool(self, db: DatabaseTransactionFixture):
        session = db.session

        work = db.work()
        entry = OPDSAcquisitionFeed.single_entry(work, Annotator())
        expect = OPDSAcquisitionFeed.error_message(
            work.presentation_edition.primary_identifier,
            403,
            "I've heard about this work but have no active licenses for it.",
        )
        assert expect == entry

    def test_error_when_work_has_no_presentation_edition(
        self, db: DatabaseTransactionFixture
    ):
        session = db.session

        """We cannot create an OPDS entry (or even an error message) for a
        Work that is disconnected from any Identifiers.
        """
        work = db.work(title="Hello, World!", with_license_pool=True)
        work.license_pools[0].presentation_edition = None
        work.presentation_edition = None
        entry = OPDSAcquisitionFeed.single_entry(work, Annotator())
        assert None == entry

    # We do not use the cached entry at the moment
    @pytest.mark.skip
    def test_cache_usage(self, db: DatabaseTransactionFixture):
        session = db.session

        work = db.work(with_open_access_download=True)
        feed = OPDSAcquisitionFeed(
            db.fresh_str(),
            db.fresh_url(),
            [],
            Annotator(),
        )

        # Set the Work's cached OPDS entry to something that's clearly wrong.
        tiny_entry = "<feed>cached entry</feed>"
        work.simple_opds_entry = tiny_entry

        # If we pass in use_cache=True, the cached value is used as a basis
        # for the annotated entry.
        annotator = Annotator()
        entry = feed.single_entry(work, annotator)
        assert tiny_entry == work.simple_opds_entry

        # We know what the final value looks like -- it's the cached entry
        # run through `Annotator.annotate_work_entry`.
        [pool] = work.license_pools
        xml = etree.fromstring(work.simple_opds_entry)
        annotator.annotate_work_entry(
            work, pool, pool.presentation_edition, pool.identifier, feed, xml
        )
        assert etree.tounicode(xml) == etree.tounicode(entry)

        # If we pass in use_cache=False, a new OPDS entry is created
        # from scratch, but the cache is not updated.
        entry = feed.create_entry(work, use_cache=False)
        assert etree.tounicode(entry) != tiny_entry
        assert tiny_entry == work.simple_opds_entry

        # If we pass in force_create, a new OPDS entry is created
        # and the cache is updated.
        entry = feed.create_entry(work, force_create=True)
        entry_string = etree.tounicode(entry)
        assert entry_string != tiny_entry
        assert work.simple_opds_entry != tiny_entry

        # Again, we got entry_string by running the (new) cached value
        # through `Annotator.annotate_work_entry`.
        full_entry = etree.fromstring(work.simple_opds_entry)
        annotator.annotate_work_entry(
            work, pool, pool.presentation_edition, pool.identifier, feed, full_entry
        )
        assert entry_string == etree.tounicode(full_entry)

    def test_exception_during_entry_creation_is_not_reraised(
        self, db: DatabaseTransactionFixture
    ):
        # This feed will raise an exception whenever it's asked
        # to create an entry.
        class DoomedFeed(OPDSAcquisitionFeed):
            @classmethod
            def _create_entry(cls, *args, **kwargs):
                raise Exception("I'm doomed!")

        work = db.work(with_open_access_download=True)

        # But calling create_entry() doesn't raise an exception, it
        # just returns None.
        entry = DoomedFeed.single_entry(work, Annotator())
        assert entry == None

    def test_unfilfullable_work(self, db: DatabaseTransactionFixture):

        work = db.work(with_open_access_download=True)
        [pool] = work.license_pools
        response = OPDSAcquisitionFeed.single_entry(
            work,
            MockUnfulfillableAnnotator(),  # type: ignore[arg-type]
        )
        assert isinstance(response, OPDSMessage)
        expect = OPDSAcquisitionFeed.error_message(
            pool.identifier,
            403,
            "I know about this work but can offer no way of fulfilling it.",
        )

        assert str(expect) == str(response)

    def test_format_types(self, db: DatabaseTransactionFixture):
        session = db.session

        m = AcquisitionHelper.format_types

        epub_no_drm, ignore = DeliveryMechanism.lookup(
            session, Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM
        )
        assert [Representation.EPUB_MEDIA_TYPE] == m(epub_no_drm)

        epub_adobe_drm, ignore = DeliveryMechanism.lookup(
            session, Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM
        )
        assert [DeliveryMechanism.ADOBE_DRM, Representation.EPUB_MEDIA_TYPE] == m(
            epub_adobe_drm
        )

        overdrive_streaming_text, ignore = DeliveryMechanism.lookup(
            session,
            DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            DeliveryMechanism.OVERDRIVE_DRM,
        )
        assert [
            OPDSFeed.ENTRY_TYPE,
            Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE,
        ] == m(overdrive_streaming_text)

        audiobook_drm, ignore = DeliveryMechanism.lookup(
            session,
            Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
            DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM,
        )

        assert [
            Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE
            + DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_PROFILE
        ] == m(audiobook_drm)

        # Test a case where there is a DRM scheme but no underlying
        # content type.
        findaway_manifest, ignore = DeliveryMechanism.lookup(
            session, DeliveryMechanism.FINDAWAY_DRM, None
        )
        assert [DeliveryMechanism.FINDAWAY_DRM] == m(findaway_manifest)

    def test_add_breadcrumbs(self, db: DatabaseTransactionFixture):
        session = db.session
        _db = session

        def getElementChildren(feed):
            f = feed.feed[0]
            children = f
            return children

        class MockFeed(OPDSAcquisitionFeed):
            def __init__(self):
                super().__init__("", "", [], MockAnnotator())
                self.feed = []

        lane = db.lane(display_name="lane")
        sublane = db.lane(parent=lane, display_name="sublane")
        subsublane = db.lane(parent=sublane, display_name="subsublane")
        subsubsublane = db.lane(parent=subsublane, display_name="subsubsublane")

        top_level = object()
        ep = AudiobooksEntryPoint

        def assert_breadcrumbs(expect_breadcrumbs_for, lane, **add_breadcrumbs_kwargs):
            # Create breadcrumbs leading up to `lane` and verify that
            # there is a breadcrumb for everything in
            # `expect_breadcrumbs_for` -- Lanes, EntryPoints, and the
            # top-level lane. Verify that the titles and URLs of the
            # breadcrumbs match what we expect.
            #
            # For easier reading, all assertions in this test are
            # written as calls to this function.
            feed = MockFeed()
            annotator = MockAnnotator()

            feed.add_breadcrumbs(lane, **add_breadcrumbs_kwargs)

            if not expect_breadcrumbs_for:
                # We are expecting no breadcrumbs at all;
                # nothing should have been added to the feed.
                assert [] == feed.feed
                return

            # At this point we expect at least one breadcrumb.
            crumbs = feed._feed.breadcrumbs

            entrypoint_selected = False
            entrypoint_query = "?entrypoint="

            # First, compare the titles of the breadcrumbs to what was
            # passed in. This makes test writing much easier.
            def title(x):
                if x is top_level:
                    return annotator.top_level_title()
                elif x is ep:
                    return x.INTERNAL_NAME
                else:
                    return x.display_name

            expect_titles = [title(x) for x in expect_breadcrumbs_for]
            actual_titles = [getattr(x, "title", None) for x in crumbs]
            assert expect_titles == actual_titles

            # Now, compare the URLs of the breadcrumbs. This is
            # trickier, mainly because the URLs change once an
            # entrypoint is selected.
            previous_breadcrumb_url = None

            for i, crumb in enumerate(crumbs):
                expect = expect_breadcrumbs_for[i]
                actual_url = crumb.href

                if expect is top_level:
                    # Breadcrumb for the library root.
                    expect_url = annotator.default_lane_url()
                elif expect is ep:
                    # Breadcrumb for the entrypoint selection.

                    # Beyond this point all URLs must propagate the
                    # selected entrypoint.
                    entrypoint_selected = True
                    entrypoint_query += expect.INTERNAL_NAME

                    # The URL for this breadcrumb is the URL for the
                    # previous breadcrumb with the addition of the
                    # entrypoint selection query.
                    expect_url = previous_breadcrumb_url + entrypoint_query
                else:
                    # Breadcrumb for a lane.

                    # The breadcrumb URL is determined by the
                    # Annotator.
                    lane_url = annotator.lane_url(expect)
                    if entrypoint_selected:
                        # All breadcrumbs after the entrypoint selection
                        # must propagate the entrypoint.
                        expect_url = lane_url + entrypoint_query
                    else:
                        expect_url = lane_url

                logging.debug(
                    "%s: expect=%s actual=%s", expect_titles[i], expect_url, actual_url
                )
                assert expect_url == actual_url

                # Keep track of the URL just used, in case the next
                # breadcrumb is the same URL but with an entrypoint
                # selection appended.
                previous_breadcrumb_url = actual_url

        # That was a complicated method, but now our assertions
        # are very easy to write and understand.

        # At the top level, there are no breadcrumbs whatsoever.
        assert_breadcrumbs([], None)

        # It doesn't matter if an entrypoint is selected.
        assert_breadcrumbs([], None, entrypoint=ep)

        # A lane with no entrypoint -- note that the breadcrumbs stop
        # _before_ the lane in question.
        assert_breadcrumbs([top_level], lane)

        # If you pass include_lane=True into add_breadcrumbs, the lane
        # itself is included.
        assert_breadcrumbs([top_level, lane], lane, include_lane=True)

        # A lane with an entrypoint selected
        assert_breadcrumbs([top_level, ep], lane, entrypoint=ep)
        assert_breadcrumbs(
            [top_level, ep, lane], lane, entrypoint=ep, include_lane=True
        )

        # One lane level down.
        assert_breadcrumbs([top_level, lane], sublane)
        assert_breadcrumbs([top_level, ep, lane], sublane, entrypoint=ep)
        assert_breadcrumbs(
            [top_level, ep, lane, sublane], sublane, entrypoint=ep, include_lane=True
        )

        # Two lane levels down.
        assert_breadcrumbs([top_level, lane, sublane], subsublane)
        assert_breadcrumbs([top_level, ep, lane, sublane], subsublane, entrypoint=ep)

        # Three lane levels down.
        assert_breadcrumbs(
            [top_level, lane, sublane, subsublane],
            subsubsublane,
        )

        assert_breadcrumbs(
            [top_level, ep, lane, sublane, subsublane], subsubsublane, entrypoint=ep
        )

        # Make the sublane a root lane for a certain patron type, and
        # the breadcrumbs will be start at that lane -- we won't see
        # the sublane's parent or the library root.
        sublane.root_for_patron_type = ["ya"]
        assert_breadcrumbs([], sublane)

        assert_breadcrumbs([sublane, subsublane], subsubsublane)

        assert_breadcrumbs(
            [sublane, subsublane, subsubsublane], subsubsublane, include_lane=True
        )

        # However, if an entrypoint is selected we will see a
        # breadcrumb for it between the patron root lane and its
        # child.
        assert_breadcrumbs([sublane, ep, subsublane], subsubsublane, entrypoint=ep)

        assert_breadcrumbs(
            [sublane, ep, subsublane, subsubsublane],
            subsubsublane,
            entrypoint=ep,
            include_lane=True,
        )

    def test_add_breadcrumb_links(self, db: DatabaseTransactionFixture):
        class MockFeed(OPDSAcquisitionFeed):
            add_link_calls = []
            add_breadcrumbs_call = None
            current_entrypoint = None

            def add_link(self, href, **kwargs):
                kwargs["href"] = href
                self.add_link_calls.append(kwargs)

            def add_breadcrumbs(self, lane, entrypoint):
                self.add_breadcrumbs_call = (lane, entrypoint)

            def show_current_entrypoint(self, entrypoint):
                self.current_entrypoint = entrypoint

        annotator = MockAnnotator
        feed = MockFeed("title", "url", [], MockAnnotator())

        lane = db.lane()
        sublane = db.lane(parent=lane)
        ep = AudiobooksEntryPoint
        feed.add_breadcrumb_links(sublane, ep)

        # add_link_to_feed was called twice, to create the 'start' and
        # 'up' links.
        start, up = feed.add_link_calls
        assert "start" == start["rel"]
        assert annotator.top_level_title() == start["title"]

        assert "up" == up["rel"]
        assert lane.display_name == up["title"]

        # The Lane and EntryPoint were passed into add_breadcrumbs.
        assert (sublane, ep) == feed.add_breadcrumbs_call

        # The EntryPoint was passed into show_current_entrypoint.
        assert ep == feed.current_entrypoint

    def test_show_current_entrypoint(self, db: DatabaseTransactionFixture):
        """Calling OPDSAcquisitionFeed.show_current_entrypoint annotates
        the top-level <feed> tag with information about the currently
        selected entrypoint, if any.
        """
        feed = OPDSAcquisitionFeed("title", "url", [], Annotator())

        # No entry point, no annotation.
        feed.show_current_entrypoint(None)
        assert feed._feed.entrypoint is None

        ep = AudiobooksEntryPoint
        feed.show_current_entrypoint(ep)
        assert ep.URI == feed._feed.entrypoint

    def test_facet_links_unrecognized_facets(self):
        # OPDSAcquisitionFeed.facet_links does not produce links for any
        # facet groups or facets not known to the current version of
        # the system, because it doesn't know what the links should look
        # like.
        class MockAnnotator:
            def facet_url(self, new_facets):
                return "url: " + new_facets

        class MockFacets:
            @property
            def facet_groups(self):
                """Yield a facet group+facet 4-tuple that passes the test we're
                running (which will be turned into a link), and then a
                bunch that don't (which will be ignored).
                """

                # Real facet group, real facet
                yield (
                    Facets.COLLECTION_FACET_GROUP_NAME,
                    Facets.COLLECTION_FULL,
                    "try the featured collection instead",
                    True,
                )

                # Real facet group, nonexistent facet
                yield (
                    Facets.COLLECTION_FACET_GROUP_NAME,
                    "no such facet",
                    "this facet does not exist",
                    True,
                )

                # Nonexistent facet group, real facet
                yield (
                    "no such group",
                    Facets.COLLECTION_FULL,
                    "this facet exists but it's in a nonexistent group",
                    True,
                )

                # Nonexistent facet group, nonexistent facet
                yield (
                    "no such group",
                    "no such facet",
                    "i just don't know",
                    True,
                )

        class MockFeed(OPDSAcquisitionFeed):
            links = []

            @classmethod
            def facet_link(cls, url, facet_title, group_title, selected):
                # Return the passed-in objects as is.
                return (url, facet_title, group_title, selected)

        annotator = MockAnnotator()
        facets = MockFacets()

        # The only 4-tuple yielded by facet_groups was passed on to us.
        # The link was run through MockAnnotator.facet_url(),
        # and the human-readable titles were found using lookups.
        #
        # The other three 4-tuples were ignored since we don't know
        # how to generate human-readable titles for them.
        [[url, facet, group, selected]] = MockFeed.facet_links(annotator, facets)
        assert "url: try the featured collection instead" == url
        assert Facets.FACET_DISPLAY_TITLES[Facets.COLLECTION_FULL] == facet
        assert Facets.GROUP_DISPLAY_TITLES[Facets.COLLECTION_FACET_GROUP_NAME] == group
        assert True == selected


class TestEntrypointLinkInsertionFixture:
    db: DatabaseTransactionFixture
    mock: Any
    no_eps: WorkList
    entrypoints: List[MediumEntryPoint]
    wl: WorkList
    lane: Lane
    annotator: Type[MockAnnotator]
    old_add_entrypoint_links: Callable


@pytest.fixture()
def entrypoint_link_insertion_fixture(
    db,
) -> Generator[TestEntrypointLinkInsertionFixture, None, None]:
    data = TestEntrypointLinkInsertionFixture()
    data.db = db

    # Mock for AcquisitionFeed.add_entrypoint_links
    class Mock:
        def add_entrypoint_links(self, *args):
            self.called_with = args

    data.mock = Mock()

    # A WorkList with no EntryPoints -- should not call the mock method.
    data.no_eps = WorkList()
    data.no_eps.initialize(library=db.default_library(), display_name="no_eps")

    # A WorkList with two EntryPoints -- may call the mock method
    # depending on circumstances.
    data.entrypoints = [AudiobooksEntryPoint, EbooksEntryPoint]  # type: ignore[list-item]
    data.wl = WorkList()
    # The WorkList must have at least one child, or we won't generate
    # a real groups feed for it.
    data.lane = db.lane()
    data.wl.initialize(
        library=db.default_library(),
        display_name="wl",
        entrypoints=data.entrypoints,
        children=[data.lane],
    )

    def works(_db, **kwargs):
        """Mock WorkList.works so we don't need any actual works
        to run the test.
        """
        return []

    data.no_eps.works = works  # type: ignore[method-assign, assignment]
    data.wl.works = works  # type: ignore[method-assign, assignment]

    data.annotator = MockAnnotator
    data.old_add_entrypoint_links = OPDSAcquisitionFeed.add_entrypoint_links
    OPDSAcquisitionFeed.add_entrypoint_links = data.mock.add_entrypoint_links  # type: ignore[method-assign]
    yield data
    OPDSAcquisitionFeed.add_entrypoint_links = data.old_add_entrypoint_links  # type: ignore[method-assign]


class TestEntrypointLinkInsertion:
    """Verify that the three main types of OPDS feeds -- grouped,
    paginated, and search results -- will all include links to the same
    feed but through a different entry point.
    """

    def test_groups(
        self,
        entrypoint_link_insertion_fixture: TestEntrypointLinkInsertionFixture,
        external_search_patch_fixture: ExternalSearchPatchFixture,
    ):
        data, db, session = (
            entrypoint_link_insertion_fixture,
            entrypoint_link_insertion_fixture.db,
            entrypoint_link_insertion_fixture.db.session,
        )

        # When AcquisitionFeed.groups() generates a grouped
        # feed, it will link to different entry points into the feed,
        # assuming the WorkList has different entry points.
        def run(wl=None, facets=None):
            """Call groups() and see what add_entrypoint_links
            was called with.
            """
            data.mock.called_with = None
            search = MockExternalSearchIndex()
            feed = OPDSAcquisitionFeed.groups(
                session,
                "title",
                "url",
                wl,
                MockAnnotator(),
                None,
                facets,
                search,
            )
            return data.mock.called_with

        # This WorkList has no entry points, so the mock method is not
        # even called.
        assert None == run(data.no_eps)

        # A WorkList with entry points does cause the mock method
        # to be called.
        facets = FeaturedFacets(
            minimum_featured_quality=db.default_library().settings.minimum_featured_quality,
            entrypoint=EbooksEntryPoint,
        )
        feed, make_link, entrypoints, selected = run(data.wl, facets)

        # add_entrypoint_links was passed both possible entry points
        # and the selected entry point.
        assert data.wl.entrypoints == entrypoints
        assert selected == EbooksEntryPoint

        # The make_link function that was passed in calls
        # TestAnnotator.groups_url() when passed an EntryPoint.
        assert "http://groups/?entrypoint=Book" == make_link(EbooksEntryPoint)

    def test_page(
        self, entrypoint_link_insertion_fixture: TestEntrypointLinkInsertionFixture
    ):
        data, db, session = (
            entrypoint_link_insertion_fixture,
            entrypoint_link_insertion_fixture.db,
            entrypoint_link_insertion_fixture.db.session,
        )

        # When AcquisitionFeed.page() generates the first page of a paginated
        # list, it will link to different entry points into the list,
        # assuming the WorkList has different entry points.

        def run(wl=None, facets=None, pagination=None):
            """Call page() and see what add_entrypoint_links
            was called with.
            """
            data.mock.called_with = None
            private = object()
            OPDSAcquisitionFeed.page(
                session,
                "title",
                "url",
                wl,
                data.annotator(),
                facets,
                pagination,
                MockExternalSearchIndex(),
            )

            return data.mock.called_with

        # The WorkList has no entry points, so the mock method is not
        # even called.
        assert None == run(data.no_eps)

        # Let's give the WorkList two possible entry points, and choose one.
        facets = Facets.default(db.default_library()).navigate(
            entrypoint=EbooksEntryPoint
        )
        feed, make_link, entrypoints, selected = run(data.wl, facets)

        # This time, add_entrypoint_links was called, and passed both
        # possible entry points and the selected entry point.
        assert data.wl.entrypoints == entrypoints
        assert selected == EbooksEntryPoint

        # The make_link function that was passed in calls
        # TestAnnotator.feed_url() when passed an EntryPoint. The
        # Facets object's other facet groups are propagated in this URL.
        first_page_url = "http://wl/?available=all&collection=full&collectionName=All&distributor=All&entrypoint=Book&order=author"
        assert first_page_url == make_link(EbooksEntryPoint)

        # Pagination information is not propagated through entry point links
        # -- you always start at the beginning of the list.
        pagination = Pagination(offset=100)
        feed, make_link, entrypoints, selected = run(data.wl, facets, pagination)
        assert first_page_url == make_link(EbooksEntryPoint)

    def test_search(
        self, entrypoint_link_insertion_fixture: TestEntrypointLinkInsertionFixture
    ):
        data, db, session = (
            entrypoint_link_insertion_fixture,
            entrypoint_link_insertion_fixture.db,
            entrypoint_link_insertion_fixture.db.session,
        )

        # When OPDSAcquisitionFeed.search() generates the first page of
        # search results, it will link to related searches for different
        # entry points, assuming the WorkList has different entry points.
        def run(wl=None, facets=None, pagination=None):
            """Call search() and see what add_entrypoint_links
            was called with.
            """
            data.mock.called_with = None
            OPDSAcquisitionFeed.search(
                session,
                "title",
                "url",
                wl,
                None,
                None,
                pagination=pagination,
                facets=facets,
                annotator=data.annotator(),
            )
            return data.mock.called_with

        # Mock search() so it never tries to return anything.
        def mock_search(self, *args, **kwargs):
            return []

        data.no_eps.search = mock_search  # type: ignore[method-assign, assignment]
        data.wl.search = mock_search  # type: ignore[method-assign, assignment]

        # This WorkList has no entry points, so the mock method is not
        # even called.
        assert None == run(data.no_eps)

        # The mock method is called for a WorkList that does have
        # entry points.
        facets = SearchFacets().navigate(entrypoint=EbooksEntryPoint)
        assert isinstance(facets, SearchFacets)
        feed, make_link, entrypoints, selected = run(data.wl, facets)

        # Since the SearchFacets has more than one entry point,
        # the EverythingEntryPoint is prepended to the list of possible
        # entry points.
        assert [
            EverythingEntryPoint,
            AudiobooksEntryPoint,
            EbooksEntryPoint,
        ] == entrypoints

        # add_entrypoint_links was passed the three possible entry points
        # and the selected entry point.
        assert selected == EbooksEntryPoint

        # The make_link function that was passed in calls
        # TestAnnotator.search_url() when passed an EntryPoint.
        first_page_url = "http://wl/?available=all&collection=full&entrypoint=Book&order=relevance&search_type=default"
        assert first_page_url == make_link(EbooksEntryPoint)

        # Pagination information is not propagated through entry point links
        # -- you always start at the beginning of the list.
        pagination = Pagination(offset=100)
        feed, make_link, entrypoints, selected = run(data.wl, facets, pagination)
        assert first_page_url == make_link(EbooksEntryPoint)


class TestLookupAcquisitionFeed:
    @staticmethod
    def _feed(session: Session, annotator=VerboseAnnotator, **kwargs):
        """Helper method to create a LookupAcquisitionFeed."""
        return LookupAcquisitionFeed(
            "Feed Title",
            "http://whatever.io",
            [],
            annotator(),
            **kwargs,
        )

    @staticmethod
    def _entry(
        session: Session, identifier, work, annotator=VerboseAnnotator, **kwargs
    ):
        """Helper method to create an entry."""
        feed = TestLookupAcquisitionFeed._feed(session, annotator, **kwargs)
        entry = feed.single_entry((identifier, work), feed.annotator)
        if isinstance(entry, OPDSMessage):
            return feed, entry
        return feed, entry

    def test_create_entry_uses_specified_identifier(
        self, db: DatabaseTransactionFixture
    ):
        # Here's a Work with two LicensePools.
        work = db.work(with_open_access_download=True)
        original_pool = work.license_pools[0]
        edition, new_pool = db.edition(
            with_license_pool=True, with_open_access_download=True
        )
        work.license_pools.append(new_pool)

        # We can generate two different OPDS entries for a single work
        # depending on which identifier we look up.
        ignore, e1 = self._entry(db.session, original_pool.identifier, work)
        assert original_pool.identifier.urn == e1.computed.identifier
        assert original_pool.presentation_edition.title == e1.computed.title.text
        assert new_pool.identifier.urn != e1.computed.identifier
        assert new_pool.presentation_edition.title != e1.computed.title.text

        # Different identifier and pool = different information
        i = new_pool.identifier
        ignore, e2 = self._entry(db.session, i, work)
        assert new_pool.identifier.urn == e2.computed.identifier
        assert new_pool.presentation_edition.title == e2.computed.title.text
        assert original_pool.presentation_edition.title != e2.computed.title.text
        assert original_pool.identifier.urn != e2.computed.identifier

    def test_error_on_mismatched_identifier(self, db: DatabaseTransactionFixture):
        """We get an error if we try to make it look like an Identifier lookup
        retrieved a Work that's not actually associated with that Identifier.
        """
        work = db.work(with_open_access_download=True)

        # Here's an identifier not associated with any LicensePool or
        # Work.
        identifier = db.identifier()

        # It doesn't make sense to make an OPDS feed out of that
        # Identifier and a totally random Work.
        expect_error = 'I tried to generate an OPDS entry for the identifier "%s" using a Work not associated with that identifier.'
        feed, entry = self._entry(db.session, identifier, work)
        assert entry == OPDSMessage(identifier.urn, 500, expect_error % identifier.urn)

        # Even if the Identifier does have a Work, if the Works don't
        # match, we get the same error.
        edition, lp = db.edition(with_license_pool=True)
        feed, entry = self._entry(db.session, lp.identifier, work)
        assert entry == OPDSMessage(
            lp.identifier.urn, 500, expect_error % lp.identifier.urn
        )

    def test_error_when_work_has_no_licensepool(self, db: DatabaseTransactionFixture):
        """Under most circumstances, a Work must have at least one
        LicensePool for a lookup to succeed.
        """

        # Here's a work with no LicensePools.
        work = db.work(title="Hello, World!", with_license_pool=False)
        identifier = work.presentation_edition.primary_identifier
        feed, entry = self._entry(db.session, identifier, work)
        # By default, a work is treated as 'not in the collection' if
        # there is no LicensePool for it.
        isinstance(entry, OPDSMessage)
        assert 404 == entry.status_code
        assert "Identifier not found in collection" == entry.message

    def test_unfilfullable_work(self, db: DatabaseTransactionFixture):
        work = db.work(with_open_access_download=True)
        [pool] = work.license_pools
        feed, entry = self._entry(
            db.session, pool.identifier, work, MockUnfulfillableAnnotator
        )
        expect = OPDSAcquisitionFeed.error_message(
            pool.identifier,
            403,
            "I know about this work but can offer no way of fulfilling it.",
        )
        assert expect == entry

    @pytest.mark.skip
    def test_create_entry_uses_cache_for_all_licensepools_for_work(
        self, db: DatabaseTransactionFixture
    ):
        """A Work's cached OPDS entries can be reused by all LicensePools for
        that Work, even LicensePools associated with different
        identifiers.
        """

        class InstrumentableActiveLicensePool(VerboseAnnotator):
            """A mock class that lets us control the output of
            active_license_pool.
            """

            ACTIVE = None

            @classmethod
            def active_licensepool_for(cls, work):
                return cls.ACTIVE

        feed = self._feed(db.session, annotator=InstrumentableActiveLicensePool())

        # Here are two completely different LicensePools for the same work.
        work = db.work(with_license_pool=True)
        work.verbose_opds_entry = "<entry>Cached</entry>"
        [pool1] = work.license_pools

        collection2 = db.collection()
        edition2 = db.edition()
        pool2 = db.licensepool(edition=edition2, collection=collection2)
        identifier2 = pool2.identifier
        work.license_pools.append(pool2)

        # Regardless of which LicensePool the annotator thinks is
        # 'active', passing in (identifier, work) will use the cache.
        m = feed.create_entry
        annotator = feed.annotator

        annotator.ACTIVE = pool1
        assert "Cached" == m((pool1.identifier, work)).text

        annotator.ACTIVE = pool2
        assert "Cached" == m((pool2.identifier, work)).text

        # If for some reason we pass in an identifier that is not
        # associated with the active license pool, we don't get
        # anything.
        work.license_pools = [pool1]
        result = m((identifier2, work))
        assert isinstance(result, OPDSMessage)
        assert "using a Work not associated with that identifier." in result.message


class TestNavigationFeedFixture:
    db: DatabaseTransactionFixture
    fiction: Lane
    fantasy: Lane
    romance: Lane
    contemporary_romance: Lane


@pytest.fixture()
def navigation_feed_fixture(
    db,
) -> TestNavigationFeedFixture:
    data = TestNavigationFeedFixture()
    data.db = db
    data.fiction = db.lane("Fiction")
    data.fantasy = db.lane("Fantasy", parent=data.fiction)
    data.romance = db.lane("Romance", parent=data.fiction)
    data.contemporary_romance = db.lane("Contemporary Romance", parent=data.romance)
    return data


class TestNavigationFeed:
    def test_add_entry(self):
        feed = NavigationFeed("title", "http://navigation", None, None)
        feed.add_entry("http://example.com", "Example", "text/html")
        [entry] = feed._feed.data_entries
        assert "Example" == entry.title
        [link] = entry.links
        assert "http://example.com" == link.href
        assert "text/html" == link.type
        assert "subsection" == link.rel

    def test_navigation_with_sublanes(
        self, navigation_feed_fixture: TestNavigationFeedFixture
    ):
        data, db, session = (
            navigation_feed_fixture,
            navigation_feed_fixture.db,
            navigation_feed_fixture.db.session,
        )

        private = object()
        response = NavigationFeed.navigation(
            session,
            "Navigation",
            "http://navigation",
            data.fiction,
            MockAnnotator(),
        )

        # The media type of this response is different than from the
        # typical OPDSFeedResponse.
        assert OPDSFeed.NAVIGATION_FEED_TYPE == response.as_response().content_type

        feed = response._feed

        assert "Navigation" == feed.metadata["title"].text
        [self_link] = feed.links
        assert "http://navigation" == self_link.href
        assert "self" == self_link.rel
        assert "http://navigation" == feed.metadata["id"].text
        [fantasy, romance] = sorted(feed.data_entries, key=lambda x: x.title)

        assert data.fantasy.display_name == fantasy.title
        assert "http://%s/" % data.fantasy.id == fantasy.id
        [fantasy_link] = fantasy.links
        assert "http://%s/" % data.fantasy.id == fantasy_link.href
        assert "subsection" == fantasy_link.rel
        assert OPDSFeed.ACQUISITION_FEED_TYPE == fantasy_link.type

        assert data.romance.display_name == romance.title
        assert "http://navigation/%s" % data.romance.id == romance.id
        [romance_link] = romance.links
        assert "http://navigation/%s" % data.romance.id == romance_link.href
        assert "subsection" == romance_link.rel
        assert OPDSFeed.NAVIGATION_FEED_TYPE == romance_link.type

    def test_navigation_without_sublanes(
        self, navigation_feed_fixture: TestNavigationFeedFixture
    ):
        data, db, session = (
            navigation_feed_fixture,
            navigation_feed_fixture.db,
            navigation_feed_fixture.db.session,
        )

        feed = NavigationFeed.navigation(
            session, "Navigation", "http://navigation", data.fantasy, MockAnnotator()
        )
        parsed = feed._feed
        assert "Navigation" == parsed.metadata["title"].text
        [self_link] = parsed.links
        assert "http://navigation" == self_link.href
        assert "self" == self_link.rel
        assert "http://navigation" == parsed.metadata["id"].text
        [fantasy] = parsed.data_entries

        assert "All " + data.fantasy.display_name == fantasy.title
        assert "http://%s/" % data.fantasy.id == fantasy.id
        [fantasy_link] = fantasy.links
        assert "http://%s/" % data.fantasy.id == fantasy_link.href
        assert "subsection" == fantasy_link.rel
        assert OPDSFeed.ACQUISITION_FEED_TYPE == fantasy_link.type