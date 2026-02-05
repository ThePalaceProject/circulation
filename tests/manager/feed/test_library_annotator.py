import datetime
from collections import defaultdict
from functools import partial
from unittest.mock import MagicMock, create_autospec, patch

import dateutil
import feedparser
import pytest
from freezegun import freeze_time
from lxml import etree
from uritemplate import URITemplate

from palace.manager.api.adobe_vendor_id import AuthdataUtility
from palace.manager.api.circulation.base import BaseCirculationAPI
from palace.manager.api.circulation.dispatcher import CirculationApiDispatcher
from palace.manager.api.circulation.fulfillment import RedirectFulfillment
from palace.manager.core.classifier import Classifier, Fantasy, Urban_Fantasy
from palace.manager.core.entrypoint import (
    AudiobooksEntryPoint,
    EbooksEntryPoint,
    EverythingEntryPoint,
)
from palace.manager.core.exceptions import BasePalaceException
from palace.manager.core.lcp.credential import LCPCredentialFactory, LCPHashedPassphrase
from palace.manager.data_layer.policy.presentation import (
    PresentationCalculationPolicy,
)
from palace.manager.feed.acquisition import OPDSAcquisitionFeed
from palace.manager.feed.annotator.circulation import LibraryAnnotator
from palace.manager.feed.annotator.loan_and_hold import LibraryLoanAndHoldAnnotator
from palace.manager.feed.facets.base import FacetsWithEntryPoint
from palace.manager.feed.facets.feed import Facets
from palace.manager.feed.opds import UnfulfillableWork
from palace.manager.feed.types import FeedData, WorkEntry
from palace.manager.feed.util import strftime
from palace.manager.feed.worklist.contributor import ContributorLane
from palace.manager.integration.goals import Goals
from palace.manager.integration.license.boundless.constants import (
    BAKER_TAYLOR_KDRM_PARAMS,
)
from palace.manager.integration.license.opds.opds1.xml_parser import OPDSXMLParser
from palace.manager.integration.metadata.novelist import (
    NoveListAPI,
    NoveListApiSettings,
)
from palace.manager.search.pagination import Pagination
from palace.manager.service.container import container_instance
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePoolType,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.flask_util import OPDSFeedResponse
from palace.manager.util.opds_writer import OPDSFeed
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture
from tests.fixtures.search import ExternalSearchFixtureFake
from tests.fixtures.vendor_id import VendorIDFixture
from tests.manager.feed.conftest import PatchedUrlFor


class LibraryAnnotatorFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.work = db.work(with_open_access_download=True)
        parent = db.lane(display_name="Fiction", languages=["eng"], fiction=True)
        self.lane = db.lane(display_name="Fantasy", languages=["eng"])
        self.lane.add_genre(Fantasy.name)
        self.lane.parent = parent
        self.annotator = LibraryAnnotator(
            None,
            self.lane,
            db.default_library(),
            top_level_title="Test Top Level Title",
        )

        # Initialize library with Adobe Vendor ID details
        db.default_library().library_registry_short_name = "FAKE"
        db.default_library().library_registry_shared_secret = "s3cr3t5"

        # A ContributorLane to test code that handles it differently.
        self.contributor, ignore = db.contributor("Someone")
        self.contributor_lane = ContributorLane(
            db.default_library(), self.contributor, languages=["eng"], audiences=None
        )


@pytest.fixture(scope="function")
def annotator_fixture(
    db: DatabaseTransactionFixture,
    patch_url_for: PatchedUrlFor,
    external_search_fake_fixture: ExternalSearchFixtureFake,
) -> LibraryAnnotatorFixture:
    return LibraryAnnotatorFixture(db)


class TestLibraryAnnotator:
    def test_add_configuration_links(
        self,
        annotator_fixture: LibraryAnnotatorFixture,
        library_fixture: LibraryFixture,
    ):
        mock_feed = FeedData()

        # Set up configuration settings for links.
        library = annotator_fixture.db.default_library()
        settings = library_fixture.settings(library)
        settings.terms_of_service = "http://terms/"
        settings.privacy_policy = "http://privacy/"
        settings.copyright = "http://copyright/"
        settings.about = "http://about/"
        settings.license = "http://license/"
        settings.help_email = "help@me"
        settings.help_web = "http://help/"

        # Set up settings for navigation links.
        settings.web_header_links = ["http://example.com/1", "http://example.com/2"]
        settings.web_header_labels = ["one", "two"]

        annotator_fixture.annotator.add_configuration_links(mock_feed)

        assert 9 == len(mock_feed.links)

        mock_feed_links = sorted(mock_feed.links, key=lambda x: x.rel or "")
        expected_links = [
            (link.href, link.type) for link in mock_feed_links if link.rel != "related"
        ]

        # They are the links we'd expect.
        assert [
            ("http://about/", "text/html"),
            ("http://copyright/", "text/html"),
            ("mailto:help@me", None),
            ("http://help/", "text/html"),
            ("http://license/", "text/html"),
            ("http://privacy/", "text/html"),
            ("http://terms/", "text/html"),
        ] == expected_links

        # There are two navigation links.
        navigation_links = [x for x in mock_feed_links if x.rel == "related"]
        assert {"navigation"} == {x.role for x in navigation_links}
        assert {"http://example.com/1", "http://example.com/2"} == {
            x.href for x in navigation_links
        }
        assert {"one", "two"} == {x.title for x in navigation_links}

    def test_top_level_title(self, annotator_fixture: LibraryAnnotatorFixture):
        assert "Test Top Level Title" == annotator_fixture.annotator.top_level_title()

    def test_group_uri_with_flattened_lane(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        spanish_lane = annotator_fixture.db.lane(
            display_name="Spanish", languages=["spa"]
        )
        flat_spanish_lane = dict(
            {"lane": spanish_lane, "label": "All Spanish", "link_to_list_feed": True}
        )
        spanish_work = annotator_fixture.db.work(
            title="Spanish Book", with_license_pool=True, language="spa"
        )
        lp = spanish_work.license_pools[0]
        annotator_fixture.annotator.lanes_by_work[spanish_work].append(
            flat_spanish_lane
        )

        feed_url = annotator_fixture.annotator.feed_url(spanish_lane)
        group_uri = annotator_fixture.annotator.group_uri(
            spanish_work, lp, lp.identifier
        )
        assert (feed_url, "All Spanish") == group_uri

    def test_lane_url(self, annotator_fixture: LibraryAnnotatorFixture):
        fantasy_lane_with_sublanes = annotator_fixture.db.lane(
            display_name="Fantasy with sublanes", languages=["eng"]
        )
        fantasy_lane_with_sublanes.add_genre(Fantasy.name)

        urban_fantasy_lane = annotator_fixture.db.lane(display_name="Urban Fantasy")
        urban_fantasy_lane.add_genre(Urban_Fantasy.name)
        fantasy_lane_with_sublanes.sublanes.append(urban_fantasy_lane)

        fantasy_lane_without_sublanes = annotator_fixture.db.lane(
            display_name="Fantasy without sublanes", languages=["eng"]
        )
        fantasy_lane_without_sublanes.add_genre(Fantasy.name)

        default_lane_url = annotator_fixture.annotator.lane_url(None)
        assert default_lane_url == annotator_fixture.annotator.default_lane_url()

        facets = FacetsWithEntryPoint(entrypoint=EbooksEntryPoint)
        default_lane_url = annotator_fixture.annotator.lane_url(None, facets=facets)
        assert default_lane_url == annotator_fixture.annotator.default_lane_url(
            facets=facets
        )

        groups_url = annotator_fixture.annotator.lane_url(fantasy_lane_with_sublanes)
        assert groups_url == annotator_fixture.annotator.groups_url(
            fantasy_lane_with_sublanes
        )

        groups_url = annotator_fixture.annotator.lane_url(
            fantasy_lane_with_sublanes, facets=facets
        )
        assert groups_url == annotator_fixture.annotator.groups_url(
            fantasy_lane_with_sublanes, facets=facets
        )

        feed_url = annotator_fixture.annotator.lane_url(fantasy_lane_without_sublanes)
        assert feed_url == annotator_fixture.annotator.feed_url(
            fantasy_lane_without_sublanes
        )

        feed_url = annotator_fixture.annotator.lane_url(
            fantasy_lane_without_sublanes, facets=facets
        )
        assert feed_url == annotator_fixture.annotator.feed_url(
            fantasy_lane_without_sublanes, facets=facets
        )

    def test_fulfill_link_issues_only_open_access_links_when_library_does_not_identify_patrons(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        # This library doesn't identify patrons.
        annotator_fixture.annotator.identifies_patrons = False

        # Because of this, normal fulfillment links are not generated.
        [pool] = annotator_fixture.work.license_pools
        [lpdm] = pool.delivery_mechanisms
        assert None == annotator_fixture.annotator.fulfill_link(pool, None, lpdm)

        # However, fulfillment links _can_ be generated with the
        # 'open-access' link relation.
        link = annotator_fixture.annotator.fulfill_link(
            pool, None, lpdm, OPDSFeed.OPEN_ACCESS_REL
        )
        assert link is not None
        assert OPDSFeed.OPEN_ACCESS_REL == link.rel

    def test_fulfill_is_templated_when_delivery_mechanism_requires_it(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        [pool] = annotator_fixture.work.license_pools
        [lpdm] = pool.delivery_mechanisms
        lpdm.delivery_mechanism.content_type = Representation.EPUB_MEDIA_TYPE
        lpdm.delivery_mechanism.drm_scheme = DeliveryMechanism.BAKER_TAYLOR_KDRM_DRM
        link = annotator_fixture.annotator.fulfill_link(pool, None, lpdm)

        assert link is not None
        assert link.templated is True
        assert link.href is not None
        assert URITemplate(link.href).variable_names == BAKER_TAYLOR_KDRM_PARAMS

    # We freeze the test time here, because this test checks that the client token
    # in the feed matches a generated client token. The client token contains an
    # expiry date based on the current time, so this test can be flaky in a slow
    # integration environment unless we make sure the clock does not change as this
    # test is being performed.
    @freeze_time("1867-07-01")
    def test_fulfill_link_includes_device_registration_tags(
        self,
        annotator_fixture: LibraryAnnotatorFixture,
        vendor_id_fixture: VendorIDFixture,
    ):
        """Verify that when Adobe Vendor ID delegation is included, the
        fulfill link for an Adobe delivery mechanism includes instructions
        on how to get a Vendor ID.
        """
        vendor_id_fixture.initialize_adobe(annotator_fixture.db.default_library())
        [pool] = annotator_fixture.work.license_pools
        identifier = pool.identifier
        patron = annotator_fixture.db.patron()
        old_credentials = list(patron.credentials)

        loan, ignore = pool.loan_to(patron, start=utc_now())
        adobe_delivery_mechanism, ignore = DeliveryMechanism.lookup(
            annotator_fixture.db.session, "text/html", DeliveryMechanism.ADOBE_DRM
        )
        other_delivery_mechanism, ignore = DeliveryMechanism.lookup(
            annotator_fixture.db.session, "text/html", DeliveryMechanism.OVERDRIVE_DRM
        )

        # The fulfill link for non-Adobe DRM does not
        # include the drm:licensor tag.
        link = annotator_fixture.annotator.fulfill_link(
            pool, loan, other_delivery_mechanism
        )
        assert link is not None
        assert link.drm_licensor is None

        # No new Credential has been associated with the patron.
        assert old_credentials == patron.credentials

        # The fulfill link for Adobe DRM includes information
        # on how to get an Adobe ID in the drm:licensor tag.
        link = annotator_fixture.annotator.fulfill_link(
            pool, loan, adobe_delivery_mechanism
        )
        assert link is not None
        assert link.drm_licensor is not None

        # An Adobe ID-specific identifier has been created for the patron.
        [adobe_id_identifier] = [
            x for x in patron.credentials if x not in old_credentials
        ]
        assert (
            AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER
            == adobe_id_identifier.type
        )
        assert DataSource.INTERNAL_PROCESSING == adobe_id_identifier.data_source.name
        assert None == adobe_id_identifier.expires

        # The drm:licensor tag is the one we get by calling
        # adobe_id_tags() on that identifier.
        assert adobe_id_identifier.credential is not None
        expect = annotator_fixture.annotator.adobe_id_tags(
            adobe_id_identifier.credential
        )
        assert link is not None
        assert expect == link.drm_licensor

    def test_no_adobe_id_tags_when_vendor_id_not_configured(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        """When vendor ID delegation is not configured, adobe_id_tags()
        returns an empty list.
        """
        assert annotator_fixture.annotator.adobe_id_tags("patron identifier") is None

    def test_adobe_id_tags_when_vendor_id_configured(
        self,
        annotator_fixture: LibraryAnnotatorFixture,
        vendor_id_fixture: VendorIDFixture,
    ):
        """When vendor ID delegation is configured, adobe_id_tags()
        returns a list containing a single tag. The tag contains
        the information necessary to get an Adobe ID and a link to the local
        DRM Device Management Protocol endpoint.
        """
        library = annotator_fixture.db.default_library()
        vendor_id_fixture.initialize_adobe(library)
        patron_identifier = "patron identifier"
        element = annotator_fixture.annotator.adobe_id_tags(patron_identifier)

        assert element is not None
        assert vendor_id_fixture.TEST_VENDOR_ID == getattr(element, "vendor", None)

        token = getattr(element, "client_token", None)
        assert token is not None
        # token is a token which we can decode, since we know
        # the secret.
        token_text = token
        authdata = AuthdataUtility.from_config(library)
        assert authdata is not None
        decoded = authdata.decode_short_client_token(token_text)
        expected_url = str(library.settings.website)
        assert (expected_url, patron_identifier) == decoded

        # If we call adobe_id_tags again we'll get a distinct tag
        # object that renders to the same data.
        same_tag = annotator_fixture.annotator.adobe_id_tags(patron_identifier)
        assert same_tag is not element
        assert same_tag == element

        # If the Adobe Vendor ID configuration is present but
        # incomplete, adobe_id_tags does nothing.

        # Delete one setting from the existing integration to check
        # this.
        vendor_id_fixture.registration.short_name = None
        assert annotator_fixture.annotator.adobe_id_tags("new identifier") is None

    def test_lcp_acquisition_link_contains_hashed_passphrase(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        [pool] = annotator_fixture.work.license_pools
        identifier = pool.identifier
        patron = annotator_fixture.db.patron()

        hashed_password = LCPHashedPassphrase("hashed password")

        # Setup LCP credentials
        lcp_credential_factory = LCPCredentialFactory()
        lcp_credential_factory.set_hashed_passphrase(
            annotator_fixture.db.session, patron, hashed_password
        )

        loan, ignore = pool.loan_to(patron, start=utc_now())
        lcp_delivery_mechanism, ignore = DeliveryMechanism.lookup(
            annotator_fixture.db.session, "text/html", DeliveryMechanism.LCP_DRM
        )
        other_delivery_mechanism, ignore = DeliveryMechanism.lookup(
            annotator_fixture.db.session, "text/html", DeliveryMechanism.OVERDRIVE_DRM
        )

        # The fulfill link for non-LCP DRM does not include the hashed_passphrase tag.
        link = annotator_fixture.annotator.fulfill_link(
            pool, loan, other_delivery_mechanism
        )
        assert link is not None
        assert link.lcp_hashed_passphrase is None

        # The fulfill link for lcp DRM includes hashed_passphrase
        link = annotator_fixture.annotator.fulfill_link(
            pool, loan, lcp_delivery_mechanism
        )
        assert link is not None
        assert link.lcp_hashed_passphrase is not None
        assert link.lcp_hashed_passphrase == hashed_password.hashed

    def test_default_lane_url(self, annotator_fixture: LibraryAnnotatorFixture):
        default_lane_url = annotator_fixture.annotator.default_lane_url()
        assert "groups" in default_lane_url
        assert str(annotator_fixture.lane.id) not in default_lane_url

        facets = FacetsWithEntryPoint(entrypoint=EbooksEntryPoint)
        default_lane_url = annotator_fixture.annotator.default_lane_url(facets=facets)
        assert "entrypoint=Book" in default_lane_url

    def test_groups_url(self, annotator_fixture: LibraryAnnotatorFixture):
        groups_url_no_lane = annotator_fixture.annotator.groups_url(None)
        assert "groups" in groups_url_no_lane
        assert str(annotator_fixture.lane.id) not in groups_url_no_lane

        groups_url_fantasy = annotator_fixture.annotator.groups_url(
            annotator_fixture.lane
        )
        assert "groups" in groups_url_fantasy
        assert str(annotator_fixture.lane.id) in groups_url_fantasy

        facets = Facets.default(
            annotator_fixture.db.default_library(), order="someorder"
        )
        groups_url_facets = annotator_fixture.annotator.groups_url(None, facets=facets)
        assert "order=someorder" in groups_url_facets

    def test_feed_url(self, annotator_fixture: LibraryAnnotatorFixture):
        # A regular Lane.
        feed_url_fantasy = annotator_fixture.annotator.feed_url(
            annotator_fixture.lane,
            Facets.default(annotator_fixture.db.default_library(), order="order"),
            Pagination.default(),
        )
        assert "feed" in feed_url_fantasy
        assert "order=order" in feed_url_fantasy
        assert str(annotator_fixture.lane.id) in feed_url_fantasy

        default_library = annotator_fixture.db.default_library()
        assert default_library.name is not None
        assert default_library.name in feed_url_fantasy

        # A QueryGeneratedLane.
        annotator_fixture.annotator.lane = annotator_fixture.contributor_lane
        feed_url_contributor = annotator_fixture.annotator.feed_url(
            annotator_fixture.contributor_lane,
            Facets.default(annotator_fixture.db.default_library()),
            Pagination.default(),
        )
        assert annotator_fixture.contributor_lane.ROUTE in feed_url_contributor
        assert (
            annotator_fixture.contributor_lane.contributor_key in feed_url_contributor
        )
        default_library = annotator_fixture.db.default_library()
        assert default_library.name is not None
        assert default_library.name in feed_url_contributor

    def test_search_url(self, annotator_fixture: LibraryAnnotatorFixture):
        search_url = annotator_fixture.annotator.search_url(
            annotator_fixture.lane,
            "query",
            Pagination.default(),
            Facets.default(annotator_fixture.db.default_library(), order="Book"),
        )
        assert "search" in search_url
        assert "query" in search_url
        assert "order=Book" in search_url
        assert str(annotator_fixture.lane.id) in search_url

    def test_facet_url(self, annotator_fixture: LibraryAnnotatorFixture):
        # A regular Lane.
        facets = Facets.default(
            annotator_fixture.db.default_library(),
        )
        facet_url = annotator_fixture.annotator.facet_url(facets)
        assert str(annotator_fixture.lane.id) in facet_url

        # A QueryGeneratedLane.
        annotator_fixture.annotator.lane = annotator_fixture.contributor_lane

        facet_url_contributor = annotator_fixture.annotator.facet_url(facets)
        assert annotator_fixture.contributor_lane.ROUTE in facet_url_contributor
        assert (
            annotator_fixture.contributor_lane.contributor_key in facet_url_contributor
        )

    def test_alternate_link_is_permalink(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        work = annotator_fixture.db.work(with_open_access_download=True)
        works = annotator_fixture.db.session.query(Work)
        annotator = LibraryAnnotator(
            None,
            annotator_fixture.lane,
            annotator_fixture.db.default_library(),
        )
        pool = annotator.active_licensepool_for(work)

        feed = self.get_parsed_feed(annotator_fixture, [work])
        [entry] = feed.entries
        assert entry.computed is not None
        assert pool is not None
        assert entry.computed.identifier == pool.identifier.urn

        [(alternate, type)] = [
            (x.href, x.type) for x in entry.computed.other_links if x.rel == "alternate"
        ]
        permalink, permalink_type = annotator_fixture.annotator.permalink_for(
            pool.identifier
        )
        assert alternate == permalink
        assert OPDSFeed.ENTRY_TYPE == type
        assert permalink_type == type

        # Make sure we are using the 'permalink' controller -- we were using
        # 'work' and that was wrong.
        assert "/host/permalink" in permalink

    def test_annotate_work_entry(self, annotator_fixture: LibraryAnnotatorFixture):
        lane = annotator_fixture.db.lane()

        # Create a Work.
        work = annotator_fixture.db.work(with_license_pool=True)
        [pool] = work.license_pools
        identifier = pool.identifier
        edition = pool.presentation_edition

        # Try building an entry for this Work with and without
        # patron authentication turned on -- each setting is valid
        # but will result in different links being available.
        linksets = []
        for auth in (True, False):
            annotator = LibraryAnnotator(
                None,
                lane,
                annotator_fixture.db.default_library(),
                library_identifies_patrons=auth,
            )
            work_entry = WorkEntry(
                work=work,
                license_pool=pool,
                edition=work.presentation_edition,
                identifier=work.presentation_edition.primary_identifier,
            )
            annotator.annotate_work_entry(work_entry)

            assert work_entry.computed is not None
            linksets.append(
                {
                    x.rel
                    for x in (
                        work_entry.computed.other_links
                        + work_entry.computed.acquisition_links
                    )
                }
            )

        with_auth, no_auth = linksets

        # Some links are present no matter what.
        for expect in ["alternate", "related"]:
            assert expect in with_auth
            assert expect in no_auth

        # A library with patron authentication offers some additional
        # links -- one to borrow the book and one to annotate the
        # book.
        for expect in [
            "http://www.w3.org/ns/oa#annotationService",
            "http://opds-spec.org/acquisition/borrow",
        ]:
            assert expect in with_auth
            assert expect not in no_auth

        # We can also build an entry for a work with no license pool,
        # but it will have no borrow link.
        work = annotator_fixture.db.work(with_license_pool=False)
        edition = work.presentation_edition
        identifier = edition.primary_identifier

        annotator = LibraryAnnotator(
            None,
            lane,
            annotator_fixture.db.default_library(),
            library_identifies_patrons=True,
        )
        work_entry = WorkEntry(
            work=work, license_pool=None, edition=edition, identifier=identifier
        )

        with patch.object(
            container_instance().analytics.analytics(),
            "is_configured",
            lambda: False,
        ):
            annotator.annotate_work_entry(work_entry)
        assert work_entry.computed is not None
        links = {
            x.rel
            for x in (
                work_entry.computed.other_links + work_entry.computed.acquisition_links
            )
        }

        # These links are still present.
        for expect in [
            "alternate",
            "related",
            "http://www.w3.org/ns/oa#annotationService",
        ]:
            assert expect in links

        # But the borrow link is gone.
        assert "http://opds-spec.org/acquisition/borrow" not in links

        # There are no links to create analytics events for this title,
        # because the library has no analytics configured.
        open_book_rel = "http://librarysimplified.org/terms/rel/analytics/open-book"
        assert open_book_rel not in links

        # If analytics are configured, a link is added to
        # create an 'open_book' analytics event for this title.
        work_entry = WorkEntry(
            work=work, license_pool=None, edition=edition, identifier=identifier
        )
        annotator.annotate_work_entry(work_entry)

        assert work_entry.computed is not None
        [analytics_link] = [
            x.href for x in work_entry.computed.other_links if x.rel == open_book_rel
        ]
        expect = annotator.url_for(
            "track_analytics_event",
            identifier_type=identifier.type,
            identifier=identifier.identifier,
            event_type=CirculationEvent.OPEN_BOOK,
            library_short_name=annotator_fixture.db.default_library().short_name,
            _external=True,
        )
        assert expect == analytics_link

        # Test sample link with media types
        link, _ = edition.primary_identifier.add_link(
            Hyperlink.SAMPLE,
            "http://example.org/sample",
            edition.data_source,
            media_type="application/epub+zip",
        )
        work_entry = WorkEntry(
            work=work, license_pool=None, edition=edition, identifier=identifier
        )
        annotator.annotate_work_entry(work_entry)
        assert work_entry.computed is not None
        [feed_link] = [
            l
            for l in work_entry.computed.other_links
            if l.rel == Hyperlink.CLIENT_SAMPLE
        ]
        assert feed_link.href == link.resource.url
        assert feed_link.type == link.resource.representation.media_type

    def test_annotate_feed(self, annotator_fixture: LibraryAnnotatorFixture):
        lane = annotator_fixture.db.lane()
        linksets = []
        for auth in (True, False):
            annotator = LibraryAnnotator(
                None,
                lane,
                annotator_fixture.db.default_library(),
                library_identifies_patrons=auth,
            )
            feed = OPDSAcquisitionFeed("test", "url", [], annotator)
            annotator.annotate_feed(feed._feed)
            linksets.append([x.rel for x in feed._feed.links])

        with_auth, without_auth = linksets

        # There's always a a search link, and an auth
        # document link.
        for rel in ("search", "http://opds-spec.org/auth/document"):
            assert rel in with_auth
            assert rel in without_auth

        # But there's only a bookshelf link and an annotation link
        # when patron authentication is enabled.
        for rel in (
            "http://opds-spec.org/shelf",
            "http://www.w3.org/ns/oa#annotationService",
        ):
            assert rel in with_auth
            assert rel not in without_auth

    def get_parsed_feed(
        self, annotator_fixture: LibraryAnnotatorFixture, works, lane=None, **kwargs
    ):
        if not lane:
            lane = annotator_fixture.db.lane(display_name="Main Lane")

        feed = OPDSAcquisitionFeed(
            "url",
            "test",
            works,
            LibraryAnnotator(
                None,
                lane,
                annotator_fixture.db.default_library(),
                **kwargs,
            ),
            facets=FacetsWithEntryPoint(),
            pagination=Pagination.default(),
        )
        feed.generate_feed()
        return feed._feed

    def assert_link_on_entry(
        self, entry, link_type=None, rels=None, partials_by_rel=None
    ):
        """Asserts that a link with a certain 'rel' value exists on a
        given feed or entry, as well as its link 'type' value and parts
        of its 'href' value.
        """

        def get_link_by_rel(rel):
            if isinstance(entry, WorkEntry):
                links = entry.computed.other_links + entry.computed.acquisition_links
            elif isinstance(entry, list):
                links = [e.link for e in entry]
            else:
                links = [entry.link]
            try:
                [link] = [x for x in links if x.rel == rel]
            except ValueError as e:
                raise AssertionError
            if link_type:
                assert link_type == link.type
            return link

        if rels:
            [get_link_by_rel(rel) for rel in rels]

        partials_by_rel = partials_by_rel or dict()
        for rel, uri_partials in list(partials_by_rel.items()):
            link = get_link_by_rel(rel)
            if not isinstance(uri_partials, list):
                uri_partials = [uri_partials]
            for part in uri_partials:
                assert part in link.href

    def test_work_entry_includes_open_access_or_borrow_link(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        open_access_work = annotator_fixture.db.work(with_open_access_download=True)
        licensed_work = annotator_fixture.db.work(with_license_pool=True)
        licensed_work.license_pools[0].open_access = False

        feed = self.get_parsed_feed(
            annotator_fixture, [open_access_work, licensed_work]
        )
        [open_access_entry, licensed_entry] = feed.entries

        self.assert_link_on_entry(open_access_entry, rels=[OPDSFeed.BORROW_REL])
        self.assert_link_on_entry(licensed_entry, rels=[OPDSFeed.BORROW_REL])

    def test_language_and_audience_key_from_work(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        work = annotator_fixture.db.work(
            language="eng", audience=Classifier.AUDIENCE_CHILDREN
        )
        result = annotator_fixture.annotator.language_and_audience_key_from_work(work)
        assert ("eng", "Children") == result

        work = annotator_fixture.db.work(
            language="fre", audience=Classifier.AUDIENCE_YOUNG_ADULT
        )
        result = annotator_fixture.annotator.language_and_audience_key_from_work(work)
        assert ("fre", "All+Ages,Children,Young+Adult") == result

        work = annotator_fixture.db.work(
            language="spa", audience=Classifier.AUDIENCE_ADULT
        )
        result = annotator_fixture.annotator.language_and_audience_key_from_work(work)
        assert ("spa", "Adult,Adults+Only,All+Ages,Children,Young+Adult") == result

        work = annotator_fixture.db.work(audience=Classifier.AUDIENCE_ADULTS_ONLY)
        result = annotator_fixture.annotator.language_and_audience_key_from_work(work)
        assert ("eng", "Adult,Adults+Only,All+Ages,Children,Young+Adult") == result

        work = annotator_fixture.db.work(audience=Classifier.AUDIENCE_RESEARCH)
        result = annotator_fixture.annotator.language_and_audience_key_from_work(work)
        assert (
            "eng",
            "Adult,Adults+Only,All+Ages,Children,Research,Young+Adult",
        ) == result

        work = annotator_fixture.db.work(audience=Classifier.AUDIENCE_ALL_AGES)
        result = annotator_fixture.annotator.language_and_audience_key_from_work(work)
        assert ("eng", "All+Ages,Children") == result

    def test_work_entry_includes_contributor_links(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        """ContributorLane links are added to works with contributors"""
        work = annotator_fixture.db.work(with_open_access_download=True)
        contributor1 = work.presentation_edition.author_contributors[0]
        feed = self.get_parsed_feed(annotator_fixture, [work])
        [entry] = feed.entries

        expected_rel_and_partial = dict(contributor="/contributor")
        self.assert_link_on_entry(
            entry.computed.authors,
            link_type=OPDSFeed.ACQUISITION_FEED_TYPE,
            partials_by_rel=expected_rel_and_partial,
        )

        # When there are two authors, they each get a contributor link.
        work.presentation_edition.add_contributor("Oprah", Contributor.Role.AUTHOR)
        work.calculate_presentation(
            PresentationCalculationPolicy(),
        )
        [entry] = self.get_parsed_feed(annotator_fixture, [work]).entries
        contributor_links = [
            l.link for l in entry.computed.authors if hasattr(l, "link")
        ]
        assert 2 == len(contributor_links)
        contributor_links.sort(key=lambda l: l.href)
        for l in contributor_links:
            assert l.type == OPDSFeed.ACQUISITION_FEED_TYPE
            assert "/contributor" in l.href
        assert contributor1.sort_name in contributor_links[0].href
        assert "Oprah" in contributor_links[1].href

        # When there's no author, there's no contributor link.
        annotator_fixture.db.session.delete(work.presentation_edition.contributions[0])
        annotator_fixture.db.session.delete(work.presentation_edition.contributions[1])
        annotator_fixture.db.session.commit()
        work.calculate_presentation(
            PresentationCalculationPolicy(),
        )
        [entry] = self.get_parsed_feed(annotator_fixture, [work]).entries
        assert [] == [l.link for l in entry.computed.authors if l.link]

    def test_work_entry_includes_series_link(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        """A series lane link is added to the work entry when its in a series"""
        work = annotator_fixture.db.work(
            with_open_access_download=True, series="Serious Cereals Series"
        )
        feed = self.get_parsed_feed(annotator_fixture, [work])
        [entry] = feed.entries
        expected_rel_and_partial = dict(series="/series")
        self.assert_link_on_entry(
            entry.computed.series,
            link_type=OPDSFeed.ACQUISITION_FEED_TYPE,
            partials_by_rel=expected_rel_and_partial,
        )

        # When there's no series, there's no series link.
        work = annotator_fixture.db.work(with_open_access_download=True)
        feed = self.get_parsed_feed(annotator_fixture, [work])
        [entry] = feed.entries
        assert None == entry.computed.series

    def test_work_entry_includes_recommendations_link(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        work = annotator_fixture.db.work(with_open_access_download=True)

        # If NoveList Select isn't configured, there's no recommendations link.
        feed = self.get_parsed_feed(annotator_fixture, [work])
        [entry] = feed.entries
        assert [] == [
            l for l in entry.computed.other_links if l.rel == "recommendations"
        ]

        # There's a recommendation link when configuration is found, though!
        annotator_fixture.db.integration_configuration(
            protocol=NoveListAPI,
            goal=Goals.METADATA_GOAL,
            libraries=[annotator_fixture.db.default_library()],
            settings=NoveListApiSettings(username="library", password="sure"),
        )

        feed = self.get_parsed_feed(annotator_fixture, [work])
        [entry] = feed.entries
        expected_rel_and_partial = dict(recommendations="/recommendations")
        self.assert_link_on_entry(
            entry,
            link_type=OPDSFeed.ACQUISITION_FEED_TYPE,
            partials_by_rel=expected_rel_and_partial,
        )

    @patch.object(NoveListAPI, "is_configured_db_check", return_value=False)
    def test_annotator_lazy_cache_novelist_is_configured(
        self,
        mock_is_configured: MagicMock,
        annotator_fixture: LibraryAnnotatorFixture,
    ):
        """Ensure that we call NoveList.is_configured only once per annotator instance."""
        works = [
            annotator_fixture.db.work(with_open_access_download=True) for _ in range(3)
        ]

        annotator_fixture.db.integration_configuration(
            protocol=NoveListAPI,
            goal=Goals.METADATA_GOAL,
            libraries=[annotator_fixture.db.default_library()],
            settings=NoveListApiSettings(username="library", password="sure"),
        )

        # Feed with one entry.
        feed = self.get_parsed_feed(annotator_fixture, [works[0]])
        assert len(feed.entries) == 1
        assert mock_is_configured.call_count == 1
        mock_is_configured.reset_mock()

        # Feed with three entries.
        feed = self.get_parsed_feed(annotator_fixture, works)
        assert len(feed.entries) == 3
        assert mock_is_configured.call_count == 1

    def test_work_entry_includes_annotations_link(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        work = annotator_fixture.db.work(with_open_access_download=True)
        identifier_str = work.license_pools[0].identifier.identifier
        uri_parts = ["/annotations", identifier_str]
        annotation_rel = "http://www.w3.org/ns/oa#annotationService"
        rel_with_partials = {annotation_rel: uri_parts}

        feed = self.get_parsed_feed(annotator_fixture, [work])
        [entry] = feed.entries
        self.assert_link_on_entry(entry, partials_by_rel=rel_with_partials)

        # If the library does not authenticate patrons, no link to the
        # annotation service is provided.
        feed = self.get_parsed_feed(
            annotator_fixture, [work], library_identifies_patrons=False
        )
        [entry] = feed.entries
        assert annotation_rel not in [x.rel for x in entry.computed.other_links]

    def test_active_loan_feed(
        self,
        annotator_fixture: LibraryAnnotatorFixture,
        vendor_id_fixture: VendorIDFixture,
    ):
        vendor_id_fixture.initialize_adobe(annotator_fixture.db.default_library())
        patron = annotator_fixture.db.patron()
        annotator = LibraryLoanAndHoldAnnotator(
            None,
            annotator_fixture.lane,
            annotator_fixture.db.default_library(),
            patron=patron,
        )

        response = OPDSAcquisitionFeed.active_loans_for(
            None, patron, annotator
        ).as_response()

        # The feed is private and should not be cached.
        assert isinstance(response, OPDSFeedResponse)

        # No entries in the feed...
        raw = str(response)
        feed = feedparser.parse(raw)
        assert 0 == len(feed["entries"])

        # ... but we have a link to the User Profile Management
        # Protocol endpoint...
        links = feed["feed"]["links"]
        [upmp_link] = [
            x
            for x in links
            if x["rel"] == "http://librarysimplified.org/terms/rel/user-profile"
        ]
        annotator = LibraryLoanAndHoldAnnotator(
            None, None, library=patron.library, patron=patron
        )
        expect_url = annotator.url_for(
            "patron_profile",
            library_short_name=patron.library.short_name,
            _external=True,
        )
        assert expect_url == upmp_link["href"]

        # ... and we have DRM licensing information.
        tree = etree.fromstring(response.get_data(as_text=True))
        parser = OPDSXMLParser()
        licensor = parser._xpath1(tree, "//atom:feed/drm:licensor")
        assert licensor is not None
        adobe_patron_identifier = AuthdataUtility._adobe_patron_identifier(patron)

        # The DRM licensing information includes the Adobe vendor ID
        # and the patron's patron identifier for Adobe purposes.
        assert (
            vendor_id_fixture.TEST_VENDOR_ID
            == licensor.attrib["{http://librarysimplified.org/terms/drm}vendor"]
        )
        [client_token] = licensor
        assert vendor_id_fixture.registration.short_name is not None
        expected = vendor_id_fixture.registration.short_name.upper()
        assert client_token.text.startswith(expected)
        assert adobe_patron_identifier in client_token.text

        # Unlike other places this tag shows up, we use the
        # 'scheme' attribute to explicitly state that this
        # <drm:licensor> tag is talking about an ACS licensing
        # scheme. Since we're in a <feed> and not a <link> to a
        # specific book, that context would otherwise be lost.
        assert (
            "http://librarysimplified.org/terms/drm/scheme/ACS"
            == licensor.attrib["{http://librarysimplified.org/terms/drm}scheme"]
        )

        # Since we're taking a round trip to and from OPDS, which only
        # represents times with second precision, generate the current
        # time with second precision to make later comparisons
        # possible.
        now = utc_now().replace(microsecond=0)
        tomorrow = now + datetime.timedelta(days=1)

        # A loan of an open-access book is open-ended.
        work1 = annotator_fixture.db.work(
            language="eng", with_open_access_download=True
        )
        loan1 = work1.license_pools[0].loan_to(patron, start=now)

        # A loan of some other kind of book has an end point.
        work2 = annotator_fixture.db.work(language="eng", with_license_pool=True)
        loan2 = work2.license_pools[0].loan_to(patron, start=now, end=tomorrow)
        unused = annotator_fixture.db.work(
            language="eng", with_open_access_download=True
        )

        # Get the feed.
        feed_obj = OPDSAcquisitionFeed.active_loans_for(
            None,
            patron,
            LibraryLoanAndHoldAnnotator(
                None,
                annotator_fixture.lane,
                annotator_fixture.db.default_library(),
                patron=patron,
            ),
        ).as_response()
        raw = str(feed_obj)
        feed = feedparser.parse(raw)

        # The only entries in the feed is the work currently out on loan
        # to this patron.
        assert 2 == len(feed["entries"])
        e1, e2 = sorted(feed["entries"], key=lambda x: x["title"])
        assert work1.title == e1["title"]
        assert work2.title == e2["title"]

        # Make sure that the start and end dates from the loan are present
        # in an <opds:availability> child of the acquisition link.
        tree = etree.fromstring(raw)
        parser = OPDSXMLParser()
        acquisitions = parser._xpath(
            tree, "//atom:entry/atom:link[@rel='http://opds-spec.org/acquisition']"
        )
        assert 2 == len(acquisitions)

        availabilities = []
        for x in acquisitions:
            availability = parser._xpath1(x, "opds:availability")
            assert availability is not None
            availabilities.append(availability)

        # One of these availability tags has 'since' but not 'until'.
        # The other one has both.
        [no_until] = [x for x in availabilities if "until" not in x.attrib]
        assert now == dateutil.parser.parse(no_until.attrib["since"])

        [has_until] = [x for x in availabilities if "until" in x.attrib]
        assert now == dateutil.parser.parse(has_until.attrib["since"])
        assert tomorrow == dateutil.parser.parse(has_until.attrib["until"])

    def test_loan_feed_includes_patron(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        patron = annotator_fixture.db.patron()

        patron.username = "bellhooks"
        patron.authorization_identifier = "987654321"
        feed_obj = OPDSAcquisitionFeed.active_loans_for(
            None,
            patron,
            LibraryLoanAndHoldAnnotator(
                None, None, annotator_fixture.db.default_library(), patron
            ),
        ).as_response()
        raw = str(feed_obj)
        feed_details = feedparser.parse(raw)["feed"]

        assert "simplified:authorizationIdentifier" in raw
        assert "simplified:username" in raw
        assert (
            patron.username == feed_details["simplified_patron"]["simplified:username"]
        )
        assert (
            "987654321"
            == feed_details["simplified_patron"]["simplified:authorizationidentifier"]
        )

    def test_loans_feed_includes_annotations_link(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        patron = annotator_fixture.db.patron()
        feed_obj = OPDSAcquisitionFeed.active_loans_for(None, patron).as_response()
        raw = str(feed_obj)
        feed = feedparser.parse(raw)["feed"]
        links = feed["links"]

        [annotations_link] = [
            x
            for x in links
            if x["rel"].lower() == "http://www.w3.org/ns/oa#annotationService".lower()
        ]
        assert "/annotations" in annotations_link["href"]

    def test_acquisition_feed_includes_license_information(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        work = annotator_fixture.db.work(with_open_access_download=True)
        pool = work.license_pools[0]

        # These numbers are impossible, but it doesn't matter for
        # purposes of this test.
        pool.type = LicensePoolType.METERED
        pool.open_access = False
        pool.licenses_owned = 100
        pool.licenses_available = 50
        pool.patrons_in_hold_queue = 25

        work_entry = WorkEntry(
            work=work,
            license_pool=pool,
            edition=work.presentation_edition,
            identifier=work.presentation_edition.primary_identifier,
        )
        annotator_fixture.annotator.annotate_work_entry(work_entry)
        assert work_entry.computed is not None
        [link] = work_entry.computed.acquisition_links
        assert link.holds_total == "25"

        assert link.copies_available == "50"
        assert link.copies_total == "100"

    def test_loans_feed_includes_fulfill_links(
        self,
        annotator_fixture: LibraryAnnotatorFixture,
        library_fixture: LibraryFixture,
    ):
        patron = annotator_fixture.db.patron()

        work = annotator_fixture.db.work(
            with_license_pool=True, with_open_access_download=False
        )
        pool = work.license_pools[0]
        pool.open_access = False
        mech1 = pool.delivery_mechanisms[0]
        mech2 = pool.set_delivery_mechanism(
            Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
            RightsStatus.IN_COPYRIGHT,
            None,
        )
        streaming_mech = pool.set_delivery_mechanism(
            DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            DeliveryMechanism.OVERDRIVE_DRM,
            RightsStatus.IN_COPYRIGHT,
            None,
        )

        now = utc_now()
        loan, ignore = pool.loan_to(patron, start=now)

        feed_obj = OPDSAcquisitionFeed.active_loans_for(
            None,
            patron,
        ).as_response()
        raw = str(feed_obj)

        entries = feedparser.parse(raw)["entries"]
        assert 1 == len(entries)

        links = entries[0]["links"]

        # Before we fulfill the loan, there are fulfill links for all three mechanisms.
        fulfill_links = [
            link for link in links if link["rel"] == "http://opds-spec.org/acquisition"
        ]
        assert 3 == len(fulfill_links)

        assert {
            mech1.delivery_mechanism.drm_scheme_media_type,
            mech2.delivery_mechanism.drm_scheme_media_type,
            OPDSFeed.ENTRY_TYPE,
        } == {link["type"] for link in fulfill_links}

        # If one of the content types is hidden, the corresponding
        # delivery mechanism does not have a link.
        library = annotator_fixture.db.default_library()
        settings = library_fixture.settings(library)
        settings.hidden_content_types = [mech1.delivery_mechanism.content_type]
        OPDSAcquisitionFeed.active_loans_for(None, patron).as_response()
        assert {
            mech2.delivery_mechanism.drm_scheme_media_type,
            OPDSFeed.ENTRY_TYPE,
        } == {link["type"] for link in fulfill_links}
        settings.hidden_content_types = []

        # When the loan is fulfilled, there are only fulfill links for that mechanism
        # and the streaming mechanism.
        loan.fulfillment = mech1

        feed_obj = OPDSAcquisitionFeed.active_loans_for(None, patron).as_response()
        raw = str(feed_obj)

        entries = feedparser.parse(raw)["entries"]
        assert 1 == len(entries)

        links = entries[0]["links"]

        fulfill_links = [
            link for link in links if link["rel"] == "http://opds-spec.org/acquisition"
        ]
        assert 2 == len(fulfill_links)

        assert {
            mech1.delivery_mechanism.drm_scheme_media_type,
            OPDSFeed.ENTRY_TYPE,
        } == {link["type"] for link in fulfill_links}

    def test_incomplete_catalog_entry_contains_an_alternate_link_to_the_complete_entry(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        circulation = create_autospec(spec=CirculationApiDispatcher)
        circulation.library = annotator_fixture.db.default_library()
        work = annotator_fixture.db.work(
            with_license_pool=True, with_open_access_download=False
        )
        pool = work.license_pools[0]

        annotator = LibraryLoanAndHoldAnnotator(
            circulation, annotator_fixture.lane, circulation.library
        )

        feed_obj = OPDSAcquisitionFeed.single_entry_loans_feed(
            circulation, pool, annotator
        )
        raw = str(feed_obj)
        entries = feedparser.parse(raw)["entries"]
        assert 1 == len(entries)

        links = entries[0]["links"]

        # We want to make sure that an incomplete catalog entry contains an alternate link to the complete entry.
        alternate_links = [
            link
            for link in links
            if link["type"] == OPDSFeed.ENTRY_TYPE and link["rel"] == "alternate"
        ]
        assert 1 == len(alternate_links)

    def test_complete_catalog_entry_with_fulfillment_link_contains_self_link(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        patron = annotator_fixture.db.patron()
        circulation = create_autospec(spec=CirculationApiDispatcher)
        circulation.library = annotator_fixture.db.default_library()
        work = annotator_fixture.db.work(
            with_license_pool=True, with_open_access_download=False
        )
        pool = work.license_pools[0]
        loan, _ = pool.loan_to(patron)

        annotator = LibraryLoanAndHoldAnnotator(circulation, None, circulation.library)
        feed_obj = OPDSAcquisitionFeed.single_entry_loans_feed(
            circulation, loan, annotator
        )
        raw = str(feed_obj)

        entries = feedparser.parse(raw)["entries"]
        assert 1 == len(entries)

        links = entries[0]["links"]

        # We want to make sure that a complete catalog entry contains an alternate link
        # because it's required by some clients (for example, an Android version of SimplyE).
        alternate_links = [
            link
            for link in links
            if link["type"] == OPDSFeed.ENTRY_TYPE and link["rel"] == "alternate"
        ]
        assert 1 == len(alternate_links)

        # We want to make sure that the complete catalog entry contains a self link.
        self_links = [
            link
            for link in links
            if link["type"] == OPDSFeed.ENTRY_TYPE and link["rel"] == "self"
        ]
        assert 1 == len(self_links)

        # We want to make sure that alternate and self links are the same.
        assert alternate_links[0]["href"] == self_links[0]["href"]

    def test_complete_catalog_entry_with_fulfillment_info_contains_self_link(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        patron = annotator_fixture.db.patron()
        circulation = create_autospec(spec=CirculationApiDispatcher)
        circulation.library = annotator_fixture.db.default_library()
        work = annotator_fixture.db.work(
            with_license_pool=True, with_open_access_download=False
        )
        pool = work.license_pools[0]
        loan, _ = pool.loan_to(patron)

        annotator = LibraryLoanAndHoldAnnotator(circulation, None, circulation.library)
        feed_obj = OPDSAcquisitionFeed.single_entry_loans_feed(
            circulation,
            loan,
            annotator,
        )
        raw = str(feed_obj)

        entries = feedparser.parse(raw)["entries"]
        assert 1 == len(entries)

        links = entries[0]["links"]

        # We want to make sure that a complete catalog entry contains an alternate link
        # because it's required by some clients (for example, an Android version of SimplyE).
        alternate_links = [
            link
            for link in links
            if link["type"] == OPDSFeed.ENTRY_TYPE and link["rel"] == "alternate"
        ]
        assert 1 == len(alternate_links)

        # We want to make sure that the complete catalog entry contains a self link.
        self_links = [
            link
            for link in links
            if link["type"] == OPDSFeed.ENTRY_TYPE and link["rel"] == "self"
        ]
        assert 1 == len(self_links)

        # We want to make sure that alternate and self links are the same.
        assert alternate_links[0]["href"] == self_links[0]["href"]

    def test_fulfill_feed(self, annotator_fixture: LibraryAnnotatorFixture):
        patron = annotator_fixture.db.patron()

        work = annotator_fixture.db.work(
            with_license_pool=True, with_open_access_download=False
        )
        pool = work.license_pools[0]
        pool.open_access = False
        streaming_mech = pool.set_delivery_mechanism(
            DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            DeliveryMechanism.OVERDRIVE_DRM,
            RightsStatus.IN_COPYRIGHT,
            None,
        )

        now = utc_now()
        loan, ignore = pool.loan_to(patron, start=now)
        fulfillment = RedirectFulfillment(
            "http://streaming_link",
            Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE,
        )

        annotator = LibraryLoanAndHoldAnnotator(None, None, patron.library)
        feed_obj = OPDSAcquisitionFeed.single_entry_loans_feed(
            None, loan, annotator, fulfillment=fulfillment
        )

        entries = feedparser.parse(str(feed_obj))["entries"]
        assert 1 == len(entries)

        links = entries[0]["links"]

        # The feed for a single fulfillment only includes one fulfill link.
        fulfill_links = [
            link for link in links if link["rel"] == "http://opds-spec.org/acquisition"
        ]
        assert 1 == len(fulfill_links)

        assert (
            Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE
            == fulfill_links[0]["type"]
        )
        assert "http://streaming_link" == fulfill_links[0]["href"]

    def test_drm_device_registration_feed_tags(
        self,
        annotator_fixture: LibraryAnnotatorFixture,
        vendor_id_fixture: VendorIDFixture,
    ):
        """Check that drm_device_registration_feed_tags returns
        a generic drm:licensor tag, except with the drm:scheme attribute
        set.
        """
        vendor_id_fixture.initialize_adobe(annotator_fixture.db.default_library())
        annotator = LibraryLoanAndHoldAnnotator(
            None,
            None,
            annotator_fixture.db.default_library(),
        )
        patron = annotator_fixture.db.patron()
        feed_tag = annotator.drm_device_registration_feed_tags(patron)
        generic_tag = annotator.adobe_id_tags(patron)

        # The feed-level tag has the drm:scheme attribute set.
        assert feed_tag is not None
        assert "http://librarysimplified.org/terms/drm/scheme/ACS" == feed_tag.scheme

        # If we remove that attribute, the feed-level tag is the same as the
        # generic tag.
        assert generic_tag is not None
        assert feed_tag != generic_tag
        feed_tag.scheme = None
        assert feed_tag == generic_tag

    def test_borrow_link_raises_unfulfillable_work(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        edition, pool = annotator_fixture.db.edition(with_license_pool=True)
        kindle_mechanism = pool.set_delivery_mechanism(
            DeliveryMechanism.KINDLE_CONTENT_TYPE,
            DeliveryMechanism.KINDLE_DRM,
            RightsStatus.IN_COPYRIGHT,
            None,
        )
        epub_mechanism = pool.set_delivery_mechanism(
            Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
            RightsStatus.IN_COPYRIGHT,
            None,
        )
        data_source_name = pool.data_source.name
        identifier = pool.identifier

        annotator = LibraryLoanAndHoldAnnotator(
            None, None, annotator_fixture.db.default_library()
        )

        # If there's no way to fulfill the book, borrow_link raises
        # UnfulfillableWork.
        pytest.raises(UnfulfillableWork, annotator.borrow_link, pool, None, [])

        pytest.raises(
            UnfulfillableWork, annotator.borrow_link, pool, None, [kindle_mechanism]
        )

        # If there's a fulfillable mechanism, everything's fine.
        link = annotator.borrow_link(pool, None, [epub_mechanism])
        assert link != None

        link = annotator.borrow_link(pool, None, [epub_mechanism, kindle_mechanism])
        assert link != None

    def test_feed_includes_lane_links(self, annotator_fixture: LibraryAnnotatorFixture):
        def annotated_links(lane, annotator):
            # Create an AcquisitionFeed is using the given Annotator.
            # extract its links and return a dictionary that maps link
            # relations to URLs.
            feed = OPDSAcquisitionFeed("test", "url", [], annotator)
            annotator.annotate_feed(feed._feed)
            links = feed._feed.links

            d = defaultdict(list)
            for link in links:
                d[link.rel.lower()].append(link.href)
            return d

        # When an EntryPoint is explicitly selected, it shows up in the
        # link to the search controller.
        facets = FacetsWithEntryPoint(entrypoint=AudiobooksEntryPoint)
        lane = annotator_fixture.db.lane()
        annotator = LibraryAnnotator(
            None,
            lane,
            annotator_fixture.db.default_library(),
            facets=facets,
        )
        [url] = annotated_links(lane, annotator)["search"]
        assert "/lane_search" in url
        assert "entrypoint=%s" % AudiobooksEntryPoint.INTERNAL_NAME in url
        assert str(lane.id) in url

        # When the selected EntryPoint is a default, it's not used --
        # instead, we search everything.
        assert annotator.facets is not None
        annotator.facets.entrypoint_is_default = True
        links = annotated_links(lane, annotator)
        [url] = links["search"]
        assert "entrypoint=%s" % EverythingEntryPoint.INTERNAL_NAME in url

        # This lane isn't based on a custom list, so there's no crawlable link.
        assert [] == links["http://opds-spec.org/crawlable"]

        # It's also not crawlable if it's based on multiple lists.
        list1, ignore = annotator_fixture.db.customlist()
        list2, ignore = annotator_fixture.db.customlist()
        lane.customlists = [list1, list2]
        links = annotated_links(lane, annotator)
        assert [] == links["http://opds-spec.org/crawlable"]

        # A lane based on a single list gets a crawlable link.
        lane.customlists = [list1]
        links = annotated_links(lane, annotator)
        [crawlable] = links["http://opds-spec.org/crawlable"]
        assert "/crawlable_list_feed" in crawlable
        assert str(list1.name) in crawlable

    def test_acquisition_links(
        self,
        annotator_fixture: LibraryAnnotatorFixture,
        library_fixture: LibraryFixture,
    ):
        annotator = LibraryLoanAndHoldAnnotator(
            None, None, annotator_fixture.db.default_library()
        )

        patron = annotator_fixture.db.patron()

        now = utc_now()
        tomorrow = now + datetime.timedelta(days=1)

        # Loan of an open-access book.
        work1 = annotator_fixture.db.work(with_open_access_download=True)
        loan1, ignore = work1.license_pools[0].loan_to(patron, start=now)

        # Loan of a licensed book.
        work2 = annotator_fixture.db.work(with_license_pool=True)
        loan2, ignore = work2.license_pools[0].loan_to(patron, start=now, end=tomorrow)

        # Hold on a licensed book.
        work3 = annotator_fixture.db.work(with_license_pool=True)
        hold, ignore = work3.license_pools[0].on_hold_to(
            patron, start=now, end=tomorrow
        )

        # Book with no loans or holds yet.
        work4 = annotator_fixture.db.work(with_license_pool=True)

        # Loan of a licensed book without a loan end.
        work5 = annotator_fixture.db.work(with_license_pool=True)
        loan5, ignore = work5.license_pools[0].loan_to(patron, start=now)

        # Ensure the state variable
        assert annotator.identifies_patrons == True

        loan1_links = annotator.acquisition_links(
            loan1.license_pool,
            loan1,
            None,
            None,
            loan1.license_pool.identifier,
        )
        # Fulfill, and revoke.
        [revoke, fulfill] = sorted(loan1_links, key=lambda x: x.rel or "")
        assert revoke.href and "revoke_loan_or_hold" in revoke.href
        assert (
            revoke.rel and "http://librarysimplified.org/terms/rel/revoke" == revoke.rel
        )
        assert fulfill.href and "fulfill" in fulfill.href
        assert fulfill.rel and "http://opds-spec.org/acquisition" == fulfill.rel

        # Allow direct open-access downloads
        # This will also filter out loan revoke links
        annotator.identifies_patrons = False
        loan1_links = annotator.acquisition_links(
            loan1.license_pool, loan1, None, None, loan1.license_pool.identifier
        )
        assert len(loan1_links) == 1
        assert {"http://opds-spec.org/acquisition/open-access"} == {
            link.rel for link in loan1_links
        }

        # Work 2 has no open access links
        loan2_links = annotator.acquisition_links(
            loan2.license_pool, loan2, None, None, loan2.license_pool.identifier
        )
        assert len(loan2_links) == 0

        # Revert the annotator state
        annotator.identifies_patrons = True

        assert strftime(loan1.start) == fulfill.availability_since
        assert loan1.end == fulfill.availability_until == None

        loan2_links = annotator.acquisition_links(
            loan2.license_pool, loan2, None, None, loan2.license_pool.identifier
        )
        # Fulfill and revoke.
        [revoke, fulfill] = sorted(loan2_links, key=lambda x: x.rel or "")
        assert revoke.href and "revoke_loan_or_hold" in revoke.href
        assert "http://librarysimplified.org/terms/rel/revoke" == revoke.rel
        assert fulfill.href and "fulfill" in fulfill.href
        assert "http://opds-spec.org/acquisition" == fulfill.rel

        assert strftime(loan2.start) == fulfill.availability_since
        assert strftime(loan2.end) == fulfill.availability_until

        # If a book is ready to be fulfilled, but the library has
        # hidden all of its available content types, the fulfill link does
        # not show up -- only the revoke link.
        library = annotator_fixture.db.default_library()
        settings = library_fixture.settings(library)
        available_types = [
            lpdm.delivery_mechanism.content_type
            for lpdm in loan2.license_pool.delivery_mechanisms
        ]
        settings.hidden_content_types = available_types

        # The list of hidden content types is stored in the Annotator
        # constructor, so this particular test needs a fresh Annotator.
        annotator_with_hidden_types = LibraryLoanAndHoldAnnotator(
            None, None, annotator_fixture.db.default_library()
        )
        loan2_links = annotator_with_hidden_types.acquisition_links(
            loan2.license_pool, loan2, None, None, loan2.license_pool.identifier
        )
        [revoke] = loan2_links
        assert "http://librarysimplified.org/terms/rel/revoke" == revoke.rel
        # Un-hide the content types so the test can continue.
        settings.hidden_content_types = []

        hold_links = annotator.acquisition_links(
            hold.license_pool, None, hold, None, hold.license_pool.identifier
        )
        # Borrow and revoke.
        [revoke, borrow] = sorted(hold_links, key=lambda x: x.rel or "")
        assert revoke.href and "revoke_loan_or_hold" in revoke.href
        assert "http://librarysimplified.org/terms/rel/revoke" == revoke.rel
        assert borrow.href and "borrow" in borrow.href
        assert "http://opds-spec.org/acquisition/borrow" == borrow.rel

        work4_links = annotator.acquisition_links(
            work4.license_pools[0],
            None,
            None,
            None,
            work4.license_pools[0].identifier,
        )
        # Borrow only.
        [borrow] = work4_links
        assert borrow.href and "borrow" in borrow.href
        assert "http://opds-spec.org/acquisition/borrow" == borrow.rel

        loan5_links = annotator.acquisition_links(
            loan5.license_pool, loan5, None, None, loan5.license_pool.identifier
        )
        # Fulfill and revoke.
        [revoke, fulfill] = sorted(loan5_links, key=lambda x: x.rel or "")
        assert revoke.href and "revoke_loan_or_hold" in revoke.href
        assert "http://librarysimplified.org/terms/rel/revoke" == revoke.rel
        assert fulfill.href and "fulfill" in fulfill.href
        assert "http://opds-spec.org/acquisition" == fulfill.rel

        assert strftime(loan5.start) == fulfill.availability_since
        # TODO: This currently fails, it should be uncommented when the CM 21 day loan bug is fixed
        # assert loan5.end == availability.until
        assert None == loan5.end

        # If patron authentication is turned off for the library, then
        # only open-access links are displayed.
        annotator.identifies_patrons = False

        [open_access] = annotator.acquisition_links(
            loan1.license_pool, loan1, None, None, loan1.license_pool.identifier
        )
        assert "http://opds-spec.org/acquisition/open-access" == open_access.rel

        # This may include links with the open-access relation for
        # non-open-access works that are available without
        # authentication.  To get such link, you pass in a list of
        # LicensePoolDeliveryMechanisms as
        # `direct_fulfillment_delivery_mechanisms`.
        [lp4] = work4.license_pools
        [lpdm4] = lp4.delivery_mechanisms
        lpdm4.set_rights_status(RightsStatus.IN_COPYRIGHT)
        [not_open_access] = annotator.acquisition_links(
            lp4,
            None,
            None,
            None,
            lp4.identifier,
            direct_fulfillment_delivery_mechanisms=[lpdm4],
        )

        # The link relation is OPDS 'open-access', which just means the
        # book can be downloaded with no hassle.
        assert "http://opds-spec.org/acquisition/open-access" == not_open_access.rel

        # The dcterms:rights attribute provides a more detailed
        # explanation of the book's copyright status -- note that it's
        # not "open access" in the typical sense.
        rights = not_open_access.rights
        assert RightsStatus.IN_COPYRIGHT == rights

        # Hold links are absent even when there are active holds in the
        # database -- there is no way to distinguish one patron from
        # another so the concept of a 'hold' is meaningless.
        hold_links = annotator.acquisition_links(
            hold.license_pool, None, hold, None, hold.license_pool.identifier
        )
        assert [] == hold_links

    def test_acquisition_links_multiple_links(
        self,
        annotator_fixture: LibraryAnnotatorFixture,
        library_fixture: LibraryFixture,
        patch_url_for: PatchedUrlFor,
    ):
        library = annotator_fixture.db.default_library()
        annotator = LibraryLoanAndHoldAnnotator(None, None, library)

        # This book has two delivery mechanisms
        work = annotator_fixture.db.work(with_license_pool=True)
        [pool] = work.license_pools
        [mech1] = pool.delivery_mechanisms
        mech2 = pool.set_delivery_mechanism(
            Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM,
            RightsStatus.IN_COPYRIGHT,
            None,
        )

        # The vendor API for LicensePools of this type requires that a
        # delivery mechanism be chosen at the point of borrowing.
        class MockAPI:
            SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.BORROW_STEP

        identifier = pool.identifier

        # This means that two different acquisition links will be
        # generated -- one for each delivery mechanism.
        links = annotator.acquisition_links(
            pool, None, None, None, identifier, mock_api=MockAPI()
        )
        assert 2 == len(links)

        create_mechanism_url = partial(
            patch_url_for.patched_url_for,
            "borrow",
            _external=True,
            identifier=identifier.identifier,
            identifier_type=identifier.type,
            library_short_name=library.short_name,
        )

        link1_expected_href = create_mechanism_url(
            mechanism_id=mech1.delivery_mechanism.id
        )
        link2_expected_href = create_mechanism_url(
            mechanism_id=mech2.delivery_mechanism.id
        )

        assert {link.href for link in links} == {
            link1_expected_href,
            link2_expected_href,
        }

        [mech1_link] = [l for l in links if l.href == link1_expected_href]
        [mech2_link] = [l for l in links if l.href == link2_expected_href]

        assert mech1_link.href != mech2_link.href

        indirects = []
        for link in [mech1_link, mech2_link]:
            # Both links should have the same subtags.
            assert link.availability_status is not None
            assert link.copies_total is not None
            assert link.holds_total is not None
            assert len(link.indirect_acquisitions) > 0
            indirects.append(link.indirect_acquisitions[0])

        # So is the media type seen in the indirectAcquisition subtag.
        [mech1_indirect, mech2_indirect] = indirects

        # The first delivery mechanism (created when the Work was created)
        # uses Adobe DRM, so that shows up as the first indirect acquisition
        # type.
        assert mech1.delivery_mechanism.drm_scheme == mech1_indirect.type

        # The second delivery mechanism doesn't use DRM, so the content
        # type shows up as the first (and only) indirect acquisition type.
        assert mech2.delivery_mechanism.content_type == mech2_indirect.type

        # If we configure the library to hide one of the content types,
        # we end up with only one link -- the one for the delivery
        # mechanism that's not hidden.
        settings = library_fixture.settings(library)
        settings.hidden_content_types = [mech1.delivery_mechanism.content_type]
        annotator = LibraryLoanAndHoldAnnotator(
            None, None, annotator_fixture.db.default_library()
        )
        [link] = annotator.acquisition_links(
            pool, None, None, None, pool.identifier, mock_api=MockAPI()
        )
        assert (
            mech2.delivery_mechanism.content_type == link.indirect_acquisitions[0].type
        )

    def test_library_conflict_when_resolving_active_license_pool(
        self,
        annotator_fixture: LibraryAnnotatorFixture,
        library_fixture: LibraryFixture,
    ):
        # we would never want to resolve an active license pool with a library other
        # than the library that was passed to construct the LibraryAnnotator.
        annotator = LibraryAnnotator(None, None, annotator_fixture.db.default_library())

        work = annotator_fixture.db.work(with_license_pool=True)

        other_library = library_fixture.library("otherlibrary")

        with pytest.raises(BasePalaceException) as execinfo:
            annotator.active_licensepool_for(work, other_library)

        assert (
            "An active license pool cannot be resolved for a library not associated with this annotator"
            in str(execinfo.value)
        )
