import datetime
import json
import re
from collections import defaultdict
from typing import Any, Dict, List
from unittest.mock import MagicMock, create_autospec

import dateutil
import feedparser
import pytest
from freezegun import freeze_time
from lxml import etree

from api.adobe_vendor_id import AuthdataUtility
from api.circulation import BaseCirculationAPI, CirculationAPI, FulfillmentInfo
from api.config import Configuration
from api.lanes import ContributorLane
from api.novelist import NoveListAPI
from api.opds import (
    CirculationManagerAnnotator,
    LibraryAnnotator,
    LibraryLoanAndHoldAnnotator,
    SharedCollectionAnnotator,
    SharedCollectionLoanAndHoldAnnotator,
)
from api.problem_details import NOT_FOUND_ON_REMOTE
from core.analytics import Analytics
from core.classifier import Classifier, Fantasy, Urban_Fantasy
from core.entrypoint import AudiobooksEntryPoint, EverythingEntryPoint
from core.external_search import MockExternalSearchIndex, WorkSearchResult
from core.lane import FacetsWithEntryPoint, WorkList
from core.lcp.credential import LCPCredentialFactory, LCPHashedPassphrase
from core.model import (
    CirculationEvent,
    ConfigurationSetting,
    Contributor,
    DataSource,
    DeliveryMechanism,
    ExternalIntegration,
    Hyperlink,
    MediaTypes,
    PresentationCalculationPolicy,
    Representation,
    RightsStatus,
    Work,
    get_one,
)
from core.model.formats import FormatPriorities
from core.model.licensing import LicensePool
from core.model.patron import Loan
from core.opds import AcquisitionFeed, MockAnnotator, UnfulfillableWork
from core.opds_import import OPDSXMLParser
from core.util.datetime_helpers import datetime_utc, utc_now
from core.util.flask_util import OPDSEntryResponse, OPDSFeedResponse
from core.util.opds_writer import AtomFeed, OPDSFeed
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.vendor_id import VendorIDFixture

_strftime = AtomFeed._strftime


class CirculationManagerAnnotatorFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.work = db.work(with_open_access_download=True)
        self.lane = db.lane(display_name="Fantasy")
        self.annotator = CirculationManagerAnnotator(
            self.lane,
            test_mode=True,
        )


@pytest.fixture(scope="function")
def circulation_fixture(
    db: DatabaseTransactionFixture,
) -> CirculationManagerAnnotatorFixture:
    return CirculationManagerAnnotatorFixture(db)


class TestCirculationManagerAnnotator:
    def test_open_access_link(
        self, circulation_fixture: CirculationManagerAnnotatorFixture
    ):
        # The resource URL associated with a LicensePoolDeliveryMechanism
        # becomes the `href` of an open-access `link` tag.
        pool = circulation_fixture.work.license_pools[0]
        [lpdm] = pool.delivery_mechanisms

        # Temporarily disconnect the Resource's Representation so we
        # can verify that this works even if there is no
        # Representation.
        representation = lpdm.resource.representation
        lpdm.resource.representation = None
        lpdm.resource.url = "http://foo.com/thefile.epub"
        link_tag = circulation_fixture.annotator.open_access_link(pool, lpdm)
        assert lpdm.resource.url == link_tag.get("href")

        # The dcterms:rights attribute may provide a more detailed
        # explanation of the book's copyright status.
        rights = link_tag.attrib["{http://purl.org/dc/terms/}rights"]
        assert lpdm.rights_status.uri == rights

        # If the Resource has a Representation, the public URL is used
        # instead of the original Resource URL.
        lpdm.resource.representation = representation
        link_tag = circulation_fixture.annotator.open_access_link(pool, lpdm)
        assert representation.public_url == link_tag.get("href")

        # If there is no Representation, the Resource's original URL is used.
        lpdm.resource.representation = None
        link_tag = circulation_fixture.annotator.open_access_link(pool, lpdm)
        assert lpdm.resource.url == link_tag.get("href")

    def test_default_lane_url(
        self, circulation_fixture: CirculationManagerAnnotatorFixture
    ):
        default_lane_url = circulation_fixture.annotator.default_lane_url()
        assert "feed" in default_lane_url
        assert str(circulation_fixture.lane.id) not in default_lane_url

    def test_feed_url(self, circulation_fixture: CirculationManagerAnnotatorFixture):
        feed_url_fantasy = circulation_fixture.annotator.feed_url(
            circulation_fixture.lane, dict(), dict()
        )
        assert "feed" in feed_url_fantasy
        assert str(circulation_fixture.lane.id) in feed_url_fantasy
        assert circulation_fixture.db.default_library().name not in feed_url_fantasy

    def test_navigation_url(
        self, circulation_fixture: CirculationManagerAnnotatorFixture
    ):
        navigation_url_fantasy = circulation_fixture.annotator.navigation_url(
            circulation_fixture.lane
        )
        assert "navigation" in navigation_url_fantasy
        assert str(circulation_fixture.lane.id) in navigation_url_fantasy

    def test_visible_delivery_mechanisms(
        self, circulation_fixture: CirculationManagerAnnotatorFixture
    ):

        # By default, all delivery mechanisms are visible
        [pool] = circulation_fixture.work.license_pools
        [epub] = list(circulation_fixture.annotator.visible_delivery_mechanisms(pool))
        assert "application/epub+zip" == epub.delivery_mechanism.content_type

        # Create an annotator that hides PDFs.
        no_pdf = CirculationManagerAnnotator(
            circulation_fixture.lane,
            hidden_content_types=["application/pdf"],
            test_mode=True,
        )

        # This has no effect on the EPUB.
        [epub2] = list(no_pdf.visible_delivery_mechanisms(pool))
        assert epub == epub2

        # Create an annotator that hides EPUBs.
        no_epub = CirculationManagerAnnotator(
            circulation_fixture.lane,
            hidden_content_types=["application/epub+zip"],
            test_mode=True,
        )

        # The EPUB is hidden, and this license pool has no delivery
        # mechanisms.
        assert [] == list(no_epub.visible_delivery_mechanisms(pool))

    def test_visible_delivery_mechanisms_configured_0(
        self, circulation_fixture: CirculationManagerAnnotatorFixture
    ):
        """Test that configuration options do affect OPDS feeds.
        Exhaustive testing of different configuration values isn't necessary
        here: See the tests for FormatProperties to see the actual semantics
        of the configuration values."""
        edition = circulation_fixture.db.edition()
        pool: LicensePool = circulation_fixture.db.licensepool(edition)

        pool.set_delivery_mechanism(
            MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM,
            RightsStatus.UNKNOWN,
            None,
        )
        pool.set_delivery_mechanism(
            MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.LCP_DRM,
            RightsStatus.UNKNOWN,
            None,
        )
        pool.set_delivery_mechanism(
            MediaTypes.PDF_MEDIA_TYPE,
            DeliveryMechanism.LCP_DRM,
            RightsStatus.UNKNOWN,
            None,
        )

        external: ExternalIntegration = pool.collection.external_integration
        prioritize_drm_setting = ConfigurationSetting.for_externalintegration(
            FormatPriorities.PRIORITIZED_DRM_SCHEMES_KEY, external
        )
        prioritize_content_type_setting = ConfigurationSetting.for_externalintegration(
            FormatPriorities.PRIORITIZED_CONTENT_TYPES_KEY, external
        )

        prioritize_drm_setting.value = f'["{DeliveryMechanism.LCP_DRM}"]'
        prioritize_content_type_setting.value = f'["{MediaTypes.PDF_MEDIA_TYPE}"]'

        annotator = CirculationManagerAnnotator(
            circulation_fixture.lane,
            hidden_content_types=[],
            test_mode=True,
        )

        # DRM-free types appear first.
        # Then our LCP'd PDF.
        # Then our LCP'd EPUB.
        # Then our Adobe DRM'd EPUB.
        results = annotator.visible_delivery_mechanisms(pool)
        assert results[0].delivery_mechanism.content_type == MediaTypes.EPUB_MEDIA_TYPE
        assert results[0].delivery_mechanism.drm_scheme == None
        assert results[1].delivery_mechanism.content_type == MediaTypes.PDF_MEDIA_TYPE
        assert results[1].delivery_mechanism.drm_scheme == DeliveryMechanism.LCP_DRM
        assert results[2].delivery_mechanism.content_type == MediaTypes.EPUB_MEDIA_TYPE
        assert results[2].delivery_mechanism.drm_scheme == DeliveryMechanism.LCP_DRM
        assert results[3].delivery_mechanism.content_type == MediaTypes.EPUB_MEDIA_TYPE
        assert results[3].delivery_mechanism.drm_scheme == DeliveryMechanism.ADOBE_DRM
        assert len(results) == 4

    def test_rights_attributes(
        self, circulation_fixture: CirculationManagerAnnotatorFixture
    ):
        m = circulation_fixture.annotator.rights_attributes

        # Given a LicensePoolDeliveryMechanism with a RightsStatus,
        # rights_attributes creates a dictionary mapping the dcterms:rights
        # attribute to the URI associated with the RightsStatus.
        lp = circulation_fixture.db.licensepool(None)
        [lpdm] = lp.delivery_mechanisms
        assert {"{http://purl.org/dc/terms/}rights": lpdm.rights_status.uri} == m(lpdm)

        # If any link in the chain is broken, rights_attributes returns
        # an empty dictionary.
        old_uri = lpdm.rights_status.uri
        lpdm.rights_status.uri = None
        assert {} == m(lpdm)
        lpdm.rights_status.uri = old_uri

        lpdm.rights_status = None
        assert {} == m(lpdm)

        assert {} == m(None)

    def test_work_entry_includes_updated(
        self, circulation_fixture: CirculationManagerAnnotatorFixture
    ):

        # By default, the 'updated' date is the value of
        # Work.last_update_time.
        work = circulation_fixture.db.work(with_open_access_download=True)
        # This date is later, but we don't check it.
        work.license_pools[0].availability_time = datetime_utc(2019, 1, 1)
        work.last_update_time = datetime_utc(2018, 2, 4)

        def entry_for(work):
            worklist = WorkList()
            worklist.initialize(None)
            annotator = CirculationManagerAnnotator(worklist, test_mode=True)
            feed = AcquisitionFeed(
                circulation_fixture.db.session, "test", "url", [work], annotator
            )
            feed = feedparser.parse(str(feed))
            [entry] = feed.entries
            return entry

        entry = entry_for(work)
        assert "2018-02-04" in entry.get("updated")

        # If the work passed in is a WorkSearchResult that indicates
        # the search index found a later 'update time', then the later
        # time is used. This value isn't always present -- it's only
        # calculated when the list is being _ordered_ by 'update time'.
        # Otherwise it's too slow to bother.
        class MockHit:
            def __init__(self, last_update):
                # Store the time the way we get it from Opensearch --
                # as a single-element list containing seconds since epoch.
                self.last_update = [
                    (last_update - datetime_utc(1970, 1, 1)).total_seconds()
                ]

        hit = MockHit(datetime_utc(2018, 2, 5))
        result = WorkSearchResult(work, hit)
        entry = entry_for(result)
        assert "2018-02-05" in entry.get("updated")

        # Any 'update time' provided by Opensearch is used even if
        # it's clearly earlier than Work.last_update_time.
        hit = MockHit(datetime_utc(2017, 1, 1))
        result._hit = hit
        entry = entry_for(result)
        assert "2017-01-01" in entry.get("updated")

    def test__single_entry_response(
        self, circulation_fixture: CirculationManagerAnnotatorFixture
    ):
        # Test the helper method that makes OPDSEntryResponse objects.

        m = CirculationManagerAnnotator._single_entry_response

        # Test the case where we accept the defaults.
        work = circulation_fixture.db.work()
        url = circulation_fixture.db.fresh_url()
        annotator = MockAnnotator()
        response = m(circulation_fixture.db.session, work, annotator, url)
        assert isinstance(response, OPDSEntryResponse)
        assert "<title>%s</title>" % work.title in response.get_data(as_text=True)

        # By default, the representation is private but can be cached
        # by the recipient.
        assert True == response.private
        assert 30 * 60 == response.max_age

        # Test the case where we override the defaults.
        response = m(
            circulation_fixture.db.session,
            work,
            annotator,
            url,
            max_age=12,
            private=False,
        )
        assert False == response.private
        assert 12 == response.max_age

        # Test the case where the Work we thought we were providing is missing.
        work = None
        response = m(circulation_fixture.db.session, work, annotator, url)

        # Instead of an entry based on the Work, we get an empty feed.
        assert isinstance(response, OPDSFeedResponse)
        response_data = response.get_data(as_text=True)
        assert "<title>Unknown work</title>" in response_data
        assert "<entry>" not in response_data

        # Since it's an error message, the representation is private
        # and not to be cached.
        assert 0 == response.max_age
        assert True == response.private


class LibraryAnnotatorFixture:
    def __init__(self, db: DatabaseTransactionFixture, vendor_id: VendorIDFixture):
        self.db = db
        self.vendor_id = vendor_id
        self.work = db.work(with_open_access_download=True)
        parent = db.lane(display_name="Fiction", languages=["eng"], fiction=True)
        self.lane = db.lane(display_name="Fantasy", languages=["eng"])
        self.lane.add_genre(Fantasy.name)
        self.lane.parent = parent
        self.annotator = LibraryAnnotator(
            None,
            self.lane,
            db.default_library(),
            test_mode=True,
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
    db: DatabaseTransactionFixture, vendor_id_fixture: VendorIDFixture
) -> LibraryAnnotatorFixture:
    return LibraryAnnotatorFixture(db, vendor_id_fixture)


class TestLibraryAnnotator:
    def test__hidden_content_types(self, annotator_fixture: LibraryAnnotatorFixture):
        def f(value):
            """Set the default library's HIDDEN_CONTENT_TYPES setting
            to a specific value and see what _hidden_content_types
            says.
            """
            library = annotator_fixture.db.default_library()
            library.setting(Configuration.HIDDEN_CONTENT_TYPES).value = value
            return LibraryAnnotator._hidden_content_types(library)

        # When the value is not set at all, no content types are hidden.
        assert [] == list(
            LibraryAnnotator._hidden_content_types(
                annotator_fixture.db.default_library()
            )
        )

        # Now set various values and see what happens.
        assert [] == f(None)
        assert [] == f("")
        assert [] == f(json.dumps([]))
        assert ["text/html"] == f("text/html")
        assert ["text/html"] == f(json.dumps("text/html"))
        assert ["text/html"] == f(json.dumps({"text/html": "some value"}))
        assert ["text/html", "text/plain"] == f(json.dumps(["text/html", "text/plain"]))

    def test_add_configuration_links(self, annotator_fixture: LibraryAnnotatorFixture):
        mock_feed: List[Any] = []
        link_config = {
            LibraryAnnotator.TERMS_OF_SERVICE: "http://terms/",
            LibraryAnnotator.PRIVACY_POLICY: "http://privacy/",
            LibraryAnnotator.COPYRIGHT: "http://copyright/",
            LibraryAnnotator.ABOUT: "http://about/",
            LibraryAnnotator.LICENSE: "http://license/",
            Configuration.HELP_EMAIL: "help@me",
            Configuration.HELP_WEB: "http://help/",
            Configuration.HELP_URI: "uri:help",
        }

        # Set up configuration settings for links.
        for rel, value in link_config.items():
            ConfigurationSetting.for_library(
                rel, annotator_fixture.db.default_library()
            ).value = value

        # Set up settings for navigation links.
        ConfigurationSetting.for_library(
            Configuration.WEB_HEADER_LINKS, annotator_fixture.db.default_library()
        ).value = json.dumps(["http://example.com/1", "http://example.com/2"])
        ConfigurationSetting.for_library(
            Configuration.WEB_HEADER_LABELS, annotator_fixture.db.default_library()
        ).value = json.dumps(["one", "two"])

        annotator_fixture.annotator.add_configuration_links(mock_feed)

        # Ten links were added to the "feed"
        assert 10 == len(mock_feed)

        # They are the links we'd expect.
        links: Dict[str, Any] = {}
        for link in mock_feed:
            rel = link.attrib["rel"]
            href = link.attrib["href"]
            if rel == "help" or rel == "related":
                continue  # Tested below
            # Check that the configuration value made it into the link.
            assert href == link_config[rel]
            assert "text/html" == link.attrib["type"]

        # There are three help links using different protocols.
        help_links = [x.attrib["href"] for x in mock_feed if x.attrib["rel"] == "help"]
        assert {"mailto:help@me", "http://help/", "uri:help"} == set(help_links)

        # There are two navigation links.
        navigation_links = [x for x in mock_feed if x.attrib["rel"] == "related"]
        assert {"navigation"} == {x.attrib["role"] for x in navigation_links}
        assert {"http://example.com/1", "http://example.com/2"} == {
            x.attrib["href"] for x in navigation_links
        }
        assert {"one", "two"} == {x.attrib["title"] for x in navigation_links}

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

        facets = dict(entrypoint="Book")
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
        assert OPDSFeed.OPEN_ACCESS_REL == link.attrib["rel"]

    # We freeze the test time here, because this test checks that the client token
    # in the feed matches a generated client token. The client token contains an
    # expiry date based on the current time, so this test can be flaky in a slow
    # integration environment unless we make sure the clock does not change as this
    # test is being performed.
    @freeze_time("1867-07-01")
    def test_fulfill_link_includes_device_registration_tags(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        """Verify that when Adobe Vendor ID delegation is included, the
        fulfill link for an Adobe delivery mechanism includes instructions
        on how to get a Vendor ID.
        """
        annotator_fixture.vendor_id.initialize_adobe(
            annotator_fixture.db.default_library()
        )
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
        for child in link:
            assert child.tag != "{http://librarysimplified.org/terms/drm}licensor"

        # No new Credential has been associated with the patron.
        assert old_credentials == patron.credentials

        # The fulfill link for Adobe DRM includes information
        # on how to get an Adobe ID in the drm:licensor tag.
        link = annotator_fixture.annotator.fulfill_link(
            pool, loan, adobe_delivery_mechanism
        )
        licensor = link[-1]
        assert "{http://librarysimplified.org/terms/drm}licensor" == licensor.tag

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
        [expect] = annotator_fixture.annotator.adobe_id_tags(
            adobe_id_identifier.credential
        )
        assert etree.tostring(expect, method="c14n2") == etree.tostring(
            licensor, method="c14n2"
        )

    def test_no_adobe_id_tags_when_vendor_id_not_configured(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        """When vendor ID delegation is not configured, adobe_id_tags()
        returns an empty list.
        """
        assert [] == annotator_fixture.annotator.adobe_id_tags("patron identifier")

    def test_adobe_id_tags_when_vendor_id_configured(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        """When vendor ID delegation is configured, adobe_id_tags()
        returns a list containing a single tag. The tag contains
        the information necessary to get an Adobe ID and a link to the local
        DRM Device Management Protocol endpoint.
        """
        library = annotator_fixture.db.default_library()
        annotator_fixture.vendor_id.initialize_adobe(library)
        patron_identifier = "patron identifier"
        [element] = annotator_fixture.annotator.adobe_id_tags(patron_identifier)
        assert "{http://librarysimplified.org/terms/drm}licensor" == element.tag

        key = "{http://librarysimplified.org/terms/drm}vendor"
        assert (
            annotator_fixture.vendor_id.adobe_vendor_id.username == element.attrib[key]
        )

        [token, device_management_link] = element

        assert "{http://librarysimplified.org/terms/drm}clientToken" == token.tag
        # token.text is a token which we can decode, since we know
        # the secret.
        token = token.text
        authdata = AuthdataUtility.from_config(library)
        decoded = authdata.decode_short_client_token(token)
        expected_url = ConfigurationSetting.for_library(
            Configuration.WEBSITE_URL, library
        ).value
        assert (expected_url, patron_identifier) == decoded

        assert "link" == device_management_link.tag
        assert (
            "http://librarysimplified.org/terms/drm/rel/devices"
            == device_management_link.attrib["rel"]
        )
        expect_url = annotator_fixture.annotator.url_for(
            "adobe_drm_devices", library_short_name=library.short_name, _external=True
        )
        assert expect_url == device_management_link.attrib["href"]

        # If we call adobe_id_tags again we'll get a distinct tag
        # object that renders to the same XML.
        [same_tag] = annotator_fixture.annotator.adobe_id_tags(patron_identifier)
        assert same_tag is not element
        assert etree.tostring(element, method="c14n2") == etree.tostring(
            same_tag, method="c14n2"
        )

        # If the Adobe Vendor ID configuration is present but
        # incomplete, adobe_id_tags does nothing.

        # Delete one setting from the existing integration to check
        # this.
        setting = ConfigurationSetting.for_library_and_externalintegration(
            annotator_fixture.db.session,
            ExternalIntegration.USERNAME,
            library,
            annotator_fixture.vendor_id.registry,
        )
        annotator_fixture.db.session.delete(setting)
        assert [] == annotator_fixture.annotator.adobe_id_tags("new identifier")

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
        for child in link:
            assert child.tag != "{%s}hashed_passphrase" % OPDSFeed.LCP_NS

        # The fulfill link for lcp DRM includes hashed_passphrase
        link = annotator_fixture.annotator.fulfill_link(
            pool, loan, lcp_delivery_mechanism
        )
        hashed_passphrase = link[-1]
        assert hashed_passphrase.tag == "{%s}hashed_passphrase" % OPDSFeed.LCP_NS
        assert hashed_passphrase.text == hashed_password.hashed

    def test_default_lane_url(self, annotator_fixture: LibraryAnnotatorFixture):
        default_lane_url = annotator_fixture.annotator.default_lane_url()
        assert "groups" in default_lane_url
        assert str(annotator_fixture.lane.id) not in default_lane_url

        facets = dict(entrypoint="Book")
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

        facets = dict(arg="value")
        groups_url_facets = annotator_fixture.annotator.groups_url(None, facets=facets)
        assert "arg=value" in groups_url_facets

    def test_feed_url(self, annotator_fixture: LibraryAnnotatorFixture):
        # A regular Lane.
        feed_url_fantasy = annotator_fixture.annotator.feed_url(
            annotator_fixture.lane, dict(facet="value"), dict()
        )
        assert "feed" in feed_url_fantasy
        assert "facet=value" in feed_url_fantasy
        assert str(annotator_fixture.lane.id) in feed_url_fantasy
        assert annotator_fixture.db.default_library().name in feed_url_fantasy

        # A QueryGeneratedLane.
        annotator_fixture.annotator.lane = annotator_fixture.contributor_lane
        feed_url_contributor = annotator_fixture.annotator.feed_url(
            annotator_fixture.contributor_lane, dict(), dict()
        )
        assert annotator_fixture.contributor_lane.ROUTE in feed_url_contributor
        assert (
            annotator_fixture.contributor_lane.contributor_key in feed_url_contributor
        )
        assert annotator_fixture.db.default_library().name in feed_url_contributor

    def test_search_url(self, annotator_fixture: LibraryAnnotatorFixture):
        search_url = annotator_fixture.annotator.search_url(
            annotator_fixture.lane, "query", dict(), dict(facet="value")
        )
        assert "search" in search_url
        assert "query" in search_url
        assert "facet=value" in search_url
        assert str(annotator_fixture.lane.id) in search_url

    def test_facet_url(self, annotator_fixture: LibraryAnnotatorFixture):
        # A regular Lane.
        facets = dict(collection="main")
        facet_url = annotator_fixture.annotator.facet_url(facets)
        assert "collection=main" in facet_url
        assert str(annotator_fixture.lane.id) in facet_url

        # A QueryGeneratedLane.
        annotator_fixture.annotator.lane = annotator_fixture.contributor_lane

        facet_url_contributor = annotator_fixture.annotator.facet_url(facets)
        assert "collection=main" in facet_url_contributor
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
            test_mode=True,
        )
        pool = annotator.active_licensepool_for(work)

        feed = self.get_parsed_feed(annotator_fixture, [work])
        [entry] = feed["entries"]
        assert entry["id"] == pool.identifier.urn

        [(alternate, type)] = [
            (x["href"], x["type"]) for x in entry["links"] if x["rel"] == "alternate"
        ]
        permalink, permalink_type = annotator_fixture.annotator.permalink_for(
            work, pool, pool.identifier
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
                test_mode=True,
                library_identifies_patrons=auth,
            )
            feed = AcquisitionFeed(
                annotator_fixture.db.session, "test", "url", [], annotator
            )
            entry = feed._make_entry_xml(work, edition)
            annotator.annotate_work_entry(work, pool, edition, identifier, feed, entry)
            parsed = feedparser.parse(etree.tostring(entry))
            [entry_parsed] = parsed["entries"]
            linksets.append({x["rel"] for x in entry_parsed["links"]})

        with_auth, no_auth = linksets

        # Some links are present no matter what.
        for expect in ["alternate", "related"]:
            assert expect in with_auth
            assert expect in no_auth

        # A library with patron authentication offers some additional
        # links -- one to borrow the book and one to annotate the
        # book.
        for expect in [
            "http://www.w3.org/ns/oa#annotationservice",
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
            test_mode=True,
            library_identifies_patrons=True,
        )
        feed = AcquisitionFeed(
            annotator_fixture.db.session, "test", "url", [], annotator
        )
        entry = feed._make_entry_xml(work, edition)
        annotator.annotate_work_entry(work, None, edition, identifier, feed, entry)
        parsed = feedparser.parse(etree.tostring(entry))
        [entry_parsed] = parsed["entries"]
        links = {x["rel"] for x in entry_parsed["links"]}

        # These links are still present.
        for expect in [
            "alternate",
            "related",
            "http://www.w3.org/ns/oa#annotationservice",
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
        Analytics.GLOBAL_ENABLED = True
        entry = feed._make_entry_xml(work, edition)
        annotator.annotate_work_entry(work, None, edition, identifier, feed, entry)
        parsed = feedparser.parse(etree.tostring(entry))
        [entry_parsed] = parsed["entries"]
        [analytics_link] = [
            x["href"] for x in entry_parsed["links"] if x["rel"] == open_book_rel
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
        feed = AcquisitionFeed(
            annotator_fixture.db.session, "test", "url", [], annotator
        )
        entry = feed._make_entry_xml(work, edition)
        annotator.annotate_work_entry(work, None, edition, identifier, feed, entry)
        parsed = feedparser.parse(etree.tostring(entry))
        [entry_parsed] = parsed["entries"]
        [feed_link] = [
            l for l in entry_parsed["links"] if l.rel == Hyperlink.CLIENT_SAMPLE
        ]
        assert feed_link["href"] == link.resource.url
        assert feed_link["type"] == link.resource.representation.media_type

    def test_annotate_feed(self, annotator_fixture: LibraryAnnotatorFixture):
        lane = annotator_fixture.db.lane()
        linksets = []
        for auth in (True, False):
            annotator = LibraryAnnotator(
                None,
                lane,
                annotator_fixture.db.default_library(),
                test_mode=True,
                library_identifies_patrons=auth,
            )
            feed = AcquisitionFeed(
                annotator_fixture.db.session, "test", "url", [], annotator
            )
            annotator.annotate_feed(feed, lane)
            parsed = feedparser.parse(str(feed))
            linksets.append([x["rel"] for x in parsed["feed"]["links"]])

        with_auth, without_auth = linksets

        # There's always a self link, a search link, and an auth
        # document link.
        for rel in ("self", "search", "http://opds-spec.org/auth/document"):
            assert rel in with_auth
            assert rel in without_auth

        # But there's only a bookshelf link and an annotation link
        # when patron authentication is enabled.
        for rel in (
            "http://opds-spec.org/shelf",
            "http://www.w3.org/ns/oa#annotationservice",
        ):
            assert rel in with_auth
            assert rel not in without_auth

    def get_parsed_feed(
        self, annotator_fixture: LibraryAnnotatorFixture, works, lane=None, **kwargs
    ):
        if not lane:
            lane = annotator_fixture.db.lane(display_name="Main Lane")
        feed = AcquisitionFeed(
            annotator_fixture.db.session,
            "test",
            "url",
            works,
            LibraryAnnotator(
                None,
                lane,
                annotator_fixture.db.default_library(),
                test_mode=True,
                **kwargs,
            ),
        )
        return feedparser.parse(str(feed))

    def assert_link_on_entry(
        self, entry, link_type=None, rels=None, partials_by_rel=None
    ):
        """Asserts that a link with a certain 'rel' value exists on a
        given feed or entry, as well as its link 'type' value and parts
        of its 'href' value.
        """

        def get_link_by_rel(rel):
            try:
                [link] = [x for x in entry["links"] if x["rel"] == rel]
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
            entry,
            link_type=OPDSFeed.ACQUISITION_FEED_TYPE,
            partials_by_rel=expected_rel_and_partial,
        )

        # When there are two authors, they each get a contributor link.
        work.presentation_edition.add_contributor("Oprah", Contributor.AUTHOR_ROLE)
        work.calculate_presentation(
            PresentationCalculationPolicy(regenerate_opds_entries=True),
            MockExternalSearchIndex(),
        )
        [entry] = self.get_parsed_feed(annotator_fixture, [work]).entries
        contributor_links = [l for l in entry.links if l.rel == "contributor"]
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
            PresentationCalculationPolicy(regenerate_opds_entries=True),
            MockExternalSearchIndex(),
        )
        [entry] = self.get_parsed_feed(annotator_fixture, [work]).entries
        assert [] == [l for l in entry.links if l.rel == "contributor"]

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
            entry,
            link_type=OPDSFeed.ACQUISITION_FEED_TYPE,
            partials_by_rel=expected_rel_and_partial,
        )

        # When there's no series, there's no series link.
        work = annotator_fixture.db.work(with_open_access_download=True)
        feed = self.get_parsed_feed(annotator_fixture, [work])
        [entry] = feed.entries
        assert [] == [l for l in entry.links if l.rel == "series"]

    def test_work_entry_includes_recommendations_link(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        work = annotator_fixture.db.work(with_open_access_download=True)

        # If NoveList Select isn't configured, there's no recommendations link.
        feed = self.get_parsed_feed(annotator_fixture, [work])
        [entry] = feed.entries
        assert [] == [l for l in entry.links if l.rel == "recommendations"]

        # There's a recommendation link when configuration is found, though!
        NoveListAPI.IS_CONFIGURED = None
        annotator_fixture.db.external_integration(
            ExternalIntegration.NOVELIST,
            goal=ExternalIntegration.METADATA_GOAL,
            username="library",
            password="sure",
            libraries=[annotator_fixture.db.default_library()],
        )

        feed = self.get_parsed_feed(annotator_fixture, [work])
        [entry] = feed.entries
        expected_rel_and_partial = dict(recommendations="/recommendations")
        self.assert_link_on_entry(
            entry,
            link_type=OPDSFeed.ACQUISITION_FEED_TYPE,
            partials_by_rel=expected_rel_and_partial,
        )

    def test_work_entry_includes_annotations_link(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        work = annotator_fixture.db.work(with_open_access_download=True)
        identifier_str = work.license_pools[0].identifier.identifier
        uri_parts = ["/annotations", identifier_str]
        annotation_rel = "http://www.w3.org/ns/oa#annotationservice"
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
        assert annotation_rel not in [x["rel"] for x in entry["links"]]

    def test_active_loan_feed(self, annotator_fixture: LibraryAnnotatorFixture):
        annotator_fixture.vendor_id.initialize_adobe(
            annotator_fixture.db.default_library()
        )
        patron = annotator_fixture.db.patron()
        patron.last_loan_activity_sync = utc_now()
        cls = LibraryLoanAndHoldAnnotator

        response = cls.active_loans_for(None, patron, test_mode=True)

        # The feed is private and should not be cached.
        assert isinstance(response, OPDSFeedResponse)
        assert 0 == response.max_age
        assert True == response.private

        # Instead, the Last-Modified header is set to the last time
        # we successfully brought the patron's bookshelf in sync with
        # the vendor APIs.
        #
        # (The timestamps aren't exactly the same because
        # last_loan_activity_sync is tracked at the millisecond level
        # and Last-Modified is tracked at the second level.)

        assert (
            patron.last_loan_activity_sync - response.last_modified
        ).total_seconds() < 1

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
        annotator = cls(
            None, None, library=patron.library, patron=patron, test_mode=True
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

        adobe_patron_identifier = AuthdataUtility._adobe_patron_identifier(patron)

        # The DRM licensing information includes the Adobe vendor ID
        # and the patron's patron identifier for Adobe purposes.
        assert (
            annotator_fixture.vendor_id.adobe_vendor_id.username
            == licensor.attrib["{http://librarysimplified.org/terms/drm}vendor"]
        )
        [client_token, device_management_link] = licensor
        expected = ConfigurationSetting.for_library_and_externalintegration(
            annotator_fixture.db.session,
            ExternalIntegration.USERNAME,
            annotator_fixture.db.default_library(),
            annotator_fixture.vendor_id.registry,
        ).value.upper()
        assert client_token.text.startswith(expected)
        assert adobe_patron_identifier in client_token.text
        assert "{http://www.w3.org/2005/Atom}link" == device_management_link.tag
        assert (
            "http://librarysimplified.org/terms/drm/rel/devices"
            == device_management_link.attrib["rel"]
        )

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
        feed_obj = LibraryLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True
        )
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

        availabilities = [parser._xpath1(x, "opds:availability") for x in acquisitions]

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
        feed_obj = LibraryLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True
        )
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
        feed_obj = LibraryLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True
        )
        raw = str(feed_obj)
        feed = feedparser.parse(raw)["feed"]
        links = feed["links"]

        [annotations_link] = [
            x
            for x in links
            if x["rel"].lower() == "http://www.w3.org/ns/oa#annotationService".lower()
        ]
        assert "/annotations" in annotations_link["href"]

    def test_active_loan_feed_ignores_inconsistent_local_data(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        patron = annotator_fixture.db.patron()

        work1 = annotator_fixture.db.work(language="eng", with_license_pool=True)
        loan, ignore = work1.license_pools[0].loan_to(patron)
        work2 = annotator_fixture.db.work(language="eng", with_license_pool=True)
        hold, ignore = work2.license_pools[0].on_hold_to(patron)

        # Uh-oh, our local loan data is bad.
        loan.license_pool.identifier = None

        # Our local hold data is also bad.
        hold.license_pool = None

        # We can still get a feed...
        feed_obj = LibraryLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True
        )

        # ...but it's empty.
        assert "<entry>" not in str(feed_obj)

    def test_acquisition_feed_includes_license_information(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        work = annotator_fixture.db.work(with_open_access_download=True)
        pool = work.license_pools[0]

        # These numbers are impossible, but it doesn't matter for
        # purposes of this test.
        pool.open_access = False
        pool.licenses_owned = 100
        pool.licenses_available = 50
        pool.patrons_in_hold_queue = 25

        feed = AcquisitionFeed(
            annotator_fixture.db.session,
            "title",
            "url",
            [work],
            annotator_fixture.annotator,
        )
        u = str(feed)
        holds_re = re.compile(r'<opds:holds\W+total="25"\W*/>', re.S)
        assert holds_re.search(u) is not None

        copies_re = re.compile('<opds:copies[^>]+available="50"', re.S)
        assert copies_re.search(u) is not None

        copies_re = re.compile('<opds:copies[^>]+total="100"', re.S)
        assert copies_re.search(u) is not None

    def test_loans_feed_includes_fulfill_links(
        self, annotator_fixture: LibraryAnnotatorFixture
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

        feed_obj = LibraryLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True
        )
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
        setting = annotator_fixture.db.default_library().setting(
            Configuration.HIDDEN_CONTENT_TYPES
        )
        setting.value = json.dumps([mech1.delivery_mechanism.content_type])
        feed_obj = LibraryLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True
        )
        assert {
            mech2.delivery_mechanism.drm_scheme_media_type,
            OPDSFeed.ENTRY_TYPE,
        } == {link["type"] for link in fulfill_links}
        setting.value = None

        # When the loan is fulfilled, there are only fulfill links for that mechanism
        # and the streaming mechanism.
        loan.fulfillment = mech1

        feed_obj = LibraryLoanAndHoldAnnotator.active_loans_for(
            None, patron, test_mode=True
        )
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
        circulation = create_autospec(spec=CirculationAPI)
        circulation.library = annotator_fixture.db.default_library()
        work = annotator_fixture.db.work(
            with_license_pool=True, with_open_access_download=False
        )
        pool = work.license_pools[0]

        feed_obj = LibraryLoanAndHoldAnnotator.single_item_feed(
            circulation, pool, test_mode=True
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
        circulation = create_autospec(spec=CirculationAPI)
        circulation.library = annotator_fixture.db.default_library()
        work = annotator_fixture.db.work(
            with_license_pool=True, with_open_access_download=False
        )
        pool = work.license_pools[0]
        loan, _ = pool.loan_to(patron)

        feed_obj = LibraryLoanAndHoldAnnotator.single_item_feed(
            circulation, loan, test_mode=True
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
        circulation = create_autospec(spec=CirculationAPI)
        circulation.library = annotator_fixture.db.default_library()
        work = annotator_fixture.db.work(
            with_license_pool=True, with_open_access_download=False
        )
        pool = work.license_pools[0]
        loan, _ = pool.loan_to(patron)
        fulfillment = FulfillmentInfo(
            pool.collection,
            pool.data_source.name,
            pool.identifier.type,
            pool.identifier.identifier,
            "http://link",
            Representation.EPUB_MEDIA_TYPE,
            None,
            None,
        )

        feed_obj = LibraryLoanAndHoldAnnotator.single_item_feed(
            circulation, loan, fulfillment, test_mode=True
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
        fulfillment = FulfillmentInfo(
            pool.collection,
            pool.data_source.name,
            pool.identifier.type,
            pool.identifier.identifier,
            "http://streaming_link",
            Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE,
            None,
            None,
        )

        response = LibraryLoanAndHoldAnnotator.single_item_feed(
            None, loan, fulfillment, test_mode=True
        )
        raw = response.get_data(as_text=True)

        entries = feedparser.parse(raw)["entries"]
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
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        """Check that drm_device_registration_feed_tags returns
        a generic drm:licensor tag, except with the drm:scheme attribute
        set.
        """
        annotator_fixture.vendor_id.initialize_adobe(
            annotator_fixture.db.default_library()
        )
        annotator = LibraryLoanAndHoldAnnotator(
            None, None, annotator_fixture.db.default_library(), test_mode=True
        )
        patron = annotator_fixture.db.patron()
        [feed_tag] = annotator.drm_device_registration_feed_tags(patron)
        [generic_tag] = annotator.adobe_id_tags(patron)

        # The feed-level tag has the drm:scheme attribute set.
        key = "{http://librarysimplified.org/terms/drm}scheme"
        assert (
            "http://librarysimplified.org/terms/drm/scheme/ACS" == feed_tag.attrib[key]
        )

        # If we remove that attribute, the feed-level tag is the same as the
        # generic tag.
        del feed_tag.attrib[key]
        assert etree.tostring(feed_tag, method="c14n2") == etree.tostring(
            generic_tag, method="c14n2"
        )

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
            None, None, annotator_fixture.db.default_library(), test_mode=True
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
            feed = AcquisitionFeed(
                annotator_fixture.db.session, "test", "url", [], annotator
            )
            annotator.annotate_feed(feed, lane)
            raw = str(feed)
            parsed = feedparser.parse(raw)["feed"]
            links = parsed["links"]

            d = defaultdict(list)
            for link in links:
                d[link["rel"].lower()].append(link["href"])
            return d

        # When an EntryPoint is explicitly selected, it shows up in the
        # link to the search controller.
        facets = FacetsWithEntryPoint(entrypoint=AudiobooksEntryPoint)
        lane = annotator_fixture.db.lane()
        annotator = LibraryAnnotator(
            None,
            lane,
            annotator_fixture.db.default_library(),
            test_mode=True,
            facets=facets,
        )
        [url] = annotated_links(lane, annotator)["search"]
        assert "/lane_search" in url
        assert "entrypoint=%s" % AudiobooksEntryPoint.INTERNAL_NAME in url
        assert str(lane.id) in url

        # When the selected EntryPoint is a default, it's not used --
        # instead, we search everything.
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

    def test_acquisition_links(self, annotator_fixture: LibraryAnnotatorFixture):
        annotator = LibraryLoanAndHoldAnnotator(
            None, None, annotator_fixture.db.default_library(), test_mode=True
        )
        feed = AcquisitionFeed(
            annotator_fixture.db.session, "test", "url", [], annotator
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

        # Ensure the state variable
        assert annotator.identifies_patrons == True

        loan1_links = annotator.acquisition_links(
            loan1.license_pool, loan1, None, None, feed, loan1.license_pool.identifier
        )
        # Fulfill, and revoke.
        [revoke, fulfill] = sorted(loan1_links, key=lambda x: x.attrib.get("rel"))
        assert "revoke_loan_or_hold" in revoke.attrib.get("href")
        assert "http://librarysimplified.org/terms/rel/revoke" == revoke.attrib.get(
            "rel"
        )
        assert "fulfill" in fulfill.attrib.get("href")
        assert "http://opds-spec.org/acquisition" == fulfill.attrib.get("rel")

        # Allow direct open-access downloads
        # This will also filter out loan revoke links
        annotator.identifies_patrons = False
        loan1_links = annotator.acquisition_links(
            loan1.license_pool, loan1, None, None, feed, loan1.license_pool.identifier
        )
        assert len(loan1_links) == 1
        assert {"http://opds-spec.org/acquisition/open-access"} == {
            link.attrib.get("rel") for link in loan1_links
        }

        # Work 2 has no open access links
        loan2_links = annotator.acquisition_links(
            loan2.license_pool, loan2, None, None, feed, loan2.license_pool.identifier
        )
        assert len(loan2_links) == 0

        # Revert the annotator state
        annotator.identifies_patrons = True

        loan2_links = annotator.acquisition_links(
            loan2.license_pool, loan2, None, None, feed, loan2.license_pool.identifier
        )
        # Fulfill and revoke.
        [revoke, fulfill] = sorted(loan2_links, key=lambda x: x.attrib.get("rel"))
        assert "revoke_loan_or_hold" in revoke.attrib.get("href")
        assert "http://librarysimplified.org/terms/rel/revoke" == revoke.attrib.get(
            "rel"
        )
        assert "fulfill" in fulfill.attrib.get("href")
        assert "http://opds-spec.org/acquisition" == fulfill.attrib.get("rel")

        # If a book is ready to be fulfilled, but the library has
        # hidden all of its available content types, the fulfill link does
        # not show up -- only the revoke link.
        hidden = annotator_fixture.db.default_library().setting(
            Configuration.HIDDEN_CONTENT_TYPES
        )
        available_types = [
            lpdm.delivery_mechanism.content_type
            for lpdm in loan2.license_pool.delivery_mechanisms
        ]
        hidden.value = json.dumps(available_types)

        # The list of hidden content types is stored in the Annotator
        # constructor, so this particular test needs a fresh Annotator.
        annotator_with_hidden_types = LibraryLoanAndHoldAnnotator(
            None, None, annotator_fixture.db.default_library(), test_mode=True
        )
        loan2_links = annotator_with_hidden_types.acquisition_links(
            loan2.license_pool, loan2, None, None, feed, loan2.license_pool.identifier
        )
        [revoke] = loan2_links
        assert "http://librarysimplified.org/terms/rel/revoke" == revoke.attrib.get(
            "rel"
        )
        # Un-hide the content types so the test can continue.
        hidden.value = None

        hold_links = annotator.acquisition_links(
            hold.license_pool, None, hold, None, feed, hold.license_pool.identifier
        )
        # Borrow and revoke.
        [revoke, borrow] = sorted(hold_links, key=lambda x: x.attrib.get("rel"))
        assert "revoke_loan_or_hold" in revoke.attrib.get("href")
        assert "http://librarysimplified.org/terms/rel/revoke" == revoke.attrib.get(
            "rel"
        )
        assert "borrow" in borrow.attrib.get("href")
        assert "http://opds-spec.org/acquisition/borrow" == borrow.attrib.get("rel")

        work4_links = annotator.acquisition_links(
            work4.license_pools[0],
            None,
            None,
            None,
            feed,
            work4.license_pools[0].identifier,
        )
        # Borrow only.
        [borrow] = work4_links
        assert "borrow" in borrow.attrib.get("href")
        assert "http://opds-spec.org/acquisition/borrow" == borrow.attrib.get("rel")

        # If patron authentication is turned off for the library, then
        # only open-access links are displayed.
        annotator.identifies_patrons = False

        [open_access] = annotator.acquisition_links(
            loan1.license_pool, loan1, None, None, feed, loan1.license_pool.identifier
        )
        assert "http://opds-spec.org/acquisition/open-access" == open_access.attrib.get(
            "rel"
        )

        # This may include links with the open-access relation for
        # non-open-access works that are available without
        # authentication.  To get such link, you pass in a list of
        # LicensePoolDeliveryMechanisms as
        # `direct_fufillment_delivery_mechanisms`.
        [lp4] = work4.license_pools
        [lpdm4] = lp4.delivery_mechanisms
        lpdm4.set_rights_status(RightsStatus.IN_COPYRIGHT)
        [not_open_access] = annotator.acquisition_links(
            lp4,
            None,
            None,
            None,
            feed,
            lp4.identifier,
            direct_fulfillment_delivery_mechanisms=[lpdm4],
        )

        # The link relation is OPDS 'open-access', which just means the
        # book can be downloaded with no hassle.
        assert (
            "http://opds-spec.org/acquisition/open-access"
            == not_open_access.attrib.get("rel")
        )

        # The dcterms:rights attribute provides a more detailed
        # explanation of the book's copyright status -- note that it's
        # not "open access" in the typical sense.
        rights = not_open_access.attrib["{http://purl.org/dc/terms/}rights"]
        assert RightsStatus.IN_COPYRIGHT == rights

        # Hold links are absent even when there are active holds in the
        # database -- there is no way to distinguish one patron from
        # another so the concept of a 'hold' is meaningless.
        hold_links = annotator.acquisition_links(
            hold.license_pool, None, hold, None, feed, hold.license_pool.identifier
        )
        assert [] == hold_links

    def test_acquisition_links_multiple_links(
        self, annotator_fixture: LibraryAnnotatorFixture
    ):
        annotator = LibraryLoanAndHoldAnnotator(
            None, None, annotator_fixture.db.default_library(), test_mode=True
        )
        feed = AcquisitionFeed(
            annotator_fixture.db.session, "test", "url", [], annotator
        )

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

        # This means that two different acquisition links will be
        # generated -- one for each delivery mechanism.
        links = annotator.acquisition_links(
            pool, None, None, None, feed, pool.identifier, mock_api=MockAPI()
        )
        assert 2 == len(links)

        mech1_param = "mechanism_id=%s" % mech1.delivery_mechanism.id
        mech2_param = "mechanism_id=%s" % mech2.delivery_mechanism.id

        # Instead of sorting, which may be wrong if the id is greater than 10
        # due to how double digits are sorted, extract the links associated
        # with the expected delivery mechanism.
        if mech1_param in links[0].attrib["href"]:
            [mech1_link, mech2_link] = links
        else:
            [mech2_link, mech1_link] = links

        indirects = []
        for link in [mech1_link, mech2_link]:
            # Both links should have the same subtags.
            [availability, copies, holds, indirect] = sorted(link, key=lambda x: x.tag)
            assert availability.tag.endswith("availability")
            assert copies.tag.endswith("copies")
            assert holds.tag.endswith("holds")
            assert indirect.tag.endswith("indirectAcquisition")
            indirects.append(indirect)

        # The target of the top-level link is different.
        assert mech1_param in mech1_link.attrib["href"]
        assert mech2_param in mech2_link.attrib["href"]

        # So is the media type seen in the indirectAcquisition subtag.
        [mech1_indirect, mech2_indirect] = indirects

        # The first delivery mechanism (created when the Work was created)
        # uses Adobe DRM, so that shows up as the first indirect acquisition
        # type.
        assert mech1.delivery_mechanism.drm_scheme == mech1_indirect.attrib["type"]

        # The second delivery mechanism doesn't use DRM, so the content
        # type shows up as the first (and only) indirect acquisition type.
        assert mech2.delivery_mechanism.content_type == mech2_indirect.attrib["type"]

        # If we configure the library to hide one of the content types,
        # we end up with only one link -- the one for the delivery
        # mechanism that's not hidden.
        annotator_fixture.db.default_library().setting(
            Configuration.HIDDEN_CONTENT_TYPES
        ).value = json.dumps([mech1.delivery_mechanism.content_type])
        annotator = LibraryLoanAndHoldAnnotator(
            None, None, annotator_fixture.db.default_library(), test_mode=True
        )
        [link] = annotator.acquisition_links(
            pool, None, None, None, feed, pool.identifier, mock_api=MockAPI()
        )
        [availability, copies, holds, indirect] = sorted(link, key=lambda x: x.tag)
        assert mech2.delivery_mechanism.content_type == indirect.attrib["type"]


class TestLibraryLoanAndHoldAnnotator:
    def test_single_item_feed(self, db: DatabaseTransactionFixture):
        # Test the generation of single-item OPDS feeds for loans (with and
        # without fulfillment) and holds.
        class MockAnnotator(LibraryLoanAndHoldAnnotator):
            def url_for(self, controller, **kwargs):
                self.url_for_called_with = (controller, kwargs)
                return "a URL"

            def _single_entry_response(self, *args, **kwargs):
                self._single_entry_response_called_with = (args, kwargs)
                # Return the annotator itself so we can look at it.
                return self

        def test_annotator(item, fulfillment=None):
            # Call MockAnnotator.single_item_feed with certain arguments
            # and make some general assertions about the return value.
            circulation = object()
            test_mode = object()
            feed_class = object()
            result = MockAnnotator.single_item_feed(
                circulation, item, fulfillment, test_mode, feed_class, extra_arg="value"
            )

            # The final result is a MockAnnotator object. This isn't
            # normal; it's because
            # MockAnnotator._single_entry_response returns the
            # MockAnnotator it creates, for us to examine.
            assert isinstance(result, MockAnnotator)

            # Let's examine the MockAnnotator itself.
            assert circulation == result.circulation
            assert db.default_library() == result.library
            assert test_mode == result.test_mode

            # Now let's see what we did with it after calling its
            # constructor.

            # First, we generated a URL to the "loan_or_hold_detail"
            # controller for the license pool's identifier.
            url_call = result.url_for_called_with
            controller_name, kwargs = url_call
            assert "loan_or_hold_detail" == controller_name
            assert db.default_library().short_name == kwargs.pop("library_short_name")
            assert pool.identifier.type == kwargs.pop("identifier_type")
            assert pool.identifier.identifier == kwargs.pop("identifier")
            assert True == kwargs.pop("_external")
            assert {} == kwargs

            # The return value of that was the string "a URL". We then
            # passed that into _single_entry_response, along with
            # `item` and a number of arguments that we made up.
            response_call = result._single_entry_response_called_with
            (_db, _work, annotator, url, _feed_class), kwargs = response_call
            assert db.session == _db
            assert work == _work
            assert result == annotator
            assert "a URL" == url
            assert feed_class == _feed_class

            # The only keyword argument is an extra argument propagated from
            # the single_item_feed call.
            assert "value" == kwargs.pop("extra_arg")

            # Return the MockAnnotator for further examination.
            return result

        # Now we're going to call test_annotator a couple times in
        # different situations.
        work = db.work(with_license_pool=True)
        [pool] = work.license_pools
        patron = db.patron()
        loan, ignore = pool.loan_to(patron)

        # First, let's ask for a single-item feed for a loan.
        annotator = test_annotator(loan)

        # Everything tested by test_annotator happened, but _also_,
        # when the annotator was created, the Loan was stored in
        # active_loans_by_work.
        assert {work: loan} == annotator.active_loans_by_work

        # Since we passed in a loan rather than a hold,
        # active_holds_by_work is empty.
        assert {} == annotator.active_holds_by_work

        # Since we didn't pass in a fulfillment for the loan,
        # active_fulfillments_by_work is empty.
        assert {} == annotator.active_fulfillments_by_work

        # Now try it again, but give the loan a fulfillment.
        fulfillment = object()
        annotator = test_annotator(loan, fulfillment)
        assert {work: loan} == annotator.active_loans_by_work
        assert {work: fulfillment} == annotator.active_fulfillments_by_work

        # Finally, try it with a hold.
        hold, ignore = pool.on_hold_to(patron)
        annotator = test_annotator(hold)
        assert {work: hold} == annotator.active_holds_by_work
        assert {} == annotator.active_loans_by_work
        assert {} == annotator.active_fulfillments_by_work

    def test_single_item_feed_without_work(self, db: DatabaseTransactionFixture):
        """If a licensepool has no work or edition the single_item_feed mustn't raise an exception"""
        mock = MagicMock()
        # A loan without a pool
        loan = Loan(patron=db.patron())
        assert (
            LibraryLoanAndHoldAnnotator.single_item_feed(mock, loan)
            == NOT_FOUND_ON_REMOTE
        )

        work = db.work(with_license_pool=True)
        pool: LicensePool = get_one(db.session, LicensePool, work_id=work.id)
        # Pool with no work, and the presentation edition has no work either
        pool.work_id = None
        work.presentation_edition_id = None
        db.session.commit()
        assert (
            LibraryLoanAndHoldAnnotator.single_item_feed(mock, pool)
            == NOT_FOUND_ON_REMOTE
        )

        # pool with no work and no presentation edition
        pool.presentation_edition_id = None
        db.session.commit()
        assert (
            LibraryLoanAndHoldAnnotator.single_item_feed(mock, pool)
            == NOT_FOUND_ON_REMOTE
        )

    def test_choose_best_hold_for_work(self, db: DatabaseTransactionFixture):
        # First create two license pools for the same work so we could create two holds for the same work.
        patron = db.patron()

        coll_1 = db.collection(name="Collection 1")
        coll_2 = db.collection(name="Collection 2")

        work = db.work()

        pool_1 = db.licensepool(
            edition=work.presentation_edition, open_access=False, collection=coll_1
        )
        pool_2 = db.licensepool(
            edition=work.presentation_edition, open_access=False, collection=coll_2
        )

        hold_1, _ = pool_1.on_hold_to(patron)
        hold_2, _ = pool_2.on_hold_to(patron)

        # When there is no licenses_owned/available on one license pool the LibraryLoanAndHoldAnnotator should choose
        # hold associated with the other license pool.
        pool_1.licenses_owned = 0
        pool_1.licenses_available = 0

        assert hold_2 == LibraryLoanAndHoldAnnotator.choose_best_hold_for_work(
            [hold_1, hold_2]
        )

        # Now we have different number of licenses owned across two LPs and the same hold position.
        # Hold associated with LP with more owned licenses will be chosen as best.
        pool_1.licenses_owned = 2

        pool_2.licenses_owned = 3
        pool_2.licenses_available = 0

        hold_1.position = 7
        hold_2.position = 7

        assert hold_2 == LibraryLoanAndHoldAnnotator.choose_best_hold_for_work(
            [hold_1, hold_2]
        )


class SharedCollectionAnnotatorFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.work = db.work(with_open_access_download=True)
        self.collection = db.collection()
        self.lane = db.lane(display_name="Fantasy")
        self.annotator = SharedCollectionAnnotator(
            self.collection,
            self.lane,
            test_mode=True,
        )


@pytest.fixture(scope="function")
def shared_collection(
    db: DatabaseTransactionFixture,
) -> SharedCollectionAnnotatorFixture:
    return SharedCollectionAnnotatorFixture(db)


class TestSharedCollectionAnnotator:
    def test_top_level_title(self, shared_collection: SharedCollectionAnnotatorFixture):
        assert (
            shared_collection.collection.name
            == shared_collection.annotator.top_level_title()
        )

    def test_feed_url(self, shared_collection: SharedCollectionAnnotatorFixture):
        feed_url_fantasy = shared_collection.annotator.feed_url(
            shared_collection.lane, dict(), dict()
        )
        assert "feed" in feed_url_fantasy
        assert str(shared_collection.lane.id) in feed_url_fantasy
        assert shared_collection.collection.name in feed_url_fantasy

    def get_parsed_feed(
        self, shared_collection: SharedCollectionAnnotatorFixture, works, lane=None
    ):
        if not lane:
            lane = shared_collection.db.lane(display_name="Main Lane")
        feed = AcquisitionFeed(
            shared_collection.db.session,
            "test",
            "url",
            works,
            SharedCollectionAnnotator(
                shared_collection.collection, lane, test_mode=True
            ),
        )
        return feedparser.parse(str(feed))

    def assert_link_on_entry(
        self, entry, link_type=None, rels=None, partials_by_rel=None
    ):
        """Asserts that a link with a certain 'rel' value exists on a
        given feed or entry, as well as its link 'type' value and parts
        of its 'href' value.
        """

        def get_link_by_rel(rel, should_exist=True):
            try:
                [link] = [x for x in entry["links"] if x["rel"] == rel]
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

    def test_work_entry_includes_updated(
        self, shared_collection: SharedCollectionAnnotatorFixture
    ):
        work = shared_collection.db.work(with_open_access_download=True)
        work.license_pools[0].availability_time = datetime_utc(2018, 1, 1, 0, 0, 0)
        work.last_update_time = datetime_utc(2018, 2, 4, 0, 0, 0)

        feed = self.get_parsed_feed(shared_collection, [work])
        [entry] = feed.entries
        assert "2018-02-04" in entry.get("updated")

    def test_work_entry_includes_open_access_or_borrow_link(
        self, shared_collection: SharedCollectionAnnotatorFixture
    ):
        open_access_work = shared_collection.db.work(with_open_access_download=True)
        licensed_work = shared_collection.db.work(with_license_pool=True)
        licensed_work.license_pools[0].open_access = False

        feed = self.get_parsed_feed(
            shared_collection, [open_access_work, licensed_work]
        )
        [open_access_entry, licensed_entry] = feed.entries

        self.assert_link_on_entry(
            open_access_entry, rels=[Hyperlink.OPEN_ACCESS_DOWNLOAD]
        )
        self.assert_link_on_entry(licensed_entry, rels=[OPDSFeed.BORROW_REL])

        # The open access entry shouldn't have a borrow link, and the licensed entry
        # shouldn't have an open access link.
        links = [
            x for x in open_access_entry["links"] if x["rel"] == OPDSFeed.BORROW_REL
        ]
        assert 0 == len(links)
        links = [
            x
            for x in licensed_entry["links"]
            if x["rel"] == Hyperlink.OPEN_ACCESS_DOWNLOAD
        ]
        assert 0 == len(links)

    def test_borrow_link_raises_unfulfillable_work(
        self, shared_collection: SharedCollectionAnnotatorFixture
    ):
        edition, pool = shared_collection.db.edition(with_license_pool=True)
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

        annotator = SharedCollectionLoanAndHoldAnnotator(
            shared_collection.collection, None, test_mode=True
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

    def test_acquisition_links(
        self, shared_collection: SharedCollectionAnnotatorFixture
    ):
        annotator = SharedCollectionLoanAndHoldAnnotator(
            shared_collection.collection, None, test_mode=True
        )
        feed = AcquisitionFeed(
            shared_collection.db.session, "test", "url", [], annotator
        )

        client = shared_collection.db.integration_client()

        now = utc_now()
        tomorrow = now + datetime.timedelta(days=1)

        # Loan of an open-access book.
        work1 = shared_collection.db.work(with_open_access_download=True)
        loan1, ignore = work1.license_pools[0].loan_to(client, start=now)

        # Loan of a licensed book.
        work2 = shared_collection.db.work(with_license_pool=True)
        loan2, ignore = work2.license_pools[0].loan_to(client, start=now, end=tomorrow)

        # Hold on a licensed book.
        work3 = shared_collection.db.work(with_license_pool=True)
        hold, ignore = work3.license_pools[0].on_hold_to(
            client, start=now, end=tomorrow
        )

        # Book with no loans or holds yet.
        work4 = shared_collection.db.work(with_license_pool=True)

        loan1_links = annotator.acquisition_links(
            loan1.license_pool, loan1, None, None, feed, loan1.license_pool.identifier
        )
        # Fulfill, open access, revoke, and loan info.
        [revoke, fulfill, open_access, info] = sorted(
            loan1_links, key=lambda x: x.attrib.get("rel")
        )
        assert "shared_collection_revoke_loan" in revoke.attrib.get("href")
        assert "http://librarysimplified.org/terms/rel/revoke" == revoke.attrib.get(
            "rel"
        )
        assert "shared_collection_fulfill" in fulfill.attrib.get("href")
        assert "http://opds-spec.org/acquisition" == fulfill.attrib.get("rel")
        assert work1.license_pools[0].delivery_mechanisms[
            0
        ].resource.representation.mirror_url == open_access.attrib.get("href")
        assert "http://opds-spec.org/acquisition/open-access" == open_access.attrib.get(
            "rel"
        )
        assert "shared_collection_loan_info" in info.attrib.get("href")
        assert "self" == info.attrib.get("rel")

        loan2_links = annotator.acquisition_links(
            loan2.license_pool, loan2, None, None, feed, loan2.license_pool.identifier
        )
        # Fulfill, revoke, and loan info.
        [revoke, fulfill, info] = sorted(loan2_links, key=lambda x: x.attrib.get("rel"))
        assert "shared_collection_revoke_loan" in revoke.attrib.get("href")
        assert "http://librarysimplified.org/terms/rel/revoke" == revoke.attrib.get(
            "rel"
        )
        assert "shared_collection_fulfill" in fulfill.attrib.get("href")
        assert "http://opds-spec.org/acquisition" == fulfill.attrib.get("rel")
        assert "shared_collection_loan_info" in info.attrib.get("href")
        assert "self" == info.attrib.get("rel")

        hold_links = annotator.acquisition_links(
            hold.license_pool, None, hold, None, feed, hold.license_pool.identifier
        )
        # Borrow, revoke, and hold info.
        [revoke, borrow, info] = sorted(hold_links, key=lambda x: x.attrib.get("rel"))
        assert "shared_collection_revoke_hold" in revoke.attrib.get("href")
        assert "http://librarysimplified.org/terms/rel/revoke" == revoke.attrib.get(
            "rel"
        )
        assert "shared_collection_borrow" in borrow.attrib.get("href")
        assert "http://opds-spec.org/acquisition/borrow" == borrow.attrib.get("rel")
        assert "shared_collection_hold_info" in info.attrib.get("href")
        assert "self" == info.attrib.get("rel")

        work4_links = annotator.acquisition_links(
            work4.license_pools[0],
            None,
            None,
            None,
            feed,
            work4.license_pools[0].identifier,
        )
        # Borrow only.
        [borrow] = work4_links
        assert "shared_collection_borrow" in borrow.attrib.get("href")
        assert "http://opds-spec.org/acquisition/borrow" == borrow.attrib.get("rel")

    def test_single_item_feed(
        self, shared_collection: SharedCollectionAnnotatorFixture
    ):
        # Test the generation of single-item OPDS feeds for loans (with and
        # without fulfillment) and holds.
        class MockAnnotator(SharedCollectionLoanAndHoldAnnotator):
            def url_for(self, controller, **kwargs):
                self.url_for_called_with = (controller, kwargs)
                return "a URL"

            def _single_entry_response(self, *args, **kwargs):
                self._single_entry_response_called_with = (args, kwargs)
                # Return the annotator itself so we can look at it.
                return self

        def test_annotator(item, fulfillment, expect_route, expect_route_kwargs):
            # Call MockAnnotator.single_item_feed with certain arguments
            # and make some general assertions about the return value.
            test_mode = object()
            feed_class = object()
            result = MockAnnotator.single_item_feed(
                shared_collection.collection,
                item,
                fulfillment,
                test_mode,
                feed_class,
                extra_arg="value",
            )

            # The final result is a MockAnnotator object. This isn't
            # normal; it's because
            # MockAnnotator._single_entry_response returns the
            # MockAnnotator it creates, for us to examine.
            assert isinstance(result, MockAnnotator)

            # Let's examine the MockAnnotator itself.
            assert shared_collection.collection == result.collection
            assert test_mode == result.test_mode

            # Now let's see what we did with it after calling its
            # constructor.

            # First, we generated a URL to a controller for the
            # license pool's identifier. _Which_ controller we used
            # depends on what `item` is.
            url_call = result.url_for_called_with
            route, route_kwargs = url_call

            # The route is the one we expect.
            assert expect_route == route

            # Apart from a few keyword arguments that are always the same,
            # the keyword arguments are the ones we expect.
            assert shared_collection.collection.name == route_kwargs.pop(
                "collection_name"
            )
            assert True == route_kwargs.pop("_external")
            assert expect_route_kwargs == route_kwargs

            # The return value of that was the string "a URL". We then
            # passed that into _single_entry_response, along with
            # `item` and a number of arguments that we made up.
            response_call = result._single_entry_response_called_with
            (_db, _work, annotator, url, _feed_class), kwargs = response_call
            assert shared_collection.db.session == _db
            assert work == _work
            assert result == annotator
            assert "a URL" == url
            assert feed_class == _feed_class

            # The only keyword argument is an extra argument propagated from
            # the single_item_feed call.
            assert "value" == kwargs.pop("extra_arg")

            # Return the MockAnnotator for further examination.
            return result

        # Now we're going to call test_annotator a couple times in
        # different situations.
        work = shared_collection.work
        [pool] = work.license_pools
        patron = shared_collection.db.patron()
        loan, ignore = pool.loan_to(patron)

        # First, let's ask for a single-item feed for a loan.
        annotator = test_annotator(
            loan,
            None,
            expect_route="shared_collection_loan_info",
            expect_route_kwargs=dict(loan_id=loan.id),
        )

        # Everything tested by test_annotator happened, but _also_,
        # when the annotator was created, the Loan was stored in
        # active_loans_by_work.
        assert {work: loan} == annotator.active_loans_by_work

        # Since we passed in a loan rather than a hold,
        # active_holds_by_work is empty.
        assert {} == annotator.active_holds_by_work

        # Since we didn't pass in a fulfillment for the loan,
        # active_fulfillments_by_work is empty.
        assert {} == annotator.active_fulfillments_by_work

        # Now try it again, but give the loan a fulfillment.
        fulfillment = object()
        annotator = test_annotator(
            loan,
            fulfillment,
            expect_route="shared_collection_loan_info",
            expect_route_kwargs=dict(loan_id=loan.id),
        )
        assert {work: loan} == annotator.active_loans_by_work
        assert {work: fulfillment} == annotator.active_fulfillments_by_work

        # Finally, try it with a hold.
        hold, ignore = pool.on_hold_to(patron)
        annotator = test_annotator(
            hold,
            None,
            expect_route="shared_collection_hold_info",
            expect_route_kwargs=dict(hold_id=hold.id),
        )
        assert {work: hold} == annotator.active_holds_by_work
        assert {} == annotator.active_loans_by_work
        assert {} == annotator.active_fulfillments_by_work
