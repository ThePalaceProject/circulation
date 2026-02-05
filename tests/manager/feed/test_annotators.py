from datetime import timedelta
from functools import cmp_to_key
from unittest.mock import create_autospec, patch

import feedparser
import pytest
from bidict import frozenbidict

from palace.manager.api.circulation.base import CirculationApiType
from palace.manager.core.classifier import Classifier
from palace.manager.feed.acquisition import OPDSAcquisitionFeed
from palace.manager.feed.annotator.base import Annotator
from palace.manager.feed.annotator.circulation import CirculationManagerAnnotator
from palace.manager.feed.annotator.verbose import VerboseAnnotator
from palace.manager.feed.types import Link, WorkEntry
from palace.manager.feed.util import strftime
from palace.manager.feed.worklist.base import WorkList
from palace.manager.integration.license.opds.for_distributors.api import (
    OPDSForDistributorsAPI,
)
from palace.manager.integration.license.opds.odl.api import OPDS2WithODLApi
from palace.manager.integration.license.opds.opds1.api import OPDSAPI
from palace.manager.integration.license.opds.opds2.api import OPDS2API
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation, Resource
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import get_one_or_create, tuple_to_numericrange
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from tests.fixtures.database import DatabaseTransactionFixture, DBStatementCounter
from tests.fixtures.services import ServicesFixture
from tests.manager.feed.conftest import PatchedUrlFor


class TestAnnotators:
    def test_all_subjects(self, db: DatabaseTransactionFixture):
        session = db.session

        work = db.work(genre="Fiction", with_open_access_download=True)
        edition = work.presentation_edition
        identifier = edition.primary_identifier
        source1 = DataSource.lookup(session, DataSource.GUTENBERG)
        source2 = DataSource.lookup(session, DataSource.OCLC)

        subjects = [
            (source1, Subject.FAST, "fast1", "name1", 1),
            (source1, Subject.LCSH, "lcsh1", "name2", 1),
            (source2, Subject.LCSH, "lcsh1", "name2", 1),
            (source1, Subject.LCSH, "lcsh2", "name3", 3),
            (
                source1,
                Subject.DDC,
                "300",
                "Social sciences, sociology & anthropology",
                1,
            ),
        ]

        for source, subject_type, subject, name, weight in subjects:
            identifier.classify(source, subject_type, subject, name, weight=weight)

        # Mock Work.all_identifier_ids (called by VerboseAnnotator.categories)
        # so we can track the value that was passed in for `cutoff`.
        def mock_all_identifier_ids(policy=None):
            work.called_with_policy = policy
            # Do the actual work so that categories() gets the
            # correct information.
            return work.original_all_identifier_ids(policy)

        work.original_all_identifier_ids = work.all_identifier_ids
        work.all_identifier_ids = mock_all_identifier_ids
        category_tags = VerboseAnnotator.categories(work)

        # When we are generating subjects as part of an OPDS feed, by
        # default we set a cutoff of 100 equivalent identifiers. This
        # gives us reasonable worst-case performance at the cost of
        # not showing every single random subject under which an
        # extremely popular book is filed.
        assert 100 == work.called_with_policy.equivalent_identifier_cutoff

        ddc_uri = Subject.uri_lookup[Subject.DDC]
        rating_value = "ratingValue"
        assert [
            {
                "term": "300",
                rating_value: 1,
                "label": "Social sciences, sociology & anthropology",
            }
        ] == category_tags[ddc_uri]

        fast_uri = Subject.uri_lookup[Subject.FAST]
        assert [{"term": "fast1", "label": "name1", rating_value: 1}] == category_tags[
            fast_uri
        ]

        lcsh_uri = Subject.uri_lookup[Subject.LCSH]
        assert [
            {"term": "lcsh1", "label": "name2", rating_value: 2},
            {"term": "lcsh2", "label": "name3", rating_value: 3},
        ] == sorted(category_tags[lcsh_uri], key=lambda x: x[rating_value])

        genre_uri = Subject.uri_lookup[Subject.SIMPLIFIED_GENRE]
        assert [
            dict(label="Fiction", term=Subject.SIMPLIFIED_GENRE + "Fiction")
        ] == category_tags[genre_uri]

        # Age range assertions
        work = db.work(fiction=False, audience=Classifier.AUDIENCE_CHILDREN)
        work.target_age = tuple_to_numericrange((8, 12))
        categories = Annotator.categories(work)
        assert categories[Subject.SIMPLIFIED_FICTION_STATUS] == [
            dict(
                term=f"{Subject.SIMPLIFIED_FICTION_STATUS}Nonfiction",
                label="Nonfiction",
            )
        ]
        assert categories[Subject.uri_lookup[Subject.AGE_RANGE]] == [
            dict(term=work.target_age_string, label=work.target_age_string)
        ]

    def test_content(self, db: DatabaseTransactionFixture):
        work = db.work()
        work.summary_text = "A Summary"
        assert Annotator.content(work) == "A Summary"

        resrc = Resource()
        db.session.add(resrc)
        resrc.set_fetched_content("text", "Representation Summary", None)

        work.summary = resrc
        work.summary_text = None
        # The resource sets the summary
        assert Annotator.content(work) == "Representation Summary"
        assert work.summary_text == "Representation Summary"

        assert Annotator.content(None) == ""

    def test_appeals(self, db: DatabaseTransactionFixture):
        session = db.session

        work = db.work(with_open_access_download=True)
        work.appeal_language = 0.1
        work.appeal_character = 0.2
        work.appeal_story = 0.3
        work.appeal_setting = 0.4

        category_tags = VerboseAnnotator.categories(work)
        appeal_tags = category_tags[Work.APPEALS_URI]
        expect = [
            (Work.APPEALS_URI + Work.LANGUAGE_APPEAL, Work.LANGUAGE_APPEAL, 0.1),
            (Work.APPEALS_URI + Work.CHARACTER_APPEAL, Work.CHARACTER_APPEAL, 0.2),
            (Work.APPEALS_URI + Work.STORY_APPEAL, Work.STORY_APPEAL, 0.3),
            (Work.APPEALS_URI + Work.SETTING_APPEAL, Work.SETTING_APPEAL, 0.4),
        ]
        actual = [(x["term"], x["label"], x["ratingValue"]) for x in appeal_tags]
        assert set(expect) == set(actual)

    def test_authors(self, db: DatabaseTransactionFixture):
        edition = db.edition()
        [c_orig] = list(edition.contributors)

        c1 = edition.add_contributor("c1", Contributor.Role.AUTHOR)
        # No name contributor
        c_none = edition.add_contributor("c2", Contributor.Role.AUTHOR)
        c_none.display_name = ""
        c_none._sort_name = ""

        authors = Annotator.authors(edition)
        # The default, c1 and c_none
        assert len(edition.contributions) == 3
        # Only default and c1 are used in the feed, because c_none has no name
        assert len(authors["authors"]) == 2
        assert set(map(lambda x: x.name, authors["authors"])) == {
            c1.sort_name,
            c_orig.sort_name,
        }

    def test_detailed_author(self, db: DatabaseTransactionFixture):
        session = db.session

        c, ignore = db.contributor("Familyname, Givenname")
        c.display_name = "Givenname Familyname"
        c.family_name = "Familyname"
        c.wikipedia_name = "Givenname Familyname (Author)"
        c.viaf = "100"
        c.lc = "n100"

        author = VerboseAnnotator.detailed_author(c)

        assert "Givenname Familyname" == author.name
        assert "Familyname, Givenname" == author.sort_name
        assert "Givenname Familyname (Author)" == author.wikipedia_name
        assert "http://viaf.org/viaf/100" == author.viaf
        assert "http://id.loc.gov/authorities/names/n100" == author.lc

        work = db.work(authors=[], with_license_pool=True)
        work.presentation_edition.add_contributor(c, Contributor.Role.PRIMARY_AUTHOR)

        [same_tag] = VerboseAnnotator.authors(work.presentation_edition)["authors"]
        assert same_tag == author

    def test_duplicate_author_names_are_ignored(self, db: DatabaseTransactionFixture):
        session = db.session

        # Ignores duplicate author names
        work = db.work(with_license_pool=True)
        duplicate = db.contributor()[0]
        duplicate.sort_name = work.author

        edition = work.presentation_edition
        edition.add_contributor(duplicate, Contributor.Role.AUTHOR)

        assert 1 == len(Annotator.authors(edition)["authors"])

    def test_all_annotators_mention_every_relevant_author(
        self, db: DatabaseTransactionFixture
    ):
        session = db.session

        work = db.work(authors=[], with_license_pool=True)
        edition = work.presentation_edition

        primary_author, ignore = db.contributor()
        author, ignore = db.contributor()
        illustrator, ignore = db.contributor()
        barrel_washer, ignore = db.contributor()

        edition.add_contributor(primary_author, Contributor.Role.PRIMARY_AUTHOR)
        edition.add_contributor(author, Contributor.Role.AUTHOR)

        # This contributor is relevant because we have a MARC Role Code
        # for the role.
        edition.add_contributor(illustrator, Contributor.Role.ILLUSTRATOR)

        # This contributor is not relevant because we have no MARC
        # Role Code for the role.
        edition.add_contributor(barrel_washer, "Barrel Washer")

        illustrator_code = Contributor.MARC_ROLE_CODES[Contributor.Role.ILLUSTRATOR]

        tags = Annotator.authors(edition)
        # We made two <author> tags and one <contributor>
        # tag, for the illustrator.
        assert 2 == len(tags["authors"])
        assert 1 == len(tags["contributors"])
        assert [None, None, illustrator_code] == [
            x.role for x in (tags["authors"] + tags["contributors"])
        ]

        # Verbose annotator only creates author tags
        tags = VerboseAnnotator.authors(edition)
        assert 2 == len(tags["authors"])
        assert 0 == len(tags["contributors"])
        assert [None, None] == [x.role for x in (tags["authors"])]

    def test_ratings(self, db: DatabaseTransactionFixture):
        session = db.session

        work = db.work(with_license_pool=True, with_open_access_download=True)
        work.quality = 1.0 / 3
        work.popularity = 0.25
        work.rating = 0.6
        entry = OPDSAcquisitionFeed._create_entry(
            work,
            work.active_license_pool(),
            work.presentation_edition,
            work.presentation_edition.primary_identifier,
            VerboseAnnotator(),
        )
        assert entry.computed is not None

        ratings = [
            (
                getattr(rating, "rating_value"),
                getattr(rating, "additional_type"),
            )
            for rating in entry.computed.ratings
        ]
        expected = [
            ("0.3333", Measurement.QUALITY),
            ("0.2500", Measurement.POPULARITY),
            ("0.6000", None),
        ]
        assert set(expected) == set(ratings)

    def test_subtitle(
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ):
        work = db.work(with_license_pool=True, with_open_access_download=True)
        work.presentation_edition.subtitle = "Return of the Jedi"

        feed = OPDSAcquisitionFeed(
            db.fresh_str(),
            db.fresh_url(),
            [work],
            CirculationManagerAnnotator(None),
        )._feed

        computed = feed.entries[0].computed
        assert computed is not None
        assert computed.subtitle is not None
        assert computed.subtitle == "Return of the Jedi"

        # If there's no subtitle, the subtitle tag isn't included.
        work.presentation_edition.subtitle = None
        feed = OPDSAcquisitionFeed(
            db.fresh_str(),
            db.fresh_url(),
            [work],
            CirculationManagerAnnotator(None),
        )._feed

        computed = feed.entries[0].computed
        assert computed is not None
        assert computed.subtitle == None

    def test_series(
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ):
        work = db.work(with_license_pool=True, with_open_access_download=True)
        work.presentation_edition.series = "Harry Otter and the Lifetime of Despair"
        work.presentation_edition.series_position = 4

        feed = OPDSAcquisitionFeed(
            db.fresh_str(),
            db.fresh_url(),
            [work],
            CirculationManagerAnnotator(None),
        )._feed
        computed = feed.entries[0].computed
        assert computed is not None

        assert computed.series is not None
        assert computed.series.name == work.presentation_edition.series
        assert computed.series.position == work.presentation_edition.series_position

        # The series position can be 0, for a prequel for example.
        work.presentation_edition.series_position = 0

        feed = OPDSAcquisitionFeed(
            db.fresh_str(),
            db.fresh_url(),
            [work],
            CirculationManagerAnnotator(None),
        )._feed
        computed = feed.entries[0].computed
        assert computed is not None
        assert computed.series is not None
        assert computed.series.name == work.presentation_edition.series
        assert computed.series.position == work.presentation_edition.series_position

        # If there's no series title, the series tag isn't included.
        work.presentation_edition.series = None
        feed = OPDSAcquisitionFeed(
            db.fresh_str(),
            db.fresh_url(),
            [work],
            CirculationManagerAnnotator(None),
        )._feed
        computed = feed.entries[0].computed
        assert computed is not None
        assert computed.series == None

        # No series name
        assert Annotator.series(None, None) == None

    def test_samples(self, db: DatabaseTransactionFixture):
        session = db.session

        work = db.work(with_license_pool=True)
        edition = work.presentation_edition

        resource = Resource(url="sampleurl")
        session.add(resource)
        session.commit()

        sample_link = Hyperlink(
            rel=Hyperlink.SAMPLE,
            resource_id=resource.id,
            identifier_id=edition.primary_identifier_id,
            data_source_id=2,
        )
        session.add(sample_link)
        session.commit()

        with DBStatementCounter(db.connection) as counter:
            links = Annotator.samples(edition)
            count = counter.count

            assert len(links) == 1
            assert links[0].id == sample_link.id
            assert links[0].resource.url == "sampleurl"
            # accessing resource should not be another query
            assert counter.count == count

        # No edition = No samples
        assert Annotator.samples(None) == []


class TestAnnotator:
    def test_annotate_work_entry(self, db: DatabaseTransactionFixture):
        work = db.work(with_license_pool=True)
        pool = work.active_license_pool()
        edition: Edition = work.presentation_edition
        now = utc_now()

        def create_sample_link(media_type: str, url: str):
            representation, _ = get_one_or_create(
                db.session, Representation, url=url, media_type=media_type
            )
            resource, _ = get_one_or_create(
                db.session,
                Resource,
                url=url + "/preview",
                data_source=edition.data_source,
                representation=representation,
            )
            link = get_one_or_create(
                db.session,
                Hyperlink,
                identifier=edition.primary_identifier,
                data_source=edition.data_source,
                rel=Hyperlink.SAMPLE,
                resource=resource,
            )
            return link

        create_sample_link(MediaTypes.PDF_MEDIA_TYPE, "http://pdf")
        create_sample_link(MediaTypes.EPUB_MEDIA_TYPE, "http://epub-b")
        create_sample_link(MediaTypes.TEXT_HTML_MEDIA_TYPE, "http://html")
        create_sample_link(MediaTypes.APPLICATION_JSON_MEDIA_TYPE, "http://json")
        create_sample_link(MediaTypes.EPUB_MEDIA_TYPE, "http://epub-a")

        edition.cover_full_url = "http://coverurl.jpg"
        edition.cover_thumbnail_url = "http://thumburl.gif"
        work.summary_text = "Summary"
        edition.language = None
        work.last_update_time = now
        edition.publisher = "publisher"
        edition.imprint = "imprint"
        edition.issued = utc_now().date()
        edition.duration = 10

        # datetime for > today
        pool.availability_time = (utc_now() + timedelta(days=1)).date()

        entry = WorkEntry(
            work=work,
            edition=edition,
            identifier=edition.primary_identifier,
            license_pool=pool,
        )
        Annotator().annotate_work_entry(entry)
        data = entry.computed
        assert data is not None

        # Images
        assert len(data.image_links) == 2
        assert data.image_links[0] == Link(
            href=edition.cover_full_url, rel=Hyperlink.IMAGE, type="image/jpeg"
        )
        assert data.image_links[1] == Link(
            href=edition.cover_thumbnail_url,
            rel=Hyperlink.THUMBNAIL_IMAGE,
            type="image/gif",
        )

        # Other values
        assert data.imprint == "imprint"
        assert data.summary and data.summary.text == "Summary"
        assert data.summary and data.summary.content_type == "html"
        assert data.publisher == "publisher"
        assert data.issued == edition.issued
        assert data.duration == edition.duration
        assert data.distribution is not None
        assert data.distribution.provider_name == "Gutenberg"

        # Missing values
        assert data.language is None
        assert data.updated == strftime(now)

        # other links
        other_links = data.other_links
        assert other_links[0].type == MediaTypes.EPUB_MEDIA_TYPE
        assert other_links[1].type == MediaTypes.EPUB_MEDIA_TYPE
        assert other_links[2].type != MediaTypes.EPUB_MEDIA_TYPE
        assert other_links[3].type != MediaTypes.EPUB_MEDIA_TYPE
        assert other_links[4].type != MediaTypes.EPUB_MEDIA_TYPE

        # If the distributor is using a deprecated name, we get the
        # new name instead.
        entry = WorkEntry(
            work=work,
            edition=edition,
            identifier=edition.primary_identifier,
            license_pool=pool,
        )
        with patch.object(
            DataSource,
            "DEPRECATED_NAMES",
            frozenbidict({DataSource.GUTENBERG: "Project Gutenberg"}),
        ):
            Annotator().annotate_work_entry(entry)
        assert entry.computed.distribution.provider_name == "Project Gutenberg"


class CirculationManagerAnnotatorFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.work = db.work(with_open_access_download=True)
        self.lane = db.lane(display_name="Fantasy")
        self.annotator = CirculationManagerAnnotator(
            self.lane,
        )


@pytest.fixture(scope="function")
def circulation_fixture(
    db: DatabaseTransactionFixture,
    patch_url_for: PatchedUrlFor,
    services_fixture: ServicesFixture,
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
        assert lpdm.resource.url == link_tag.href

        # The dcterms:rights attribute may provide a more detailed
        # explanation of the book's copyright status.
        assert lpdm.rights_status.uri == link_tag.rights

        # If the Resource has a Representation, the public URL is used
        # instead of the original Resource URL.
        lpdm.resource.representation = representation
        link_tag = circulation_fixture.annotator.open_access_link(pool, lpdm)
        assert representation.public_url == link_tag.href

        # If there is no Representation, the Resource's original URL is used.
        lpdm.resource.representation = None
        link_tag = circulation_fixture.annotator.open_access_link(pool, lpdm)
        assert lpdm.resource.url == link_tag.href

    def test_default_lane_url(
        self, circulation_fixture: CirculationManagerAnnotatorFixture
    ):
        default_lane_url = circulation_fixture.annotator.default_lane_url()
        assert "feed" in default_lane_url
        assert str(circulation_fixture.lane.id) not in default_lane_url

    def test_feed_url(self, circulation_fixture: CirculationManagerAnnotatorFixture):
        feed_url_fantasy = circulation_fixture.annotator.feed_url(
            circulation_fixture.lane
        )
        assert "feed" in feed_url_fantasy
        assert str(circulation_fixture.lane.id) in feed_url_fantasy
        assert (
            str(circulation_fixture.db.default_library().name) not in feed_url_fantasy
        )

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
        [pool] = circulation_fixture.work.license_pools

        # Create a PDF delivery mechanism, that is not available.
        pool.set_delivery_mechanism(
            MediaTypes.PDF_MEDIA_TYPE, None, None, available=False
        )

        # Only the available delivery mechanisms are returned.
        [epub] = list(circulation_fixture.annotator.visible_delivery_mechanisms(pool))
        assert "application/epub+zip" == epub.delivery_mechanism.content_type

        # Create an annotator that hides PDFs.
        no_pdf = CirculationManagerAnnotator(
            circulation_fixture.lane,
            hidden_content_types=["application/pdf"],
        )

        # This has no effect on the EPUB.
        [epub2] = list(no_pdf.visible_delivery_mechanisms(pool))
        assert epub == epub2

        # Create an annotator that hides EPUBs.
        no_epub = CirculationManagerAnnotator(
            circulation_fixture.lane,
            hidden_content_types=["application/epub+zip"],
        )

        # The EPUB is hidden, and this license pool has no delivery
        # mechanisms.
        assert [] == list(no_epub.visible_delivery_mechanisms(pool))

    @pytest.mark.parametrize(
        "protocol, settings_type",
        [
            pytest.param(OPDSAPI, "opds_settings", id="OPDS"),
            pytest.param(OPDS2API, "opds_settings", id="OPDS2"),
            pytest.param(OPDS2WithODLApi, "opds2_odl_settings", id="OPDS2+ODL"),
            pytest.param(
                OPDSForDistributorsAPI,
                "opds_for_distributors_settings",
                id="OPDS for Distributors",
            ),
        ],
    )
    def test_visible_delivery_mechanisms_modified_by_circulation_api(
        self,
        circulation_fixture: CirculationManagerAnnotatorFixture,
        db: DatabaseTransactionFixture,
        protocol: type[CirculationApiType],
        settings_type: str,
    ):
        """
        Test that configuration options for a collection can change the
        order of delivery mechanisms returned by visible_delivery_mechanisms.

        Exhaustive testing of different configuration values isn't necessary
        here: See the tests for FormatProperties to see the actual semantics
        of the configuration values.
        """
        settings_callable = getattr(db, settings_type)
        collection = db.collection(
            protocol=protocol,
            settings=settings_callable(
                prioritized_drm_schemes=[DeliveryMechanism.LCP_DRM],
                prioritized_content_types=[MediaTypes.PDF_MEDIA_TYPE],
            ),
        )
        edition = db.edition(collection=collection)
        pool: LicensePool = db.licensepool(edition, collection=collection)

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

        annotator = CirculationManagerAnnotator(
            circulation_fixture.lane,
            hidden_content_types=[],
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

    def test_visible_delivery_mechanisms_calls_sort_delivery_mechanisms(
        self,
        db: DatabaseTransactionFixture,
    ) -> None:
        edition = db.edition()
        pool = db.licensepool(edition)

        mock_circulation_api = create_autospec(pool.collection.circulation_api)
        pool.collection.circulation_api = mock_circulation_api

        annotator = CirculationManagerAnnotator(
            None,
            hidden_content_types=[],
        )
        result = annotator.visible_delivery_mechanisms(pool)

        # Assert we made the expected calls
        mock_circulation_api.assert_called_once_with()
        mock_circulation_api.return_value.sort_delivery_mechanisms.assert_called_once_with(
            pool.delivery_mechanisms,
        )

        # Assert we returned the expected value
        assert (
            result
            == mock_circulation_api.return_value.sort_delivery_mechanisms.return_value
        )

    def test_rights_attribute(
        self, circulation_fixture: CirculationManagerAnnotatorFixture
    ):
        m = circulation_fixture.annotator.rights_attribute

        # Given a LicensePoolDeliveryMechanism with a RightsStatus,
        # rights_attribute returns the URI associated with the RightsStatus.
        lp = circulation_fixture.db.licensepool(None)
        [lpdm] = lp.delivery_mechanisms
        assert lpdm.rights_status.uri == m(lpdm)

        # If any link in the chain is broken, rights_attribute returns None.
        old_uri = lpdm.rights_status.uri
        lpdm.rights_status.uri = None
        assert m(lpdm) is None
        lpdm.rights_status.uri = old_uri

        lpdm.rights_status = None
        assert m(lpdm) is None

        assert m(None) is None

    def test_work_entry_includes_updated(
        self, circulation_fixture: CirculationManagerAnnotatorFixture
    ):
        # The 'updated' date is the value of
        # Work.last_update_time.
        work = circulation_fixture.db.work(with_open_access_download=True)
        # This date is later, but we don't check it.
        work.license_pools[0].availability_time = datetime_utc(2019, 1, 1)
        work.last_update_time = datetime_utc(2018, 2, 4)

        def entry_for(work):
            worklist = WorkList()
            worklist.initialize(None)
            annotator = CirculationManagerAnnotator(worklist)
            feed = (
                OPDSAcquisitionFeed("test", "url", [work], annotator).as_response().data
            )
            [entry] = feedparser.parse(str(feed)).entries
            return entry

        entry = entry_for(work)
        assert "2018-02-04" in entry.get("updated")

    def test_sample_link_sort(self):
        epub_link_a = Link(rel=None, href="a", type=MediaTypes.EPUB_MEDIA_TYPE)
        epub_link_b = Link(rel=None, href="b", type=MediaTypes.EPUB_MEDIA_TYPE)
        html_link = Link(rel=None, href="a", type=MediaTypes.TEXT_HTML_MEDIA_TYPE)
        pdf_link = Link(rel=None, href="a", type=MediaTypes.PDF_MEDIA_TYPE)
        kepub_link = Link(rel=None, href="a", type="application/kepub+zip")

        expected_sorted_order = [
            epub_link_a,
            epub_link_b,
            kepub_link,
            pdf_link,
            html_link,
        ]

        def test_expected_order(result, expected):
            assert result[0].type == expected[0].type
            assert result[1].type == expected[1].type
            assert result[2].type != MediaTypes.EPUB_MEDIA_TYPE
            assert result[3].type != MediaTypes.EPUB_MEDIA_TYPE
            assert result[4].type != MediaTypes.EPUB_MEDIA_TYPE

        test_1 = expected_sorted_order.copy()
        test_1.sort(key=cmp_to_key(Annotator._sample_link_comparator))
        test_expected_order(test_1, expected_sorted_order)

        test_2 = [kepub_link, html_link, pdf_link, epub_link_b, epub_link_a]
        test_2.sort(key=cmp_to_key(Annotator._sample_link_comparator))
        test_expected_order(test_2, expected_sorted_order)

        test_3 = [epub_link_b, epub_link_a, pdf_link, kepub_link, html_link]
        test_3.sort(key=cmp_to_key(Annotator._sample_link_comparator))
        test_expected_order(test_3, expected_sorted_order)
