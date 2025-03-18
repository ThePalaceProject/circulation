import csv
import datetime
import logging
from copy import deepcopy

import pytest
from freezegun import freeze_time

from palace.manager.core.classifier import NO_NUMBER, NO_VALUE
from palace.manager.core.metadata_layer import (
    CirculationData,
    ContributorData,
    CSVMetadataImporter,
    IdentifierData,
    LinkData,
    MeasurementData,
    Metadata,
    ReplacementPolicy,
    SubjectData,
    TimestampData,
)
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.coverage import (
    CoverageRecord,
    Timestamp,
    WorkCoverageRecord,
)
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import RightsStatus
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from palace.manager.util.sentinel import SentinelType
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import FilesFixture
from tests.mocks.mock import LogCaptureHandler


class TestIdentifierData:
    def test_constructor(self):
        data = IdentifierData(Identifier.ISBN, "foo", 0.5)
        assert Identifier.ISBN == data.type
        assert "foo" == data.identifier
        assert 0.5 == data.weight


class CSVFilesFixture(FilesFixture):
    """A fixture providing access to CSV files."""

    def __init__(self):
        super().__init__("csv")


@pytest.fixture()
def csv_files_fixture() -> CSVFilesFixture:
    """A fixture providing access to CSV files."""
    return CSVFilesFixture()


class TestMetadataImporter:
    def test_parse(self, csv_files_fixture: CSVFilesFixture):
        path = csv_files_fixture.sample_path("staff_picks_small.csv")
        reader = csv.DictReader(open(path))
        importer = CSVMetadataImporter(
            DataSource.LIBRARY_STAFF,
        )
        generator = importer.to_metadata(reader)
        m1, m2, m3 = list(generator)

        assert "Horrorst\xf6r" == m1.title
        assert "Grady Hendrix" == m1.contributors[0].display_name
        assert "Martin Jensen" == m2.contributors[0].display_name

        # Let's check out the identifiers we found.

        # The first book has an Overdrive ID
        [overdrive] = m1.identifiers
        assert Identifier.OVERDRIVE_ID == overdrive.type
        assert "504BA8F6-FF4E-4B57-896E-F1A50CFFCA0C" == overdrive.identifier
        assert 0.75 == overdrive.weight

        # The second book has no ID at all.
        assert [] == m2.identifiers

        # The third book has both a 3M ID and an Overdrive ID.
        overdrive, threem = sorted(m3.identifiers, key=lambda x: x.identifier)

        assert Identifier.OVERDRIVE_ID == overdrive.type
        assert "eae60d41-e0b8-4f9d-90b5-cbc43d433c2f" == overdrive.identifier
        assert 0.75 == overdrive.weight

        assert Identifier.THREEM_ID == threem.type
        assert "eswhyz9" == threem.identifier
        assert 0.75 == threem.weight

        # Now let's check out subjects.
        assert [
            ("schema:typicalAgeRange", "Adult", 100),
            ("tag", "Character Driven", 100),
            ("tag", "Historical", 100),
            ("tag", "Nail-Biters", 100),
            ("tag", "Setting Driven", 100),
        ] == [
            (x.type, x.identifier, x.weight)
            for x in sorted(m2.subjects, key=lambda x: x.identifier or "")
        ]

    def test_classifications_from_another_source_not_updated(
        self, db: DatabaseTransactionFixture
    ):
        # Set up an edition whose primary identifier has two
        # classifications.
        source1 = DataSource.lookup(db.session, DataSource.AXIS_360)
        source2 = DataSource.lookup(db.session, DataSource.METADATA_WRANGLER)
        edition = db.edition()
        identifier = edition.primary_identifier
        c1 = identifier.classify(source1, Subject.TAG, "i will persist")
        c2 = identifier.classify(source2, Subject.TAG, "i will perish")

        # Now we get some new metadata from source #2.
        subjects = [SubjectData(type=Subject.TAG, identifier="i will conquer")]
        metadata = Metadata(subjects=subjects, data_source=source2)
        replace = ReplacementPolicy(subjects=True)
        metadata.apply(edition, None, replace=replace)

        # The old classification from source #2 has been destroyed.
        # The old classification from source #1 is still there.
        assert ["i will conquer", "i will persist"] == sorted(
            x.subject.identifier for x in identifier.classifications
        )

    def test_classifications_with_missing_subject_name_and_ident(
        self, db: DatabaseTransactionFixture
    ):
        # A subject with no name or identifier should result in an
        # error message and no new classification.
        subjects = [SubjectData(type=Subject.TAG, name=None, identifier=None)]

        source1 = DataSource.lookup(db.session, DataSource.AXIS_360)
        edition = db.edition()
        identifier = edition.primary_identifier
        metadata = Metadata(subjects=subjects, data_source=source1)
        replace = ReplacementPolicy(subjects=True)
        with LogCaptureHandler(logging.root) as logs:
            metadata.apply(edition, None, replace=replace)
            assert len(logs.error) == 1
            assert str(logs.error[0]).startswith("Error classifying subject:")
            assert str(logs.error[0]).endswith(
                "Cannot look up Subject when neither identifier nor name is provided."
            )
        assert len(identifier.classifications) == 0

    def test_links(self, db: DatabaseTransactionFixture):
        edition = db.edition()
        l1 = LinkData(rel=Hyperlink.IMAGE, href="http://example.com/")
        l2 = LinkData(rel=Hyperlink.DESCRIPTION, content="foo")
        metadata = Metadata(links=[l1, l2], data_source=edition.data_source)
        metadata.apply(edition, None)
        [image, description] = sorted(
            edition.primary_identifier.links, key=lambda x: x.rel
        )
        assert Hyperlink.IMAGE == image.rel
        assert "http://example.com/" == image.resource.url

        assert Hyperlink.DESCRIPTION == description.rel
        assert b"foo" == description.resource.representation.content

    def test_image_with_original_and_rights(self, db: DatabaseTransactionFixture):
        edition = db.edition()
        data_source = DataSource.lookup(db.session, DataSource.LIBRARY_STAFF)
        original = LinkData(
            rel=Hyperlink.IMAGE,
            href="http://example.com/",
            media_type=Representation.PNG_MEDIA_TYPE,
            rights_uri=RightsStatus.PUBLIC_DOMAIN_USA,
            rights_explanation="This image is from 1922",
        )
        image_data = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x01\x03\x00\x00\x00%\xdbV\xca\x00\x00\x00\x06PLTE\xffM\x00\x01\x01\x01\x8e\x1e\xe5\x1b\x00\x00\x00\x01tRNS\xcc\xd24V\xfd\x00\x00\x00\nIDATx\x9cc`\x00\x00\x00\x02\x00\x01H\xaf\xa4q\x00\x00\x00\x00IEND\xaeB`\x82"
        derivative = LinkData(
            rel=Hyperlink.IMAGE,
            href="generic uri",
            content=image_data,
            media_type=Representation.PNG_MEDIA_TYPE,
            rights_uri=RightsStatus.PUBLIC_DOMAIN_USA,
            rights_explanation="This image is from 1922",
            original=original,
            transformation_settings=dict(position="top"),
        )

        metadata = Metadata(links=[derivative], data_source=data_source)
        metadata.apply(edition, None)
        [image] = edition.primary_identifier.links
        assert Hyperlink.IMAGE == image.rel
        assert "generic uri" == image.resource.url
        assert image_data == image.resource.representation.content
        assert RightsStatus.PUBLIC_DOMAIN_USA == image.resource.rights_status.uri
        assert "This image is from 1922" == image.resource.rights_explanation

        assert [] == image.resource.transformations
        transformation = image.resource.derived_through
        assert image.resource == transformation.derivative

        assert "http://example.com/" == transformation.original.url
        assert (
            RightsStatus.PUBLIC_DOMAIN_USA == transformation.original.rights_status.uri
        )
        assert "This image is from 1922" == transformation.original.rights_explanation
        assert "top" == transformation.settings.get("position")

    def test_image_and_thumbnail(self, db: DatabaseTransactionFixture):
        edition = db.edition()
        l2 = LinkData(
            rel=Hyperlink.THUMBNAIL_IMAGE,
            href="http://thumbnail.com/",
            media_type=Representation.JPEG_MEDIA_TYPE,
        )
        l1 = LinkData(
            rel=Hyperlink.IMAGE,
            href="http://example.com/",
            thumbnail=l2,
            media_type=Representation.JPEG_MEDIA_TYPE,
        )

        # Even though we're only passing in the primary image link...
        metadata = Metadata(links=[l1], data_source=edition.data_source)
        metadata.apply(edition, None)

        # ...a Hyperlink is also created for the thumbnail.
        [image, thumbnail] = sorted(
            edition.primary_identifier.links, key=lambda x: x.rel
        )
        assert Hyperlink.IMAGE == image.rel
        assert [
            thumbnail.resource.representation
        ] == image.resource.representation.thumbnails

    def test_thumbnail_isnt_a_thumbnail(self, db: DatabaseTransactionFixture):
        edition = db.edition()
        not_a_thumbnail = LinkData(
            rel=Hyperlink.DESCRIPTION,
            content="A great book",
            media_type=Representation.TEXT_PLAIN,
        )
        image = LinkData(
            rel=Hyperlink.IMAGE,
            href="http://example.com/",
            thumbnail=not_a_thumbnail,
            media_type=Representation.JPEG_MEDIA_TYPE,
        )

        metadata = Metadata(links=[image], data_source=edition.data_source)
        metadata.apply(edition, None)

        # Only one Hyperlink was created for the image, because
        # the alleged 'thumbnail' wasn't actually a thumbnail.
        [image_obj] = edition.primary_identifier.links
        assert Hyperlink.IMAGE == image_obj.rel
        assert [] == image_obj.resource.representation.thumbnails

        # If we pass in the 'thumbnail' separately, a Hyperlink is
        # created for it, but it's still not a thumbnail of anything.
        metadata = Metadata(
            links=[image, not_a_thumbnail], data_source=edition.data_source
        )
        metadata.apply(edition, None)
        [hyperlink_image, description] = sorted(
            edition.primary_identifier.links, key=lambda x: x.rel
        )
        assert Hyperlink.DESCRIPTION == description.rel
        assert b"A great book" == description.resource.representation.content
        assert [] == hyperlink_image.resource.representation.thumbnails
        assert None == description.resource.representation.thumbnail_of

    def test_image_and_thumbnail_are_the_same(self, db: DatabaseTransactionFixture):
        edition = db.edition()
        url = "http://tinyimage.com/image.jpg"
        l2 = LinkData(
            rel=Hyperlink.THUMBNAIL_IMAGE,
            href=url,
        )
        l1 = LinkData(
            rel=Hyperlink.IMAGE,
            href=url,
            thumbnail=l2,
        )
        metadata = Metadata(links=[l1, l2], data_source=edition.data_source)
        metadata.apply(edition, None)
        [image, thumbnail] = sorted(
            edition.primary_identifier.links, key=lambda x: x.rel
        )

        # The image and its thumbnail point to the same resource.
        assert image.resource == thumbnail.resource

        assert Hyperlink.IMAGE == image.rel
        assert Hyperlink.THUMBNAIL_IMAGE == thumbnail.rel

        # The thumbnail is marked as a thumbnail of the main image.
        assert [
            thumbnail.resource.representation
        ] == image.resource.representation.thumbnails
        assert url == edition.cover_full_url
        assert url == edition.cover_thumbnail_url

    def test_image_becomes_representation_but_thumbnail_does_not(
        self, db: DatabaseTransactionFixture
    ):
        edition = db.edition()

        # The thumbnail link has no media type, and none can be
        # derived from the URL.
        l2 = LinkData(
            rel=Hyperlink.THUMBNAIL_IMAGE,
            href="http://tinyimage.com/",
        )

        # The full-sized image link does not have this problem.
        l1 = LinkData(
            rel=Hyperlink.IMAGE,
            href="http://largeimage.com/",
            thumbnail=l2,
            media_type=Representation.JPEG_MEDIA_TYPE,
        )
        metadata = Metadata(links=[l1], data_source=edition.data_source)
        metadata.apply(edition, None)

        # Both LinkData objects have been imported as Hyperlinks.
        [image, thumbnail] = sorted(
            edition.primary_identifier.links, key=lambda x: x.rel
        )

        # However, since no Representation was created for the thumbnail,
        # the relationship between the main image and the thumbnail could
        # not be imported.
        assert None == thumbnail.resource.representation
        assert [] == image.resource.representation.thumbnails

        # The edition ends up with a full-sized image but no
        # thumbnail. This could potentially be improved, since we know
        # the two Resources are associated with the same Identifier.
        # But we lose track of the fact that the two Resources are
        # _the same image_ at different resolutions.
        assert "http://largeimage.com/" == edition.cover_full_url
        assert None == edition.cover_thumbnail_url

    def test_measurements(self, db: DatabaseTransactionFixture):
        edition = db.edition()
        measurement = MeasurementData(
            quantity_measured=Measurement.POPULARITY, value=100
        )
        metadata = Metadata(measurements=[measurement], data_source=edition.data_source)
        metadata.apply(edition, None)
        [m] = edition.primary_identifier.measurements
        assert Measurement.POPULARITY == m.quantity_measured
        assert 100 == m.value

    def test_coverage_record(self, db: DatabaseTransactionFixture):
        edition, pool = db.edition(with_license_pool=True)
        data_source = edition.data_source

        # No preexisting coverage record
        coverage = CoverageRecord.lookup(edition, data_source)
        assert coverage == None

        last_update = datetime_utc(2015, 1, 1)

        m = Metadata(
            data_source=data_source,
            title="New title",
            data_source_last_updated=last_update,
        )
        m.apply(edition, None)

        coverage = CoverageRecord.lookup(edition, data_source)
        assert last_update == coverage.timestamp
        assert "New title" == edition.title

        older_last_update = datetime_utc(2014, 1, 1)
        m = Metadata(
            data_source=data_source,
            title="Another new title",
            data_source_last_updated=older_last_update,
        )
        m.apply(edition, None)
        assert "New title" == edition.title

        coverage = CoverageRecord.lookup(edition, data_source)
        assert last_update == coverage.timestamp

        m.apply(edition, None, force=True)
        assert "Another new title" == edition.title
        coverage = CoverageRecord.lookup(edition, data_source)
        assert older_last_update == coverage.timestamp


class TestContributorData:
    def test__init__(self):
        # Roles defaults to AUTHOR
        assert ContributorData().roles == [Contributor.Role.AUTHOR]

        # If roles is a string, it is converted into a list
        assert ContributorData(roles="foo").roles == ["foo"]

        # if roles is a sequence (tuple, list, etc), it is copied to a list
        assert ContributorData(roles=("x", "y")).roles == ["x", "y"]
        assert ContributorData(roles=["x", "y"]).roles == ["x", "y"]

    def test_from_contribution(self, db: DatabaseTransactionFixture):
        # Makes sure ContributorData.from_contribution copies all the fields over.

        # make author with that name, add author to list and pass to edition
        contributors = ["PrimaryAuthor"]
        edition, pool = db.edition(with_license_pool=True, authors=contributors)

        contribution = edition.contributions[0]
        contributor = contribution.contributor
        contributor.lc = "1234567"
        contributor.viaf = "ABC123"
        contributor.aliases = ["Primo"]
        contributor.display_name = "Test Author For The Win"
        contributor.family_name = "TestAuttie"
        contributor.wikipedia_name = "TestWikiAuth"
        contributor.biography = "He was born on Main Street."

        contributor_data = ContributorData.from_contribution(contribution)

        # make sure contributor fields are still what I expect
        assert contributor_data.lc == contributor.lc
        assert contributor_data.viaf == contributor.viaf
        assert contributor_data.aliases == contributor.aliases
        assert contributor_data.display_name == contributor.display_name
        assert contributor_data.family_name == contributor.family_name
        assert contributor_data.wikipedia_name == contributor.wikipedia_name
        assert contributor_data.biography == contributor.biography

    def test_lookup(self, db: DatabaseTransactionFixture):
        # Test the method that uses the database to gather as much
        # self-consistent information as possible about a person.
        def m(*args, **kwargs):
            return ContributorData.lookup(db.session, *args, **kwargs)

        # We know very little about this person.
        l1, ignore = db.contributor(
            display_name="Ann Leckie",
            sort_name="Leckie, Ann",
        )

        # We know a lot about this person.
        pkd, ignore = db.contributor(
            sort_name="Dick, Phillip K.",
            display_name="Phillip K. Dick",
            viaf="27063583",
            lc="n79018147",
        )

        def _match(expect, actual):
            # Verify that two ContributorData objects have the
            # same db.
            #
            # If a value is None in one ContributorData, it must be None
            # in the other.
            assert isinstance(actual, ContributorData)
            assert expect.sort_name == actual.sort_name
            assert expect.display_name == actual.display_name
            assert expect.lc == actual.lc
            assert expect.viaf == actual.viaf

        # If there's no Contributor that matches the request, the method
        # returns None.
        assert None == m(sort_name="Marenghi, Garth")

        # If one and only one Contributor matches the request, the method
        # returns a ContributorData with all necessary information.
        _match(pkd, m(display_name="Phillip K. Dick"))
        _match(pkd, m(sort_name="Dick, Phillip K."))
        _match(pkd, m(viaf="27063583"))
        _match(pkd, m(lc="n79018147"))

        # If we're able to identify a Contributor from part of the
        # input, then any contradictory input is ignored in favor of
        # what we know from the database.
        _match(
            pkd,
            m(
                display_name="Phillip K. Dick",
                sort_name="Marenghi, Garth",
                viaf="1234",
                lc="abcd",
            ),
        )

        # If we're able to identify a Contributor, but we don't know some
        # of the information, those fields are left blank.
        expect = ContributorData(display_name="Ann Leckie", sort_name="Leckie, Ann")
        _match(expect, m(display_name="Ann Leckie"))

        # Now let's test cases where the database lookup finds
        # multiple Contributors.

        # An exact duplicate of an existing Contributor changes
        # nothing.
        duplicate, ignore = db.contributor(
            display_name="Ann Leckie",
            sort_name="Leckie, Ann",
        )
        _match(expect, m(display_name="Ann Leckie"))

        # If there's a duplicate that adds more information, multiple
        # records are consolidated, creating a synthetic
        # ContributorData that doesn't correspond to any one
        # Contributor.
        with_viaf, ignore = db.contributor(
            display_name="Ann Leckie",
            viaf="73520345",
        )
        # _contributor() set sort_name to a random value; remove it.
        with_viaf.sort_name = None

        expect = ContributorData(
            display_name="Ann Leckie", sort_name="Leckie, Ann", viaf="73520345"
        )
        _match(expect, m(display_name="Ann Leckie"))

        # Again, this works even if some of the incoming arguments
        # turn out not to be supported by the database db.
        _match(
            expect, m(display_name="Ann Leckie", sort_name="Ann Leckie", viaf="abcd")
        )

        # If there's a duplicate that provides conflicting information,
        # the corresponding field is left blank -- we don't know which
        # value is correct.
        with_incorrect_viaf, ignore = db.contributor(
            display_name="Ann Leckie",
            viaf="abcd",
        )
        with_incorrect_viaf.sort_name = None
        expect = ContributorData(
            display_name="Ann Leckie",
            sort_name="Leckie, Ann",
        )
        _match(expect, m(display_name="Ann Leckie"))

        # If there's conflicting information in the database for a
        # field, but the input included a value for that field, then
        # the input value is used.
        expect.viaf = "73520345"
        _match(expect, m(display_name="Ann Leckie", viaf="73520345"))

    def test_apply(self, db: DatabaseTransactionFixture):
        # Makes sure ContributorData.apply copies all the fields over when there's changes to be made.

        contributor_old, made_new = db.contributor(
            sort_name="Doe, John", viaf="viaf12345"
        )

        kwargs = dict()
        kwargs[Contributor.BIRTH_DATE] = "2001-01-01"

        contributor_data = ContributorData(
            sort_name="Doerr, John",
            lc="1234567",
            viaf="ABC123",
            aliases=["Primo"],
            display_name="Test Author For The Win",
            family_name="TestAuttie",
            wikipedia_name="TestWikiAuth",
            biography="He was born on Main Street.",
            extra=kwargs,
        )

        contributor_new, changed = contributor_data.apply(contributor_old)

        assert changed == True
        assert contributor_new.sort_name == "Doerr, John"
        assert contributor_new.lc == "1234567"
        assert contributor_new.viaf == "ABC123"
        assert contributor_new.aliases == ["Primo"]
        assert contributor_new.display_name == "Test Author For The Win"
        assert contributor_new.family_name == "TestAuttie"
        assert contributor_new.wikipedia_name == "TestWikiAuth"
        assert contributor_new.biography == "He was born on Main Street."

        assert contributor_new.extra[Contributor.BIRTH_DATE] == "2001-01-01"
        # assert_equal(contributor_new.contributions, "Audio")

        contributor_new, changed = contributor_data.apply(contributor_new)
        assert changed == False

    def test_display_name_to_sort_name_from_existing_contributor(
        self, db: DatabaseTransactionFixture
    ):
        # If there's an existing contributor with a matching display name,
        # we'll use their sort name.
        existing_contributor, ignore = db.contributor(
            sort_name="Sort, Name", display_name="John Doe"
        )
        assert (
            "Sort, Name"
            == ContributorData.display_name_to_sort_name_from_existing_contributor(
                db.session, "John Doe"
            )
        )

        # Otherwise, we don't know.
        assert (
            None
            == ContributorData.display_name_to_sort_name_from_existing_contributor(
                db.session, "Jane Doe"
            )
        )

    def test_find_sort_name(self, db: DatabaseTransactionFixture):
        existing_contributor, ignore = db.contributor(
            sort_name="Author, E.", display_name="Existing Author"
        )
        contributor_data = ContributorData()

        # If there's already a sort name, keep it.
        contributor_data.sort_name = "Sort Name"
        assert True == contributor_data.find_sort_name(db.session)
        assert "Sort Name" == contributor_data.sort_name

        contributor_data.sort_name = "Sort Name"
        contributor_data.display_name = "Existing Author"
        assert True == contributor_data.find_sort_name(db.session)
        assert "Sort Name" == contributor_data.sort_name

        contributor_data.sort_name = "Sort Name"
        contributor_data.display_name = "Metadata Client Author"
        assert True == contributor_data.find_sort_name(db.session)
        assert "Sort Name" == contributor_data.sort_name

        # If there's no sort name but there's already an author with the same display name,
        # use that author's sort name.
        contributor_data.sort_name = None
        contributor_data.display_name = "Existing Author"
        assert True == contributor_data.find_sort_name(db.session)
        assert "Author, E." == contributor_data.sort_name

        # If there's no sort name, no existing author, and nothing from the metadata
        # wrangler, guess the sort name based on the display name.
        contributor_data.sort_name = None
        contributor_data.display_name = "New Author"
        assert True == contributor_data.find_sort_name(db.session)
        assert "Author, New" == contributor_data.sort_name


class TestLinkData:
    def test_guess_media_type(self):
        rel = Hyperlink.IMAGE

        # Sometimes we have no idea what media type is at the other
        # end of a link.
        unknown = LinkData(rel, href="http://foo/bar.unknown")
        assert None == unknown.guessed_media_type

        # Sometimes we can guess based on the file extension.
        jpeg = LinkData(rel, href="http://foo/bar.jpeg")
        assert Representation.JPEG_MEDIA_TYPE == jpeg.guessed_media_type

        # An explicitly known media type takes precedence over
        # something we guess from the file extension.
        png = LinkData(
            rel, href="http://foo/bar.jpeg", media_type=Representation.PNG_MEDIA_TYPE
        )
        assert Representation.PNG_MEDIA_TYPE == png.guessed_media_type

        description = LinkData(Hyperlink.DESCRIPTION, content="Some content")
        assert None == description.guessed_media_type


class TestMetadata:
    def test_defaults(self):
        # Verify that a Metadata object doesn't make any assumptions
        # about an item's medium.
        m = Metadata(data_source=DataSource.OCLC)
        assert None == m.medium

    def test_from_edition(self, db: DatabaseTransactionFixture):
        session = db.session

        # Makes sure Metadata.from_edition copies all the fields over.

        edition, pool = db.edition(with_license_pool=True)
        edition.series = "Harry Otter and the Mollusk of Infamy"
        edition.series_position = "14"
        edition.primary_identifier.add_link(
            Hyperlink.IMAGE, "image", edition.data_source
        )
        edition.duration = 100.1
        metadata = Metadata.from_edition(edition)

        # make sure the metadata and the originating edition match
        for field in Metadata.BASIC_EDITION_FIELDS:
            assert getattr(edition, field) == getattr(metadata, field)

        e_contribution = edition.contributions[0]
        m_contributor_data = metadata.contributors[0]
        assert e_contribution.contributor.sort_name == m_contributor_data.sort_name
        assert e_contribution.role == m_contributor_data.roles[0]

        assert edition.data_source == metadata.data_source(session)
        assert (
            edition.primary_identifier.identifier
            == metadata.primary_identifier.identifier
        )

        e_link = edition.primary_identifier.links[0]
        m_link = metadata.links[0]
        assert e_link.rel == m_link.rel
        assert e_link.resource.url == m_link.href

        # The series position can also be 0.
        edition.series_position = 0
        metadata = Metadata.from_edition(edition)
        assert edition.series_position == metadata.series_position

    def test_update(self, db: DatabaseTransactionFixture):
        # Tests that Metadata.update correctly prefers new fields to old, unless
        # new fields aren't defined.

        edition_old, pool = db.edition(with_license_pool=True)
        edition_old.publisher = "test_old_publisher"
        edition_old.subtitle = "old_subtitile"
        edition_old.series = "old_series"
        edition_old.series_position = 5
        edition_old.duration = 10
        metadata_old = Metadata.from_edition(edition_old)

        edition_new, pool = db.edition(with_license_pool=True)
        # set more fields on metadatas
        edition_new.publisher = None
        edition_new.subtitle = "new_updated_subtitile"
        edition_new.series = "new_series"
        edition_new.series_position = 0
        edition_new.duration = 11
        metadata_new = Metadata.from_edition(edition_new)

        metadata_old.update(metadata_new)

        assert metadata_old.publisher == "test_old_publisher"
        assert metadata_old.subtitle == metadata_new.subtitle
        assert metadata_old.series == edition_new.series
        assert metadata_old.series_position == edition_new.series_position
        assert metadata_old.duration == metadata_new.duration

    def test_apply(self, db: DatabaseTransactionFixture):
        edition_old, pool = db.edition(with_license_pool=True)

        metadata = Metadata(
            data_source=DataSource.OVERDRIVE,
            title="The Harry Otter and the Seaweed of Ages",
            sort_title="Harry Otter and the Seaweed of Ages, The",
            subtitle="Kelp At It",
            series="The Harry Otter Sagas",
            series_position=4,
            language="eng",
            medium="Audio",
            publisher="Scholastic Inc",
            imprint="Follywood",
            published=datetime.date(1987, 5, 4),
            issued=datetime.date(1989, 4, 5),
            duration=10,
        )

        edition_new, changed = metadata.apply(edition_old, pool.collection)

        assert changed == True
        assert edition_new.title == "The Harry Otter and the Seaweed of Ages"
        assert edition_new.sort_title == "Harry Otter and the Seaweed of Ages, The"
        assert edition_new.subtitle == "Kelp At It"
        assert edition_new.series == "The Harry Otter Sagas"
        assert edition_new.series_position == 4
        assert edition_new.language == "eng"
        assert edition_new.medium == "Audio"
        assert edition_new.publisher == "Scholastic Inc"
        assert edition_new.imprint == "Follywood"
        assert edition_new.published == datetime.date(1987, 5, 4)
        assert edition_new.issued == datetime.date(1989, 4, 5)
        assert edition_new.duration == 10

        edition_new, changed = metadata.apply(edition_new, pool.collection)
        assert changed == False

        # The series position can also be 0.
        metadata.series_position = 0
        edition_new, changed = metadata.apply(edition_new, pool.collection)
        assert changed == True
        assert edition_new.series_position == 0

        # Metadata.apply() does not create a Work if no Work exists.
        assert 0 == db.session.query(Work).count()

    def test_apply_wipes_presentation_calculation_records(
        self, db: DatabaseTransactionFixture
    ):
        # We have a work.
        work = db.work(title="The Wrong Title", with_license_pool=True)

        # We learn some more information about the work's identifier.
        metadata = Metadata(
            data_source=DataSource.OVERDRIVE,
            primary_identifier=work.presentation_edition.primary_identifier,
            title="The Harry Otter and the Seaweed of Ages",
        )
        edition, ignore = metadata.edition(db.session)
        metadata.apply(edition, None)

        # The work still has the wrong title.
        assert "The Wrong Title" == work.title

        # However, the work is now slated to have its presentation
        # edition recalculated -- that will fix it.
        def assert_registered(full):
            """Verify that the WorkCoverageRecord for a full (full=True) or
            partial (full=false) presentation recalculation operation
            is in the 'registered' state, and that the
            WorkCoverageRecord for the other presentation
            recalculation operation is in the 'success' state.

            The verified WorkCoverageRecord will be reset to the 'success'
            state so that this can be called over and over without any
            extra setup.
            """
            WCR = WorkCoverageRecord
            for x in work.coverage_records:
                if x.operation == WCR.CLASSIFY_OPERATION:
                    if full:
                        assert WCR.REGISTERED == x.status
                        x.status = WCR.SUCCESS
                    else:
                        assert WCR.SUCCESS == x.status
                elif x.operation == WCR.CHOOSE_EDITION_OPERATION:
                    if full:
                        assert WCR.SUCCESS == x.status
                    else:
                        assert WCR.REGISTERED == x.status
                        x.status = WCR.SUCCESS

        assert_registered(full=False)

        # We then learn about a subject under which the work
        # is classified.
        metadata.title = None
        metadata.subjects = [SubjectData(Subject.TAG, "subject")]
        metadata.apply(edition, None)

        # The work is now slated to have its presentation completely
        # recalculated.

        # We then find a new description for the work.
        metadata.subjects = []
        metadata.links = [LinkData(rel=Hyperlink.DESCRIPTION, content="a description")]
        metadata.apply(edition, None)

        # We need to do a full recalculation again.
        assert_registered(full=True)

        # We then find a new cover image for the work.
        metadata.subjects = []
        metadata.links = [LinkData(rel=Hyperlink.IMAGE, href="http://image/")]
        metadata.apply(edition, None)

        # We need to choose a new presentation edition.
        assert_registered(full=False)

    def test_apply_identifier_equivalency(self, db: DatabaseTransactionFixture):
        # Set up an Edition.
        edition, pool = db.edition(with_license_pool=True)

        # Create two IdentifierData objects -- one corresponding to the
        # Edition's existing Identifier, and one new one.
        primary = edition.primary_identifier
        primary_as_data = IdentifierData(
            type=primary.type, identifier=primary.identifier
        )
        other_data = IdentifierData(type="abc", identifier="def")

        # Create a Metadata object that mentions the primary
        # identifier (as an Identifier) in `primary_identifier`, but doesn't
        # mention it in `identifiers`.
        metadata = Metadata(
            data_source=DataSource.OVERDRIVE,
            primary_identifier=primary,
            identifiers=[other_data],
        )

        # Metadata.identifiers has two elements -- the primary and the
        # other one.
        assert 2 == len(metadata.identifiers)
        assert primary_as_data in metadata.identifiers

        # Test case where the primary identifier is mentioned both as
        # primary_identifier and in identifiers
        metadata2 = Metadata(
            data_source=DataSource.OVERDRIVE,
            primary_identifier=primary,
            identifiers=[primary_as_data, other_data],
        )
        assert 2 == len(metadata2.identifiers)
        assert primary_as_data in metadata2.identifiers
        assert other_data in metadata2.identifiers

        # Write this state of affairs to the database.
        metadata2.apply(edition, pool.collection)

        # The new identifier has been marked as equivalent to the
        # Editions' primary identifier, but the primary identifier
        # itself is untouched, even though it showed up twice in the
        # list of identifiers.
        assert 1 == len(primary.equivalencies)
        [equivalency] = primary.equivalencies
        assert equivalency.output.type == "abc"
        assert equivalency.output.identifier == "def"

    def test_apply_no_value(self, db: DatabaseTransactionFixture):
        edition_old, pool = db.edition(with_license_pool=True)

        metadata = Metadata(
            data_source=DataSource.PRESENTATION_EDITION,
            subtitle=NO_VALUE,
            series=NO_VALUE,
            series_position=NO_NUMBER,
        )

        edition_new, changed = metadata.apply(edition_old, pool.collection)

        assert changed == True
        assert edition_new.title == edition_old.title
        assert edition_new.sort_title == edition_old.sort_title
        assert edition_new.subtitle == None
        assert edition_new.series == None
        assert edition_new.series_position == None
        assert edition_new.language == edition_old.language
        assert edition_new.medium == edition_old.medium
        assert edition_new.publisher == edition_old.publisher
        assert edition_new.imprint == edition_old.imprint
        assert edition_new.published == edition_old.published
        assert edition_new.issued == edition_old.issued

    def test_apply_creates_coverage_records(self, db: DatabaseTransactionFixture):
        edition, pool = db.edition(with_license_pool=True)

        metadata = Metadata(data_source=DataSource.OVERDRIVE, title=db.fresh_str())

        edition, changed = metadata.apply(edition, pool.collection)

        # One success was recorded.
        records = (
            db.session.query(CoverageRecord)
            .filter(CoverageRecord.identifier_id == edition.primary_identifier.id)
            .filter(CoverageRecord.operation == None)
        )
        assert 1 == records.count()
        assert CoverageRecord.SUCCESS == records.all()[0].status

        # Apply metadata from a different source.
        metadata = Metadata(data_source=DataSource.GUTENBERG, title=db.fresh_str())

        edition, changed = metadata.apply(edition, pool.collection)

        # Another success record was created.
        records = (
            db.session.query(CoverageRecord)
            .filter(CoverageRecord.identifier_id == edition.primary_identifier.id)
            .filter(CoverageRecord.operation == None)
        )
        assert 2 == records.count()
        for record in records.all():
            assert CoverageRecord.SUCCESS == record.status

    def test_update_contributions(self, db: DatabaseTransactionFixture):
        edition = db.edition()

        # A test edition is created with a test contributor. This
        # particular contributor is about to be destroyed and replaced by
        # new db.
        [old_contributor] = edition.contributors

        contributor = ContributorData(
            display_name="Robert Jordan",
            sort_name="Jordan, Robert",
            wikipedia_name="Robert_Jordan",
            viaf="79096089",
            lc="123",
            roles=[Contributor.Role.PRIMARY_AUTHOR],
        )

        metadata = Metadata(DataSource.OVERDRIVE, contributors=[contributor])
        metadata.update_contributions(db.session, edition, replace=True)

        # The old contributor has been removed and replaced with the new
        # one.
        [contributor] = edition.contributors
        assert contributor != old_contributor

        # And the new one has all the information provided by
        # the Metadata object.
        assert "Jordan, Robert" == contributor.sort_name
        assert "Robert Jordan" == contributor.display_name
        assert "79096089" == contributor.viaf
        assert "123" == contributor.lc
        assert "Robert_Jordan" == contributor.wikipedia_name

    def test_filter_recommendations(self, db: DatabaseTransactionFixture):
        metadata = Metadata(DataSource.OVERDRIVE)
        known_identifier = db.identifier()
        unknown_identifier = IdentifierData(Identifier.ISBN, "hey there")

        # Unknown identifiers are filtered out of the recommendations.
        metadata.recommendations += [known_identifier, unknown_identifier]
        metadata.filter_recommendations(db.session)
        assert [known_identifier] == metadata.recommendations

        # It works with IdentifierData as well.
        known_identifier_data = IdentifierData(
            known_identifier.type, known_identifier.identifier
        )
        metadata.recommendations = [known_identifier_data, unknown_identifier]
        metadata.filter_recommendations(db.session)
        [result] = metadata.recommendations
        # The IdentifierData has been replaced by a bonafide Identifier.
        assert isinstance(result, Identifier)
        # The genuine article.
        assert known_identifier == result

        # Recommendations are filtered to make sure the primary identifier is not recommended.
        primary_identifier = db.identifier()
        metadata = Metadata(DataSource.OVERDRIVE, primary_identifier=primary_identifier)
        metadata.recommendations = [
            known_identifier_data,
            unknown_identifier,
            primary_identifier,
        ]
        metadata.filter_recommendations(db.session)
        assert [known_identifier] == metadata.recommendations

    def test_metadata_can_be_deepcopied(self):
        # Check that we didn't put something in the metadata that
        # will prevent it from being copied. (e.g., self.log)

        subject = SubjectData(Subject.TAG, "subject")
        contributor = ContributorData()
        identifier = IdentifierData(Identifier.GUTENBERG_ID, "1")
        link = LinkData(Hyperlink.OPEN_ACCESS_DOWNLOAD, "example.epub")
        measurement = MeasurementData(Measurement.RATING, 5)
        circulation = CirculationData(
            data_source=DataSource.GUTENBERG,
            primary_identifier=identifier,
            licenses_owned=0,
            licenses_available=0,
            licenses_reserved=0,
            patrons_in_hold_queue=0,
        )
        primary_as_data = IdentifierData(
            type=identifier.type, identifier=identifier.identifier
        )
        other_data = IdentifierData(type="abc", identifier="def")

        m = Metadata(
            DataSource.GUTENBERG,
            subjects=[subject],
            contributors=[contributor],
            primary_identifier=identifier,
            links=[link],
            measurements=[measurement],
            circulation=circulation,
            title="Hello Title",
            subtitle="Subtle Hello",
            sort_title="Sorting Howdy",
            language="US English",
            medium=Edition.BOOK_MEDIUM,
            series="1",
            series_position=1,
            publisher="Hello World Publishing House",
            imprint="Follywood",
            issued=utc_now(),
            published=utc_now(),
            identifiers=[primary_as_data, other_data],
            data_source_last_updated=utc_now(),
        )

        m_copy = deepcopy(m)

        # If deepcopy didn't throw an exception we're ok.
        assert m_copy is not None

    def test_links_filtered(self):
        # test that filter links to only metadata-relevant ones
        link1 = LinkData(Hyperlink.OPEN_ACCESS_DOWNLOAD, "example.epub")
        link2 = LinkData(rel=Hyperlink.IMAGE, href="http://example.com/")
        link3 = LinkData(rel=Hyperlink.DESCRIPTION, content="foo")
        link4 = LinkData(
            rel=Hyperlink.THUMBNAIL_IMAGE,
            href="http://thumbnail.com/",
            media_type=Representation.JPEG_MEDIA_TYPE,
        )
        link5 = LinkData(
            rel=Hyperlink.IMAGE,
            href="http://example.com/",
            thumbnail=link4,
            media_type=Representation.JPEG_MEDIA_TYPE,
        )
        links = [link1, link2, link3, link4, link5]

        identifier = IdentifierData(Identifier.GUTENBERG_ID, "1")
        metadata = Metadata(
            data_source=DataSource.GUTENBERG,
            primary_identifier=identifier,
            links=links,
        )

        filtered_links = sorted(metadata.links, key=lambda x: x.rel)

        assert [link2, link5, link4, link3] == filtered_links


class TestTimestampData:
    def test_constructor(self):
        # By default, all fields are set to None
        d = TimestampData()
        for i in (
            d.service,
            d.service_type,
            d.collection_id,
            d.start,
            d.finish,
            d.achievements,
            d.counter,
            d.exception,
        ):
            assert i == None

        # Some, but not all, of the fields can be set to real values.
        d = TimestampData(
            start="a", finish="b", achievements="c", counter="d", exception="e"
        )
        assert "a" == d.start
        assert "b" == d.finish
        assert "c" == d.achievements
        assert "d" == d.counter
        assert "e" == d.exception

    def test_is_failure(self):
        # A TimestampData represents failure if its exception is set to
        # any value other than None or SentinelType.ClearValue.
        d = TimestampData()
        assert False == d.is_failure

        d.exception = "oops"
        assert True == d.is_failure

        d.exception = None
        assert False == d.is_failure

        d.exception = SentinelType.ClearValue
        assert False == d.is_failure

    def test_is_complete(self):
        # A TimestampData is complete if it represents a failure
        # (see above) or if its .finish is set to any value other
        # than None or SentinelType.ClearValue

        d = TimestampData()
        assert False == d.is_complete

        d.finish = "done!"
        assert True == d.is_complete

        d.finish = None
        assert False == d.is_complete

        d.finish = SentinelType.ClearValue
        assert False == d.is_complete

        d.exception = "oops"
        assert True == d.is_complete

    @freeze_time()
    def test_finalize_minimal(self, db: DatabaseTransactionFixture):
        # Calling finalize() with only the minimal arguments sets the
        # timestamp values to sensible defaults and leaves everything
        # else alone.

        # This TimestampData starts out with everything set to None.
        d = TimestampData()
        d.finalize("service", "service_type", db.default_collection())

        # finalize() requires values for these arguments, and sets them.
        assert "service" == d.service
        assert "service_type" == d.service_type
        assert db.default_collection().id == d.collection_id

        # The timestamp values are set to sensible defaults.
        assert d.start == d.finish == utc_now()

        # Other fields are still at None.
        for i in d.achievements, d.counter, d.exception:
            assert i is None

    def test_finalize_full(self, db: DatabaseTransactionFixture):
        # You can call finalize() with a complete set of arguments.
        d = TimestampData()
        start = utc_now() - datetime.timedelta(days=1)
        finish = utc_now() - datetime.timedelta(hours=1)
        counter = 100
        d.finalize(
            "service",
            "service_type",
            db.default_collection(),
            start=start,
            finish=finish,
            counter=counter,
            exception="exception",
        )
        assert start == d.start
        assert finish == d.finish
        assert counter == d.counter
        assert "exception" == d.exception

        # If the TimestampData fields are already set to values other
        # than SentinelType.ClearValue, the required fields will be overwritten but
        # the optional fields will be left alone.
        new_collection = db.collection()
        d.finalize(
            "service2",
            "service_type2",
            new_collection,
            start=utc_now(),
            finish=utc_now(),
            counter=15555,
            exception="exception2",
        )
        # These have changed.
        assert "service2" == d.service
        assert "service_type2" == d.service_type
        assert new_collection.id == d.collection_id

        # These have not.
        assert start == d.start
        assert finish == d.finish
        assert counter == d.counter
        assert "exception" == d.exception

    def test_collection(self, db: DatabaseTransactionFixture):
        session = db.session

        d = TimestampData()
        d.finalize("service", "service_type", db.default_collection())
        assert db.default_collection() == d.collection(session)

    @freeze_time()
    def test_apply(self, db: DatabaseTransactionFixture):
        session = db.session

        # You can't apply a TimestampData that hasn't been finalized.
        d = TimestampData()
        with pytest.raises(ValueError) as excinfo:
            d.apply(session)
        assert "Not enough information to write TimestampData to the database." in str(
            excinfo.value
        )

        # Set the basic timestamp information. Optional fields will stay
        # at None.
        collection = db.default_collection()
        d.finalize("service", Timestamp.SCRIPT_TYPE, collection)
        d.apply(session)

        timestamp = Timestamp.lookup(
            session, "service", Timestamp.SCRIPT_TYPE, collection
        )
        assert timestamp.start == timestamp.finish == utc_now()

        # Now set the optional fields as well.
        d.counter = 100
        d.achievements = "yay"
        d.exception = "oops"
        d.apply(session)

        assert 100 == timestamp.counter
        assert "yay" == timestamp.achievements
        assert "oops" == timestamp.exception

        # We can also use apply() to clear out the values for all
        # fields other than the ones that uniquely identify the
        # Timestamp.
        d.start = SentinelType.ClearValue
        d.finish = SentinelType.ClearValue
        d.counter = SentinelType.ClearValue
        d.achievements = SentinelType.ClearValue
        d.exception = SentinelType.ClearValue
        d.apply(session)

        assert None == timestamp.start
        assert None == timestamp.finish
        assert None == timestamp.counter
        assert None == timestamp.achievements
        assert None == timestamp.exception


class TestAssociateWithIdentifiersBasedOnPermanentWorkID:
    def test_success(self, db: DatabaseTransactionFixture):
        pwid = "pwid1"

        # Here's a print book.
        book = db.edition()
        book.medium = Edition.BOOK_MEDIUM
        book.permanent_work_id = pwid

        # Here's an audio book with the same PWID.
        audio = db.edition()
        audio.medium = Edition.AUDIO_MEDIUM
        audio.permanent_work_id = pwid

        # Here's an Metadata object for a second print book with the
        # same PWID.
        identifier = db.identifier()
        identifierdata = IdentifierData(
            type=identifier.type, identifier=identifier.identifier
        )
        metadata = Metadata(
            DataSource.GUTENBERG,
            primary_identifier=identifierdata,
            medium=Edition.BOOK_MEDIUM,
        )
        metadata.permanent_work_id = pwid

        # Call the method we're testing.
        metadata.associate_with_identifiers_based_on_permanent_work_id(db.session)

        # The identifier of the second print book has been associated
        # with the identifier of the first print book, but not
        # with the identifier of the audiobook
        equivalent_identifiers = [x.output for x in identifier.equivalencies]
        assert [book.primary_identifier] == equivalent_identifiers
