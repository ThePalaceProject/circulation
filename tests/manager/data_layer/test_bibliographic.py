import datetime
import logging
from copy import deepcopy
from functools import partial
from unittest.mock import patch

import pytest
from freezegun import freeze_time
from sqlalchemy import select

from palace.manager.core.classifier import NO_NUMBER, NO_VALUE
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.bibliographic import (
    _BASIC_EDITION_FIELDS,
    BibliographicData,
)
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.link import LinkData
from palace.manager.data_layer.measurement import MeasurementData
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.data_layer.subject import SubjectData
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.coverage import CoverageRecord
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import RightsStatus
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks.mock import LogCaptureHandler


class TestBibliographicData:
    def test_classifications_from_another_source_not_updated(
        self, db: DatabaseTransactionFixture
    ):
        # Set up an edition whose primary identifier has two
        # classifications.
        source1 = DataSource.lookup(db.session, DataSource.BOUNDLESS)
        source2 = DataSource.lookup(db.session, DataSource.METADATA_WRANGLER)
        edition = db.edition()
        identifier = edition.primary_identifier
        c1 = identifier.classify(source1, Subject.TAG, "i will persist")
        c2 = identifier.classify(source2, Subject.TAG, "i will perish")

        # Now we get some new bibliographic data from source #2.
        subjects = [SubjectData(type=Subject.TAG, identifier="i will conquer")]
        bibliographic = BibliographicData(
            subjects=subjects, data_source_name=source2.name
        )
        replace = ReplacementPolicy(subjects=True)
        bibliographic.apply(db.session, edition, None, replace=replace)

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

        source1 = DataSource.lookup(db.session, DataSource.BOUNDLESS)
        edition = db.edition()
        identifier = edition.primary_identifier
        bibliographic = BibliographicData(
            subjects=subjects, data_source_name=source1.name
        )
        replace = ReplacementPolicy(subjects=True)
        with LogCaptureHandler(logging.root) as logs:
            bibliographic.apply(db.session, edition, None, replace=replace)
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
        bibliographic = BibliographicData(
            links=[l1, l2], data_source_name=edition.data_source.name
        )
        bibliographic.apply(db.session, edition, None)
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

        bibliographic = BibliographicData(
            links=[derivative], data_source_name=data_source.name
        )
        bibliographic.apply(db.session, edition, None)
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
        bibliographic = BibliographicData(
            links=[l1], data_source_name=edition.data_source.name
        )
        bibliographic.apply(db.session, edition, None)

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

        bibliographic = BibliographicData(
            links=[image], data_source_name=edition.data_source.name
        )
        bibliographic.apply(db.session, edition, None)

        # Only one Hyperlink was created for the image, because
        # the alleged 'thumbnail' wasn't actually a thumbnail.
        [image_obj] = edition.primary_identifier.links
        assert Hyperlink.IMAGE == image_obj.rel
        assert [] == image_obj.resource.representation.thumbnails

        # If we pass in the 'thumbnail' separately, a Hyperlink is
        # created for it, but it's still not a thumbnail of anything.
        bibliographic = BibliographicData(
            links=[image, not_a_thumbnail], data_source_name=edition.data_source.name
        )
        bibliographic.apply(db.session, edition, None)
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
        bibliographic = BibliographicData(
            links=[l1, l2], data_source_name=edition.data_source.name
        )
        bibliographic.apply(db.session, edition, None)
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
        bibliographic = BibliographicData(
            links=[l1], data_source_name=edition.data_source.name
        )
        bibliographic.apply(db.session, edition, None)

        # Both LinkData objects have been imported as Hyperlinks.
        [image, thumbnail] = sorted(
            edition.primary_identifier.links, key=lambda x: x.rel
        )

        # However, since no Representation was created for the thumbnail,
        # the relationship between the main image and the thumbnail could
        # not be imported.
        assert None == thumbnail.resource.representation
        assert [] == image.resource.representation.thumbnails

        # The edition ends up with a full-sized image. Since no thumbnail
        # representation exists, the fallback behavior uses the full-size
        # image as the thumbnail.
        assert "http://largeimage.com/" == edition.cover_full_url
        assert "http://largeimage.com/" == edition.cover_thumbnail_url

    def test_process_thumbnails_skips_missing_links(
        self, db: DatabaseTransactionFixture
    ):
        """Test that _process_thumbnails gracefully handles links not in link_objects.

        This tests the defensive code path where a link in self.links might not
        be present in the link_objects dictionary passed to _process_thumbnails.
        This could happen in edge cases or future code changes.
        """
        edition = db.edition()
        data_source = edition.data_source

        # Create some links
        image_link = LinkData(
            rel=Hyperlink.IMAGE,
            href="http://example.com/image.jpg",
            media_type=Representation.JPEG_MEDIA_TYPE,
        )

        description_link = LinkData(
            rel=Hyperlink.DESCRIPTION,
            content="A description",
            media_type=Representation.TEXT_PLAIN,
        )

        # Create BibliographicData with both links
        bibliographic = BibliographicData(
            links=[image_link, description_link], data_source_name=data_source.name
        )

        # Apply to create the Hyperlink objects
        bibliographic.apply(db.session, edition, None)

        # Get the created hyperlink for image
        image_hyperlink = next(
            (l for l in edition.primary_identifier.links if l.rel == Hyperlink.IMAGE),
            None,
        )
        assert image_hyperlink is not None

        # Create a partial link_objects dict that only includes the image link
        # This simulates a scenario where description_link is in self.links
        # but not in link_objects
        partial_link_objects = {image_link: image_hyperlink}

        # Directly call _process_thumbnails with the partial dict
        # This should not raise a KeyError when it encounters description_link
        # which is in self.links but not in partial_link_objects
        bibliographic._process_thumbnails(db.session, data_source, partial_link_objects)

        # The method should complete without error, having skipped description_link

    def test_links_are_are_inserted_and_deleted_when_in_replace_mode_when_change_occurs(
        self, db: DatabaseTransactionFixture
    ):
        edition = db.edition()
        l1 = LinkData(
            rel=Hyperlink.IMAGE,
            href="http://example.com/",
            media_type=Representation.JPEG_MEDIA_TYPE,
        )

        link_to_be_deleted, ignore = edition.primary_identifier.add_link(
            rel=Hyperlink.IMAGE,
            href="http://example.com/to_be_deleted",
            data_source=edition.data_source,
            media_type=Representation.JPEG_MEDIA_TYPE,
        )

        assert len(edition.primary_identifier.links) == 1

        bibliographic = BibliographicData(
            links=[l1],
            data_source_name=edition.data_source.name,
        )
        replace = ReplacementPolicy(links=True)
        bibliographic.apply(db.session, edition, None, replace=replace)

        # we expect a change because the image link has changed.
        assert len(edition.primary_identifier.links) == 1
        assert edition.primary_identifier.links != [link_to_be_deleted]

    def test_that_links_are_not_deleted_and_reinserted_when_no_change(
        self, db: DatabaseTransactionFixture
    ):
        edition = db.edition()
        l1 = LinkData(
            rel=Hyperlink.IMAGE,
            href="http://example.com/existing_link",
            media_type=Representation.JPEG_MEDIA_TYPE,
        )

        existing_link, ignore = edition.primary_identifier.add_link(
            rel=Hyperlink.IMAGE,
            href="http://example.com/existing_link",
            data_source=edition.data_source,
            media_type=Representation.JPEG_MEDIA_TYPE,
        )

        assert len(edition.primary_identifier.links) == 1

        bibliographic = BibliographicData(
            links=[l1],
            data_source_name=edition.data_source.name,
        )
        replace = ReplacementPolicy(links=True)
        bibliographic.apply(db.session, edition, None, replace=replace)

        # we expect a change because we're in replace mode and the existing link disappeared and a new one was added.
        assert len(edition.primary_identifier.links) == 1
        assert existing_link.id == edition.primary_identifier.links[0].id

    def test_that_new_links_are_inserted_and_existing_links_not_deleted_and_reinserted_when_replacement_policy_is_false(
        self, db: DatabaseTransactionFixture
    ):
        edition = db.edition()
        l1 = LinkData(
            rel=Hyperlink.IMAGE,
            href="http://example.com/new_link",
            media_type=Representation.JPEG_MEDIA_TYPE,
        )

        existing_link, ignore = edition.primary_identifier.add_link(
            rel=Hyperlink.IMAGE,
            href="http://example.com/existing_link",
            data_source=edition.data_source,
            media_type=Representation.JPEG_MEDIA_TYPE,
        )

        assert len(edition.primary_identifier.links) == 1

        bibliographic = BibliographicData(
            links=[l1],
            data_source_name=edition.data_source.name,
        )
        replace = ReplacementPolicy(links=False)
        bibliographic.apply(db.session, edition, None, replace=replace)

        # We expect the new link to be added and the existing link preserved.
        assert len(edition.primary_identifier.links) == 2
        assert existing_link in edition.primary_identifier.links

    def test_measurements(self, db: DatabaseTransactionFixture):
        edition = db.edition()
        measurement = MeasurementData(
            quantity_measured=Measurement.POPULARITY, value=100
        )
        bibliographic = BibliographicData(
            measurements=[measurement], data_source_name=edition.data_source.name
        )
        bibliographic.apply(db.session, edition, None)
        [m] = edition.primary_identifier.measurements
        assert Measurement.POPULARITY == m.quantity_measured
        assert 100 == m.value

    def test_defaults(self) -> None:
        # Verify that a BibliographicData object doesn't make any assumptions
        # about an item's medium.
        bibliographic = BibliographicData(data_source_name=DataSource.OCLC)
        assert None == bibliographic.medium

    def test_from_edition(self, db: DatabaseTransactionFixture):
        session = db.session

        # Makes sure BibliographicData.from_edition copies all the fields over.

        edition, pool = db.edition(with_license_pool=True)
        edition.series = "Harry Otter and the Mollusk of Infamy"
        edition.series_position = 14
        edition.primary_identifier.add_link(
            Hyperlink.IMAGE, "image", edition.data_source
        )
        edition.duration = 100.1
        bibliographic = BibliographicData.from_edition(edition)

        # make sure the bibliographic and the originating edition match
        for field in _BASIC_EDITION_FIELDS:
            assert getattr(edition, field) == getattr(bibliographic, field)

        e_contribution = edition.contributions[0]
        m_contributor_data = bibliographic.contributors[0]
        assert e_contribution.contributor.sort_name == m_contributor_data.sort_name
        assert e_contribution.role == m_contributor_data.roles[0]

        assert edition.data_source == bibliographic.load_data_source(session)
        assert (
            edition.primary_identifier.identifier
            == bibliographic.primary_identifier_data.identifier
        )

        e_link = edition.primary_identifier.links[0]
        m_link = bibliographic.links[0]
        assert e_link.rel == m_link.rel
        assert e_link.resource.url == m_link.href
        assert edition.updated_at == bibliographic.data_source_last_updated

        # The series position can also be 0.
        edition.series_position = 0
        bibliographic = BibliographicData.from_edition(edition)
        assert edition.series_position == bibliographic.series_position

    def test_update(self, db: DatabaseTransactionFixture):
        # Tests that BibliographicData.update correctly prefers new fields to old, unless
        # new fields aren't defined.

        edition_old, pool = db.edition(with_license_pool=True)
        edition_old.publisher = "test_old_publisher"
        edition_old.subtitle = "old_subtitile"
        edition_old.series = "old_series"
        edition_old.series_position = 5
        edition_old.duration = 10
        bibliographic_old = BibliographicData.from_edition(edition_old)

        edition_new, pool = db.edition(with_license_pool=True)
        # set more fields on the edition
        edition_new.publisher = None
        edition_new.subtitle = "new_updated_subtitile"
        edition_new.series = "new_series"
        edition_new.series_position = 0
        edition_new.duration = 11
        bibliographic_new = BibliographicData.from_edition(edition_new)

        bibliographic_old.update(bibliographic_new)

        assert bibliographic_old.publisher == "test_old_publisher"
        assert bibliographic_old.subtitle == bibliographic_new.subtitle
        assert bibliographic_old.series == edition_new.series
        assert bibliographic_old.series_position == edition_new.series_position
        assert bibliographic_old.duration == bibliographic_new.duration

    def test_apply(self, db: DatabaseTransactionFixture):
        edition_old, pool = db.edition(with_license_pool=True)

        bibliographic = BibliographicData(
            data_source_name=DataSource.OVERDRIVE,
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

        edition_new, changed = bibliographic.apply(
            db.session, edition_old, pool.collection
        )

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

        edition_new, changed = bibliographic.apply(
            db.session, edition_new, pool.collection
        )
        assert changed == False

        # The series position can also be 0.
        bibliographic.series_position = 0
        edition_new, changed = bibliographic.apply(
            db.session, edition_new, pool.collection
        )
        assert changed == True
        assert edition_new.series_position == 0

        # BibliographicData.apply() does not create a Work if no Work exists.
        assert 0 == db.session.query(Work).count()

    def test_apply_causes_presentation_recalculation(
        self,
        db: DatabaseTransactionFixture,
    ):
        """Test that apply() calculates work presentation directly with the correct policy."""
        # We have a work.
        work = db.work(title="The Wrong Title", with_license_pool=True)

        # We learn some more information about the work's identifier.
        bibliographic = BibliographicData(
            data_source_name=DataSource.OVERDRIVE,
            primary_identifier_data=IdentifierData.from_identifier(
                work.presentation_edition.primary_identifier
            ),
            title="The Harry Otter and the Seaweed of Ages",
        )
        edition, ignore = bibliographic.edition(db.session)

        # After the refactoring, apply() calls work.calculate_presentation() directly
        with patch.object(Work, "calculate_presentation") as calculate:
            bibliographic.apply(db.session, edition, None)
            assert calculate.call_count == 1
            policy = calculate.call_args[1]["policy"]
            # Should use recalculate_presentation_edition policy for edition-only changes
            assert policy.classify is False
            assert policy.choose_summary is False

            # We then learn about a subject under which the work is classified.
            bibliographic.title = None
            bibliographic.subjects = [
                SubjectData(type=Subject.TAG, identifier="subject")
            ]

            bibliographic.apply(db.session, edition, None)
            # The work has now had its presentation recalculated directly.
            assert calculate.call_count == 2
            policy = calculate.call_args[1]["policy"]
            # Should use recalculate_everything policy for subject changes
            assert policy.classify is True
            assert policy.choose_summary is True

            # We then find a new description for the work.
            bibliographic.subjects = []
            bibliographic.links = [
                LinkData(rel=Hyperlink.DESCRIPTION, content="a description")
            ]

            bibliographic.apply(db.session, edition, None)
            # Full recalculation again for description changes.
            assert calculate.call_count == 3
            policy = calculate.call_args[1]["policy"]
            assert policy.classify is True
            assert policy.choose_summary is True

            # We then find a new cover image for the work.
            bibliographic.subjects = []
            bibliographic.links = [LinkData(rel=Hyperlink.IMAGE, href="http://image/")]

            bibliographic.apply(db.session, edition, None)
            # Presentation edition recalculation for image changes.
            assert calculate.call_count == 4
            policy = calculate.call_args[1]["policy"]
            assert policy.classify is False
            assert policy.choose_summary is False

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

        # Create a BibliographicData object that mentions the primary
        # identifier in `primary_identifier`, but doesn't
        # mention it in `identifiers`.
        bibliographic = BibliographicData(
            data_source_name=DataSource.OVERDRIVE,
            primary_identifier_data=primary_as_data,
            identifiers=[other_data],
        )

        # BibliographicData.identifiers has two elements -- the primary and the
        # other one.
        assert 2 == len(bibliographic.identifiers)
        assert primary_as_data in bibliographic.identifiers

        # Test case where the primary identifier is mentioned both as
        # primary_identifier and in identifiers
        bibliographic2 = BibliographicData(
            data_source_name=DataSource.OVERDRIVE,
            primary_identifier_data=primary_as_data,
            identifiers=[primary_as_data, other_data],
        )
        assert 2 == len(bibliographic2.identifiers)
        assert primary_as_data in bibliographic2.identifiers
        assert other_data in bibliographic2.identifiers

        # Write this state of affairs to the database.
        bibliographic2.apply(db.session, edition, pool.collection)

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

        bibliographic = BibliographicData(
            data_source_name=DataSource.PRESENTATION_EDITION,
            subtitle=NO_VALUE,
            series=NO_VALUE,
            series_position=NO_NUMBER,
        )

        edition_new, changed = bibliographic.apply(
            db.session, edition_old, pool.collection
        )

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

        bibliographic = BibliographicData(
            data_source_name=DataSource.OVERDRIVE, title=db.fresh_str()
        )

        edition, changed = bibliographic.apply(db.session, edition, pool.collection)

        # One success was recorded.
        records = (
            db.session.query(CoverageRecord)
            .filter(CoverageRecord.identifier_id == edition.primary_identifier.id)
            .filter(CoverageRecord.operation == None)
        )
        assert 1 == records.count()
        assert CoverageRecord.SUCCESS == records.all()[0].status

        # Apply BibliographicData from a different source.
        bibliographic = BibliographicData(
            data_source_name=DataSource.GUTENBERG, title=db.fresh_str()
        )

        edition, changed = bibliographic.apply(db.session, edition, pool.collection)

        # Another success record was created.
        records = (
            db.session.query(CoverageRecord)
            .filter(CoverageRecord.identifier_id == edition.primary_identifier.id)
            .filter(CoverageRecord.operation == None)
        )
        assert 2 == records.count()
        for record in records.all():
            assert CoverageRecord.SUCCESS == record.status

    def test_apply_does_not_create_coverage_records(
        self, db: DatabaseTransactionFixture
    ):
        edition, pool = db.edition(with_license_pool=True)

        bibliographic = BibliographicData(
            data_source_name=DataSource.OVERDRIVE, title=db.fresh_str()
        )

        bibliographic.apply(
            db.session, edition, pool.collection, create_coverage_record=False
        )

        # No coverage records were created.
        records = db.session.scalars(select(CoverageRecord)).all()
        assert len(records) == 0

    def test_apply_no_changes_needed(self, db: DatabaseTransactionFixture):
        edition, pool = db.edition(with_license_pool=True)
        edition.title = "Old title"
        edition.updated_at = utc_now()

        # Create a bibliographic data object that is slightly out of date.
        # It has a different title, but its data_source_last_updated is
        # earlier than the Edition's updated_at, so no changes will be made.
        stale_bibliographic = BibliographicData(
            data_source_name=edition.data_source.name,
            primary_identifier_data=IdentifierData.from_identifier(
                edition.primary_identifier
            ),
            title="New title",
            data_source_last_updated=utc_now() - datetime.timedelta(days=1),
        )

        # Applying a BibliographicData object created from an
        # Edition to that same Edition results in no changes.
        edition, changed = stale_bibliographic.apply(
            db.session, edition, pool.collection
        )
        assert changed == False
        assert edition.title != "New title"

        # If we roll back the edition's updated_at to before the
        # bibliographic's data_source_last_updated, the change will
        # be made.
        edition.updated_at = utc_now() - datetime.timedelta(days=2)
        edition, changed = stale_bibliographic.apply(
            db.session, edition, pool.collection
        )
        assert changed == True
        assert edition.title == "New title"

        # We will apply the changes even if we don't think its necessary when the replacement policy has
        # even_if_not_apparently_updated set to True
        edition.title = "Old title"
        edition.updated_at = utc_now()
        edition, changed = stale_bibliographic.apply(
            db.session,
            edition,
            pool.collection,
            replace=ReplacementPolicy(even_if_not_apparently_updated=True),
        )
        assert changed is True
        assert edition.title == "New title"

        # If the stale bibliographic has no data_source_last_updated, the change will be made.
        stale_bibliographic.data_source_last_updated = None
        edition.title = "Old title"
        edition.updated_at = utc_now()

        edition, changed = stale_bibliographic.apply(
            db.session, edition, pool.collection
        )
        assert changed == True
        assert edition.title == "New title"

        # Even if we don't need to update the BibliographicData, we still check to see if the CirculationData
        # needs to be updated.
        stale_bibliographic.data_source_last_updated = utc_now() - datetime.timedelta(
            days=1
        )
        stale_bibliographic.circulation = CirculationData(
            data_source_name=stale_bibliographic.data_source_name,
            primary_identifier_data=stale_bibliographic.primary_identifier_data,
            licenses_owned=10,
        )
        assert pool.licenses_owned != 10
        edition.title = "Old title"
        edition.updated_at = utc_now()
        edition, changed = stale_bibliographic.apply(
            db.session, edition, pool.collection
        )
        assert changed is False
        assert edition.title == "Old title"
        assert pool.licenses_owned == 10

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

        bibliographic = BibliographicData(
            data_source_name=DataSource.OVERDRIVE, contributors=[contributor]
        )
        bibliographic.update_contributions(db.session, edition, replace=True)

        # The old contributor has been removed and replaced with the new
        # one.
        [contributor] = edition.contributors
        assert contributor != old_contributor

        # And the new one has all the information provided by
        # the BibliographicData object.
        assert "Jordan, Robert" == contributor.sort_name
        assert "Robert Jordan" == contributor.display_name
        assert "79096089" == contributor.viaf
        assert "123" == contributor.lc
        assert "Robert_Jordan" == contributor.wikipedia_name

    def test_update_contributions_in_relace_mode_with_no_changes(
        self, db: DatabaseTransactionFixture
    ):
        edition = db.edition()
        original_contributions = list(edition.contributions)
        original_contributors = edition.contributors
        assert len(edition.contributions) == 1
        contribution = edition.contributions[0]
        contributor = contribution.contributor

        # update the contributions with the identical contribution.
        contributor_data = ContributorData(
            display_name=contributor.display_name,
            sort_name=contributor.sort_name,
            wikipedia_name=contributor.wikipedia_name,
            viaf=contributor.viaf,
            lc=contributor.lc,
            roles=[contribution.role],
        )

        bibliographic = BibliographicData(
            data_source_name=DataSource.OVERDRIVE, contributors=[contributor_data]
        )

        # we expect that no change occurred despite the replace flag.
        assert not bibliographic.update_contributions(db.session, edition, replace=True)

        # validate that the contributions did not change since no change occurred.
        assert edition.contributions == original_contributions
        assert edition.contributors == original_contributors

    def test_update_contributions_with_blank_display_name(
        self, db: DatabaseTransactionFixture
    ):
        edition = db.edition()
        contributor = ContributorData(
            display_name="",
            roles=[Contributor.Role.PRIMARY_AUTHOR],
        )
        bibliographic = BibliographicData(
            data_source_name=DataSource.OVERDRIVE, contributors=[contributor]
        )

        # the contributions should change because the contributor with a blank name will be removed from
        # the bibliographic data's contributors list.
        assert bibliographic.update_contributions(_db=db.session, edition=edition)

        # now specify a contributor with a display_name
        contributor = ContributorData(
            display_name="test",
            roles=[Contributor.Role.PRIMARY_AUTHOR],
        )

        bibliographic = BibliographicData(
            data_source_name=DataSource.OVERDRIVE, contributors=[contributor]
        )

        # the contributions should change because the contributor will not be removed.
        assert bibliographic.update_contributions(_db=db.session, edition=edition)

        # now specify a contributor with a sort name but no display_name
        contributor = ContributorData(
            display_name="",
            sort_name="test",
            roles=[Contributor.Role.PRIMARY_AUTHOR],
        )

        bibliographic = BibliographicData(
            data_source_name=DataSource.OVERDRIVE, contributors=[contributor]
        )

        edition2 = db.edition()
        # the contributions should change because the contributor will not be removed. Why not?
        # Because a display name is not required if a sort name is provided.
        assert bibliographic.update_contributions(_db=db.session, edition=edition2)

    def test_bibliographic_data_can_be_deepcopied(self):
        # Check that we didn't put something in the BibliographicData that
        # will prevent it from being copied. (e.g., self.log)

        subject = SubjectData(type=Subject.TAG, identifier="subject")
        contributor = ContributorData()
        identifier = IdentifierData(type=Identifier.GUTENBERG_ID, identifier="1")
        link = LinkData(rel=Hyperlink.OPEN_ACCESS_DOWNLOAD, href="example.epub")
        measurement = MeasurementData(quantity_measured=Measurement.RATING, value=5)
        circulation = CirculationData(
            data_source_name=DataSource.GUTENBERG,
            primary_identifier_data=identifier,
            licenses_owned=0,
            licenses_available=0,
            licenses_reserved=0,
            patrons_in_hold_queue=0,
        )
        primary_as_data = IdentifierData(
            type=identifier.type, identifier=identifier.identifier
        )
        other_data = IdentifierData(type="abc", identifier="def")

        bibliographic = BibliographicData(
            data_source_name=DataSource.GUTENBERG,
            subjects=[subject],
            contributors=[contributor],
            primary_identifier_data=identifier,
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
            issued=utc_now().date(),
            published=utc_now().date(),
            identifiers=[primary_as_data, other_data],
            data_source_last_updated=utc_now(),
        )

        bibliographic_copy = deepcopy(bibliographic)

        # If deepcopy didn't throw an exception we're ok.
        assert bibliographic_copy is not None

    def test_links_filtered(self):
        # test that filter links to only bibliographic-relevant ones
        link1 = LinkData(rel=Hyperlink.OPEN_ACCESS_DOWNLOAD, href="example.epub")
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

        identifier = IdentifierData(type=Identifier.GUTENBERG_ID, identifier="1")
        bibliographic = BibliographicData(
            data_source_name=DataSource.GUTENBERG,
            primary_identifier_data=identifier,
            links=links,
        )

        filtered_links = sorted(bibliographic.links, key=lambda x: x.rel)

        assert [link2, link5, link4, link3] == filtered_links

    def test_associate_with_identifiers_based_on_permanent_work_id(
        self, db: DatabaseTransactionFixture
    ):
        pwid = "pwid1"

        # Here's a print book.
        book = db.edition()
        book.medium = Edition.BOOK_MEDIUM
        book.permanent_work_id = pwid

        # Here's an audio book with the same PWID.
        audio = db.edition()
        audio.medium = Edition.AUDIO_MEDIUM
        audio.permanent_work_id = pwid

        # Here's an BibliographicData object for a second print book with the
        # same PWID.
        identifier = db.identifier()
        identifierdata = IdentifierData.from_identifier(identifier)
        bibliographic = BibliographicData(
            data_source_name=DataSource.GUTENBERG,
            primary_identifier_data=identifierdata,
            medium=Edition.BOOK_MEDIUM,
        )
        bibliographic.permanent_work_id = pwid

        # Call the method we're testing.
        bibliographic.associate_with_identifiers_based_on_permanent_work_id(db.session)

        # The identifier of the second print book has been associated
        # with the identifier of the first print book, but not
        # with the identifier of the audiobook
        equivalent_identifiers = [x.output for x in identifier.equivalencies]
        assert [book.primary_identifier] == equivalent_identifiers

    def test_edition_autocreate_false(self, db: DatabaseTransactionFixture) -> None:
        identifier = IdentifierData(
            type=db.fresh_str(),
            identifier=db.fresh_str(),
        )
        data = BibliographicData(
            data_source_name=db.fresh_str(),
        )

        get_edition = partial(data.edition, db.session, autocreate=False)

        # Need to have a primary identifier to get an edition.
        with pytest.raises(
            PalaceValueError, match="BibliographicData has no primary identifier"
        ):
            get_edition()

        data.primary_identifier_data = identifier

        # No datasource, no edition.
        assert get_edition() == (None, False)

        # Datasource exists, but no identifier.
        DataSource.lookup(db.session, data.data_source_name, autocreate=True)
        assert get_edition() == (None, False)

        # Identifier exists, but no edition.
        identifier.load(db.session)
        assert get_edition() == (None, False)

        # Edition exists!
        edition, _ = Edition.for_foreign_id(
            db.session, data.data_source_name, identifier.type, identifier.identifier
        )
        assert get_edition() == (edition, False)

    def test_roundtrip(self) -> None:
        bibliographic = BibliographicData(
            data_source_name=DataSource.OCLC,
            title="Test Title",
            subtitle="Test Subtitle",
            sort_title="Test Sort Title",
            language="eng",
            medium=Edition.BOOK_MEDIUM,
            publisher="Test Publisher",
            imprint="Test Imprint",
            published=datetime.date(2023, 1, 1),
            issued=datetime.date(2023, 1, 2),
        )
        deserialized = BibliographicData.model_validate_json(
            bibliographic.model_dump_json()
        )

        assert bibliographic == deserialized

    def test_data_source_last_updated_updates_timestamp(
        self, db: DatabaseTransactionFixture
    ):
        # Test that data_source_last_updated updates the timestamp
        # when a BibliographicData object is applied.
        last_updated = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        past = last_updated - datetime.timedelta(days=5)

        # Create an edition with a created and updated timestamp in the past.
        edition = db.edition()
        edition.created_at = past
        edition.updated_at = past
        db.session.flush()

        # When we apply the bibliographic data, the updated_at timestamp gets
        # set to the data_source_last_updated value.
        bibliographic = BibliographicData(
            data_source_name=DataSource.OVERDRIVE,
            data_source_last_updated=last_updated,
        )
        bibliographic.apply(db.session, edition, None)
        assert edition.updated_at == last_updated

        # If we apply the bibliographic data again with a past timestamp,
        # the updated_at timestamp should not change.
        bibliographic.data_source_last_updated = past
        bibliographic.apply(db.session, edition, None)
        assert edition.updated_at == last_updated
        assert edition.updated_at != past

        # If data_source_last_updated is None, updated_at will be updated to the
        # current time.
        bibliographic.data_source_last_updated = None
        now = utc_now()
        with freeze_time(now):
            bibliographic.apply(db.session, edition, None)
        assert edition.updated_at == now

    def test_validate_primary_identifier_case_insensitive(
        self, db: DatabaseTransactionFixture
    ):
        """Test that _validate_primary_identifier performs case-insensitive comparison."""
        # Create an edition with a mixed-case identifier
        edition = db.edition()
        edition.primary_identifier.identifier = "ABC123def"
        db.session.flush()

        identifier_lower = edition.primary_identifier.identifier.lower()
        identifier_upper = edition.primary_identifier.identifier.upper()
        identifier_mixed = edition.primary_identifier.identifier.swapcase()

        # Test with lowercase version - should NOT raise an error
        # because the identifiers match exactly -- no difference in case
        bibliographic_lower = BibliographicData(
            data_source_name=edition.data_source.name,
            primary_identifier_data=IdentifierData(
                type=edition.primary_identifier.type,
                identifier=identifier_lower,
            ),
            title="Lowercase",
        )
        # This should not raise an error due to case-insensitive comparison
        updated_edition, changed = bibliographic_lower.apply(db.session, edition, None)
        assert updated_edition.title == "Lowercase"

        # Test with uppercase version - should NOT raise an error
        bibliographic_upper = BibliographicData(
            data_source_name=edition.data_source.name,
            primary_identifier_data=IdentifierData(
                type=edition.primary_identifier.type,
                identifier=identifier_upper,
            ),
            title="Uppercase",
        )
        updated_edition, changed = bibliographic_upper.apply(db.session, edition, None)
        assert updated_edition.title == "Uppercase"

        # Test with swapped case version - should NOT raise an error
        bibliographic_mixed = BibliographicData(
            data_source_name=edition.data_source.name,
            primary_identifier_data=IdentifierData(
                type=edition.primary_identifier.type,
                identifier=identifier_mixed,
            ),
            title="Mixed Case",
        )
        updated_edition, changed = bibliographic_mixed.apply(db.session, edition, None)

        # Test that an identifier with differences beyond case still raises an error.
        bibliographic_wrong = BibliographicData(
            data_source_name=edition.data_source.name,
            primary_identifier_data=IdentifierData(
                type=edition.primary_identifier.type,
                identifier="completely_different_id",
            ),
            title="Completely Different",
        )
        with pytest.raises(PalaceValueError, match="primary identifier"):
            bibliographic_wrong.apply(db.session, edition, None)
