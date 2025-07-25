import datetime
import logging
from copy import deepcopy
from unittest.mock import patch

import pytest
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
from palace.manager.data_layer.policy.presentation import PresentationCalculationPolicy
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
        bibliographic = BibliographicData(
            measurements=[measurement], data_source_name=edition.data_source.name
        )
        bibliographic.apply(db.session, edition, None)
        [m] = edition.primary_identifier.measurements
        assert Measurement.POPULARITY == m.quantity_measured
        assert 100 == m.value

    def test_disable_async_calculation_flag(self, db: DatabaseTransactionFixture):
        edition, pool = db.edition(
            with_license_pool=True,
        )
        work = db.work()
        edition.work = work
        pool.work = work

        data_source = edition.data_source

        m = BibliographicData(
            data_source_name=data_source.name,
            title="New title",
            data_source_last_updated=datetime.datetime.now(),
        )

        with patch.object(Work, "queue_presentation_recalculation") as queue:
            m.apply(db.session, edition, None, disable_async_calculation=True)
            assert "New title" == edition.title
            # verify that the queue_presentation_recalculation was not called
            assert queue.call_count == 0

            m = BibliographicData(
                data_source_name=data_source.name,
                title="Another new title",
                data_source_last_updated=datetime.datetime.now(),
            )
            m.apply(db.session, edition, None)
            assert "Another new title" == edition.title
            # verify that the queue_presentation_recalculation was called
            assert queue.call_count == 1

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

        with patch(
            "palace.manager.celery.tasks.work.calculate_work_presentation"
        ) as calculate:
            bibliographic.apply(db.session, edition, None)
            assert calculate.delay.call_count == 1
            policy = PresentationCalculationPolicy.recalculate_presentation_edition()
            assert calculate.delay.call_args_list[0].kwargs == {
                "work_id": work.id,
                "policy": policy,
            }

            # The work still has the wrong title, but a full recalculation has been queued.
            assert "The Wrong Title" == work.title

            # We then learn about a subject under which the work
            # is classified.
            bibliographic.title = None
            bibliographic.subjects = [
                SubjectData(type=Subject.TAG, identifier="subject")
            ]

            bibliographic.apply(db.session, edition, None)
            # The work is now slated to have its presentation completely
            # recalculated.
            assert calculate.delay.call_count == 2
            policy = PresentationCalculationPolicy.recalculate_everything()
            assert calculate.delay.call_args_list[1].kwargs == {
                "work_id": work.id,
                "policy": policy,
            }

            # We then find a new description for the work.
            bibliographic.subjects = []
            bibliographic.links = [
                LinkData(rel=Hyperlink.DESCRIPTION, content="a description")
            ]

            bibliographic.apply(db.session, edition, None)
            # We need to do a full recalculation again.
            assert calculate.delay.call_count == 3
            policy = PresentationCalculationPolicy.recalculate_everything()
            assert calculate.delay.call_args_list[2].kwargs == {
                "work_id": work.id,
                "policy": policy,
            }

            # We then find a new cover image for the work.
            bibliographic.subjects = []
            bibliographic.links = [LinkData(rel=Hyperlink.IMAGE, href="http://image/")]

            bibliographic.apply(db.session, edition, None)
            # We need to choose a new presentation edition.
            assert calculate.delay.call_count == 4
            policy = PresentationCalculationPolicy.recalculate_presentation_edition()
            assert calculate.delay.call_args_list[3].kwargs == {
                "work_id": work.id,
                "policy": policy,
            }

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

    def test_load_edition(self, db: DatabaseTransactionFixture) -> None:
        identifier = IdentifierData(
            type=db.fresh_str(),
            identifier=db.fresh_str(),
        )
        data = BibliographicData(
            data_source_name=db.fresh_str(),
        )

        # Need to have a primary identifier to load an edition.
        with pytest.raises(
            PalaceValueError, match="BibliographicData has no primary identifier"
        ):
            data.load_edition(db.session)

        data.primary_identifier_data = identifier

        # No datasource, no edition.
        assert data.load_edition(db.session) is None

        # Datasource exists, but no identifier.
        DataSource.lookup(db.session, data.data_source_name, autocreate=True)
        assert data.load_edition(db.session) is None

        # Identifier exists, but no edition.
        identifier.load(db.session)
        assert data.load_edition(db.session) is None

        # Edition exists!
        edition, _ = Edition.for_foreign_id(
            db.session, data.data_source_name, identifier.type, identifier.identifier
        )
        assert data.load_edition(db.session) is edition

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
