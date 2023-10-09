from __future__ import annotations

import datetime
from typing import TYPE_CHECKING
from unittest.mock import MagicMock
from urllib.parse import quote

import pytest
from freezegun import freeze_time
from pymarc import MARCReader, Record

from core.config import CannotLoadConfiguration
from core.external_search import Filter
from core.lane import WorkList
from core.marc import Annotator, MARCExporter, MARCExporterFacets
from core.model import (
    CachedMARCFile,
    Contributor,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Genre,
    Identifier,
    LicensePoolDeliveryMechanism,
    Representation,
    RightsStatus,
    Work,
    get_one,
)
from core.util.datetime_helpers import datetime_utc, utc_now
from tests.mocks.search import ExternalSearchIndexFake

if TYPE_CHECKING:
    from tests.fixtures.database import DatabaseTransactionFixture
    from tests.fixtures.s3 import S3ServiceFixture
    from tests.fixtures.search import ExternalSearchFixtureFake


class TestAnnotator:
    def test_annotate_work_record(self, db: DatabaseTransactionFixture):
        session = db.session

        # Verify that annotate_work_record adds the distributor and formats.
        class MockAnnotator(Annotator):
            add_distributor_called_with = None
            add_formats_called_with = None

            def add_distributor(self, record, pool):
                self.add_distributor_called_with = [record, pool]

            def add_formats(self, record, pool):
                self.add_formats_called_with = [record, pool]

        annotator = MockAnnotator()
        record = Record()
        work = db.work(with_license_pool=True)
        pool = work.license_pools[0]

        annotator.annotate_work_record(work, pool, None, None, record)
        assert [record, pool] == annotator.add_distributor_called_with
        assert [record, pool] == annotator.add_formats_called_with

    def test_leader(self, db: DatabaseTransactionFixture):
        work = db.work(with_license_pool=True)
        leader = Annotator.leader(work)
        assert "00000nam  2200000   4500" == leader

        # If there's already a marc record cached, the record status changes.
        work.marc_record = "cached"
        leader = Annotator.leader(work)
        assert "00000cam  2200000   4500" == leader

    def _check_control_field(self, record, tag, expected):
        [field] = record.get_fields(tag)
        assert expected == field.value()

    def _check_field(self, record, tag, expected_subfields, expected_indicators=None):
        if not expected_indicators:
            expected_indicators = [" ", " "]
        [field] = record.get_fields(tag)
        assert expected_indicators == field.indicators
        for subfield, value in expected_subfields.items():
            assert value == field.get_subfields(subfield)[0]

    def test_add_control_fields(self, db: DatabaseTransactionFixture):
        # This edition has one format and was published before 1900.
        edition, pool = db.edition(with_license_pool=True)
        identifier = pool.identifier
        edition.issued = datetime_utc(956, 1, 1)

        now = utc_now()
        record = Record()

        Annotator.add_control_fields(record, identifier, pool, edition)
        self._check_control_field(record, "001", identifier.urn)
        assert now.strftime("%Y%m%d") in record.get_fields("005")[0].value()
        self._check_control_field(record, "006", "m        d        ")
        self._check_control_field(record, "007", "cr cn ---anuuu")
        self._check_control_field(
            record, "008", now.strftime("%y%m%d") + "s0956    xxu                 eng  "
        )

        # This French edition has two formats and was published in 2018.
        edition2, pool2 = db.edition(with_license_pool=True)
        identifier2 = pool2.identifier
        edition2.issued = datetime_utc(2018, 2, 3)
        edition2.language = "fre"
        LicensePoolDeliveryMechanism.set(
            pool2.data_source,
            identifier2,
            Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
            RightsStatus.IN_COPYRIGHT,
        )

        record = Record()
        Annotator.add_control_fields(record, identifier2, pool2, edition2)
        self._check_control_field(record, "001", identifier2.urn)
        assert now.strftime("%Y%m%d") in record.get_fields("005")[0].value()
        self._check_control_field(record, "006", "m        d        ")
        self._check_control_field(record, "007", "cr cn ---mnuuu")
        self._check_control_field(
            record, "008", now.strftime("%y%m%d") + "s2018    xxu                 fre  "
        )

    def test_add_marc_organization_code(self):
        record = Record()
        Annotator.add_marc_organization_code(record, "US-MaBoDPL")
        self._check_control_field(record, "003", "US-MaBoDPL")

    def test_add_isbn(self, db: DatabaseTransactionFixture):
        isbn = db.identifier(identifier_type=Identifier.ISBN)
        record = Record()
        Annotator.add_isbn(record, isbn)
        self._check_field(record, "020", {"a": isbn.identifier})

        # If the identifier isn't an ISBN, but has an equivalent that is, it still
        # works.
        equivalent = db.identifier()
        data_source = DataSource.lookup(db.session, DataSource.OCLC)
        equivalent.equivalent_to(data_source, isbn, 1)
        record = Record()
        Annotator.add_isbn(record, equivalent)
        self._check_field(record, "020", {"a": isbn.identifier})

        # If there is no ISBN, the field is left out.
        non_isbn = db.identifier()
        record = Record()
        Annotator.add_isbn(record, non_isbn)
        assert [] == record.get_fields("020")

    def test_add_title(self, db: DatabaseTransactionFixture):
        edition = db.edition()
        edition.title = "The Good Soldier"
        edition.sort_title = "Good Soldier, The"
        edition.subtitle = "A Tale of Passion"

        record = Record()
        Annotator.add_title(record, edition)
        [field] = record.get_fields("245")
        self._check_field(
            record,
            "245",
            {
                "a": edition.title,
                "b": edition.subtitle,
                "c": edition.author,
            },
            ["0", "4"],
        )

        # If there's no subtitle or no author, those subfields are left out.
        edition.subtitle = None
        edition.author = None

        record = Record()
        Annotator.add_title(record, edition)
        [field] = record.get_fields("245")
        self._check_field(
            record,
            "245",
            {
                "a": edition.title,
            },
            ["0", "4"],
        )
        assert [] == field.get_subfields("b")
        assert [] == field.get_subfields("c")

    def test_add_contributors(self, db: DatabaseTransactionFixture):
        author = "a"
        author2 = "b"
        translator = "c"

        # Edition with one author gets a 100 field and no 700 fields.
        edition = db.edition(authors=[author])
        edition.sort_author = "sorted"

        record = Record()
        Annotator.add_contributors(record, edition)
        assert [] == record.get_fields("700")
        self._check_field(record, "100", {"a": edition.sort_author}, ["1", " "])

        # Edition with two authors and a translator gets three 700 fields and no 100 fields.
        edition = db.edition(authors=[author, author2])
        edition.add_contributor(translator, Contributor.TRANSLATOR_ROLE)

        record = Record()
        Annotator.add_contributors(record, edition)
        assert [] == record.get_fields("100")
        fields = record.get_fields("700")
        for field in fields:
            assert ["1", " "] == field.indicators
        [author_field, author2_field, translator_field] = sorted(
            fields, key=lambda x: x.get_subfields("a")[0]
        )
        assert author == author_field.get_subfields("a")[0]
        assert Contributor.PRIMARY_AUTHOR_ROLE == author_field.get_subfields("e")[0]
        assert author2 == author2_field.get_subfields("a")[0]
        assert Contributor.AUTHOR_ROLE == author2_field.get_subfields("e")[0]
        assert translator == translator_field.get_subfields("a")[0]
        assert Contributor.TRANSLATOR_ROLE == translator_field.get_subfields("e")[0]

    def test_add_publisher(self, db: DatabaseTransactionFixture):
        edition = db.edition()
        edition.publisher = db.fresh_str()
        edition.issued = datetime_utc(1894, 4, 5)

        record = Record()
        Annotator.add_publisher(record, edition)
        self._check_field(
            record,
            "264",
            {
                "a": "[Place of publication not identified]",
                "b": edition.publisher,
                "c": "1894",
            },
            [" ", "1"],
        )

        # If there's no publisher, the field is left out.
        record = Record()
        edition.publisher = None
        Annotator.add_publisher(record, edition)
        assert [] == record.get_fields("264")

    def test_add_distributor(self, db: DatabaseTransactionFixture):
        edition, pool = db.edition(with_license_pool=True)
        record = Record()
        Annotator.add_distributor(record, pool)
        self._check_field(record, "264", {"b": pool.data_source.name}, [" ", "2"])

    def test_add_physical_description(self, db: DatabaseTransactionFixture):
        book = db.edition()
        book.medium = Edition.BOOK_MEDIUM
        audio = db.edition()
        audio.medium = Edition.AUDIO_MEDIUM

        record = Record()
        Annotator.add_physical_description(record, book)
        self._check_field(record, "300", {"a": "1 online resource"})
        self._check_field(
            record,
            "336",
            {
                "a": "text",
                "b": "txt",
                "2": "rdacontent",
            },
        )
        self._check_field(
            record,
            "337",
            {
                "a": "computer",
                "b": "c",
                "2": "rdamedia",
            },
        )
        self._check_field(
            record,
            "338",
            {
                "a": "online resource",
                "b": "cr",
                "2": "rdacarrier",
            },
        )
        self._check_field(
            record,
            "347",
            {
                "a": "text file",
                "2": "rda",
            },
        )
        self._check_field(
            record,
            "380",
            {
                "a": "eBook",
                "2": "tlcgt",
            },
        )

        record = Record()
        Annotator.add_physical_description(record, audio)
        self._check_field(
            record,
            "300",
            {
                "a": "1 sound file",
                "b": "digital",
            },
        )
        self._check_field(
            record,
            "336",
            {
                "a": "spoken word",
                "b": "spw",
                "2": "rdacontent",
            },
        )
        self._check_field(
            record,
            "337",
            {
                "a": "computer",
                "b": "c",
                "2": "rdamedia",
            },
        )
        self._check_field(
            record,
            "338",
            {
                "a": "online resource",
                "b": "cr",
                "2": "rdacarrier",
            },
        )
        self._check_field(
            record,
            "347",
            {
                "a": "audio file",
                "2": "rda",
            },
        )
        assert [] == record.get_fields("380")

    def test_add_audience(self, db: DatabaseTransactionFixture):
        for audience, term in list(Annotator.AUDIENCE_TERMS.items()):
            work = db.work(audience=audience)
            record = Record()
            Annotator.add_audience(record, work)
            self._check_field(
                record,
                "385",
                {
                    "a": term,
                    "2": "tlctarget",
                },
            )

    def test_add_series(self, db: DatabaseTransactionFixture):
        edition = db.edition()
        edition.series = db.fresh_str()
        edition.series_position = 5
        record = Record()
        Annotator.add_series(record, edition)
        self._check_field(
            record,
            "490",
            {
                "a": edition.series,
                "v": str(edition.series_position),
            },
            ["0", " "],
        )

        # If there's no series position, the same field is used without
        # the v subfield.
        edition.series_position = None
        record = Record()
        Annotator.add_series(record, edition)
        self._check_field(
            record,
            "490",
            {
                "a": edition.series,
            },
            ["0", " "],
        )
        [field] = record.get_fields("490")
        assert [] == field.get_subfields("v")

        # If there's no series, the field is left out.
        edition.series = None
        record = Record()
        Annotator.add_series(record, edition)
        assert [] == record.get_fields("490")

    def test_add_system_details(self):
        record = Record()
        Annotator.add_system_details(record)
        self._check_field(record, "538", {"a": "Mode of access: World Wide Web."})

    def test_add_formats(self, db: DatabaseTransactionFixture):
        edition, pool = db.edition(with_license_pool=True)
        epub_no_drm, ignore = DeliveryMechanism.lookup(
            db.session, Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM
        )
        pool.delivery_mechanisms[0].delivery_mechanism = epub_no_drm
        LicensePoolDeliveryMechanism.set(
            pool.data_source,
            pool.identifier,
            Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
            RightsStatus.IN_COPYRIGHT,
        )

        record = Record()
        Annotator.add_formats(record, pool)
        fields = record.get_fields("538")
        assert 2 == len(fields)
        [pdf, epub] = sorted(fields, key=lambda x: x.get_subfields("a")[0])
        assert "Adobe PDF eBook" == pdf.get_subfields("a")[0]
        assert [" ", " "] == pdf.indicators
        assert "EPUB eBook" == epub.get_subfields("a")[0]
        assert [" ", " "] == epub.indicators

    def test_add_summary(self, db: DatabaseTransactionFixture):
        work = db.work(with_license_pool=True)
        work.summary_text = "<p>Summary</p>"

        # Build and validate a record with a `520|a` summary.
        record = Record()
        Annotator.add_summary(record, work)
        self._check_field(record, "520", {"a": " Summary "})
        exported_record = record.as_marc()

        # Round trip the exported record to validate it.
        marc_reader = MARCReader(exported_record)
        round_tripped_record = next(marc_reader)
        self._check_field(round_tripped_record, "520", {"a": " Summary "})

    def test_add_simplified_genres(self, db: DatabaseTransactionFixture):
        work = db.work(with_license_pool=True)
        fantasy, ignore = Genre.lookup(db.session, "Fantasy", autocreate=True)
        romance, ignore = Genre.lookup(db.session, "Romance", autocreate=True)
        work.genres = [fantasy, romance]

        record = Record()
        Annotator.add_simplified_genres(record, work)
        fields = record.get_fields("650")
        [fantasy_field, romance_field] = sorted(
            fields, key=lambda x: x.get_subfields("a")[0]
        )
        assert ["0", "7"] == fantasy_field.indicators
        assert "Fantasy" == fantasy_field.get_subfields("a")[0]
        assert "Library Simplified" == fantasy_field.get_subfields("2")[0]
        assert ["0", "7"] == romance_field.indicators
        assert "Romance" == romance_field.get_subfields("a")[0]
        assert "Library Simplified" == romance_field.get_subfields("2")[0]

    def test_add_ebooks_subject(self):
        record = Record()
        Annotator.add_ebooks_subject(record)
        self._check_field(record, "655", {"a": "Electronic books."}, [" ", "0"])


class MarcExporterFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db

        self.integration = self._integration(db)
        self.now = utc_now()
        self.exporter = MARCExporter.from_config(db.default_library())
        self.annotator = Annotator()
        self.w1 = db.work(genre="Mystery", with_open_access_download=True)
        self.w2 = db.work(genre="Mystery", with_open_access_download=True)

        self.search_engine = ExternalSearchIndexFake(db.session)
        self.search_engine.mock_query_works([self.w1, self.w2])

    @staticmethod
    def _integration(db: DatabaseTransactionFixture):
        return db.external_integration(
            ExternalIntegration.MARC_EXPORT,
            ExternalIntegration.CATALOG_GOAL,
            libraries=[db.default_library()],
        )


@pytest.fixture
def marc_exporter_fixture(
    db: DatabaseTransactionFixture,
    external_search_fake_fixture: ExternalSearchFixtureFake,
) -> MarcExporterFixture:
    # external_search_fake_fixture is used only for the integration it creates
    return MarcExporterFixture(db)


class TestMARCExporter:
    def test_from_config(self, db: DatabaseTransactionFixture):
        pytest.raises(
            CannotLoadConfiguration, MARCExporter.from_config, db.default_library()
        )

        integration = MarcExporterFixture._integration(db)
        exporter = MARCExporter.from_config(db.default_library())
        assert integration == exporter.integration
        assert db.default_library() == exporter.library

        other_library = db.library()
        pytest.raises(CannotLoadConfiguration, MARCExporter.from_config, other_library)

    def test_create_record(self, db: DatabaseTransactionFixture):
        work = db.work(
            with_license_pool=True,
            title="old title",
            authors=["old author"],
            data_source_name=DataSource.OVERDRIVE,
        )
        annotator = Annotator()

        # The record isn't cached yet, so a new record is created and cached.
        assert None == work.marc_record
        record = MARCExporter.create_record(work, annotator)
        [title_field] = record.get_fields("245")
        assert "old title" == title_field.get_subfields("a")[0]
        [author_field] = record.get_fields("100")
        assert "author, old" == author_field.get_subfields("a")[0]
        [distributor_field] = record.get_fields("264")
        assert DataSource.OVERDRIVE == distributor_field.get_subfields("b")[0]
        cached = work.marc_record
        assert "old title" in cached
        assert "author, old" in cached
        # The distributor isn't part of the cached record.
        assert DataSource.OVERDRIVE not in cached

        work.presentation_edition.title = "new title"
        work.presentation_edition.sort_author = "author, new"
        new_data_source = DataSource.lookup(db.session, DataSource.BIBLIOTHECA)
        work.license_pools[0].data_source = new_data_source

        # Now that the record is cached, creating a record will
        # use the cache. Distributor will be updated since it's
        # not part of the cached record.
        record = MARCExporter.create_record(work, annotator)
        [title_field] = record.get_fields("245")
        assert "old title" == title_field.get_subfields("a")[0]
        [author_field] = record.get_fields("100")
        assert "author, old" == author_field.get_subfields("a")[0]
        [distributor_field] = record.get_fields("264")
        assert DataSource.BIBLIOTHECA == distributor_field.get_subfields("b")[0]

        # But we can force an update to the cached record.
        record = MARCExporter.create_record(work, annotator, force_create=True)
        [title_field] = record.get_fields("245")
        assert "new title" == title_field.get_subfields("a")[0]
        [author_field] = record.get_fields("100")
        assert "author, new" == author_field.get_subfields("a")[0]
        [distributor_field] = record.get_fields("264")
        assert DataSource.BIBLIOTHECA == distributor_field.get_subfields("b")[0]
        cached = work.marc_record
        assert "old title" not in cached
        assert "author, old" not in cached
        assert "new title" in cached
        assert "author, new" in cached

        # If we pass in an integration, it's passed along to the annotator.
        integration = MarcExporterFixture._integration(db)

        class MockAnnotator(Annotator):
            integration = None

            def annotate_work_record(
                self, work, pool, edition, identifier, record, integration
            ):
                self.integration = integration

        annotator = MockAnnotator()
        record = MARCExporter.create_record(work, annotator, integration=integration)
        assert integration == annotator.integration

    @freeze_time("2020-01-01 00:00:00")
    def test_create_record_roundtrip(self, db: DatabaseTransactionFixture):
        # Create a marc record from a work with special characters
        # in both the title and author name and round-trip it to
        # the DB and back again to make sure we are creating records
        # we can understand.
        #
        # We freeze the current time here, because a MARC record has
        # a timestamp when it was created and we need the created
        # records to match.

        annotator = Annotator()

        # Creates a new record and saves it to the database
        work = db.work(
            title="Little Mimi\u2019s First Counting Lesson",
            authors=["Lagerlo\xf6f, Selma Ottiliana Lovisa,"],
            with_license_pool=True,
        )
        record = MARCExporter.create_record(work, annotator)
        loaded_record = MARCExporter.create_record(work, annotator)
        assert record.as_marc() == loaded_record.as_marc()

        # Loads a existing record from the DB
        new_work = get_one(db.session, Work, id=work.id)
        new_record = MARCExporter.create_record(new_work, annotator)
        assert record.as_marc() == new_record.as_marc()

    @pytest.mark.parametrize("object_type", ["lane", "worklist"])
    def test_records_lane(
        self,
        object_type: str,
        db: DatabaseTransactionFixture,
        s3_service_fixture: S3ServiceFixture,
        marc_exporter_fixture: MarcExporterFixture,
    ):
        if object_type == "lane":
            lane_or_wl = db.lane("Test Lane", genres=["Mystery"])
        elif object_type == "worklist":
            lane_or_wl = WorkList()
            lane_or_wl.initialize(db.default_library(), display_name="All Books")
        else:
            raise RuntimeError()
        exporter = marc_exporter_fixture.exporter
        annotator = marc_exporter_fixture.annotator
        search_engine = marc_exporter_fixture.search_engine

        # If there's a storage protocol but not corresponding storage integration,
        # it raises an exception.
        pytest.raises(Exception, exporter.records, lane_or_wl, annotator)

        storage_service = s3_service_fixture.mock_service()
        exporter.records(
            lane_or_wl,
            annotator,
            storage_service,
            query_batch_size=1,
            search_engine=search_engine,
        )

        # The file was mirrored and a CachedMARCFile was created to track the mirrored file.
        assert len(storage_service.uploads) == 1
        [cache] = db.session.query(CachedMARCFile).all()
        assert cache.library == db.default_library()
        if object_type == "lane":
            assert cache.lane == lane_or_wl
        else:
            assert cache.lane is None
        assert cache.representation.content is None
        assert storage_service.uploads[0].key == "{}/{}/{}.mrc".format(
            db.default_library().short_name,
            str(cache.representation.fetched_at),
            lane_or_wl.display_name,
        )
        assert quote(storage_service.uploads[0].key) in cache.representation.mirror_url
        assert cache.start_time is None
        assert marc_exporter_fixture.now < cache.end_time

        records = list(MARCReader(storage_service.uploads[0].content))
        assert len(records) == 2

        title_fields = [record.get_fields("245") for record in records]
        titles = [fields[0].get_subfields("a")[0] for fields in title_fields]
        assert set(titles) == {
            marc_exporter_fixture.w1.title,
            marc_exporter_fixture.w2.title,
        }

        assert marc_exporter_fixture.w1.title in marc_exporter_fixture.w1.marc_record
        assert marc_exporter_fixture.w2.title in marc_exporter_fixture.w2.marc_record

    def test_records_start_time(
        self,
        db: DatabaseTransactionFixture,
        s3_service_fixture: S3ServiceFixture,
        marc_exporter_fixture: MarcExporterFixture,
    ):
        # If a start time is set, it's used in the mirror url.
        #
        # (Our mock search engine returns everthing in its 'index',
        # so this doesn't test that the start time is actually used to
        # find works -- that's in the search index tests and the
        # tests of MARCExporterFacets.)
        start_time = marc_exporter_fixture.now - datetime.timedelta(days=3)
        exporter = marc_exporter_fixture.exporter
        annotator = marc_exporter_fixture.annotator
        search_engine = marc_exporter_fixture.search_engine
        lane = db.lane("Test Lane", genres=["Mystery"])
        storage_service = s3_service_fixture.mock_service()

        exporter.records(
            lane,
            annotator,
            storage_service,
            start_time=start_time,
            query_batch_size=2,
            search_engine=search_engine,
        )
        [cache] = db.session.query(CachedMARCFile).all()

        assert cache.library == db.default_library()
        assert cache.lane == lane
        assert cache.representation.content is None
        assert storage_service.uploads[0].key == "{}/{}-{}/{}.mrc".format(
            db.default_library().short_name,
            str(start_time),
            str(cache.representation.fetched_at),
            lane.display_name,
        )
        assert cache.start_time == start_time
        assert marc_exporter_fixture.now < cache.end_time

    def test_records_empty_search(
        self,
        db: DatabaseTransactionFixture,
        s3_service_fixture: S3ServiceFixture,
        marc_exporter_fixture: MarcExporterFixture,
    ):
        # If the search engine returns no contents for the lane,
        # nothing will be mirrored, but a CachedMARCFile is still
        # created to track that we checked for updates.
        exporter = marc_exporter_fixture.exporter
        annotator = marc_exporter_fixture.annotator
        empty_search_engine = ExternalSearchIndexFake(db.session)
        lane = db.lane("Test Lane", genres=["Mystery"])
        storage_service = s3_service_fixture.mock_service()

        exporter.records(
            lane,
            annotator,
            storage_service,
            search_engine=empty_search_engine,
        )

        assert [] == storage_service.uploads
        [cache] = db.session.query(CachedMARCFile).all()
        assert cache.library == db.default_library()
        assert cache.lane == lane
        assert cache.representation.content is None
        assert cache.start_time is None
        assert marc_exporter_fixture.now < cache.end_time

    def test_records_minimum_size(
        self,
        db: DatabaseTransactionFixture,
        s3_service_fixture: S3ServiceFixture,
        marc_exporter_fixture: MarcExporterFixture,
    ):
        lane = db.lane(genres=["Mystery"])
        storage_service = s3_service_fixture.mock_service()
        exporter = marc_exporter_fixture.exporter
        annotator = marc_exporter_fixture.annotator
        search_engine = marc_exporter_fixture.search_engine

        # Make sure we page exactly how many times we need to
        works = [
            db.work(genre="Mystery", with_open_access_download=True) for _ in range(4)
        ]
        search_engine.mock_query_works(works)

        exporter.MINIMUM_UPLOAD_BATCH_SIZE_BYTES = 100
        # Mock the "records" generated, and force the response to be of certain sizes
        created_record_mock = MagicMock()
        created_record_mock.as_marc = MagicMock(
            side_effect=[b"1" * 600, b"2" * 20, b"3" * 500, b"4" * 10]
        )
        exporter.create_record = lambda *args: created_record_mock

        exporter.records(
            lane,
            annotator,
            storage_service,
            search_engine=search_engine,
            query_batch_size=1,
        )

        assert storage_service.mocked_multipart_upload is not None
        # Even though there are 4 parts, we upload in 3 batches due to minimum size limitations
        # The "4"th part gets uploaded due it being the tail piece
        assert len(storage_service.mocked_multipart_upload.content_parts) == 3
        assert storage_service.mocked_multipart_upload.content_parts == [
            b"1" * 600,
            b"2" * 20 + b"3" * 500,
            b"4" * 10,
        ]


class TestMARCExporterFacets:
    def test_modify_search_filter(self):
        # A facet object.
        facets = MARCExporterFacets("some start time")

        # A filter about to be modified by the facet object.
        filter = Filter()
        filter.order_ascending = False

        facets.modify_search_filter(filter)

        # updated_after has been set and results are to be returned in
        # order of increasing last_update_time.
        assert "last_update_time" == filter.order
        assert True == filter.order_ascending
        assert "some start time" == filter.updated_after

    def test_scoring_functions(self):
        # A no-op.
        facets = MARCExporterFacets("some start time")
        assert [] == facets.scoring_functions(object())
