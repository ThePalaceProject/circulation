from __future__ import annotations

import datetime
import functools
import logging
import urllib
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, create_autospec, patch

import pytest
from _pytest.logging import LogCaptureFixture
from pymarc import MARCReader, Record

from core.marc import Annotator, MARCExporter
from core.model import (
    Contributor,
    DataSource,
    DeliveryMechanism,
    Edition,
    Genre,
    Identifier,
    LicensePoolDeliveryMechanism,
    MarcFile,
    Representation,
    RightsStatus,
)
from core.util.datetime_helpers import datetime_utc, utc_now
from core.util.uuid import uuid_encode

if TYPE_CHECKING:
    from tests.fixtures.database import DatabaseTransactionFixture
    from tests.fixtures.s3 import MockS3Service, S3ServiceFixture


class AnnotateWorkRecordFixture:
    def __init__(self):
        self.cm_url = "http://cm.url"
        self.short_name = "short_name"
        self.web_client_urls = ["http://webclient.url"]
        self.organization_name = "org"
        self.include_summary = True
        self.include_genres = True

        self.annotator = Annotator(
            self.cm_url,
            self.short_name,
            self.web_client_urls,
            self.organization_name,
            self.include_summary,
            self.include_genres,
        )

        self.revised = MagicMock()
        self.work = MagicMock()
        self.pool = MagicMock()
        self.edition = MagicMock()
        self.identifier = MagicMock()

        self.mock_leader = create_autospec(self.annotator.leader, return_value=" " * 24)
        self.mock_add_control_fields = create_autospec(
            self.annotator.add_control_fields
        )
        self.mock_add_marc_organization_code = create_autospec(
            self.annotator.add_marc_organization_code
        )
        self.mock_add_isbn = create_autospec(self.annotator.add_isbn)
        self.mock_add_title = create_autospec(self.annotator.add_title)
        self.mock_add_contributors = create_autospec(self.annotator.add_contributors)
        self.mock_add_publisher = create_autospec(self.annotator.add_publisher)
        self.mock_add_distributor = create_autospec(self.annotator.add_distributor)
        self.mock_add_physical_description = create_autospec(
            self.annotator.add_physical_description
        )
        self.mock_add_audience = create_autospec(self.annotator.add_audience)
        self.mock_add_series = create_autospec(self.annotator.add_series)
        self.mock_add_system_details = create_autospec(
            self.annotator.add_system_details
        )
        self.mock_add_formats = create_autospec(self.annotator.add_formats)
        self.mock_add_summary = create_autospec(self.annotator.add_summary)
        self.mock_add_genres = create_autospec(self.annotator.add_genres)
        self.mock_add_ebooks_subject = create_autospec(
            self.annotator.add_ebooks_subject
        )
        self.mock_add_web_client_urls = create_autospec(
            self.annotator.add_web_client_urls
        )

        self.annotator.leader = self.mock_leader
        self.annotator.add_control_fields = self.mock_add_control_fields
        self.annotator.add_marc_organization_code = self.mock_add_marc_organization_code
        self.annotator.add_isbn = self.mock_add_isbn
        self.annotator.add_title = self.mock_add_title
        self.annotator.add_contributors = self.mock_add_contributors
        self.annotator.add_publisher = self.mock_add_publisher
        self.annotator.add_distributor = self.mock_add_distributor
        self.annotator.add_physical_description = self.mock_add_physical_description
        self.annotator.add_audience = self.mock_add_audience
        self.annotator.add_series = self.mock_add_series
        self.annotator.add_system_details = self.mock_add_system_details
        self.annotator.add_formats = self.mock_add_formats
        self.annotator.add_summary = self.mock_add_summary
        self.annotator.add_genres = self.mock_add_genres
        self.annotator.add_ebooks_subject = self.mock_add_ebooks_subject
        self.annotator.add_web_client_urls = self.mock_add_web_client_urls

        self.annotate_work_record = functools.partial(
            self.annotator.annotate_work_record,
            self.revised,
            self.work,
            self.pool,
            self.edition,
            self.identifier,
        )


@pytest.fixture
def annotate_work_record_fixture() -> AnnotateWorkRecordFixture:
    return AnnotateWorkRecordFixture()


class TestAnnotator:
    def test_annotate_work_record(
        self, annotate_work_record_fixture: AnnotateWorkRecordFixture
    ) -> None:
        fixture = annotate_work_record_fixture
        with patch("core.marc.Record") as mock_record:
            fixture.annotate_work_record()

        mock_record.assert_called_once_with(
            force_utf8=True, leader=fixture.mock_leader.return_value
        )
        fixture.mock_leader.assert_called_once_with(fixture.revised)
        record = mock_record()
        fixture.mock_add_control_fields.assert_called_once_with(
            record, fixture.identifier, fixture.pool, fixture.edition
        )
        fixture.mock_add_marc_organization_code.assert_called_once_with(
            record, fixture.organization_name
        )
        fixture.mock_add_isbn.assert_called_once_with(record, fixture.identifier)
        fixture.mock_add_title.assert_called_once_with(record, fixture.edition)
        fixture.mock_add_contributors.assert_called_once_with(record, fixture.edition)
        fixture.mock_add_publisher.assert_called_once_with(record, fixture.edition)
        fixture.mock_add_distributor.assert_called_once_with(record, fixture.pool)
        fixture.mock_add_physical_description.assert_called_once_with(
            record, fixture.edition
        )
        fixture.mock_add_audience.assert_called_once_with(record, fixture.work)
        fixture.mock_add_series.assert_called_once_with(record, fixture.edition)
        fixture.mock_add_system_details.assert_called_once_with(record)
        fixture.mock_add_formats.assert_called_once_with(record, fixture.pool)
        fixture.mock_add_summary.assert_called_once_with(record, fixture.work)
        fixture.mock_add_genres.assert_called_once_with(record, fixture.work)
        fixture.mock_add_ebooks_subject.assert_called_once_with(record)
        fixture.mock_add_web_client_urls.assert_called_once_with(
            record,
            fixture.identifier,
            fixture.short_name,
            fixture.cm_url,
            fixture.web_client_urls,
        )

    def test_annotate_work_record_no_summary(
        self, annotate_work_record_fixture: AnnotateWorkRecordFixture
    ) -> None:
        fixture = annotate_work_record_fixture
        fixture.annotator.include_summary = False
        fixture.annotate_work_record()

        assert fixture.mock_add_summary.call_count == 0

    def test_annotate_work_record_no_genres(
        self, annotate_work_record_fixture: AnnotateWorkRecordFixture
    ) -> None:
        fixture = annotate_work_record_fixture
        fixture.annotator.include_genres = False
        fixture.annotate_work_record()

        assert fixture.mock_add_genres.call_count == 0

    def test_annotate_work_record_no_organization_code(
        self, annotate_work_record_fixture: AnnotateWorkRecordFixture
    ) -> None:
        fixture = annotate_work_record_fixture
        fixture.annotator.organization_code = None
        fixture.annotate_work_record()

        assert fixture.mock_add_marc_organization_code.call_count == 0

    def test_leader(self):
        leader = Annotator.leader(False)
        assert leader == "00000nam  2200000   4500"

        # If the record is revised, the leader is different.
        leader = Annotator.leader(True)
        assert leader == "00000cam  2200000   4500"

    @staticmethod
    def _check_control_field(record, tag, expected):
        [field] = record.get_fields(tag)
        assert field.value() == expected

    @staticmethod
    def _check_field(record, tag, expected_subfields, expected_indicators=None):
        if not expected_indicators:
            expected_indicators = [" ", " "]
        [field] = record.get_fields(tag)
        assert field.indicators == expected_indicators
        for subfield, value in expected_subfields.items():
            assert field.get_subfields(subfield)[0] == value

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
        Annotator.add_genres(record, work)
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

    def test_add_web_client_urls_empty(self):
        record = MagicMock(spec=Record)
        identifier = MagicMock()
        Annotator.add_web_client_urls(record, identifier, "", "", [])
        assert record.add_field.call_count == 0

    def test_add_web_client_urls(self, db: DatabaseTransactionFixture):
        record = Record()
        identifier = db.identifier()
        short_name = "short_name"
        cm_url = "http://cm.url"
        web_client_urls = ["http://webclient1.url", "http://webclient2.url"]
        Annotator.add_web_client_urls(
            record, identifier, short_name, cm_url, web_client_urls
        )
        fields = record.get_fields("856")
        assert len(fields) == 2
        [field1, field2] = fields
        assert field1.indicators == ["4", "0"]
        assert field2.indicators == ["4", "0"]

        # The URL for a work is constructed as:
        # - <cm-base>/<lib-short-name>/works/<qualified-identifier>
        work_link_template = "{cm_base}/{lib}/works/{qid}"
        # It is then encoded and the web client URL is constructed in this form:
        # - <web-client-base>/book/<encoded-work-url>
        client_url_template = "{client_base}/book/{work_link}"

        qualified_identifier = urllib.parse.quote(
            identifier.type + "/" + identifier.identifier, safe=""
        )

        expected_work_link = work_link_template.format(
            cm_base=cm_url, lib=short_name, qid=qualified_identifier
        )
        encoded_work_link = urllib.parse.quote(expected_work_link, safe="")

        expected_client_url_1 = client_url_template.format(
            client_base=web_client_urls[0], work_link=encoded_work_link
        )
        expected_client_url_2 = client_url_template.format(
            client_base=web_client_urls[1], work_link=encoded_work_link
        )

        # A few checks to ensure that our setup is useful.
        assert web_client_urls[0] != web_client_urls[1]
        assert expected_client_url_1 != expected_client_url_2
        assert expected_client_url_1.startswith(web_client_urls[0])
        assert expected_client_url_2.startswith(web_client_urls[1])

        assert field1.get_subfields("u")[0] == expected_client_url_1
        assert field2.get_subfields("u")[0] == expected_client_url_2


class MarcExporterFixture:
    def __init__(self, db: DatabaseTransactionFixture, s3: MockS3Service):
        self.db = db

        self.now = utc_now()
        self.library = db.default_library()
        self.s3_service = s3
        self.exporter = MARCExporter(self.db.session, s3)
        self.mock_annotator = MagicMock(spec=Annotator)
        assert self.library.short_name is not None
        self.annotator = Annotator(
            "http://cm.url",
            self.library.short_name,
            ["http://webclient.url"],
            "org",
            True,
            True,
        )

        self.library = db.library()
        self.collection = db.collection()
        self.collection.libraries.append(self.library)

        self.now = utc_now()
        self.yesterday = self.now - datetime.timedelta(days=1)
        self.last_week = self.now - datetime.timedelta(days=7)

        self.w1 = db.work(
            genre="Mystery", with_open_access_download=True, collection=self.collection
        )
        self.w1.last_update_time = self.yesterday
        self.w2 = db.work(
            genre="Mystery", with_open_access_download=True, collection=self.collection
        )
        self.w2.last_update_time = self.last_week

        self.records = functools.partial(
            self.exporter.records,
            self.library,
            self.collection,
            annotator=self.annotator,
            creation_time=self.now,
        )


@pytest.fixture
def marc_exporter_fixture(
    db: DatabaseTransactionFixture,
    s3_service_fixture: S3ServiceFixture,
) -> MarcExporterFixture:
    return MarcExporterFixture(db, s3_service_fixture.mock_service())


class TestMARCExporter:
    def test_create_record(
        self, db: DatabaseTransactionFixture, marc_exporter_fixture: MarcExporterFixture
    ):
        work = db.work(
            with_license_pool=True,
            title="old title",
            authors=["old author"],
            data_source_name=DataSource.OVERDRIVE,
        )

        mock_revised = MagicMock()

        create_record = functools.partial(
            MARCExporter.create_record,
            revised=mock_revised,
            work=work,
            annotator=marc_exporter_fixture.mock_annotator,
        )

        record = create_record()
        assert record is not None

        # Make sure we pass the expected arguments to Annotator.annotate_work_record
        marc_exporter_fixture.mock_annotator.annotate_work_record.assert_called_once_with(
            mock_revised,
            work,
            work.license_pools[0],
            work.license_pools[0].presentation_edition,
            work.license_pools[0].identifier,
        )

    def test_records(
        self,
        db: DatabaseTransactionFixture,
        marc_exporter_fixture: MarcExporterFixture,
    ):
        storage_service = marc_exporter_fixture.s3_service
        creation_time = marc_exporter_fixture.now

        marc_exporter_fixture.records()

        # The file was mirrored and a MarcFile was created to track the mirrored file.
        assert len(storage_service.uploads) == 1
        [cache] = db.session.query(MarcFile).all()
        assert cache.library == marc_exporter_fixture.library
        assert cache.collection == marc_exporter_fixture.collection

        short_name = marc_exporter_fixture.library.short_name
        collection_name = marc_exporter_fixture.collection.name
        date_str = creation_time.strftime("%Y-%m-%d")
        uuid_str = uuid_encode(cache.id)

        assert (
            cache.key
            == f"marc/{short_name}/{collection_name}.full.{date_str}.{uuid_str}.mrc"
        )
        assert cache.created == creation_time
        assert cache.since is None

        records = list(MARCReader(storage_service.uploads[0].content))
        assert len(records) == 2

        title_fields = [record.get_fields("245") for record in records]
        titles = {fields[0].get_subfields("a")[0] for fields in title_fields}
        assert titles == {
            marc_exporter_fixture.w1.title,
            marc_exporter_fixture.w2.title,
        }

    def test_records_since_time(
        self,
        db: DatabaseTransactionFixture,
        marc_exporter_fixture: MarcExporterFixture,
    ):
        # If the `since` parameter is set, only works updated since that time
        # are included in the export and the filename reflects that we created
        # a partial export.
        since = marc_exporter_fixture.now - datetime.timedelta(days=3)
        storage_service = marc_exporter_fixture.s3_service
        creation_time = marc_exporter_fixture.now

        marc_exporter_fixture.records(
            since_time=since,
        )
        [cache] = db.session.query(MarcFile).all()
        assert cache.library == marc_exporter_fixture.library
        assert cache.collection == marc_exporter_fixture.collection

        short_name = marc_exporter_fixture.library.short_name
        collection_name = marc_exporter_fixture.collection.name
        from_date = since.strftime("%Y-%m-%d")
        to_date = creation_time.strftime("%Y-%m-%d")
        uuid_str = uuid_encode(cache.id)

        assert (
            cache.key
            == f"marc/{short_name}/{collection_name}.delta.{from_date}.{to_date}.{uuid_str}.mrc"
        )
        assert cache.created == creation_time
        assert cache.since == since

        # Only the work updated since the `since` time is included in the export.
        [record] = list(MARCReader(storage_service.uploads[0].content))
        [title_field] = record.get_fields("245")
        assert title_field.get_subfields("a")[0] == marc_exporter_fixture.w1.title

    def test_records_none(
        self,
        db: DatabaseTransactionFixture,
        marc_exporter_fixture: MarcExporterFixture,
        caplog: LogCaptureFixture,
    ):
        # If there are no works to export, no file is created and a log message is generated.
        caplog.set_level(logging.INFO)

        storage_service = marc_exporter_fixture.s3_service

        # Remove the works from the database.
        db.session.delete(marc_exporter_fixture.w1)
        db.session.delete(marc_exporter_fixture.w2)

        marc_exporter_fixture.records()

        assert [] == storage_service.uploads
        assert db.session.query(MarcFile).count() == 0
        assert len(caplog.records) == 1
        assert "No MARC records to upload" in caplog.text

    def test_records_exception(
        self,
        db: DatabaseTransactionFixture,
        marc_exporter_fixture: MarcExporterFixture,
        caplog: LogCaptureFixture,
    ):
        # If an exception occurs while exporting, no file is created and a log message is generated.
        caplog.set_level(logging.ERROR)

        exporter = marc_exporter_fixture.exporter
        storage_service = marc_exporter_fixture.s3_service

        # Mock our query function to raise an exception.
        exporter.query_works = MagicMock(side_effect=Exception("Boom!"))

        marc_exporter_fixture.records()

        assert [] == storage_service.uploads
        assert db.session.query(MarcFile).count() == 0
        assert len(caplog.records) == 1
        assert "Failed to upload MARC file" in caplog.text
        assert "Boom!" in caplog.text

    def test_records_minimum_size(
        self,
        marc_exporter_fixture: MarcExporterFixture,
    ):
        exporter = marc_exporter_fixture.exporter
        storage_service = marc_exporter_fixture.s3_service

        exporter.MINIMUM_UPLOAD_BATCH_SIZE_BYTES = 100

        # Mock the "records" generated, and force the response to be of certain sizes
        created_record_mock = MagicMock()
        created_record_mock.as_marc = MagicMock(
            side_effect=[b"1" * 600, b"2" * 20, b"3" * 500, b"4" * 10]
        )
        exporter.create_record = lambda *args: created_record_mock

        # Mock the query_works to return 4 works
        exporter.query_works = MagicMock(
            return_value=[MagicMock(), MagicMock(), MagicMock(), MagicMock()]
        )

        marc_exporter_fixture.records()

        assert storage_service.mocked_multipart_upload is not None
        # Even though there are 4 parts, we upload in 3 batches due to minimum size limitations
        # The "4"th part gets uploaded due it being the tail piece
        assert len(storage_service.mocked_multipart_upload.content_parts) == 3
        assert storage_service.mocked_multipart_upload.content_parts == [
            b"1" * 600,
            b"2" * 20 + b"3" * 500,
            b"4" * 10,
        ]
