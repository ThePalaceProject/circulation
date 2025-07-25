from __future__ import annotations

import functools
import urllib
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time
from pymarc import Indicators, MARCReader, Record

from palace.manager.integration.catalog.marc.annotator import Annotator
from palace.manager.sqlalchemy.model.classification import Genre
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.resource import Representation
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class AnnotatorFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self._db = db
        self.cm_url = "http://cm.url"
        self.short_name = "short_name"
        self.web_client_urls = ["http://webclient.url"]
        self.organization_name = "org"
        self.include_summary = True
        self.include_genres = True

        self.annotator = Annotator()

    @staticmethod
    def assert_control_field(record: Record, tag: str, expected: str) -> None:
        [field] = record.get_fields(tag)
        assert field.value() == expected

    @staticmethod
    def assert_field(
        record: Record,
        tag: str,
        expected_subfields: dict[str, str],
        expected_indicators: Indicators | None = None,
    ) -> None:
        if not expected_indicators:
            expected_indicators = Indicators(" ", " ")
        [field] = record.get_fields(tag)
        assert field.indicators == expected_indicators
        for subfield, value in expected_subfields.items():
            assert field.get_subfields(subfield)[0] == value

    @staticmethod
    def record_tags(record: Record) -> set[int]:
        return {int(f.tag) for f in record.fields}

    def assert_record_tags(
        self,
        record: Record,
        includes: set[int] | None = None,
        excludes: set[int] | None = None,
    ) -> None:
        tags = self.record_tags(record)
        assert includes or excludes
        if includes:
            assert includes.issubset(tags)
        if excludes:
            assert excludes.isdisjoint(tags)

    def record(self) -> Record:
        return self.annotator._record()

    def test_work(self) -> tuple[Work, LicensePool]:
        edition, pool = self._db.edition(
            with_license_pool=True, identifier_type=Identifier.ISBN
        )
        work = self._db.work(presentation_edition=edition)
        work.summary_text = "Summary"
        fantasy, ignore = Genre.lookup(self._db.session, "Fantasy", autocreate=True)
        romance, ignore = Genre.lookup(self._db.session, "Romance", autocreate=True)
        work.genres = [fantasy, romance]
        edition.issued = datetime_utc(956, 1, 1)
        edition.series = self._db.fresh_str()
        edition.series_position = 5
        return work, pool


@pytest.fixture
def annotator_fixture(
    db: DatabaseTransactionFixture,
) -> AnnotatorFixture:
    return AnnotatorFixture(db)


class TestAnnotator:
    def test_marc_record(
        self,
        db: DatabaseTransactionFixture,
        annotator_fixture: AnnotatorFixture,
    ) -> None:
        work, pool = annotator_fixture.test_work()
        annotator = annotator_fixture.annotator

        record = annotator.marc_record(work, pool.identifier, pool)
        assert annotator_fixture.record_tags(record) == {
            1,
            5,
            6,
            7,
            8,
            20,
            245,
            100,
            264,
            300,
            336,
            385,
            490,
            655,
            520,
            650,
            337,
            338,
            347,
            380,
        }

    def test__copy_record(self, annotator_fixture: AnnotatorFixture):
        work, pool = annotator_fixture.test_work()
        annotator = annotator_fixture.annotator
        record = annotator.marc_record(work, None, pool)
        copied = annotator_fixture.annotator._copy_record(record)
        assert copied is not record
        assert copied.as_marc() == record.as_marc()

    def test_library_marc_record(self, annotator_fixture: AnnotatorFixture):
        work, pool = annotator_fixture.test_work()
        annotator = annotator_fixture.annotator
        generic_record = annotator.marc_record(work, None, pool)

        library_marc_record = functools.partial(
            annotator.library_marc_record,
            record=generic_record,
            identifier=pool.identifier,
            base_url="http://cm.url",
            library_short_name="short_name",
            web_client_urls=["http://webclient.url"],
            organization_code="xyz",
            include_summary=True,
            include_genres=True,
            delta=False,
        )

        library_record = library_marc_record()
        annotator_fixture.assert_record_tags(
            library_record, includes={3, 520, 650, 856}
        )

        # Make sure the generic record did not get modified.
        assert generic_record != library_record
        assert generic_record.as_marc() != library_record.as_marc()
        annotator_fixture.assert_record_tags(generic_record, excludes={3, 856})

        # If the summary is not included, the 520 field is left out.
        library_record = library_marc_record(include_summary=False)
        annotator_fixture.assert_record_tags(
            library_record, includes={3, 650, 856}, excludes={520}
        )

        # If the genres are not included, the 650 field is left out.
        library_record = library_marc_record(include_genres=False)
        annotator_fixture.assert_record_tags(
            library_record, includes={3, 520, 856}, excludes={650}
        )

        # If the genres and summary are not included, the 520 and 650 fields are left out.
        library_record = library_marc_record(
            include_summary=False, include_genres=False
        )
        annotator_fixture.assert_record_tags(
            library_record, includes={3, 856}, excludes={520, 650}
        )

        # If the organization code is not provided, the 003 field is left out.
        library_record = library_marc_record(organization_code=None)
        annotator_fixture.assert_record_tags(
            library_record, includes={520, 650, 856}, excludes={3}
        )

        # If the web client URLs are not provided, the 856 fields are left out.
        library_record = library_marc_record(web_client_urls=[])
        annotator_fixture.assert_record_tags(
            library_record, includes={3, 520, 650}, excludes={856}
        )

        # If the record is part of a delta, then the flag is set
        library_record = library_marc_record(delta=False)
        assert library_record.leader.record_status == "n"

        library_record = library_marc_record(delta=True)
        assert library_record.leader.record_status == "c"

    def test_leader(self, annotator_fixture: AnnotatorFixture):
        leader = annotator_fixture.annotator.leader(False)
        assert leader == "00000nam  2200000   4500"

        # If the record is revised, the leader is different.
        leader = Annotator.leader(True)
        assert leader == "00000cam  2200000   4500"

    @freeze_time()
    def test_add_control_fields(
        self,
        db: DatabaseTransactionFixture,
        annotator_fixture: AnnotatorFixture,
    ):
        # This edition has one format and was published before 1900.
        edition, pool = db.edition(with_license_pool=True)
        identifier = pool.identifier
        edition.issued = datetime_utc(956, 1, 1)

        now = utc_now()
        record = annotator_fixture.record()

        annotator_fixture.annotator.add_control_fields(
            record, identifier, pool, edition
        )
        annotator_fixture.assert_control_field(record, "001", identifier.urn)
        assert now.strftime("%Y%m%d") in record.get_fields("005")[0].value()
        annotator_fixture.assert_control_field(record, "006", "m     o  d        ")
        annotator_fixture.assert_control_field(record, "007", "cr cn ---anuuu")
        annotator_fixture.assert_control_field(
            record, "008", now.strftime("%y%m%d") + "s0956    xxu     o     ||| ||eng d"
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

        record = annotator_fixture.record()
        annotator_fixture.annotator.add_control_fields(
            record, identifier2, pool2, edition2
        )
        annotator_fixture.assert_control_field(record, "001", identifier2.urn)
        assert now.strftime("%Y%m%d") in record.get_fields("005")[0].value()
        annotator_fixture.assert_control_field(record, "006", "m     o  d        ")
        annotator_fixture.assert_control_field(record, "007", "cr cn ---mnuuu")
        annotator_fixture.assert_control_field(
            record, "008", now.strftime("%y%m%d") + "s2018    xxu     o     ||| ||fre d"
        )

    def test_add_marc_organization_code(self, annotator_fixture: AnnotatorFixture):
        record = annotator_fixture.record()
        annotator_fixture.annotator.add_marc_organization_code(record, "US-MaBoDPL")
        annotator_fixture.assert_control_field(record, "003", "US-MaBoDPL")

    def test_add_isbn(
        self,
        db: DatabaseTransactionFixture,
        annotator_fixture: AnnotatorFixture,
    ):
        isbn = db.identifier(identifier_type=Identifier.ISBN)
        record = annotator_fixture.record()
        annotator_fixture.annotator.add_isbn(record, isbn)
        annotator_fixture.assert_field(record, "020", {"a": isbn.identifier})

        # If there is no ISBN, the field is left out.
        record = annotator_fixture.record()
        annotator_fixture.annotator.add_isbn(record, None)
        assert [] == record.get_fields("020")

    def test_add_title(
        self,
        db: DatabaseTransactionFixture,
        annotator_fixture: AnnotatorFixture,
    ):
        edition = db.edition()
        edition.title = "The Good Soldier"
        edition.sort_title = "Good Soldier, The"
        edition.subtitle = "A Tale of Passion"

        record = annotator_fixture.record()
        annotator_fixture.annotator.add_title(record, edition)
        assert len(record.get_fields("245")) == 1
        annotator_fixture.assert_field(
            record,
            "245",
            {
                "a": edition.title,
                "b": edition.subtitle,
                "c": edition.author,
            },
            Indicators("0", "4"),
        )

        # If there's no subtitle or no author, those subfields are left out.
        edition.subtitle = None
        edition.author = None

        record = annotator_fixture.record()
        annotator_fixture.annotator.add_title(record, edition)
        [field] = record.get_fields("245")
        annotator_fixture.assert_field(
            record,
            "245",
            {
                "a": edition.title,
            },
            Indicators("0", "4"),
        )
        assert [] == field.get_subfields("b")
        assert [] == field.get_subfields("c")

    def test_add_contributors(
        self,
        db: DatabaseTransactionFixture,
        annotator_fixture: AnnotatorFixture,
    ):
        author = "a"
        author2 = "b"
        translator = "c"

        # Edition with one author gets a 100 field and no 700 fields.
        edition = db.edition(authors=[author])
        edition.sort_author = "sorted"

        record = annotator_fixture.record()
        annotator_fixture.annotator.add_contributors(record, edition)
        assert [] == record.get_fields("700")
        annotator_fixture.assert_field(
            record, "100", {"a": edition.sort_author}, Indicators("1", " ")
        )

        # Edition with two authors and a translator gets three 700 fields and no 100 fields.
        edition = db.edition(authors=[author, author2])
        edition.add_contributor(translator, Contributor.Role.TRANSLATOR)

        record = annotator_fixture.record()
        annotator_fixture.annotator.add_contributors(record, edition)
        assert [] == record.get_fields("100")
        fields = record.get_fields("700")
        for field in fields:
            assert Indicators("1", " ") == field.indicators
        [author_field, author2_field, translator_field] = sorted(
            fields, key=lambda x: x.get_subfields("a")[0]
        )
        assert author == author_field.get_subfields("a")[0]
        assert Contributor.Role.PRIMARY_AUTHOR == author_field.get_subfields("e")[0]
        assert author2 == author2_field.get_subfields("a")[0]
        assert Contributor.Role.AUTHOR == author2_field.get_subfields("e")[0]
        assert translator == translator_field.get_subfields("a")[0]
        assert Contributor.Role.TRANSLATOR == translator_field.get_subfields("e")[0]

    def test_add_publisher(
        self,
        db: DatabaseTransactionFixture,
        annotator_fixture: AnnotatorFixture,
    ):
        edition = db.edition()
        edition.publisher = db.fresh_str()
        edition.issued = datetime_utc(1894, 4, 5)

        record = annotator_fixture.record()
        annotator_fixture.annotator.add_publisher(record, edition)
        annotator_fixture.assert_field(
            record,
            "264",
            {
                "a": "[Place of publication not identified]",
                "b": edition.publisher,
                "c": "1894",
            },
            Indicators(" ", "1"),
        )

        # If there's no publisher, the field is left out.
        record = annotator_fixture.record()
        edition.publisher = None
        annotator_fixture.annotator.add_publisher(record, edition)
        assert [] == record.get_fields("264")

    def test_add_distributor(
        self,
        db: DatabaseTransactionFixture,
        annotator_fixture: AnnotatorFixture,
    ):
        edition, pool = db.edition(with_license_pool=True)
        record = annotator_fixture.record()
        annotator_fixture.annotator.add_distributor(record, pool)
        annotator_fixture.assert_field(
            record, "264", {"b": pool.data_source.name}, Indicators(" ", "2")
        )

    def test_add_physical_description(
        self,
        db: DatabaseTransactionFixture,
        annotator_fixture: AnnotatorFixture,
    ):
        book = db.edition()
        book.medium = Edition.BOOK_MEDIUM
        audio = db.edition()
        audio.medium = Edition.AUDIO_MEDIUM

        record = annotator_fixture.record()
        annotator_fixture.annotator.add_physical_description(record, book)
        annotator_fixture.assert_field(record, "300", {"a": "1 online resource"})
        annotator_fixture.assert_field(
            record,
            "336",
            {
                "a": "text",
                "b": "txt",
                "2": "rdacontent",
            },
        )
        annotator_fixture.assert_field(
            record,
            "337",
            {
                "a": "computer",
                "b": "c",
                "2": "rdamedia",
            },
        )
        annotator_fixture.assert_field(
            record,
            "338",
            {
                "a": "online resource",
                "b": "cr",
                "2": "rdacarrier",
            },
        )
        annotator_fixture.assert_field(
            record,
            "347",
            {
                "a": "text file",
                "2": "rda",
            },
        )
        annotator_fixture.assert_field(
            record,
            "380",
            {
                "a": "eBook",
                "2": "tlcgt",
            },
        )

        record = annotator_fixture.record()
        annotator_fixture.annotator.add_physical_description(record, audio)
        annotator_fixture.assert_field(
            record,
            "300",
            {
                "a": "1 sound file",
                "b": "digital",
            },
        )
        annotator_fixture.assert_field(
            record,
            "336",
            {
                "a": "spoken word",
                "b": "spw",
                "2": "rdacontent",
            },
        )
        annotator_fixture.assert_field(
            record,
            "337",
            {
                "a": "computer",
                "b": "c",
                "2": "rdamedia",
            },
        )
        annotator_fixture.assert_field(
            record,
            "338",
            {
                "a": "online resource",
                "b": "cr",
                "2": "rdacarrier",
            },
        )
        annotator_fixture.assert_field(
            record,
            "347",
            {
                "a": "audio file",
                "2": "rda",
            },
        )
        assert [] == record.get_fields("380")

    def test_add_audience(
        self,
        db: DatabaseTransactionFixture,
        annotator_fixture: AnnotatorFixture,
    ):
        for audience, term in list(annotator_fixture.annotator.AUDIENCE_TERMS.items()):
            work = db.work(audience=audience)
            record = annotator_fixture.record()
            annotator_fixture.annotator.add_audience(record, work)
            annotator_fixture.assert_field(
                record,
                "385",
                {
                    "a": term,
                    "2": "marctarget",
                },
            )

    def test_add_series(
        self,
        db: DatabaseTransactionFixture,
        annotator_fixture: AnnotatorFixture,
    ):
        edition = db.edition()
        edition.series = db.fresh_str()
        edition.series_position = 5
        record = annotator_fixture.record()
        annotator_fixture.annotator.add_series(record, edition)
        annotator_fixture.assert_field(
            record,
            "490",
            {
                "a": edition.series,
                "v": str(edition.series_position),
            },
            Indicators("0", " "),
        )

        # If there's no series position, the same field is used without
        # the v subfield.
        edition.series_position = None
        record = annotator_fixture.record()
        annotator_fixture.annotator.add_series(record, edition)
        annotator_fixture.assert_field(
            record,
            "490",
            {
                "a": edition.series,
            },
            Indicators("0", " "),
        )
        [field] = record.get_fields("490")
        assert [] == field.get_subfields("v")

        # If there's no series, the field is left out.
        edition.series = None
        record = annotator_fixture.record()
        annotator_fixture.annotator.add_series(record, edition)
        assert [] == record.get_fields("490")

    def test_add_summary(
        self,
        db: DatabaseTransactionFixture,
        annotator_fixture: AnnotatorFixture,
    ):
        work = db.work(with_license_pool=True)
        work.summary_text = "<p>Summary</p>"

        # Build and validate a record with a `520|a` summary.
        record = annotator_fixture.record()
        annotator_fixture.annotator.add_summary(record, work)
        annotator_fixture.assert_field(record, "520", {"a": " Summary "})
        exported_record = record.as_marc()

        # Round trip the exported record to validate it.
        marc_reader = MARCReader(exported_record)
        round_tripped_record = next(marc_reader)
        annotator_fixture.assert_field(round_tripped_record, "520", {"a": " Summary "})

    def test_add_simplified_genres(
        self,
        db: DatabaseTransactionFixture,
        annotator_fixture: AnnotatorFixture,
    ):
        work = db.work(with_license_pool=True)
        fantasy, ignore = Genre.lookup(db.session, "Fantasy", autocreate=True)
        romance, ignore = Genre.lookup(db.session, "Romance", autocreate=True)
        work.genres = [fantasy, romance]

        record = annotator_fixture.record()
        annotator_fixture.annotator.add_genres(record, work)
        fields = record.get_fields("650")
        [fantasy_field, romance_field] = sorted(
            fields, key=lambda x: x.get_subfields("a")[0]
        )
        assert Indicators("0", "7") == fantasy_field.indicators
        assert "Fantasy" == fantasy_field.get_subfields("a")[0]
        assert "Library Simplified" == fantasy_field.get_subfields("2")[0]
        assert Indicators("0", "7") == romance_field.indicators
        assert "Romance" == romance_field.get_subfields("a")[0]
        assert "Library Simplified" == romance_field.get_subfields("2")[0]

    def test_add_ebooks_subject(self, annotator_fixture: AnnotatorFixture):
        record = annotator_fixture.record()
        annotator_fixture.annotator.add_ebooks_subject(record)
        annotator_fixture.assert_field(
            record, "655", {"a": "Electronic books."}, Indicators(" ", "0")
        )

    def test_add_web_client_urls_empty(self, annotator_fixture: AnnotatorFixture):
        record = MagicMock(spec=Record)
        identifier = MagicMock()
        annotator_fixture.annotator.add_web_client_urls(record, identifier, "", "", [])
        assert record.add_field.call_count == 0

    def test_add_web_client_urls(
        self,
        db: DatabaseTransactionFixture,
        annotator_fixture: AnnotatorFixture,
    ):
        record = annotator_fixture.record()
        identifier = db.identifier()
        short_name = "short_name"
        cm_url = "http://cm.url"
        web_client_urls = ["http://webclient1.url", "http://webclient2.url"]
        annotator_fixture.annotator.add_web_client_urls(
            record, identifier, short_name, cm_url, web_client_urls
        )
        fields = record.get_fields("856")
        assert len(fields) == 2
        [field1, field2] = fields
        assert field1.indicators == Indicators("4", "0")
        assert field2.indicators == Indicators("4", "0")

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
