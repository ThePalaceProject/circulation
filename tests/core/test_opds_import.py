import random
from functools import partial
from io import StringIO
from typing import Optional
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
import requests_mock
from lxml import etree
from psycopg2.extras import NumericRange

from api.circulation import CirculationAPI, FulfillmentInfo, LoanInfo
from api.circulation_exceptions import CurrentlyAvailable, FormatNotAvailable, NotOnHold
from api.saml.credential import SAMLCredentialManager
from api.saml.metadata.model import (
    SAMLAttributeStatement,
    SAMLNameID,
    SAMLNameIDFormat,
    SAMLSubject,
)
from core.config import IntegrationException
from core.coverage import CoverageFailure
from core.metadata_layer import CirculationData, LinkData, Metadata
from core.model import (
    Contributor,
    CoverageRecord,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    LicensePool,
    LicensePoolDeliveryMechanism,
    Measurement,
    MediaTypes,
    Representation,
    Resource,
    RightsStatus,
    Subject,
    Work,
    WorkCoverageRecord,
)
from core.opds_import import OPDSAPI, OPDSImporter, OPDSImportMonitor, OPDSXMLParser
from core.saml.wayfless import SAMLWAYFlessFulfillmentError
from core.util import first_or_default
from core.util.datetime_helpers import datetime_utc
from core.util.http import BadResponseException
from core.util.opds_writer import AtomFeed, OPDSFeed, OPDSMessage
from tests.core.mock import DummyHTTPClient
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.opds_files import OPDSFilesFixture


class DoomedOPDSImporter(OPDSImporter):
    def import_edition_from_metadata(self, metadata, *args):
        if metadata.title == "Johnny Crow's Party":
            # This import succeeds.
            return super().import_edition_from_metadata(metadata, *args)
        else:
            # Any other import fails.
            raise Exception("Utter failure!")


class DoomedWorkOPDSImporter(OPDSImporter):
    """An OPDS Importer that imports editions but can't create works."""

    def update_work_for_edition(self, edition, *args, **kwargs):
        if edition.title == "Johnny Crow's Party":
            # This import succeeds.
            return super().update_work_for_edition(edition, *args, **kwargs)
        else:
            # Any other import fails.
            raise Exception("Utter work failure!")


class OPDSImporterFixture:
    def __init__(
        self, db: DatabaseTransactionFixture, opds_files_fixture: OPDSFilesFixture
    ):
        self.db = db
        self.content_server_feed = opds_files_fixture.sample_data("content_server.opds")
        self.content_server_mini_feed = opds_files_fixture.sample_text(
            "content_server_mini.opds"
        )
        self.audiobooks_opds = opds_files_fixture.sample_data("audiobooks.opds")
        self.wayfless_feed = opds_files_fixture.sample_data("wayfless.opds")
        self.feed_with_id_and_dcterms_identifier = opds_files_fixture.sample_data(
            "feed_with_id_and_dcterms_identifier.opds"
        )
        self.importer = partial(
            OPDSImporter, _db=self.db.session, collection=self.db.default_collection()
        )
        db.set_settings(
            db.default_collection().integration_configuration,
            "data_source",
            DataSource.OA_CONTENT_SERVER,
        )


@pytest.fixture()
def opds_importer_fixture(
    db: DatabaseTransactionFixture,
    opds_files_fixture: OPDSFilesFixture,
) -> OPDSImporterFixture:
    data = OPDSImporterFixture(db, opds_files_fixture)
    return data


class TestOPDSImporter:
    def test_constructor(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        # The default way of making HTTP requests is with
        # Representation.cautious_http_get.
        importer = opds_importer_fixture.importer()
        assert Representation.cautious_http_get == importer.http_get

        # But you can pass in anything you want.
        do_get = MagicMock()
        importer = OPDSImporter(
            session, collection=db.default_collection(), http_get=do_get
        )
        assert do_get == importer.http_get

    def test_data_source_autocreated(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        name = "New data source " + db.fresh_str()
        importer = opds_importer_fixture.importer(data_source_name=name)
        source1 = importer.data_source
        assert name == source1.name

    def test_extract_next_links(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        importer = opds_importer_fixture.importer()
        next_links = importer.extract_next_links(data.content_server_mini_feed)

        assert 1 == len(next_links)
        assert "http://localhost:5000/?after=327&size=100" == next_links[0]

    def test_extract_last_update_dates(
        self, opds_importer_fixture: OPDSImporterFixture
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        importer = opds_importer_fixture.importer()

        # This file has two <entry> tags and one <simplified:message> tag.
        # The <entry> tags have their last update dates extracted,
        # the message is ignored.
        last_update_dates = importer.extract_last_update_dates(
            data.content_server_mini_feed
        )

        assert 2 == len(last_update_dates)

        identifier1, updated1 = last_update_dates[0]
        identifier2, updated2 = last_update_dates[1]

        assert "urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441" == identifier1
        assert datetime_utc(2015, 1, 2, 16, 56, 40) == updated1

        assert "urn:librarysimplified.org/terms/id/Gutenberg%20ID/10557" == identifier2
        assert datetime_utc(2015, 1, 2, 16, 56, 40) == updated2

    def test_extract_last_update_dates_ignores_entries_with_no_update(
        self, opds_importer_fixture: OPDSImporterFixture
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        importer = opds_importer_fixture.importer()

        # Rename the <updated> and <published> tags in the content
        # server so they don't show up.
        content = data.content_server_mini_feed.replace("updated>", "irrelevant>")
        content = content.replace("published>", "irrelevant>")
        last_update_dates = importer.extract_last_update_dates(content)

        # No updated dates!
        assert [] == last_update_dates

    def test_extract_metadata(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        data_source_name = "Data source name " + db.fresh_str()
        importer = opds_importer_fixture.importer(data_source_name=data_source_name)
        metadata, failures = importer.extract_feed_data(data.content_server_mini_feed)

        m1 = metadata["http://www.gutenberg.org/ebooks/10441"]
        m2 = metadata["http://www.gutenberg.org/ebooks/10557"]
        c1 = metadata["http://www.gutenberg.org/ebooks/10441"]
        c2 = metadata["http://www.gutenberg.org/ebooks/10557"]

        assert "The Green Mouse" == m1.title
        assert "A Tale of Mousy Terror" == m1.subtitle

        assert data_source_name == m1._data_source
        assert data_source_name == m2._data_source
        assert data_source_name == c1._data_source
        assert data_source_name == c2._data_source

        [[failure]] = list(failures.values())
        assert isinstance(failure, CoverageFailure)
        assert (
            "202: I'm working to locate a source for this identifier."
            == failure.exception
        )

    def test_use_dcterm_identifier_as_id_with_id_and_dcterms_identifier(
        self, opds_importer_fixture: OPDSImporterFixture
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        collection_to_test = db.default_collection()
        collection_to_test.primary_identifier_source = (
            ExternalIntegration.DCTERMS_IDENTIFIER
        )
        importer = opds_importer_fixture.importer(collection=collection_to_test)

        metadata, failures = importer.extract_feed_data(
            data.feed_with_id_and_dcterms_identifier
        )

        # First book doesn't have <dcterms:identifier>, so <id> must be used as identifier
        book_1 = metadata.get("https://root.uri/1")
        assert book_1 is not None
        # Second book have <id> and <dcterms:identifier>, so <dcters:identifier> must be used as id
        book_2 = metadata.get("urn:isbn:9781468316438")
        assert book_2 is not None
        # Verify if id was add in the end of identifier
        book_2_identifiers = book_2.identifiers
        found = False
        for entry in book_2.identifiers:
            if entry.identifier == "https://root.uri/2":
                found = True
                break
        assert found is True
        # Third book has more than one dcterms:identifers, all of then must be present as metadata identifier
        book_3 = metadata.get("urn:isbn:9781683351993")
        assert book_3 is not None
        # Verify if id was add in the end of identifier
        book_3_identifiers = book_3.identifiers
        expected_identifier = [
            "9781683351993",
            "https://root.uri/3",
            "9781683351504",
            "9780312939458",
        ]
        result_identifier = [entry.identifier for entry in book_3.identifiers]
        assert set(expected_identifier) == set(result_identifier)

    def test_use_id_with_existing_dcterms_identifier(
        self, opds_importer_fixture: OPDSImporterFixture
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        collection_to_test = db.default_collection()
        collection_to_test.primary_identifier_source = None
        importer = opds_importer_fixture.importer(collection=collection_to_test)

        metadata, failures = importer.extract_feed_data(
            data.feed_with_id_and_dcterms_identifier
        )

        book_1 = metadata.get("https://root.uri/1")
        assert book_1 != None
        book_2 = metadata.get("https://root.uri/2")
        assert book_2 != None
        book_3 = metadata.get("https://root.uri/3")
        assert book_3 != None

    def test_extract_link(self):
        no_rel = AtomFeed.E.link(href="http://foo/")
        assert None == OPDSImporter.extract_link(no_rel)

        no_href = AtomFeed.E.link(href="", rel="foo")
        assert None == OPDSImporter.extract_link(no_href)

        good = AtomFeed.E.link(href="http://foo", rel="bar")
        link = OPDSImporter.extract_link(good)
        assert "http://foo" == link.href
        assert "bar" == link.rel

        relative = AtomFeed.E.link(href="/foo/bar", rel="self")
        link = OPDSImporter.extract_link(relative, "http://server")
        assert "http://server/foo/bar" == link.href

    def test_get_medium_from_links(self):
        audio_links = [
            LinkData(
                href="url",
                rel="http://opds-spec.org/acquisition/",
                media_type="application/audiobook+json;param=value",
            ),
            LinkData(href="url", rel="http://opds-spec.org/image"),
        ]
        book_links = [
            LinkData(href="url", rel="http://opds-spec.org/image"),
            LinkData(
                href="url",
                rel="http://opds-spec.org/acquisition/",
                media_type=random.choice(MediaTypes.BOOK_MEDIA_TYPES) + ";param=value",
            ),
        ]

        m = OPDSImporter.get_medium_from_links

        assert m(audio_links) == "Audio"
        assert m(book_links) == "Book"

    def test_extract_link_rights_uri(self):
        # Most of the time, a link's rights URI is inherited from the entry.
        entry_rights = RightsStatus.PUBLIC_DOMAIN_USA

        link_tag = AtomFeed.E.link(href="http://foo", rel="bar")
        link = OPDSImporter.extract_link(link_tag, entry_rights_uri=entry_rights)
        assert RightsStatus.PUBLIC_DOMAIN_USA == link.rights_uri

        # But a dcterms:rights tag beneath the link can override this.
        rights_attr = "{%s}rights" % AtomFeed.DCTERMS_NS
        link_tag.attrib[rights_attr] = RightsStatus.IN_COPYRIGHT
        link = OPDSImporter.extract_link(link_tag, entry_rights_uri=entry_rights)
        assert RightsStatus.IN_COPYRIGHT == link.rights_uri

    def test_extract_data_from_feedparser(
        self, opds_importer_fixture: OPDSImporterFixture
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        data_source = DataSource.lookup(session, DataSource.OA_CONTENT_SERVER)
        importer = opds_importer_fixture.importer(data_source_name=data_source.name)
        values, failures = importer.extract_data_from_feedparser(
            data.content_server_mini_feed, data_source
        )

        # The <entry> tag became a Metadata object.
        metadata = values["urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441"]
        assert "The Green Mouse" == metadata["title"]
        assert "A Tale of Mousy Terror" == metadata["subtitle"]
        assert "en" == metadata["language"]
        assert "Project Gutenberg" == metadata["publisher"]

        circulation = metadata["circulation"]
        assert DataSource.GUTENBERG == circulation["data_source"]

        # The <simplified:message> tag did not become a
        # CoverageFailure -- that's handled by
        # extract_metadata_from_elementtree.
        assert {} == failures

    def test_extract_data_from_feedparser_handles_exception(
        self, opds_importer_fixture: OPDSImporterFixture
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        class DoomedFeedparserOPDSImporter(OPDSImporter):
            """An importer that can't extract metadata from feedparser."""

            @classmethod
            def _data_detail_for_feedparser_entry(cls, entry, data_source):
                raise Exception("Utter failure!")

        data_source = DataSource.lookup(session, DataSource.OA_CONTENT_SERVER)
        importer = DoomedFeedparserOPDSImporter(
            session, db.default_collection(), data_source_name=data_source.name
        )
        values, failures = importer.extract_data_from_feedparser(
            data.content_server_mini_feed, data_source
        )

        # No metadata was extracted.
        assert 0 == len(list(values.keys()))

        # There are 2 failures, both from exceptions. The 202 message
        # found in content_server_mini.opds is not extracted
        # here--it's extracted by extract_metadata_from_elementtree.
        assert 2 == len(failures)

        # The first error message became a CoverageFailure.
        failure = failures["urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441"]
        assert isinstance(failure, CoverageFailure)
        assert True == failure.transient
        assert "Utter failure!" in failure.exception

        # The second error message became a CoverageFailure.
        failure = failures["urn:librarysimplified.org/terms/id/Gutenberg%20ID/10557"]
        assert isinstance(failure, CoverageFailure)
        assert True == failure.transient
        assert "Utter failure!" in failure.exception

    def test_extract_metadata_from_elementtree(
        self, opds_importer_fixture: OPDSImporterFixture
    ):
        fixture, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        data_source = DataSource.lookup(session, DataSource.OA_CONTENT_SERVER)

        data, failures = OPDSImporter.extract_metadata_from_elementtree(
            fixture.content_server_feed, data_source
        )

        # There are 76 entries in the feed, and we got metadata for
        # every one of them.
        assert 76 == len(data)
        assert 0 == len(failures)

        # We're going to do spot checks on a book and a periodical.

        # First, the book.
        book_id = "urn:librarysimplified.org/terms/id/Gutenberg%20ID/1022"
        book = data[book_id]
        assert Edition.BOOK_MEDIUM == book["medium"]

        [contributor] = book["contributors"]
        assert "Thoreau, Henry David" == contributor.sort_name
        assert [Contributor.AUTHOR_ROLE] == contributor.roles

        subjects = book["subjects"]
        assert ["LCSH", "LCSH", "LCSH", "LCC"] == [x.type for x in subjects]
        assert ["Essays", "Nature", "Walking", "PS"] == [x.identifier for x in subjects]
        assert [None, None, None, "American Literature"] == [
            x.name for x in book["subjects"]
        ]
        assert [1, 1, 1, 10] == [x.weight for x in book["subjects"]]

        assert [] == book["measurements"]

        assert datetime_utc(1862, 6, 1) == book["published"]

        [link] = book["links"]
        assert Hyperlink.OPEN_ACCESS_DOWNLOAD == link.rel
        assert "http://www.gutenberg.org/ebooks/1022.epub.noimages" == link.href
        assert Representation.EPUB_MEDIA_TYPE == link.media_type

        # And now, the periodical.
        periodical_id = "urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441"
        periodical = data[periodical_id]
        assert Edition.PERIODICAL_MEDIUM == periodical["medium"]

        subjects = periodical["subjects"]
        assert [
            "LCSH",
            "LCSH",
            "LCSH",
            "LCSH",
            "LCC",
            "schema:audience",
            "schema:typicalAgeRange",
        ] == [x.type for x in subjects]
        assert [
            "Courtship -- Fiction",
            "New York (N.Y.) -- Fiction",
            "Fantasy fiction",
            "Magic -- Fiction",
            "PZ",
            "Children",
            "7",
        ] == [x.identifier for x in subjects]
        assert [1, 1, 1, 1, 1, 1, 1] == [x.weight for x in subjects]

        r1, r2, r3 = periodical["measurements"]

        assert Measurement.QUALITY == r1.quantity_measured
        assert 0.3333 == r1.value
        assert 1 == r1.weight

        assert Measurement.RATING == r2.quantity_measured
        assert 0.6 == r2.value
        assert 1 == r2.weight

        assert Measurement.POPULARITY == r3.quantity_measured
        assert 0.25 == r3.value
        assert 1 == r3.weight

        assert "Animal Colors" == periodical["series"]
        assert "1" == periodical["series_position"]

        assert datetime_utc(1910, 1, 1) == periodical["published"]

    def test_extract_metadata_from_elementtree_treats_message_as_failure(
        self,
        opds_importer_fixture: OPDSImporterFixture,
        opds_files_fixture: OPDSFilesFixture,
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        data_source = DataSource.lookup(session, DataSource.OA_CONTENT_SERVER)

        feed = opds_files_fixture.sample_data("unrecognized_identifier.opds")
        values, failures = OPDSImporter.extract_metadata_from_elementtree(
            feed, data_source
        )

        # We have no Metadata objects and one CoverageFailure.
        assert {} == values

        # The CoverageFailure contains the information that was in a
        # <simplified:message> tag in unrecognized_identifier.opds.
        key = "http://www.gutenberg.org/ebooks/100"
        assert [key] == list(failures.keys())
        failure = failures[key]
        assert "404: I've never heard of this work." == failure.exception
        assert key == failure.obj.urn

    def test_extract_messages(self, opds_files_fixture: OPDSFilesFixture):
        parser = OPDSXMLParser()
        feed = opds_files_fixture.sample_text("unrecognized_identifier.opds")
        root = etree.parse(StringIO(feed))
        [message] = OPDSImporter.extract_messages(parser, root)
        assert "urn:librarysimplified.org/terms/id/Gutenberg ID/100" == message.urn
        assert 404 == message.status_code
        assert "I've never heard of this work." == message.message

    def test_extract_medium(self):
        m = OPDSImporter.extract_medium

        # No tag -- the default is used.
        assert "Default" == m(None, "Default")

        def medium(additional_type, format, default="Default"):
            # Make an <atom:entry> tag with the given tags.
            # Parse it and call extract_medium on it.
            entry = '<entry xmlns:schema="http://schema.org/" xmlns:dcterms="http://purl.org/dc/terms/"'
            if additional_type:
                entry += ' schema:additionalType="%s"' % additional_type
            entry += ">"
            if format:
                entry += "<dcterms:format>%s</dcterms:format>" % format
            entry += "</entry>"
            tag = etree.parse(StringIO(entry))
            return m(tag.getroot(), default=default)

        audio_type = random.choice(MediaTypes.AUDIOBOOK_MEDIA_TYPES) + ";param=value"
        ebook_type = random.choice(MediaTypes.BOOK_MEDIA_TYPES) + ";param=value"

        # schema:additionalType is checked first. If present, any
        # potentially contradictory information in dcterms:format is
        # ignored.
        assert Edition.AUDIO_MEDIUM == medium(
            "http://bib.schema.org/Audiobook", ebook_type
        )
        assert Edition.BOOK_MEDIUM == medium("http://schema.org/EBook", audio_type)

        # When schema:additionalType is missing or not useful, the
        # value of dcterms:format is mapped to a medium using
        # Edition.medium_from_media_type.
        assert Edition.AUDIO_MEDIUM == medium("something-else", audio_type)
        assert Edition.BOOK_MEDIUM == medium(None, ebook_type)

        # If both pieces of information are missing or useless, the
        # default is used.
        assert "Default" == medium(None, None)
        assert "Default" == medium("something-else", "image/jpeg")

    def test_handle_failure(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        axis_id = db.identifier(identifier_type=Identifier.AXIS_360_ID)
        axis_isbn = db.identifier(Identifier.ISBN, "9781453219539")
        importer = opds_importer_fixture.importer(
            data_source_name=DataSource.OA_CONTENT_SERVER,
        )

        # The simplest case -- an identifier associated with a
        # CoverageFailure. The Identifier and CoverageFailure are
        # returned as-is.
        input_failure = CoverageFailure(object(), "exception")

        urn = "urn:isbn:9781449358068"
        expect_identifier, ignore = Identifier.parse_urn(session, urn)
        identifier, output_failure = importer.handle_failure(urn, input_failure)
        assert expect_identifier == identifier
        assert input_failure == output_failure

        # A normal OPDSImporter would consider this a failure, but
        # because the 'failure' is an Identifier, not a
        # CoverageFailure, we're going to treat it as a success.
        identifier, not_a_failure = importer.handle_failure(
            "urn:isbn:9781449358068", db.identifier()
        )
        assert expect_identifier == identifier
        assert identifier == not_a_failure
        # Note that the 'failure' object retuned is the Identifier that
        # was passed in, not the Identifier that substituted as the 'failure'.
        # (In real usage, though, they should be the same.)

    def test_coveragefailure_from_message(
        self, opds_importer_fixture: OPDSImporterFixture
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        """Test all the different ways a <simplified:message> tag might
        become a CoverageFailure.
        """
        data_source = DataSource.lookup(session, DataSource.OA_CONTENT_SERVER)

        def f(*args):
            message = OPDSMessage(*args)
            return OPDSImporter.coveragefailure_from_message(data_source, message)

        # If the URN is invalid we can't create a CoverageFailure.
        invalid_urn = f("urnblah", "500", "description")
        assert invalid_urn == None

        identifier = db.identifier()

        # If the 'message' is that everything is fine, no CoverageFailure
        # is created.
        this_is_fine = f(identifier.urn, "200", "description")
        assert None == this_is_fine

        # Test the various ways the status code and message might be
        # transformed into CoverageFailure.exception.
        description_and_status_code = f(identifier.urn, "404", "description")
        assert "404: description" == description_and_status_code.exception
        assert identifier == description_and_status_code.obj

        description_only = f(identifier.urn, None, "description")
        assert "description" == description_only.exception

        status_code_only = f(identifier.urn, "404", None)
        assert "404" == status_code_only.exception

        no_information = f(identifier.urn, None, None)
        assert "No detail provided." == no_information.exception

    def test_extract_metadata_from_elementtree_handles_messages_that_become_identifiers(
        self, opds_importer_fixture: OPDSImporterFixture
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )
        not_a_failure = db.identifier()

        class MockOPDSImporter(OPDSImporter):
            @classmethod
            def coveragefailures_from_messages(
                cls, data_source, message, success_on_200=False
            ):
                """No matter what input we get, we act as though there were
                a single simplified:message tag in the OPDS feed, which we
                decided to treat as success rather than failure.
                """
                return [not_a_failure]

        data_source = DataSource.lookup(session, DataSource.OA_CONTENT_SERVER)

        values, failures = MockOPDSImporter.extract_metadata_from_elementtree(
            data.content_server_mini_feed, data_source
        )
        assert {not_a_failure.urn: not_a_failure} == failures

    def test_extract_metadata_from_elementtree_handles_exception(
        self, opds_importer_fixture: OPDSImporterFixture
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        class DoomedElementtreeOPDSImporter(OPDSImporter):
            """An importer that can't extract metadata from elementttree."""

            @classmethod
            def _detail_for_elementtree_entry(cls, *args, **kwargs):
                raise Exception("Utter failure!")

        data_source = DataSource.lookup(session, DataSource.OA_CONTENT_SERVER)

        (
            values,
            failures,
        ) = DoomedElementtreeOPDSImporter.extract_metadata_from_elementtree(
            data.content_server_mini_feed, data_source
        )

        # No metadata was extracted.
        assert 0 == len(list(values.keys()))

        # There are 3 CoverageFailures - every <entry> threw an
        # exception and the <simplified:message> indicated failure.
        assert 3 == len(failures)

        # The entry with the 202 message became an appropriate
        # CoverageFailure because its data was not extracted through
        # extract_metadata_from_elementtree.
        failure = failures["http://www.gutenberg.org/ebooks/1984"]
        assert isinstance(failure, CoverageFailure)
        assert True == failure.transient
        assert failure.exception.startswith("202")
        assert "Utter failure!" not in failure.exception

        # The other entries became generic CoverageFailures due to the failure
        # of extract_metadata_from_elementtree.
        failure = failures["urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441"]
        assert isinstance(failure, CoverageFailure)
        assert True == failure.transient
        assert "Utter failure!" in failure.exception

        failure = failures["urn:librarysimplified.org/terms/id/Gutenberg%20ID/10557"]
        assert isinstance(failure, CoverageFailure)
        assert True == failure.transient
        assert "Utter failure!" in failure.exception

    def test_import_exception_if_unable_to_parse_feed(
        self, opds_importer_fixture: OPDSImporterFixture
    ):
        feed = "I am not a feed."
        importer = opds_importer_fixture.importer()

        pytest.raises(etree.XMLSyntaxError, importer.import_from_feed, feed)

    def test_import(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )
        feed = data.content_server_mini_feed

        collection = db.default_collection()
        importer = opds_importer_fixture.importer(
            collection=collection, data_source_name=DataSource.METADATA_WRANGLER
        )
        imported_editions, pools, works, failures = importer.import_from_feed(feed)

        [crow, mouse] = sorted(imported_editions, key=lambda x: str(x.title))

        # Work was created for both books.
        assert crow.data_source.name == DataSource.METADATA_WRANGLER
        assert crow.work is not None
        assert crow.medium == Edition.BOOK_MEDIUM
        assert crow.license_pools[0].collection == db.default_collection()

        assert mouse.work is not None
        assert mouse.medium == Edition.PERIODICAL_MEDIUM

        # Four links have been added to the identifier of the 'mouse'
        # edition.
        acquisition, image, thumbnail, description = sorted(
            mouse.primary_identifier.links, key=lambda x: str(x.rel)
        )

        # A Representation was imported for the summary with known
        # content.
        description_rep = description.resource.representation
        assert b"This is a summary!" == description_rep.content
        assert Representation.TEXT_PLAIN == description_rep.media_type

        # A Representation was imported for the image with a media type
        # inferred from its URL.
        image_rep = image.resource.representation
        assert image_rep.url.endswith("_9.png")
        assert Representation.PNG_MEDIA_TYPE == image_rep.media_type

        # The thumbnail was imported similarly, and its representation
        # was marked as a thumbnail of the full-sized image.
        thumbnail_rep = thumbnail.resource.representation
        assert Representation.PNG_MEDIA_TYPE == thumbnail_rep.media_type
        assert image_rep == thumbnail_rep.thumbnail_of

        # Three links were added to the identifier of the 'crow' edition.
        broken_image, working_image, acquisition = sorted(
            crow.primary_identifier.links, key=lambda x: str(x.resource.url)
        )

        # Because these images did not have a specified media type or a
        # distinctive extension, and we have not actually retrieved
        # the URLs yet, we were not able to determine their media type,
        # so they have no associated Representation.
        assert broken_image.resource.url is not None
        assert broken_image.resource.url.endswith("/broken-cover-image")
        assert working_image.resource.url is not None
        assert working_image.resource.url.endswith("/working-cover-image")
        assert broken_image.resource.representation is None
        assert working_image.resource.representation is None

        # Three measurements have been added to the 'mouse' edition.
        popularity, quality, rating = sorted(
            (x for x in mouse.primary_identifier.measurements if x.is_most_recent),
            key=lambda x: str(x.quantity_measured),
        )

        assert DataSource.METADATA_WRANGLER == popularity.data_source.name
        assert Measurement.POPULARITY == popularity.quantity_measured
        assert 0.25 == popularity.value

        assert DataSource.METADATA_WRANGLER == quality.data_source.name
        assert Measurement.QUALITY == quality.quantity_measured
        assert 0.3333 == quality.value

        assert DataSource.METADATA_WRANGLER == rating.data_source.name
        assert Measurement.RATING == rating.quantity_measured
        assert 0.6 == rating.value

        seven, children, courtship, fantasy, pz, magic, new_york = sorted(
            mouse.primary_identifier.classifications, key=lambda x: str(x.subject.name)
        )

        pz_s = pz.subject
        assert "Juvenile Fiction" == pz_s.name
        assert "PZ" == pz_s.identifier

        new_york_s = new_york.subject
        assert "New York (N.Y.) -- Fiction" == new_york_s.name
        assert "sh2008108377" == new_york_s.identifier

        assert "7" == seven.subject.identifier
        assert 100 == seven.weight
        assert Subject.AGE_RANGE == seven.subject.type
        from core.classifier import Classifier

        classifier = Classifier.classifiers.get(seven.subject.type, None)
        classifier.classify(seven.subject)

        [crow_pool, mouse_pool] = sorted(
            pools, key=lambda x: x.presentation_edition.title
        )

        assert db.default_collection() == crow_pool.collection
        assert db.default_collection() == mouse_pool.collection
        assert crow_pool.work is not None
        assert mouse_pool.work is not None

        work = mouse_pool.work
        work.calculate_presentation()
        assert 0.4142 == round(work.quality, 4)
        assert Classifier.AUDIENCE_CHILDREN == work.audience
        assert NumericRange(7, 7, "[]") == work.target_age

        # Bonus: make sure that delivery mechanisms are set appropriately.
        [mech] = mouse_pool.delivery_mechanisms
        assert Representation.EPUB_MEDIA_TYPE == mech.delivery_mechanism.content_type
        assert DeliveryMechanism.NO_DRM == mech.delivery_mechanism.drm_scheme
        assert "http://www.gutenberg.org/ebooks/10441.epub.images" == mech.resource.url

        # If we import the same file again, we get the same list of Editions.
        imported_editions_2, pools_2, works_2, failures_2 = importer.import_from_feed(
            feed
        )
        assert imported_editions_2 == imported_editions

    def test_import_with_lendability(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )
        """Test that OPDS import creates Edition, LicensePool, and Work
        objects, as appropriate.
        """
        feed = data.content_server_mini_feed

        # This import will create Editions, but not LicensePools or
        # Works, because there is no Collection.
        importer = opds_importer_fixture.importer()
        imported_editions, pools, works, failures = importer.import_from_feed(feed)

        # Both editions were imported, because they were new.
        assert 2 == len(imported_editions)

        # And pools and works were created
        assert 2 == len(pools)
        assert 2 == len(works)

        # 1 error message, corresponding to the <simplified:message> tag
        # at the end of content_server_mini.opds.
        assert 1 == len(failures)

        # The pools have presentation editions.
        assert {"The Green Mouse", "Johnny Crow's Party"} == {
            x.presentation_edition.title for x in pools
        }

        # The information used to create the first LicensePool said
        # that the licensing authority is Project Gutenberg, so that's used
        # as the DataSource for the first LicensePool. The information used
        # to create the second LicensePool didn't include a data source,
        # so the source of the OPDS feed (the open-access content server)
        # was used.
        assert {DataSource.GUTENBERG, DataSource.OA_CONTENT_SERVER} == {
            pool.data_source.name for pool in pools
        }

    def test_import_with_unrecognized_distributor_creates_distributor(
        self,
        opds_importer_fixture: OPDSImporterFixture,
        opds_files_fixture: OPDSFilesFixture,
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )
        """We get a book from a previously unknown data source, with a license
        that comes from a second previously unknown data source. The
        book is imported and both DataSources are created.
        """
        feed = opds_files_fixture.sample_data("unrecognized_distributor.opds")
        DatabaseTransactionFixture.set_settings(
            db.default_collection().integration_configuration,
            "data_source",
            "some new source",
        )
        importer = opds_importer_fixture.importer()
        imported_editions, pools, works, failures = importer.import_from_feed(feed)
        assert {} == failures

        # We imported an Edition because there was metadata.
        [edition] = imported_editions
        new_data_source = edition.data_source
        assert "some new source" == new_data_source.name

        # We imported a LicensePool because there was an open-access
        # link, even though the ultimate source of the link was one
        # we'd never seen before.
        [pool] = pools
        assert "Unknown Source" == pool.data_source.name

        # From an Edition and a LicensePool we created a Work.
        assert 1 == len(works)

    def test_import_updates_metadata(
        self,
        opds_importer_fixture: OPDSImporterFixture,
        opds_files_fixture: OPDSFilesFixture,
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        feed = opds_files_fixture.sample_text("metadata_wrangler_overdrive.opds")

        edition, is_new = db.edition(
            DataSource.OVERDRIVE, Identifier.OVERDRIVE_ID, with_license_pool=True
        )
        [old_license_pool] = edition.license_pools
        old_license_pool.calculate_work()
        work = old_license_pool.work

        feed = feed.replace("{OVERDRIVE ID}", edition.primary_identifier.identifier)

        DatabaseTransactionFixture.set_settings(
            db.default_collection().integration_configuration,
            "data_source",
            DataSource.OVERDRIVE,
        )
        (
            imported_editions,
            imported_pools,
            imported_works,
            failures,
        ) = opds_importer_fixture.importer().import_from_feed(feed)

        # The edition we created has had its metadata updated.
        [new_edition] = imported_editions
        assert new_edition == edition
        assert "The Green Mouse" == new_edition.title
        assert DataSource.OVERDRIVE == new_edition.data_source.name

        # But the license pools have not changed.
        assert edition.license_pools == [old_license_pool]
        assert work.license_pools == [old_license_pool]

    def test_import_from_license_source(
        self, opds_importer_fixture: OPDSImporterFixture
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )
        # Instead of importing this data as though it came from the
        # metadata wrangler, let's import it as though it came from the
        # open-access content server.
        feed = data.content_server_mini_feed
        importer = opds_importer_fixture.importer()

        (
            imported_editions,
            imported_pools,
            imported_works,
            failures,
        ) = importer.import_from_feed(feed)

        # Two works have been created, because the content server
        # actually tells you how to get copies of these books.
        [crow, mouse] = sorted(imported_works, key=lambda x: x.title)

        # Each work has one license pool.
        [crow_pool] = crow.license_pools
        [mouse_pool] = mouse.license_pools

        # The OPDS importer sets the data source of the license pool
        # to Project Gutenberg, since that's the authority that grants
        # access to the book.
        assert DataSource.GUTENBERG == mouse_pool.data_source.name

        # But the license pool's presentation edition has a data
        # source associated with the Library Simplified open-access
        # content server, since that's where the metadata comes from.
        assert (
            DataSource.OA_CONTENT_SERVER
            == mouse_pool.presentation_edition.data_source.name
        )

        # Since the 'mouse' book came with an open-access link, the license
        # pool delivery mechanism has been marked as open access.
        assert True == mouse_pool.open_access
        assert (
            RightsStatus.GENERIC_OPEN_ACCESS
            == mouse_pool.delivery_mechanisms[0].rights_status.uri
        )

        # The 'mouse' work was marked presentation-ready immediately.
        assert True == mouse_pool.work.presentation_ready

        # The OPDS feed didn't actually say where the 'crow' book
        # comes from, but we did tell the importer to use the open access
        # content server as the data source, so both a Work and a LicensePool
        # were created, and their data source is the open access content server,
        # not Project Gutenberg.
        assert DataSource.OA_CONTENT_SERVER == crow_pool.data_source.name

    def test_import_from_feed_treats_message_as_failure(
        self,
        opds_importer_fixture: OPDSImporterFixture,
        opds_files_fixture: OPDSFilesFixture,
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )
        feed = opds_files_fixture.sample_data("unrecognized_identifier.opds")
        (
            imported_editions,
            imported_pools,
            imported_works,
            failures,
        ) = opds_importer_fixture.importer().import_from_feed(feed)

        [[failure]] = list(failures.values())
        assert isinstance(failure, CoverageFailure)
        assert True == failure.transient
        assert "404: I've never heard of this work." == failure.exception

    def test_import_edition_failure_becomes_coverage_failure(
        self, opds_importer_fixture: OPDSImporterFixture
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )
        # Make sure that an exception during import generates a
        # meaningful error message.

        feed = data.content_server_mini_feed
        imported_editions, pools, works, failures = DoomedOPDSImporter(
            session,
            collection=db.default_collection(),
        ).import_from_feed(feed)

        # Only one book was imported, the other failed.
        assert 1 == len(imported_editions)

        # The other failed to import, and became a CoverageFailure
        [failure] = failures["http://www.gutenberg.org/ebooks/10441"]
        assert isinstance(failure, CoverageFailure)
        assert False == failure.transient
        assert "Utter failure!" in failure.exception

    def test_import_work_failure_becomes_coverage_failure(
        self, opds_importer_fixture: OPDSImporterFixture
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )
        # Make sure that an exception while updating a work for an
        # imported edition generates a meaningful error message.

        feed = data.content_server_mini_feed
        DatabaseTransactionFixture.set_settings(
            db.default_collection().integration_configuration,
            "data_source",
            DataSource.OA_CONTENT_SERVER,
        )
        importer = DoomedWorkOPDSImporter(session, collection=db.default_collection())

        imported_editions, pools, works, failures = importer.import_from_feed(feed)

        # One work was created, the other failed.
        assert 1 == len(works)

        # There's an error message for the work that failed.
        [failure] = failures["http://www.gutenberg.org/ebooks/10441"]
        assert isinstance(failure, CoverageFailure)
        assert False == failure.transient
        assert "Utter work failure!" in failure.exception

    def test_consolidate_links(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )
        # If a link turns out to be a dud, consolidate_links()
        # gets rid of it.
        none_links = [None, None]
        assert [] == OPDSImporter.consolidate_links(none_links)

        links = [
            LinkData(href=db.fresh_url(), rel=rel, media_type="image/jpeg")
            for rel in [
                Hyperlink.OPEN_ACCESS_DOWNLOAD,
                Hyperlink.IMAGE,
                Hyperlink.THUMBNAIL_IMAGE,
                Hyperlink.OPEN_ACCESS_DOWNLOAD,
            ]
        ]
        old_link = links[2]
        links = OPDSImporter.consolidate_links(links)
        assert [
            Hyperlink.OPEN_ACCESS_DOWNLOAD,
            Hyperlink.IMAGE,
            Hyperlink.OPEN_ACCESS_DOWNLOAD,
        ] == [x.rel for x in links]
        link = links[1]
        assert old_link == link.thumbnail

        links = [
            LinkData(href=db.fresh_url(), rel=rel, media_type="image/jpeg")
            for rel in [
                Hyperlink.THUMBNAIL_IMAGE,
                Hyperlink.IMAGE,
                Hyperlink.THUMBNAIL_IMAGE,
                Hyperlink.IMAGE,
            ]
        ]
        t1, i1, t2, i2 = links
        links = OPDSImporter.consolidate_links(links)
        assert [Hyperlink.IMAGE, Hyperlink.IMAGE] == [x.rel for x in links]
        assert t1 == i1.thumbnail
        assert t2 == i2.thumbnail

        links = [
            LinkData(href=db.fresh_url(), rel=rel, media_type="image/jpeg")
            for rel in [Hyperlink.THUMBNAIL_IMAGE, Hyperlink.IMAGE, Hyperlink.IMAGE]
        ]
        t1, i1, i2 = links
        links = OPDSImporter.consolidate_links(links)
        assert [Hyperlink.IMAGE, Hyperlink.IMAGE] == [x.rel for x in links]
        assert t1 == i1.thumbnail
        assert None == i2.thumbnail

    def test_import_book_that_offers_no_license(
        self,
        opds_importer_fixture: OPDSImporterFixture,
        opds_files_fixture: OPDSFilesFixture,
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        feed = opds_files_fixture.sample_data("book_without_license.opds")
        importer = OPDSImporter(session, db.default_collection())
        (
            imported_editions,
            imported_pools,
            imported_works,
            failures,
        ) = importer.import_from_feed(feed)

        # We got an Edition for this book, but no LicensePool and no Work.
        [edition] = imported_editions
        assert "Howards End" == edition.title
        assert [] == imported_pools
        assert [] == imported_works

        # We were able to figure out the medium of the Edition
        # based on its <dcterms:format> tag.
        assert Edition.AUDIO_MEDIUM == edition.medium

    def test_update_work_for_edition_having_no_work(
        self, db: DatabaseTransactionFixture, opds_importer_fixture: OPDSImporterFixture
    ):
        # We have an Edition and a LicensePool but no Work.
        edition, lp = db.edition(with_license_pool=True)
        assert None == lp.work

        importer = opds_importer_fixture.importer()
        returned_pool, returned_work = importer.update_work_for_edition(edition)

        # We now have a presentation-ready work.
        work = lp.work
        assert True == work.presentation_ready

        # The return value of update_work_for_edition is the affected
        # LicensePool and Work.
        assert returned_pool == lp
        assert returned_work == work

        # That happened because LicensePool.calculate_work() was
        # called. But now that there's a presentation-ready work,
        # further presentation recalculation happens in the
        # background. Calling update_work_for_edition() will not
        # immediately call LicensePool.calculate_work().
        def explode():
            raise Exception("boom!")

        lp.calculate_work = explode
        importer.update_work_for_edition(edition)

    def test_update_work_for_edition_having_incomplete_work(
        self, db: DatabaseTransactionFixture, opds_importer_fixture: OPDSImporterFixture
    ):
        session = db.session

        # We have a work, but it's not presentation-ready because
        # the title is missing.
        work = db.work(with_license_pool=True)
        [pool] = work.license_pools
        edition = work.presentation_edition
        edition.title = None
        work.presentation_ready = False

        # Fortunately, new data has come in that includes a title.
        i = edition.primary_identifier
        new_edition = db.edition(
            data_source_name=DataSource.METADATA_WRANGLER,
            identifier_type=i.type,
            identifier_id=i.identifier,
            title="A working title",
        )

        importer = opds_importer_fixture.importer()
        returned_pool, returned_work = importer.update_work_for_edition(edition)
        assert returned_pool == pool
        assert returned_work == work

        # We now have a presentation-ready work.
        assert "A working title" == work.title
        assert True == work.presentation_ready

    def test_update_work_for_edition_having_presentation_ready_work(
        self, db: DatabaseTransactionFixture, opds_importer_fixture: OPDSImporterFixture
    ):
        session = db.session

        # We have a presentation-ready work.
        work = db.work(with_license_pool=True, title="The old title")
        edition = work.presentation_edition
        [pool] = work.license_pools

        # The work's presentation edition has been chosen.
        work.calculate_presentation()
        op = WorkCoverageRecord.CHOOSE_EDITION_OPERATION

        # But we're about to find out a new title for the book.
        i = edition.primary_identifier
        new_edition = db.edition(
            data_source_name=DataSource.LIBRARY_STAFF,
            identifier_type=i.type,
            identifier_id=i.identifier,
            title="A new title",
        )

        importer = opds_importer_fixture.importer()
        returned_pool, returned_work = importer.update_work_for_edition(new_edition)

        # The existing LicensePool and Work were returned.
        assert returned_pool == pool
        assert returned_work == work

        # The work is still presentation-ready.
        assert True == work.presentation_ready

    def test_update_work_for_edition_having_multiple_license_pools(
        self, db: DatabaseTransactionFixture, opds_importer_fixture: OPDSImporterFixture
    ):
        session = db.session

        # There are two collections with a LicensePool associated with
        # this Edition.
        edition, lp = db.edition(with_license_pool=True)
        collection2 = db.collection()
        lp2 = db.licensepool(edition=edition, collection=collection2)
        importer = opds_importer_fixture.importer()

        # Calling update_work_for_edition creates a Work and associates
        # it with the edition.
        assert None == edition.work
        importer.update_work_for_edition(edition)
        work = edition.work
        assert isinstance(work, Work)

        # Both LicensePools are associated with that work.
        assert lp.work == work
        assert lp2.work == work

    def test_assert_importable_content(self, db: DatabaseTransactionFixture):
        session = db.session
        collection = db.collection(
            protocol=ExternalIntegration.OPDS_IMPORT, data_source_name="OPDS"
        )

        class Mock(OPDSImporter):
            """An importer that may or may not be able to find
            real open-access content.
            """

            # Set this variable to control whether any open-access links
            # are "found" in the OPDS feed.
            open_access_links: Optional[list] = None

            extract_feed_data_called_with = None
            _is_open_access_link_called_with = []

            def extract_feed_data(self, feed, feed_url):
                # There's no need to return realistic metadata,
                # since _open_access_links is also mocked.
                self.extract_feed_data_called_with = (feed, feed_url)
                return {"some": "metadata"}, {}

            def _open_access_links(self, metadatas):
                self._open_access_links_called_with = metadatas
                yield from self.open_access_links

            def _is_open_access_link(self, url, type):
                self._is_open_access_link_called_with.append((url, type))
                return False

        class NoLinks(Mock):
            "Simulate an OPDS feed that contains no open-access links."
            open_access_links = []

        # We won't be making any HTTP requests, even simulated ones.
        do_get = MagicMock()

        # Here, there are no links at all.
        importer = NoLinks(session, collection, do_get)
        with pytest.raises(IntegrationException) as excinfo:
            importer.assert_importable_content("feed", "url")
        assert "No open-access links were found in the OPDS feed." in str(excinfo.value)

        # We extracted 'metadata' from the feed and URL.
        assert ("feed", "url") == importer.extract_feed_data_called_with

        # But there were no open-access links in the 'metadata',
        # so we had nothing to check.
        assert [] == importer._is_open_access_link_called_with

        oa = Hyperlink.OPEN_ACCESS_DOWNLOAD

        class BadLinks(Mock):
            """Simulate an OPDS feed that contains open-access links that
            don't actually work, because _is_open_access always returns False
            """

            open_access_links = [
                LinkData(href="url1", rel=oa, media_type="text/html"),
                LinkData(href="url2", rel=oa, media_type="application/json"),
                LinkData(
                    href="I won't be tested", rel=oa, media_type="application/json"
                ),
            ]

        bad_links_importer = BadLinks(session, collection, do_get)
        with pytest.raises(IntegrationException) as excinfo:
            bad_links_importer.assert_importable_content(
                "feed", "url", max_get_attempts=2
            )
        assert (
            "Was unable to GET supposedly open-access content such as url2 (tried 2 times)"
            in str(excinfo.value)
        )

        # We called _is_open_access_link on the first and second links
        # found in the 'metadata', but failed both times.
        #
        # We didn't bother with the third link because max_get_attempts was
        # set to 2.
        try1, try2 = bad_links_importer._is_open_access_link_called_with
        assert ("url1", "text/html") == try1
        assert ("url2", "application/json") == try2

        class GoodLink(Mock):
            """Simulate an OPDS feed that contains two bad open-access links
            and one good one.
            """

            _is_open_access_link_called_with = []
            open_access_links = [
                LinkData(href="bad", rel=oa, media_type="text/html"),
                LinkData(href="good", rel=oa, media_type="application/json"),
                LinkData(href="also bad", rel=oa, media_type="text/html"),
            ]

            def _is_open_access_link(self, url, type):
                self._is_open_access_link_called_with.append((url, type))
                if url == "bad":
                    return False
                return "this is a book"

        good_link_importer = GoodLink(session, collection, do_get)
        result = good_link_importer.assert_importable_content(
            "feed", "url", max_get_attempts=5
        )
        assert True == result

        # The first link didn't work, but the second one did,
        # so we didn't try the third one.
        try1, try2 = good_link_importer._is_open_access_link_called_with
        assert ("bad", "text/html") == try1
        assert ("good", "application/json") == try2

    def test__open_access_links(self, db: DatabaseTransactionFixture):
        session = db.session

        """Test our ability to find open-access links in Metadata objects."""
        m = OPDSImporter._open_access_links

        # No Metadata objects, no links.
        assert [] == list(m([]))

        # This Metadata has no associated CirculationData and will be
        # ignored.
        no_circulation = Metadata(DataSource.GUTENBERG)

        # This CirculationData has no open-access links, so it will be
        # ignored.
        circulation = CirculationData(DataSource.GUTENBERG, db.identifier())
        no_open_access_links = Metadata(DataSource.GUTENBERG, circulation=circulation)

        # This has three links, but only the open-access links
        # will be returned.
        circulation = CirculationData(DataSource.GUTENBERG, db.identifier())
        oa = Hyperlink.OPEN_ACCESS_DOWNLOAD
        for rel in [oa, Hyperlink.IMAGE, oa]:
            circulation.links.append(LinkData(href=db.fresh_url(), rel=rel))
        two_open_access_links = Metadata(DataSource.GUTENBERG, circulation=circulation)

        oa_only = [x for x in circulation.links if x.rel == oa]
        assert oa_only == list(
            m([no_circulation, two_open_access_links, no_open_access_links])
        )

    def test__is_open_access_link(
        self, db: DatabaseTransactionFixture, opds_importer_fixture: OPDSImporterFixture
    ):
        session = db.session
        http = DummyHTTPClient()

        # We only check that the response entity-body isn't tiny. 11
        # kilobytes of data is enough.
        enough_content = "a" * (1024 * 11)

        # Set up an HTTP response that looks enough like a book
        # to convince _is_open_access_link.
        http.queue_response(200, content=enough_content)
        monitor = opds_importer_fixture.importer(http_get=http.do_get)

        url = db.fresh_url()
        type = "text/html"
        assert "Found a book-like thing at %s" % url == monitor._is_open_access_link(
            url, type
        )

        # We made a GET request to the appropriate URL.
        assert url == http.requests.pop()

        # This HTTP response looks OK but it's not big enough to be
        # any kind of book.
        http.queue_response(200, content="not enough content")
        monitor = opds_importer_fixture.importer(http_get=http.do_get)
        assert False == monitor._is_open_access_link(url, None)

        # This HTTP response is clearly an error page.
        http.queue_response(404, content=enough_content)
        monitor = opds_importer_fixture.importer(http_get=http.do_get)
        assert False == monitor._is_open_access_link(url, None)

    def test_import_open_access_audiobook(
        self, opds_importer_fixture: OPDSImporterFixture
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        feed = data.audiobooks_opds
        download_manifest_url = "https://api.archivelab.org/books/kniga_zitij_svjatyh_na_mesjac_avgust_eu_0811_librivox/opds_audio_manifest"

        importer = OPDSImporter(
            session,
            collection=db.default_collection(),
        )

        (
            imported_editions,
            imported_pools,
            imported_works,
            failures,
        ) = importer.import_from_feed(feed)

        assert 1 == len(imported_editions)

        [august] = imported_editions
        assert "Zhitiia Sviatykh, v. 12 - August" == august.title

        [august_pool] = imported_pools
        assert True == august_pool.open_access
        assert download_manifest_url == august_pool._open_access_download_url

        [lpdm] = august_pool.delivery_mechanisms
        assert (
            Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE
            == lpdm.delivery_mechanism.content_type
        )
        assert DeliveryMechanism.NO_DRM == lpdm.delivery_mechanism.drm_scheme

    @pytest.fixture()
    def wayfless_circulation_api(self, opds_importer_fixture: OPDSImporterFixture):
        db, session = (
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        def _wayfless_circulation_api(
            has_saml_entity_id=True,
            has_saml_credential=True,
        ):
            idp_entityID = (
                "https://mycompany.com/adfs/services/trust"
                if has_saml_entity_id
                else None
            )

            feed = opds_importer_fixture.wayfless_feed
            library = db.library("Test library with SAML authentication", "SAML")
            patron = db.patron(library=library)
            saml_subject = SAMLSubject(
                idp_entityID,
                SAMLNameID(
                    SAMLNameIDFormat.PERSISTENT.value, "", "", "patron@university.com"
                ),
                SAMLAttributeStatement([]),
            )
            saml_credential_manager = SAMLCredentialManager()
            if has_saml_credential:
                saml_credential_manager.create_saml_token(session, patron, saml_subject)

            collection = db.collection(
                "OPDS collection with a WAYFless acquisition link",
                ExternalIntegration.OPDS_IMPORT,
                data_source_name="test",
            )
            library.collections.append(collection)

            DatabaseTransactionFixture.set_settings(
                collection.integration_configuration,
                "saml_wayfless_url_template",
                "https://fsso.springer.com/saml/login?idp={idp}&targetUrl={targetUrl}",
            )

            imported_editions, pools, works, failures = OPDSImporter(
                session, collection=collection
            ).import_from_feed(feed)

            pool = pools[0]
            pool.loan_to(patron)

            return CirculationAPI(session, library), patron, pool

        yield _wayfless_circulation_api

    def test_wayfless_url(self, wayfless_circulation_api):
        circulation, patron, pool = wayfless_circulation_api()
        fulfilment = circulation.fulfill(
            patron, "test", pool, first_or_default(pool.delivery_mechanisms)
        )
        assert (
            "https://fsso.springer.com/saml/login?idp=https%3A%2F%2Fmycompany.com%2Fadfs%2Fservices%2Ftrust&targetUrl=http%3A%2F%2Fwww.gutenberg.org%2Febooks%2F10441.epub.images"
            == fulfilment.content_link
        )

    def test_wayfless_url_no_saml_credential(self, wayfless_circulation_api):
        circulation, patron, pool = wayfless_circulation_api(has_saml_credential=False)
        with pytest.raises(SAMLWAYFlessFulfillmentError) as excinfo:
            circulation.fulfill(
                patron, "test", pool, first_or_default(pool.delivery_mechanisms)
            )
        assert str(excinfo.value).startswith(
            "There are no existing SAML credentials for patron"
        )

    def test_wayfless_url_no_saml_entity_id(self, wayfless_circulation_api):
        circulation, patron, pool = wayfless_circulation_api(has_saml_entity_id=False)
        with pytest.raises(SAMLWAYFlessFulfillmentError) as excinfo:
            circulation.fulfill(
                patron, "test", pool, first_or_default(pool.delivery_mechanisms)
            )
        assert str(excinfo.value).startswith("SAML subject")
        assert str(excinfo.value).endswith("does not contain an IdP's entityID")


class TestCombine:
    """Test that OPDSImporter.combine combines dictionaries in sensible
    ways.
    """

    def test_combine(self):
        """An overall test that duplicates a lot of functionality
        in the more specific tests.
        """
        d1 = dict(
            a_list=[1],
            a_scalar="old value",
            a_dict=dict(key1=None, key2=[2], key3="value3"),
        )

        d2 = dict(
            a_list=[2],
            a_scalar="new value",
            a_dict=dict(key1="finally a value", key4="value4", key2=[200]),
        )

        combined = OPDSImporter.combine(d1, d2)

        # Dictionaries get combined recursively.
        d = combined["a_dict"]

        # Normal scalar values can be overridden once set.
        assert "new value" == combined["a_scalar"]

        # Missing values are filled in.
        assert "finally a value" == d["key1"]
        assert "value3" == d["key3"]
        assert "value4" == d["key4"]

        # Lists get extended.
        assert [1, 2] == combined["a_list"]
        assert [2, 200] == d["key2"]

    def test_combine_null_cases(self):
        """Test combine()'s ability to handle empty and null dictionaries."""
        c = OPDSImporter.combine
        empty = dict()
        nonempty = dict(a=1)
        assert nonempty == c(empty, nonempty)
        assert empty == c(None, None)
        assert nonempty == c(nonempty, None)
        assert nonempty == c(None, nonempty)

    def test_combine_missing_value_is_replaced(self):
        c = OPDSImporter.combine
        a_is_missing = dict(b=None)
        a_is_present = dict(a=None, b=None)
        expect = dict(a=None, b=None)
        assert expect == c(a_is_missing, a_is_present)

        a_is_present["a"] = True
        expect = dict(a=True, b=None)
        assert expect == c(a_is_missing, a_is_present)

    def test_combine_present_value_replaced(self):
        """When both dictionaries define a scalar value, the second
        dictionary's value takes presedence.
        """
        c = OPDSImporter.combine
        a_is_true = dict(a=True)
        a_is_false = dict(a=False)
        assert a_is_false == c(a_is_true, a_is_false)
        assert a_is_true == c(a_is_false, a_is_true)

        a_is_old = dict(a="old value")
        a_is_new = dict(a="new value")
        assert "new value" == c(a_is_old, a_is_new)["a"]

    def test_combine_present_value_not_replaced_with_none(self):
        """When combining a dictionary where a key is set to None
        with a dictionary where that key is present, the value
        is left alone.
        """
        a_is_present = dict(a=True)
        a_is_none = dict(a=None, b=True)
        expect = dict(a=True, b=True)
        assert expect == OPDSImporter.combine(a_is_present, a_is_none)

    def test_combine_present_value_extends_list(self):
        """When both dictionaries define a list, the combined value
        is a combined list.
        """
        a_is_true = dict(a=[True])
        a_is_false = dict(a=[False])
        assert dict(a=[True, False]) == OPDSImporter.combine(a_is_true, a_is_false)

    def test_combine_present_value_extends_dictionary(self):
        """When both dictionaries define a dictionary, the combined value is
        the result of combining the two dictionaries with a recursive
        combine() call.
        """
        a_is_true = dict(a=dict(b=[True]))
        a_is_false = dict(a=dict(b=[False]))
        assert dict(a=dict(b=[True, False])) == OPDSImporter.combine(
            a_is_true, a_is_false
        )


class TestOPDSImportMonitor:
    def test_constructor(self, db: DatabaseTransactionFixture):
        session = db.session

        with pytest.raises(ValueError) as excinfo:
            OPDSImportMonitor(session, None, OPDSImporter)  # type: ignore[arg-type]
        assert (
            "OPDSImportMonitor can only be run in the context of a Collection."
            in str(excinfo.value)
        )

        db.default_collection().integration_configuration.protocol = (
            ExternalIntegration.OVERDRIVE
        )
        with pytest.raises(ValueError) as excinfo:
            OPDSImportMonitor(session, db.default_collection(), OPDSImporter)
        assert (
            "Collection Default Collection is configured for protocol Overdrive, not OPDS Import."
            in str(excinfo.value)
        )

        db.default_collection().integration_configuration.protocol = (
            ExternalIntegration.OPDS_IMPORT
        )
        DatabaseTransactionFixture.set_settings(
            db.default_collection().integration_configuration, "data_source", None
        )
        with pytest.raises(ValueError) as excinfo:
            OPDSImportMonitor(session, db.default_collection(), OPDSImporter)
        assert "Collection Default Collection has no associated data source." in str(
            excinfo.value
        )

        DatabaseTransactionFixture.set_settings(
            db.default_collection().integration_configuration, "data_source", "OPDS"
        )
        db.default_collection().external_account_id = (
            "https://opds.import.com/feed?size=100"
        )
        monitor = OPDSImportMonitor(session, db.default_collection(), OPDSImporter)
        assert monitor._feed_base_url == "https://opds.import.com/"

    def test_get(self, db: DatabaseTransactionFixture):
        session = db.session

        ## Test whether relative urls work
        DatabaseTransactionFixture.set_settings(
            db.default_collection().integration_configuration, "data_source", "OPDS"
        )
        db.default_collection().external_account_id = (
            "https://opds.import.com:9999/feed"
        )
        monitor = OPDSImportMonitor(session, db.default_collection(), OPDSImporter)

        with patch("core.opds_import.HTTP.get_with_timeout") as mock_get:
            monitor._get("/absolute/path", {})
            assert mock_get.call_args[0] == (
                "https://opds.import.com:9999/absolute/path",
            )

            mock_get.reset_mock()
            monitor._get("relative/path", {})
            assert mock_get.call_args[0] == (
                "https://opds.import.com:9999/relative/path",
            )

    def test_external_integration(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        monitor = OPDSImportMonitor(
            session,
            db.default_collection(),
            import_class=OPDSImporter,
        )
        assert (
            db.default_collection().external_integration
            == monitor.external_integration(session)
        )

    def test__run_self_tests(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )
        """Verify the self-tests of an OPDS collection."""

        class MockImporter(OPDSImporter):
            def assert_importable_content(self, content, url):
                self.assert_importable_content_called_with = (content, url)
                return "looks good"

        class Mock(OPDSImportMonitor):
            follow_one_link_called_with = []

            # First we will get the first page of the OPDS feed.
            def follow_one_link(self, url):
                self.follow_one_link_called_with.append(url)
                return ([], "some content")

        feed_url = db.fresh_url()
        db.default_collection().external_account_id = feed_url
        monitor = Mock(session, db.default_collection(), import_class=MockImporter)
        [first_page, found_content] = monitor._run_self_tests(session)
        expect = "Retrieve the first page of the OPDS feed (%s)" % feed_url
        assert expect == first_page.name
        assert True == first_page.success
        assert ([], "some content") == first_page.result

        # follow_one_link was called once.
        [link] = monitor.follow_one_link_called_with
        assert monitor.feed_url == link

        # Then, assert_importable_content was called on the importer.
        assert "Checking for importable content" == found_content.name
        assert True == found_content.success
        assert (
            "some content",
            feed_url,
        ) == monitor.importer.assert_importable_content_called_with  # type: ignore[attr-defined]
        assert "looks good" == found_content.result

    def test_hook_methods(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )
        """By default, the OPDS URL and data source used by the importer
        come from the collection configuration.
        """
        monitor = OPDSImportMonitor(
            session,
            db.default_collection(),
            import_class=OPDSImporter,
        )
        assert db.default_collection().external_account_id == monitor.opds_url(
            db.default_collection()
        )

        assert db.default_collection().data_source == monitor.data_source(
            db.default_collection()
        )

    def test_feed_contains_new_data(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        feed = data.content_server_mini_feed

        class MockOPDSImportMonitor(OPDSImportMonitor):
            def _get(self, url, headers):
                return 200, {"content-type": AtomFeed.ATOM_TYPE}, feed

        monitor = OPDSImportMonitor(
            session,
            db.default_collection(),
            import_class=OPDSImporter,
        )
        timestamp = monitor.timestamp()

        # Nothing has been imported yet, so all data is new.
        assert True == monitor.feed_contains_new_data(feed)
        assert None == timestamp.start

        # Now import the editions.
        monitor = MockOPDSImportMonitor(
            session,
            collection=db.default_collection(),
            import_class=OPDSImporter,
        )
        monitor.run()

        # Editions have been imported.
        assert 2 == session.query(Edition).count()

        # The timestamp has been updated, although unlike most
        # Monitors the timestamp is purely informational.
        assert timestamp.finish != None

        editions = session.query(Edition).all()
        data_source = DataSource.lookup(session, DataSource.OA_CONTENT_SERVER)

        # If there are CoverageRecords that record work are after the updated
        # dates, there's nothing new.
        record, ignore = CoverageRecord.add_for(
            editions[0],
            data_source,
            CoverageRecord.IMPORT_OPERATION,
            collection=db.default_collection(),
        )
        record.timestamp = datetime_utc(2016, 1, 1, 1, 1, 1)

        record2, ignore = CoverageRecord.add_for(
            editions[1],
            data_source,
            CoverageRecord.IMPORT_OPERATION,
            collection=db.default_collection(),
        )
        record2.timestamp = datetime_utc(2016, 1, 1, 1, 1, 1)

        assert False == monitor.feed_contains_new_data(feed)

        # If the monitor is set up to force reimport, it doesn't
        # matter that there's nothing new--we act as though there is.
        monitor.force_reimport = True
        assert True == monitor.feed_contains_new_data(feed)
        monitor.force_reimport = False

        # If an entry was updated after the date given in that entry's
        # CoverageRecord, there's new data.
        record2.timestamp = datetime_utc(1970, 1, 1, 1, 1, 1)
        assert True == monitor.feed_contains_new_data(feed)

        # If a CoverageRecord is a transient failure, we try again
        # regardless of whether it's been updated.
        for r in [record, record2]:
            r.timestamp = datetime_utc(2016, 1, 1, 1, 1, 1)
            r.exception = "Failure!"
            r.status = CoverageRecord.TRANSIENT_FAILURE
        assert True == monitor.feed_contains_new_data(feed)

        # If a CoverageRecord is a persistent failure, we don't try again...
        for r in [record, record2]:
            r.status = CoverageRecord.PERSISTENT_FAILURE
        assert False == monitor.feed_contains_new_data(feed)

        # ...unless the feed updates.
        record.timestamp = datetime_utc(1970, 1, 1, 1, 1, 1)
        assert True == monitor.feed_contains_new_data(feed)

    def test_follow_one_link(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        monitor = OPDSImportMonitor(
            session,
            collection=db.default_collection(),
            import_class=OPDSImporter,
        )
        feed = data.content_server_mini_feed

        http = DummyHTTPClient()

        # If there's new data, follow_one_link extracts the next links.
        def follow():
            return monitor.follow_one_link("http://url", do_get=http.do_get)

        http.queue_response(200, OPDSFeed.ACQUISITION_FEED_TYPE, content=feed)
        next_links, content = follow()
        assert 1 == len(next_links)
        assert "http://localhost:5000/?after=327&size=100" == next_links[0]

        assert feed.encode("utf-8") == content

        # Now import the editions and add coverage records.
        monitor.importer.import_from_feed(feed)
        assert 2 == session.query(Edition).count()

        editions = session.query(Edition).all()
        data_source = DataSource.lookup(session, DataSource.OA_CONTENT_SERVER)

        for edition in editions:
            record, ignore = CoverageRecord.add_for(
                edition,
                data_source,
                CoverageRecord.IMPORT_OPERATION,
                collection=db.default_collection(),
            )
            record.timestamp = datetime_utc(2016, 1, 1, 1, 1, 1)

        # If there's no new data, follow_one_link returns no next
        # links and no content.
        #
        # Note that this works even when the media type is imprecisely
        # specified as Atom or bare XML.
        for imprecise_media_type in OPDSFeed.ATOM_LIKE_TYPES:
            http.queue_response(200, imprecise_media_type, content=feed)
            next_links, content = follow()
            assert 0 == len(next_links)
            assert None == content

        http.queue_response(200, AtomFeed.ATOM_TYPE, content=feed)
        next_links, content = follow()
        assert 0 == len(next_links)
        assert None == content

        # If the media type is missing or is not an Atom feed,
        # an exception is raised.
        http.queue_response(200, None, content=feed)
        with pytest.raises(BadResponseException) as excinfo:
            follow()
        assert "Expected Atom feed, got None" in str(excinfo.value)

        http.queue_response(200, "not/atom", content=feed)
        with pytest.raises(BadResponseException) as excinfo:
            follow()
        assert "Expected Atom feed, got not/atom" in str(excinfo.value)

    def test_import_one_feed(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )
        # Check coverage records are created.

        monitor = OPDSImportMonitor(
            session,
            collection=db.default_collection(),
            import_class=DoomedOPDSImporter,
        )
        db.default_collection().external_account_id = "http://root-url/index.xml"
        data_source = DataSource.lookup(session, DataSource.OA_CONTENT_SERVER)

        feed = data.content_server_mini_feed

        imported, failures = monitor.import_one_feed(feed)

        editions = session.query(Edition).all()

        # One edition has been imported
        assert 1 == len(editions)
        [edition] = editions

        # The return value of import_one_feed includes the imported
        # editions.
        assert [edition] == imported

        # That edition has a CoverageRecord.
        record = CoverageRecord.lookup(
            editions[0].primary_identifier,
            data_source,
            operation=CoverageRecord.IMPORT_OPERATION,
            collection=db.default_collection(),
        )
        assert CoverageRecord.SUCCESS == record.status
        assert None == record.exception

        # The edition's primary identifier has some cover links whose
        # relative URL have been resolved relative to the Collection's
        # external_account_id.
        covers = {
            x.resource.url
            for x in editions[0].primary_identifier.links
            if x.rel == Hyperlink.IMAGE
        }
        assert covers == {
            "http://root-url/broken-cover-image",
            "http://root-url/working-cover-image",
        }

        # The 202 status message in the feed caused a transient failure.
        # The exception caused a persistent failure.

        coverage_records = session.query(CoverageRecord).filter(
            CoverageRecord.operation == CoverageRecord.IMPORT_OPERATION,
            CoverageRecord.status != CoverageRecord.SUCCESS,
        )
        assert sorted(
            [CoverageRecord.TRANSIENT_FAILURE, CoverageRecord.PERSISTENT_FAILURE]
        ) == sorted(x.status for x in coverage_records)

        identifier, ignore = Identifier.parse_urn(
            session, "urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441"
        )
        failure = CoverageRecord.lookup(
            identifier,
            data_source,
            operation=CoverageRecord.IMPORT_OPERATION,
            collection=db.default_collection(),
        )
        assert "Utter failure!" in failure.exception

        # Both failures were reported in the return value from
        # import_one_feed
        assert 2 == len(failures)

    def test_run_once(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        class MockOPDSImportMonitor(OPDSImportMonitor):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.responses = []
                self.imports = []

            def queue_response(self, response):
                self.responses.append(response)

            def follow_one_link(self, link, cutoff_date=None, do_get=None):
                return self.responses.pop()

            def import_one_feed(self, feed):
                # Simulate two successes and one failure on every page.
                self.imports.append(feed)
                return [object(), object()], {"identifier": "Failure"}

        monitor = MockOPDSImportMonitor(
            session,
            collection=db.default_collection(),
            import_class=OPDSImporter,
        )

        monitor.queue_response([[], "last page"])
        monitor.queue_response([["second next link"], "second page"])
        monitor.queue_response([["next link"], "first page"])

        progress = monitor.run_once(MagicMock())

        # Feeds are imported in reverse order
        assert ["last page", "second page", "first page"] == monitor.imports

        # Every page of the import had two successes and one failure.
        assert "Items imported: 6. Failures: 3." == progress.achievements

        # The TimestampData returned by run_once does not include any
        # timing information; that's provided by run().
        assert None == progress.start
        assert None == progress.finish

    def test_update_headers(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        # Test the _update_headers helper method.
        monitor = OPDSImportMonitor(
            session,
            collection=db.default_collection(),
            import_class=OPDSImporter,
        )

        # _update_headers return a new dictionary. An Accept header will be setted
        # using the value of custom_accept_header. If the value is not set a
        # default value will be used.
        headers = {"Some other": "header"}
        new_headers = monitor._update_headers(headers)
        assert ["Some other"] == list(headers.keys())
        assert ["Accept", "Some other"] == sorted(list(new_headers.keys()))

        # If a custom_accept_header exist, will be used instead a default value
        new_headers = monitor._update_headers(headers)
        old_value = new_headers["Accept"]
        target_value = old_value + "more characters"
        monitor.custom_accept_header = target_value
        new_headers = monitor._update_headers(headers)
        assert new_headers["Accept"] == target_value
        assert old_value != target_value

        # If the monitor has a username and password, an Authorization
        # header using HTTP Basic Authentication is also added.
        monitor.username = "a user"
        monitor.password = "a password"
        headers = {}
        new_headers = monitor._update_headers(headers)
        assert new_headers["Authorization"].startswith("Basic")

        # However, if the Authorization and/or Accept headers have been
        # filled in by some other piece of code, _update_headers does
        # not touch them.
        expect = dict(Accept="text/html", Authorization="Bearer abc")
        headers = dict(expect)
        new_headers = monitor._update_headers(headers)
        assert headers == expect

    def test_retry(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        retry_count = 15
        feed = data.content_server_mini_feed
        feed_url = "https://example.com/feed.opds"

        # After we overrode the value of configuration setting we can instantiate OPDSImportMonitor.
        # It'll load new "Max retry count"'s value from the database.
        DatabaseTransactionFixture.set_settings(
            db.default_collection().integration_configuration,
            "connection_max_retry_count",
            retry_count,
        )
        monitor = OPDSImportMonitor(
            session,
            collection=db.default_collection(),
            import_class=OPDSImporter,
        )

        # We mock Retry class to ensure that the correct retry count had been passed.
        with patch("core.util.http.Retry") as retry_constructor_mock:
            with requests_mock.Mocker() as request_mock:
                request_mock.get(
                    feed_url,
                    text=feed,
                    status_code=200,
                    headers={"content-type": OPDSFeed.ACQUISITION_FEED_TYPE},
                )

                monitor.follow_one_link(feed_url)

                # Ensure that the correct retry count had been passed.
                retry_constructor_mock.assert_called_once_with(
                    total=retry_count,
                    status_forcelist=[429, 500, 502, 503, 504],
                    backoff_factor=1.0,
                )


class OPDSAPIFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.session = db.session
        self.collection = db.collection(
            protocol=OPDSAPI.label(), data_source_name="OPDS"
        )
        self.api = OPDSAPI(self.session, self.collection)

        self.mock_patron = MagicMock()
        self.mock_pin = MagicMock(spec=str)
        self.mock_licensepool = MagicMock(spec=LicensePool)
        self.mock_licensepool.collection = self.collection


@pytest.fixture
def opds_api_fixture(db: DatabaseTransactionFixture) -> OPDSAPIFixture:
    return OPDSAPIFixture(db)


class TestOPDSAPI:
    def test_checkin(self, opds_api_fixture: OPDSAPIFixture) -> None:
        # Make sure we can call checkin() without getting an exception.
        # The function is a no-op for this api, so we don't need to
        # test anything else.
        opds_api_fixture.api.checkin(
            opds_api_fixture.mock_patron,
            opds_api_fixture.mock_pin,
            opds_api_fixture.mock_licensepool,
        )

    def test_release_hold(self, opds_api_fixture: OPDSAPIFixture) -> None:
        # This api doesn't support holds. So we expect an exception.
        with pytest.raises(NotOnHold):
            opds_api_fixture.api.release_hold(
                opds_api_fixture.mock_patron,
                opds_api_fixture.mock_pin,
                opds_api_fixture.mock_licensepool,
            )

    def test_place_hold(self, opds_api_fixture: OPDSAPIFixture) -> None:
        # This api doesn't support holds. So we expect an exception.
        with pytest.raises(CurrentlyAvailable):
            opds_api_fixture.api.place_hold(
                opds_api_fixture.mock_patron,
                opds_api_fixture.mock_pin,
                opds_api_fixture.mock_licensepool,
                None,
            )

    def test_update_availability(self, opds_api_fixture: OPDSAPIFixture) -> None:
        # This function is a no-op since we already know the availability
        # of the license pool for any OPDS content. So we just make sure
        # we can call it without getting an exception.
        opds_api_fixture.api.update_availability(opds_api_fixture.mock_licensepool)

    def test_checkout(self, opds_api_fixture: OPDSAPIFixture) -> None:
        # Make sure checkout returns a LoanInfo object with the correct
        # collection id.
        mock_collection_property = PropertyMock(
            return_value=opds_api_fixture.collection
        )
        type(opds_api_fixture.mock_licensepool).collection = mock_collection_property
        delivery_mechanism = MagicMock(spec=LicensePoolDeliveryMechanism)
        loan = opds_api_fixture.api.checkout(
            opds_api_fixture.mock_patron,
            opds_api_fixture.mock_pin,
            opds_api_fixture.mock_licensepool,
            delivery_mechanism,
        )
        assert isinstance(loan, LoanInfo)
        assert mock_collection_property.call_count == 1
        assert loan.collection_id == opds_api_fixture.collection.id

    def test_can_fulfill_without_loan(self, opds_api_fixture: OPDSAPIFixture) -> None:
        # This should always return True.
        mock_lpdm = MagicMock(spec=LicensePoolDeliveryMechanism)
        assert (
            opds_api_fixture.api.can_fulfill_without_loan(
                opds_api_fixture.mock_patron,
                opds_api_fixture.mock_licensepool,
                mock_lpdm,
            )
            is True
        )

    def test_fulfill(self, opds_api_fixture: OPDSAPIFixture) -> None:
        # We only fulfill if the requested format matches an available format
        # for the license pool.
        mock_mechanism = MagicMock(spec=DeliveryMechanism)
        mock_lpdm = MagicMock(spec=LicensePoolDeliveryMechanism)
        mock_lpdm.delivery_mechanism = mock_mechanism

        # This license pool has no available formats.
        opds_api_fixture.mock_licensepool.delivery_mechanisms = []
        with pytest.raises(FormatNotAvailable):
            opds_api_fixture.api.fulfill(
                opds_api_fixture.mock_patron,
                opds_api_fixture.mock_pin,
                opds_api_fixture.mock_licensepool,
                mock_lpdm,
            )

        # This license pool has a delivery mechanism, but it's not the one
        # we're looking for.
        opds_api_fixture.mock_licensepool.delivery_mechanisms = [
            MagicMock(),
            MagicMock(),
        ]
        with pytest.raises(FormatNotAvailable):
            opds_api_fixture.api.fulfill(
                opds_api_fixture.mock_patron,
                opds_api_fixture.mock_pin,
                opds_api_fixture.mock_licensepool,
                mock_lpdm,
            )

        # This license pool has the delivery mechanism we're looking for, but
        # it does not have a resource.
        mock_lpdm.resource = None
        opds_api_fixture.mock_licensepool.delivery_mechanisms = [mock_lpdm]
        with pytest.raises(FormatNotAvailable):
            opds_api_fixture.api.fulfill(
                opds_api_fixture.mock_patron,
                opds_api_fixture.mock_pin,
                opds_api_fixture.mock_licensepool,
                mock_lpdm,
            )

        # This license pool has the delivery mechanism we're looking for, and
        # it has a resource, but the resource doesn't have a representation.
        mock_lpdm.resource = MagicMock(spec=Resource)
        mock_lpdm.resource.representation = None
        opds_api_fixture.mock_licensepool.delivery_mechanisms = [mock_lpdm]
        with pytest.raises(FormatNotAvailable):
            opds_api_fixture.api.fulfill(
                opds_api_fixture.mock_patron,
                opds_api_fixture.mock_pin,
                opds_api_fixture.mock_licensepool,
                mock_lpdm,
            )

        # This license pool has the delivery mechanism we're looking for, and
        # it has a resource, the resource has a representation, but the
        # representation doesn't have a URL.
        mock_lpdm.resource.representation = MagicMock(spec=Representation)
        mock_lpdm.resource.representation.public_url = None
        opds_api_fixture.mock_licensepool.delivery_mechanisms = [mock_lpdm]
        with pytest.raises(FormatNotAvailable):
            opds_api_fixture.api.fulfill(
                opds_api_fixture.mock_patron,
                opds_api_fixture.mock_pin,
                opds_api_fixture.mock_licensepool,
                mock_lpdm,
            )

        # This license pool has everything we need, so we can fulfill.
        mock_lpdm.resource.representation.public_url = "http://foo.com/bar.epub"
        opds_api_fixture.mock_licensepool.delivery_mechanisms = [
            MagicMock(),
            MagicMock(),
            mock_lpdm,
        ]
        fulfillment = opds_api_fixture.api.fulfill(
            opds_api_fixture.mock_patron,
            opds_api_fixture.mock_pin,
            opds_api_fixture.mock_licensepool,
            mock_lpdm,
        )
        assert isinstance(fulfillment, FulfillmentInfo)
        assert fulfillment.content_link == mock_lpdm.resource.representation.public_url
        assert fulfillment.content_type == mock_lpdm.resource.representation.media_type
        assert fulfillment.content is None
        assert fulfillment.content_expires is None
        assert fulfillment.collection_id == opds_api_fixture.collection.id
