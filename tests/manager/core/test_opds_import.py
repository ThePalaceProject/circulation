from __future__ import annotations

import random
from functools import partial
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest
import requests_mock
from lxml import etree
from psycopg2.extras import NumericRange

from palace.manager.api.circulation import CirculationAPI, LoanInfo, RedirectFulfillment
from palace.manager.api.circulation_exceptions import (
    CurrentlyAvailable,
    FormatNotAvailable,
    NotOnHold,
)
from palace.manager.api.overdrive.api import OverdriveAPI
from palace.manager.api.saml.credential import SAMLCredentialManager
from palace.manager.api.saml.metadata.model import (
    SAMLAttributeStatement,
    SAMLNameID,
    SAMLNameIDFormat,
    SAMLSubject,
)
from palace.manager.core.coverage import CoverageFailure
from palace.manager.core.metadata_layer import LinkData
from palace.manager.core.opds_import import (
    OPDSAPI,
    IdentifierSource,
    OPDSImporter,
    OPDSImportMonitor,
    OPDSXMLParser,
)
from palace.manager.integration.configuration.wayfless import (
    SAMLWAYFlessFulfillmentError,
)
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.coverage import CoverageRecord, WorkCoverageRecord
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation, Resource
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util import first_or_default
from palace.manager.util.datetime_helpers import datetime_utc
from palace.manager.util.http import BadResponseException
from palace.manager.util.opds_writer import AtomFeed, OPDSFeed, OPDSMessage
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import OPDSFilesFixture
from tests.mocks.mock import MockHTTPClient, MockRequestsResponse


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


@pytest.fixture()
def opds_importer_fixture(
    db: DatabaseTransactionFixture,
    opds_files_fixture: OPDSFilesFixture,
) -> OPDSImporterFixture:
    data = OPDSImporterFixture(db, opds_files_fixture)
    return data


class TestOPDSImporter:
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

        assert m1.circulation.should_track_playtime == True
        assert m2.circulation.should_track_playtime == False

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

        collection_to_test = db.collection(
            settings=db.opds_settings(
                external_account_id="http://root.uri",
                primary_identifier_source=IdentifierSource.DCTERMS_IDENTIFIER,
            )
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

        data_source = DataSource.lookup(
            session, DataSource.OA_CONTENT_SERVER, autocreate=True
        )
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

        data_source = DataSource.lookup(
            session, DataSource.OA_CONTENT_SERVER, autocreate=True
        )
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

        data_source = DataSource.lookup(
            session, DataSource.OA_CONTENT_SERVER, autocreate=True
        )

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
        assert [Contributor.Role.AUTHOR] == contributor.roles

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

        data_source = DataSource.lookup(
            session, DataSource.OA_CONTENT_SERVER, autocreate=True
        )

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
        data_source = DataSource.lookup(
            session, DataSource.OA_CONTENT_SERVER, autocreate=True
        )

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

        data_source = DataSource.lookup(
            session, DataSource.OA_CONTENT_SERVER, autocreate=True
        )

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

        data_source = DataSource.lookup(
            session, DataSource.OA_CONTENT_SERVER, autocreate=True
        )

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
        assert mouse.medium == Edition.AUDIO_MEDIUM

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
        from palace.manager.core.classifier import Classifier

        classifier = Classifier.classifiers[seven.subject.type]
        classifier.classify(seven.subject)

        def sort_key(x: LicensePool) -> str:
            assert x.presentation_edition.title is not None
            return x.presentation_edition.title

        [crow_pool, mouse_pool] = sorted(pools, key=sort_key)

        assert db.default_collection() == crow_pool.collection
        assert db.default_collection() == mouse_pool.collection
        assert crow_pool.work is not None
        assert mouse_pool.work is not None

        work = mouse_pool.work
        work.calculate_presentation()
        assert work.quality is not None
        assert 0.4142 == round(work.quality, 4)
        assert Classifier.AUDIENCE_CHILDREN == work.audience
        assert NumericRange(7, 7, "[]") == work.target_age

        # Bonus: make sure that delivery mechanisms are set appropriately.
        [mech] = mouse_pool.delivery_mechanisms
        assert (
            Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE
            == mech.delivery_mechanism.content_type
        )
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
        # so the source of the OPDS feed "OPDS" was used.
        assert {DataSource.GUTENBERG, "OPDS"} == {
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

        collection = db.collection(
            protocol=OPDSAPI, settings=db.opds_settings(data_source="some new source")
        )
        importer = opds_importer_fixture.importer(collection=collection)
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

        collection = db.collection(
            protocol=OPDSAPI,
            settings=db.opds_settings(data_source=DataSource.OVERDRIVE),
        )
        (
            imported_editions,
            imported_pools,
            imported_works,
            failures,
        ) = opds_importer_fixture.importer(collection=collection).import_from_feed(feed)

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
        # source associated with the collection.
        assert "OPDS" == mouse_pool.presentation_edition.data_source.name

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
        # were created, and their data source is the datasoruce of the collection
        assert "OPDS" == crow_pool.data_source.name

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
        assert download_manifest_url == august_pool.open_access_download_url

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
                protocol=OPDSAPI,
                settings=db.opds_settings(
                    external_account_id="http://wayfless.example.com/feed",
                    saml_wayfless_url_template="https://fsso.springer.com/saml/login?idp={idp}&targetUrl={targetUrl}",
                ),
            )
            collection.associated_libraries.append(library)

            imported_editions, pools, works, failures = OPDSImporter(
                session, collection=collection
            ).import_from_feed(feed)

            pool = pools[0]
            pool.loan_to(patron)

            return (
                CirculationAPI(
                    session, library, {collection.id: OPDSAPI(session, collection)}
                ),
                patron,
                pool,
            )

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

    @patch.object(Identifier, "parse_urn")
    def test_parse_identifier(
        self,
        mock_parse_urn: MagicMock,
        opds_importer_fixture: OPDSImporterFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # Normal case, we just call out to Identifier.parse_urn
        mock_parse_urn.return_value = ("returned value", True)
        importer = opds_importer_fixture.importer()
        test_identifier = "test"
        value = importer.parse_identifier(test_identifier)
        assert value == "returned value"
        mock_parse_urn.assert_called_once_with(
            opds_importer_fixture.db.session, test_identifier
        )

        # In the case of an exception, we log the relevant info.
        mock_parse_urn.reset_mock()
        mock_parse_urn.side_effect = ValueError("My god, it's full of stars")
        assert importer.parse_identifier(test_identifier) is None
        assert (
            "An unexpected exception occurred during parsing identifier 'test': My god, it's full of stars"
            in caplog.text
        )
        assert "Traceback" in caplog.text


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


class OPDSImportMonitorFixture:
    def collection(self, feed_url: str | None = None) -> Collection:
        feed_url = feed_url or "http://fake.opds/"
        settings = {"external_account_id": feed_url, "data_source": "OPDS"}
        return self.db.collection(protocol=OPDSAPI.label(), settings=settings)

    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db


@pytest.fixture()
def opds_import_monitor_fixture(
    db: DatabaseTransactionFixture,
) -> OPDSImportMonitorFixture:
    return OPDSImportMonitorFixture(db)


class TestOPDSImportMonitor:
    def test_constructor(self, db: DatabaseTransactionFixture):
        session = db.session

        with pytest.raises(ValueError) as excinfo:
            OPDSImportMonitor(session, None, OPDSImporter)  # type: ignore[arg-type]
        assert (
            "OPDSImportMonitor can only be run in the context of a Collection."
            in str(excinfo.value)
        )
        c1 = db.collection(protocol=OverdriveAPI)
        with pytest.raises(ValueError) as excinfo:
            OPDSImportMonitor(session, c1, OPDSImporter)
        assert (
            f"Collection {c1.name} is configured for protocol Overdrive, not OPDS Import."
            in str(excinfo.value)
        )

        c2 = db.collection(protocol=OPDSAPI)
        c2.integration_configuration.settings_dict = {}
        with pytest.raises(ValueError) as excinfo:
            OPDSImportMonitor(session, c2, OPDSImporter)
        assert f"Collection {c2.name} has no associated data source." in str(
            excinfo.value
        )

        c3 = db.collection(
            protocol=OPDSAPI,
            settings=db.opds_settings(
                external_account_id="https://opds.import.com/feed?size=100",
            ),
        )
        monitor = OPDSImportMonitor(session, c3, OPDSImporter)
        assert monitor._feed_base_url == "https://opds.import.com/"

    def test_get(
        self,
        db: DatabaseTransactionFixture,
    ):
        session = db.session

        ## Test whether relative urls work
        collection = db.collection(
            settings=db.opds_settings(
                external_account_id="https://opds.import.com:9999/feed",
            ),
        )
        monitor = OPDSImportMonitor(session, collection, OPDSImporter)

        with patch("palace.manager.core.opds_import.HTTP.get_with_timeout") as mock_get:
            monitor._get("/absolute/path", {})
            assert mock_get.call_args[0] == (
                "https://opds.import.com:9999/absolute/path",
            )

            mock_get.reset_mock()
            monitor._get("relative/path", {})
            assert mock_get.call_args[0] == (
                "https://opds.import.com:9999/relative/path",
            )

    def test_hook_methods(self, db: DatabaseTransactionFixture):
        """By default, the OPDS URL and data source used by the importer
        come from the collection configuration.
        """
        collection = db.collection()
        monitor = OPDSImportMonitor(
            db.session,
            collection,
            import_class=OPDSImporter,
        )

        assert collection.data_source == monitor.data_source(collection)

    def test_feed_contains_new_data(
        self,
        opds_importer_fixture: OPDSImporterFixture,
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        feed = data.content_server_mini_feed

        class MockOPDSImportMonitor(OPDSImportMonitor):
            def _get(self, url, headers):
                return MockRequestsResponse(
                    200, {"content-type": AtomFeed.ATOM_TYPE}, feed
                )

        data_source_name = "OPDS"
        collection = db.collection(
            protocol=OPDSAPI,
            settings=db.opds_settings(data_source=data_source_name),
        )
        monitor = OPDSImportMonitor(
            session,
            collection,
            import_class=OPDSImporter,
        )
        timestamp = monitor.timestamp()

        # Nothing has been imported yet, so all data is new.
        assert monitor.feed_contains_new_data(feed) is True
        assert timestamp.start is None

        # Now import the editions.
        monitor = MockOPDSImportMonitor(
            session,
            collection=collection,
            import_class=OPDSImporter,
        )
        monitor.run()

        # Editions have been imported.
        assert 2 == session.query(Edition).count()

        # The timestamp has been updated, although unlike most
        # Monitors the timestamp is purely informational.
        assert timestamp.finish is not None

        editions = session.query(Edition).all()
        data_source = DataSource.lookup(session, data_source_name)

        # If there are CoverageRecords that record work are after the updated
        # dates, there's nothing new.
        record, ignore = CoverageRecord.add_for(
            editions[0],
            data_source,
            CoverageRecord.IMPORT_OPERATION,
            collection=collection,
        )
        record.timestamp = datetime_utc(2016, 1, 1, 1, 1, 1)

        record2, ignore = CoverageRecord.add_for(
            editions[1],
            data_source,
            CoverageRecord.IMPORT_OPERATION,
            collection=collection,
        )
        record2.timestamp = datetime_utc(2016, 1, 1, 1, 1, 1)

        assert monitor.feed_contains_new_data(feed) is False

        # If the monitor is set up to force reimport, it doesn't
        # matter that there's nothing new--we act as though there is.
        monitor.force_reimport = True
        assert monitor.feed_contains_new_data(feed) is True
        monitor.force_reimport = False

        # If an entry was updated after the date given in that entry's
        # CoverageRecord, there's new data.
        record2.timestamp = datetime_utc(1970, 1, 1, 1, 1, 1)
        assert monitor.feed_contains_new_data(feed) is True

        # If a CoverageRecord is a transient failure, we try again
        # regardless of whether it's been updated.
        for r in [record, record2]:
            r.timestamp = datetime_utc(2016, 1, 1, 1, 1, 1)
            r.exception = "Failure!"
            r.status = CoverageRecord.TRANSIENT_FAILURE
        assert monitor.feed_contains_new_data(feed) is True

        # If a CoverageRecord is a persistent failure, we don't try again...
        for r in [record, record2]:
            r.status = CoverageRecord.PERSISTENT_FAILURE
        assert monitor.feed_contains_new_data(feed) is False

        # ...unless the feed updates.
        record.timestamp = datetime_utc(1970, 1, 1, 1, 1, 1)
        assert monitor.feed_contains_new_data(feed) is True

    def test_follow_one_link(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )
        data_source_name = "OPDS"
        collection = db.collection(
            protocol=OPDSAPI,
            settings=db.opds_settings(data_source=data_source_name),
        )
        monitor = OPDSImportMonitor(
            session,
            collection=collection,
            import_class=OPDSImporter,
        )
        feed = data.content_server_mini_feed

        http = MockHTTPClient()

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
        data_source = DataSource.lookup(session, data_source_name)

        for edition in editions:
            record, ignore = CoverageRecord.add_for(
                edition,
                data_source,
                CoverageRecord.IMPORT_OPERATION,
                collection=collection,
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
        data_source_name = "OPDS"
        collection = db.collection(
            settings=db.opds_settings(
                external_account_id="http://root-url/index.xml",
                data_source=data_source_name,
            ),
        )
        monitor = OPDSImportMonitor(
            session,
            collection=collection,
            import_class=DoomedOPDSImporter,
        )
        data_source = DataSource.lookup(session, data_source_name)

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
            collection=collection,
        )
        assert CoverageRecord.SUCCESS == record.status
        assert record.exception is None

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
            collection=collection,
        )
        assert "Utter failure!" in failure.exception

        # Both failures were reported in the return value from
        # import_one_feed
        assert 2 == len(failures)

    def test_run_once(self, db: DatabaseTransactionFixture):
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

        collection = db.collection()

        monitor = MockOPDSImportMonitor(
            db.session,
            collection=collection,
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
        assert progress.start is None
        assert progress.finish is None

    def test_update_headers(self, db: DatabaseTransactionFixture):
        collection = db.collection()

        # Test the _update_headers helper method.
        monitor = OPDSImportMonitor(
            db.session,
            collection=collection,
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

        collection = db.collection(
            settings=db.opds_settings(
                external_account_id=feed_url,
                max_retry_count=retry_count,
            ),
        )

        # The importer takes its retry count from the collection settings.
        monitor = OPDSImportMonitor(
            session,
            collection=collection,
            import_class=OPDSImporter,
        )

        # We mock Retry class to ensure that the correct retry count had been passed.
        with patch("palace.manager.util.http.Retry") as retry_constructor_mock:
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
            protocol=OPDSAPI,
            settings=db.opds_settings(
                external_account_id="http://opds2.example.org/feed",
            ),
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
        delivery_mechanism = MagicMock(spec=LicensePoolDeliveryMechanism)
        loan = opds_api_fixture.api.checkout(
            opds_api_fixture.mock_patron,
            opds_api_fixture.mock_pin,
            opds_api_fixture.mock_licensepool,
            delivery_mechanism,
        )
        assert isinstance(loan, LoanInfo)
        assert loan.collection_id == opds_api_fixture.mock_licensepool.collection_id

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
        assert isinstance(fulfillment, RedirectFulfillment)
        assert fulfillment.content_link == mock_lpdm.resource.representation.public_url
        assert fulfillment.content_type == mock_lpdm.resource.representation.media_type
